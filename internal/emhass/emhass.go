package emhass

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/beaujr/emprometheus/internal/provider"
	"github.com/beaujr/emprometheus/internal/store"
	"github.com/beaujr/emprometheus/internal/types"
)

type Emhass struct {
	baseUrl *url.URL
	logger  *slog.Logger
	dir     string
	db      store.Store
}

func New(logger *slog.Logger, db store.Store, baseURL, dir string) (*Emhass, error) {
	e, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	return &Emhass{logger: logger, baseUrl: e, db: db, dir: dir}, nil
}

func (e *Emhass) Forecast(ctx context.Context, forecastMethod, body string) error {
	forecastURL := e.baseUrl.ResolveReference(&url.URL{Path: filepath.Join(e.baseUrl.Path, forecastMethod)})
	c := http.Client{Timeout: 60 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, forecastURL.String(), strings.NewReader(body))
	if err != nil {
		return err
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return err
	}
	logger := e.logger.With(slog.String("file", filepath.Join(e.dir, provider.CSVForecastName)))
	if err = e.copyFile(logger, e.dir, provider.CSVForecastName, forecastMethod); err != nil {
		logger.Error("error reading file", slog.String("error", err.Error()))
		return err
	}
	return nil
}

func (e *Emhass) copyFile(logger *slog.Logger, dir, src, forecastMethod string) error {
	optimizationFile := filepath.Join(dir, src)
	logger.Info("opening file")
	sourceFile, err := os.Open(optimizationFile)
	if err != nil {
		return err
	}
	defer sourceFile.Close()
	_, err = sourceFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	reader := bufio.NewScanner(sourceFile)
	logger.Info("reading file")
	results, err := types.ReadOptimizationResults(logger, reader, forecastMethod)
	if err != nil {
		return err
	}
	logger.Info("reading finished", slog.Int("rows", len(results)))
	priceHorizon, err := e.getLoadForecastHorizon()
	if err != nil {
		return err
	}
	for _, o := range results {
		if o.Time().After(priceHorizon) {
			continue
		}
		err = e.db.InsertOptimization(o)
		if err != nil {
			logger.Error("failed inserting optimization", slog.String("error", err.Error()))
			return err
		}
	}
	return nil
}

func (e *Emhass) getLoadForecastHorizon() (time.Time, error) {
	f, err := os.Open(filepath.Join(e.dir, provider.CSVFileName))
	defer f.Close()
	if err != nil {
		return time.Time{}, err
	}
	scanner := bufio.NewScanner(f)

	var latest time.Time

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// CSV format:
		// 2026-09-21 10:00:00+01:00,0.2439
		fields := strings.SplitN(line, ",", 2)
		if len(fields) != 2 {
			continue
		}

		t, err := time.Parse("2006-01-02 15:04:05-07:00", fields[0])
		if err != nil {
			return time.Time{}, fmt.Errorf("parse date %q: %w", fields[0], err)
		}

		if t.After(latest) {
			latest = t
		}
	}

	if err := scanner.Err(); err != nil {
		return time.Time{}, err
	}

	if latest.IsZero() {
		return time.Time{}, fmt.Errorf("no dates found in %s", provider.CSVScheduleName)
	}

	return latest, nil
}
