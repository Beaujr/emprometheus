package emhass

import (
	"bufio"
	"github.com/beaujr/emprometheus/internal/provider"
	"github.com/beaujr/emprometheus/internal/store"
	"github.com/beaujr/emprometheus/internal/types"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
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

func (e *Emhass) Forecast(forecastMethod, body string) error {
	forecastURL := e.baseUrl.ResolveReference(&url.URL{Path: filepath.Join(e.baseUrl.Path, forecastMethod)})
	c := http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest(http.MethodPost, forecastURL.String(), strings.NewReader(body))
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
	for _, o := range results {
		err = e.db.InsertOptimization(o)
		if err != nil {
			logger.Error("failed inserting optimization", slog.String("error", err.Error()))
			return err
		}
	}
	return nil
}
