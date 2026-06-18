package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
	_ "time/tzdata"

	"github.com/beaujr/emprometheus/internal/emhass"
	"github.com/beaujr/emprometheus/internal/provider"
	"github.com/beaujr/emprometheus/internal/provider/octopus"
	"github.com/beaujr/emprometheus/internal/server/hass"
	"github.com/prometheus/common/model"
)

var (
	emhassUrl = flag.String("emhass_url", "http://localhost:5000", "Emhass")
	dir       = flag.String("dir", "./emhass_config/data", "the directory to serve files from")
)

func main() {
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	var fetcher = func(_ int) error {
		return octopus.ProduceOctopusCosyTariff(*dir)
	}

	ha := hass.New(logger, fetcher, &FakePrometheus{}, 60)
	mux := http.NewServeMux()
	ha.Register(mux)
	go func() {
		if err := http.ListenAndServe(":8123", mux); err != nil {
			panic(err)
		}
	}()
	c := http.Client{Timeout: 60 * time.Second}
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/action/dayahead-optim", *emhassUrl), nil)
	if err != nil {
		panic(err)
	}
	resp, err := c.Do(req)
	if err != nil {
		panic(err)
	}
	defer func(Body io.ReadCloser) {
		err = Body.Close()
		if err != nil {
			logger.Error(err.Error())
		}
	}(resp.Body)
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		panic(fmt.Errorf("unexpected status code: %d", resp.StatusCode))
	}
	optimizationFile := filepath.Join(*dir, provider.CSVForecastName)
	logger.Info("opening file")
	sourceFile, err := os.Open(optimizationFile)
	if err != nil {
		panic(err)
	}
	defer func(sourceFile *os.File) {
		err = sourceFile.Close()
		if err != nil {
			logger.Error(err.Error())
		}
	}(sourceFile)
	_, err = sourceFile.Seek(0, io.SeekStart)
	if err != nil {
		panic(err)
	}
	reader := bufio.NewScanner(sourceFile)
	results, err := emhass.ReadOptimizationResults(logger, reader, provider.ActionForecast)
	if err != nil {
		panic(err)
	}
	logger.Info("reading finished", slog.Int("rows", len(results)))

}

type FakePrometheus struct{}

func (f FakePrometheus) GetRange(
	_ context.Context,
	query string,
	start, end time.Time,
	step time.Duration,
) (model.Matrix, error) {

	if step <= 0 || end.Before(start) {
		return nil, fmt.Errorf("invalid time range")
	}

	// Seed based on query so same query returns same "random" series
	h := fnv.New64a()
	_, _ = h.Write([]byte(query))
	seed := int64(h.Sum64())

	rnd := rand.New(rand.NewSource(seed))

	// Generate a small number of fake series (1–3)
	numSeries := rnd.Intn(3) + 1

	matrix := make(model.Matrix, 0, numSeries)

	for s := 0; s < numSeries; s++ {
		labels := model.Metric{
			"__name__": model.LabelValue("fake_metric"),
			"query":    model.LabelValue(query),
			"series":   model.LabelValue(strconv.Itoa(s)),
		}

		var samples []model.SamplePair

		// random base + drift per series
		base := rnd.Float64()*50 + float64(s*10)
		trend := (rnd.Float64() - 0.5) * 0.01

		i := 0
		for t := start; !t.After(end); t = t.Add(step) {
			noise := (rnd.Float64() - 0.5) * 5.0

			// simple random walk + noise
			val := base + float64(i)*trend + noise

			// clamp to avoid negative nonsense unless desired
			if val < 0 {
				val = 0
			}

			samples = append(samples, model.SamplePair{
				Timestamp: model.TimeFromUnixNano(t.UnixNano()),
				Value:     model.SampleValue(val),
			})

			i++
		}

		matrix = append(matrix, &model.SampleStream{
			Metric: labels,
			Values: samples,
		})
	}

	return matrix, nil
}
