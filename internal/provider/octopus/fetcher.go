package octopus

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/beaujr/emprometheus/internal/provider"
)

type Results struct {
	Count    int         `json:"count"`
	Next     string      `json:"next"`
	Previous interface{} `json:"previous"`
	Results  []Result    `json:"results"`
}
type Result struct {
	ValueExcVat   float64     `json:"value_exc_vat"`
	ValueIncVat   float64     `json:"value_inc_vat"`
	ValidFrom     time.Time   `json:"valid_from"`
	ValidTo       time.Time   `json:"valid_to"`
	PaymentMethod interface{} `json:"payment_method"`
}

type Octopus struct {
	dir, product, tariff string
	loc                  *time.Location
	client               *http.Client
}

type Option func(*Octopus)

func WithClient(c *http.Client) Option {
	return func(o *Octopus) {
		o.client = c
	}
}

func New(product, tariff, dir string, loc *time.Location, opts ...Option) *Octopus {
	o := &Octopus{
		dir:     dir,
		product: product,
		tariff:  tariff,
		loc:     loc,
		client:  &http.Client{Timeout: 30 * time.Second},
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func (o *Octopus) Fetch(ctx context.Context) ([]Result, error) {
	url := fmt.Sprintf("https://api.octopus.energy/v1/products/%s/electricity-tariffs/%s/standard-unit-rates/?page_size=100", o.product, o.tariff)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	res, err := o.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	out, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	var r Results
	err = json.Unmarshal(out, &r)
	if err != nil {
		return nil, err
	}
	return r.Results, nil
}

func (o *Octopus) GenerateOctopusTariff(ctx context.Context, steps int) error {
	now := time.Now()

	resolution := (time.Hour * 24) / time.Duration(steps)
	results, err := o.Fetch(ctx)
	if err != nil {
		return err
	}
	contents := o.generateTariffContents(results, nextTariffBoundary(now, resolution), time.Hour*24, resolution)
	body := bytes.Join(contents, nil)
	fo, err := os.Create(filepath.Join(o.dir, provider.CSVFileName))
	if err != nil {
		return err
	}
	defer func() {
		if err = fo.Close(); err != nil {
			panic(err)
		}
	}()
	if _, err = fo.Write(body); err != nil {
		return err
	}
	return nil
}

// todo: remove this, exists as manual fix for debugging
func ProduceOctopusCosyTariff(dir string) error {
	// open output file
	fo, err := os.Create(filepath.Join(dir, provider.CSVFileName))
	if err != nil {
		return err
	}
	// close fo on exit and check for its returned error
	defer func() {
		if err := fo.Close(); err != nil {
			panic(err)
		}
	}()
	// Rates in £/kWh
	cosy := 0.1017471
	peak := 0.383418
	standard := 0.2439213

	// Cosy time windows
	cosyWindows := []struct {
		start int // hour inclusive
		end   int // hour exclusive
	}{
		{4, 7},
		{13, 16},
		{22, 24},
		{0, 0}, // handled by day-split logic
	}

	// Peak window (16:00–19:00)
	peakStart := 16
	peakEnd := 19
	loc, err := time.LoadLocation("Europe/London")
	if err != nil {
		return err
	}
	now := time.Now().In(loc)
	start := now.Truncate(time.Hour).Add(time.Hour)

	steps := 24
	t := start

	for i := 0; i < steps; i++ {
		hour := t.Hour()

		// Determine rate
		rate := standard

		// Peak
		if hour >= peakStart && hour < peakEnd {
			rate = peak
		}

		// Cosy
		for _, w := range cosyWindows {
			if w.start <= hour && hour < w.end {
				rate = cosy
				break
			}
		}

		// Print CSV line
		line := fmt.Sprintf("%s,%.4f\n", t.In(loc).Format("2006-01-02 15:04:05-07:00"), rate)
		if _, err = fo.Write([]byte(line)); err != nil {
			return err
		}
		t = t.Add(time.Hour)
	}
	return nil
}

func (o *Octopus) generateTariffContents(
	results []Result,
	start time.Time,
	duration time.Duration,
	interval time.Duration,
) [][]byte {
	if duration <= 0 {
		return nil
	}

	if interval <= 0 {
		return nil
	}

	steps := int(duration / interval)
	if steps <= 0 {
		return nil
	}

	// Ensure results are ordered. This isn't strictly required for correctness,
	// but makes lookup deterministic and allows the lookup to be optimised.
	slices.SortFunc(results, func(a, b Result) int {
		return a.ValidFrom.Compare(b.ValidFrom)
	})

	contents := make([][]byte, 0, steps)

	for i := 0; i < steps; i++ {
		t := start.Add(time.Duration(i) * interval)

		row, ok := findTariff(results, t)
		if !ok {
			continue
		}

		line := fmt.Sprintf(
			"%s,%.4f\n",
			t.In(o.loc).Format("2006-01-02 15:04:05-07:00"),
			row.ValueIncVat/100,
		)

		contents = append(contents, []byte(line))
	}

	return contents
}

func findTariff(results []Result, t time.Time) (Result, bool) {
	for _, row := range results {
		if !row.ValidFrom.After(t) && row.ValidTo.After(t) {
			return row, true
		}
	}

	return Result{}, false
}

func nextTariffBoundary(now time.Time, resolution time.Duration) time.Time {
	if resolution <= 0 {
		return now
	}

	truncated := now.Truncate(resolution)
	if !truncated.After(now) {
		truncated = truncated.Add(resolution)
	}

	return truncated
}
