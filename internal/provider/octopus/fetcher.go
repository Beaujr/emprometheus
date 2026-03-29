package octopus

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
	_ "time/tzdata"

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
	dir, product, tariff, timezone string
}

func New(product, tariff, dir, timezone string) *Octopus {
	return &Octopus{
		dir:      dir,
		product:  product,
		tariff:   tariff,
		timezone: timezone,
	}
}

func (o *Octopus) GenerateOctopusTariff() error {
	client := http.Client{Timeout: 180 * time.Second}
	url := fmt.Sprintf("https://api.octopus.energy/v1/products/%s/electricity-tariffs/%s/standard-unit-rates/?page_size=100", o.product, o.tariff)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	out, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	var r Results
	err = json.Unmarshal(out, &r)
	if err != nil {
		return err
	}
	slices.SortFunc(r.Results, func(a, b Result) int {
		return cmp.Compare(a.ValidFrom.Unix(), b.ValidFrom.Unix())
	})
	fo, err := os.Create(filepath.Join(o.dir, provider.CSVFileName))
	if err != nil {
		return err
	}
	// close fo on exit and check for its returned error
	defer func() {
		if err = fo.Close(); err != nil {
			panic(err)
		}
	}()
	now := time.Now()
	steps := 24
	start := now.Truncate(time.Hour).Add(time.Hour)
	loc, err := time.LoadLocation(o.timezone)
	if err != nil {
		return err
	}

	t := start
	var contents [][]byte
	for i := 0; i < steps; i++ {
		for _, row := range r.Results {
			if (row.ValidFrom.Before(t) || row.ValidFrom.Equal(t)) && row.ValidTo.After(t) {
				// Print CSV line
				line := fmt.Sprintf("%s,%.4f\n", t.In(loc).Format("2006-01-02 15:04:05-07:00"), row.ValueIncVat/100)
				contents = append(contents, []byte(line))
			}
		}
		step := float64(24*60) / float64(steps*60)
		d := time.Duration(step * 60)
		t = t.Add(d * time.Minute)
	}
	if len(contents) != steps {
		// todo fix later, but useful for debug now
		switch {
		case o.product == "COSY-FIX-12M-25-09-24" && o.tariff == "E-1R-COSY-FIX-12M-25-09-24-N":
			return o.produceOctopusCosyTariff()
		case strings.HasPrefix(o.product, "AGILE"):
			t = time.Now()
			for _, row := range r.Results {
				if row.ValidFrom.Before(t) && row.ValidFrom.After(t.AddDate(0, 0, -1)) {
					// Print CSV line
					line := fmt.Sprintf("%s,%.4f\n", t.In(loc).Format("2006-01-02 15:04:05-07:00"), row.ValueIncVat/100)
					contents = append(contents, []byte(line))
				}
			}
		default:
			return provider.TariffNotAvailable
		}
		return provider.TariffNotAvailable
	}
	body := bytes.Join(contents, nil)
	if _, err = fo.Write(body); err != nil {
		return err
	}
	return nil
}

// todo: remove this, exists as manual fix for debugging
func (o *Octopus) produceOctopusCosyTariff() error {
	// open output file
	fo, err := os.Create(filepath.Join(o.dir, provider.CSVFileName))
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
	cosy := 0.1368
	peak := 0.4185
	standard := 0.279

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
	loc, err := time.LoadLocation(o.timezone)
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
