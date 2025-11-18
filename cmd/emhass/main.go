package main

import (
	"cmp"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	p "github.com/beaujr/emprometheus/internal/prometheus"
	s "github.com/beaujr/emprometheus/internal/server"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"
)

var (
	dir            = flag.String("dir", "./emhass_config/data", "the directory to serve files from")
	tariff         = flag.Bool("tariff", false, "generate tariff files")
	process        = flag.Bool("process", false, "process emhasses")
	server         = flag.Bool("server", false, "run data server")
	promApi        = flag.String("prometheus", "http://192.168.1.112:9090", "http://promapi:port")
	promUser       = flag.String("prometheus.user", "", "http://promapi:port")
	promPass       = flag.String("prometheus.pass", "", "http://promapi:port")
	octopusProduct = flag.String("octopus.product", "COSY-FIX-12M-25-09-24", "Octopus Product")
	octopusTariff  = flag.String("octopus.tariff", "E-1R-COSY-FIX-12M-25-09-24-N", "Octopus Tariff")
)

type basicAuthTransport struct {
	Transport          http.RoundTripper
	Username, Password string
}

func (bat *basicAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.SetBasicAuth(bat.Username, bat.Password)
	return bat.Transport.RoundTrip(req)
}

func main() {
	flag.Parse()
	if *tariff {
		generateOctopusTariff()
	}
	if *process {
		output()
		return
	}

	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if *server {
		client := &http.Client{
			Timeout: 180 * time.Second,
		}
		if len(*promPass) > 0 {
			client.Transport = &basicAuthTransport{
				Transport: http.DefaultTransport, // Use the default transport
				Username:  *promUser,
				Password:  *promPass,
			}
		}
		pclient, err := api.NewClient(api.Config{
			Address:      *promApi,
			RoundTripper: client.Transport,
		})
		if err != nil {
			panic(err.Error())
		}

		if err = s.NewServer(ctx, logger, generateAndReview, p.New(v1.NewAPI(pclient))); err != nil {
			panic(err.Error())
		}
		return
	}
}

type Row struct {
	Timestamp time.Time
	PPV       float64
	Load      float64
	PBatt     float64
	SOC       float64
	Price     float64
}

func parseFloat(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

func generateAndReview() {
	generateOctopusTariff()
	output()
}

func output() {
	file, err := os.Open(filepath.Join(*dir, "opt_res_latest.csv"))
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	allRows, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}

	var rows []Row
	for i := 1; i < len(allRows); i++ {
		r := allRows[i]

		t, err := time.Parse("2006-01-02 15:04:05-07:00", r[0])
		if err != nil {
			fmt.Println("Timestamp parse error:", r[0], err)
			continue
		}
		rows = append(rows, Row{
			Timestamp: t,
			PPV:       parseFloat(r[1]),
			Load:      parseFloat(r[2]),
			PBatt:     parseFloat(r[6]),
			SOC:       parseFloat(r[7]),
			Price:     parseFloat(r[8]),
		})
	}

	if len(rows) == 0 {
		fmt.Println("No rows parsed.")
		return
	}
	for idx, r := range rows {
		if r.PBatt < 0 {
			if r.PPV > r.Load {
				// Load First: Work mode priority
				// Battery first grid charge: Disabled
				// Load first stop discharge: 10% (dont set min battery when PV will pay for load)
				fmt.Println(fmt.Sprintf("%s PV Charge Battery: %f to %f", r.Timestamp, r.PBatt, r.SOC*100))
				continue
			}
			// Work mode priority: Battery First
			// Battery first grid charge: Enabled
			// Load first stop discharge: 100%
			fmt.Println(fmt.Sprintf("%s Grid Charge Battery: %f to %f", r.Timestamp, r.PBatt, r.SOC*100))
			continue
		}
		if idx > 0 && rows[idx-1].SOC > r.SOC {
			fmt.Println(fmt.Sprintf("%s Use Battery: %f to %f", r.Timestamp, r.PBatt, r.SOC*100))
		}
	}

}

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

func generateOctopusTariff() {
	client := http.Client{Timeout: 180 * time.Second}
	url := fmt.Sprintf("https://api.octopus.energy/v1/products/%s/electricity-tariffs/%s/standard-unit-rates/?page_size=100", *octopusProduct, *octopusTariff)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return
	}
	res, err := client.Do(req)
	if err != nil {
		return
	}
	out, err := io.ReadAll(res.Body)
	if err != nil {
		return
	}
	var r Results
	err = json.Unmarshal(out, &r)
	if err != nil {
		return
	}
	slices.SortFunc(r.Results, func(a, b Result) int {
		return cmp.Compare(a.ValidFrom.Unix(), b.ValidFrom.Unix())
	})
	fo, err := os.Create(filepath.Join(*dir, "data_load_cost_forecast.csv"))
	if err != nil {
		panic(err)
	}
	// close fo on exit and check for its returned error
	defer func() {
		if err := fo.Close(); err != nil {
			panic(err)
		}
	}()
	now := time.Now()
	start := now.Truncate(time.Hour).Add(time.Hour)

	steps := 24
	t := start

	for i := 0; i < steps; i++ {
		for _, row := range r.Results {
			if (row.ValidFrom.Before(t) || row.ValidFrom.Equal(t)) && row.ValidTo.After(t) {
				// Print CSV line
				line := fmt.Sprintf("%s,%.4f\n", t.Local().Format("2006-01-02 15:04:05-07:00"), row.ValueIncVat)
				if _, err = fo.Write([]byte(line)); err != nil {
					panic(err)
				}
			}
		}
		t = t.Add(time.Hour)
	}
}

func produceTariff() {
	// lazy
	if !*tariff {
		return
	}
	// open output file
	fo, err := os.Create(filepath.Join(*dir, "data_load_cost_forecast.csv"))
	if err != nil {
		panic(err)
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

	now := time.Now().Local()
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
		line := fmt.Sprintf("%s,%.4f\n", t.Local().Format("2006-01-02 15:04:05-07:00"), rate)
		if _, err = fo.Write([]byte(line)); err != nil {
			panic(err)
		}
		t = t.Add(time.Hour)
	}
}
