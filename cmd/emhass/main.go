package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	p "github.com/beaujr/emprometheus/internal/prometheus"
	s "github.com/beaujr/emprometheus/internal/server"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

var (
	dir      = flag.String("dir", "./emhass_config/data", "the directory to serve files from")
	tariff   = flag.Bool("tariff", false, "generate tariff files")
	process  = flag.Bool("process", false, "process emhasses")
	server   = flag.Bool("server", false, "run data server")
	promApi  = flag.String("prometheus", "http://192.168.1.112:9090", "http://promapi:port")
	promUser = flag.String("prometheus.user", "", "http://promapi:port")
	promPass = flag.String("prometheus.pass", "", "http://promapi:port")
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
		produceTariff()
	}
	if *process {
		output()
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

		if err = s.NewServer(ctx, logger, produceTariff, p.New(v1.NewAPI(pclient))); err != nil {
			panic(err.Error())
		}
		return
	}
}

type Row struct {
	Timestamp time.Time
	PPV       float64
	PBatt     float64
	SOC       float64
	Price     float64
}

func parseFloat(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

func output() {
	// --- CONFIGURABLE PARAMETERS ---
	const chargePower = 2500.0       // W
	const batteryCapacity = 10000.0  // Wh (10 kWh example; adjust!)
	const targetSOC = 0.90           // desired SOC after charge
	const cheapPriceThreshold = 0.20 // cheap energy threshold

	// --- OPEN CSV ---
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

	// skip header
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
			PBatt:     parseFloat(r[6]),
			SOC:       parseFloat(r[7]),
			Price:     parseFloat(r[8]),
		})
	}

	if len(rows) == 0 {
		fmt.Println("No rows parsed.")
		return
	}

	// --- Current SOC ---
	currentSOC := rows[0].SOC
	socDelta := targetSOC - currentSOC
	if socDelta <= 0 {
		fmt.Printf("SOC already above target (%.2f). No grid charging needed.\n", currentSOC)
		return
	}

	// --- Energy & Time Required ---
	energyNeededWh := socDelta * batteryCapacity
	hoursNeeded := energyNeededWh / chargePower

	fmt.Printf("Current SOC: %.3f → Target: %.3f\n", currentSOC, targetSOC)
	fmt.Printf("Energy needed: %.1f Wh\n", energyNeededWh)
	fmt.Printf("Charging hours required: %.2f\n\n", hoursNeeded)

	// --- Detect cheap windows ---
	type Window struct {
		Start time.Time
		End   time.Time
		Rows  []Row
	}

	var windows []Window
	var curr Window

	for _, r := range rows {
		if r.Price < cheapPriceThreshold {
			// inside cheap window
			if curr.Start.IsZero() {
				curr.Start = r.Timestamp
			}
			curr.Rows = append(curr.Rows, r)
			curr.End = r.Timestamp
		} else {
			// window finished
			if len(curr.Rows) > 0 {
				windows = append(windows, curr)
			}
			curr = Window{}
		}
	}
	if len(curr.Rows) > 0 {
		windows = append(windows, curr)
	}

	if len(windows) == 0 {
		fmt.Println("No cheap windows found.")
		return
	}

	fmt.Println("Detected cheap windows:")
	for _, w := range windows {
		fmt.Printf(" - %s → %s (%d hours)\n",
			w.Start.Format(time.RFC3339),
			w.End.Format(time.RFC3339),
			len(w.Rows))
	}
	// --- PICK LAST CHEAP WIND
}

func produceTariff() {
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
