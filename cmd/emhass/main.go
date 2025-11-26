package main

import (
	"context"
	"flag"
	"fmt"
	p "github.com/beaujr/emprometheus/internal/prometheus"
	"github.com/beaujr/emprometheus/internal/provider"
	"github.com/beaujr/emprometheus/internal/provider/octopus"
	s "github.com/beaujr/emprometheus/internal/server"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	temporalsdk "go.temporal.io/sdk/client"
	"log"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

var (
	dir               = flag.String("dir", "./emhass_config/data", "the directory to serve files from")
	tariff            = flag.Bool("tariff", false, "generate tariff files")
	process           = flag.Bool("process", false, "process emhasses")
	server            = flag.Bool("server", false, "run data server")
	promApi           = flag.String("prometheus", "http://192.168.1.112:9090", "http://promapi:port")
	promUser          = flag.String("prometheus.user", "", "http://promapi:port")
	promPass          = flag.String("prometheus.pass", "", "http://promapi:port")
	octopusProduct    = flag.String("octopus.product", "COSY-FIX-12M-25-09-24", "Octopus Product")
	octopusTariff     = flag.String("octopus.tariff", "E-1R-COSY-FIX-12M-25-09-24-N", "Octopus Tariff")
	useTemporal       = flag.Bool("temporal.enable", false, "use temporal")
	temporalNamespace = flag.String("temporal.namespace", "beau", "temporal namespace")
	temporalAddress   = flag.String("temporal.address", "temporal-frontend-headless.temporal.svc.cluster.local:7233", "temporal address")
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
	ctx := context.Background()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	var rateFetcher provider.RateFetcher = func() error {
		logger.Warn(fmt.Sprintf("rate fetcher is disabled, relying on %s already existing", provider.CSVFileName))
		// dont modify any files and rely on external file creating data_load_cost_forecast.csv
		return nil
	}
	if *tariff {
		if *octopusProduct != "" {
			o := octopus.New(*octopusProduct, *octopusTariff, *dir)
			rateFetcher = o.GenerateOctopusTariff
		}
	}
	if *process {
		return
	}

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
		var c temporalsdk.Client
		if *useTemporal {
			// The client is a heavyweight object that should be created once per process.
			temporalClient, err := temporalsdk.Dial(temporalsdk.Options{
				HostPort:  *temporalAddress,
				Namespace: *temporalNamespace,
			})
			if err != nil {
				log.Fatalln("Unable to create client", err)
			}
			c = temporalClient
			defer c.Close()
		}

		if err = s.NewServer(ctx, logger, rateFetcher, c, p.New(v1.NewAPI(pclient)), *dir); err != nil {
			panic(err.Error())
		}
		return
	}
}

// todo: remove this, exists as manual fix for debugging
func produceOctopusCosyTariff() error {
	// lazy
	if !*tariff {
		return provider.TariffNotAvailable
	}
	// open output file
	fo, err := os.Create(filepath.Join(*dir, provider.CSVFileName))
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
			return err
		}
		t = t.Add(time.Hour)
	}
	return nil
}
