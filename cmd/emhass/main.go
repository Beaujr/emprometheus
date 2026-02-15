package main

import (
	"context"
	"flag"
	"fmt"
	p "github.com/beaujr/emprometheus/internal/prometheus"
	"github.com/beaujr/emprometheus/internal/provider"
	"github.com/beaujr/emprometheus/internal/provider/octopus"
	"github.com/beaujr/emprometheus/internal/scheduler"
	s "github.com/beaujr/emprometheus/internal/server"
	"github.com/beaujr/emprometheus/internal/temporal"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	temporalsdk "go.temporal.io/sdk/client"
	"golang.org/x/sync/errgroup"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
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
	temporalSchedule  = flag.String("temporal.schedule", "2 11,23 * * *", "temporal schedule")
	temporalTLS       = flag.Bool("temporal.tls", false, "TLS connection for temporal client")
	mpc               = flag.Bool("mpc", false, "use mpc or just rely on forecast")
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
		if err := rateFetcher(); err != nil {
			logger.Warn("failed to fetch rates on start up")
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
		querier := p.New(v1.NewAPI(pclient))
		var c temporalsdk.Client
		var sch scheduler.Scheduler = &scheduler.DebugScheduler{}
		if *useTemporal {
			temporalClient, err := temporal.NewClient(logger.With(slog.String("pkg", "temporal")), *temporalAddress, *temporalNamespace, *temporalTLS)
			if err != nil {
				log.Fatalln("Unable to create client", err)
			}
			c = temporalClient
			defer c.Close()
			temporalScheduler, err := temporal.New(ctx, logger, c, rateFetcher, querier, *dir, *temporalSchedule, *mpc)
			if err != nil {
				panic(err.Error())
			}
			sch = temporalScheduler
		}

		srv := s.NewServer(ctx, logger, rateFetcher, querier, *dir, sch, *mpc)
		errGrp, ctx := errgroup.WithContext(ctx)
		ctx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
		defer stop()
		errGrp.Go(func() error {
			if err = sch.Start(ctx); err != nil {
				return err
			}
			return nil
		})
		errGrp.Go(func() error {
			if err = srv.ListenAndServe(); err != nil {
				if err = srv.Shutdown(ctx); err != nil {
					return err
				}
				if err = srv.Close(); err != nil {
					return err
				}
			}
			return nil
		})
		// Goroutine to watch for signal
		errGrp.Go(func() error {
			<-ctx.Done() // signal received
			fmt.Println("signal received, shutting down server...")

			// Shutdown server with timeout
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			return srv.Shutdown(shutdownCtx) // makes errgroup return error
		})

		if err = errGrp.Wait(); err != nil {
			panic(err)
		}
	}
}
