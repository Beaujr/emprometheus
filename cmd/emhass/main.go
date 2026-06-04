package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	_ "time/tzdata"

	p "github.com/beaujr/emprometheus/internal/prometheus"
	"github.com/beaujr/emprometheus/internal/provider"
	"github.com/beaujr/emprometheus/internal/provider/octopus"
	"github.com/beaujr/emprometheus/internal/scheduler"
	s "github.com/beaujr/emprometheus/internal/server"
	"github.com/beaujr/emprometheus/internal/solarassistant"
	"github.com/beaujr/emprometheus/internal/store"
	"github.com/beaujr/emprometheus/internal/store/postgres"
	"github.com/beaujr/emprometheus/internal/temporal"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	temporalsdk "go.temporal.io/sdk/client"
	"golang.org/x/sync/errgroup"
)

var (
	dir                    = flag.String("dir", "./emhass_config/data", "the directory to serve files from")
	tariff                 = flag.Bool("tariff", false, "generate tariff files")
	process                = flag.Bool("process", false, "process emhasses")
	server                 = flag.Bool("server", false, "run data server")
	promApi                = flag.String("prometheus", "http://192.168.1.112:9090", "http://promapi:port")
	promUser               = flag.String("prometheus.user", "", "http://promapi:port")
	promPass               = flag.String("prometheus.pass", "", "http://promapi:port")
	octopusProduct         = flag.String("octopus.product", "COSY-FIX-12M-25-09-24", "Octopus Product")
	octopusTariff          = flag.String("octopus.tariff", "E-1R-COSY-FIX-12M-25-09-24-N", "Octopus Tariff")
	useTemporal            = flag.Bool("temporal.enable", false, "use temporal")
	temporalNamespace      = flag.String("temporal.namespace", "beau", "temporal namespace")
	temporalAddress        = flag.String("temporal.address", "temporal-frontend-headless.temporal.svc.cluster.local:7233", "temporal address")
	temporalSchedule       = flag.String("temporal.schedule", "2 11,23 * * *", "temporal schedule")
	temporalTLS            = flag.Bool("temporal.tls", false, "TLS connection for temporal client")
	mpc                    = flag.Bool("mpc", false, "use mpc or just rely on forecast")
	createSchedulesOnStart = flag.Bool("init", true, "create schedules on application start")
	dsn                    = flag.String("dsn", "", "postgres DSN if using database to store schedules")
	password               = flag.String("password", "", "password for admin endpoint")
	timezone               = flag.String("timezone", "Europe/London", "time zone")
	steps                  = flag.Int("steps", 24, "number of steps to generate")
)

type basicAuthTransport struct {
	Transport          http.RoundTripper
	Username, Password string
}

func (bat *basicAuthTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.SetBasicAuth(bat.Username, bat.Password)
	return bat.Transport.RoundTrip(req)
}

var psqldb *sql.DB

func main() {
	flag.Parse()
	sigkillCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM)
	defer stop()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	var rateFetcher provider.RateFetcher = func(steps int) error {
		logger.Warn(fmt.Sprintf("rate fetcher is disabled, relying on %s already existing", provider.CSVFileName))
		// dont modify any files and rely on external file creating data_load_cost_forecast.csv
		return nil
	}
	loc, err := time.LoadLocation(*timezone)
	if err != nil {
		logger.Error(err.Error())
		panic(err)
	}
	if *tariff {
		if *octopusProduct != "" {
			o := octopus.New(*octopusProduct, *octopusTariff, *dir, loc)
			rateFetcher = o.GenerateOctopusTariff
		}
		if err = rateFetcher(*steps); err != nil {
			logger.Warn("failed to fetch rates on start up", slog.String("error", err.Error()))
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

		// use filestore by default
		storeOpts := []store.Option{store.WithFilestore(*dir, provider.CSVScheduleName)}
		if *dsn != "" {
			psqldb, err = postgres.New(*dsn)
			if err != nil {
				panic(err.Error())
			}
			storeOpts = []store.Option{store.WithDB(psqldb)}
		}
		db, err := store.New(storeOpts...)
		if err != nil {
			panic(err.Error())
		}
		defer db.Close()

		var c temporalsdk.Client
		var sch scheduler.Scheduler = &scheduler.DebugScheduler{}
		saOptions := []solarassistant.Option{solarassistant.WithOTEL()}
		if *dsn != "" {
			saOptions = append(saOptions, solarassistant.WithStateHandler(db))
		}
		sa, err := solarassistant.New(logger.With(slog.String("pkg", "solarassistant")), saOptions...)
		if err != nil {
			panic(err.Error())
		}
		if *useTemporal {
			temporalClient, err := temporal.NewClient(logger.With(slog.String("pkg", "temporal")), *temporalAddress, *temporalNamespace, *temporalTLS)
			if err != nil {
				log.Fatalln("Unable to create client", err)
			}
			c = temporalClient
			defer c.Close()
			var temporalOpts []temporal.Option
			if *createSchedulesOnStart {
				temporalOpts = append(temporalOpts, temporal.WithInitOnStart())
			}

			temporalScheduler, err := temporal.New(sigkillCtx, logger, c, rateFetcher, sa, *temporalSchedule, *mpc, db, loc, *steps, temporalOpts...)
			if err != nil {
				panic(err.Error())
			}
			sch = temporalScheduler
		}

		srv := s.NewServer(sigkillCtx, logger, rateFetcher, querier, *dir, *password, sch, loc, db, sa, *steps)
		errGrp, ctx := errgroup.WithContext(sigkillCtx)
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
		errGrp.Go(func() error {
			return p.Serve(logger.With(slog.String("pkg", "prometheus")))
		})
		go func() {
			if err = errGrp.Wait(); err != nil {
				panic(err)
			}
		}()
		<-sigkillCtx.Done() // signal received
		logger.Warn("signal received, shutting down server...")

		// Shutdown server with timeout
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err = srv.Shutdown(shutdownCtx) // makes errgroup return error
		if err != nil {
			logger.Error(err.Error())
		}
		err = sa.Stop(shutdownCtx)
		if err != nil {
			logger.Error(err.Error())
		}
		err = srv.Shutdown(shutdownCtx)
		if err != nil {
			logger.Error(err.Error())
		}
	}
}
