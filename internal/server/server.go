package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/beaujr/emprometheus/internal/prometheus"
	"github.com/beaujr/emprometheus/internal/provider"
	"github.com/beaujr/emprometheus/internal/scheduler"
	"github.com/beaujr/emprometheus/internal/store"
	"github.com/prometheus/common/model"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Server struct {
	logger    *slog.Logger
	r         prometheus.Reporter
	tariff    provider.RateFetcher
	dir       string
	scheduler scheduler.Scheduler
	db        store.Store
}

func NewServer(ctx context.Context, logger *slog.Logger, tariffs provider.RateFetcher, r prometheus.Reporter, dir string, scheduler scheduler.Scheduler, mpc bool, db store.Store) *http.Server {
	s := &Server{
		logger:    logger,
		r:         r,
		tariff:    tariffs,
		dir:       dir,
		scheduler: scheduler,
		db:        db,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/history/period/{range}", s.Handle)
	mux.HandleFunc("/init", func(w http.ResponseWriter, r *http.Request) {
		if err := s.scheduler.InitForecastSchedule(ctx); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		return
	})
	mux.HandleFunc("/process", func(w http.ResponseWriter, r *http.Request) {
		method := provider.ActionForecast
		if forecastMethod := r.Header.Get("Forecast-Method"); len(forecastMethod) > 0 {
			method = forecastMethod
		}
		if err := s.scheduler.Run(ctx, dir, method); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		return
	})
	mux.HandleFunc("/action/{forecast}", func(w http.ResponseWriter, r *http.Request) {
		c := http.Client{Timeout: 10 * time.Second}
		req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:5000%s", r.URL.Path), r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		resp, err := c.Do(req)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(resp.StatusCode)
		_, err = io.Copy(w, resp.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		fm := r.PathValue("forecast")
		if err = s.copyFile(dir, provider.CSVForecastName, fm); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
	})
	mux.HandleFunc("/schedule", func(w http.ResponseWriter, r *http.Request) {
		startOfToday := time.Date(
			time.Now().Year(),
			time.Now().Month(),
			time.Now().Day(),
			0, 0, 0, 0,
			time.Now().Location(),
		)
		rows, err := db.Select(startOfToday)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
		}
		if _, err = w.Write([]byte("Optimization,time,Work Mode, Grid Charge, Stop Discharge at SOC, Target SOC\n")); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		for _, row := range rows {
			if _, err = w.Write([]byte(row.String() + "\n")); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		return
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		s.logger.Info(r.URL.Path)
	})
	mux.HandleFunc("/api/config", func(w http.ResponseWriter, r *http.Request) {
		if err := s.tariff(); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		resp := `{}`
		w.Write([]byte(resp))
	})
	s.logger.Info("server started")
	srv := http.Server{Addr: ":8123", Handler: mux}

	return &srv
}

func (s *Server) copyFile(dir, src, forecastMethod string) error {
	sourceFile, err := os.Open(filepath.Join(dir, src))
	if err != nil {
		return err
	}
	defer sourceFile.Close()
	_, err = sourceFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	reader := bufio.NewScanner(sourceFile)
	idx := 0
	for reader.Scan() {
		if idx == 0 {
			idx++
			continue
		}
		o, err := store.OptimizationFromString(fmt.Sprintf("%s,%s", forecastMethod, reader.Text()))
		if err != nil {
			return err
		}
		err = s.db.InsertOptimization(o)
		if err != nil {
			return err
		}
	}
	return err
}

func (s *Server) Handle(w http.ResponseWriter, r *http.Request) {
	period := r.PathValue("range")
	rLogger := s.logger.With(slog.String("path", r.URL.Path+"?"+r.URL.RawQuery))
	rLogger.Info("request started")
	entity := r.URL.Query().Get("filter_entity_id")
	pieces := strings.Split(entity, ".")
	if len(pieces) != 2 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	cleanStart := strings.ReplaceAll(strings.ReplaceAll(period, "T", " "), "Z", "")
	start, err := time.Parse(time.DateTime, cleanStart[:19])
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	step := (time.Now().Unix() - start.Unix()) / 11000
	if step < 3600 {
		step = 300
	}
	query := fmt.Sprintf("%s unless changes(%s[2m]) == 0", pieces[1], pieces[1])
	values, err := getRange(r.Context(), s.r, query, start, time.Now(), step)
	if err != nil {
		if !errors.Is(err, prometheus.ErrNoRows) {
			rLogger.Warn("error", slog.String("error", err.Error()))
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

	}
	response := make([]Response, 0)
	for _, item := range values {
		for _, i := range item.Values {
			times := i.Timestamp.Time()
			response = append(response, Response{
				EntityID: entity,
				State:    i.Value.String(),
				Attributes: Attributes{
					StateClass:        "measurement",
					UnitOfMeasurement: "W",
					DeviceClass:       "power",
					FriendlyName:      query,
				},
				LastChanged: times.Format("2006-01-02T15:04:05+00:00"),
				LastUpdated: times.Format("2006-01-02T15:04:05+00:00"),
			})
		}
	}
	out, err := json.Marshal(response)
	if err != nil {
		rLogger.Info("error", slog.String("error", err.Error()))
		return
	}
	out = []byte("[" + string(out) + "]")
	w.Header().Set("Content-Type", "application/json")
	w.Write(out)
	return
}

// getRange is a wrapper to handle ErrNoRows scenario and push the start back by 1 day
func getRange(ctx context.Context, r prometheus.Reporter, query string, start time.Time, end time.Time, step int64) (model.Matrix, error) {
	values, err := r.GetRange(ctx, query, start, end, time.Second*time.Duration(step))
	if err != nil {
		if errors.Is(err, prometheus.ErrNoRows) {
			// its empty due to no results, lets set the start to the previous day values as a backup
			return r.GetRange(ctx, query, start.Add(-24*time.Hour), end, time.Second*time.Duration(step))
		}
		return nil, err
	}
	return values, nil
}
