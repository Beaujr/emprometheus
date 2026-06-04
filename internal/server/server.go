package server

import (
	"bufio"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/beaujr/emprometheus/internal/emhass"
	"github.com/beaujr/emprometheus/internal/prometheus"
	"github.com/beaujr/emprometheus/internal/provider"
	"github.com/beaujr/emprometheus/internal/scheduler"
	"github.com/beaujr/emprometheus/internal/store"
	"github.com/prometheus/common/model"
)

//go:embed templates/*.html
var templateFS embed.FS

type Server struct {
	logger    *slog.Logger
	r         prometheus.Reporter
	tariff    provider.RateFetcher
	dir       string
	scheduler scheduler.Scheduler
	db        store.Store
	spp       scheduler.SimplePowerPlant
	password  string
	steps     int
}

func NewServer(ctx context.Context, logger *slog.Logger, tariffs provider.RateFetcher, r prometheus.Reporter, dir, password string, scheduler scheduler.Scheduler, loc *time.Location, db store.Store, plant scheduler.SimplePowerPlant, steps int) *http.Server {
	s := &Server{
		logger:    logger,
		r:         r,
		tariff:    tariffs,
		dir:       dir,
		scheduler: scheduler,
		db:        db,
		spp:       plant,
		password:  password,
		steps:     steps,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Write([]byte("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"))
	})
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
		if err := s.scheduler.Run(ctx, method); err != nil {
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
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		fm := r.PathValue("forecast")
		logger := s.logger.With(slog.String("file", filepath.Join(dir, provider.CSVForecastName)))
		if err = s.copyFile(logger, dir, provider.CSVForecastName, fm); err != nil {
			logger.Error("error reading file", slog.String("error", err.Error()))
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, err = io.Copy(w, resp.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

	})

	var tmpl = template.Must(template.ParseFS(templateFS, "templates/admin.html"))
	mux.HandleFunc("/admin", func(w http.ResponseWriter, r *http.Request) {
		batteryfirstgridcharge, workmodepriority, soc, err := s.spp.Status(r.Context())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		err = tmpl.Execute(w, struct {
			WorkModePriority       string
			SOC                    int64
			BatteryFirstGridCharge string
		}{
			WorkModePriority:       workmodepriority,
			SOC:                    soc,
			BatteryFirstGridCharge: batteryfirstgridcharge,
		})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
	})

	mux.HandleFunc("/admin/process", func(w http.ResponseWriter, r *http.Request) {
		jsonResponse := func(success bool, message string) {
			if err := json.NewEncoder(w).Encode(struct {
				Success bool   `json:"success"`
				Message string `json:"message"`
			}{success, message}); err != nil {
				w.Write([]byte(err.Error()))
			}
		}
		if err := r.ParseMultipartForm(10 << 20); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			jsonResponse(false, err.Error())
			return
		}

		battery := r.FormValue("batteryfirstgridcharge")
		workmode := r.FormValue("workmodepriority")
		socStr := r.FormValue("soc")
		password = r.FormValue("password")
		if password != s.password {
			jsonResponse(false, "password mismatch")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		soc, err := strconv.ParseInt(socStr, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			jsonResponse(false, err.Error())
			return
		}
		if err := s.spp.Process(r.Context(), battery, workmode, soc); err != nil {
			jsonResponse(false, err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		jsonResponse(true, "success")
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
			if _, err = w.Write([]byte(row.StringWithTimezone(loc) + "\n")); err != nil {
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
		if err := s.tariff(steps); err != nil {
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

func (s *Server) copyFile(logger *slog.Logger, dir, src, forecastMethod string) error {
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
	results, err := emhass.ReadOptimizationResults(logger, reader, forecastMethod)
	if err != nil {
		return err
	}
	logger.Info("reading finished", slog.Int("rows", len(results)))
	for _, o := range results {
		err = s.db.InsertOptimization(o)
		if err != nil {
			logger.Error("failed inserting optimization", slog.String("error", err.Error()))
			return err
		}
	}
	return nil
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
