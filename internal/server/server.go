package server

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/beaujr/emprometheus/internal/prometheus"
	"github.com/beaujr/emprometheus/internal/provider"
	"github.com/beaujr/emprometheus/internal/scheduler"
	"github.com/beaujr/emprometheus/internal/temporal/inverter"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type Server struct {
	logger    *slog.Logger
	r         prometheus.Reporter
	tariff    provider.RateFetcher
	i         *inverter.Inverter
	dir       string
	scheduler scheduler.Scheduler
}

func NewServer(ctx context.Context, logger *slog.Logger, tariffs provider.RateFetcher, r prometheus.Reporter, dir string, scheduler scheduler.Scheduler) *http.Server {
	s := &Server{
		logger:    logger,
		r:         r,
		tariff:    tariffs,
		dir:       dir,
		scheduler: scheduler,
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
		if err := s.scheduler.Run(ctx, dir); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
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

func (s *Server) Handle(w http.ResponseWriter, r *http.Request) {
	s.logger.Info(r.URL.Path)
	period := r.PathValue("range")
	s.logger.Info(period + r.URL.RawQuery)
	entity := r.URL.Query().Get("filter_entity_id")
	pieces := strings.Split(entity, ".")
	if len(pieces) != 2 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	cleanStart := strings.ReplaceAll(strings.ReplaceAll(period, "T", " "), "Z", "")
	start, _ := time.Parse(time.DateTime, cleanStart[:19])
	step := (time.Now().Unix() - start.Unix()) / 11000
	if step < 3600 {
		step = 300
	}
	query := fmt.Sprintf("%s unless changes(%s[2m]) == 0", pieces[1], pieces[1])
	values, err := s.r.GetRange(r.Context(), query, start, time.Now(), time.Second*time.Duration(step))
	//values, err := s.r.GetRange(r.Context(), query, start, time.Now(), time.Hour)
	if err != nil {
		slog.Info("error", slog.String("error", err.Error()))
		return
	}
	response := make([]Response, 0)
	for _, item := range values {
		for _, values := range item.Values {
			times := values.Timestamp.Time()
			response = append(response, Response{
				EntityID: entity,
				State:    values.Value.String(),
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
		slog.Info("error", slog.String("error", err.Error()))
		return
	}
	//lazy add array brackets again
	out = []byte("[" + string(out) + "]")
	w.Header().Set("Content-Type", "application/json")
	w.Write(out)
}
