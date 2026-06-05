package hass

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/beaujr/emprometheus/internal/prometheus"
	"github.com/beaujr/emprometheus/internal/provider"
)

type Hass struct {
	p       prometheus.Reporter
	fetcher provider.RateFetcher
	steps   int
	logger  *slog.Logger
}

func New(logger *slog.Logger, fetcher provider.RateFetcher, p prometheus.Reporter, steps int) *Hass {
	h := &Hass{p: p, fetcher: fetcher, steps: steps, logger: logger.With(slog.String("component", "hass"))}
	return h
}

func (h *Hass) config(w http.ResponseWriter, r *http.Request) {
	if err := h.fetcher(h.steps); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		h.logger.Error(err.Error())
		return
	}
	resp := `{}`
	w.Write([]byte(resp))
}

func (h *Hass) getData(w http.ResponseWriter, r *http.Request) {
	period := r.PathValue("range")
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
	values, err := prometheus.GetRange(r.Context(), h.p, query, start, time.Now(), step)
	if err != nil {
		if !errors.Is(err, prometheus.ErrNoRows) {
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
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	out = []byte("[" + string(out) + "]")
	w.Header().Set("Content-Type", "application/json")
	w.Write(out)
	return
}

func (h *Hass) Register(mux *http.ServeMux) {
	mux.HandleFunc("/api/history/period/{range}", h.getData)
	mux.HandleFunc("/api/config", h.config)
}
