package server

import (
	"context"
	"encoding/json"
	"github.com/beaujr/emprometheus/internal/prometheus"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type server struct {
	logger *slog.Logger
	r      prometheus.Reporter
	tariff func()
}

func NewServer(ctx context.Context, logger *slog.Logger, tariffs func(), r prometheus.Reporter) error {
	s := &server{logger: logger, r: r, tariff: tariffs}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/history/period/{range}", s.Handle)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		s.logger.Info(r.URL.Path)
	})
	mux.HandleFunc("/api/config", func(w http.ResponseWriter, r *http.Request) {
		s.tariff()
		resp := `{
    "allowlist_external_dirs": [
        "/media",
        "/config/www"
    ],
    "allowlist_external_urls": [],
    "components": [
        "input_boolean",
        "config",
        "usage_prediction",
        "file",
        "timer",
        "sensor",
        "homeassistant_alerts",
        "backup",
        "webhook",
        "zeroconf",
        "shopping_list",
        "met",
        "system_health",
        "my",
        "ssdp",
        "persistent_notification",
        "device_automation",
        "onboarding",
        "auth",
        "file_upload",
        "trace",
        "mobile_app.notify",
        "system_log",
        "automation",
        "mqtt",
        "conversation",
        "counter",
        "default_config",
        "notify",
        "bluetooth",
        "input_select",
        "cloud",
        "sun.binary_sensor",
        "person",
        "todo",
        "repairs",
        "camera",
        "backup.sensor",
        "google_translate.tts",
        "lovelace",
        "diagnostics",
        "scene",
        "input_number",
        "shell_command",
        "google_translate",
        "binary_sensor",
        "mobile_app",
        "dhcp",
        "mqtt.device_tracker",
        "device_tracker",
        "media_player",
        "backup.event",
        "sun",
        "energy.sensor",
        "bluetooth_adapters",
        "input_datetime",
        "script",
        "recorder",
        "tts",
        "http",
        "go2rtc",
        "switch",
        "stream",
        "schedule",
        "wake_word",
        "media_source",
        "logger",
        "mqtt.number",
        "select",
        "cloud.tts",
        "zone",
        "input_text",
        "input_button",
        "blueprint",
        "search",
        "image_upload",
        "energy",
        "history",
        "met.weather",
        "stt",
        "number",
        "tag",
        "analytics",
        "api",
        "network",
        "homeassistant.scene",
        "usb",
        "ffmpeg",
        "logbook",
        "mqtt.sensor",
        "assist_pipeline",
        "cast.media_player",
        "mqtt.select",
        "mqtt.switch",
        "homeassistant",
        "intent",
        "frontend",
        "shopping_list.todo",
        "thread",
        "event",
        "application_credentials",
        "sun.sensor",
        "hardware",
        "weather",
        "cast",
        "websocket_api",
        "radio_browser"
    ],
    "config_dir": "/config",
    "config_source": "storage",
    "country": "GB",
    "currency": "GBP",
    "debug": false,
    "elevation": 0,
    "external_url": null,
    "internal_url": null,
    "language": "en-GB",
    "latitude": 46.02,
    "location_name": "Home",
    "longitude": -2.7279996871948247,
    "radius": 100,
    "recovery_mode": false,
    "safe_mode": false,
    "state": "RUNNING",
    "time_zone": "Europe/London",
    "unit_system": {
        "length": "km",
        "accumulated_precipitation": "mm",
        "area": "m²",
        "mass": "g",
        "pressure": "Pa",
        "temperature": "°C",
        "volume": "L",
        "wind_speed": "m/s"
    },
    "version": "2025.11.2",
    "whitelist_external_dirs": [
        "/media",
        "/config/www"
    ]
}`
		w.Write([]byte(resp))
	})
	s.logger.Info("server started")
	srv := http.Server{Addr: ":8123", Handler: mux}
	if err := srv.ListenAndServe(); err != nil {
		s.logger.Warn("failed starting http server", slog.String("error", err.Error()))
		if err = srv.Shutdown(ctx); err != nil {
			return err
		}
		if err = srv.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) Handle(w http.ResponseWriter, r *http.Request) {
	s.logger.Info(r.URL.Path)
	period := r.PathValue("range")
	s.logger.Info(period + r.URL.RawQuery)
	entity := r.URL.Query().Get("filter_entity_id")
	pieces := strings.Split(entity, ".")
	if len(pieces) != 2 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	query := pieces[1]
	start, _ := time.Parse(time.DateTime, strings.ReplaceAll(strings.ReplaceAll(period, "T", " "), "Z", ""))
	step := (time.Now().Unix() - start.Unix()) / 11000
	if step < 1800 {
		step = 1800
	}
	values, err := s.r.GetRange(r.Context(), query, start, time.Now(), time.Second*time.Duration(step))
	if err != nil {
		slog.Info("error", slog.String("error", err.Error()))
		return
	}
	response := make([]Response, 0)
	for _, item := range values {
		for _, values := range item.Values {
			response = append(response, Response{
				EntityID: entity,
				State:    values.Value.String(),
				Attributes: Attributes{
					StateClass:        "measurement",
					UnitOfMeasurement: "W",
					DeviceClass:       "power",
					FriendlyName:      query,
				},
				LastChanged: values.Timestamp.Time().Format("2006-01-02T15:04:05+00:00"),
				LastUpdated: values.Timestamp.Time().Format("2006-01-02T15:04:05+00:00"),
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
