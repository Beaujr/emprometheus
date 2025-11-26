package server

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/beaujr/emprometheus/internal/prometheus"
	"github.com/beaujr/emprometheus/internal/provider"
	"github.com/beaujr/emprometheus/internal/temporal"
	"github.com/beaujr/emprometheus/internal/temporal/emhass"
	"github.com/google/uuid"
	"go.temporal.io/sdk/client"
	temporal2 "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"golang.org/x/sync/errgroup"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type server struct {
	logger *slog.Logger
	r      prometheus.Reporter
	tariff provider.RateFetcher
	c      client.Client
	//w      worker.Worker
	s   client.ScheduleClient
	dir string
}

func NewServer(ctx context.Context, logger *slog.Logger, tariffs provider.RateFetcher, c client.Client, r prometheus.Reporter, dir string) error {
	s := &server{
		logger: logger,
		r:      r,
		tariff: tariffs,
		c:      c,
		dir:    dir,
	}
	var errGrp errgroup.Group
	if s.c != nil {
		s.s = c.ScheduleClient()
		w := worker.New(c, temporal.TaskQueue, worker.Options{})
		w.RegisterWorkflow(temporal.Workflow)
		w.RegisterActivity(temporal.Activity)
		errGrp.Go(func() error {
			if err := w.Run(worker.InterruptCh()); err != nil {
				return err
			}
			return nil
		})
		e := worker.New(c, emhass.TaskQueue, worker.Options{})
		f := emhass.New(s.logger.With(slog.String("pkg", "emhass")), tariffs)
		e.RegisterWorkflow(f.Workflow)
		e.RegisterActivity(f.BuildScheduleActivity)
		e.RegisterActivity(f.ForecastActivity)
		errGrp.Go(func() error {
			if err := e.Run(worker.InterruptCh()); err != nil {
				return err
			}
			return nil
		})
		if err := s.initForecast(ctx, f); err != nil {
			return err
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/history/period/{range}", s.Handle)
	mux.HandleFunc("/process", func(w http.ResponseWriter, r *http.Request) {
		if s.c == nil {
			w.WriteHeader(http.StatusNotImplemented)
			return
		}
		schedules, err := s.s.List(ctx, client.ScheduleListOptions{})
		if err != nil {
			return
		}
		for {
			if !schedules.HasNext() {
				break
			}
			sle, err := schedules.Next()
			if err != nil {
				return
			}
			if strings.HasPrefix(sle.ID, temporal.WorkflowId) {
				if err = s.s.GetHandle(ctx, sle.ID).Delete(r.Context()); err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
			}
		}
		if err = s.output(r.Context()); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		return
	})
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
	errGrp.Go(func() error {
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
	})

	return errGrp.Wait()
}

func (s *server) initForecast(ctx context.Context, f *emhass.Forecaster) error {
	s.logger.Info("forecast")
	scheduleID := emhass.WorkflowId
	_, err := s.s.Create(ctx, client.ScheduleOptions{
		ID: scheduleID,
		Spec: client.ScheduleSpec{
			CronExpressions: []string{"1 * * * *"},
		},
		Action: &client.ScheduleWorkflowAction{
			ID:        scheduleID,
			Workflow:  f.Workflow,
			TaskQueue: emhass.TaskQueue,
			Args:      []interface{}{"http://promhass.temporal.svc.cluster.local:5000", "http://promhass.temporal.svc.cluster.local:8123"},
		},
	})
	if err != nil {
		if errors.Is(err, temporal2.ErrScheduleAlreadyRunning) {
			s.logger.Warn("deleting workflow schedule", slog.String("scheduleID", scheduleID), slog.String("error", err.Error()))
			if err = s.s.GetHandle(ctx, scheduleID).Delete(ctx); err != nil {
				return err
			}
			return s.initForecast(ctx, f)
		}
		return err
	}
	return nil
}

func (s *server) Process(ctx context.Context, t time.Time, action string) error {
	s.logger.Info("action", slog.String("schedule", action))
	scheduleID := fmt.Sprintf("%s-%d", temporal.WorkflowId, t.Unix())
	_, err := s.s.Create(ctx, client.ScheduleOptions{
		ID: scheduleID,
		Spec: client.ScheduleSpec{
			Calendars: []client.ScheduleCalendarSpec{
				{
					// Each field uses []ScheduleRange
					Year: []client.ScheduleRange{{
						Start: int(t.Year()),
						End:   int(t.Year()),
					}},
					Month: []client.ScheduleRange{{
						Start: int(t.Month()),
						End:   int(t.Month()),
					}},
					DayOfMonth: []client.ScheduleRange{{
						Start: int(t.Day()),
						End:   int(t.Day()),
					}},
					Hour: []client.ScheduleRange{{
						Start: int(t.Hour()),
						End:   int(t.Hour()),
					}},
					Minute: []client.ScheduleRange{{
						Start: int(t.Minute()),
						End:   int(t.Minute()),
					}},
					Second: []client.ScheduleRange{{
						Start: int(t.Second()),
						End:   int(t.Second()),
					}},
				},
			},
			// Required for one-shot schedule so it doesn't fire again
			EndAt: t.Add(1 * time.Second),
		},
		Action: &client.ScheduleWorkflowAction{
			ID:        fmt.Sprintf("%s-%s", temporal.WorkflowId, uuid.New()),
			Workflow:  temporal.Workflow, // reference to your workflow function
			TaskQueue: temporal.TaskQueue,
			Args:      []interface{}{action}, // workflow arguments, if any
		},
	})
	if err != nil {
		if errors.Is(err, temporal2.ErrScheduleAlreadyRunning) {
			if err = s.s.GetHandle(ctx, scheduleID).Delete(ctx); err != nil {
				return err
			}
			return s.Process(ctx, t, action)
		}
		return err
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

func (s *server) output(ctx context.Context) error {
	file, err := os.Open(filepath.Join(s.dir, "opt_res_latest.csv"))
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
		return nil
	}
	var wg sync.WaitGroup
	for idx, r := range rows {
		if r.PBatt < 0 {
			if r.PPV > r.Load {
				// Load First: Work mode priority
				// Battery first grid charge: Disabled
				// Load first stop discharge: 10% (dont set min battery when PV will pay for load)
				wg.Add(1)
				go func() {
					defer wg.Done()
					action := fmt.Sprintf("%s PV Charge Battery: %f to %f", r.Timestamp.Local().Format(time.RFC3339), r.PBatt, r.SOC*100)
					if err = s.Process(ctx, r.Timestamp, action); err != nil {
						s.logger.Warn("error", slog.String("error", err.Error()))
					}
				}()
				continue
			}
			// Work mode priority: Battery First
			// Battery first grid charge: Enabled
			// Load first stop discharge: 100%
			wg.Add(1)
			go func() {
				defer wg.Done()
				action := fmt.Sprintf("%s Grid Charge Battery: %f to %f", r.Timestamp.Local().Format(time.RFC3339), r.PBatt, r.SOC*100)
				if err = s.Process(ctx, r.Timestamp, action); err != nil {
					s.logger.Warn("error", slog.String("error", err.Error()))
				}
			}()
			continue
		}
		if idx > 0 && rows[idx-1].SOC > r.SOC {
			wg.Add(1)
			go func() {
				defer wg.Done()
				action := fmt.Sprintf("%s Use Battery: %f to %f", r.Timestamp.Local().Format(time.RFC3339), r.PBatt, r.SOC*100)
				if err = s.Process(ctx, r.Timestamp, action); err != nil {
					s.logger.Warn("error", slog.String("error", err.Error()))
				}
			}()
		}
	}
	wg.Wait()
	return nil
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
