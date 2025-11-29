package temporal

import (
	"cmp"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/beaujr/emprometheus/internal/prometheus"
	"github.com/beaujr/emprometheus/internal/provider"
	"github.com/beaujr/emprometheus/internal/temporal/emhass"
	"github.com/beaujr/emprometheus/internal/temporal/inverter"
	"github.com/google/uuid"
	"go.temporal.io/sdk/client"
	temporal2 "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"golang.org/x/sync/errgroup"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

type Temporal struct {
	logger  *slog.Logger
	s       client.ScheduleClient
	i       *inverter.Inverter
	c       client.Client
	f       *emhass.Forecaster
	tariffs provider.RateFetcher
	prom    prometheus.Reporter
	cron    string
}

func (fs *Temporal) Start(ctx context.Context) error {
	errGrp, ctx := errgroup.WithContext(ctx)
	w := worker.New(fs.c, inverter.TaskQueue, worker.Options{})
	w.RegisterWorkflow(fs.i.Workflow)
	w.RegisterActivity(fs.i.Activity)
	interruptCh := worker.InterruptCh()
	errGrp.Go(func() error {
		if err := w.Run(interruptCh); err != nil {
			return err
		}
		return nil
	})
	e := worker.New(fs.c, emhass.TaskQueue, worker.Options{})
	e.RegisterWorkflow(fs.f.Workflow)
	e.RegisterActivity(fs.f.BuildScheduleActivity)
	e.RegisterActivity(fs.f.ForecastActivity)
	errGrp.Go(func() error {
		if err := e.Run(interruptCh); err != nil {
			return err
		}
		return nil
	})
	errGrp.Go(func() error {
		if err := fs.InitForecastSchedule(ctx); err != nil {
			return err
		}
		return nil
	})
	return errGrp.Wait()

}

func New(ctx context.Context, logger *slog.Logger, c client.Client, tariffs provider.RateFetcher, p prometheus.Reporter, cron string) (*Temporal, error) {
	s := c.ScheduleClient()
	f := emhass.New(logger.With(slog.String("pkg", "emhass")), s, tariffs, p)
	i := inverter.New(logger, s)
	return &Temporal{
		logger:  logger,
		c:       c,
		s:       s,
		f:       f,
		i:       i,
		cron:    cron,
		tariffs: tariffs,
	}, nil
}

func (fs *Temporal) InitForecastSchedule(ctx context.Context) error {
	fs.logger.Info("forecast")
	scheduleID := emhass.WorkflowId
	action := &client.ScheduleWorkflowAction{
		ID:        scheduleID,
		Workflow:  fs.f.Workflow,
		TaskQueue: emhass.TaskQueue,
		Args:      []interface{}{"http://localhost:5000", "http://localhost:8123"},
		RetryPolicy: &temporal2.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	_, err := fs.s.Create(ctx, client.ScheduleOptions{
		ID: scheduleID,
		Spec: client.ScheduleSpec{
			CronExpressions: []string{fs.cron},
		},
		Action: action,
	})
	if err != nil {
		if errors.Is(err, temporal2.ErrScheduleAlreadyRunning) {
			fs.logger.Warn("updating workflow schedule", slog.String("scheduleID", scheduleID))
			existing := fs.s.GetHandle(ctx, scheduleID)
			err = existing.Update(ctx, client.ScheduleUpdateOptions{DoUpdate: func(input client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
				input.Description.Schedule.Spec = &client.ScheduleSpec{
					CronExpressions: []string{fs.cron},
				}
				input.Description.Schedule.Action = action
				return &client.ScheduleUpdate{
					Schedule: &input.Description.Schedule,
				}, nil
			}})
			if err != nil {
				return err
			}
		}
		return err
	}
	return nil
}

func (fs *Temporal) process(ctx context.Context, t time.Time, workmodepriority, batteryfirstgridcharge string, soc float64) error {
	fs.logger.Info(t.Format(time.RFC3339), slog.String("work mode", workmodepriority), slog.String("battery first gridcharge", batteryfirstgridcharge))
	scheduleID := fmt.Sprintf("%s-%d", inverter.WorkflowId, t.Unix())
	action := &client.ScheduleWorkflowAction{
		ID:        fmt.Sprintf("%s-%s", inverter.WorkflowId, uuid.New()),
		Workflow:  fs.i.Workflow,
		TaskQueue: inverter.TaskQueue,
		Args:      []interface{}{scheduleID, workmodepriority, batteryfirstgridcharge, soc},
		Memo:      map[string]interface{}{"Work Mode Priority": workmodepriority, "Battery First Grid Charge": batteryfirstgridcharge, "SOC": soc},
	}
	_, err := fs.s.Create(ctx, client.ScheduleOptions{
		ID: scheduleID,
		Spec: client.ScheduleSpec{
			Calendars: []client.ScheduleCalendarSpec{
				{
					Year: []client.ScheduleRange{{
						Start: t.Year(),
						End:   t.Year(),
					}},
					Month: []client.ScheduleRange{{
						Start: int(t.Month()),
						End:   int(t.Month()),
					}},
					DayOfMonth: []client.ScheduleRange{{
						Start: t.Day(),
						End:   t.Day(),
					}},
					Hour: []client.ScheduleRange{{
						Start: t.Hour(),
						End:   t.Hour(),
					}},
					Minute: []client.ScheduleRange{{
						Start: t.Minute(),
						End:   t.Minute(),
					}},
					Second: []client.ScheduleRange{{
						Start: t.Second(),
						End:   t.Second(),
					}},
				},
			},
			// Required for one-shot schedule so it doesn't fire again
			EndAt: t.Add(1 * time.Second),
		},
		Action: action,
		Memo:   map[string]interface{}{"Work Mode Priority": workmodepriority, "Battery First Grid Charge": batteryfirstgridcharge, "SOC": soc},
	})
	if err != nil {
		if errors.Is(err, temporal2.ErrScheduleAlreadyRunning) {
			handler := fs.s.GetHandle(ctx, scheduleID)
			err = handler.Update(ctx, client.ScheduleUpdateOptions{DoUpdate: func(input client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
				input.Description.Schedule.Action = action
				return &client.ScheduleUpdate{
					Schedule: &input.Description.Schedule,
				}, nil
			}})
			if err != nil {
				return err
			}
			return nil
		}
		return err
	}
	return nil
}

func (fs *Temporal) Run(ctx context.Context, dir string) error {
	schedules, err := fs.s.List(ctx, client.ScheduleListOptions{})
	if err != nil {
		return err
	}
	for {
		if !schedules.HasNext() {
			break
		}
		sle, err := schedules.Next()
		if err != nil {
			return err
		}
		if strings.HasPrefix(sle.ID, inverter.WorkflowId) {
			if err = fs.s.GetHandle(ctx, sle.ID).Delete(ctx); err != nil {
				return err
			}
		}
	}
	if err = fs.output(ctx, dir); err != nil {
		return err
	}
	return err
}

type Schedule struct {
	workmode, chargeBatteryFromGrid string
	soc                             float64
	time                            time.Time
}

func (fs *Temporal) output(ctx context.Context, dir string) error {
	file, err := os.Open(filepath.Join(dir, "opt_res_latest.csv"))
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
		fs.logger.Info("No rows parsed.")
		return nil
	}
	minPrice := slices.MinFunc(rows, func(a, b Row) int {
		return cmp.Compare(a.Price, b.Price)
	})
	var schedules []Schedule
	now := time.Now()
	for idx, r := range rows {
		d := r.Timestamp.Sub(now)
		// is if the next hour block from now?
		if d < time.Hour && d > 0 {
			workmodepriority := workModePriorityLoadFirst
			batteryfirstgridcharge := batteryFirstGridChargeDisabled
			// is it expected to be charged in the next hour?
			switch {
			case r.PPV > r.Load:
				// solar will charge it, grid will be used
				workmodepriority = workModePriorityLoadFirst
				batteryfirstgridcharge = batteryFirstGridChargeDisabled
			case r.SOC < rows[idx+1].SOC && minPrice.Price == r.Price, r.PBatt < 0:
				// it needs charged
				workmodepriority = workModePriorityBatteryFirst
				batteryfirstgridcharge = batteryFirstGridChargeEnabled
			}
			fs.logger.Info(r.Timestamp.Format(time.RFC3339), slog.String("work mode", workmodepriority), slog.String("battery first gridcharge", batteryfirstgridcharge))
			schedules = append(schedules, Schedule{
				workmode:              workmodepriority,
				chargeBatteryFromGrid: batteryfirstgridcharge,
				soc:                   r.SOC,
				time:                  r.Timestamp,
			})
			//if err = s.Process(ctx, r.Timestamp, workmodepriority, batteryfirstgridcharge, r.SOC); err != nil {
			//	s.logger.Warn("error", slog.String("error", err.Error()))
			//}
			continue
		}
		// there should always be a schedule in the array at this point, safe check to be sure
		if len(schedules) == 0 {
			return errors.New("schedule is missing next hours action")
		}
		var p *Row
		if idx > 0 {
			p = &rows[idx-1]
		}
		workmodepriority, batteryfirstgridcharge, err := fs.getAction(p, r, minPrice.Price)
		if err != nil {
			return err
		}
		if workmodepriority == "" {
			continue
		}
		prevSchedule := schedules[len(schedules)-1]
		if prevSchedule.workmode == workmodepriority && prevSchedule.chargeBatteryFromGrid == batteryfirstgridcharge {
			if prevSchedule.soc < r.SOC {
				schedules[len(schedules)-1].soc = r.SOC
			}
			continue
		}
		schedules = append(schedules, Schedule{
			workmode:              workmodepriority,
			chargeBatteryFromGrid: batteryfirstgridcharge,
			soc:                   r.SOC,
			time:                  r.Timestamp,
		})
	}
	for _, schedule := range schedules {
		fs.logger.Info(schedule.time.Format(time.RFC3339), slog.String("work mode", schedule.workmode), slog.String("battery first gridcharge", schedule.chargeBatteryFromGrid))
		if err = fs.process(ctx, schedule.time, schedule.workmode, schedule.chargeBatteryFromGrid, schedule.soc); err != nil {
			fs.logger.Warn("error", slog.String("error", err.Error()))
		}
	}
	return nil
}

const (
	workModePriorityLoadFirst      = "Load first"
	workModePriorityBatteryFirst   = "Battery first"
	batteryFirstGridChargeEnabled  = "Enabled"
	batteryFirstGridChargeDisabled = "Disabled"
)

func (fs *Temporal) getAction(p *Row, r Row, minPrice float64) (string, string, error) {
	if r.PBatt < 0 {
		if r.PPV > r.Load {
			// Load First: Work mode priority
			// Battery first grid charge: Disabled
			// Load first stop discharge: 10% (dont set min battery when PV will pay for load)
			action := fmt.Sprintf("%s PV Charge Battery: %f to %f", r.Timestamp.Local().Format(time.RFC3339), r.PBatt, r.SOC*100)
			fs.logger.Info(action)
			return workModePriorityLoadFirst, batteryFirstGridChargeDisabled, nil
		}

		// only charge at cheapest
		// todo: tariff might go: low, mid, high, mid, high and this doesnt account for that tariff structure
		if r.Price != minPrice {
			return workModePriorityLoadFirst, batteryFirstGridChargeDisabled, nil
		}
		// Work mode priority: Battery First
		// Battery first grid charge: Enabled
		// Load first stop discharge: 100%
		action := fmt.Sprintf("%s Grid Charge Battery: %f to %f", r.Timestamp.Local().Format(time.RFC3339), r.PBatt, r.SOC*100)
		fs.logger.Info(action)
		return workModePriorityBatteryFirst, batteryFirstGridChargeEnabled, nil
	}

	// its discharging as previous row was gt soc
	// if its expected to have higher SOC or same SOC for an hour
	if p != nil && (p.SOC > r.SOC) {
		action := fmt.Sprintf("%s Use Battery: %f to %f", r.Timestamp.Local().Format(time.RFC3339), r.PBatt, r.SOC*100)
		fs.logger.Info(action)
		return workModePriorityLoadFirst, batteryFirstGridChargeDisabled, nil
	}
	return "", "", nil
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
