package temporal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	emhass2 "github.com/beaujr/emprometheus/internal/emhass"
	"github.com/beaujr/emprometheus/internal/provider"
	"github.com/beaujr/emprometheus/internal/scheduler"
	"github.com/beaujr/emprometheus/internal/store"
	"github.com/beaujr/emprometheus/internal/temporal/emhass"
	"github.com/beaujr/emprometheus/internal/temporal/inverter"
	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	temporal2 "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"golang.org/x/sync/errgroup"
)

type Temporal struct {
	logger      *slog.Logger
	s           client.ScheduleClient
	i           *inverter.Inverter
	c           client.Client
	f           *emhass.Forecaster
	tariffs     provider.RateFetcher
	db          store.Store
	loc         *time.Location
	cron        string
	mpc         bool
	initOnStart bool
}

type workers struct {
	queue      string
	workflow   interface{}
	activities []interface{}
}

func (fs *Temporal) Start(ctx context.Context) error {
	ws := []workers{
		{
			queue:      inverter.TaskQueue,
			workflow:   fs.i.Workflow,
			activities: []interface{}{fs.i.Activity},
		}, {
			queue:      emhass.TaskQueue,
			workflow:   fs.f.ForecastWorkflow,
			activities: []interface{}{fs.f.ForecastActivity, fs.f.BuildScheduleActivity},
		},
	}
	if fs.mpc {
		ws = append(ws, workers{
			queue:      emhass.TaskQueueMPC,
			workflow:   fs.f.MPCWorkflow,
			activities: []interface{}{fs.f.BuildScheduleActivity, fs.f.MPCActivity, fs.f.GetHorizonSOCActivity},
		})
	}
	errGrp, ctx := errgroup.WithContext(ctx)
	interruptCh := worker.InterruptCh()
	for _, w := range ws {
		e := worker.New(fs.c, w.queue, worker.Options{WorkerStopTimeout: 5 * time.Second})
		e.RegisterWorkflow(w.workflow)
		for _, a := range w.activities {
			e.RegisterActivity(a)
		}
		errGrp.Go(func() error {
			if err := e.Run(interruptCh); err != nil {
				return err
			}
			return nil
		})

	}

	errGrp.Go(func() error {
		return fs.i.Start(ctx)
	})
	if fs.initOnStart {
		errGrp.Go(func() error {
			fs.logger.Info("initializing forecast workflow")
			if err := fs.InitForecastSchedule(ctx); err != nil {
				return err
			}
			return nil
		})
	}

	return errGrp.Wait()

}

type Option func(t *Temporal)

func WithInitOnStart() Option {
	return func(t *Temporal) {
		t.initOnStart = true
	}
}

func New(_ context.Context, logger *slog.Logger, em *emhass2.Emhass, c client.Client, tariffs provider.RateFetcher, sa scheduler.ControllablePowerPlant, cron string, mpc bool, db store.Store, loc *time.Location, steps int, opts ...Option) (*Temporal, error) {
	s := c.ScheduleClient()
	i, err := inverter.New(s, db, sa)
	if err != nil {
		return nil, err
	}
	t := &Temporal{
		logger:  logger,
		c:       c,
		s:       s,
		f:       nil,
		i:       i,
		cron:    cron,
		tariffs: tariffs,
		mpc:     mpc,
		db:      db,
		loc:     loc,
	}
	f := emhass.New(tariffs, sa.GetCurrentSOC, t.Run, em, db, steps)
	t.f = f
	for _, opt := range opts {
		opt(t)
	}
	return t, nil
}

func (fs *Temporal) InitForecastSchedule(ctx context.Context) error {
	switch {
	case fs.mpc:
		if err := fs.setUpMPCWorkflows(ctx); err != nil {
			return err
		}
	default:
		existing, err := fs.s.List(ctx, client.ScheduleListOptions{})
		if err != nil {
			return err
		}
		for !existing.HasNext() {
			next, err := existing.Next()
			if err != nil {
				fs.logger.Warn(err.Error())
				break
			}
			if strings.HasPrefix(next.ID, emhass.WorkflowIdMPC) {
				if err = fs.s.GetHandle(ctx, next.ID).Delete(ctx); err != nil {
					fs.logger.Warn(err.Error())
					continue
				}
			}
			if strings.HasPrefix(next.ID, emhass.WorkflowId) {
				if err = fs.s.GetHandle(ctx, next.ID).Delete(ctx); err != nil {
					fs.logger.Warn(err.Error())
					continue
				}
			}
			if strings.HasPrefix(next.ID, inverter.WorkflowId) {
				if err = fs.s.GetHandle(ctx, next.ID).Delete(ctx); err != nil {
					fs.logger.Warn(err.Error())
					continue
				}
			}
		}
	}
	optimizations, err := fs.db.Select(ctx, time.Now())
	if err != nil {
		return err
	}
	for _, optimization := range optimizations {
		if err = fs.db.DeleteAnyOptimization(ctx, optimization.Time); err != nil {
			return err
		}
		if err = fs.db.Delete(ctx, optimization); err != nil {
			return err
		}
	}
	if err := fs.setUpForecastWorkflows(ctx); err != nil {
		return err
	}
	return nil
}

func (fs *Temporal) setUpMPCWorkflows(ctx context.Context) error {
	fs.logger.Info("mpc")
	scheduleID := emhass.WorkflowIdMPC
	action := &client.ScheduleWorkflowAction{
		ID:        scheduleID,
		Workflow:  fs.f.MPCWorkflow,
		TaskQueue: emhass.TaskQueueMPC,
		Args:      nil,
		RetryPolicy: &temporal2.RetryPolicy{
			MaximumAttempts: 1,
		},
	}
	cronSchedule := "58 3,12,21 * * *"
	_, err := fs.s.Create(ctx, client.ScheduleOptions{
		ID: scheduleID,
		Spec: client.ScheduleSpec{
			CronExpressions: []string{cronSchedule},
		},
		Action: action,
	})
	if err != nil {
		if errors.Is(err, temporal2.ErrScheduleAlreadyRunning) {
			fs.logger.Warn("updating workflow schedule", slog.String("scheduleID", scheduleID))
			existing := fs.s.GetHandle(ctx, scheduleID)
			err = existing.Update(ctx, client.ScheduleUpdateOptions{DoUpdate: func(input client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
				input.Description.Schedule.Spec = &client.ScheduleSpec{
					CronExpressions: []string{cronSchedule},
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

func (fs *Temporal) setUpForecastWorkflows(ctx context.Context) error {
	fs.logger.Info("forecast")
	scheduleID := emhass.WorkflowId
	action := &client.ScheduleWorkflowAction{
		ID:        scheduleID,
		Workflow:  fs.f.ForecastWorkflow,
		TaskQueue: emhass.TaskQueue,
		Args:      nil,
		RetryPolicy: &temporal2.RetryPolicy{
			MaximumAttempts: 10,
			InitialInterval: time.Second * 30,
		},
	}
	sch, err := fs.s.Create(ctx, client.ScheduleOptions{
		ID: scheduleID,
		Spec: client.ScheduleSpec{
			CronExpressions: []string{fs.cron},
		},
		Action: action,
	})
	if err != nil {
		if !errors.Is(err, temporal2.ErrScheduleAlreadyRunning) {
			return err
		}
		fs.logger.Warn("updating workflow schedule", slog.String("scheduleID", scheduleID))
		sch = fs.s.GetHandle(ctx, scheduleID)
		err = sch.Update(ctx, client.ScheduleUpdateOptions{DoUpdate: func(input client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
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
	return sch.Trigger(ctx, client.ScheduleTriggerOptions{})
}

func (fs *Temporal) scheduleForecastWorkflow(ctx context.Context, t time.Time) error {
	fs.logger.Info("forecast")
	action := &client.ScheduleWorkflowAction{
		ID:        fmt.Sprintf("%s-once-off", emhass.WorkflowId),
		Workflow:  fs.f.ForecastWorkflow,
		TaskQueue: emhass.TaskQueue,
		Args:      nil,
		RetryPolicy: &temporal2.RetryPolicy{
			MaximumAttempts: 10,
			InitialInterval: time.Second * 30,
		},
	}
	opts := client.ScheduleOptions{
		ID: action.ID,
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
			EndAt: t.Add(1 * time.Second),
		},
		Action: action,
	}
	_, err := fs.s.Create(ctx, opts)
	if err != nil {
		if !errors.Is(err, temporal2.ErrScheduleAlreadyRunning) {
			return err
		}
		fs.logger.Warn("updating workflow schedule", slog.String("scheduleID", action.ID))
		sch := fs.s.GetHandle(ctx, action.ID)
		err = sch.Update(ctx, client.ScheduleUpdateOptions{DoUpdate: func(input client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			input.Description.Schedule.Spec = &opts.Spec
			input.Description.Schedule.Action = opts.Action
			return &client.ScheduleUpdate{
				Schedule: &input.Description.Schedule,
			}, nil
		}})
		if err != nil {
			return err
		}
	}
	return nil
}

func (fs *Temporal) process(ctx context.Context, t time.Time, workmodepriority, batteryfirstgridcharge string, soc float64) error {
	fs.logger.Info(t.Format(time.RFC3339), slog.String("work mode", workmodepriority), slog.String("battery first gridcharge", batteryfirstgridcharge))

	scheduleID := fmt.Sprintf("%s-%d", inverter.WorkflowId, t.Unix())
	if err := fs.s.GetHandle(ctx, scheduleID).Delete(ctx); err != nil {
		var notFound *serviceerror.NotFound
		if !errors.As(err, &notFound) {
			fs.logger.Warn("error deleting schedule", slog.String("scheduleID", scheduleID), slog.String("error", err.Error()))
		}
	}
	action := &client.ScheduleWorkflowAction{
		ID:        fmt.Sprintf("%s-%s", inverter.WorkflowId, uuid.New()),
		Workflow:  fs.i.Workflow,
		TaskQueue: inverter.TaskQueue,
		Args:      []interface{}{scheduleID, workmodepriority, batteryfirstgridcharge, soc, t.In(fs.loc).Format(time.RFC3339)},
		Memo:      map[string]interface{}{"Work Mode Priority": workmodepriority, "Battery First Grid Charge": batteryfirstgridcharge, "SOC": soc, "timestamp": t.Format(time.RFC3339)},
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

func (fs *Temporal) Run(ctx context.Context, method string) error {
	if err := fs.output(ctx, method); err != nil {
		return err
	}
	return nil
}

func (fs *Temporal) output(ctx context.Context, method string) error {
	schedules, err := scheduler.GenerateSchedules(ctx, fs.logger, fs.db, time.Now(), method)
	if err != nil {
		return err
	}
	var lastScheduledTime time.Time
	for _, schedule := range schedules {
		if err = fs.process(ctx, schedule.Time(), schedule.Workmode(), schedule.ChargeBatteryFromGrid(), schedule.Soc()); err != nil {
			fs.logger.Warn("error", slog.String("error", err.Error()))
		}
	}
	if lastScheduledTime.IsZero() {
		fs.logger.Warn("No rows parsed.")
		return nil
	}
	return fs.scheduleForecastWorkflow(ctx, lastScheduledTime)
}
