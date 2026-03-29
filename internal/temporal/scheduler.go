package temporal

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

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

func New(_ context.Context, logger *slog.Logger, c client.Client, tariffs provider.RateFetcher, sa scheduler.ControllablePowerPlant, cron string, mpc bool, db store.Store, loc *time.Location, opts ...Option) (*Temporal, error) {
	s := c.ScheduleClient()
	i, err := inverter.New(s, db, sa)
	if err != nil {
		return nil, err
	}
	f := emhass.New(s, tariffs, sa.GetCurrentSOC, http.Client{Timeout: 60 * time.Second}, db)
	t := &Temporal{
		logger:  logger,
		c:       c,
		s:       s,
		f:       f,
		i:       i,
		cron:    cron,
		tariffs: tariffs,
		mpc:     mpc,
		db:      db,
		loc:     loc,
	}
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
		for {
			if !existing.HasNext() {
				break
			}
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
		Args:      []interface{}{"http://localhost:8123", "http://localhost:8123"},
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
		Args:      []interface{}{"http://localhost:8123", "http://localhost:8123"},
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

type Schedule struct {
	workmode, chargeBatteryFromGrid string
	soc, targetSOC                  float64
	time                            time.Time
}

func (fs *Temporal) output(ctx context.Context, method string) error {
	rows, err := fs.db.SelectOptimization(time.Now(), method)
	if err != nil {
		return err
	}
	//file, err := os.Open(filepath.Join(dir, fmt.Sprintf("%s.csv", method)))
	//if err != nil {
	//	return err
	//}
	//defer file.Close()
	//reader := csv.NewReader(file)
	//allRows, err := reader.ReadAll()
	//if err != nil {
	//	return err
	//}
	//var rows []Row
	//for i := 1; i < len(allRows); i++ {
	//	r := allRows[i]
	//
	//	t, err := time.Parse("2006-01-02 15:04:05-07:00", r[0])
	//	if err != nil {
	//		fmt.Println("Timestamp parse error:", r[0], err)
	//		continue
	//	}
	//	rows = append(rows, Row{
	//		Timestamp: t,
	//		PPV:       parseFloat(r[1]),
	//		Load:      parseFloat(r[2]),
	//		PBatt:     parseFloat(r[6]),
	//		SOC:       parseFloat(r[7]),
	//		Price:     parseFloat(r[8]),
	//	})
	//}

	if len(rows) == 0 {
		fs.logger.Info("No rows parsed.")
		return nil
	}
	var schedules []Schedule
	schedules = GetCommands(rows)

	for _, schedule := range schedules {
		fs.logger.Info(schedule.time.Format(time.RFC3339), slog.String("work mode", schedule.workmode), slog.String("battery first gridcharge", schedule.chargeBatteryFromGrid))
		if err = fs.db.Upsert(store.Row{
			Optimization:     method,
			Time:             schedule.time,
			WorkMode:         schedule.workmode,
			GridCharge:       schedule.chargeBatteryFromGrid,
			StopDischargeSOC: schedule.soc,
			TargetSOC:        schedule.targetSOC,
		}); err != nil {
			return err
		}
		if err = fs.process(ctx, schedule.time, schedule.workmode, schedule.chargeBatteryFromGrid, schedule.soc); err != nil {
			fs.logger.Warn("error", slog.String("error", err.Error()))
		}
	}
	return nil
}

func GetCommands(rows []store.OptimizationResult) []Schedule {
	if len(rows) == 0 {
		return nil
	}
	minGridRow := slices.MinFunc(rows, func(a, b store.OptimizationResult) int {
		return cmp.Compare(a.UnitLoadCost, b.UnitLoadCost)
	})
	minPrice := minGridRow.UnitLoadCost

	type block struct {
		start int
		end   int
	}

	var blocks []block
	inBlock := false
	startIdx := 0

	for i := range rows {
		if rows[i].UnitLoadCost == minPrice {
			if !inBlock {
				inBlock = true
				startIdx = i
			}
		} else {
			if inBlock {
				blocks = append(blocks, block{start: startIdx, end: i - 1})
				inBlock = false
			}
		}
	}
	if inBlock {
		blocks = append(blocks, block{start: startIdx, end: len(rows) - 1})
	}

	monotonicSOC := make([]float64, len(rows))

	for _, b := range blocks {
		// First hour SoC target (converted to %)
		lastSOC := rows[b.start].SOCOpt * 100

		for i := b.start; i <= b.end; i++ {
			target := rows[i].SOCOpt * 100

			// Only allow increases inside the block
			if target > lastSOC {
				lastSOC = target
			}

			monotonicSOC[i] = lastSOC
		}
	}

	var commands []Schedule
	for i, row := range rows {
		workMode := scheduler.WorkModeLoadFirst
		gridCharge := scheduler.BatteryFirstGridChargeDisabled
		soc := 10.0 // default load-first SoC
		targetSOCFromOptimisation := row.SOCOpt
		if row.UnitLoadCost == minPrice {
			// Inside cheap block
			workMode = scheduler.WorkModeBatteryFirst
			gridCharge = scheduler.BatteryFirstGridChargeEnabled
			tSoc := monotonicSOC[i]
			// if its not the last index
			if i != len(rows)-1 {
				// if the next row is higher than this row
				// set the target soc of this hour to reach that target soc
				// rows are the bottom of the hour eg 16:00:00 70% soc, so 15:00:00 should set target soc to 70%
				nextRowSocOpt := rows[i+1].SOCOpt * 100
				//nextRowUnitLoadCost := rows[i+1].UnitLoadCost
				if nextRowSocOpt > tSoc {
					tSoc = nextRowSocOpt
				}
			}
			// if its equal to 10, which is the minimum, then no point setting charge vars
			if tSoc == soc {
				workMode = scheduler.WorkModeLoadFirst
				gridCharge = scheduler.BatteryFirstGridChargeDisabled
			}
			soc = tSoc // ← monotonic enforced target
		}
		if workMode == scheduler.WorkModeBatteryFirst {
			targetSOCFromOptimisation = soc / 100
		}
		commands = append(commands, Schedule{
			time:                  row.Time,
			workmode:              workMode,
			soc:                   soc,
			chargeBatteryFromGrid: gridCharge,
			targetSOC:             targetSOCFromOptimisation,
		})
	}

	return commands
}

type Row struct {
	Timestamp time.Time
	PPV       float64
	Load      float64
	PBatt     float64
	SOC       float64
	Price     float64
}
