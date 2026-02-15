package temporal

import (
	"cmp"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/beaujr/emprometheus/internal/prometheus"
	"github.com/beaujr/emprometheus/internal/provider"
	"github.com/beaujr/emprometheus/internal/scheduler"
	"github.com/beaujr/emprometheus/internal/temporal/emhass"
	"github.com/beaujr/emprometheus/internal/temporal/inverter"
	"github.com/google/uuid"
	"go.temporal.io/sdk/client"
	temporal2 "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"golang.org/x/sync/errgroup"
	"log/slog"
	"net/http"
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
	mpc     bool
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
		e := worker.New(fs.c, w.queue, worker.Options{})
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

	errGrp.Go(func() error {
		if err := fs.InitForecastSchedule(ctx); err != nil {
			return err
		}
		return nil
	})

	return errGrp.Wait()

}

func New(ctx context.Context, logger *slog.Logger, c client.Client, tariffs provider.RateFetcher, p prometheus.Reporter, dir, cron string, mpc bool) (*Temporal, error) {
	s := c.ScheduleClient()
	f := emhass.New(s, tariffs, p, http.Client{Timeout: 60 * time.Second}, dir)
	//f.MPCActivity(ctx, "http://localhost:8123", 72.00)
	i, err := inverter.New(s)
	if err != nil {
		return nil, err
	}
	return &Temporal{
		logger:  logger,
		c:       c,
		s:       s,
		f:       f,
		i:       i,
		cron:    cron,
		tariffs: tariffs,
		mpc:     mpc,
	}, nil
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
		if err := fs.setUpForecastWorkflows(ctx); err != nil {
			return err
		}
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
	_, err := fs.s.Create(ctx, client.ScheduleOptions{
		ID: scheduleID,
		Spec: client.ScheduleSpec{
			CronExpressions: []string{"2 */6 * * *"},
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

func (fs *Temporal) setUpForecastWorkflows(ctx context.Context) error {
	fs.logger.Info("forecast")
	scheduleID := emhass.WorkflowId
	schedule := "1 0 * * *"
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
			CronExpressions: []string{schedule},
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
				CronExpressions: []string{schedule},
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

func (fs *Temporal) Run(ctx context.Context, dir, method string) error {
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
				if err.Error() != "workflow execution already completed" {
					return err
				}
			}
		}
	}
	if _, err = os.Stat(filepath.Join(dir, provider.CSVForecastName)); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		switch fs.mpc {
		case true:
			if err = fs.setUpMPCWorkflows(ctx); err != nil {
				return err
			}
		default:
			if err = fs.InitForecastSchedule(ctx); err != nil {
				return err
			}
		}

	}
	if err = fs.output(ctx, dir, method); err != nil {
		return err
	}
	return err
}

type Schedule struct {
	workmode, chargeBatteryFromGrid string
	soc, targetSOC                  float64
	time                            time.Time
}

func (fs *Temporal) output(ctx context.Context, dir, method string) error {
	file, err := os.Open(filepath.Join(dir, fmt.Sprintf("%s.csv", method)))
	if err != nil {
		return err
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
	var schedules []Schedule
	schedules = fs.getCommands(rows)

	var s *os.File
	for idx, schedule := range schedules {
		if idx == 0 {
			f, err := os.Create(filepath.Join(dir, provider.CSVScheduleName))
			if err != nil {
				return err
			}
			s = f
			defer s.Close()
			if _, err = s.WriteString("time,Work Mode, Grid Charge, Stop Discharge at SOC, Target SOC\n"); err != nil {
				return err
			}
		}
		fs.logger.Info(schedule.time.Format(time.RFC3339), slog.String("work mode", schedule.workmode), slog.String("battery first gridcharge", schedule.chargeBatteryFromGrid))
		if _, err = s.WriteString(fmt.Sprintf("%s,%s,%s,%.2f,%.2f\n", schedule.time.Format(time.RFC3339), schedule.workmode, schedule.chargeBatteryFromGrid, schedule.soc, schedule.targetSOC)); err != nil {
			return err
		}
		if err = fs.process(ctx, schedule.time, schedule.workmode, schedule.chargeBatteryFromGrid, schedule.soc); err != nil {
			fs.logger.Warn("error", slog.String("error", err.Error()))
		}
	}
	return nil
}

func (fs *Temporal) getCommands(rows []Row) []Schedule {
	if len(rows) == 0 {
		return nil
	}

	//
	// 1. Find minimum grid price (cheapest tariff)
	//
	minGridRow := slices.MinFunc(rows, func(a, b Row) int {
		return cmp.Compare(a.Price, b.Price)
	})
	minPrice := minGridRow.Price

	//
	// 2. Identify continuous blocks of cheapest hours
	//
	type block struct {
		start int
		end   int
	}

	var blocks []block
	inBlock := false
	startIdx := 0

	for i := range rows {
		if rows[i].Price == minPrice {
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

	//
	// 3. Build monotonic SoC table for the entire day
	//
	monotonicSOC := make([]float64, len(rows))

	for _, b := range blocks {
		// First hour SoC target (converted to %)
		lastSOC := rows[b.start].SOC * 100

		for i := b.start; i <= b.end; i++ {
			target := rows[i].SOC * 100

			// Only allow increases inside the block
			if target > lastSOC {
				lastSOC = target
			}

			monotonicSOC[i] = lastSOC
		}
	}

	//
	// 4. Build final inverter command list
	//
	var commands []Schedule
	for i, row := range rows {
		workMode := scheduler.WorkModeLoadFirst
		gridCharge := scheduler.BatteryFirstGridChargeDisabled
		soc := 10.0 // default load-first SoC

		if row.Price == minPrice {
			// Inside cheap block
			workMode = scheduler.WorkModeBatteryFirst
			gridCharge = scheduler.BatteryFirstGridChargeEnabled
			soc = monotonicSOC[i] // ← monotonic enforced target
		}

		commands = append(commands, Schedule{
			time:                  row.Timestamp,
			workmode:              workMode,
			soc:                   soc,
			chargeBatteryFromGrid: gridCharge,
			targetSOC:             row.SOC,
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

func parseFloat(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}
