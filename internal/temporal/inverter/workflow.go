package inverter

import (
	"context"
	"errors"
	"fmt"
	"github.com/beaujr/emprometheus/internal/scheduler"
	"github.com/beaujr/emprometheus/internal/store"
	"go.temporal.io/sdk/client"
	"log/slog"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

const (
	WorkflowId = "inverter"
	TaskQueue  = "inverterqueue"
)

type Inverter struct {
	s  client.ScheduleClient
	pp scheduler.ControllablePowerPlant
	db store.MinimalStore
}

func New(s client.ScheduleClient, db store.MinimalStore, sa scheduler.ControllablePowerPlant) (*Inverter, error) {
	return &Inverter{
		s:  s,
		pp: sa,
		db: db,
	}, nil
}

func (i *Inverter) Start(ctx context.Context) error {
	if err := i.pp.Start(ctx); err != nil {
		return err
	}
	return nil
}

func (i *Inverter) Workflow(ctx workflow.Context, scheduleId, workmodepriority, batteryfirstgridcharge string, soc float64, timestamp string) (string, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	dCtx := context.Background()
	logger := workflow.GetLogger(ctx)
	if err := i.s.GetHandle(dCtx, scheduleId).Delete(dCtx); err != nil {
		logger.Warn("failed to delete schedule", slog.String("scheduleId", scheduleId))
	}
	logger.Info("Emprometheus workflow started", "workmodepriority", workmodepriority, "batteryfirstgridcharge", batteryfirstgridcharge, "soc", soc)
	currentSoc, err := i.pp.GetCurrentSOC()
	if err != nil {
		logger.Warn("failed to get current soc", slog.String("scheduleId", scheduleId))
	}
	t, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		return "", err
	}

	if err = i.db.SetActualSoc(t, float64(currentSoc)/100); err != nil {
		logger.Warn("failed to set actual soc", slog.String("scheduleId", scheduleId))
	}
	var result string
	err = workflow.ExecuteActivity(ctx, i.Activity, workmodepriority, batteryfirstgridcharge, soc, t).Get(ctx, &result)
	if err != nil {
		logger.Error("Activity failed.", "Error", err)
		return "", err
	}

	logger.Info("workflow completed.", "result", result)
	return result, nil
}

func (i *Inverter) Activity(ctx context.Context, workmodepriority, batteryfirstgridcharge string, soc float64, t time.Time) (string, error) {
	timestamp := t.Format(time.RFC3339)
	logger := activity.GetLogger(ctx)
	logger.Info("Activity Args", "workmodepriority", workmodepriority, "batteryfirstgridcharge", batteryfirstgridcharge, "soc", soc, "timestamp", timestamp)
	r, err := i.db.Find(t)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return "", err
		}
		logger.Warn("Unable to find schedule in database", "timestamp", t.Format(time.RFC3339))
	}
	logger.Info("Activity Stored", "workmodepriority", r.WorkMode, "batteryfirstgridcharge", r.GridCharge, "soc", r.TargetSOC, "timestamp", timestamp)
	if err = i.pp.Process(ctx, batteryfirstgridcharge, workmodepriority, int64(soc)); err != nil {
		logger.Error("Processing failed.", "Error", err)
		return "", err
	}

	return fmt.Sprintf("Executed workmodepriority: %s, batteryfirstgridcharge, %s to SOC %f", workmodepriority, batteryfirstgridcharge, soc), nil
}
