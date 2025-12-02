package inverter

import (
	"context"
	"fmt"
	"github.com/beaujr/emprometheus/internal/scheduler"
	"github.com/beaujr/emprometheus/internal/solarassistant"
	"go.temporal.io/sdk/client"
	"log/slog"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

const (
	WorkflowId = "inverterworkflow"
	TaskQueue  = "inverterqueue"
)

type Inverter struct {
	s  client.ScheduleClient
	pp scheduler.ControllablePowerPlant
}

func New(s client.ScheduleClient) (*Inverter, error) {
	sa, err := solarassistant.New()
	if err != nil {
		return nil, err
	}
	return &Inverter{
		s:  s,
		pp: sa,
	}, nil
}

func (i *Inverter) Start(ctx context.Context) error {
	if err := i.pp.Start(ctx); err != nil {
		return err
	}
	return nil
}

func (i *Inverter) Workflow(ctx workflow.Context, scheduleId, workmodepriority, batteryfirstgridcharge string, soc float64) (string, error) {
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

	var result string
	err := workflow.ExecuteActivity(ctx, i.Activity, workmodepriority, batteryfirstgridcharge, soc).Get(ctx, &result)
	if err != nil {
		logger.Error("Activity failed.", "Error", err)
		return "", err
	}

	logger.Info("workflow completed.", "result", result)
	return result, nil
}

func (i *Inverter) Activity(ctx context.Context, workmodepriority, batteryfirstgridcharge string, soc float64) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Activity", "workmodepriority", workmodepriority, "batteryfirstgridcharge", batteryfirstgridcharge, "soc", soc)

	if err := i.pp.SetBatteryFirstGridCharge(batteryfirstgridcharge); err != nil {
		return "", err
	}
	if err := i.pp.SetWorkModePriority(workmodepriority); err != nil {
		return "", err
	}

	switch workmodepriority {

	case scheduler.WorkModeBatteryFirst:
		if err := i.pp.SetLoadFirstStopDischarge(int64(soc) * 100); err != nil {
			return "", err
		}
	default:
		// always allow discharging to 10 (minimum) unless explicitly grid charging
		if err := i.pp.SetLoadFirstStopDischarge(10); err != nil {
			return "", err
		}
	}

	return fmt.Sprintf("Executed workmodepriority: %s, batteryfirstgridcharge, %s to SOC %f", workmodepriority, batteryfirstgridcharge, soc), nil
}
