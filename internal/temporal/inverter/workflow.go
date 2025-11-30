package inverter

import (
	"context"
	"fmt"
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
	s client.ScheduleClient
}

func New(s client.ScheduleClient) *Inverter {
	return &Inverter{
		s: s,
	}
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
	return fmt.Sprintf("Executed workmodepriority: %s, batteryfirstgridcharge, %s to SOC %f", workmodepriority, batteryfirstgridcharge, soc), nil
}
