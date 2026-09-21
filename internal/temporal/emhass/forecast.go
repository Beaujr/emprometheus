package emhass

import (
	"context"
	"errors"
	"time"

	"github.com/beaujr/emprometheus/internal/emhass"

	"github.com/beaujr/emprometheus/internal/provider"
	"github.com/beaujr/emprometheus/internal/store"
	"go.temporal.io/sdk/temporal"

	"go.temporal.io/sdk/workflow"
)

const (
	WorkflowId    = "emhassforecast"
	TaskQueue     = "emhassforecastqueue"
	WorkflowIdMPC = "emhassmpc"
	TaskQueueMPC  = "emhassmpcqueue"
)

type Forecaster struct {
	tariff  provider.RateFetcher
	getSoc  func() (int64, error)
	em      *emhass.Emhass
	horizon int64
	db      store.MinimalStore
	steps   int
	run     func(ctx context.Context, method string) error
}

type Run = func(ctx context.Context, method string) error
type GetSoc = func() (int64, error)

func New(tariff provider.RateFetcher, getSoc GetSoc, run Run, em *emhass.Emhass, db store.Store, steps int) *Forecaster {
	return &Forecaster{tariff: tariff, getSoc: getSoc, run: run, em: em, horizon: 6, db: db, steps: steps}
}

func (f *Forecaster) ForecastWorkflow(ctx workflow.Context) (string, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Minute,
			MaximumAttempts: 2,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	logger := workflow.GetLogger(ctx)
	logger.Info("forecast workflow started")

	err := workflow.ExecuteActivity(ctx, f.ForecastActivity).Get(ctx, nil)
	if err != nil {
		if errors.Is(err, provider.TariffNotAvailable) {
			return "Not Ready", nil
		}
		logger.Error("Activity failed.", "Error", err)
		return "", err
	}
	err = workflow.ExecuteActivity(ctx, f.BuildScheduleActivity, provider.ActionForecast).Get(ctx, nil)
	if err != nil {
		logger.Error("Activity failed.", "Error", err)
		return "", err
	}
	return "OK", nil
}

func (f *Forecaster) ForecastActivity(ctx context.Context) error {
	if err := f.tariff(ctx, f.steps); err != nil {
		return err
	}

	if err := f.em.Forecast(ctx, provider.ActionForecast, "{\"publish_prefix\":\"dh_\"}"); err != nil {
		return err
	}

	return nil
}

func (f *Forecaster) BuildScheduleActivity(ctx context.Context, forecastMethod string) error {
	return f.run(ctx, forecastMethod)
}
