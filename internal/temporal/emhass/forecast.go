package emhass

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/beaujr/emprometheus/internal/emhass"

	"github.com/beaujr/emprometheus/internal/provider"
	"github.com/beaujr/emprometheus/internal/store"
	"go.temporal.io/sdk/client"
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
	s       client.ScheduleClient
	getSoc  func() (int64, error)
	em      *emhass.Emhass
	horizon int64
	db      store.MinimalStore
	steps   int
}

func New(s client.ScheduleClient, tariff provider.RateFetcher, getSoc func() (int64, error), em *emhass.Emhass, db store.Store, steps int) *Forecaster {
	return &Forecaster{tariff: tariff, s: s, getSoc: getSoc, em: em, horizon: 6, db: db, steps: steps}
}

func (f *Forecaster) ForecastWorkflow(ctx workflow.Context, emprometheusUrl string) (string, error) {
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
	err = workflow.ExecuteActivity(ctx, f.BuildScheduleActivity, emprometheusUrl, provider.ActionForecast).Get(ctx, nil)
	if err != nil {
		logger.Error("Activity failed.", "Error", err)
		return "", err
	}
	return "OK", nil
}

func (f *Forecaster) ForecastActivity(ctx context.Context) error {
	if err := f.tariff(f.steps); err != nil {
		return err
	}

	if err := f.em.Forecast(provider.ActionForecast, "{\"publish_prefix\":\"dh_\"}"); err != nil {
		return err
	}

	return nil
}

func (f *Forecaster) BuildScheduleActivity(ctx context.Context, emprometheus, forecastMethod string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/process", emprometheus), nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("forecast-method", forecastMethod)
	c := &http.Client{Timeout: 40 * time.Second}
	resp, err := c.Do(req)
	if err != nil {
		return 0, err
	}
	return resp.StatusCode, nil
}
