package emhass

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"time"

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

var (
	soc = flag.String("soc", "battery_soc", "the prometheus battery SOC metric")
)

type Forecaster struct {
	tariff  provider.RateFetcher
	s       client.ScheduleClient
	getSoc  func() (int64, error)
	c       http.Client
	horizon int64
	db      store.MinimalStore
	steps   int
}

func New(s client.ScheduleClient, tariff provider.RateFetcher, getSoc func() (int64, error), c http.Client, db store.Store, steps int) *Forecaster {
	return &Forecaster{tariff: tariff, s: s, getSoc: getSoc, c: c, horizon: 6, db: db, steps: steps}
}

func (f *Forecaster) ForecastWorkflow(ctx workflow.Context, emhassUrl, emprometheusUrl string) (string, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: time.Minute,
			MaximumAttempts: 2,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	logger := workflow.GetLogger(ctx)
	logger.Info("forecast workflow started", "url", emhassUrl)

	var result int
	err := workflow.ExecuteActivity(ctx, f.ForecastActivity, emhassUrl).Get(ctx, &result)
	if err != nil {
		if errors.Is(err, provider.TariffNotAvailable) {
			return "Not Ready", nil
		}
		logger.Error("Activity failed.", "Error", err)
		return "", err
	}
	if result != http.StatusCreated {
		return "", errors.New("failed ForecastActivity")
	}
	err = workflow.ExecuteActivity(ctx, f.BuildScheduleActivity, emprometheusUrl, provider.ActionForecast).Get(ctx, &result)
	if err != nil {
		logger.Error("Activity failed.", "Error", err)
		return "", err
	}
	if result != http.StatusOK {
		return "", errors.New("failed BuildScheduleActivity")
	}
	return "OK", nil
}

func (f *Forecaster) ForecastActivity(ctx context.Context, emhassUrl string) (int, error) {
	if err := f.tariff(f.steps); err != nil {
		return 0, err
	}
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/action/%s", emhassUrl, provider.ActionForecast), strings.NewReader("{\"publish_prefix\":\"dh_\"}"))
	if err != nil {
		return 0, err
	}

	resp, err := f.c.Do(req)
	if err != nil {
		return 0, err
	}
	return resp.StatusCode, nil
}

func (f *Forecaster) BuildScheduleActivity(ctx context.Context, emprometheus, forecastMethod string) (int, error) {
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/process", emprometheus), nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("forecast-method", forecastMethod)
	resp, err := f.c.Do(req)
	if err != nil {
		return 0, err
	}
	return resp.StatusCode, nil
}
