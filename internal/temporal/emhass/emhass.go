package emhass

import (
	"context"
	"errors"
	"fmt"
	"github.com/beaujr/emprometheus/internal/provider"
	"log/slog"
	"net/http"
	"time"

	"go.temporal.io/sdk/workflow"
)

const (
	WorkflowId = "emhassforecast"
	TaskQueue  = "emhassforecastqueue"
)

type Forecaster struct {
	logger *slog.Logger
	tariff provider.RateFetcher
}

func New(logger *slog.Logger, tariff provider.RateFetcher) *Forecaster {
	return &Forecaster{logger: logger, tariff: tariff}
}

func (f *Forecaster) Workflow(ctx workflow.Context, emhassUrl, emprometheusUrl string) (string, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
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
		return "", errors.New("failed to forecast emhass")
	}
	err = workflow.ExecuteActivity(ctx, f.BuildScheduleActivity, emprometheusUrl).Get(ctx, &result)
	if err != nil {
		logger.Error("Activity failed.", "Error", err)
		return "", err
	}
	if result != http.StatusOK {
		return "", errors.New("failed to forecast emhass")
	}

	return "OK", nil
}

func (f *Forecaster) ForecastActivity(ctx context.Context, emhassUrl string) (int, error) {
	if err := f.tariff(); err != nil {
		return 0, err
	}
	client := http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/action/dayahead-optim", emhassUrl), nil)
	if err != nil {
		return 0, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	return resp.StatusCode, nil
}
func (f *Forecaster) BuildScheduleActivity(ctx context.Context, emprometheus string) (int, error) {
	client := http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/process", emprometheus), nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	return resp.StatusCode, nil
}
