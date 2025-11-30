package emhass

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/beaujr/emprometheus/internal/prometheus"
	"github.com/beaujr/emprometheus/internal/provider"
	"github.com/prometheus/common/model"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

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
	tariff provider.RateFetcher
	s      client.ScheduleClient
	p      prometheus.Reporter
}

func New(s client.ScheduleClient, tariff provider.RateFetcher, p prometheus.Reporter) *Forecaster {
	return &Forecaster{tariff: tariff, s: s, p: p}
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
func (f *Forecaster) MPCWorkflow(ctx workflow.Context, emhassUrl, emprometheusUrl string) (string, error) {
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
	err := workflow.ExecuteActivity(ctx, f.MPCActivity, emhassUrl).Get(ctx, &result)
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

func (f *Forecaster) MPCActivity(ctx context.Context, emhassUrl string) (int, error) {
	if err := f.tariff(); err != nil {
		return 0, err
	}
	// get current soc
	v, _, err := f.p.Query(ctx, *soc, time.Now())
	if err != nil {
		return 0, err
	}
	if len(v.(model.Vector)) == 0 {
		return 0, err
	}
	value := v.(model.Vector)[0].Value.String()
	batterySOC, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, err
	}
	httpClient := http.Client{Timeout: 30 * time.Second}
	payload := fmt.Sprintf("{\"soc_init\": %.2f}", batterySOC/100)
	logger := activity.GetLogger(ctx)
	logger.Info("payload", slog.String("payload", payload))
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/action/naive-mpc-optim", emhassUrl), strings.NewReader(payload))
	if err != nil {
		return 0, err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	return resp.StatusCode, nil
}

func (f *Forecaster) ForecastActivity(ctx context.Context, emhassUrl string) (int, error) {
	if err := f.tariff(); err != nil {
		return 0, err
	}
	httpClient := http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/action/dayahead-optim", emhassUrl), strings.NewReader("{\"publish_prefix\":\"dh_\"}"))
	if err != nil {
		return 0, err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	return resp.StatusCode, nil
}

func (f *Forecaster) BuildScheduleActivity(ctx context.Context, emprometheus string) (int, error) {
	httpClient := http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/process", emprometheus), nil)
	if err != nil {
		return 0, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	return resp.StatusCode, nil
}
