package emhass

import (
	"context"
	"errors"
	"fmt"
	"github.com/beaujr/emprometheus/internal/provider"
	"github.com/prometheus/common/model"
	"go.temporal.io/sdk/temporal"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/sdk/workflow"
)

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
	// get current soc
	v, _, err := f.p.Query(context.Background(), *soc, time.Now())
	if err != nil {
		return "", err
	}
	if len(v.(model.Vector)) == 0 {
		return "", err
	}
	value := v.(model.Vector)[0].Value.String()
	batterySOC, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return "", err
	}
	var finalSOC float64
	err = workflow.ExecuteActivity(ctx, f.GetHorizonSOCActivity, emprometheusUrl).Get(ctx, &finalSOC)
	if err != nil {
		logger.Error("Activity failed.", "Error", err)
		return "", err
	}
	var result int
	err = workflow.ExecuteActivity(ctx, f.MPCActivity, emhassUrl, batterySOC, finalSOC).Get(ctx, &result)
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
	err = workflow.ExecuteActivity(ctx, f.BuildScheduleActivity, emprometheusUrl, provider.ActionMPC).Get(ctx, &result)
	if err != nil {
		logger.Error("Activity failed.", "Error", err)
		return "", err
	}
	if result != http.StatusOK {
		return "", errors.New("failed to forecast emhass")
	}

	return "OK", nil
}

func (f *Forecaster) GetHorizonSOCActivity(ctx context.Context) (float64, error) {
	out, err := os.ReadFile(filepath.Join(f.dir, fmt.Sprintf("%s.csv", provider.ActionForecast)))
	if err != nil {
		return 0, err
	}
	now := time.Now()
	finalSOC := 0.0
	for idx, line := range strings.Split(string(out), "\n") {
		if idx == 0 {
			continue
		}
		pieces := strings.Split(line, ",")
		if len(pieces) > 0 {
			t, err := time.Parse("2006-01-02 15:04:05-07:00", pieces[0])
			if err != nil {
				continue
			}
			if t.After(now.Add(time.Duration(f.horizon)*time.Hour)) && t.Before(now.Add(time.Duration(f.horizon+1)*time.Hour)) {
				finalSOC, err = strconv.ParseFloat(pieces[7], 64)
				if err != nil {
					continue
				}
				break
			}
		}
	}
	return finalSOC, nil

}

func (f *Forecaster) MPCActivity(ctx context.Context, emhassUrl string, currentSoc, finalSoc float64) (int, error) {
	defer func() {
		err := recover()
		if err != nil {
			return
		}
	}()
	if err := f.tariff(); err != nil {
		return 0, err
	}

	payload := fmt.Sprintf("{\"soc_init\": %.2f, \"prediction_horizon\": %d, \"soc_final\": %.2f}", currentSoc/100, f.horizon, finalSoc)
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/action/%s", emhassUrl, provider.ActionMPC), strings.NewReader(payload))
	if err != nil {
		return 0, err
	}

	resp, err := f.c.Do(req)
	if err != nil {
		return 0, err
	}
	return resp.StatusCode, nil
}
