package emhass

import (
	"context"
	"errors"
	"fmt"
	"github.com/beaujr/emprometheus/internal/provider"
	"github.com/beaujr/emprometheus/internal/store"
	"go.temporal.io/sdk/temporal"
	"net/http"
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
	v, err := f.getSoc()
	if err != nil {
		return "", err
	}
	batterySOC := float64(v)
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
	now := time.Now()
	rows, err := f.db.Select(now)
	if err != nil {
		return 0.0, err
	}
	var r *store.Row
	for i, scheduleRow := range rows {
		if scheduleRow.Time.Before(now) {
			continue
		}

		if scheduleRow.WorkMode == "Load first" &&
			rows[i+1].WorkMode == "Battery first" {
			r = &rows[i]
			break
		}
	}
	// if I cant find the SoC assume fully charged
	if r == nil {
		return 1.0, nil
	}

	f.horizon = int64(r.Time.Sub(now).Hours())
	if f.horizon < 5 {
		f.horizon = 5
		fiveHourstime, err := f.db.Find(time.Date(
			now.Year(),
			now.Month(),
			now.Day(),
			now.Hour()+5,
			0, 0, 0,
			now.Location(),
		))
		if err != nil {
			return 0.0, err
		}
		return fiveHourstime.TargetSOC, nil
	}
	return r.TargetSOC, nil

}

func (f *Forecaster) MPCActivity(ctx context.Context, emhassUrl string, currentSoc, finalSoc float64) (int, error) {
	defer func() {
		if err := recover(); err != nil {
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
