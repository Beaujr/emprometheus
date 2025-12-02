package scheduler

import (
	"context"
)

const (
	WorkModeBatteryFirst           = "Battery first"
	WorkModeGridFirst              = "Grid first"
	WorkModeLoadFirst              = "Load first"
	BatteryFirstGridChargeEnabled  = "Enabled"
	BatteryFirstGridChargeDisabled = "Disabled"
)

type Scheduler interface {
	InitForecastSchedule(ctx context.Context) error
	Run(ctx context.Context, dir string) error
	Start(ctx context.Context) error
}
type ControllablePowerPlant interface {
	SetBatteryFirstGridCharge(enabled string) error
	SetWorkModePriority(workmode string) error
	SetLoadFirstStopDischarge(soc int64) error
	GetSOC() (int64, error)
	Start(ctx context.Context) error
}
type DebugScheduler struct{}

func (DebugScheduler) InitForecastSchedule(ctx context.Context) error {
	return nil
}

func (DebugScheduler) Run(ctx context.Context, dir string) error {
	return nil
}

func (DebugScheduler) Start(ctx context.Context) error {
	return nil
}
