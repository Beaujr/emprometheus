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
	Run(ctx context.Context, method string) error
	Start(ctx context.Context) error
}

type SimplePowerPlant interface {
	Status(ctx context.Context) (batteryfirstgridcharge string, workmodepriority string, soc int64, err error)
	Process(ctx context.Context, batteryfirstgridcharge string, workmodepriority string, soc int64) error
}

type ControllablePowerPlant interface {
	SimplePowerPlant
	SetBatteryFirstGridCharge(ctx context.Context, enabled string) error
	SetWorkModePriority(ctx context.Context, workmode string) error
	SetLoadFirstStopDischargeState(ctx context.Context, soc int64) error
	SetCurrentSOC(ctx context.Context, soc int64) error
	GetCurrentSOC(ctx context.Context) (int64, error)
	GetTargetSoc(ctx context.Context) (int64, error)
	SetCurrentDeviceMode(ctx context.Context, mode string) error
	GetCurrentDeviceMode(ctx context.Context) (string, error)
	GetTargetDeviceMode(ctx context.Context) (string, error)
	SetCurrentBatteryFirstGridCharge(ctx context.Context, gridFirstBatteryCharge string) error
	GetCurrentBatteryFirstGridCharge(ctx context.Context) (string, error)
	GetTargetBatteryFirstGridCharge(ctx context.Context) (string, error)
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}
type DebugScheduler struct{}

func (DebugScheduler) InitForecastSchedule(ctx context.Context) error {
	return nil
}

func (DebugScheduler) Run(ctx context.Context, method string) error {
	return nil
}

func (DebugScheduler) Start(ctx context.Context) error {
	return nil
}
