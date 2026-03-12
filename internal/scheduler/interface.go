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

type ProcessFunc func(ctx context.Context, batteryfirstgridcharge string, workmodepriority string, soc int64) error

type Scheduler interface {
	InitForecastSchedule(ctx context.Context) error
	Run(ctx context.Context, method string) error
	Start(ctx context.Context) error
}
type ControllablePowerPlant interface {
	SetBatteryFirstGridCharge(enabled string) error
	SetWorkModePriority(workmode string) error
	SetLoadFirstStopDischarge(soc int64) error
	Process(ctx context.Context, batteryfirstgridcharge string, workmodepriority string, soc int64) error
	SetCurrentSOC(soc int64) error
	GetCurrentSOC() (int64, error)
	GetTargetSOC() (int64, error)
	SetCurrentDeviceMode(mode string)
	GetCurrentDeviceMode() (string, error)
	GetTargetDeviceMode() (string, error)
	SetCurrentBatteryFirstGridCharge(gridFirstBatteryCharge string)
	GetCurrentBatteryFirstGridCharge() (string, error)
	GetTargetBatteryFirstGridCharge() (string, error)
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
