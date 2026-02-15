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
	Run(ctx context.Context, dir, method string) error
	Start(ctx context.Context) error
}
type ControllablePowerPlant interface {
	SetBatteryFirstGridCharge(enabled string) error
	SetWorkModePriority(workmode string) error
	SetLoadFirstStopDischarge(soc int64) error
	Process(batteryfirstgridcharge string, workmodepriority string, soc int64) error
	SetCurrentSOC(soc int64) error
	GetCurrentSOC() (int64, error)
	GetTargetSOC() (int64, error)
	SetCurrentDeviceMode(mode string)
	GetCurrentDeviceMode() string
	GetTargetDeviceMode() string
	SetCurrentBatteryFirstGridCharge(gridFirstBatteryCharge string)
	GetCurrentBatteryFirstGridCharge() string
	GetTargetBatteryFirstGridCharge() string
	Start(ctx context.Context) error
}
type DebugScheduler struct{}

func (DebugScheduler) InitForecastSchedule(ctx context.Context) error {
	return nil
}

func (DebugScheduler) Run(ctx context.Context, dir, method string) error {
	return nil
}

func (DebugScheduler) Start(ctx context.Context) error {
	return nil
}
