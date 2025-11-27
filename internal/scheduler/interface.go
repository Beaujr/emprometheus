package scheduler

import (
	"context"
)

type Scheduler interface {
	InitForecastSchedule(ctx context.Context) error
	Run(ctx context.Context, dir string) error
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
