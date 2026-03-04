package temporal

import (
	"github.com/beaujr/emprometheus/internal/scheduler"
	"github.com/beaujr/emprometheus/internal/store"
	"slices"
	"testing"
	"time"
)

func TestSchedulerGetCommands(t *testing.T) {
	minUnitLoadCost := 1.0
	maxUnitLoadCost := 10.0
	medUnitLoadCost := 5.0
	now := time.Now()
	schedules := []store.OptimizationResult{
		{
			Time:         now.Add(time.Hour),
			SOCOpt:       0.1,
			UnitLoadCost: maxUnitLoadCost,
		},
		{
			Time:         now.Add(2 * time.Hour),
			SOCOpt:       0.1,
			UnitLoadCost: maxUnitLoadCost,
		},
		{
			Time:         now.Add(3 * time.Hour),
			SOCOpt:       0.5,
			UnitLoadCost: minUnitLoadCost,
		},
		{
			Time:         now.Add(4 * time.Hour),
			SOCOpt:       1.0,
			UnitLoadCost: minUnitLoadCost,
		},
		{
			Time:         now.Add(5 * time.Hour),
			SOCOpt:       0.8,
			UnitLoadCost: medUnitLoadCost,
		},
		{
			Time:         now.Add(6 * time.Hour),
			SOCOpt:       0.7,
			UnitLoadCost: medUnitLoadCost,
		},
		{
			Time:         now.Add(6 * time.Hour),
			SOCOpt:       0.7,
			UnitLoadCost: minUnitLoadCost,
		},
		{
			Time:         now.Add(6 * time.Hour),
			SOCOpt:       0.7,
			UnitLoadCost: maxUnitLoadCost,
		},
		{
			Time:         now.Add(6 * time.Hour),
			SOCOpt:       0.7,
			UnitLoadCost: minUnitLoadCost,
		},
	}
	schs := GetCommands(schedules)
	if schs == nil {
		t.Fail()
		return
	}

	if len(schedules) != len(schs) {
		t.Fail()
		return
	}
	chargeIdxs := []int{2, 3, 6, 8}
	for idx, sch := range schs {
		if slices.Contains(chargeIdxs, idx) {
			if sch.chargeBatteryFromGrid != scheduler.BatteryFirstGridChargeEnabled {
				t.Fail()
				return
			}
			if sch.workmode != scheduler.WorkModeBatteryFirst {
				t.Fail()
				return
			}
			if sch.targetSOC != schedules[idx].SOCOpt {
				t.Fail()
				return
			}
			continue
		}
		if sch.chargeBatteryFromGrid != scheduler.BatteryFirstGridChargeDisabled {
			t.Fail()
			return
		}
		if sch.workmode != scheduler.WorkModeLoadFirst {
			t.Fail()
			return
		}

		if sch.soc != 10 {
			t.Fail()
			return
		}
	}
}

func TestThirtyMinuteSchedule(t *testing.T) {
	minUnitLoadCost := 1.0
	maxUnitLoadCost := 10.0
	medUnitLoadCost := 5.0
	now := time.Now()
	schedules := []store.OptimizationResult{
		{
			Time:         getNextThirtyMinuteSlot(now, 1, 0),
			SOCOpt:       0.1,
			UnitLoadCost: maxUnitLoadCost,
		},
		{
			Time:         getNextThirtyMinuteSlot(now, 1, 30),
			SOCOpt:       0.1,
			UnitLoadCost: maxUnitLoadCost,
		},
		{
			Time:         getNextThirtyMinuteSlot(now, 2, 00),
			SOCOpt:       0.1,
			UnitLoadCost: maxUnitLoadCost,
		},
		{
			Time:         getNextThirtyMinuteSlot(now, 2, 30),
			SOCOpt:       0.5,
			UnitLoadCost: minUnitLoadCost,
		},
		{
			Time:         getNextThirtyMinuteSlot(now, 3, 00),
			SOCOpt:       1.0,
			UnitLoadCost: minUnitLoadCost,
		},
		{
			Time:         getNextThirtyMinuteSlot(now, 3, 30),
			SOCOpt:       0.8,
			UnitLoadCost: medUnitLoadCost,
		},
		{
			Time:         getNextThirtyMinuteSlot(now, 4, 00),
			SOCOpt:       0.7,
			UnitLoadCost: medUnitLoadCost,
		},
		{
			Time:         getNextThirtyMinuteSlot(now, 4, 30),
			SOCOpt:       0.7,
			UnitLoadCost: minUnitLoadCost,
		},
		{
			Time:         getNextThirtyMinuteSlot(now, 5, 00),
			SOCOpt:       0.7,
			UnitLoadCost: maxUnitLoadCost,
		},
		{
			Time:         getNextThirtyMinuteSlot(now, 5, 30),
			SOCOpt:       0.7,
			UnitLoadCost: minUnitLoadCost,
		},
	}
	schs := GetCommands(schedules)
	if schs == nil {
		t.Fail()
		return
	}

	if len(schedules) != len(schs) {
		t.Fail()
		return
	}
	for _, sch := range schs {
		t.Log(sch)
		for _, result := range schedules {
			if sch.time.Equal(result.Time) {
				if result.UnitLoadCost == minUnitLoadCost {
					if sch.workmode != scheduler.WorkModeBatteryFirst {
						t.Fail()
					}
					if sch.chargeBatteryFromGrid != scheduler.BatteryFirstGridChargeEnabled {
						t.Fail()
					}
				}
				break
			}
		}
	}
}

func getNextThirtyMinuteSlot(t time.Time, hours int, minutes int) time.Time {
	return t.Add(time.Duration(hours) * time.Hour).Truncate(time.Hour).Add(time.Duration(minutes) * time.Minute)
}
