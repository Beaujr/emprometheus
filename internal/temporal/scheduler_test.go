package temporal

import (
	"slices"
	"testing"
	"time"

	"github.com/beaujr/emprometheus/internal/emhass"
	"github.com/beaujr/emprometheus/internal/scheduler"
	"github.com/beaujr/emprometheus/internal/store"
)

func TestSchedulerGetCommands(t *testing.T) {
	minUnitLoadCost := 1.0
	maxUnitLoadCost := 10.0
	medUnitLoadCost := 5.0
	now := time.Now()
	schedules := []emhass.OptimizationResult{emhass.NewOptimizationResult(now.Add(time.Hour), 0.1, maxUnitLoadCost, 0, 0),
		emhass.NewOptimizationResult(now.Add(2*time.Hour), 0.1, maxUnitLoadCost, 0, 0),
		emhass.NewOptimizationResult(now.Add(3*time.Hour), 0.5, minUnitLoadCost, 0, 0),
		emhass.NewOptimizationResult(now.Add(4*time.Hour), 1.0, minUnitLoadCost, 0, 0),
		emhass.NewOptimizationResult(now.Add(5*time.Hour), 0.8, medUnitLoadCost, 0, 0),
		emhass.NewOptimizationResult(now.Add(6*time.Hour), 0.7, medUnitLoadCost, 0, 0),
		emhass.NewOptimizationResult(now.Add(6*time.Hour), 0.7, minUnitLoadCost, 0, 0),
		emhass.NewOptimizationResult(now.Add(6*time.Hour), 0.7, maxUnitLoadCost, 0, 0),
		emhass.NewOptimizationResult(now.Add(6*time.Hour), 0.7, minUnitLoadCost, 0, 0),
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

			if sch.targetSOC != (sch.soc / 100) {
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
	schedules := []emhass.OptimizationResult{
		emhass.NewOptimizationResult(getNextThirtyMinuteSlot(now, 1, 0), 0.1, maxUnitLoadCost, 0, 0),
		emhass.NewOptimizationResult(getNextThirtyMinuteSlot(now, 1, 30), 0.1, maxUnitLoadCost, 0, 0),
		emhass.NewOptimizationResult(getNextThirtyMinuteSlot(now, 2, 00), 0.1, maxUnitLoadCost, 0, 0),
		emhass.NewOptimizationResult(getNextThirtyMinuteSlot(now, 2, 30), 0.5, minUnitLoadCost, 0, 0),
		emhass.NewOptimizationResult(getNextThirtyMinuteSlot(now, 2, 30), 0.5, minUnitLoadCost, 0, 0),
		emhass.NewOptimizationResult(getNextThirtyMinuteSlot(now, 3, 00), 1.0, minUnitLoadCost, 0, 0),
		emhass.NewOptimizationResult(getNextThirtyMinuteSlot(now, 3, 30), 0.8, medUnitLoadCost, 0, 0),
		emhass.NewOptimizationResult(getNextThirtyMinuteSlot(now, 4, 00), 0.7, medUnitLoadCost, 0, 0),
		emhass.NewOptimizationResult(getNextThirtyMinuteSlot(now, 4, 30), 0.7, minUnitLoadCost, 0, 0),
		emhass.NewOptimizationResult(getNextThirtyMinuteSlot(now, 5, 00), 0.7, maxUnitLoadCost, 0, 0),
		emhass.NewOptimizationResult(getNextThirtyMinuteSlot(now, 5, 30), 0.7, minUnitLoadCost, 0, 0),
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
			if sch.time.Equal(result.Time()) {
				if result.UnitLoadCost() == minUnitLoadCost {
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
func TestHigherSOCInNextRowSchedule(t *testing.T) {
	minUnitLoadCost := 1.0
	maxUnitLoadCost := 10.0
	now := time.Now()
	schedules := []emhass.OptimizationResult{
		emhass.NewOptimizationResult(getNextThirtyMinuteSlot(now, 1, 0), 0.1, maxUnitLoadCost, 0, 0),
		emhass.NewOptimizationResult(getNextThirtyMinuteSlot(now, 1, 30), 0.1, minUnitLoadCost, 0, 0),
		emhass.NewOptimizationResult(getNextThirtyMinuteSlot(now, 2, 00), 1.0, maxUnitLoadCost, 0, 0),
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
	t.Log("Optimization,time,Work Mode, Grid Charge, Stop Discharge at SOC, Target SOC")
	for _, sch := range schs {
		r := store.Row{
			Optimization:     "test",
			Time:             sch.time,
			WorkMode:         sch.workmode,
			GridCharge:       sch.chargeBatteryFromGrid,
			StopDischargeSOC: sch.soc,
			TargetSOC:        sch.targetSOC,
		}
		t.Log(r.String())
		for idx, result := range schedules {
			if sch.time.Equal(result.Time()) {
				if result.UnitLoadCost() == minUnitLoadCost {
					if sch.workmode != scheduler.WorkModeBatteryFirst {
						t.Fail()
					}
					if sch.chargeBatteryFromGrid != scheduler.BatteryFirstGridChargeEnabled {
						t.Fail()
					}
					if sch.soc != (schedules[idx+1].SOCOpt() * 100) {
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
