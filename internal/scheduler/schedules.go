package scheduler

import (
	"cmp"
	"context"
	"log/slog"
	"slices"
	"time"

	"github.com/beaujr/emprometheus/internal/store"
	"github.com/beaujr/emprometheus/internal/types"
)

type Schedule struct {
	workmode, chargeBatteryFromGrid string
	soc, TargetSoc                  float64
	time                            time.Time
}

func (s Schedule) Workmode() string {
	return s.workmode
}

func (s Schedule) ChargeBatteryFromGrid() string {
	return s.chargeBatteryFromGrid
}

func (s Schedule) Soc() float64 {
	return s.soc
}

func (s Schedule) Time() time.Time {
	return s.time
}

func GetCommands(rows []types.OptimizationResult) []Schedule {
	if len(rows) == 0 {
		return nil
	}
	minGridRow := slices.MinFunc(rows, func(a, b types.OptimizationResult) int {
		// avoid setting rows with 0 cost as minGridRow
		if a.UnitLoadCost() == 0 {
			return 1
		}
		return cmp.Compare(a.UnitLoadCost(), b.UnitLoadCost())
	})
	minPrice := minGridRow.UnitLoadCost()

	type block struct {
		start int
		end   int
	}

	var blocks []block
	inBlock := false
	startIdx := 0

	for i := range rows {
		if rows[i].UnitLoadCost() == minPrice || rows[i].UnitLoadCost() == 0 {
			if !inBlock {
				inBlock = true
				startIdx = i
			}
		} else {
			if inBlock {
				blocks = append(blocks, block{start: startIdx, end: i - 1})
				inBlock = false
			}
		}
	}
	if inBlock {
		blocks = append(blocks, block{start: startIdx, end: len(rows) - 1})
	}

	monotonicSOC := make([]float64, len(rows))

	for _, b := range blocks {
		// First hour SoC target (converted to %)
		lastSOC := rows[b.start].SOCOpt() * 100

		for i := b.start; i <= b.end; i++ {
			target := rows[i].SOCOpt() * 100

			// Only allow increases inside the block
			if target > lastSOC {
				lastSOC = target
			}

			monotonicSOC[i] = lastSOC
		}
	}

	var commands []Schedule
	for i, row := range rows {
		workMode := WorkModeLoadFirst
		gridCharge := BatteryFirstGridChargeDisabled
		soc := 10.0 // default load-first SoC
		TargetSocFromOptimisation := row.SOCOpt()
		if row.UnitLoadCost() == minPrice || row.UnitLoadCost() == 0 {
			// Inside cheap block
			workMode = WorkModeBatteryFirst
			gridCharge = BatteryFirstGridChargeEnabled
			tSoc := monotonicSOC[i]
			// if its not the last index
			if i != len(rows)-1 {
				// if the next row is higher than this row
				// set the target soc of this hour to reach that target soc
				// rows are the bottom of the hour eg 16:00:00 70% soc, so 15:00:00 should set target soc to 70%
				nextRowSocOpt := rows[i+1].SOCOpt() * 100
				//nextRowUnitLoadCost := rows[i+1].UnitLoadCost
				if nextRowSocOpt > tSoc {
					tSoc = nextRowSocOpt
				}
			}
			// if its equal to 10, which is the minimum, then no point setting charge vars
			if tSoc == soc {
				workMode = WorkModeLoadFirst
				gridCharge = BatteryFirstGridChargeDisabled
			}
			soc = tSoc // ← monotonic enforced target
		}
		if workMode == WorkModeBatteryFirst {
			TargetSocFromOptimisation = soc / 100
		}
		commands = append(commands, Schedule{
			time:                  row.Time(),
			workmode:              workMode,
			soc:                   soc,
			chargeBatteryFromGrid: gridCharge,
			TargetSoc:             TargetSocFromOptimisation,
		})
	}

	return commands
}

func GenerateSchedules(ctx context.Context, logger *slog.Logger, db store.Store, t time.Time, method string) ([]Schedule, error) {
	rows, err := db.SelectOptimization(ctx, t, method)
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		logger.Info("No rows parsed.")
		return nil, nil
	}
	schedules := GetCommands(rows)
	var lastScheduledTime time.Time
	for _, schedule := range schedules {
		logger.Info(schedule.time.Format(time.RFC3339), slog.String("work mode", schedule.workmode), slog.String("battery first gridcharge", schedule.chargeBatteryFromGrid))
		if schedule.time.After(lastScheduledTime) {
			lastScheduledTime = schedule.time
		}
		if err = db.Upsert(ctx, store.Row{
			Optimization:     method,
			Time:             schedule.time,
			WorkMode:         schedule.workmode,
			GridCharge:       schedule.chargeBatteryFromGrid,
			StopDischargeSoc: schedule.soc,
			TargetSoc:        schedule.TargetSoc,
		}); err != nil {
			return nil, err
		}
	}
	return schedules, nil
}
