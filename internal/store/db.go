package store

import (
	"context"
	"log/slog"
	"time"

	"github.com/beaujr/emprometheus/internal/store/sqlc"
	"github.com/beaujr/emprometheus/internal/types"

	_ "github.com/lib/pq"
)

const (
	StateCurrent = "current"
	StateTarget  = "target"
)

type PostgresStore struct {
	postgres *sqlc.Queries
	logger   *slog.Logger
}

func (p PostgresStore) Delete(ctx context.Context, row Row) error {
	return p.postgres.DeleteSchedule(ctx, row.Time)
}

func (p PostgresStore) DeleteAnyOptimization(ctx context.Context, start time.Time) error {
	return p.postgres.DeleteOptimizationResultAtTime(ctx, start)
}

func (p PostgresStore) InsertOptimization(ctx context.Context, r types.OptimizationResult) error {
	err := p.postgres.InsertOptimization(ctx, sqlc.InsertOptimizationParams{
		Optimization:  r.Optimization,
		Time:          r.Time(),
		PPv:           r.PPV(),
		PLoad:         r.PLoad(),
		PGridPos:      r.PGridPos(),
		PGridNeg:      r.PGridNeg(),
		PGrid:         r.PGrid(),
		PBatt:         r.PBatt(),
		SocOpt:        r.SOCOpt(),
		UnitLoadCost:  r.UnitLoadCost(),
		UnitProdPrice: r.UnitProdPrice(),
		CostProfit:    r.CostProfit(),
		CostFunProfit: r.CostFunProfit(),
		OptimStatus:   r.OptimStatus(),
	})
	return err

}

func (p PostgresStore) checkDB(ctx context.Context) error {
	count, err := p.postgres.CountInverterSettings(ctx)
	if err != nil {
		return err
	}
	if count == 0 {
		inserts := []string{StateCurrent, StateTarget}
		for _, i := range inserts {
			err = p.postgres.InsertInverterSetting(ctx, sqlc.InsertInverterSettingParams{
				Soc:        10,
				DeviceMode: "Load First",
				GridCharge: "Disabled",
				State:      i,
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (p PostgresStore) Insert(ctx context.Context, row Row) error {
	return p.postgres.InsertSchedule(ctx, row.toInsert())
}

func (p PostgresStore) Find(ctx context.Context, t time.Time) (Row, error) {
	row, err := p.postgres.GetScheduleByTime(ctx, t)
	if err != nil {
		return Row{}, err
	}
	return Row{
		Optimization:     row.Optimization,
		Time:             row.Time,
		WorkMode:         row.WorkMode,
		GridCharge:       row.GridCharge,
		StopDischargeSoc: row.StopDischargeSoc,
		TargetSoc:        row.TargetSoc,
	}, nil
}

func (p PostgresStore) SelectOptimization(ctx context.Context, start time.Time, optimization string) ([]types.OptimizationResult, error) {
	rows, err := p.postgres.SelectOptimizationByTimeAndOptimization(ctx, sqlc.SelectOptimizationByTimeAndOptimizationParams{
		Time:         start,
		Optimization: optimization,
	})
	if err != nil {
		return nil, err
	}
	var result []types.OptimizationResult
	for _, r := range rows {
		result = append(result, types.NewOptimizationResult(r.Time, r.SocOpt, r.UnitLoadCost, r.PPv, r.PBatt))
	}
	return result, nil
}

func (p PostgresStore) SetActualSoc(ctx context.Context, socTime time.Time, soc float64) error {
	return p.postgres.SetActualSocAtTime(ctx, sqlc.SetActualSocAtTimeParams{
		ASoc: soc,
		Time: socTime,
	})
}

func (p PostgresStore) Select(ctx context.Context, start time.Time) ([]Row, error) {
	rows, err := p.postgres.GetSchedulesAfterTime(ctx, start)
	if err != nil {
		return nil, err
	}
	var result []Row
	for _, row := range rows {
		result = append(result, Row{
			Optimization:     row.Optimization,
			Time:             row.Time,
			WorkMode:         row.WorkMode,
			GridCharge:       row.GridCharge,
			StopDischargeSoc: row.StopDischargeSoc,
			TargetSoc:        row.TargetSoc,
		})
	}
	return result, nil
}

func (p PostgresStore) Upsert(ctx context.Context, row Row) error {
	return p.Insert(ctx, row)
}

func (p PostgresStore) Close() error {
	return nil
}

func (p PostgresStore) SetBatteryFirstGridCharge(ctx context.Context, enabled string) error {
	return p.postgres.SetGridCharge(ctx, sqlc.SetGridChargeParams{GridCharge: enabled, State: StateCurrent})
}

func (p PostgresStore) SetSOC(ctx context.Context, soc int64) error {
	return p.postgres.SetSOC(ctx, sqlc.SetSOCParams{Soc: soc, State: StateCurrent})
}

func (p PostgresStore) SetDeviceMode(ctx context.Context, mode string) error {
	return p.postgres.SetDeviceMode(ctx, sqlc.SetDeviceModeParams{DeviceMode: mode, State: StateCurrent})

}

func (p PostgresStore) GetBatteryFirstGridCharge(ctx context.Context) (string, error) {
	return p.postgres.GetGridCharge(ctx, StateCurrent)
}

func (p PostgresStore) GetSOC(ctx context.Context) (int64, error) {
	return p.postgres.GetSOC(ctx, StateCurrent)
}

func (p PostgresStore) GetDeviceMode(ctx context.Context) (string, error) {
	return p.postgres.GetDeviceMode(ctx, StateCurrent)

}

func (p PostgresStore) SetBatteryFirstGridChargeTarget(ctx context.Context, enabled string) error {
	return p.postgres.SetGridCharge(ctx, sqlc.SetGridChargeParams{GridCharge: enabled, State: StateTarget})
}

func (p PostgresStore) SetSOCTarget(ctx context.Context, soc int64) error {
	if err := p.postgres.SetSOC(ctx, sqlc.SetSOCParams{Soc: soc, State: StateTarget}); err != nil {
		return err
	}
	// set the load_first_stop_discharge to same as target SoC
	// this is because we want to charge to the target soc
	return p.postgres.SetLoadFirstStopDischarge(ctx, sqlc.SetLoadFirstStopDischargeParams{State: StateTarget, LoadFirstStopDischarge: soc})
}

func (p PostgresStore) SetDeviceModeTarget(ctx context.Context, mode string) error {
	return p.postgres.SetDeviceMode(ctx, sqlc.SetDeviceModeParams{State: StateTarget, DeviceMode: mode})
}

func (p PostgresStore) GetBatteryFirstGridChargeTarget(ctx context.Context) (string, error) {
	return p.postgres.GetGridCharge(ctx, StateTarget)
}

func (p PostgresStore) GetSOCTarget(ctx context.Context) (int64, error) {
	return p.postgres.GetSOC(ctx, StateTarget)

}

func (p PostgresStore) GetDeviceModeTarget(ctx context.Context) (string, error) {
	return p.postgres.GetDeviceMode(ctx, StateTarget)
}

func (p PostgresStore) GetLoadFirstStopDischarge(ctx context.Context) (int64, error) {
	return p.postgres.GetLoadFirstStopDischarge(ctx, StateCurrent)
}

func (p PostgresStore) SetLoadFirstStopDischarge(ctx context.Context, soc int64) error {
	return p.postgres.SetLoadFirstStopDischarge(ctx, sqlc.SetLoadFirstStopDischargeParams{LoadFirstStopDischarge: soc, State: StateCurrent})
}
