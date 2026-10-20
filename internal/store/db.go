package store

import (
	"context"
	"database/sql"
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
	db       *sql.DB
	postgres *sqlc.Queries
	logger   *slog.Logger
}

func (p PostgresStore) Delete(ctx context.Context, row Row) error {
	return p.postgres.DeleteSchedule(ctx, row.Time)
}

func (p PostgresStore) DeleteAnyOptimization(start time.Time) error {
	query := `
delete from optimization_results where time = $1;
`
	_, err := p.db.Exec(
		query,
		start,
	)
	return err
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
	return p.postgres.InsertSchedule(ctx, row.InsertScheduleParams)
}

func (p PostgresStore) Find(t time.Time) (Row, error) {
	var row Row

	query := `
SELECT
	optimization,
	time,
	work_mode,
	grid_charge,
	stop_discharge_soc,
	target_soc
FROM schedule
WHERE time = $1;
`

	err := p.db.QueryRow(query, t).Scan(
		&row.Optimization,
		&row.Time,
		&row.WorkMode,
		&row.GridCharge,
		&row.StopDischargeSoc,
		&row.TargetSoc,
	)
	if err != nil {
		return Row{}, err
	}
	return row, nil
}

func (p PostgresStore) SelectOptimization(start time.Time, optimization string) ([]types.OptimizationResult, error) {
	query := `
SELECT
	time, p_pv, p_batt, soc_opt, unit_load_cost
FROM optimization_results
WHERE time >= $1 AND optimization = $2
ORDER BY time ASC;
`
	rows, err := p.db.Query(query, start, optimization)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err = rows.Close(); err != nil {
			p.logger.Error(err.Error())
		}
	}()

	var result []types.OptimizationResult

	for rows.Next() {
		var t time.Time
		var pPV, pBatt, socOpt, unitLoadCost float64
		if err = rows.Scan(
			&t,
			&pPV,
			&pBatt,
			&socOpt,
			&unitLoadCost,
		); err != nil {
			return nil, err
		}
		result = append(result, types.NewOptimizationResult(t, socOpt, unitLoadCost, pPV, pBatt))
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (p PostgresStore) SetActualSoc(socTime time.Time, soc float64) error {
	const query = `
		UPDATE optimization_results
		SET a_soc = $1
		WHERE time = $2;
	`
	res, err := p.db.Exec(query, soc, socTime)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		return ErrNotFound
	}
	return nil
}

func (p PostgresStore) Select(start time.Time) ([]Row, error) {
	query := `
SELECT
	optimization,
	time,
	work_mode,
	grid_charge,
	stop_discharge_soc,
	target_soc
FROM schedule
WHERE time >= $1
ORDER BY time ASC;
`

	rows, err := p.db.Query(query, start)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err = rows.Close(); err != nil {
			p.logger.Error(err.Error())
		}
	}()

	var result []Row

	for rows.Next() {
		var r Row
		if err = rows.Scan(
			&r.Optimization,
			&r.Time,
			&r.WorkMode,
			&r.GridCharge,
			&r.StopDischargeSoc,
			&r.TargetSoc,
		); err != nil {
			return nil, err
		}
		result = append(result, r)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (p PostgresStore) Upsert(ctx context.Context, row Row) error {
	return p.Insert(ctx, row)
}

func (p PostgresStore) Close() error {
	return p.db.Close()
}

func (p PostgresStore) setField(state, field string, value any) error {
	query := `
		UPDATE inverter_settings
        SET ` + field + ` = $2
        WHERE state = $1;
`
	_, err := p.db.Exec(query, state, value)
	return err
}

func (p PostgresStore) getField(state, field string, dest any) error {
	query := `
		SELECT ` + field + `
		FROM inverter_settings
		WHERE state = $1
	`
	err := p.db.QueryRow(query, state).Scan(dest)
	if err != nil {
		return err
	}
	return nil
}

func (p PostgresStore) SetBatteryFirstGridCharge(enabled string) error {
	return p.setField(StateCurrent, "grid_charge", enabled)
}

func (p PostgresStore) SetSOC(soc int64) error {
	return p.setField(StateCurrent, "soc", soc)
}

func (p PostgresStore) SetDeviceMode(mode string) error {
	return p.setField(StateCurrent, "device_mode", mode)

}

func (p PostgresStore) GetBatteryFirstGridCharge() (string, error) {
	var v string
	err := p.getField(StateCurrent, "grid_charge", &v)
	if err != nil {
		return "", err
	}
	return v, err
}

func (p PostgresStore) GetSOC() (int64, error) {
	var v int64
	err := p.getField(StateCurrent, "soc", &v)
	if err != nil {
		return 0, err
	}
	return v, nil
}

func (p PostgresStore) GetDeviceMode() (string, error) {
	var v string
	err := p.getField(StateCurrent, "device_mode", &v)
	if err != nil {
		return "", err
	}
	return v, nil
}

func (p PostgresStore) SetBatteryFirstGridChargeTarget(enabled string) error {
	return p.setField(StateTarget, "grid_charge", enabled)
}

func (p PostgresStore) SetSOCTarget(soc int64) error {
	if err := p.setField(StateTarget, "soc", soc); err != nil {
		return err
	}
	// set the load_first_stop_discharge to same as target SoC
	// this is because we want to charge to the target soc
	return p.setField(StateTarget, "load_first_stop_discharge", soc)
}

func (p PostgresStore) SetDeviceModeTarget(mode string) error {
	return p.setField(StateTarget, "device_mode", mode)
}

func (p PostgresStore) GetBatteryFirstGridChargeTarget() (string, error) {
	var v string
	err := p.getField(StateTarget, "grid_charge", &v)
	if err != nil {
		return "", err
	}
	return v, err
}

func (p PostgresStore) GetSOCTarget() (int64, error) {
	var v int64
	err := p.getField(StateTarget, "soc", &v)
	if err != nil {
		return 0, err
	}
	return v, nil
}

func (p PostgresStore) GetDeviceModeTarget() (string, error) {
	var v string
	err := p.getField(StateTarget, "device_mode", &v)
	if err != nil {
		return "", err
	}
	return v, nil
}

func (p PostgresStore) GetLoadFirstStopDischarge() (int64, error) {
	var v int64
	err := p.getField(StateCurrent, "load_first_stop_discharge", &v)
	if err != nil {
		return v, err
	}
	return v, nil
}

func (p PostgresStore) SetLoadFirstStopDischarge(soc int64) error {
	return p.setField(StateCurrent, "load_first_stop_discharge", soc)
}
