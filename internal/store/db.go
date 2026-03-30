package store

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

const (
	StateCurrent = "current"
	StateTarget  = "target"
)

type PostgresStore struct {
	db *sql.DB
}

func (p PostgresStore) Delete(row Row) error {
	query := `
delete from schedule where time = $1;
`
	_, err := p.db.Exec(
		query,
		row.Time,
	)
	return err
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

func (p PostgresStore) InsertOptimization(r OptimizationResult) error {
	query := `
INSERT INTO optimization_results (
	optimization,
	time,
	p_pv,
	p_load,
	p_grid_pos,
	p_grid_neg,
	p_grid,
	p_batt,
	soc_opt,
	unit_load_cost,
	unit_prod_price,
	cost_profit,
	cost_fun_profit,
	optim_status
)
VALUES (
	$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14
)
ON CONFLICT (optimization, time) DO UPDATE SET
	p_pv = EXCLUDED.p_pv,
	p_load = EXCLUDED.p_load,
	p_grid_pos = EXCLUDED.p_grid_pos,
	p_grid_neg = EXCLUDED.p_grid_neg,
	p_grid = EXCLUDED.p_grid,
	p_batt = EXCLUDED.p_batt,
	soc_opt = EXCLUDED.soc_opt,
	unit_load_cost = EXCLUDED.unit_load_cost,
	unit_prod_price = EXCLUDED.unit_prod_price,
	cost_profit = EXCLUDED.cost_profit,
	cost_fun_profit = EXCLUDED.cost_fun_profit,
	optim_status = EXCLUDED.optim_status;
`
	_, err := p.db.Exec(
		query,
		r.Optimization,
		r.Time,
		r.PPV,
		r.PLoad,
		r.PGridPos,
		r.PGridNeg,
		r.PGrid,
		r.PBatt,
		r.SOCOpt,
		r.UnitLoadCost,
		r.UnitProdPrice,
		r.CostProfit,
		r.CostFunProfit,
		r.OptimStatus,
	)
	return err

}

func (p PostgresStore) migrations() error {
	var migrations []string
	createTable := `
	CREATE TABLE IF NOT EXISTS schedule (
		time TIMESTAMPTZ PRIMARY KEY,
		optimization TEXT NOT NULL,
        work_mode TEXT NOT NULL,
		grid_charge TEXT NOT NULL,
		stop_discharge_soc DOUBLE PRECISION NOT NULL,
		target_soc DOUBLE PRECISION NOT NULL
	);
	`
	createOptimizationTable := `
	CREATE TABLE IF NOT EXISTS optimization_results (
		optimization TEXT NOT NULL,
		time TIMESTAMPTZ NOT NULL,
		p_pv DOUBLE PRECISION NOT NULL,
		p_load DOUBLE PRECISION NOT NULL,
		p_grid_pos DOUBLE PRECISION NOT NULL,
		p_grid_neg DOUBLE PRECISION NOT NULL,
		p_grid DOUBLE PRECISION NOT NULL,
		p_batt DOUBLE PRECISION NOT NULL,
		soc_opt DOUBLE PRECISION NOT NULL,
		unit_load_cost DOUBLE PRECISION NOT NULL,
		unit_prod_price DOUBLE PRECISION NOT NULL,
		cost_profit DOUBLE PRECISION NOT NULL,
		cost_fun_profit DOUBLE PRECISION NOT NULL,
		optim_status TEXT NOT NULL,
		PRIMARY KEY (optimization, time)
	);
	`
	actualSocColumn := `
	ALTER TABLE optimization_results
	ADD COLUMN IF NOT EXISTS a_soc DOUBLE PRECISION NOT NULL DEFAULT 0;`

	stateTable := `
	CREATE TABLE IF NOT EXISTS inverter_settings (
		soc BIGINT NOT NULL,
		device_mode TEXT NOT NULL,
		grid_charge TEXT NOT NULL,
		state TEXT NOT NULL,
		PRIMARY KEY (state)
	);
`
	migrations = append(migrations, createOptimizationTable, createTable, actualSocColumn, stateTable)
	for _, migration := range migrations {
		if _, err := p.db.Exec(migration); err != nil {
			return err
		}
	}
	settingsQuery := `
		SELECT count(*)
		FROM inverter_settings;
	`
	count := 0
	err := p.db.QueryRow(settingsQuery).Scan(&count)
	if err != nil {
		return err
	}
	if count == 0 {
		inserts := []string{StateCurrent, StateTarget}
		for _, i := range inserts {
			q := fmt.Sprintf("insert into inverter_settings (soc, device_mode, grid_charge, state) values (10, 'Load First', 'Disabled', '%s');", i)
			if _, err := p.db.Exec(q); err != nil {
				return err
			}
		}

	}
	return nil

}

func (p PostgresStore) Insert(row Row) error {
	insert := `
INSERT INTO schedule (
	optimization,
    time,
	work_mode,
	grid_charge,
	stop_discharge_soc,
	target_soc
)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (time) DO UPDATE SET
    optimization = EXCLUDED.optimization,
	work_mode = EXCLUDED.work_mode,
	grid_charge = EXCLUDED.grid_charge,
	stop_discharge_soc = EXCLUDED.stop_discharge_soc,
	target_soc = EXCLUDED.target_soc;
`
	_, err := p.db.Exec(insert,
		row.Optimization,
		row.Time,
		row.WorkMode,
		row.GridCharge,
		row.StopDischargeSOC,
		row.TargetSOC,
	)
	if err != nil {
		return err
	}
	return nil
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
		&row.StopDischargeSOC,
		&row.TargetSOC,
	)
	if err != nil {
		return Row{}, err
	}
	return row, nil
}

func (p PostgresStore) SelectOptimization(start time.Time, optimization string) ([]OptimizationResult, error) {
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
	defer rows.Close()

	var result []OptimizationResult

	for rows.Next() {
		var r OptimizationResult
		if err = rows.Scan(
			&r.Time,
			&r.PPV,
			&r.PBatt,
			&r.SOCOpt,
			&r.UnitLoadCost,
		); err != nil {
			return nil, err
		}
		result = append(result, r)
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
	defer rows.Close()

	var result []Row

	for rows.Next() {
		var r Row
		if err := rows.Scan(
			&r.Optimization,
			&r.Time,
			&r.WorkMode,
			&r.GridCharge,
			&r.StopDischargeSOC,
			&r.TargetSOC,
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

func (p PostgresStore) Upsert(row Row) error {
	return p.Insert(row)
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
	//queryCounter.Add(context.Background(), 1, api.WithAttributes(attribute.Key("action").String(fmt.Sprintf("SetField%s%s", field, value))))
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
	//queryCounter.Add(context.Background(), 1, api.WithAttributes(attribute.Key("action").String(fmt.Sprintf("GetField%s%s", field, dest))))
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
	err := p.setField(StateTarget, "grid_charge", enabled)
	if err != nil {
		return err
	}
	return nil
}

func (p PostgresStore) SetSOCTarget(soc int64) error {
	err := p.setField(StateTarget, "soc", soc)
	if err != nil {
		return err
	}
	return nil
}

func (p PostgresStore) SetDeviceModeTarget(mode string) error {
	err := p.setField(StateTarget, "device_mode", mode)
	if err != nil {
		return err
	}
	return nil
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
		return "", nil
	}
	return v, nil
}
