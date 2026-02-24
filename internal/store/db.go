package store

import (
	"database/sql"
	_ "github.com/lib/pq"
	"time"
)

type PostgresStore struct {
	db *sql.DB
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

func newPostgresStore(dsn string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	p := &PostgresStore{db: db}
	if err = p.migrations(); err != nil {
		return nil, err
	}
	return p, nil
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
	migrations = append(migrations, createOptimizationTable, createTable, actualSocColumn)
	for _, migration := range migrations {
		if _, err := p.db.Exec(migration); err != nil {
			return err
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
