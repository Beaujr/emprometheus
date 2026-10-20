-- name: CountInverterSettings :one
SELECT count(*)
FROM inverter_settings;


-- name: InsertInverterSetting :exec
INSERT INTO inverter_settings (soc, device_mode, grid_charge, state)
VALUES ($1, $2, $3, $4);

-- name: InsertSchedule :exec
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

-- name: DeleteSchedule :exec
delete from schedule where time = $1;

-- name: InsertOptimization :exec
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

-- name: GetScheduleByTime :one
SELECT
    optimization,
    time,
    work_mode,
    grid_charge,
    stop_discharge_soc,
    target_soc
FROM schedule
WHERE time = $1;

-- name: DeleteOptimizationResultAtTime :exec
delete from optimization_results where time = $1;

-- name: SelectOptimizationByTimeAndOptimization :many
SELECT
    time, p_pv, p_batt, soc_opt, unit_load_cost
FROM optimization_results
WHERE time >= $1 AND optimization = $2
ORDER BY time ASC;

-- name: SetActualSocAtTime :exec
UPDATE optimization_results
SET a_soc = $1
WHERE time = $2;

-- name: GetSchedulesAfterTime :many
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

-- name: SetSOC :exec
UPDATE inverter_settings
SET soc = $1
WHERE state = $2;

-- name: GetSOC :one
SELECT soc
FROM inverter_settings
WHERE state = $1
    LIMIT 1;

-- name: SetDeviceMode :exec
UPDATE inverter_settings
SET device_mode = $1
WHERE state = $2;

-- name: GetDeviceMode :one
SELECT device_mode
FROM inverter_settings
WHERE state = $1
    LIMIT 1;

-- name: SetGridCharge :exec
UPDATE inverter_settings
SET grid_charge = $1
WHERE state = $2;

-- name: GetGridCharge :one
SELECT grid_charge
FROM inverter_settings
WHERE state = $1
    LIMIT 1;

-- name: GetLoadFirstStopDischarge :one
SELECT load_first_stop_discharge
FROM inverter_settings
WHERE state = $1
    LIMIT 1;

-- name: SetLoadFirstStopDischarge :exec
UPDATE inverter_settings
SET load_first_stop_discharge = $1
WHERE state = $2;