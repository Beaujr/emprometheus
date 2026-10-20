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

-- -- name: InsertIP :one
-- INSERT INTO ip_addresses (ip_address, lat, lng, city, country)
-- VALUES (?, ?, ?, ?, ?)
--     ON CONFLICT(ip_address) DO UPDATE SET
--     lat = COALESCE(excluded.lat, ip_addresses.lat),
--                                    lng = COALESCE(excluded.lng, ip_addresses.lng)
--                                    RETURNING id;
--
-- -- name: InsertLog :exec
-- INSERT INTO request_logs (ip_id, host_id, service, status_code, timestamp)
-- VALUES (?, ?, ?, ?, ?);
--
-- -- name: GetAbusiveIPs :many
-- SELECT
--     ip.ip_address,
--     COUNT(log.id) AS total_requests
-- FROM request_logs log
--          JOIN ip_addresses ip ON log.ip_id = ip.id
-- WHERE log.timestamp >= datetime('now', ?) -- dynamic interval string like '-10 seconds'
-- GROUP BY ip.ip_address
-- HAVING COUNT(log.id) > ?;
--
-- -- name: GetLocationByIP :one
-- SELECT lat, lng, country, city
-- FROM ip_addresses
-- WHERE ip_address = ?;
--
-- -- name: GetPrometheusMetrics :many
-- SELECT
--     ip.ip_address,
--     ip.lat,
--     ip.lng,
--     ip.city,
--     ip.country,
--     log.service,
--     h.host_name,
--     log.status_code,
--     COUNT(log.id) AS total_hits
-- FROM request_logs log
--          JOIN ip_addresses ip ON log.ip_id = ip.id
--          JOIN hosts h ON log.host_id = h.id
-- WHERE log.timestamp >= datetime('now', ?) -- e.g., '-45 seconds' to give a safe buffer
-- GROUP BY ip.lat, ip.lng, ip.city, h.host_name, log.status_code;
--
-- -- name: UpdateCityAndCountry :exec
-- UPDATE ip_addresses
-- SET city = ?,
--     country = ?
-- WHERE ip_address = ?;
--
-- -- name: GetRequestHitsByTimeRange :many
-- SELECT
--     ip.ip_address,
--     ip.lat,
--     ip.lng,
--     ip.city,
--     ip.country,
--     log.service,
--     h.host_name,
--     log.status_code,
--     CAST(strftime('%s', log.timestamp) AS INTEGER) / 60 * 60 AS time_bucket,
--     COUNT(log.id) AS total_hits
-- FROM request_logs log
--          JOIN ip_addresses ip ON log.ip_id = ip.id
--          JOIN hosts h ON log.host_id = h.id
-- WHERE log.timestamp >= sqlc.arg('from')
--   AND log.timestamp <= sqlc.arg('to')
-- GROUP BY
--     ip.ip_address,
--     ip.lat,
--     ip.lng,
--     ip.city,
--     ip.country,
--     log.service,
--     h.host_name,
--     log.status_code,
--     time_bucket
-- ORDER BY time_bucket;
