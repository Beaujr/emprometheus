-- migrate:up
CREATE TABLE IF NOT EXISTS schedule (
                                        time TIMESTAMPTZ PRIMARY KEY,
                                        optimization TEXT NOT NULL,
                                        work_mode TEXT NOT NULL,
                                        grid_charge TEXT NOT NULL,
                                        stop_discharge_soc DOUBLE PRECISION NOT NULL,
                                        target_soc DOUBLE PRECISION NOT NULL
);

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
                                                    a_soc DOUBLE PRECISION NOT NULL DEFAULT 0,
                                                    PRIMARY KEY (optimization, time)
);

CREATE TABLE IF NOT EXISTS inverter_settings (
                                                 soc BIGINT NOT NULL,
                                                 device_mode TEXT NOT NULL,
                                                 grid_charge TEXT NOT NULL,
                                                 state TEXT NOT NULL,
                                                 load_first_stop_discharge BIGINT NOT NULL DEFAULT 10,
                                                 PRIMARY KEY (state)
);

-- migrate:down
DROP TABLE IF EXISTS schedule;
DROP TABLE IF EXISTS optimization_results;
DROP TABLE IF EXISTS inverter_settings;

