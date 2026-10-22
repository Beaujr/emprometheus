-- migrate:up
CREATE OR REPLACE VIEW battery_grid_charge_periods AS
WITH filtered AS (
    SELECT schedule."time"
    FROM schedule
    WHERE schedule.work_mode = 'Battery first'
      AND schedule.grid_charge = 'Enabled'
),
ordered AS (
    SELECT
        "time",
        LAG("time") OVER (ORDER BY "time") AS previous_time
    FROM filtered
),
grouped AS (
    SELECT
        "time",
        SUM(
            CASE
                WHEN previous_time IS NULL
                  OR "time" - previous_time <> INTERVAL '30 minutes'
                THEN 1
                ELSE 0
            END
        ) OVER (ORDER BY "time") AS grp
    FROM ordered
)
SELECT
    MIN("time") AS "time",
    MAX("time") + INTERVAL '30 minutes' AS "timeEnd",
    'Battery first + Grid charge enabled'::text AS text
FROM grouped
GROUP BY grp
ORDER BY MIN("time") DESC;

-- migrate:down
DROP VIEW IF EXISTS battery_grid_charge_periods;
