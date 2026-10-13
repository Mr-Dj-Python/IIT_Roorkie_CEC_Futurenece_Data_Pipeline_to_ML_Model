-- elt_12_stg_stores.sql
-- STAGING stores.

CREATE TABLE IF NOT EXISTS stg_duplicate_rows (
    source_file VARCHAR, row_key VARCHAR, _row_id BIGINT, failure_reason VARCHAR, warning_reason VARCHAR,
    _run_id VARCHAR, _loaded_at TIMESTAMP);

CREATE OR REPLACE TEMP TABLE _chk_stores AS
WITH src AS (
    SELECT coalesce(store_id, '') AS store_id, coalesce(store_name, '') AS store_name, coalesce(zone, '') AS zone,
           coalesce(area, '') AS area, coalesce(pincode, '') AS pincode, coalesce(open_date, '') AS open_date,
           coalesce(manager_name, '') AS manager_name,
           coalesce(capacity_orders_per_day, '') AS capacity_orders_per_day,
           _loaded_at, _source_file, _run_id, _row_id
    FROM raw_stores WHERE _run_id = $run_id
),
parsed AS (
    SELECT *, num(capacity_orders_per_day) AS capacity_n, dt(open_date) AS open_d,
           row_number() OVER (PARTITION BY CASE WHEN blank(store_id) THEN '#' || _row_id ELSE store_id END
                              ORDER BY _row_id) AS rn
    FROM src
)
SELECT *,
    reasons(list_value(
        flag(blank(zone), 'zone_not_null'),
        flag(rn > 1, 'store_id_unique'),
        flag(NOT blank(zone) AND zone NOT IN ('North', 'South', 'East', 'West'), 'zone_valid'),
        flag(open_d::TIMESTAMP > $as_of::TIMESTAMP, 'open_date_not_future'))) AS failure_reason,
    reasons(list_value(
        flag(NOT regexp_matches(trim(pincode), '^\d{6}$'), 'pincode_6_digits'),
        flag(capacity_n <= 0, 'capacity_positive'))) AS warning_reason
FROM parsed;

CREATE OR REPLACE TABLE stg_stores AS
SELECT nullif(store_id, '') AS store_id,
       trim(store_name) AS store_name,
       nullif(trim(zone), '') AS zone,
       trim(area) AS area,
       trim(pincode) AS pincode,
       open_d AS open_date,
       trim(manager_name) AS manager_name,
       capacity_n::INTEGER AS capacity_orders_per_day,
       CASE WHEN failure_reason = '' THEN 'PASSED' ELSE 'FAILED' END AS dq_status,
       failure_reason, warning_reason, _source_file, _run_id, _row_id, _loaded_at
FROM _chk_stores WHERE rn = 1;

DELETE FROM stg_duplicate_rows WHERE _run_id = $run_id AND source_file = $stores_file;
INSERT INTO stg_duplicate_rows
SELECT _source_file, store_id, _row_id, failure_reason, warning_reason, _run_id, _loaded_at
FROM _chk_stores WHERE rn > 1;
