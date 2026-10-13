-- elt_31_dq_results.sql
-- Scorecard: one row per check per run in dq_results (shared with the ETL DAG, told apart by dag_id).
-- Failure counts come from the failure_reason / warning_reason columns written by staging, plus
-- stg_duplicate_rows for the extra copies of repeated keys, so no rejected row is missed.

CREATE TABLE IF NOT EXISTS dq_results (
    run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR, severity VARCHAR,
    rows_checked BIGINT, rows_failed BIGINT, status VARCHAR, checked_at TIMESTAMP);

CREATE OR REPLACE TEMP TABLE _dq_rows AS
SELECT 'orders' AS file, failure_reason, warning_reason FROM stg_orders WHERE _run_id = $run_id
UNION ALL SELECT 'orders', failure_reason, warning_reason FROM stg_duplicate_rows WHERE _run_id = $run_id AND source_file = $orders_file
UNION ALL SELECT 'payments', failure_reason, warning_reason FROM stg_payments
UNION ALL SELECT 'payments', failure_reason, warning_reason FROM stg_duplicate_rows WHERE _run_id = $run_id AND source_file = $payments_file
UNION ALL SELECT 'customers', failure_reason, warning_reason FROM stg_customers
UNION ALL SELECT 'customers', failure_reason, warning_reason FROM stg_duplicate_rows WHERE _run_id = $run_id AND source_file = $customers_file
UNION ALL SELECT 'products', failure_reason, warning_reason FROM stg_products
UNION ALL SELECT 'products', failure_reason, warning_reason FROM stg_duplicate_rows WHERE _run_id = $run_id AND source_file = $products_file
UNION ALL SELECT 'stores', failure_reason, warning_reason FROM stg_stores
UNION ALL SELECT 'stores', failure_reason, warning_reason FROM stg_duplicate_rows WHERE _run_id = $run_id AND source_file = $stores_file;

CREATE OR REPLACE TEMP TABLE _dq_totals AS
SELECT file, count(*) AS rows_checked FROM _dq_rows GROUP BY file;

CREATE OR REPLACE TEMP TABLE _dq_failures AS
SELECT file, check_name, count(*) AS rows_failed
FROM (SELECT file, unnest(string_split(failure_reason || ';' || warning_reason, ';')) AS check_name FROM _dq_rows)
WHERE check_name <> ''
GROUP BY file, check_name;

DELETE FROM dq_results WHERE run_id = $run_id AND dag_id = $dag_id;

INSERT INTO dq_results
SELECT $run_id, $dag_id, c.file || '.' || c.check_name, c.dimension, c.severity,
       t.rows_checked, coalesce(f.rows_failed, 0),
       CASE WHEN coalesce(f.rows_failed, 0) = 0 THEN 'PASS' WHEN c.severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END,
       $loaded_at::TIMESTAMP
FROM dq_check_catalog c
JOIN _dq_totals t ON t.file = c.file
LEFT JOIN _dq_failures f ON f.file = c.file AND f.check_name = c.check_name
WHERE c.check_name NOT IN ('latest_order_not_stale', 'volume_drop_vs_history');

-- Timeliness (Warning): the newest non-future order must be less than a day old at run time
INSERT INTO dq_results
SELECT $run_id, $dag_id, 'orders.latest_order_not_stale', 'Timeliness', 'Warning', rows_checked,
       CASE WHEN stale THEN rows_checked ELSE 0 END, CASE WHEN stale THEN 'WARN' ELSE 'PASS' END,
       $loaded_at::TIMESTAMP
FROM (
    SELECT (SELECT rows_checked FROM _dq_totals WHERE file = 'orders') AS rows_checked,
           coalesce($as_of::TIMESTAMP - max(order_ts) FILTER (WHERE order_ts <= $as_of::TIMESTAMP) > INTERVAL 1 DAY, true) AS stale
    FROM stg_orders WHERE _run_id = $run_id
);

-- Completeness (Warning): a full orders file with over 90% fewer rows than the largest earlier full file.
-- Increment files are small by design and are skipped. The baseline is the MAXIMUM of earlier runs, so one
-- anomalous run cannot lower the bar for the next.
INSERT INTO dq_results
SELECT $run_id, $dag_id, 'orders.volume_drop_vs_history', 'Completeness', 'Warning', rows_checked,
       CASE WHEN dropped THEN rows_checked ELSE 0 END, CASE WHEN dropped THEN 'WARN' ELSE 'PASS' END,
       $loaded_at::TIMESTAMP
FROM (
    SELECT rows_checked,
           coalesce(NOT lower($orders_file) LIKE '%increment%' AND baseline IS NOT NULL AND rows_checked < 0.1 * baseline,
                    false) AS dropped
    FROM (
        SELECT (SELECT rows_checked FROM _dq_totals WHERE file = 'orders') AS rows_checked,
               (SELECT max(n) FROM (SELECT count(*) AS n FROM raw_orders
                                    WHERE _run_id <> $run_id AND lower(_source_file) NOT LIKE '%increment%'
                                    GROUP BY _run_id)) AS baseline
    )
);
