-- elt_14_stg_payments.sql
-- STAGING payments, rebuilt from this run's snapshot. One row per payment_id.

CREATE TABLE IF NOT EXISTS stg_duplicate_rows (
    source_file VARCHAR, row_key VARCHAR, _row_id BIGINT, failure_reason VARCHAR, warning_reason VARCHAR,
    _run_id VARCHAR, _loaded_at TIMESTAMP);

CREATE OR REPLACE TEMP TABLE _chk_payments AS
WITH src AS (
    SELECT coalesce(payment_id, '') AS payment_id, coalesce(order_id, '') AS order_id,
           coalesce(payment_ts, '') AS payment_ts, coalesce(payment_mode, '') AS payment_mode,
           coalesce(amount_paid, '') AS amount_paid, coalesce(payment_status, '') AS payment_status,
           coalesce(gateway_ref, '') AS gateway_ref, _loaded_at, _source_file, _run_id, _row_id
    FROM raw_payments WHERE _run_id = $run_id
),
parsed AS (
    SELECT s.*, ts(payment_ts) AS ts_v, std_mode(payment_mode) AS mode_s, num(amount_paid) AS a,
           upper(trim(payment_status)) AS status, o.order_id AS o_id,
           row_number() OVER (PARTITION BY CASE WHEN blank(s.payment_id) THEN '#' || s._row_id ELSE s.payment_id END
                              ORDER BY s._row_id) AS rn
    FROM src s
    LEFT JOIN (SELECT DISTINCT order_id FROM stg_orders WHERE order_id IS NOT NULL) o ON o.order_id = s.order_id
)
SELECT *,
    reasons(list_value(
        flag(blank(payment_id), 'payment_id_not_null'),
        flag(rn > 1, 'payment_id_unique'),
        flag(status NOT IN ('SUCCESS', 'FAILED', 'REFUNDED'), 'payment_status_valid'),
        flag(mode_s IS NULL, 'payment_mode_valid'),
        flag(ts_v IS NULL, 'payment_ts_parses'),
        flag(a IS NULL, 'amount_paid_numeric'),
        flag(payment_status = 'SUCCESS' AND a <= 0, 'success_amount_positive'),
        flag(o_id IS NULL, 'order_id_in_orders'),
        flag(ts_v > $as_of::TIMESTAMP, 'payment_ts_not_future'))) AS failure_reason,
    reasons(list_value(
        flag(mode_s IS NOT NULL AND payment_mode <> mode_s, 'payment_mode_nonstandard'))) AS warning_reason
FROM parsed;

CREATE OR REPLACE TABLE stg_payments AS
SELECT nullif(payment_id, '') AS payment_id,
       nullif(order_id, '') AS order_id,
       ts_v AS payment_ts,
       mode_s AS payment_mode,
       a AS amount_paid,
       status AS payment_status,
       nullif(trim(gateway_ref), '') AS gateway_ref,
       CASE WHEN failure_reason = '' THEN 'PASSED' ELSE 'FAILED' END AS dq_status,
       failure_reason, warning_reason, _source_file, _run_id, _row_id, _loaded_at
FROM _chk_payments WHERE rn = 1;

DELETE FROM stg_duplicate_rows WHERE _run_id = $run_id AND source_file = $payments_file;
INSERT INTO stg_duplicate_rows
SELECT _source_file, payment_id, _row_id, failure_reason, warning_reason, _run_id, _loaded_at
FROM _chk_payments WHERE rn > 1;
