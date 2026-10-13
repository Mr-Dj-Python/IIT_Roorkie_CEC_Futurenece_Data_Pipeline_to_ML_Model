-- elt_13_stg_orders.sql
-- STAGING orders: cast, standardise, dedupe, apply quality rules, then MERGE (upsert) on order_id.
--   * a run loads one orders file (orders.csv, or the next-day increment file)
--   * new order_ids are INSERTed; an existing order_id is UPDATEd, so a late status change
--     overwrites the old row and re-running the same file never duplicates anything
--   * bad rows stay in staging as dq_status = 'FAILED'; extra copies of a repeated order_id
--     go to stg_duplicate_rows

CREATE TABLE IF NOT EXISTS stg_duplicate_rows (
    source_file VARCHAR, row_key VARCHAR, _row_id BIGINT, failure_reason VARCHAR, warning_reason VARCHAR,
    _run_id VARCHAR, _loaded_at TIMESTAMP);

CREATE TABLE IF NOT EXISTS stg_orders (
    order_id VARCHAR, order_ts TIMESTAMP, customer_id VARCHAR, store_id VARCHAR, product_id VARCHAR,
    quantity BIGINT, unit_price DOUBLE, discount_pct DOUBLE, order_amount DOUBLE, payment_mode VARCHAR,
    order_status VARCHAR, channel VARCHAR, delivery_minutes DOUBLE,
    dq_status VARCHAR, failure_reason VARCHAR, warning_reason VARCHAR,
    _source_file VARCHAR, _run_id VARCHAR, _row_id BIGINT, _loaded_at TIMESTAMP);

CREATE OR REPLACE TEMP TABLE _chk_orders AS
WITH src AS (
    SELECT coalesce(order_id, '') AS order_id, coalesce(order_ts, '') AS order_ts,
           coalesce(customer_id, '') AS customer_id, coalesce(store_id, '') AS store_id,
           coalesce(product_id, '') AS product_id, coalesce(quantity, '') AS quantity,
           coalesce(unit_price, '') AS unit_price, coalesce(discount_pct, '') AS discount_pct,
           coalesce(order_amount, '') AS order_amount, coalesce(payment_mode, '') AS payment_mode,
           coalesce(order_status, '') AS order_status, coalesce(channel, '') AS channel,
           coalesce(delivery_minutes, '') AS delivery_minutes,
           _loaded_at, _source_file, _run_id, _row_id
    FROM raw_orders WHERE _run_id = $run_id
),
parsed AS (
    SELECT s.*, ts(order_ts) AS ts_v, std_status(order_status) AS status, std_mode(payment_mode) AS mode_s,
           num(quantity) AS q, num(unit_price) AS p, num(discount_pct) AS d, num(order_amount) AS a,
           num(delivery_minutes) AS dm,
           c.dq_status AS c_status, c.customer_id AS c_id,
           pr.dq_status AS p_status, pr.product_id AS p_id,
           st.dq_status AS s_status, st.store_id AS s_id,
           row_number() OVER (PARTITION BY CASE WHEN blank(s.order_id) THEN '#' || s._row_id ELSE s.order_id END
                              ORDER BY s._row_id) AS rn
    FROM src s
    LEFT JOIN (SELECT customer_id, dq_status FROM stg_customers WHERE customer_id IS NOT NULL) c
           ON c.customer_id = s.customer_id
    LEFT JOIN (SELECT product_id, dq_status FROM stg_products WHERE product_id IS NOT NULL) pr
           ON pr.product_id = s.product_id
    LEFT JOIN (SELECT store_id, dq_status FROM stg_stores WHERE store_id IS NOT NULL) st
           ON st.store_id = s.store_id
)
SELECT *,
    reasons(list_value(
        flag(blank(customer_id), 'customer_id_not_null'),
        flag(blank(order_ts), 'order_ts_not_null'),
        flag(status = 'DELIVERED' AND blank(delivery_minutes), 'delivered_has_delivery_minutes'),
        flag(rn > 1, 'order_id_unique'),
        flag(status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED'), 'order_status_valid'),
        flag(mode_s IS NULL, 'payment_mode_valid'),
        flag(NOT blank(order_ts) AND ts_v IS NULL, 'order_ts_parses'),
        flag(q IS NULL OR p IS NULL OR d IS NULL OR a IS NULL
             OR (NOT blank(delivery_minutes) AND dm IS NULL), 'numeric_columns_parse'),
        flag(d < 0 OR d > 100, 'discount_pct_in_0_100'),
        flag(q <= 0, 'quantity_positive'),
        flag(p <= 0, 'unit_price_positive'),
        flag(abs(a - round(q * p * (1 - d / 100), 2)) > 0.0101, 'order_amount_matches_formula'),
        flag(dm >= 120, 'delivery_minutes_below_120'),
        flag(NOT blank(customer_id) AND c_id IS NULL, 'customer_id_in_master'),
        flag(p_id IS NULL, 'product_id_in_master'),
        flag(s_id IS NULL, 'store_id_in_master'),
        flag(c_status = 'FAILED', 'customer_master_row_quarantined'),
        flag(p_status = 'FAILED', 'product_master_row_quarantined'),
        flag(s_status = 'FAILED', 'store_master_row_quarantined'),
        flag(ts_v > $as_of::TIMESTAMP, 'order_ts_not_future'))) AS failure_reason,
    reasons(list_value(
        flag(status IN ('DELIVERED', 'CANCELLED', 'RETURNED') AND order_status <> status, 'order_status_casing'),
        flag(mode_s IS NOT NULL AND payment_mode <> mode_s, 'payment_mode_nonstandard'),
        flag(dm < 8 OR dm > 35, 'delivery_minutes_in_8_35'))) AS warning_reason
FROM parsed;

DELETE FROM stg_duplicate_rows WHERE _run_id = $run_id AND source_file = $orders_file;
INSERT INTO stg_duplicate_rows
SELECT _source_file, order_id, _row_id, failure_reason, warning_reason, _run_id, _loaded_at
FROM _chk_orders WHERE rn > 1;

-- the result of this statement (one row per INSERT / UPDATE) is logged by the DAG task
MERGE INTO stg_orders AS t
USING (
    SELECT nullif(order_id, '') AS order_id,
           ts_v AS order_ts,
           nullif(customer_id, '') AS customer_id,
           nullif(store_id, '') AS store_id,
           nullif(product_id, '') AS product_id,
           q::BIGINT AS quantity,
           p AS unit_price,
           d AS discount_pct,
           a AS order_amount,
           mode_s AS payment_mode,
           status AS order_status,
           nullif(trim(channel), '') AS channel,
           dm AS delivery_minutes,
           CASE WHEN failure_reason = '' THEN 'PASSED' ELSE 'FAILED' END AS dq_status,
           failure_reason, warning_reason, _source_file, _run_id, _row_id, _loaded_at
    FROM _chk_orders WHERE rn = 1
) AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
RETURNING merge_action;
