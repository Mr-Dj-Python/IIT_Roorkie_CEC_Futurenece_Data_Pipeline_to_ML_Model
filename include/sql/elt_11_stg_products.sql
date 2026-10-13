-- elt_11_stg_products.sql
-- STAGING products. When a product_id repeats, the current row wins over a "(old)" variant,
-- then the first row in the file.

CREATE TABLE IF NOT EXISTS stg_duplicate_rows (
    source_file VARCHAR, row_key VARCHAR, _row_id BIGINT, failure_reason VARCHAR, warning_reason VARCHAR,
    _run_id VARCHAR, _loaded_at TIMESTAMP);

CREATE OR REPLACE TEMP TABLE _chk_products AS
WITH src AS (
    SELECT coalesce(product_id, '') AS product_id, coalesce(product_name, '') AS product_name,
           coalesce(category, '') AS category, coalesce(sub_category, '') AS sub_category,
           coalesce(brand, '') AS brand, coalesce(mrp, '') AS mrp, coalesce(cost_price, '') AS cost_price,
           coalesce(unit, '') AS unit, coalesce(is_perishable, '') AS is_perishable,
           coalesce(launch_date, '') AS launch_date, _loaded_at, _source_file, _run_id, _row_id
    FROM raw_products WHERE _run_id = $run_id
),
parsed AS (
    SELECT *, num(mrp) AS mrp_n, num(cost_price) AS cost_n, dt(launch_date) AS launch_d,
           row_number() OVER (PARTITION BY CASE WHEN blank(product_id) THEN '#' || _row_id ELSE product_id END
                              ORDER BY regexp_matches(lower(product_name), '\(old\)')::int, _row_id) AS rn
    FROM src
)
SELECT *,
    reasons(list_value(
        flag(blank(category), 'category_not_null'),
        flag(rn > 1, 'product_id_unique'),
        flag(mrp_n IS NULL OR cost_n IS NULL, 'price_columns_numeric'),
        flag(launch_d IS NULL, 'launch_date_parses'),
        flag(mrp_n <= 0, 'mrp_positive'),
        flag(cost_n <= 0, 'cost_price_positive'),
        flag(cost_n > mrp_n, 'cost_price_not_above_mrp'),
        flag(launch_d::TIMESTAMP > $as_of::TIMESTAMP, 'launch_date_not_future'))) AS failure_reason,
    '' AS warning_reason
FROM parsed;

CREATE OR REPLACE TABLE stg_products AS
SELECT nullif(product_id, '') AS product_id,
       trim(product_name) AS product_name,
       nullif(trim(category), '') AS category,
       trim(sub_category) AS sub_category,
       trim(brand) AS brand,
       mrp_n AS mrp,
       cost_n AS cost_price,
       trim(unit) AS unit,
       trim(is_perishable) AS is_perishable,
       launch_d AS launch_date,
       CASE WHEN failure_reason = '' THEN 'PASSED' ELSE 'FAILED' END AS dq_status,
       failure_reason, warning_reason, _source_file, _run_id, _row_id, _loaded_at
FROM _chk_products WHERE rn = 1;

DELETE FROM stg_duplicate_rows WHERE _run_id = $run_id AND source_file = $products_file;
INSERT INTO stg_duplicate_rows
SELECT _source_file, product_id, _row_id, failure_reason, warning_reason, _run_id, _loaded_at
FROM _chk_products WHERE rn > 1;
