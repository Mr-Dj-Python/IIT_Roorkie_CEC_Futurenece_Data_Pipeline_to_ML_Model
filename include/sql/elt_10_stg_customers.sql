-- elt_10_stg_customers.sql
-- STAGING: cast, standardise, dedupe, mask PII, apply quality rules. One row per customer_id.
-- Bad rows stay here with dq_status = 'FAILED' and a failure_reason; marts only read PASSED rows.
-- Extra copies of a repeated key go to stg_duplicate_rows so no row is silently dropped.

CREATE TABLE IF NOT EXISTS stg_duplicate_rows (
    source_file VARCHAR, row_key VARCHAR, _row_id BIGINT, failure_reason VARCHAR, warning_reason VARCHAR,
    _run_id VARCHAR, _loaded_at TIMESTAMP);

CREATE OR REPLACE TEMP TABLE _chk_customers AS
WITH src AS (
    SELECT coalesce(customer_id, '') AS customer_id, coalesce(email, '') AS email, coalesce(phone, '') AS phone,
           coalesce(city, '') AS city, coalesce(area, '') AS area, coalesce(pincode, '') AS pincode,
           coalesce(signup_date, '') AS signup_date, coalesce(loyalty_tier, '') AS loyalty_tier,
           coalesce(is_active, '') AS is_active, _loaded_at, _source_file, _run_id, _row_id
    FROM raw_customers WHERE _run_id = $run_id
),
parsed AS (
    SELECT *, dt(signup_date) AS signup_d, std_tier(loyalty_tier) AS tier,
           row_number() OVER (PARTITION BY CASE WHEN blank(customer_id) THEN '#' || _row_id ELSE customer_id END
                              ORDER BY _row_id) AS rn
    FROM src
)
SELECT *,
    reasons(list_value(
        flag(blank(customer_id), 'customer_id_not_null'),
        flag(rn > 1, 'customer_id_unique'),
        flag(signup_d IS NULL, 'signup_date_parses'),
        flag(tier NOT IN ('Bronze', 'Silver', 'Gold', 'Platinum'), 'loyalty_tier_valid'),
        flag(signup_d::TIMESTAMP > $as_of::TIMESTAMP, 'signup_date_not_future'))) AS failure_reason,
    reasons(list_value(
        flag(blank(city), 'city_not_null'),
        flag(NOT regexp_matches(trim(email), '^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$'), 'email_format'),
        flag(NOT regexp_matches(trim(phone), '^\d{10}$'), 'phone_10_digits'),
        flag(NOT regexp_matches(trim(pincode), '^\d{6}$'), 'pincode_6_digits'),
        flag(tier IN ('Bronze', 'Silver', 'Gold', 'Platinum') AND loyalty_tier <> tier, 'loyalty_tier_casing'))) AS warning_reason
FROM parsed;

-- raw email, phone and name never leave this statement: only masked values are stored
CREATE OR REPLACE TABLE stg_customers AS
SELECT nullif(customer_id, '') AS customer_id,
       mask_email(email) AS email_masked,
       mask_phone(phone) AS phone_masked,
       nullif(trim(city), '') AS city,
       nullif(trim(area), '') AS area,
       nullif(trim(pincode), '') AS pincode,
       signup_d AS signup_date,
       tier AS loyalty_tier,
       nullif(trim(is_active), '') AS is_active,
       CASE WHEN failure_reason = '' THEN 'PASSED' ELSE 'FAILED' END AS dq_status,
       failure_reason, warning_reason, _source_file, _run_id, _row_id, _loaded_at
FROM _chk_customers WHERE rn = 1;

DELETE FROM stg_duplicate_rows WHERE _run_id = $run_id AND source_file = $customers_file;
INSERT INTO stg_duplicate_rows
SELECT _source_file, customer_id, _row_id, failure_reason, warning_reason, _run_id, _loaded_at
FROM _chk_customers WHERE rn > 1;
