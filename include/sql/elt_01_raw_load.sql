-- elt_01_raw_load.sql
-- RAW layer: exact copy of each CSV. Every column is text (empty field -> NULL, the only
-- representation CSV allows). Nothing is cleaned and no row is dropped, bad ones included.
-- Each run appends a new snapshot tagged with _run_id; only a retry of the SAME run replaces
-- its own partial load. _row_id is the line number in the source file (header = line 1).

CREATE TABLE IF NOT EXISTS raw_orders (
    order_id VARCHAR, order_ts VARCHAR, customer_id VARCHAR, store_id VARCHAR, product_id VARCHAR,
    quantity VARCHAR, unit_price VARCHAR, discount_pct VARCHAR, order_amount VARCHAR, payment_mode VARCHAR,
    order_status VARCHAR, channel VARCHAR, delivery_minutes VARCHAR,
    _loaded_at TIMESTAMP, _source_file VARCHAR, _run_id VARCHAR, _row_id BIGINT);
CREATE TABLE IF NOT EXISTS raw_payments (
    payment_id VARCHAR, order_id VARCHAR, payment_ts VARCHAR, payment_mode VARCHAR, amount_paid VARCHAR,
    payment_status VARCHAR, gateway_ref VARCHAR,
    _loaded_at TIMESTAMP, _source_file VARCHAR, _run_id VARCHAR, _row_id BIGINT);
CREATE TABLE IF NOT EXISTS raw_customers (
    customer_id VARCHAR, full_name VARCHAR, email VARCHAR, phone VARCHAR, city VARCHAR, area VARCHAR,
    pincode VARCHAR, signup_date VARCHAR, loyalty_tier VARCHAR, is_active VARCHAR,
    _loaded_at TIMESTAMP, _source_file VARCHAR, _run_id VARCHAR, _row_id BIGINT);
CREATE TABLE IF NOT EXISTS raw_products (
    product_id VARCHAR, product_name VARCHAR, category VARCHAR, sub_category VARCHAR, brand VARCHAR,
    mrp VARCHAR, cost_price VARCHAR, unit VARCHAR, is_perishable VARCHAR, launch_date VARCHAR,
    _loaded_at TIMESTAMP, _source_file VARCHAR, _run_id VARCHAR, _row_id BIGINT);
CREATE TABLE IF NOT EXISTS raw_stores (
    store_id VARCHAR, store_name VARCHAR, zone VARCHAR, area VARCHAR, pincode VARCHAR, open_date VARCHAR,
    manager_name VARCHAR, capacity_orders_per_day VARCHAR,
    _loaded_at TIMESTAMP, _source_file VARCHAR, _run_id VARCHAR, _row_id BIGINT);

DELETE FROM raw_orders WHERE _run_id = $run_id;
DELETE FROM raw_payments WHERE _run_id = $run_id;
DELETE FROM raw_customers WHERE _run_id = $run_id;
DELETE FROM raw_products WHERE _run_id = $run_id;
DELETE FROM raw_stores WHERE _run_id = $run_id;

INSERT INTO raw_orders
SELECT *, $loaded_at::TIMESTAMP, $orders_file, $run_id, row_number() OVER () + 1
FROM read_csv($orders_path, all_varchar = true, header = true, parallel = false);

INSERT INTO raw_payments
SELECT *, $loaded_at::TIMESTAMP, $payments_file, $run_id, row_number() OVER () + 1
FROM read_csv($payments_path, all_varchar = true, header = true, parallel = false);

INSERT INTO raw_customers
SELECT *, $loaded_at::TIMESTAMP, $customers_file, $run_id, row_number() OVER () + 1
FROM read_csv($customers_path, all_varchar = true, header = true, parallel = false);

INSERT INTO raw_products
SELECT *, $loaded_at::TIMESTAMP, $products_file, $run_id, row_number() OVER () + 1
FROM read_csv($products_path, all_varchar = true, header = true, parallel = false);

INSERT INTO raw_stores
SELECT *, $loaded_at::TIMESTAMP, $stores_file, $run_id, row_number() OVER () + 1
FROM read_csv($stores_path, all_varchar = true, header = true, parallel = false);
