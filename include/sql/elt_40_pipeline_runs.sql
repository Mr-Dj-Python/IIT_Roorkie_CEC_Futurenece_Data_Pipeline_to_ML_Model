-- elt_40_pipeline_runs.sql
-- Run audit: one row per DAG run, written by a task that runs even when upstream tasks failed.

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP,
    rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR);

DELETE FROM pipeline_runs WHERE run_id = $run_id AND dag_id = $dag_id;

INSERT INTO pipeline_runs
SELECT $run_id, $dag_id, $start_time::TIMESTAMP, $end_time::TIMESTAMP,
       (SELECT count(*) FROM raw_orders WHERE _run_id = $run_id)
     + (SELECT count(*) FROM raw_payments WHERE _run_id = $run_id)
     + (SELECT count(*) FROM raw_customers WHERE _run_id = $run_id)
     + (SELECT count(*) FROM raw_products WHERE _run_id = $run_id)
     + (SELECT count(*) FROM raw_stores WHERE _run_id = $run_id),
       (SELECT count(*) FROM stg_orders WHERE _run_id = $run_id AND dq_status = 'PASSED')
     + (SELECT count(*) FROM stg_payments WHERE _run_id = $run_id AND dq_status = 'PASSED')
     + (SELECT count(*) FROM stg_customers WHERE _run_id = $run_id AND dq_status = 'PASSED')
     + (SELECT count(*) FROM stg_products WHERE _run_id = $run_id AND dq_status = 'PASSED')
     + (SELECT count(*) FROM stg_stores WHERE _run_id = $run_id AND dq_status = 'PASSED'),
       (SELECT count(*) FROM stg_orders WHERE _run_id = $run_id AND dq_status = 'FAILED')
     + (SELECT count(*) FROM stg_payments WHERE _run_id = $run_id AND dq_status = 'FAILED')
     + (SELECT count(*) FROM stg_customers WHERE _run_id = $run_id AND dq_status = 'FAILED')
     + (SELECT count(*) FROM stg_products WHERE _run_id = $run_id AND dq_status = 'FAILED')
     + (SELECT count(*) FROM stg_stores WHERE _run_id = $run_id AND dq_status = 'FAILED')
     + (SELECT count(*) FROM stg_duplicate_rows WHERE _run_id = $run_id),
       $status;
