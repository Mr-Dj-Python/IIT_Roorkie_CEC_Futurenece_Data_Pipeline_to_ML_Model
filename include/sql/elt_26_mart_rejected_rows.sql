-- elt_26_mart_rejected_rows.sql
-- Business question 6: rows rejected, by file and reason. One row per run, file and reason, so the
-- history stays; "today" is the latest run. A row that breaks two rules counts under both reasons.

CREATE TABLE IF NOT EXISTS mart_rejected_rows (
    run_id VARCHAR, source_file VARCHAR, failure_reason VARCHAR, rows_rejected BIGINT, recorded_at TIMESTAMP);

DELETE FROM mart_rejected_rows WHERE run_id = $run_id;

INSERT INTO mart_rejected_rows
WITH rejected AS (
    SELECT _source_file AS source_file, failure_reason FROM stg_orders WHERE dq_status = 'FAILED' AND _run_id = $run_id
    UNION ALL SELECT _source_file, failure_reason FROM stg_payments WHERE dq_status = 'FAILED'
    UNION ALL SELECT _source_file, failure_reason FROM stg_customers WHERE dq_status = 'FAILED'
    UNION ALL SELECT _source_file, failure_reason FROM stg_products WHERE dq_status = 'FAILED'
    UNION ALL SELECT _source_file, failure_reason FROM stg_stores WHERE dq_status = 'FAILED'
    UNION ALL SELECT source_file, failure_reason FROM stg_duplicate_rows WHERE _run_id = $run_id
)
SELECT $run_id, source_file, reason, count(*), $loaded_at::TIMESTAMP
FROM (SELECT source_file, unnest(string_split(failure_reason, ';')) AS reason FROM rejected)
WHERE reason <> ''
GROUP BY source_file, reason
ORDER BY source_file, reason;
