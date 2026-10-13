-- elt_20_mart_orders_enriched.sql
-- MART base: PASSED orders joined to PASSED customers, products and stores, with the derived
-- measures. net_revenue and gross_margin count DELIVERED orders only; other statuses are 0.

CREATE OR REPLACE TABLE mart_orders_enriched AS
SELECT o.order_id,
       o.order_ts,
       o.order_ts::DATE AS order_date,
       o.customer_id,
       o.store_id,
       s.zone,
       s.store_name,
       o.product_id,
       p.category,
       o.quantity,
       o.unit_price,
       o.discount_pct,
       o.order_amount,
       o.payment_mode,
       o.order_status,
       o.channel,
       o.delivery_minutes,
       CASE WHEN o.order_status = 'DELIVERED' THEN round(o.order_amount, 2) ELSE 0 END AS net_revenue,
       CASE WHEN o.order_status = 'DELIVERED'
            THEN round(round(o.order_amount, 2) - o.quantity * p.cost_price, 2) ELSE 0 END AS gross_margin,
       CASE WHEN o.order_status = 'DELIVERED' THEN o.delivery_minutes <= 15 END AS within_15_min,
       o._source_file, o._run_id, o._row_id
FROM stg_orders o
JOIN stg_products p ON p.product_id = o.product_id AND p.dq_status = 'PASSED'
JOIN stg_stores s ON s.store_id = o.store_id AND s.dq_status = 'PASSED'
JOIN stg_customers c ON c.customer_id = o.customer_id AND c.dq_status = 'PASSED'
WHERE o.dq_status = 'PASSED';
