-- elt_23_mart_delivery_sla.sql
-- Business question 3: average delivery time per store and % of delivered orders within 15 minutes.

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT store_id, store_name, zone,
       count(*) AS delivered_orders,
       round(avg(delivery_minutes), 2) AS avg_delivery_minutes,
       round(100.0 * count(*) FILTER (WHERE within_15_min) / count(*), 2) AS pct_within_15_min
FROM mart_orders_enriched
WHERE order_status = 'DELIVERED'
GROUP BY store_id, store_name, zone
ORDER BY store_id;
