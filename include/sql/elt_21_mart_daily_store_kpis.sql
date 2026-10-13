-- elt_21_mart_daily_store_kpis.sql
-- Business question 1: daily net revenue by zone and by store (delivered orders, after discount).

CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT order_date, zone, store_id, store_name,
       count(*) FILTER (WHERE order_status = 'DELIVERED') AS delivered_orders,
       round(sum(net_revenue), 2) AS net_revenue
FROM mart_orders_enriched
GROUP BY ALL
ORDER BY order_date, zone, store_id;
