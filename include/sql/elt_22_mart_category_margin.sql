-- elt_22_mart_category_margin.sql
-- Business question 2: revenue and gross margin by product category (margin = revenue - qty * cost_price).

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT category,
       round(sum(net_revenue), 2) AS revenue,
       round(sum(gross_margin), 2) AS gross_margin,
       round(100 * sum(gross_margin) / nullif(sum(net_revenue), 0), 2) AS margin_pct
FROM mart_orders_enriched
GROUP BY category
ORDER BY revenue DESC;
