-- elt_25_mart_top_customers.sql
-- Business question 5: top 10 customers by spend, with loyalty tier. Only masked contact details.

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT row_number() OVER (ORDER BY total_spend DESC, customer_id) AS spend_rank, *
FROM (
    SELECT e.customer_id, c.loyalty_tier, c.email_masked, c.phone_masked,
           count(*) FILTER (WHERE e.order_status = 'DELIVERED') AS delivered_orders,
           round(sum(e.net_revenue), 2) AS total_spend
    FROM mart_orders_enriched e
    JOIN stg_customers c ON c.customer_id = e.customer_id AND c.dq_status = 'PASSED'
    GROUP BY ALL
)
QUALIFY spend_rank <= 10
ORDER BY spend_rank;
