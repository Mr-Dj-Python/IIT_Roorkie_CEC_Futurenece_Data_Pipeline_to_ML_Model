-- elt_24_mart_payment_reconciliation.sql
-- Business question 4: every order/payment mismatch with a reason, and revenue by payment mode
-- next to the amount that does not reconcile with the gateway.

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH o AS (
    SELECT order_id, order_status, order_amount FROM mart_orders_enriched
),
p AS (
    SELECT payment_id, order_id, payment_status, amount_paid FROM stg_payments WHERE dq_status = 'PASSED'
),
joined AS (
    SELECT coalesce(o.order_id, p.order_id) AS order_id, p.payment_id, o.order_status, p.payment_status,
           o.order_amount, p.amount_paid, round(p.amount_paid - o.order_amount, 2) AS difference,
           o.order_id IS NOT NULL AS has_order, p.payment_id IS NOT NULL AS has_payment
    FROM o FULL OUTER JOIN p ON o.order_id = p.order_id
),
reasoned AS (
    SELECT *,
        CASE
            WHEN has_order AND has_payment
                 AND payment_status IS DISTINCT FROM CASE order_status WHEN 'DELIVERED' THEN 'SUCCESS'
                                                                       WHEN 'RETURNED' THEN 'REFUNDED' END
                THEN 'payment_status_' || lower(payment_status) || '_but_order_' || lower(order_status)
            WHEN NOT has_order THEN 'payment_without_clean_order'
            WHEN NOT has_payment AND order_status <> 'CANCELLED' THEN 'missing_payment'
            WHEN payment_status = 'SUCCESS' AND abs(difference) > 0.0101 THEN 'amount_mismatch'
        END AS reason
    FROM joined
)
SELECT order_id, payment_id, order_status, payment_status, order_amount, amount_paid, difference,
       reason, $run_id AS _run_id
FROM reasoned WHERE reason IS NOT NULL
ORDER BY reason, order_id;

CREATE OR REPLACE TABLE mart_revenue_by_payment_mode AS
SELECT e.payment_mode,
       count(*) FILTER (WHERE e.order_status = 'DELIVERED') AS delivered_orders,
       round(sum(e.net_revenue), 2) AS net_revenue,
       -- gateway amount differs from the order amount
       round(coalesce(sum(abs(r.difference)) FILTER (WHERE r.reason = 'amount_mismatch'), 0), 2) AS amount_mismatch_value,
       -- delivered orders whose payment failed or is missing: revenue with no cash behind it
       round(coalesce(sum(e.order_amount) FILTER (WHERE r.reason IN ('payment_status_failed_but_order_delivered', 'missing_payment')
                                                      AND e.order_status = 'DELIVERED'), 0), 2) AS unpaid_delivered_value
FROM mart_orders_enriched e
LEFT JOIN mart_payment_reconciliation r ON r.order_id = e.order_id
GROUP BY e.payment_mode
ORDER BY net_revenue DESC;
