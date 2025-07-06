WITH order_dates AS (
    SELECT customer_id, order_date, LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS prev_order_date
    FROM {{ ref('stg_orders') }}
),
order_intervals AS (
    SELECT customer_id, (order_date - prev_order_date) AS days_between_orders
    FROM order_dates
    WHERE prev_order_date IS NOT NULL
)
SELECT customer_id, AVG(days_between_orders) AS avg_days_between_orders
FROM order_intervals
GROUP BY customer_id