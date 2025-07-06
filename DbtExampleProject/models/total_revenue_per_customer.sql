WITH revenue AS (
    SELECT customer_id, SUM(order_total) AS total_spent
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
)
SELECT * FROM revenue