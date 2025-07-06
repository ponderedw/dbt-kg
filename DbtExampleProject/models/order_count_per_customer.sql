WITH orders AS (
    SELECT customer_id, COUNT(order_id) AS order_count
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
)
SELECT * FROM orders