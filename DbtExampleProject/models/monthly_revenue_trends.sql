WITH revenue AS (
    SELECT 
        DATE_TRUNC('month', order_date) AS month,
        SUM(order_total) AS total_revenue
    FROM {{ ref('stg_orders') }}
    GROUP BY 1
)
SELECT * FROM revenue