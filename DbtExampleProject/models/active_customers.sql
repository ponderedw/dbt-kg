WITH recent_orders AS (
    SELECT DISTINCT customer_id
    FROM {{ ref('stg_orders') }}
    WHERE order_date >= NOW() - INTERVAL '30 days'
)
SELECT c.customer_id, c.customer_name, c.email
FROM {{ ref('stg_customers') }} c
JOIN recent_orders r ON c.customer_id = r.customer_id