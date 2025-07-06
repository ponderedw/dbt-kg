WITH revenue AS (
    SELECT customer_id, total_spent
    FROM {{ ref('total_revenue_per_customer') }}
),
segmented AS (
    SELECT customer_id, total_spent,
        CASE 
            WHEN total_spent < 100 THEN 'Low'
            WHEN total_spent BETWEEN 100 AND 300 THEN 'Medium'
            ELSE 'High'
        END AS segment
    FROM revenue
)
SELECT * FROM segmented