WITH revenue AS (
    SELECT * FROM {{ ref('total_revenue_per_customer') }}
)
SELECT customer_id, total_spent
FROM revenue
WHERE total_spent > 300