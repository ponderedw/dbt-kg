WITH source AS (
    SELECT * FROM {{ source('raw', 'orders') }}
)
SELECT 
    order_id, 
    customer_id, 
    order_total,
    order_date
FROM source