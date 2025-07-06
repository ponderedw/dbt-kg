WITH source AS (
    SELECT * FROM {{ source('raw', 'customers') }}
)
SELECT 
    customer_id, 
    LOWER(name) AS customer_name,
    email,
    created_at
FROM source