{% snapshot customers_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='check',
      check_cols=['customer_name', 'email']
    )
}}

select 
    customer_id, customer_name, email
from {{ ref('active_customers') }}

{% endsnapshot %}