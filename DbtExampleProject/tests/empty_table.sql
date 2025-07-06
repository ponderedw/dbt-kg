select 1
from {{ ref('stg_orders') }}
where 1 = 0