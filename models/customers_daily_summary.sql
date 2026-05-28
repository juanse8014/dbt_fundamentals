select
    customer_id,
    order_id,
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'order_id']) }} as primary_key,
    count(*) as count
from {{ ref('stg_jaffle_shop__orders') }}
group by customer_id, order_id