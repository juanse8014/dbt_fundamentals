select
    id as order_id,
    user_id as customer_id,
    order_date,
    status as order_status,
    _ETL_LOADED_AT as last_update
from {{ source('jaffle_shop', 'orders') }}