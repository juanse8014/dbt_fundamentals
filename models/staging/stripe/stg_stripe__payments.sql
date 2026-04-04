select
    id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status,
    {{ cents_to_dollars("amount") }} as amount, --Use a Macro to convert Cents to Dollars
    created as created_at,
from {{ source("stripe", "payment") }}
