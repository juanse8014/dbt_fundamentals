{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy="microbatch",
        event_time="order_date",
        begin="2025-02-13",
        batch_size="day"
    )
}}

with orders as  (
    select * from {{ ref ('stg_jaffle_shop__orders' )}}
),

payments as (
    select * from {{ ref ('stg_stripe__payments') }}
),

order_payments as (
    select
        order_id,
        sum (case when status = 'success' then amount end) as amount

    from payments
    group by 1
),

 final as (

    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        coalesce (order_payments.amount, 0) as amount

    from orders
    left join order_payments using (order_id)
)

select * from final
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where order_date >= (select max(order_date) from {{ this }})  -- "=" can cause duplicates, but in next sections we'll learn how to manage duplicates
{% endif %}
