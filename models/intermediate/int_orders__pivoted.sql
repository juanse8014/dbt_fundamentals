with payments as
(
    select *
    from {{ ref('stg_stripe__payments') }}
    where status = 'success'
)

, pivoted as
(
    select order_id,
        {% set payment_methods=['bank_transfer','coupon','credit_card','gift_card'] %}
        {% for payment_method in payment_methods %}
            sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{payment_method}}_amount
            {#To avoid putting a comma in the last iteration#}
            {% if not loop.last %}
                ,
            {% endif %}
        {% endfor %}
        

    from payments
    group by 1
)
select * from pivoted
order by order_id