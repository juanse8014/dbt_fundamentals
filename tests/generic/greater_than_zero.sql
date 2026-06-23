{% test greater_than_zero(model, column_name) %}

    {{ config(severity='warn') }}

    select
        {{ column_name }}
    from {{ model }}
    where {{ column_name }} <= 0

{% endtest %}