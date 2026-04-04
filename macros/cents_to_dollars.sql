{% macro cents_to_dollars(value) %}
    round({{ value }} / 100, 2)
{% endmacro %}