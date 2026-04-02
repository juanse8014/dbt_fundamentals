{% set foods=['radish','cucumber','chicken nugget','avocado'] %}

{%- for food in  foods -%}
    {% if food == 'chicken nugget' %}
        {%- set food_type='snack' -%}
    {% else %}
        {%- set food_type='vegetable' -%}
    {% endif %}

    The delicious {{ food }} is my favorite {{ foo_type }}
{% endfor %}