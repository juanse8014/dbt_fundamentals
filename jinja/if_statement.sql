{% set feel_drowsy = 1%}
Today I fell drowsy so I need to drink a
{% if feel_drowsy == 1 %}
coffee
{% else %}
Regular Water
{% endif %}