{% set dictionary= {
    'word':'data',
    'part_of_speech':'noun',
    'definition':'the building block of life'
} %}

{{ dictionary['word'] }} ({{ dictionary['part_of_speech'] }}): defined as "{{ dictionary['definition'] }}"