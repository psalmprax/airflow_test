{% macro test_unique_where(model) %}
{# to be replaced by an identical schema test in gemma dbt utils #}


{% set column_name = kwargs.get('column_name', kwargs.get('arg')) %}
{% set where = kwargs.get('where', kwargs.get('arg')) or 'true' %}

select count(*)
from (

    select
        {{ column_name }}

    from {{ model }}
    where {{ column_name }} is not null and {{ where }}
    group by {{ column_name }}
    having count(*) > 1

) validation_errors

{% endmacro %}
