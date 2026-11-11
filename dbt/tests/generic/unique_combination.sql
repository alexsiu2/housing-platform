{#- Fails for each combination of `columns` that appears more than once.
    (Same idea as dbt_utils.unique_combination_of_columns, without the package.) -#}
{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
