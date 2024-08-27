{% test duplicate_rows(model) %}

with original_count as (

    select count(*) as cnt
    from {{ model }}

),

distinct_count as (

    select count(*) as cnt
    from (
      select distinct * 
      from {{ model }}
    ) AS distinct_rows

),

validation_error as (

    select 
        original_count.cnt as original_count,
        distinct_count.cnt as distinct_count,
        (original_count.cnt - distinct_count.cnt) as duplicate_count
    from original_count, distinct_count

)

select duplicate_count
from validation_error
where duplicate_count > 0
{% endtest %}



