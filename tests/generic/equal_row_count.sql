-- with this macro, comapre the bronze and silver layer and if count mismatches, a backfill/full refresh is required
{% test equal_rowcount(model, compare_model) %}

with table_silver as (

    select
      1 as id,
      count(*) as count_a
    from {{ model }}


),
table_bronze as (

    select
      1 as id,
      count(*) as count_b
    from {{ compare_model }}

),
validation as (

    select

        count_a,
        count_b,
        abs(count_a - count_b) as diff_count

    from table_silver
    full join table_bronze
    on table_silver.id = table_bronze.id

)

select * from validation


{% endtest %}
