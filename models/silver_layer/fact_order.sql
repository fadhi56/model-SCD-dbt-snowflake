{{ config(
  schema='silver_model',
  on_schema_change='append_new_columns',
  materialized='incremental',
) }}

with source as (
    select * from {{ source('bronze_order', 'raw_order') }}
    {% if is_incremental() %}
    -- implement a buffer for a period of 7 days, based on data_tests, then do a periodic full refresh to accommodate further later arrivals 
    where order_date > dateadd(day, -7, (select max(order_date) from {{ this }}))
    {% endif %}
)

{% if is_incremental() %},
-- remove duplicates if it is a incremental run 
dedupe as (
    select
        order_id,
        customer_id,
        product_id,
        order_date,
        number_articles,
        order_status,
        sale_price,
        sale_percentage,
        coupon_value,
        row_number() over (partition by order_id order by order_date desc) as row_num
    from
        source
),

final_data as (
    select
        order_id,
        customer_id,
        product_id,
        order_date,
        number_articles,
        order_status,
        sale_price,
        sale_percentage,
        coupon_value
    from
        dedupe
    where
        row_num = 1
)
{% else %},
final_data as (
    select
        order_id,
        customer_id,
        product_id,
        order_date,
        number_articles,
        order_status,
        sale_price,
        sale_percentage,
        coupon_value
    from
        source
)
{% endif %}

select  
    order_id,
    customer_id,
    product_id,
    order_date,
    number_articles,
    order_status,
    sale_price,
    sale_percentage,
    coupon_value
from 
    final_data
