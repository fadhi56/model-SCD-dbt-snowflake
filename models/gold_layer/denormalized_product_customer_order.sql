{{ config(
  schema='gold_model',
  materialized='table'
) }}

with denormalize_data as (
select
    c.customer_id,
    c.customer_name,
    c.gender,
    c.birth_date,
    c.email_address,
    c.country,
    c.zip_code,
    c.street,
    c.street_number,
    p.product_id,
    p.product_name,
    p.brand,
    p.rrp,          
    p.currency,
    p.product_category,
    p.product_main_category,
    o.order_id,
    o.order_date,
    o.number_articles,
    o.order_status,
    o.sale_price,
    o.sale_percentage,
    o.coupon_value
from
    {{ ref('fact_order') }} o
join
    {{ ref('dim_product') }} p
on
    o.product_id = p.product_id
join
    {{ ref('dim_customer') }} c
on
    o.customer_id = c.customer_id
)
select
    customer_id,
    customer_name,
    gender,
    birth_date,
    email_address,
    country,
    zip_code,
    street,
    street_number,
    product_id,
    product_name,
    brand,
    rrp,          
    currency,
    product_category,
    product_main_category,
    order_id,
    order_date,
    number_articles,
    order_status,
    sale_price,
    sale_percentage,
    coupon_value
from
    denormalize_data
order by
    brand,
    customer_id
