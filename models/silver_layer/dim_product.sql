{{ config(
  schema='silver_model',
  on_schema_change='append_new_columns',
  materialized='incremental',
) }}

-- Step 1: Get all data and if incremental get the new records
with source as (
    select * from {{ source('bronze_product', 'raw_product') }}
    {% if is_incremental() %}
    where product_id NOT IN (select product_id from {{ this }})
    {% endif %}
),

-- Step 2: Run only if model is incremental
{% if is_incremental() %}
-- Get all records from current dimension
existing_data as (
    select * from {{ this }}
),
-- Get changed records by inner joining current dim table and bronze table where values have changes for a product_id
changed_records as (
    select
        s.product_id,
        s.product_name,
        s.brand,
        s.rrp,          
        s.currency,
        s.product_category,
        s.product_main_category,
        'active' as status
    from
        {{ source('bronze_product', 'raw_product') }} s
    inner join
        existing_data e
    on
        s.product_id = e.product_id
    where
        s.product_name <> e.product_name or
        s.brand <> e.brand or
        s.rrp <> e.rrp or
        s.currency <> e.currency or
        s.product_category <> e.product_category or
        s.product_main_category <> e.product_main_category
),
-- Set the status for the changed records to inactive from the current dim table
inactive_old_records as (
    select
        e.product_id,
        e.product_name,
        e.brand,
        e.rrp,          
        e.currency,
        e.product_category,
        e.product_main_category,
        'inactive' as status
    from
        existing_data e
    inner join
        changed_records c
    on
        e.product_id = c.product_id
),
{% endif %}

-- Combine new records, changed record marked as active and old records marked as inactive
final_data as (
    select
        product_id,
        product_name,
        brand,
        rrp,          
        currency,
        product_category,
        product_main_category,
        'active' as status
    from 
        source
    {% if is_incremental() %}
    union all
    select 
        product_id,
        product_name,
        brand,
        rrp,          
        currency,
        product_category,
        product_main_category,
        status
    from inactive_old_records
    union all
    select 
        product_id,
        product_name,
        brand,
        rrp,          
        currency,
        product_category,
        product_main_category,
        status
    from changed_records
    {% endif %}
)

select  
    product_id,
    product_name,
    brand,
    rrp,          
    currency,
    product_category,
    product_main_category,
    status
from 
    final_data
