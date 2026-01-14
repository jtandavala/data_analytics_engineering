-- models/marts/core/dim_products.sql
{{
    config(
        materialized='table',
        description='Product dimension with performance metrics',
        tags=['core', 'dimension', 'conformed']
    )
}}

with products as (
    select * from {{ ref('stg_products') }}
),
product_performance as (
    select * from {{ ref('int_product_performance') }}
)

select
    p.product_id,
    p.category_name,
    p.weight_g as product_weight_g,
    p.length_cm as product_length_cm,
    p.height_cm as product_height_cm,
    p.width_cm as product_width_cm,
    
    -- Calculated attributes
    p.length_cm * p.height_cm * p.width_cm as product_volume_cm3,
    case
        when p.weight_g > 5000 then 'Heavy'
        when p.weight_g > 2000 then 'Medium'
        else 'Light'
    end as weight_category,
    
    -- Performance metrics
    coalesce(pp.order_count, 0) as total_orders,
    coalesce(pp.units_sold, 0) as total_units_sold,
    coalesce(pp.total_revenue, 0) as total_revenue,
    coalesce(pp.avg_price, 0) as avg_price,
    coalesce(pp.delivery_rate, 0) as delivery_rate,
    
    -- Classification
    case
        when coalesce(pp.total_revenue, 0) > 10000 then 'High Value'
        when coalesce(pp.total_revenue, 0) > 1000 then 'Medium Value'
        else 'Low Value'
    end as value_segment

from products p
left join product_performance pp on p.product_id = pp.product_id