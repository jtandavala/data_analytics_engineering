{{
    config(
        materialized='table',
        description='Seller dimension with performance metrics',
        tags=['core', 'dimension', 'conformed']
    )
}}

with sellers as (
    select * from {{ ref('stg_sellers') }}
),
seller_performance as (
    select * from {{ ref('int_seller_performance') }}
)

select
    s.seller_id,
    s.seller_city,
    s.seller_state,
    s.seller_zip_code_prefix,
    
    -- Performance metrics
    coalesce(sp.total_orders, 0) as total_orders,
    coalesce(sp.unique_products, 0) as unique_products,
    coalesce(sp.total_items_sold, 0) as total_items_sold,
    coalesce(sp.total_revenue, 0) as total_revenue,
    coalesce(sp.delivered_revenue, 0) as delivered_revenue,
    coalesce(sp.avg_price, 0) as avg_price,
    coalesce(sp.median_item_price, 0) as median_item_price,
    
    -- Performance tier
    case
        when coalesce(sp.total_revenue, 0) > 50000 then 'Top Performer'
        when coalesce(sp.total_revenue, 0) > 10000 then 'Good Performer'
        when coalesce(sp.total_revenue, 0) > 0 then 'Active'
        else 'Inactive'
    end as performance_tier,
    
    -- Delivery rate
    case
        when coalesce(sp.total_orders, 0) > 0
        then coalesce(sp.delivered_orders, 0)::float / sp.total_orders
        else 0
    end as delivery_rate

from sellers s
left join seller_performance sp on s.seller_id = sp.seller_id