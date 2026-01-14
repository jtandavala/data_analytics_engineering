with product_performance as (
    select * from {{ ref('int_product_performance') }}
),
category_metrics as (
    select
        category_name,
        count(distinct product_id) as product_count,
        round(sum(order_count), 2) as total_orders,
        round(sum(units_sold), 2) as total_units_sold,
        round(sum(total_revenue), 2) as total_revenue,
        round(sum(delivered_revenue), 2) as delivered_revenue,
        round(avg(avg_price), 2) as avg_category_price,

        -- Topo products
        count(distinct case when order_count >= 10 then product_id end) as high_volume_products,
        -- Diversidade
        count(distinct product_id)::float / nullif(sum(order_count), 0) as product_diversity_ratio
    from product_performance
    where category_name is not null
    group by category_name
)
select
    category_name,
    product_count,
    total_orders,
    total_units_sold,
    total_revenue,
    delivered_revenue,
    avg_category_price,
    high_volume_products,
    product_diversity_ratio,
    -- Market shared
    total_revenue::float / nullif(sum(total_revenue) over (), 0) as revenue_share,
    -- Performance ranking
    rank() over (order by total_revenue desc) as revenue_rank
from category_metrics
order by total_revenue desc