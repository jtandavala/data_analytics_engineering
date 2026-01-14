with customers as (
    select * from {{ ref('stg_customers') }}
),
sellers as (
    select * from {{ ref('stg_sellers') }}
),
customer_metrics as (
    select * from {{ ref('int_customer_metrics') }}
),

seller_performance as (
    select * from {{ ref('int_seller_performance') }}
),
state_metrics as (
    select
        customer_state as state,
        'customer' as entity_type,
        count(distinct customer_id) as entity_count,
        sum(order_count) as total_orders,
        sum(delivered_lifetime_value) as total_revenue
    from customer_metrics
    group by customer_state

    union all

    select
        seller_state as state,
        'seller' as entity_type,
        count(distinct seller_id) as entity_count,
        sum(total_orders) as total_orders,
        sum(total_revenue) as total_revenue
    from seller_performance
    group by seller_state
)

select
    state,
    sum(case when entity_type = 'customer' then entity_count else 0 end) as customer_count,
    sum(case when entity_type = 'seller' then entity_count else 0 end) as seller_count,
    sum(total_orders) as total_orders,
    sum(total_revenue) as total_revenue,
    
    -- Densidade
    sum(case when entity_type = 'customer' then entity_count else 0 end)::float / 
    nullif(sum(case when entity_type = 'seller' then entity_count else 0 end), 0) as customer_seller_ratio
    
from state_metrics
group by state
order by total_revenue desc