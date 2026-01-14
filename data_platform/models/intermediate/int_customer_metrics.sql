with customers as (
    select * from {{ ref('stg_customers') }}
),
orders_enriched as (
    select * from {{ ref('int_orders') }}
),
customer_orders as (
    select
        c.customer_id,
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        c.zip_code_prefix,

        -- Frequency
        count(distinct oe.order_id) as order_count,
        count(distinct case when oe.is_delivered = 1 then oe.order_id end) as delivered_order_count,
        -- Value
        round(sum(oe.order_value)::numeric, 2) as total_lifetime_value,
        round(sum(case when oe.is_delivered = 1 then oe.order_value else 0 end)::numeric, 2) as delivered_lifetime_value,
        round(avg(oe.order_value)::numeric,2 ) as avg_order_value,
        max(oe.order_value) as max_order_value,
        -- Recency
        max(oe.order_purchase_timestamp) as last_order_date,
        min(oe.order_purchase_timestamp) as first_order_date,
        extract(epoch from (current_timestamp::timestamp - max(oe.order_purchase_timestamp::timestamp))) / 86400 as days_since_last_order,
        -- Time as customer
         extract(epoch from (max(oe.order_purchase_timestamp::timestamp) - min(oe.order_purchase_timestamp::timestamp))) / 86400 as customer_lifetime_days,
         -- Paying customer flag: has at least one order with payment 
         max(case when oe.payment_total > 0 then 1 else 0 end) as is_paying_customer
    from customers c
    left join orders_enriched oe on c.customer_id = oe.customer_id
    group by 
        c.customer_id,
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        c.customer_state,
        c.zip_code_prefix
)
select  
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state,
    zip_code_prefix,
    -- RFM-like metrics
    order_count,
    delivered_order_count,
    total_lifetime_value,
    delivered_lifetime_value,
    avg_order_value,
    max_order_value,
    first_order_date,
    last_order_date,
    days_since_last_order,
    customer_lifetime_days,
    -- Paying customer flag
    case 
        when coalesce(is_paying_customer, 0) = 1 then true
        else false
    end as is_paying_customer,
    -- Segmentação
    case
        when order_count = 0 then 'No Orders'
        when order_count = 1 then 'One-time'
        when order_count between 2 and 4 then 'Repeat'
        else 'VIP'
    end as customer_segment,
    -- Valor médio mensal (CLV aproximado)
    case
        when customer_lifetime_days > 0
        then (delivered_lifetime_value / nullif(customer_lifetime_days, 0)) * 30
        else 0
    end as estimated_monthly_value
    
from customer_orders
