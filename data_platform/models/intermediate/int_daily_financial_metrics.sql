with orders_enriched as (
    select * from {{ ref('int_orders') }}
),
daily_metrics as (
    select
        date(order_purchase_timestamp) as order_date,
        -- Volume
        count(distinct order_id) as total_orders,
        count(distinct customer_id) as unique_customers,
        -- Revenue
        sum(order_value) as gross_revenue,
        round(sum(items_total)::numeric, 2) as items_revenue,
        round(sum(freight_total)::numeric, 2) as freight_revenue,
        round(sum(case when is_delivered = 1 then order_value else 0 end)::numeric, 2) as delivered_revenue,
        round(sum(case when is_canceled = 1 then order_value else 0 end)::numeric, 2) as canceled_revenue,
        -- AOV (Average Order Value)
        round(avg(order_value)::numeric, 2) as avg_order_value,
        round(percentile_cont(0.5) within group (order by order_value)::numeric, 2) as median_order_value,
        -- Conversion rates
        round(sum(case when is_delivered = 1 then 1 else 0 end) / count(distinct order_id), 2) as delivered_conversion_rate,
        round(sum(case when is_canceled = 1 then 1 else 0 end) / count(distinct order_id), 2) as canceled_conversion_rate,
        -- Cancel rates
        round(sum(case when is_canceled = 1 then 1 else 0 end) / count(distinct order_id), 2) as cancel_rate
    from orders_enriched
    where order_purchase_timestamp is not null
    group by date(order_purchase_timestamp)
)
select * from daily_metrics
order by order_date desc