-- models/marts/finance/finance_kpis.sql
{{
    config(
        materialized='table',
        description='Finance KPIs ready for BI and AI consumption',
        tags=['finance', 'kpis', 'ai-ready']
    )
}}

with daily_financials as (
    select * from {{ ref('fact_daily_financials') }}
),
monthly_agg as (
    select
        year,
        month,
        sum(gross_revenue) as monthly_revenue,
        sum(net_revenue) as monthly_net_revenue,
        sum(total_orders) as monthly_orders,
        sum(delivered_revenue) as monthly_delivered_revenue,
        avg(avg_order_value) as monthly_avg_aov,
        avg(delivered_conversion_rate) as monthly_delivery_rate
    from daily_financials
    group by year, month
),
with_growth as (
    select
        *,
        lag(monthly_revenue) over (order by year, month) as prev_month_revenue,
        lag(monthly_orders) over (order by year, month) as prev_month_orders
    from monthly_agg
)

select
    year,
    month,
    monthly_revenue,
    monthly_net_revenue,
    monthly_orders,
    monthly_delivered_revenue,
    monthly_avg_aov,
    monthly_delivery_rate,
    
    -- Growth metrics
    case
        when prev_month_revenue > 0
        then ((monthly_revenue - prev_month_revenue) / prev_month_revenue) * 100
        else null
    end as revenue_growth_pct,
    
    case
        when prev_month_orders > 0
        then ((monthly_orders - prev_month_orders) / prev_month_orders) * 100
        else null
    end as orders_growth_pct,
    
    -- Financial health indicators
    case
        when monthly_delivery_rate >= 0.95 then 'Excellent'
        when monthly_delivery_rate >= 0.90 then 'Good'
        when monthly_delivery_rate >= 0.80 then 'Fair'
        else 'Poor'
    end as delivery_health_status,
    
    -- Revenue quality score (0-100)
    (monthly_delivery_rate * 100) as revenue_quality_score

from with_growth
order by year desc, month desc