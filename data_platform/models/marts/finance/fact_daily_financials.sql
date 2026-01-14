-- models/marts/finance/fact_daily_financials.sql
{{
    config(
        materialized='table',
        description='Daily financial metrics for finance reporting',
        tags=['finance', 'fact', 'daily']
    )
}}

with daily_financials as (
    select * from {{ ref('int_daily_financial_metrics') }}
),
date_dim as (
    select * from {{ ref('dim_date') }}
)

select
    df.order_date,
    dd.date_key,
    dd.year,
    dd.quarter,
    dd.month,
    dd.week_of_year,
    dd.day_of_week,
    dd.is_weekend,
    dd.season_br,
    
    -- Volume metrics
    df.total_orders,
    df.unique_customers,
    
    -- Revenue metrics (all in same currency - R$)
    df.gross_revenue,
    df.items_revenue,
    df.freight_revenue,
    df.delivered_revenue,
    df.canceled_revenue,
    
    -- Value metrics
    df.avg_order_value,
    df.median_order_value,
    
    -- Conversion metrics
    df.delivered_conversion_rate,
    df.canceled_conversion_rate,
    df.cancel_rate,
    
    -- Calculated metrics
    df.gross_revenue - df.canceled_revenue as net_revenue,
    case
        when df.total_orders > 0
        then df.delivered_revenue / df.total_orders
        else 0
    end as delivered_aov,
    
    -- Growth metrics (calculated in downstream models)
    null::numeric as revenue_growth_pct,
    null::numeric as orders_growth_pct

from daily_financials df
inner join date_dim dd on df.order_date = dd.date