-- models/marts/core/dim_geography.sql
{{
    config(
        materialized='table',
        description='Geography dimension conformed for customers and sellers',
        tags=['core', 'dimension', 'conformed']
    )
}}

with customers as (
    select distinct
        customer_state as state,
        customer_city as city,
        'customer' as entity_type
    from {{ ref('stg_customers') }}
),
sellers as (
    select distinct
        seller_state as state,
        seller_city as city,
        'seller' as entity_type
    from {{ ref('stg_sellers') }}
),
geographic_metrics as (
    select * from {{ ref('int_geographic_metrics') }}
)

select
    geo.state,
    geo.city,
    coalesce(gm.customer_count, 0) as customer_count,
    coalesce(gm.seller_count, 0) as seller_count,
    coalesce(gm.total_orders, 0) as total_orders,
    coalesce(gm.total_revenue, 0) as total_revenue,
    coalesce(gm.customer_seller_ratio, 0) as customer_seller_ratio,
    
    -- Region classification (Brazilian regions)
    case geo.state
        when 'AC' then 'Norte'
        when 'AL' then 'Nordeste'
        when 'AP' then 'Norte'
        when 'AM' then 'Norte'
        when 'BA' then 'Nordeste'
        when 'CE' then 'Nordeste'
        when 'DF' then 'Centro-Oeste'
        when 'ES' then 'Sudeste'
        when 'GO' then 'Centro-Oeste'
        when 'MA' then 'Nordeste'
        when 'MT' then 'Centro-Oeste'
        when 'MS' then 'Centro-Oeste'
        when 'MG' then 'Sudeste'
        when 'PA' then 'Norte'
        when 'PB' then 'Nordeste'
        when 'PR' then 'Sul'
        when 'PE' then 'Nordeste'
        when 'PI' then 'Nordeste'
        when 'RJ' then 'Sudeste'
        when 'RN' then 'Nordeste'
        when 'RS' then 'Sul'
        when 'RO' then 'Norte'
        when 'RR' then 'Norte'
        when 'SC' then 'Sul'
        when 'SP' then 'Sudeste'
        when 'SE' then 'Nordeste'
        when 'TO' then 'Norte'
        else 'Unknown'
    end as region

from (
    select distinct state, city
    from (
        select state, city from customers
        union
        select state, city from sellers
    )
) geo
left join geographic_metrics gm on geo.state = gm.state