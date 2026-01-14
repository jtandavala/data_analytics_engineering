{{
    config(
        materialized='table',
        description='Date dimension for time intelligence',
        tags=['core', 'dimension', 'conformed']
    )
}}

with date_spine as (
    select
        generate_series(
            '2016-01-01'::date,
            '2018-12-31'::date,
            '1 day'::interval
        )::date as date_day
)

select
    date_day as date_key,
    date_day as date,
    extract(year from date_day) as year,
    extract(quarter from date_day) as quarter,
    extract(month from date_day) as month,
    extract(week from date_day) as week_of_year,
    extract(day from date_day) as day_of_month,
    extract(dow from date_day) as day_of_week,
    to_char(date_day, 'Day') as day_name,
    to_char(date_day, 'Month') as month_name,
    case when extract(dow from date_day) in (0, 6) then true else false end as is_weekend,
    case when extract(month from date_day) in (12, 1, 2) then 'Summer'
         when extract(month from date_day) in (3, 4, 5) then 'Fall'
         when extract(month from date_day) in (6, 7, 8) then 'Winter'
         else 'Spring' end as season_br,
    date_trunc('month', date_day) as month_start,
    date_trunc('quarter', date_day) as quarter_start,
    date_trunc('year', date_day) as year_start

from date_spine