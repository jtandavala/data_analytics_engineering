{{
    config(
        materialized='table',
        description='Time spine model for MetricFlow semantic layer',
        tags=['metricflow', 'time_spine'],
        meta={
            "semantic_layer": {
                "time_spine": True
            }
        }
    )
}}

select
    cast(date_day as date) as date_day
from (
    select
        generate_series(
            '2016-01-01'::date,
            '2018-12-31'::date,
            '1 day'::interval
        )::date as date_day
) date_spine
