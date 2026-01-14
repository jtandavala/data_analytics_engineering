select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix as zip_code_prefix,
    customer_city,
    customer_state
from {{ source('olist_raw', 'customers') }}