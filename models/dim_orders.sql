select distinct

    order_id,
    status as order_status,
    sales_channel,
    b2b

from {{ ref('stg_sales') }}
