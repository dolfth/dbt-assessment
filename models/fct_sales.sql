select

    dd.date_key,
    sales.order_id,
    dp.product_key,
    dl.location_key,
    df.fulfillment_key,
    sales.quantity,
    sales.currency,
    sales.amount,
    sales.promotion_ids

from {{ ref('stg_sales') }} as sales

left join {{ ref('dim_dates') }} as dd
    on sales.order_date = dd.iso_date

left join {{ ref('dim_products') }} as dp
    on
        sales.sku = dp.sku
        and sales.asin = dp.asin

left join {{ ref('dim_locations') }} as dl
    on
        sales.city = dl.city
        and sales.state = dl.state
        and sales.postal_code = dl.postal_code
        and sales.country = dl.country

left join {{ ref('dim_fulfillments') }} as df
    on
        sales.fulfillment_provider = df.fulfillment_provider
        and sales.fulfilled_by = df.fulfilled_by
        and sales.shipment_service_level = df.shipment_service_level
        and sales.courier_status = df.courier_status
