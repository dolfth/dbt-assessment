/*
    Clean up the raw data for further processing
    1. Reformat column names to snake cased for consistency and ease-of-use
    2. Rename column names for dimensional tables
    3. Drop index column as it seems not to be an ID
    3. Drop undefined column "Unnamed: 22"
    4. Remove nulls
    5. Parse postal code to Indian 6-digit format
    6. Remove duplicates (7 rows with order/asin duplicated)
    7. Correct typo in 'Fulfilment'


*/

with source_sales as (

    select * from {{ source("fashionable", "sales") }}

),

cleaned_sales as (

    select

        "Order ID" as order_id,
        date as order_date,
        status as status,
        fulfilment as fulfillment_provider,
        "Sales Channel" as sales_channel,
        style as style,
        sku as sku,
        category as category,
        size as size,
        asin as asin,
        qty as quantity,
        b2b as b2b,
        coalesce("Courier Status", '<unknown>') as courier_status,
        coalesce(currency, '<unknown>') as currency,
        coalesce(amount, 0) as amount,
        coalesce("ship-service-level") as shipment_service_level,
        coalesce("ship-city", '<unknown>') as city,
        coalesce("ship-state", '<unknown>') as state,
        coalesce(("ship-postal-code"::VARCHAR)[:6], '<unknown>')
        as postal_code,
        coalesce("ship-country", '<unknown>') as country,
        coalesce("promotion-ids", '<unknown>') as promotion_ids,
        coalesce("fulfilled-by", '<unknown>') as fulfilled_by,
        count(*) as cnt

    from source_sales
    group by all
    having cnt = 1
)

select * exclude (cnt) from cleaned_sales
