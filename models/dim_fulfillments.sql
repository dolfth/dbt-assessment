select distinct

     {{ dbt_utils.generate_surrogate_key(['fulfillment_provider',
     'fulfilled_by', 'shipment_service_level', 'courier_status']) }} 
         as fulfillment_key,
    fulfillment_provider,
    fulfilled_by,
    shipment_service_level,
    courier_status


from {{ ref('stg_sales') }}
