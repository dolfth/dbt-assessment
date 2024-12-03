select distinct

    {{ dbt_utils.generate_surrogate_key(['city', 'state', 
    'postal_code', 'country']) }} as location_key,
    city,
    state,
    postal_code,
    country

from {{ ref('stg_sales') }}
