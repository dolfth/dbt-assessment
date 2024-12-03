select distinct

    {{ dbt_utils.generate_surrogate_key(['city', 'state', 
    'postal_code', 'country']) }} as location_key,    city,
    state,
    postal_code,
    country,
    case
        when postal_code[1] in ('1', '2') then 'north'
        when postal_code[1] in ('3', '4') then 'west'
        when postal_code[1] in ('5', '6') then 'south'
        when postal_code[1] in ('7', '8') then 'east'
        when postal_code[1] = '9' then 'APS'
        else '<unknown>'
    end as zone

from {{ ref('stg_sales') }}
