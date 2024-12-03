select distinct

    {{ dbt_utils.generate_surrogate_key(['sku', 'asin']) }} as product_key,
    sku,
    asin,
    style,
    category,
    size

from {{ ref('stg_sales') }}
