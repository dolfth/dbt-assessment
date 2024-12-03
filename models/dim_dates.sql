select distinct

    strftime(order_date, '%Y%m%d')::INT as date_key,
    order_date as iso_date,
    extract(year from order_date) as year_of_date,
    extract(quarter from order_date) as quarter_of_year,
    extract(month from order_date) as month_of_year,
    extract(week from order_date) as week_of_year,
    extract(day from order_date) as day_of_month,
    extract(dayofweek from order_date) as day_of_week,
    dayname(order_date) as day_name

from {{ ref('stg_sales') }}
