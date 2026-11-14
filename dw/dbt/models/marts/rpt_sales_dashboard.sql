-- Vista para el tablero de QuickSight: una fila por línea de venta con los atributos de las
-- dimensiones ya resueltos (versión del cliente/producto vigente en la fecha de la venta).
{{ config(materialized='view') }}

select
    sales.sale_sk,
    dates.date as sale_date,
    dates.year,
    dates.month,
    dates.day_of_week,
    dates.is_holiday,
    product.category,
    product.name as product_name,
    channel.sales_channel,
    geography.department,
    geography.city,
    sales.quantity,
    sales.unit_price,
    sales.subtotal
from {{ ref('fct_sales') }} as sales
left join {{ ref('dim_date') }} as dates on dates.date_sk = sales.date_sk
left join {{ ref('dim_product') }} as product on product.product_sk = sales.product_sk
left join {{ ref('dim_sales_channel') }} as channel on channel.sales_channel_sk = sales.sales_channel_sk
left join {{ ref('dim_geography') }} as geography on geography.geography_sk = sales.geography_sk
