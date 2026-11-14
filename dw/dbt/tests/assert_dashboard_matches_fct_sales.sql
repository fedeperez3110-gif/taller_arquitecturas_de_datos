-- Falla si la vista del tablero pierde o duplica ventas frente al hecho.
with dashboard as (
    select count(*) as rows_count, coalesce(sum(subtotal), 0) as subtotal from {{ ref('rpt_sales_dashboard') }}
),
facts as (
    select count(*) as rows_count, coalesce(sum(subtotal), 0) as subtotal from {{ ref('fct_sales') }}
)
select *
from dashboard
cross join facts
where dashboard.rows_count <> facts.rows_count
   or dashboard.subtotal <> facts.subtotal
