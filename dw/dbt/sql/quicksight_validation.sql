-- Validación del tablero de QuickSight (ejecutar en Redshift Query Editor v2 o con psql).
-- El tablero consume marts.rpt_sales_dashboard en modo DIRECT QUERY (sin SPICE): cada vez que se
-- abre consulta Redshift, así que muestra el último lote COMPLETE sin pasos manuales.

-- 0) Fecha de actualización: último lote cargado en raw_data y hora de extracción.
select max(_batch_id) as ultimo_lote, max(_extracted_at) as extraido_utc
from raw_data.mysql_order_items;

-- 1) KPI "Ventas totales" SIN filtros: debe ser igual al KPI del tablero y a marts.fct_sales.
select sum(subtotal) as ventas_totales, count(*) as lineas
from marts.rpt_sales_dashboard;

-- 2) KPI con el MISMO filtro que se aplique en el tablero (ejemplo: canal = 'web').
--    Cambia el valor por el que selecciones en el control del tablero.
select sum(subtotal) as ventas_totales_filtradas, count(*) as lineas
from marts.rpt_sales_dashboard
where sales_channel = 'web';

-- 3) Gráfica "Ventas por mes y categoría": mismos valores que cada barra del visual.
select year, month, category, sum(subtotal) as ventas
from marts.rpt_sales_dashboard
group by year, month, category
order by year, month, category;

-- 4) Conciliación: la vista del tablero no pierde ni duplica ventas frente al hecho.
select (select sum(subtotal) from marts.rpt_sales_dashboard) as total_tablero,
       (select sum(subtotal) from marts.fct_sales)           as total_hecho;
