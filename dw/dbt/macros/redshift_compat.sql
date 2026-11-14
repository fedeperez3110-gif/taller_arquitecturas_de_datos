{# Equivalencias para Redshift de funciones que el proyecto local usaba en DuckDB. #}

{# Versión vigente de cada llave en raw_data: la extracción más reciente (raw acumula lotes). #}
{% macro latest_per_key(relation, key) %}
select *
from (
    select
        src.*,
        row_number() over (
            partition by src.{{ key }}
            order by src._extracted_at desc, src._batch_id desc
        ) as _version_rank
    from {{ relation }} as src
) as ranked
where _version_rank = 1
{% endmacro %}

{# DuckDB: timezone('America/Bogota', ts). Las fechas de DocumentDB se guardan en UTC sin zona. #}
{% macro to_business_time(utc_timestamp) -%}
convert_timezone('UTC', '{{ var("business_timezone") }}', {{ utc_timestamp }})
{%- endmacro %}

{# DuckDB/PostgreSQL: a IS DISTINCT FROM b (no soportado oficialmente en Redshift). #}
{% macro null_safe_distinct(a, b) -%}
(({{ a }} <> {{ b }}) or ({{ a }} is null and {{ b }} is not null) or ({{ a }} is not null and {{ b }} is null))
{%- endmacro %}

{# DuckDB: extract(isodow from d) -> 1 = lunes ... 7 = domingo. En Redshift dow va de 0 (domingo) a 6. #}
{% macro iso_day_of_week(d) -%}
case when date_part(dow, {{ d }}) = 0 then 7 else cast(date_part(dow, {{ d }}) as integer) end
{%- endmacro %}
