{# Filtro de cargas incrementales para la fila candidata (alias candidate). Carga las llaves nuevas, las filas cuya fecha de negocio cae en la ventana de reproceso (desde incremental_lookback_days antes de la fecha máxima ya cargada) y las filas cuyas llaves de dimensión resueltas cambiaron (tracked_columns), por ejemplo cuando una versión SCD2 nueva reemplaza a la anterior.
   Redshift no soporta subconsultas correlacionadas (EXISTS) combinadas con OR, así que la versión de
   DuckDB se reescribió con LEFT JOIN contra la tabla existente y un CROSS JOIN con el límite de la ventana.
   La macro se coloca justo después de "from <cte> as candidate". #}
{% macro incremental_candidate_filter(key_column, business_date_column, tracked_columns=[]) %}
{% if is_incremental() %}
left join {{ this }} as existing
    on existing.{{ key_column }} = candidate.{{ key_column }}
cross join (
    select dateadd(day, -{{ var('incremental_lookback_days') }}, max(dates.date)) as lookback_from
    from {{ this }} as loaded
    inner join {{ ref('dim_date') }} as dates on dates.date_sk = loaded.date_sk
) as bounds
where existing.{{ key_column }} is null
or candidate.{{ business_date_column }} >= bounds.lookback_from
{% for column in tracked_columns %}
or {{ null_safe_distinct('existing.' ~ column, 'candidate.' ~ column) }}
{% endfor %}
{% endif %}
{% endmacro %}

{# Elimina de la tabla de hechos las filas cuya llave de origen ya no existe en la capa validada. #}
{% macro delete_orphaned_facts(sk_column, source_relation, source_key) %}
delete from {{ this }}
where {{ sk_column }} not in (
    select {{ dbt_utils.generate_surrogate_key(['cast(' ~ source_key ~ ' as varchar)']) }}
    from {{ source_relation }}
)
{% endmacro %}
