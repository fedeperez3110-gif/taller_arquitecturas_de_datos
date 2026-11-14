# Control de entrega — Data Lake

Lista de chequeo contra la sección 5 del enunciado. Las evidencias se generan automáticamente en `evidencias/` (ver sección 3).

Leyenda: ✅ listo · 🟡 parcial · ⏳ pendiente · 🟢 mecanismo listo, falta ejecutar

## Avance por fase

| Fase | Estado |
|---|---|
| 0. Infraestructura (Terraform, Airflow en EC2) | ✅ |
| 1–2. Fuentes RSS e ingesta Bronze | ✅ |
| 3. Silver (limpieza, validación, deduplicación, cuarentena) | ✅ |
| 4. Amazon Comprehend | ✅ |
| 5. Gold | ✅ |
| 6. Glue Data Catalog y Athena | ✅ |
| 7. DAG completo | ✅ |
| 8. Escenarios, documentación final y demostración | ✅ 8/8 escenarios, documentación completa y evidencias generadas en `evidencias/` |

## 1. Repositorio de código

| Entregable | Estado | Dónde |
|---|---|---|
| DAG de Airflow | ✅ 8 tareas |  `dags/rss_datalake_dag.py` |
| Scripts de ingesta | ✅ | `src/datalake/ingest_bronze.py` |
| Scripts de transformación | ✅ | `src/datalake/silver.py`, `gold.py`, `text_utils.py`, `parquet_io.py` |
| Script de enriquecimiento (Comprehend) | ✅ | `src/datalake/enrich.py` |
| Definiciones de tablas Glue | ✅ 9 tablas explícitas | `infra/terraform/glue_tables.tf`, registro de particiones en `src/datalake/catalog.py` |
| Consultas SQL de Athena | ✅ | `sql/01`–`07`, validaciones en `src/datalake/sql_validations/` |
| Despliegue y variables sin secretos | ✅ | `infra/terraform/`, `.env.example`, `.gitignore` |
| Pruebas automáticas | ✅ 72 pruebas | `tests/` |

## 2. Documentación

| Entregable | Estado | Dónde |
|---|---|---|
| Diagrama de arquitectura | ✅ flujo de datos y despliegue (Mermaid + PNG) | `docs/ARQUITECTURA.md` |
| Fuentes RSS seleccionadas | ✅ | README › Fuentes seleccionadas |
| Diccionario de datos Silver | ✅ 5 tablas + archivos de control | `docs/DICCIONARIO_DATOS.md` |
| Diccionario de datos Gold | ✅ 4 tablas con granularidad y métricas | `docs/DICCIONARIO_DATOS.md` |
| Reglas de calidad, identificación y actualización | ✅ | README › Capa Silver |
| Estrategia de incrementalidad y recuperación | ✅ | README › Incrementalidad y recuperación |
| Instrucciones de despliegue, ejecución y eliminación | ✅ incluye problemas frecuentes | README › Despliegue, ejecución y eliminación |
| Estimación de costos | ✅ laboratorio ≈ 7,80 USD/mes; 24/7 ≈ 69,30 USD/mes | `docs/COSTOS.md` |

## 3. Evidencias

Se generan **automáticamente** (no se requieren capturas; el enunciado indica que las capturas solo
complementan la entrega). Desde tu computador:

```bash
./infra/collect_evidence.sh      # con la EC2 encendida; deja todo en evidencias/
```

| Evidencia pedida (5.3) | Archivo generado |
|---|---|
| Ejecución del DAG en Airflow | `evidencias/01_airflow_ejecuciones.md`, `02_logs_por_ejecucion.md` |
| Archivos de Bronze, Silver y Gold | `evidencias/03_archivos_s3.md` |
| Resultados de Comprehend | `evidencias/04_comprehend.md` |
| Tablas y particiones en Glue | `evidencias/05_glue_catalog.md` |
| Consultas y resultados en Athena (incl. bytes con y sin partición) | `evidencias/06_athena_consultas.md` |
| Validación de calidad, incrementalidad y recuperación | `evidencias/07_validaciones.md`, `08_escenarios.md` |

Regenerar al final, después de ejecutar todos los escenarios, para que queden incluidos.

## 4. Escenarios de la sección 4.5

Cómo provocar y verificar cada uno: README › Cómo demostrar los escenarios.

| Escenario | Estado | Evidencia |
|---|---|---|
| Carga inicial | ✅ | `08_escenarios.md` |
| Registros defectuosos | ✅ lote controlado `20261113T211328Z`: 2 rechazados con motivo | `08_escenarios.md` |
| Repetición de un lote exitoso | ✅ `20261113T211328Z` repetido: nada reescrito en Silver, Comprehend ni Gold | `08_escenarios.md` |
| Nuevo lote incremental | ✅ lote `20261112T060000Z`: 30 nuevas, 30 desde caché | `08_escenarios.md` |
| Noticia con contenido modificado | ✅ lote `20261113T211328Z`: 2 actualizadas, Comprehend solo 3 textos | `08_escenarios.md` |
| Fallo controlado y reejecución | ✅ lote `20261113T212209Z`: `build_gold` falla, reintento automático, validaciones en 0 | `08_escenarios.md` |
| Reprocesamiento desde Bronze | ✅ | `08_escenarios.md` |
| Nueva partición | ✅ `2026-11-13` registrada en Glue automáticamente | `08_escenarios.md` |

La explicación de una limitación del enriquecimiento está en README › Limitaciones del enriquecimiento.

Validaciones exigidas (unicidad, campos obligatorios, fechas y enlaces, entidades sin noticia,
conciliación Silver–Gold, clasificación nuevo/repetido/actualizado/rechazado): ✅ implementadas como
v01–v10 en `src/datalake/sql_validations/`; el DAG las ejecuta en Athena en cada lote.

## 5. Demostración

✅ Guion paso a paso con tiempos, comandos, resultados esperados, explicación de una limitación del enriquecimiento y preguntas probables: `docs/DEMOSTRACION.md`.

## Fuera de alcance por ahora

El Data Warehouse (sección 4 del enunciado) tiene sus propios entregables y se retomará después.

## Hallazgos durante las pruebas (útiles para la sustentación)

- **run_id con fecha lógica de Airflow:** una ejecución programada que corrió tarde quedó con fecha de ingesta falsa (`20261112T060000Z`, descargado el 13). Corregido: el run_id usa la hora real de inicio.
- **Regresión de metadatos al reprocesar un lote intermedio:** reprocesar el lote controlado después de una descarga real devolvía fechas antiguas. Corregido: los cambios se aplican solo si el lote es posterior a la última vez que se vio la noticia (pruebas en `tests/test_silver.py`).
