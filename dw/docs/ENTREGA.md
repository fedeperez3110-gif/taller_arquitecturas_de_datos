# Control de entrega — Data Warehouse

Lista de chequeo contra la sección 4 del enunciado. Leyenda: ✅ listo.

## 1. Repositorio de código

| Entregable | Estado | Dónde |
|---|---|---|
| DAG de Airflow | ✅ 14 tareas | `dags/dw_ecommerce_pipeline.py` |
| Scripts de extracción y carga | ✅ | `src/dw/extract.py`, `src/dw/load.py`, `src/dw/batches.py` |
| Generación y conexión de fuentes adaptadas | ✅ | `src/dw/sources/`, `src/dw/connections.py` |
| Proyecto dbt adaptado a Redshift | ✅ | `dbt/` (macros de compatibilidad en `dbt/macros/redshift_compat.sql`) |
| Configuración de despliegue sin secretos | ✅ | `infra/terraform/`, `../datalake/Dockerfile`, `../datalake/docker-compose.yaml` |
| Archivo de ejemplo con las variables | ✅ | `.env.example`, `infra/terraform/terraform.tfvars.example` |

## 2. Documentación

| Entregable | Estado | Dónde |
|---|---|---|
| Diagrama de arquitectura | ✅ | `docs/arquitectura.png` (fuente: `docs/arquitectura.mmd`) |
| Despliegue, ejecución y eliminación de recursos | ✅ | `README.md` › Despliegue |
| Decisiones técnicas y diferencias frente a la versión local | ✅ | `README.md` › Decisiones técnicas (incluye MongoDB → DocumentDB) |
| Cómo se identifica un lote y cuándo avanza la marca | ✅ | `README.md` › Cómo se identifica un lote |
| Estimación de costos y controles de seguridad | ✅ | `README.md` › Costos estimados, Controles de seguridad |

## 3. Evidencias de funcionamiento

Generadas automáticamente con `./infra/collect_evidence.sh` (código: `src/dw/evidence.py`).

| Evidencia pedida | Dónde |
|---|---|
| Ejecución exitosa del pipeline en Airflow | `evidencias/01_airflow_ejecuciones.md`, `02_logs_por_ejecucion.md` |
| Resultados de las pruebas de dbt | `evidencias/05_dbt_pruebas.md` (95 pruebas en verde) |
| Carga inicial, calidad, incrementalidad, idempotencia, SCD2 y recuperación | `evidencias/08_escenarios.md` (6/6 OK), `06_cuarentena.md`, `07_conciliacion.md` |
| Dashboard de QuickSight y consulta SQL que verifica el indicador | `evidencias/dashboard_quicksight.png`, `evidencias/09_quicksight_validacion.md`, `dbt/sql/quicksight_validation.sql` |

Indicador conciliado: ventas totales **7.420.730,58** en 259 líneas, igual en `marts.fct_sales`,
`marts.rpt_sales_dashboard`, el KPI del dashboard y la fuente MySQL menos las 6 líneas en cuarentena.

## 4. Demostración

Guion en `docs/DEMOSTRACION.md`: nuevo lote de las fuentes al dashboard y recuperación de una ejecución
fallida.
