# Guion de demostración — Data Warehouse (sección 4.4)

Duración: ~10 minutos. Antes de empezar (en tu computador, desde `dw/`):

```bash
./infra/dw.sh start                       # MySQL y DocumentDB (5-10 min si estaban detenidos)
(cd ../datalake && ./infra/ec2.sh start)  # EC2 de Airflow
./infra/dw.sh status                      # las tres fuentes en available
```

Ten abiertos: Airflow (vista **Graph** de `dw_ecommerce_pipeline`), el dashboard `Ventas e-commerce`
en QuickSight y una terminal en la EC2 (`cd ../datalake && ./infra/ec2.sh ssh`).

## 1. Procesar un nuevo lote desde las fuentes hasta el dashboard

1. **Valor actual.** En el dashboard, anota el KPI *Ventas totales*.
2. **Nuevos datos en las fuentes** (en la EC2):
   ```bash
   cd ~/datalake
   docker compose exec -T airflow-scheduler python -m dw.sources.append_batch --batch-id 3
   ```
   Inserta 3 pedidos con sus líneas en RDS MySQL y 3 reseñas y 3 eventos en DocumentDB.
3. **Ejecutar el pipeline.** En Airflow: *Trigger DAG w/ config* con los valores por defecto
   (`reprocess_batch_id` vacío, `extraction_mode=auto`, `simulate_failure=false`). Mostrar en Graph:
   validación → extracción → COPY → dbt (staging, snapshots, marts, pruebas) → conciliación → COMPLETE.
4. **Mostrar el lote.** Log de `extract_mysql`: `[batch_id=...][source=mysql][entity=orders] modo=incremental
   registros=3 marca=...`; en S3, `raw/mysql/orders/batch_id=<id>/` y `control/batches/batch_id=<id>/`.
5. **Dashboard.** Recargar la página: el KPI aumenta en el subtotal de las 3 líneas nuevas (Direct Query,
   no hay que refrescar SPICE).
6. **Comprobar con SQL** (en la EC2):
   ```bash
   docker compose exec -T airflow-scheduler python -m dw.dashboard_check
   ```
   Ejecuta `dbt/sql/quicksight_validation.sql`: el total coincide con el KPI y el último lote es el recién creado.

## 2. Cómo se identifica y recupera una ejecución fallida

1. *Trigger DAG w/ config* con `simulate_failure=true`.
2. En Graph: `load_redshift` hace el COPY, falla, reintenta 2 veces (30 s) y queda en rojo; `mark_failed`
   corre y el resto queda en `upstream_failed`.
3. **Identificar:** log de `load_redshift` → `[batch_id=<id>][entity=customers] FALLO SIMULADO` y
   `carga fallida: ROLLBACK, raw_data sin cambios`; log de `mark_failed` → tareas fallidas y comando de
   reproceso. En S3, `control/batches/batch_id=<id>/status.json` dice `FAILED`.
4. **Sin efectos parciales:** `select count(*) from raw_data.mysql_orders where _batch_id = '<id>'` → 0.
   La marca de avance sigue en el último lote COMPLETE.
5. **Recuperar:** *Trigger DAG w/ config* con `reprocess_batch_id=<id>` y `simulate_failure=false`.
   La extracción reutiliza los Parquet del lote (log: `ya extraído; se reutiliza S3 sin consultar la fuente`),
   la carga hace DELETE + COPY del lote y todo termina en verde con `status=COMPLETE`.
6. La conciliación del lote (`control/batches/batch_id=<id>/reconciliation.json`) confirma que no hubo
   pérdida ni duplicación.

La versión automatizada de ambos casos (escenarios 4 y 6) está en `evidencias/08_escenarios.md`.

## Al terminar

```bash
./infra/dw.sh stop                        # detiene MySQL y DocumentDB
(cd ../datalake && ./infra/ec2.sh stop)
```
