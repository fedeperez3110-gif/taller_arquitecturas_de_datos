-- V10. Identificación de registros nuevos, repetidos, actualizados y rechazados en el lote:
-- por cada fuente, recibidos = rechazados + duplicados + nuevos + actualizados + sin cambio + antiguos.
SELECT count(*) AS violations
FROM batch_log
WHERE run_id = '{run_id}'
  AND items_received <> rejected + duplicates_in_batch + "new" + updated + unchanged + stale
