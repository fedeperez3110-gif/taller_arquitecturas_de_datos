-- 07. Conteos de entrada y resultado por lote (registro del procesamiento).
SELECT run_id, source_id, bronze_status, items_received, rejected, duplicates_in_batch,
       "new", updated, unchanged, stale
FROM batch_log
ORDER BY run_id DESC, source_id;

-- 07b. Registros rechazados con motivo y trazabilidad hasta Bronze.
SELECT run_id, source_id, item_position, reasons, title_raw, link, bronze_key
FROM silver_quarantine
ORDER BY run_id DESC, source_id, item_position;

-- 07c. Noticias actualizadas (versión > 1) y lote del que viene el contenido vigente.
SELECT news_id, title, version, first_seen_run_id, content_run_id, bronze_key
FROM silver_news
WHERE version > 1;
