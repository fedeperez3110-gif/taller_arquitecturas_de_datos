-- 06. Comparar bytes escaneados con y sin filtro de partición.
-- Ejecuta cada consulta por separado y anota "Data scanned" en la consola de Athena
-- (o revisa control/runs/.../validations.json, que guarda bytes_scanned por consulta).

-- 06a. SIN filtro de partición: lee todos los archivos de silver_news.
SELECT count(*) AS noticias, count(DISTINCT media) AS medios
FROM silver_news
WHERE pub_date_status = 'OK';

-- 06b. CON filtro de partición: solo lee los archivos del día indicado.
SELECT count(*) AS noticias, count(DISTINCT media) AS medios
FROM silver_news
WHERE published_date = '2026-11-11'
  AND pub_date_status = 'OK';
