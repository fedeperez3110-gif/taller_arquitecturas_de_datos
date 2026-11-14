-- 03. Comparar la presencia de una entidad entre días (Gold, pregunta 4).
-- Cambia 'petro' por la entidad normalizada que quieras (minúsculas, sin tildes).
-- Los días sin menciones aparecen con 0 gracias al LEFT JOIN contra todos los días con noticias.
WITH dias AS (
  SELECT published_date, sum(news_count) AS total_noticias
  FROM gold_news_by_day
  WHERE published_date <> 'unknown'
  GROUP BY published_date
), entidad AS (
  SELECT published_date, sum(news_count) AS news_count
  FROM gold_entity_daily
  WHERE entity_normalized = 'petro'
  GROUP BY published_date
)
SELECT d.published_date,
       coalesce(e.news_count, 0) AS news_count,
       coalesce(e.news_count, 0)
         - lag(coalesce(e.news_count, 0)) OVER (ORDER BY d.published_date) AS cambio_vs_dia_anterior,
       round(100.0 * coalesce(e.news_count, 0) / d.total_noticias, 1)    AS pct_noticias_del_dia
FROM dias d
LEFT JOIN entidad e ON e.published_date = d.published_date
ORDER BY d.published_date;
