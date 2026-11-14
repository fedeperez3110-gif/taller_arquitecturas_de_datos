-- 05. Conciliar una agregación de Gold contra Silver (noticias por día, medio y sección).
-- Debe devolver 0 filas con diferencia. Muestra lado a lado ambos valores.
WITH s AS (
  SELECT published_date, media, sec AS section, count(DISTINCT news_id) AS silver_count
  FROM silver_news CROSS JOIN UNNEST(sections) AS t(sec)
  GROUP BY 1, 2, 3
)
SELECT coalesce(s.published_date, g.published_date) AS published_date,
       coalesce(s.media, g.media)                   AS media,
       coalesce(s.section, g.section)               AS section,
       s.silver_count,
       g.news_count                                 AS gold_count,
       coalesce(s.silver_count, 0) - coalesce(g.news_count, 0) AS diferencia
FROM s
FULL OUTER JOIN gold_news_by_section g
  ON s.published_date = g.published_date AND s.media = g.media AND s.section = g.section
ORDER BY diferencia DESC, published_date, section;

-- 05b. Totales globales: registros recibidos vs noticias únicas publicadas.
-- Los recibidos son más porque incluyen duplicados entre feeds y lotes repetidos.
SELECT (SELECT sum(items_received) FROM batch_log)            AS registros_recibidos_todos_los_lotes,
       (SELECT count(*) FROM silver_news)                     AS noticias_unicas_vigentes,
       (SELECT sum(news_count) FROM gold_news_by_day)         AS noticias_en_gold;
