-- V06. Conciliación Gold vs Silver: noticias por día, medio y sección.
WITH s AS (
  SELECT published_date, media, sec AS section, count(DISTINCT news_id) AS c
  FROM silver_news CROSS JOIN UNNEST(sections) AS t(sec)
  GROUP BY 1, 2, 3
), g AS (
  SELECT published_date, media, section, news_count AS c FROM gold_news_by_section
)
SELECT count(*) AS violations
FROM s FULL OUTER JOIN g
  ON s.published_date = g.published_date AND s.media = g.media AND s.section = g.section
WHERE coalesce(s.c, -1) <> coalesce(g.c, -1)
