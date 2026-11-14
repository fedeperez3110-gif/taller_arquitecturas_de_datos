-- V07. Conciliación Gold vs Silver: noticias distintas por día y medio.
WITH s AS (
  SELECT published_date, media, count(DISTINCT news_id) AS c FROM silver_news GROUP BY 1, 2
)
SELECT count(*) AS violations
FROM s FULL OUTER JOIN gold_news_by_day g
  ON s.published_date = g.published_date AND s.media = g.media
WHERE coalesce(s.c, -1) <> coalesce(g.news_count, -1)
