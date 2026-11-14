-- V08. Conciliación Gold vs Silver: presencia de entidades (noticias distintas por día).
WITH s AS (
  SELECT e.published_date, e.entity_type, e.entity_normalized, count(DISTINCT e.news_id) AS c
  FROM silver_news_entities e
  JOIN silver_news n ON e.news_id = n.news_id AND e.content_hash = n.content_hash
  GROUP BY 1, 2, 3
)
SELECT count(*) AS violations
FROM s FULL OUTER JOIN gold_entity_daily g
  ON s.published_date = g.published_date AND s.entity_type = g.entity_type
 AND s.entity_normalized = g.entity_normalized
WHERE coalesce(s.c, -1) <> coalesce(g.news_count, -1)
