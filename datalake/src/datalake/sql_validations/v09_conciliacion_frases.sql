-- V09. Conciliación Gold vs Silver: frases clave por sección y día.
WITH s AS (
  SELECT p.published_date, sec AS section, p.phrase_normalized, count(DISTINCT p.news_id) AS c
  FROM silver_news_key_phrases p
  JOIN silver_news n ON p.news_id = n.news_id AND p.content_hash = n.content_hash
  CROSS JOIN UNNEST(n.sections) AS t(sec)
  GROUP BY 1, 2, 3
)
SELECT count(*) AS violations
FROM s FULL OUTER JOIN gold_phrase_by_section g
  ON s.published_date = g.published_date AND s.section = g.section
 AND s.phrase_normalized = g.phrase_normalized
WHERE coalesce(s.c, -1) <> coalesce(g.news_count, -1)
