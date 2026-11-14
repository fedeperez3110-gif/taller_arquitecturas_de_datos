-- V04. Ausencia de entidades sin una noticia vigente asociada (misma noticia y misma versión del texto).
SELECT count(*) AS violations
FROM silver_news_entities e
LEFT JOIN silver_news n ON e.news_id = n.news_id AND e.content_hash = n.content_hash
WHERE n.news_id IS NULL
