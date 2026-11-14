-- V05. Ausencia de frases clave sin una noticia vigente asociada.
SELECT count(*) AS violations
FROM silver_news_key_phrases p
LEFT JOIN silver_news n ON p.news_id = n.news_id AND p.content_hash = n.content_hash
WHERE n.news_id IS NULL
