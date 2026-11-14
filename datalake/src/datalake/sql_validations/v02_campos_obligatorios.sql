-- V02. Presencia de campos obligatorios en el conjunto vigente.
SELECT count(*) AS violations
FROM silver_news
WHERE news_id IS NULL OR trim(coalesce(title, '')) = '' OR link IS NULL
   OR content_hash IS NULL OR cardinality(sections) = 0
