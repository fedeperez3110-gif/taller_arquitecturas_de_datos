-- V01. Unicidad del identificador de noticia en el conjunto vigente.
SELECT count(*) - count(DISTINCT news_id) AS violations
FROM silver_news
