-- 01. Consultar noticias por fecha y sección (Silver).
-- Cambia el rango de fechas y la sección. El filtro sobre published_date (partición)
-- hace que Athena lea solo los archivos de esos días.
SELECT n.published_date,
       sec AS section,
       n.title,
       n.link,
       n.published_at_utc,
       n.version
FROM silver_news n
CROSS JOIN UNNEST(n.sections) AS t(sec)
WHERE n.published_date BETWEEN '2026-11-10' AND '2026-11-12'
  AND sec = 'medellin'
ORDER BY n.published_at_utc DESC;

-- 01b. Conteo por día, medio y sección (Gold, pregunta 1).
SELECT published_date, media, section, news_count
FROM gold_news_by_section
WHERE published_date BETWEEN '2026-11-10' AND '2026-11-12'
ORDER BY published_date, section;
