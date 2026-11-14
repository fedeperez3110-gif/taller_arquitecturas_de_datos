-- 04. Frases clave más frecuentes por sección (Gold, pregunta 3). Top 5 por sección.
WITH totales AS (
  SELECT section,
         phrase_normalized,
         max_by(phrase_text, news_count) AS phrase_text,
         sum(news_count)                 AS news_count
  FROM gold_phrase_by_section
  GROUP BY section, phrase_normalized
)
SELECT section, phrase_text, news_count
FROM (
  SELECT *, row_number() OVER (PARTITION BY section ORDER BY news_count DESC, phrase_normalized) AS rk
  FROM totales
)
WHERE rk <= 5
ORDER BY section, rk;
