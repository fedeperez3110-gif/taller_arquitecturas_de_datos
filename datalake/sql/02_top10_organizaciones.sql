-- 02. Las diez organizaciones presentes en más noticias (Gold, pregunta 2).
-- Presencia = noticias distintas. Como cada noticia pertenece a una sola fecha,
-- sumar news_count entre días no repite noticias.
-- Cambia 'ORGANIZATION' por 'PERSON' o 'LOCATION' para las otras categorías.
SELECT entity_normalized,
       max_by(entity_text, news_count) AS entity_text,
       sum(news_count)                 AS news_count,
       sum(mention_count)              AS mention_count
FROM gold_entity_daily
WHERE entity_type = 'ORGANIZATION'
GROUP BY entity_normalized
ORDER BY news_count DESC, entity_normalized
LIMIT 10;
