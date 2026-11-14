-- V03. Validez de fechas y enlaces:
--  * fecha válida -> published_at_utc no nulo y coherente con la partición
--  * fecha inválida/ausente -> partición 'unknown'
--  * enlaces http(s)
SELECT count(*) AS violations
FROM silver_news
WHERE (pub_date_status IN ('OK', 'TZ_ASSUMED')
       AND (published_at_utc IS NULL
            OR published_date <> date_format(published_at_utc, '%Y-%m-%d')))
   OR (pub_date_status NOT IN ('OK', 'TZ_ASSUMED') AND published_date <> 'unknown')
   OR NOT regexp_like(link, '^https?://[^ ]+$')
