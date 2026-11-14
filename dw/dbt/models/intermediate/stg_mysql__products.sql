select
    cast(product_id as bigint) as product_id,
    nullif(trim(name), '') as name,
    nullif(trim(category), '') as category,
    cast(price as decimal(12, 2)) as price,
    nullif(trim(description), '') as description,
    cast(is_active as boolean) as is_active,
    cast(updated_at as timestamp) as updated_at,
    _batch_id,
    _extracted_at,
    _source
-- Una fila por llave: la versión de la extracción más reciente (raw_data acumula lotes).
from ({{ latest_per_key(source('mysql_raw', 'mysql_products'), 'product_id') }}) as raw_latest
