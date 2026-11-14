select
    cast(event_id as bigint) as event_id,
    nullif(trim(event_type), '') as event_type,
    cast(occurred_at as timestamp) as occurred_at,
    cast(customer_id as bigint) as customer_id,
    cast(product_id as bigint) as product_id,
    nullif(trim(sales_channel), '') as sales_channel,
    document_json,
    _batch_id,
    _extracted_at,
    _source
-- Una fila por llave: la versión de la extracción más reciente (raw_data acumula lotes).
from ({{ latest_per_key(source('mongodb_raw', 'mongo_navigation_events'), 'event_id') }}) as raw_latest
