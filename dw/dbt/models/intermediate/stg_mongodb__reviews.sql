select
    cast(review_id as bigint) as review_id,
    cast(customer_id as bigint) as customer_id,
    cast(product_id as bigint) as product_id,
    cast(rating as integer) as rating,
    cast(reviewed_at as timestamp) as reviewed_at,
    case lower(json_extract_path_text(document_json, 'verification', 'purchase_confirmed', true))
        when 'true' then true
        when 'false' then false
    end as is_verified_purchase,
    document_json,
    _batch_id,
    _extracted_at,
    _source
-- Una fila por llave: la versión de la extracción más reciente (raw_data acumula lotes).
from ({{ latest_per_key(source('mongodb_raw', 'mongo_reviews'), 'review_id') }}) as raw_latest
