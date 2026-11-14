select
    cast(customer_id as bigint) as customer_id,
    nullif(trim(name), '') as name,
    nullif(lower(trim(email)), '') as email,
    nullif(trim(phone), '') as phone,
    nullif(trim(city), '') as city,
    nullif(trim(department), '') as department,
    cast(registration_date as date) as registration_date,
    cast(updated_at as timestamp) as updated_at,
    _batch_id,
    _extracted_at,
    _source
-- Una fila por llave: la versión de la extracción más reciente (raw_data acumula lotes).
from ({{ latest_per_key(source('mysql_raw', 'mysql_customers'), 'customer_id') }}) as raw_latest
