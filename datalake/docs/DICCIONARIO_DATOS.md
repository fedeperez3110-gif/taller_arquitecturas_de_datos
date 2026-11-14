# Diccionario de datos — Silver y Gold

Todas las tablas están en Parquet (snappy) en el bucket único, registradas en la base de Glue `rss_datalake`.
Un archivo por partición (`part-00000.parquet`). La columna de partición no se guarda dentro del archivo: Athena la lee de la ruta.
Este documento se genera a partir de los esquemas de `src/datalake/parquet_io.py`, los mismos que usan el pipeline y `infra/terraform/glue_tables.tf`.

| Capa | Tabla | Ubicación | Partición |
|---|---|---|---|
| Silver | [`silver_news`](#silver_news) | `silver/news/` | `published_date` |
| Silver | [`silver_news_entities`](#silver_news_entities) | `silver/news_entities/` | `published_date` |
| Silver | [`silver_news_key_phrases`](#silver_news_key_phrases) | `silver/news_key_phrases/` | `published_date` |
| Silver | [`silver_quarantine`](#silver_quarantine) | `quarantine/silver_rejected/` | `run_id` |
| Control | [`batch_log`](#batch_log) | `control/batch_log/` | `run_id` |
| Gold | [`gold_news_by_section`](#gold_news_by_section) | `gold/news_by_section/` | `published_date` |
| Gold | [`gold_news_by_day`](#gold_news_by_day) | `gold/news_by_day/` | `published_date` |
| Gold | [`gold_entity_daily`](#gold_entity_daily) | `gold/entity_daily/` | `published_date` |
| Gold | [`gold_phrase_by_section`](#gold_phrase_by_section) | `gold/phrase_by_section/` | `published_date` |

## silver_news

Una fila por noticia **vigente** (`news_id` único). Se actualiza en cada lote; conserva valores originales y trazabilidad a Bronze.

Ubicación: `s3://<bucket>/silver/news/published_date=<valor>/part-00000.parquet`

| Columna | Tipo | Descripción |
|---|---|---|
| `published_date` *(partición)* | string | Fecha de publicación UTC `AAAA-MM-DD`, o `unknown` si la fecha falta o es inválida. |
| `news_id` | string | Identificador de la noticia: `sha256(medio | valor)[:32]`, donde valor es el GUID (normalizado si es URL) o, sin GUID, el enlace normalizado. **Clave única.** |
| `id_strategy` | string | `GUID` o `LINK`: de dónde salió el identificador. |
| `id_source_value` | string | Valor normalizado usado para calcular `news_id`. |
| `guid` | string | GUID tal como viene en el RSS (puede ser nulo). |
| `link` | string | Enlace a la noticia tal como viene en el RSS. Obligatorio, `http(s)`. |
| `title` | string | Título limpio (sin HTML ni entidades, espacios normalizados). Obligatorio. |
| `description` | string | Descripción limpia. Puede ser vacía. |
| `title_raw` | string | Título original del RSS. |
| `description_raw` | string | Descripción original del RSS. |
| `published_at_raw` | string | Fecha de publicación original (texto del RSS). |
| `published_at_utc` | timestamp (UTC) | Fecha de publicación convertida a UTC. Nula si la fecha falta o es inválida. |
| `pub_date_status` | string | `OK`, `TZ_ASSUMED` (sin zona; se asumió America/Bogota), `MISSING`, `INVALID`, `FUTURE` (> 1 día en el futuro). |
| `media` | string | Medio de comunicación (`el_tiempo`). |
| `sections` | array<string> | Todas las secciones en que ha aparecido la noticia (acumulado entre lotes y feeds). |
| `source_ids` | array<string> | Fuentes (feeds) en que ha aparecido. |
| `content_hash` | string | SHA-256 de título + descripción limpios. Detecta cambios de contenido y es la clave de la caché de Comprehend. |
| `version` | int | 1 al insertar; +1 cada vez que cambia `content_hash` en un lote posterior. |
| `first_seen_run_id` | string | Primer lote en que se vio la noticia. |
| `first_seen_at_utc` | timestamp (UTC) | Hora de ingesta de ese primer lote. |
| `last_seen_run_id` | string | Lote más reciente en que se vio. |
| `last_seen_at_utc` | timestamp (UTC) | Hora de ingesta de ese lote. |
| `content_run_id` | string | Lote del que viene el contenido vigente. |
| `content_ingested_at_utc` | timestamp (UTC) | Hora de ingesta del contenido vigente. |
| `bronze_key` | string | Ruta del `feed.xml` de Bronze del que viene el contenido vigente (trazabilidad). |

## silver_news_entities

Entidades detectadas por Comprehend. **Granularidad: una fila por (noticia, tipo, entidad normalizada)**; las repeticiones dentro del mismo texto se suman en `mention_count`.

Ubicación: `s3://<bucket>/silver/news_entities/published_date=<valor>/part-00000.parquet`

| Columna | Tipo | Descripción |
|---|---|---|
| `published_date` *(partición)* | string | Fecha de publicación UTC `AAAA-MM-DD`, o `unknown` si la fecha falta o es inválida. |
| `news_id` | string | Noticia a la que pertenece (FK a `silver_news.news_id`). |
| `content_hash` | string | Versión del texto analizado; coincide con `silver_news.content_hash` (validación v04). |
| `entity_type` | string | `PERSON`, `ORGANIZATION` o `LOCATION`. |
| `entity_text` | string | Forma más frecuente de la entidad en ese texto. |
| `entity_normalized` | string | Clave de agrupación: minúsculas, sin tildes ni puntuación en los extremos. |
| `mention_count` | int | Veces que aparece en el texto (informativo). |
| `max_score` | double | Confianza máxima de Comprehend (0–1). Se conservan ≥ 0,50. |
| `media` | string | Medio (copiado de la noticia). |
| `sections` | array<string> | Secciones de la noticia (copiado). |
| `published_at_utc` | timestamp (UTC) | Fecha de publicación de la noticia (copiado). |

## silver_news_key_phrases

Frases clave de Comprehend. **Granularidad: una fila por (noticia, frase normalizada)**.

Ubicación: `s3://<bucket>/silver/news_key_phrases/published_date=<valor>/part-00000.parquet`

| Columna | Tipo | Descripción |
|---|---|---|
| `published_date` *(partición)* | string | Fecha de publicación UTC `AAAA-MM-DD`, o `unknown` si la fecha falta o es inválida. |
| `news_id` | string | Noticia a la que pertenece (FK a `silver_news.news_id`). |
| `content_hash` | string | Versión del texto analizado (validación v05). |
| `phrase_text` | string | Forma más frecuente de la frase. |
| `phrase_normalized` | string | Clave de agrupación (minúsculas, sin tildes). Mínimo 3 caracteres, no solo números. |
| `mention_count` | int | Veces que aparece en el texto. |
| `max_score` | double | Confianza máxima (≥ 0,50). |
| `media` | string | Medio (copiado de la noticia). |
| `sections` | array<string> | Secciones de la noticia (copiado). |
| `published_at_utc` | timestamp (UTC) | Fecha de publicación (copiado). |

## silver_quarantine

Registros rechazados por las reglas de calidad. Una fila por ítem rechazado; se sobrescribe completa por lote.

Ubicación: `s3://<bucket>/quarantine/silver_rejected/run_id=<valor>/part-00000.parquet`

| Columna | Tipo | Descripción |
|---|---|---|
| `run_id` *(partición)* | string | Lote `AAAAMMDDTHHMMSSZ` (hora real UTC de inicio). |
| `source_id` | string | Fuente de la que vino. |
| `section` | string | Sección de la fuente. |
| `bronze_key` | string | `feed.xml` de Bronze que contiene el ítem. |
| `item_position` | int | Posición del `<item>` dentro del feed (0 = primero). |
| `reasons` | array<string> | Motivos: `MISSING_ID`, `MISSING_TITLE`, `INVALID_LINK`. |
| `guid` | string | GUID original. |
| `link` | string | Enlace original. |
| `title_raw` | string | Título original. |
| `description_raw` | string | Descripción original. |
| `published_at_raw` | string | Fecha original. |
| `rejected_at_utc` | timestamp (UTC) | Momento del rechazo. |

## batch_log

Registro del procesamiento: una fila por (lote, fuente). Cumple `items_received = rejected + duplicates_in_batch + new + updated + unchanged + stale` (validación v10).

Ubicación: `s3://<bucket>/control/batch_log/run_id=<valor>/part-00000.parquet`

| Columna | Tipo | Descripción |
|---|---|---|
| `run_id` *(partición)* | string | Lote `AAAAMMDDTHHMMSSZ` (hora real UTC de inicio). |
| `source_id` | string | Fuente. |
| `bronze_status` | string | Estado de la fuente en Bronze: `OK`, `OK_EMPTY_FEED`, `HTTP_ERROR`, `NETWORK_ERROR`, `MALFORMED_XML`, `EMPTY_RESPONSE`. |
| `items_received` | int | Ítems leídos del feed. |
| `rejected` | int | Enviados a cuarentena. |
| `duplicates_in_batch` | int | Repetidos dentro del mismo lote (misma noticia en otro feed o ítem). |
| `new` | int | Noticias que no existían. |
| `updated` | int | Texto distinto y lote posterior a la última vez que se vio → nueva versión. |
| `unchanged` | int | Mismo texto que la versión vigente. |
| `stale` | int | Texto distinto pero lote más antiguo: no se modifica la versión vigente. |
| `processed_at_utc` | timestamp (UTC) | Momento del procesamiento. |

## gold_news_by_section

**P1.** Noticias por día, medio y sección. Granularidad: (día, medio, sección). Una noticia presente en dos secciones cuenta en ambas.

Ubicación: `s3://<bucket>/gold/news_by_section/published_date=<valor>/part-00000.parquet`

| Columna | Tipo | Descripción |
|---|---|---|
| `published_date` *(partición)* | string | Fecha de publicación UTC `AAAA-MM-DD`, o `unknown` si la fecha falta o es inválida. |
| `media` | string | Medio. |
| `section` | string | Sección. |
| `news_count` | int | Noticias distintas (`count(DISTINCT news_id)`). |

## gold_news_by_day

**P1 (total).** Noticias distintas por día y medio, sin repetir por sección.

Ubicación: `s3://<bucket>/gold/news_by_day/published_date=<valor>/part-00000.parquet`

| Columna | Tipo | Descripción |
|---|---|---|
| `published_date` *(partición)* | string | Fecha de publicación UTC `AAAA-MM-DD`, o `unknown` si la fecha falta o es inválida. |
| `media` | string | Medio. |
| `news_count` | int | Noticias distintas del día. |
| `sections_count` | int | Secciones distintas con publicaciones ese día. |

## gold_entity_daily

**P2 y P4.** Presencia de cada entidad por día. Granularidad: (día, tipo, entidad normalizada). Sumar `news_count` entre días da noticias distintas (cada noticia tiene una sola fecha).

Ubicación: `s3://<bucket>/gold/entity_daily/published_date=<valor>/part-00000.parquet`

| Columna | Tipo | Descripción |
|---|---|---|
| `published_date` *(partición)* | string | Fecha de publicación UTC `AAAA-MM-DD`, o `unknown` si la fecha falta o es inválida. |
| `entity_type` | string | `PERSON`, `ORGANIZATION`, `LOCATION`. |
| `entity_normalized` | string | Clave de agrupación. |
| `entity_text` | string | Forma más frecuente ese día. |
| `news_count` | int | **Métrica de presencia:** noticias distintas que mencionan la entidad. |
| `mention_count` | int | Menciones totales (informativo; no usar como presencia). |

## gold_phrase_by_section

**P3.** Frases clave por sección y día. Granularidad: (día, sección, frase normalizada).

Ubicación: `s3://<bucket>/gold/phrase_by_section/published_date=<valor>/part-00000.parquet`

| Columna | Tipo | Descripción |
|---|---|---|
| `published_date` *(partición)* | string | Fecha de publicación UTC `AAAA-MM-DD`, o `unknown` si la fecha falta o es inválida. |
| `section` | string | Sección. |
| `phrase_normalized` | string | Clave de agrupación. |
| `phrase_text` | string | Forma más frecuente. |
| `news_count` | int | Noticias distintas de la sección que contienen la frase. |

## Archivos de control (JSON)

| Ruta | Contenido |
|---|---|
| `bronze/source=…/ingestion_date=…/run_id=…/manifest.json` | Metadatos de la extracción: URL, hora UTC, estado HTTP, estado de la fuente, ítems, SHA-256, ruta del XML. En lotes controlados: `controlled_copy_of`, `controlled_changes` |
| `control/airflow_runs/<run de Airflow>.json` | run_id del lote asignado a una ejecución de Airflow |
| `control/runs/run_id=…/bronze.json` | Resumen de la ingesta: `COMPLETE`, `PARTIAL` o `FAILED` y resultado por fuente |
| `control/runs/run_id=…/silver.json` | Totales de clasificación, noticias vigentes y particiones escritas |
| `control/runs/run_id=…/comprehend.json` | Textos analizados, desde caché, fallidos, truncados y filas generadas |
| `control/runs/run_id=…/gold.json` | Filas por tabla y particiones escritas |
| `control/runs/run_id=…/catalog.json` | Particiones agregadas y eliminadas por tabla en Glue |
| `control/runs/run_id=…/validations.json` | Resultado y bytes escaneados de v01–v10 |
| `control/runs/run_id=…/final.json` | Estado final del lote y de cada tarea |
| `cache/comprehend/<hh>/<content_hash>.json` | Respuesta de Comprehend (entidades y frases con offsets y score), idioma y versión del análisis |
