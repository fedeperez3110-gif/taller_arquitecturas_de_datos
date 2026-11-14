# Data Lake de noticias RSS — El Tiempo

Pipeline en AWS: RSS → S3 (Bronze / Silver / Gold) → Amazon Comprehend → AWS Glue Data Catalog → Amazon Athena, orquestado con Apache Airflow en EC2.

> **Estado:** Data Lake completo y verificado en AWS (`us-east-2`): Bronze → Silver → Comprehend → Gold → Glue → validaciones en Athena, con los 8 escenarios de la sección 4.5 demostrados.
>
> Documentación: [arquitectura](docs/ARQUITECTURA.md) · [diccionario de datos](docs/DICCIONARIO_DATOS.md) · [costos](docs/COSTOS.md) · [guion de demostración](docs/DEMOSTRACION.md) · [control de entrega](docs/ENTREGA.md)

## Estructura

```
config/sources.yaml        Fuentes RSS (medio, sección, URL)
sql/                       Consultas de negocio para Athena (01–07)
src/datalake/sql_validations/  Validaciones que el DAG ejecuta en Athena en cada lote
docs/ARQUITECTURA.md       Diagrama y componentes
docs/DICCIONARIO_DATOS.md  Columnas de Silver y Gold
docs/COSTOS.md             Estimación de costos
docs/DEMOSTRACION.md       Guion de la demostración
docs/ENTREGA.md            Control de entregables
dags/rss_datalake_dag.py   DAG de Airflow
src/datalake/              Código del pipeline (ingesta, transformaciones…)
tests/                     Pruebas con S3 simulado (moto)
infra/terraform/           Infraestructura como código (S3, IAM, EC2, Glue, Athena, presupuesto)
infra/ec2.sh               Encender / apagar / conectarse a la EC2
infra/sync_to_ec2.sh       Copiar el código local a la EC2
infra/collect_evidence.sh  Generar las evidencias en la EC2 y descargarlas a evidencias/
evidencias/                Evidencias de funcionamiento generadas automáticamente
docker-compose.yaml        Airflow 2.10 (LocalExecutor + Postgres); también monta ../dw (Data Warehouse)
.env.example               Variables requeridas, sin secretos
```

## Fuentes seleccionadas

| source_id | Sección | URL |
|---|---|---|
| eltiempo_medellin | Medellín | https://www.eltiempo.com/rss/colombia_medellin.xml |
| eltiempo_economia | Economía | https://www.eltiempo.com/rss/economia.xml |
| eltiempo_justicia | Justicia | https://www.eltiempo.com/rss/justicia.xml |

Observaciones (verificado el 2026-10-22): RSS 2.0, ~10 ítems por feed, `guid` igual al `link`, `pubDate` de los ítems en ISO 8601 con zona horaria (`2026-10-22T06:11:08-05:00`), títulos con entidades HTML (`&#039;`). Una misma noticia puede aparecer en varios feeds (p. ej. una nota de `economia/sectores` publicada también en Justicia).

## Organización del bucket (un solo bucket)

```
bronze/source=<fuente>/ingestion_date=AAAA-MM-DD/run_id=<id>/feed.xml        XML original
bronze/source=<fuente>/ingestion_date=AAAA-MM-DD/run_id=<id>/manifest.json   metadatos del lote
silver/news/published_date=AAAA-MM-DD|unknown/part-00000.parquet             noticias vigentes
silver/news_entities/published_date=.../part-00000.parquet                   entidades por noticia
silver/news_key_phrases/published_date=.../part-00000.parquet                frases clave por noticia
cache/comprehend/<hh>/<content_hash>.json                                     respuesta de Comprehend por texto
gold/news_by_section|news_by_day|entity_daily|phrase_by_section/published_date=.../part-00000.parquet
quarantine/bronze_rejected/...                                                respuestas HTTP/XML inválidas
quarantine/silver_rejected/run_id=<id>/part-00000.parquet                     noticias rechazadas + motivo
control/batch_log/run_id=<id>/part-00000.parquet                              conteos por lote y fuente
control/runs/run_id=<id>/bronze.json | silver.json | comprehend.json | gold.json | catalog.json | validations.json | final.json               estado del lote
athena-results/   (expira a los 7 días)
```

`manifest.json` registra: run_id, fuente, URL, fecha/hora UTC, estado HTTP, estado de la fuente, cantidad de ítems, SHA-256 del contenido y ruta del archivo original.

## Capa Silver

### Reglas de identificación
- `news_id = sha256(medio | valor)[:32]`. El valor es el `guid` (normalizado si es URL); si no hay `guid`, el enlace normalizado.
- Normalización de URL: esquema `https`, dominio en minúsculas, sin `#fragmento`, sin parámetros de seguimiento (`utm_*`, `fbclid`, `gclid`…), sin barra final.
- **Noticias en varios feeds:** una sola fila en `silver/news`; las columnas `sections` y `source_ids` guardan todas las secciones/fuentes en que apareció (acumuladas entre lotes).
- **Limitaciones:** si el medio cambia el `guid` o la URL de una nota, se trata como una noticia distinta; dos notas distintas con la misma URL se fusionarían; si una noticia llega con textos distintos en dos feeds del mismo lote, se conserva la primera (orden alfabético de fuente).

### Reglas de calidad
| Regla | Tratamiento |
|---|---|
| Sin `guid` ni enlace válido | Cuarentena `MISSING_ID` |
| Título vacío tras limpiar HTML | Cuarentena `MISSING_TITLE` |
| Enlace ausente o no `http(s)` | Cuarentena `INVALID_LINK` |
| Fecha ausente / no interpretable / más de 1 día en el futuro | Se conserva la noticia con `published_at_utc` nulo, `pub_date_status` = `MISSING` / `INVALID` / `FUTURE` y partición `published_date=unknown` |
| Fecha sin zona horaria | Se asume `America/Bogota`, `pub_date_status = TZ_ASSUMED` |
| Descripción vacía | Se permite (se analiza solo el título) |

Siempre se conserva el valor original (`title_raw`, `description_raw`, `published_at_raw`) y la referencia al archivo de Bronze (`bronze_key`, `content_run_id`).

### Clasificación por lote y actualización
| Estado | Condición | Efecto en Silver |
|---|---|---|
| `new` | `news_id` no existía | Se inserta con `version = 1` |
| `unchanged` | Mismo `content_hash` (título + descripción limpios) | Se unen `sections`; si el lote es posterior a la última vez que se vio, se toman sus metadatos (fecha, enlace) y `last_seen_*` |
| `updated` | Texto distinto y el lote es posterior a la **última vez que se vio** la noticia | Se reemplaza el contenido, `version + 1` |
| `stale` | Texto distinto pero el lote no es posterior a la última vez que se vio la noticia (reproceso de un lote viejo o intermedio) | No se modifica el contenido vigente |
| `duplicates_in_batch` | Repetida en el mismo lote (otro feed u otro ítem) | Se fusiona en una sola fila |
| `rejected` | Falla una regla obligatoria | `quarantine/silver_rejected/run_id=<id>/` |

**Conciliación por lote y fuente:** `items_received = rejected + duplicates_in_batch + new + updated + unchanged + stale`. Por eso los registros recibidos siempre son más que las noticias únicas publicadas.

### Publicación sin resultados duplicados ni parciales
- Cada partición es **un único archivo** con nombre fijo (`part-00000.parquet`). Reescribirla es un `PUT` atómico en S3: Athena ve la versión anterior completa o la nueva completa.
- Solo se reescriben las particiones que cambiaron. Si una noticia cambia de fecha, se reescriben la partición vieja (sin ella) y la nueva.
- Reprocesar un lote produce exactamente los mismos archivos (idempotencia). Si un fallo ocurre entre la escritura de dos particiones, la reejecución completa el resultado porque la clasificación se recalcula contra el estado publicado.
- La cuarentena y el registro del lote se sobrescriben completos por `run_id`.

## Enriquecimiento con Amazon Comprehend

- **Texto analizado:** `título + "\n" + descripción` limpios de `silver/news`, idioma `es`. APIs: `BatchDetectEntities` y `BatchDetectKeyPhrases` (hasta 25 textos por llamada, reintentos adaptativos ante throttling).
- **Caché por texto:** la respuesta se guarda en `cache/comprehend/<hh>/<content_hash>.json`. Una noticia sin cambios en título o descripción conserva su `content_hash` y **no se vuelve a analizar**, ni en lotes nuevos ni en reprocesos. Si el texto cambia, cambia el hash y se analiza la nueva versión. Si cambian las reglas del análisis se incrementa `ANALYSIS_VERSION` en `src/datalake/enrich.py` para forzar el reanálisis.
- **Entidades conservadas:** `PERSON`, `ORGANIZATION`, `LOCATION` con confianza ≥ 0,50. Frases clave con confianza ≥ 0,50, al menos 3 caracteres y no solo números.
- **Granularidad sin inflar conteos:** `news_entities` tiene **una fila por (noticia, tipo, entidad normalizada)**; las repeticiones dentro del mismo texto se suman en `mention_count`. Igual para `news_key_phrases`. Para medir presencia se cuentan `news_id` distintos.
- **Normalización:** `entity_normalized` / `phrase_normalized` en minúsculas, sin tildes, sin puntuación en los extremos (`"Bogotá"` y `"BOGOTA"` → `bogota`). `entity_text` guarda la forma más frecuente.
- **Coherencia con Silver:** las tablas se reconstruyen a partir del conjunto vigente y la caché; cada fila guarda el `content_hash` de la versión analizada. Nunca quedan entidades de noticias que ya no existen o de versiones anteriores. Solo se reescriben las particiones cuyo contenido cambió.
- **Errores:** si Comprehend falla para un texto, no se guarda en caché, la noticia queda sin entidades en ese lote y se reintenta en el siguiente. Si falla la llamada completa (permisos, servicio caído), la tarea falla y Airflow la reintenta.
- **Textos largos:** se recortan a 4 500 bytes UTF-8 (límite de 5 000 bytes por documento en las APIs Batch) y se cuentan en `texts_truncated`.

### Limitaciones del enriquecimiento
- Comprehend no resuelve correferencias: "Gustavo Petro", "Petro" y "el presidente" son entidades distintas.
- Solo se analiza título y descripción del RSS (no el cuerpo completo de la noticia), así que se pierden entidades que solo aparecen en el artículo.
- Puede clasificar mal nombres ambiguos (p. ej. "Nacional" como organización o lugar) y la confianza es un indicador, no una garantía.

## Capa Gold

Se **recalcula completa desde el conjunto vigente de Silver** en cada lote (no se suman lotes), por lo que repetir o reprocesar un lote nunca produce doble conteo. Solo se reescriben las particiones cuyo contenido cambió. Todas las tablas están particionadas por `published_date` (fecha de publicación en UTC); las noticias sin fecha válida van a `published_date=unknown`: cuentan en los totales generales pero no en las series por día.

| Tabla | Pregunta | Granularidad | Métricas |
|---|---|---|---|
| `gold_news_by_section` | P1 | día × medio × sección | `news_count`: noticias distintas. Una noticia en 2 secciones cuenta en ambas |
| `gold_news_by_day` | P1 | día × medio | `news_count`: noticias distintas (sin repetir por sección); `sections_count` |
| `gold_entity_daily` | P2, P4 | día × tipo × entidad normalizada | `news_count`: noticias distintas que la mencionan (**métrica de presencia**); `mention_count`: menciones totales, solo informativo |
| `gold_phrase_by_section` | P3 | día × sección × frase normalizada | `news_count`: noticias distintas |

Como cada noticia pertenece a una sola fecha, sumar `news_count` entre días da noticias distintas en el rango.

## Glue Data Catalog y Athena

- **Tablas explícitas en Terraform** (`infra/terraform/glue_tables.tf`), no crawlers: ubicación S3, columnas y tipos, formato Parquet (SerDe `ParquetHiveSerDe`) y clave de partición. Silver: `silver_news`, `silver_news_entities`, `silver_news_key_phrases`, `silver_quarantine`, `batch_log`. Gold: `gold_news_by_section`, `gold_news_by_day`, `gold_entity_daily`, `gold_phrase_by_section`.
- **Particiones nuevas:** la tarea `register_partitions` compara las particiones en S3 con las del catálogo y ejecuta `BatchCreatePartition` para las nuevas y `BatchDeletePartition` para las que quedaron vacías (`src/datalake/catalog.py`). Una partición nueva queda consultable en cuanto termina el lote, sin `MSCK REPAIR TABLE` ni crawlers.
- **Workgroup `rss-datalake`:** resultados cifrados en `athena-results/` (expiran a los 7 días) y límite de 1 GiB escaneado por consulta.
- **Consultas de negocio:** `sql/01` a `sql/07` (noticias por fecha y sección, top 10 organizaciones, presencia de una entidad entre días, frases por sección, conciliación Gold–Silver, bytes con y sin filtro de partición, calidad por lote).

### Validaciones automáticas en cada lote (tarea `validate_with_athena`)
Cada consulta de `src/datalake/sql_validations/` devuelve `violations`; si alguna es distinta de 0 la tarea falla y el lote queda `FAILED`. Resultado y bytes escaneados en `control/runs/run_id=<id>/validations.json`.

| Validación | Qué comprueba |
|---|---|
| v01 | Unicidad de `news_id` en el conjunto vigente |
| v02 | Campos obligatorios presentes |
| v03 | Fechas coherentes con su partición y enlaces `http(s)` |
| v04, v05 | Ninguna entidad o frase sin noticia vigente asociada (misma versión del texto) |
| v06–v09 | Conciliación de cada tabla de Gold contra Silver |
| v10 | En el lote, `recibidos = rechazados + duplicados + nuevos + actualizados + sin cambio + antiguos` |

## Incrementalidad y recuperación

**Identificación del lote.** Cada ejecución tiene un `run_id` (`AAAAMMDDTHHMMSSZ`) con la **hora real UTC** en que empezó; se guarda en `control/airflow_runs/<run de Airflow>.json` y los reintentos o reejecuciones de esa misma ejecución reutilizan el mismo `run_id`. Un lote se considera **procesado en Bronze** cuando existe su `manifest.json` con estado exitoso (se escribe al final, después de `feed.xml`), y **completo** cuando existe `control/runs/run_id=<id>/final.json` con `final_status` = `COMPLETE` (o `PARTIAL` si alguna fuente falló).

**Incrementalidad.**
- Bronze: cada lote nuevo se guarda en su propio `run_id`; nunca sobrescribe otros lotes.
- Silver: solo cambian las noticias `new` / `updated`; las `unchanged` no reescriben su partición.
- Comprehend: solo se analizan textos sin caché (noticias nuevas o con texto modificado).
- Gold: se recalcula desde Silver, pero solo se reescriben las particiones cuyo resultado cambió.
- Glue: solo se registran las particiones nuevas.

**Recuperación.**
- Toda escritura es idempotente: reejecutar una tarea o el lote completo produce los mismos archivos.
- Cada partición es un único archivo con nombre fijo y se reemplaza con un `PUT` atómico de S3, así que Athena nunca ve archivos a medias ni copias duplicadas.
- Reintentos: 2 por tarea con espera exponencial (2 a 10 min). Si Bronze queda `PARTIAL`, el reintento solo vuelve a consultar las fuentes fallidas; si siguen fallando, el lote continúa con las válidas y queda `PARTIAL`.
- `max_active_runs=1`: nunca hay dos lotes escribiendo a la vez.
- **Reproceso desde Bronze:** disparar el DAG con `reprocess_run_id` reconstruye Silver, Comprehend (desde caché), Gold y Glue sin consultar los RSS.
- **Fallo controlado:** disparar el DAG con `simulate_failure` = nombre de una tarea hace que esa tarea falle a propósito en su primer intento, después de escribir sus resultados. El reintento automático debe terminar en `success` sin duplicar ni perder datos (las validaciones v01–v10 lo comprueban).

## Cómo demostrar los escenarios de la sección 4.5

Todos los comandos `docker compose` se ejecutan en la EC2, dentro de `~/datalake`. "Trigger" = botón ▶ del DAG en Airflow.

| Escenario | Cómo se provoca | Qué se verifica |
|---|---|---|
| Carga inicial | Trigger con parámetros vacíos | 8 tareas en verde; datos en Bronze, Silver y Gold; consultas `sql/01`–`05` en Athena |
| Repetición de un lote exitoso | Trigger con `reprocess_run_id` = un lote ya procesado | Silver `nuevos=0`, `particiones=[]`; Comprehend `analizados=0`; Gold sin particiones escritas; v01 y v06–v09 en 0 |
| Nuevo lote incremental | Trigger con parámetros vacíos horas después (o esperar la ejecución programada) | Silver muestra `nuevos` > 0 y el resto `sin_cambio`; Comprehend solo analiza las nuevas |
| Reprocesamiento desde Bronze | Igual que la repetición; el log de `ingest_bronze` dice `REPROCESO: se reutiliza Bronze, sin consultar RSS` | Ninguna petición a los RSS |
| Noticia con contenido modificado | Lote controlado `--modify 2` (abajo) y Trigger con `reprocess_run_id` = el lote controlado | Silver `actualizados=2`, `version=2` (`sql/07c`); Comprehend `analizados` = solo las modificadas (+ las nuevas) |
| Registros defectuosos | Lote controlado `--defects` | Silver `rechazados=2`; `sql/07b` muestra motivo y `bronze_key` |
| Nueva partición | El ítem con fecha inválida del lote controlado crea `published_date=unknown` (o un día nuevo de noticias reales) | Log de `register_partitions` con `nuevas=['unknown']`; la partición aparece en Glue y en Athena |
| Fallo controlado y reejecución | Trigger con `simulate_failure` = `build_gold` (u otra tarea) | La tarea queda `up_for_retry`, el reintento (≈2 min) termina en `success`; validaciones en 0 y mismos conteos en Gold |

### Evidencias
`./infra/collect_evidence.sh` (desde tu computador, con la EC2 encendida) ejecuta `src/datalake/evidence.py` en el contenedor de Airflow: lee el historial de ejecuciones y los logs de Airflow, los archivos de S3, el catálogo de Glue, ejecuta las consultas de `sql/` en Athena y detecta automáticamente qué ejecuciones demuestran cada escenario. El resultado (Markdown) queda en `evidencias/`.

### Lote controlado (copia de un RSS de Bronze)
```bash
docker compose exec airflow-scheduler python -m datalake.controlled_batch \
    --from-run <run_id_real> --modify 2 --defects
```
Imprime el `controlled_run_id`. Luego Trigger con `reprocess_run_id` = ese valor. El lote queda marcado en sus `manifest.json` (`controlled_copy_of`, `controlled_changes`) para distinguirlo de una extracción real.

## Despliegue, ejecución y eliminación (instrucciones reproducibles)

Probado de punta a punta en una cuenta AWS personal, región `us-east-2`, desde Windows 11 + WSL (Ubuntu). Tiempo total aproximado: 30–45 min.

### 0. Requisitos en tu computador

| Herramienta | Verificación | Notas |
|---|---|---|
| Terminal bash | — | En Windows: **WSL (Ubuntu)**. El proyecto debe estar en el sistema de archivos de Linux (`~/datalake`), **no** en `/mnt/c/...`: ahí `ssh` rechaza la llave y los scripts pierden el permiso de ejecución |
| AWS CLI v2 | `aws --version` | Instalado **dentro de WSL** |
| Terraform ≥ 1.6 | `terraform version` | Repositorio apt de HashiCorp |
| Python 3.11 | `python --version` | Solo para las pruebas locales. Se usó Miniconda (ver 0.3) |

**0.1 Usuario de AWS para desplegar.** En la consola: IAM → Users → crear un usuario (p. ej. `datalake-admin`) **sin** acceso a consola, con la política `AdministratorAccess`, y crear una *access key* de tipo CLI. No usar el usuario raíz. Recomendado: MFA en la cuenta raíz y un presupuesto con alerta en *Billing → Budgets*.

**0.2 Configurar la CLI en la región del proyecto:**
```bash
aws configure                       # access key, secret key, región us-east-2, formato json
aws configure get region            # debe decir us-east-2
aws sts get-caller-identity         # debe mostrar tu usuario de despliegue
```
> Si la región de la CLI es otra, Terraform igual crea todo en `us-east-2` (está fijado en `terraform.tfvars`) y los scripts de `infra/` también la toman de Terraform; pero los comandos `aws` que escribas a mano buscarían en otra región.

**0.3 Entorno de Python para las pruebas:**
```bash
conda create -n datalake python=3.11 -y
conda activate datalake
pip install -r requirements-dev.txt
pytest -q                           # todas las pruebas deben pasar
```
> Si tienes ROS u otro software que define `PYTHONPATH` en `~/.bashrc`, aíslalo en el entorno con
> `conda env config vars set PYTHONPATH=""` (y vuelve a activar el entorno). Alternativa sin conda: `python3 -m venv .venv`.

**0.4 Permisos de los scripts** (necesario si el proyecto se descomprimió o copió desde Windows):
```bash
chmod +x infra/*.sh
```

### 1. Infraestructura con Terraform
```bash
ssh-keygen -t ed25519 -f ~/.ssh/rss-datalake -N ""     # llave SSH (solo la primera vez)
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars           # región us-east-2; opcional: correo para alerta de costos
terraform init
terraform plan                                         # debe planear 24 recursos
terraform apply                                        # escribe "yes"
terraform output                                       # bucket, URL de Airflow, valores para el .env
cd ../..
```
Crea: bucket S3, rol IAM e *instance profile*, security group, key pair, EC2 `t3.large` con Docker, base de datos y 9 tablas de Glue, workgroup de Athena (y presupuesto si definiste `alert_email`).

Espera ~3 min a que la EC2 instale Docker. Si cambias de red (otra IP pública), vuelve a ejecutar `terraform apply`: solo actualiza la regla del security group.

### 2. Copiar el código a la EC2
```bash
./infra/sync_to_ec2.sh
```
Repite este comando cada vez que cambies código. Usa `--rebuild` si cambias `requirements.txt`, `Dockerfile` o `docker-compose.yaml`. El código se edita **siempre en tu computador**; un cambio hecho directamente en la EC2 se perdería en la siguiente sincronización.

### 3. Levantar Airflow en la EC2 (solo la primera vez)
```bash
./infra/ec2.sh ssh                      # entra a la EC2 (prompt: [ec2-user@ip-...])
ls ~/BOOTSTRAP_OK && docker compose version
cd ~/datalake
docker build -t rss-datalake-airflow:2.10.5 .
openssl rand -base64 32 | tr '+/' '-_'  # -> AIRFLOW__CORE__FERNET_KEY
openssl rand -hex 32                    # -> AIRFLOW__WEBSERVER__SECRET_KEY
id -u                                   # -> AIRFLOW_UID
cp .env.example .env && nano .env       # + los 4 valores de `terraform output env_values` y una contraseña para Airflow
docker compose up airflow-init          # termina con "exited with code 0"
docker compose up -d
docker compose ps                       # postgres, airflow-webserver y airflow-scheduler en "Up"
docker compose exec airflow-scheduler python -c "import boto3; print(boto3.client('sts').get_caller_identity()['Arn'])"
                                        # debe mostrar assumed-role/rss-datalake-ec2-role/...
```
El `.env` vive solo en la EC2 y nunca se sube a git ni lo sobrescribe la sincronización.

### 4. Ejecutar el pipeline
1. Abre la URL de `./infra/ec2.sh status` (es `http://`, puerto 8080) y entra con `admin` y tu contraseña.
2. Activa el DAG `rss_datalake` y pulsa ▶ **Trigger DAG** dejando los parámetros vacíos.
3. En **Graph**, las 8 tareas deben terminar en `success` (1–2 min).
4. Revisa el resultado en la EC2 (`cd ~/datalake`):
   ```bash
   grep -rhE "task=(build_silver|enrich_comprehend|build_gold|validate_with_athena)" logs/ | tail -20
   ```
5. Consulta los indicadores en la consola de Athena: workgroup `rss-datalake`, base `rss_datalake`, consultas de `sql/`.

El DAG queda programado cada 2 horas mientras la EC2 esté encendida. Para los escenarios de prueba ver *Cómo demostrar los escenarios*.

### 5. Apagar entre sesiones
```bash
./infra/ec2.sh stop        # deja de cobrar la EC2 y su IP pública; se conservan Airflow, .env y datos
./infra/ec2.sh start       # al retomar: imprime la URL nueva (el DNS cambia en cada arranque)
./infra/ec2.sh status
```
Al encender, Airflow arranca solo y puede lanzar la ejecución programada pendiente.

### 6. Eliminar todos los recursos
```bash
cd infra/terraform
terraform destroy          # escribe "yes"; borra también TODO el contenido del bucket
```
Verificación posterior: en la consola, S3 sin el bucket `rss-datalake-<cuenta>-us-east-2`, EC2 sin instancias, Glue sin la base `rss_datalake`, IAM sin el rol `rss-datalake-ec2-role`. Luego puedes desactivar la access key del usuario de despliegue.

### Problemas frecuentes (encontrados durante el desarrollo)

| Síntoma | Causa | Solución |
|---|---|---|
| `terraform plan`: `not authorized to perform: ec2:DescribeVpcs` | El usuario de la CLI no tiene permisos suficientes | Asignarle `AdministratorAccess` (paso 0.1) |
| `InvalidInstanceID.NotFound` en `ec2.sh` | La CLI apunta a otra región | `aws configure set region us-east-2` (los scripts ya usan la región de Terraform) |
| `Permission denied` al ejecutar `./infra/...` | Scripts sin permiso de ejecución | `chmod +x infra/*.sh` |
| `no configuration file provided` | `docker compose` fuera de `~/datalake` en la EC2 | `cd ~/datalake` |
| `ssh` se queda colgado | Cambió tu IP pública | `terraform apply` |
| `validate_config` falla con `HeadBucket 404` | `DATALAKE_BUCKET` o región incorrectos en el `.env` | Corregir el `.env` y `docker compose up -d --force-recreate` |
| El formulario de Trigger no deja campos vacíos | Versión antigua del DAG | Sincronizar el código actual |
| `pytest` falla cargando el plugin `launch_testing` | `PYTHONPATH` de ROS dentro del entorno | `conda env config vars set PYTHONPATH=""` |

## Controles de seguridad implementados
- Bucket privado: bloqueo de acceso público, ACLs desactivadas, cifrado SSE-S3 y política que rechaza conexiones sin TLS.
- Sin llaves de AWS en el código ni en `.env`: Airflow usa el rol IAM de la EC2 (IMDSv2 obligatorio).
- Rol IAM de mínimo privilegio, limitado al bucket, a la base de Glue y al workgroup de Athena del proyecto.
- Security group con SSH y Airflow abiertos solo a la IP del desarrollador.
- Llave SSH generada localmente: Terraform solo recibe la llave pública.
- Disco de la EC2 cifrado. XML procesado con `defusedxml` (bloquea entidades externas y "XML bombs").
- Workgroup de Athena con resultados cifrados y límite de bytes escaneados por consulta.
- `.env`, llaves y estado de Terraform excluidos de git.

## Decisiones técnicas
- **Terraform para la infraestructura**: todo lo que se puede declarar (S3, IAM, EC2, Glue, Athena, presupuesto) se crea y destruye de forma reproducible con `terraform apply` / `terraform destroy`. El estado es local porque el proyecto es individual.
- **Airflow 2.10.5 con Docker Compose**: despliegue simple en una sola EC2; LocalExecutor + Postgres.
- **run_id = hora real de inicio (`AAAAMMDDTHHMMSSZ`), no la fecha lógica de Airflow**: si una ejecución programada corre tarde (p. ej. la EC2 estuvo apagada), la fecha lógica daría una fecha de ingesta falsa en Bronze. La hora real se persiste en `control/airflow_runs/` para que los reintentos reutilicen el mismo lote (idempotencia). *Nota:* el lote `20261112T060000Z` se creó antes de este cambio y, aunque su nombre dice 12 de noviembre, se descargó el 13 (ver `ingested_at_utc` en su `manifest.json`).
- **manifest.json como marca de commit**: se escribe después de `feed.xml`; si existe con estado exitoso, la fuente no se vuelve a consultar en ese lote.
- **Fuente caída ≠ feed vacío**: `NETWORK_ERROR` / `HTTP_ERROR` / `MALFORMED_XML` se distinguen de `OK_EMPTY_FEED`. El lote queda `COMPLETE`, `PARTIAL` o `FAILED`.
- **Lote parcial**: Airflow reintenta (hasta 2 veces) solo las fuentes fallidas; si siguen fallando, continúa con las válidas y el estado final queda `PARTIAL`.
- **max_active_runs=1**: evita que dos lotes escriban a la vez.
