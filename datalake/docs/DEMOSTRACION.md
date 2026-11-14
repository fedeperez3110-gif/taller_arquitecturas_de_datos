# Guion de la demostración — Data Lake

Duración estimada: **12–15 minutos**. Cubre los 5 puntos de la sección 5.4 del enunciado, en un orden que aprovecha la espera del reintento automático.

| Orden | Punto del enunciado | Tiempo |
|---|---|---|
| 1 | Arquitectura (contexto) | 1–2 min |
| 2 | Ejecutar una ingesta completa | 3 min |
| 3 | Recuperar una ejecución después de un fallo controlado (se lanza aquí; el reintento ocurre mientras se hace el punto 4) | 1 min + espera |
| 4 | Consultar los indicadores en Athena | 3–4 min |
| 5 | Cierre del fallo controlado | 1 min |
| 6 | Reprocesar un lote sin duplicar resultados | 2 min |
| 7 | Explicar una limitación del enriquecimiento de texto | 1–2 min |

## Preparación (antes de presentar)

1. `./infra/ec2.sh start` y anotar la URL de Airflow. Si cambiaste de red: `terraform apply` en `infra/terraform`.
2. Esperar ~1 min, entrar a Airflow y verificar que el DAG `rss_datalake` está activo y la última ejecución en verde.
3. Abrir en pestañas: Airflow (vista **Graph**), consola de **Athena** (workgroup `rss-datalake`, base `rss_datalake`), consola de **S3** en el bucket, y una terminal SSH en la EC2 (`./infra/ec2.sh ssh`, `cd ~/datalake`).
4. Tener a mano un `run_id` ya procesado para el reproceso. Por ejemplo `20261113T211328Z`, o el último:
   ```bash
   aws s3 ls s3://rss-datalake-<cuenta>-us-east-2/control/runs/ --region us-east-2 | tail -3
   ```
5. Pegar en la consola de Athena las consultas `sql/01`, `02`, `03`, `04`, `05` y `06` en pestañas separadas. En `03`, poner una entidad que exista (ver `sql/02` con `PERSON`).

## 1. Arquitectura (1–2 min)

Mostrar `docs/arquitectura_flujo.png`. Mensajes clave:
- Bucket único con capas Bronze (XML original), Silver (noticias limpias, una por `news_id`), Gold (agregados), más cuarentena, caché y control.
- Airflow en una EC2 orquesta 8 tareas; credenciales por rol IAM, sin llaves en el código.
- Toda la infraestructura está en Terraform y se elimina con `terraform destroy`.

## 2. Ingesta completa (3 min)

1. En Airflow: ▶ **Trigger DAG** con los parámetros vacíos.
2. En **Graph**, mostrar cómo avanzan las 8 tareas (~1 min).
3. En la terminal:
   ```bash
   grep -rhE "task=(ingest_bronze|build_silver|enrich_comprehend|build_gold)\]" logs/ | tail -8
   ```
   Explicar: *recibidos* por fuente, *nuevos* vs *sin_cambio*, y que Comprehend solo analizó los textos nuevos (`analizados` vs `desde_cache`).
4. En S3, abrir `bronze/source=eltiempo_medellin/ingestion_date=<hoy>/run_id=<lote>/` y mostrar `feed.xml` + `manifest.json`.

> Si El Tiempo no publicó nada desde la última ejecución, saldrá todo `sin_cambio` y `analizados=0`. Es un buen momento para decir que un lote sin novedades no reprocesa nada.

## 3. Lanzar el fallo controlado (1 min)

1. ▶ **Trigger DAG** con `simulate_failure` = `build_gold` (y `reprocess_run_id` vacío).
2. Mostrar `build_gold` en **amarillo** (*up_for_retry*) y explicar:
   - La tarea falla **después** de escribir sus archivos, a propósito.
   - Airflow la reintentará sola en 2–4 minutos (2 reintentos con espera exponencial).
   - Cada partición es un único archivo que se reemplaza de forma atómica, así que el reintento no puede duplicar ni dejar resultados a medias.

Mientras tanto, pasar a Athena.

## 4. Indicadores en Athena (3–4 min)

| Consulta | Qué mostrar |
|---|---|
| `sql/01` | Noticias por fecha y sección. Señalar que la partición `published_date` limita lo que se lee |
| `sql/02` | Top 10 organizaciones por **noticias distintas** (no menciones) |
| `sql/03` | Presencia de una entidad por día, con el cambio frente al día anterior |
| `sql/04` | Top 5 frases clave por sección |
| `sql/05` | **Conciliación Gold vs Silver:** columna `diferencia` en 0 en todas las filas. En `05b`, registros recibidos > noticias únicas (duplicados entre feeds y lotes) |
| `sql/06` | Comparar *Data scanned* de `06a` (sin filtro de partición) y `06b` (con filtro) |

## 5. Cierre del fallo controlado (1 min)

1. Volver a Airflow: `build_gold` ya en verde y las tareas siguientes completas.
2. En la terminal:
   ```bash
   grep -rh "FALLO CONTROLADO" logs/ | tail -1
   grep -rh "task=validate_with_athena" logs/ | tail -10
   ```
   Las 10 validaciones (unicidad, campos obligatorios, fechas, entidades sin noticia, conciliaciones Gold–Silver, conciliación del lote) en **0** después de la recuperación.

## 6. Reproceso sin duplicar (2 min)

1. ▶ **Trigger DAG** con `reprocess_run_id` = el lote preparado (p. ej. `20261113T211328Z`).
2. En la terminal:
   ```bash
   grep -rh "REPROCESO" logs/ | tail -1
   grep -rh "run_id=<lote>" logs/ | grep -E "task=build_silver\]|analizados=|task=build_gold\]" | tail -5
   ```
3. Mostrar: no se consultan los RSS; Silver `nuevos=0`, `particiones=[]`; Comprehend `analizados=0`; Gold sin particiones escritas. Volver a ejecutar `sql/05` en Athena: sigue todo en 0.
4. Si el lote es el controlado, señalar `antiguos=2`: sus 2 títulos modificados son más viejos que la última descarga real, y el pipeline se niega a retroceder la noticia a esa versión.

## 7. Limitación del enriquecimiento (1–2 min)

Ejecutar en Athena:
```sql
SELECT entity_type, entity_text, entity_normalized, sum(news_count) AS noticias
FROM gold_entity_daily
WHERE entity_type = 'PERSON'
GROUP BY 1, 2, 3
ORDER BY noticias DESC
LIMIT 20;
```
Buscar en el resultado un caso concreto y explicarlo. Los más comunes:
- **Sin resolución de correferencias:** "Gustavo Petro", "Petro" y "el presidente" quedan como entidades distintas, así que la presencia real de la persona se subestima.
- **Ambigüedad de tipo:** un mismo nombre puede salir como `ORGANIZATION` en una noticia y como `LOCATION` en otra (p. ej. "Nacional", "Medellín" como club o como ciudad).
- **Texto parcial:** solo se analiza título + descripción del RSS, no el artículo completo; las entidades que solo aparecen en el cuerpo no se detectan.

Mitigaciones posibles: diccionario de alias para normalizar entidades frecuentes, analizar el cuerpo del artículo, o un modelo de reconocimiento personalizado de Comprehend (más costoso).

## Preguntas probables

| Pregunta | Respuesta corta |
|---|---|
| ¿Cómo evitan duplicados si la misma noticia sale en dos feeds? | Mismo `news_id` (GUID/URL normalizada) → una fila con ambas secciones; `duplicates_in_batch` lo registra |
| ¿Y si el medio corrige una noticia? | Cambia `content_hash` → `updated`, `version + 1`, y solo esa noticia se reanaliza en Comprehend |
| ¿Qué pasa si un RSS no responde? | El lote queda `PARTIAL`; Airflow reintenta solo esa fuente y, si sigue fallando, continúa con las demás. Una fuente caída nunca se registra como "sin noticias" |
| ¿Cómo aparece una partición nueva en Athena? | La tarea `register_partitions` compara S3 con Glue y registra las nuevas al final de cada lote |
| ¿Por qué no un crawler de Glue? | Las tablas tienen esquema explícito versionado en Terraform; el crawler podría inferir tipos distintos y es más lento |
| ¿Cuánto cuesta? | ~8 USD/mes como se usó; ~69 USD/mes 24/7, casi todo la EC2 (ver `docs/COSTOS.md`) |
| ¿Qué errores encontraron? | Un `run_id` basado en la fecha lógica de Airflow daba fechas de ingesta falsas, y un reproceso intermedio podía retroceder metadatos. Ambos se detectaron en los escenarios y se corrigieron con pruebas (`docs/ENTREGA.md`) |

## Después de la demostración

```bash
./infra/ec2.sh stop
```
