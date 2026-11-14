# Estimación de costos — Data Lake

Región `us-east-2`, precios *on-demand* consultados el 2026-11-13 (ver fuentes al final). Todos los valores en USD.

## Supuestos de volumen (medidos en las ejecuciones reales)

| Variable | Valor | Origen |
|---|---|---|
| Ítems por lote | 30 (3 feeds × 10) | Logs de `build_silver` |
| Noticias nuevas por día | ~30–60 | Lote incremental del 13-nov: 30 nuevas en ~1 día |
| Caracteres por texto analizado (título + descripción) | 250–500 → 3–5 unidades de Comprehend | Mínimo facturable: 3 unidades (300 caracteres) por documento y por API |
| Lotes por día en operación continua | 12 (cada 2 horas) | `schedule="0 */2 * * *"` |
| Bytes escaneados por validación | 0,5–36 KB → se factura el mínimo de 10 MB | `validations.json` |
| Datos en S3 tras un mes | < 0,1 GB | XML de ~15 KB por feed y Parquet de pocos KB |

## Precios unitarios

| Servicio | Precio | Notas |
|---|---|---|
| EC2 `t3.large` (Linux) | 0,0832 / hora | Solo mientras está encendida |
| IPv4 pública | 0,005 / hora | Mientras la instancia está encendida (la IP automática se libera al apagar) |
| EBS gp3 | 0,08 / GB-mes | 30 GB; se cobra **aunque la EC2 esté apagada** |
| S3 Standard | 0,023 / GB-mes · PUT/LIST 0,005 / 1 000 · GET 0,0004 / 1 000 | |
| Comprehend (entidades y frases clave) | 0,0001 / unidad de 100 caracteres | Mínimo 3 unidades por documento. Capa gratuita: 50 000 unidades por API al mes durante 12 meses |
| Athena | 5,00 / TB escaneado | Mínimo 10 MB por consulta; las DDL son gratuitas |
| Glue Data Catalog | Gratis hasta 1 millón de objetos y 1 millón de solicitudes al mes | El proyecto usa < 200 objetos |
| AWS Budgets | Gratis (las 2 primeras alertas) | |

## Escenario A · Laboratorio (como se usó en el taller)

EC2 encendida solo durante las sesiones de trabajo: ~60 horas en el mes, ~40 lotes.

| Concepto | Cálculo | Costo |
|---|---|---|
| EC2 `t3.large` | 60 h × 0,0832 | 4,99 |
| IPv4 pública | 60 h × 0,005 | 0,30 |
| EBS 30 GB | 30 × 0,08 | 2,40 |
| S3 (almacenamiento + solicitudes) | < 0,1 GB; ~40 lotes × ~500 solicitudes | 0,03 |
| Athena | 40 lotes × 10 validaciones × 10 MB = 4 GB | 0,02 |
| Comprehend | ~100 textos × 2 APIs × ~4 unidades = 800 unidades | 0,08 (0 con capa gratuita) |
| Glue, Budgets | Capa gratuita | 0,00 |
| **Total mensual** | | **≈ 7,80** |

Casi todo el costo es la EC2 y su disco. Apagarla entre sesiones (`./infra/ec2.sh stop`) es lo que mantiene el costo bajo; el disco sigue cobrando ~0,08 por día hasta `terraform destroy`.

## Escenario B · Operación continua (24/7)

EC2 siempre encendida, 12 lotes diarios (360 al mes), ~60 noticias nuevas por día.

| Concepto | Cálculo | Costo |
|---|---|---|
| EC2 `t3.large` | 730 h × 0,0832 | 60,74 |
| IPv4 pública | 730 h × 0,005 | 3,65 |
| EBS 30 GB | 30 × 0,08 | 2,40 |
| S3 solicitudes | ~18 000 PUT/LIST + ~1 millón GET/HEAD (la caché de Comprehend se consulta por cada noticia vigente en cada lote) | 0,49 |
| S3 almacenamiento | < 0,1 GB | < 0,01 |
| Athena | 360 × 10 × 10 MB = 36 GB ≈ 0,035 TB | 0,18 |
| Comprehend | 1 800 textos × 2 APIs × 5 unidades = 18 000 unidades | 1,80 (0 con capa gratuita) |
| Glue, Budgets | Capa gratuita | 0,00 |
| **Total mensual** | | **≈ 69,30** (≈ 67,50 con capa gratuita de Comprehend) |

*Comprobación:* 60,74 + 3,65 + 2,40 + 0,49 + 0,01 + 0,18 + 1,80 = 69,27.

El 93 % del costo es la EC2 y su IP. Los servicios de datos (S3, Athena, Comprehend, Glue) suman menos de 3 USD al mes con este volumen.

## Cómo reducirlo

| Medida | Ahorro aproximado (escenario B) |
|---|---|
| Usar `t3.medium` (4 GiB; suficiente para Airflow con LocalExecutor, aunque más justo) | ~30 / mes |
| Encender la EC2 solo en ventanas de ingesta (p. ej. EventBridge Scheduler que la arranca y apaga) | Proporcional a las horas apagada |
| Savings Plan o instancia reservada a 1 año | ~30–40 % de la EC2 |
| Unir las 10 validaciones en una sola consulta de Athena (paga un mínimo de 10 MB en vez de 10) | ~0,16 |
| Leer la caché de Comprehend solo para noticias nuevas o modificadas (hoy se consulta para todas las vigentes) | La mayor parte de los GET de S3 |

**Alternativas descartadas:** Amazon MWAA (Airflow administrado) cuesta del orden de cientos de dólares al mes en su entorno más pequeño, varias veces más que la EC2, sin aportar nada necesario para este volumen. Una arquitectura sin servidor (Lambda + Step Functions) costaría centavos, pero el taller exige Airflow.

## Cómo verificar el costo real

En *Billing → Cost Explorer*, filtra por servicio y región `us-east-2`. Todos los recursos de Terraform llevan la etiqueta `Project = rss-datalake`; para filtrar por ella hay que activarla como *cost allocation tag* en *Billing → Cost allocation tags* (aplica desde su activación, no hacia atrás).

## Fuentes

- [Amazon Comprehend Pricing](https://aws.amazon.com/comprehend/pricing) (unidad de 100 caracteres, mínimo 3 unidades, capa gratuita) y [CloudZero: Amazon Comprehend pricing](https://www.cloudzero.com/blog/amazon-comprehend-pricing/) (0,0001 por unidad)
- [Amazon Athena Pricing 2026](https://cloudburn.io/blog/amazon-athena-pricing) (5 USD/TB, mínimo 10 MB, DDL gratis, capa gratuita de Glue)
- [AWS News Blog: cargo por IPv4 pública](https://aws.amazon.com/blogs/aws/new-aws-public-ipv4-address-charge-public-ip-insights) (0,005 por hora)
- [DoiT: t3.large en us-east-2](https://www.doit.com/compute/spot/us-east-2/t3.large) (0,0832 por hora)
- [Amazon EBS pricing](https://cloudburn.io/blog/amazon-ebs-pricing) (gp3 0,08 por GB-mes)
- [Cloudian: componentes del precio de S3](https://cloudian.com/blog/5-components-of-aws-s3-storage-pricing/) (S3 Standard y solicitudes)

Los precios de EBS, S3 y Comprehend de las fuentes corresponden a US East; `us-east-2` tiene la misma tarifa para estos servicios, pero conviene confirmarlo en la [calculadora de AWS](https://calculator.aws/) antes de la entrega.
