# Arquitecturas de Datos — Data Lake y Data Warehouse en AWS

Taller de Ingeniería de Datos (Universidad EIA, 2026-2). Dos proyectos desplegados en AWS (`us-east-2`)
con Terraform, orquestados por Apache Airflow en una misma instancia EC2.

| Proyecto | Qué hace | Documentación | Evidencias |
|---|---|---|---|
| [`datalake/`](datalake/) | Noticias RSS → S3 medallón (Bronze/Silver/Gold) → Amazon Comprehend → Glue Data Catalog → Athena | [README](datalake/README.md) · [entrega](datalake/docs/ENTREGA.md) | [8 escenarios](datalake/evidencias/README.md) |
| [`dw/`](dw/) | RDS MySQL + DocumentDB → S3 → Redshift Serverless (COPY) → dbt → QuickSight | [README](dw/README.md) · [entrega](dw/docs/ENTREGA.md) | [6 escenarios](dw/evidencias/README.md) |

## Cómo se relacionan

* La EC2 del Data Lake (Airflow 2.10 en Docker Compose) ejecuta **los dos DAGs**: `rss_datalake` y
  `dw_ecommerce_pipeline`. La imagen (`datalake/Dockerfile`) incluye las dependencias de ambos y dbt en su
  propio entorno virtual; `datalake/docker-compose.yaml` monta la carpeta `dw/`.
* Cada proyecto tiene su propio Terraform. El del DW reutiliza el rol IAM y el security group de la EC2.
* Orden de despliegue: primero `datalake/` (ver su README), después `dw/`.

## Seguridad del repositorio

No se versionan secretos ni estado: `.env`, `*.pem`, `*.tfstate*` y `*.tfvars` están en `.gitignore`. Las
contraseñas de las bases de datos solo existen en AWS Secrets Manager. Los archivos `.env.example` y
`terraform.tfvars.example` documentan las variables requeridas.

## Eliminación de recursos

```bash
cd dw/infra/terraform && terraform destroy          # primero el DW (depende de la EC2)
cd ../../../datalake/infra/terraform && terraform destroy
```

En QuickSight, borrar el dashboard, el análisis, el dataset y la fuente de datos, y cancelar la suscripción.
