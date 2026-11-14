FROM apache/airflow:2.10.5-python3.11

# Se instalan las dependencias respetando los "constraints" oficiales de Airflow
# para no romper sus propias librerías.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.10.5" -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"

# --- Data Warehouse ---
# Certificado de AWS para TLS con RDS MySQL y DocumentDB.
USER root
RUN mkdir -p /opt/certs \
    && curl -fsSL https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem -o /opt/certs/global-bundle.pem \
    && chmod 644 /opt/certs/global-bundle.pem
USER airflow
# dbt en su propio entorno virtual: sus dependencias no chocan con las de Airflow.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-core~=1.9.0" "dbt-redshift~=1.9.0"
