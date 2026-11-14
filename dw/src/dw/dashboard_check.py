"""Ejecuta dbt/sql/quicksight_validation.sql en Redshift e imprime los resultados.

    docker compose exec -T airflow-scheduler python -m dw.dashboard_check
"""
from pathlib import Path

from dw.connections import redshift_query
from dw.settings import load_settings

SQL = Path("/opt/airflow/dw/dbt/sql/quicksight_validation.sql")


def main() -> None:
    settings = load_settings()
    for stmt in SQL.read_text().split(";"):
        body = "\n".join(ln for ln in stmt.splitlines() if not ln.strip().startswith("--")).strip()
        if body:
            print("\n>>", body.splitlines()[0][:90])
            for row in redshift_query(settings, body):
                print("  ", tuple(str(v) for v in row))


if __name__ == "__main__":
    main()
