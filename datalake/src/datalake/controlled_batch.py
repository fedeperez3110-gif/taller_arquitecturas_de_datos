"""Crea un LOTE CONTROLADO en Bronze a partir de un lote real (para demostrar escenarios 4.5).

El enunciado permite usar "una copia controlada de un RSS almacenado en Bronze" para no depender
de que el medio publique cambios. Esta herramienta copia los feed.xml de un lote existente a un
run_id NUEVO y aplica cambios conocidos:

  --modify N    cambia el título de las primeras N noticias de la primera fuente
                -> escenario "Noticia con contenido modificado" (updated, version+1, nuevo Comprehend)
  --defects     agrega 3 ítems a la primera fuente:
                  * sin título                       -> cuarentena MISSING_TITLE
                  * sin guid y con enlace inválido   -> cuarentena MISSING_ID + INVALID_LINK
                  * fecha de publicación inválida    -> se conserva en published_date=unknown
                -> escenarios "Registros defectuosos" y "Nueva partición"

El lote queda marcado en su manifest.json (controlled_copy_of, controlled_changes) para que nunca se
confunda con una extracción real. Luego se procesa con el DAG usando reprocess_run_id=<nuevo run_id>,
así que tampoco se consultan los RSS.

Uso (en la EC2):
  docker compose exec airflow-scheduler python -m datalake.controlled_batch \
      --from-run 20261112T054654Z --modify 2 --defects
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

from defusedxml import ElementTree as SafeET

from datalake import s3_utils
from datalake.config import Settings, load_settings
from datalake.ingest_bronze import (
    OK, RUN_COMPLETE, bronze_prefix, new_run_id, run_control_key, validate_run_id,
)
from datalake.silver import bronze_manifests

MODIFIED_SUFFIX = " [ACTUALIZADA EN LOTE CONTROLADO]"


def _defect_items(stamp: str) -> list[ET.Element]:
    def item(**fields):
        el = ET.Element("item")
        for tag, value in fields.items():
            ET.SubElement(el, tag).text = value
        return el
    return [
        item(link=f"https://www.eltiempo.com/control/sin-titulo-{stamp}",
             guid=f"https://www.eltiempo.com/control/sin-titulo-{stamp}",
             description="Ítem de prueba sin título", pubDate="2026-11-11T08:00:00-05:00"),
        item(title="Ítem de prueba con enlace inválido", link="esto-no-es-un-enlace",
             description="Sin guid y con enlace inválido", pubDate="2026-11-11T08:00:00-05:00"),
        item(title=f"Ítem de prueba con fecha inválida {stamp}",
             link=f"https://www.eltiempo.com/control/fecha-invalida-{stamp}",
             guid=f"https://www.eltiempo.com/control/fecha-invalida-{stamp}",
             description="La fecha de publicación no se puede interpretar", pubDate="ayer en la tarde"),
    ]


def create_controlled_batch(settings: Settings, from_run: str, modify: int = 0, defects: bool = False,
                            s3=None, now: datetime | None = None) -> str:
    validate_run_id(from_run)
    s3 = s3 or s3_utils.s3_client(settings.region)
    bucket = settings.bucket
    now = now or datetime.now(timezone.utc)
    run_id = new_run_id(now)
    if run_id <= from_run:
        raise ValueError("El lote controlado debe ser posterior al lote de origen")

    manifests = [m for m in bronze_manifests(s3, bucket, from_run) if m["status"] == OK]
    if not manifests:
        raise RuntimeError(f"El lote {from_run} no tiene fuentes válidas en Bronze")

    results = []
    for i, m in enumerate(manifests):
        root = SafeET.fromstring(s3_utils.read_bytes(s3, bucket, m["s3_raw_key"]))
        channel = root.find("channel")
        changes = []
        if i == 0:
            for item in channel.findall("item")[:modify]:
                title = item.find("title")
                changes.append({"change": "TITLE_MODIFIED", "guid": item.findtext("guid"),
                                "old_title": title.text})
                title.text = (title.text or "") + MODIFIED_SUFFIX
            if defects:
                for el in _defect_items(run_id):
                    channel.append(el)
                changes.append({"change": "DEFECTIVE_ITEMS_ADDED", "count": 3})

        body = ET.tostring(root, encoding="utf-8", xml_declaration=True)
        prefix = bronze_prefix(m["source_id"], run_id)
        s3_utils.put_bytes(s3, bucket, prefix + "feed.xml", body, "application/rss+xml")
        manifest = {
            **m, "run_id": run_id, "ingested_at_utc": now.isoformat(), "action": "CONTROLLED_COPY",
            "item_count": len(channel.findall("item")), "content_sha256": hashlib.sha256(body).hexdigest(),
            "content_bytes": len(body), "s3_raw_key": prefix + "feed.xml",
            "controlled_copy_of": from_run, "controlled_changes": changes,
        }
        s3_utils.put_json(s3, bucket, prefix + "manifest.json", manifest)
        results.append(manifest)

    s3_utils.put_json(s3, bucket, run_control_key(run_id, "bronze"), {
        "run_id": run_id, "stage": "bronze", "finished_at_utc": now.isoformat(),
        "run_status": RUN_COMPLETE, "controlled_copy_of": from_run,
        "sources_total": len(results), "sources_ok": len(results),
        "items_received": sum(r["item_count"] for r in results), "sources": results,
    })
    return run_id


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--from-run", required=True, help="run_id de origen en Bronze")
    ap.add_argument("--modify", type=int, default=0, help="noticias a las que se cambia el título")
    ap.add_argument("--defects", action="store_true", help="agregar 3 ítems defectuosos")
    a = ap.parse_args()
    new_id = create_controlled_batch(load_settings(), a.from_run, a.modify, a.defects)
    print(json.dumps({"controlled_run_id": new_id,
                      "siguiente_paso": f'Trigger DAG con reprocess_run_id = "{new_id}"'}, ensure_ascii=False))
