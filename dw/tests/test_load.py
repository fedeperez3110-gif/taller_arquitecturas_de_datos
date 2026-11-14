import pytest

from dw import batches, s3io
from dw.entities import ALL_ENTITIES, BY_SOURCE
from dw.load import load_batch

BATCH = "20261113T120000Z"


class FakeRedshift:
    """Simula COPY: cuenta las filas cargadas por tabla y lote; ROLLBACK las descarta."""

    def __init__(self, rows_per_copy=None):
        self.sql, self.committed, self.rolled_back = [], False, False
        self.rows_per_copy = rows_per_copy or {}
        self.pending, self.result = {}, None

    def cursor(self):
        return self

    def execute(self, sql, params=None):
        self.sql.append(sql)
        if sql.startswith("COPY"):
            table = sql.split()[1]
            self.pending[table] = self.rows_per_copy.get(table, 2)
        elif sql.startswith("DELETE"):
            self.pending[sql.split()[2]] = 0
        elif sql.startswith("SELECT COUNT"):
            self.result = (self.pending.get(sql.split()[3], 0),)

    def fetchone(self):
        return self.result

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back, self.pending = True, {}

    def close(self):
        pass


def write_manifests(s3, settings, rows=2):
    for source, entities in BY_SOURCE.items():
        s3io.put_json(s3, settings.bucket, batches.control_key(BATCH, f"extract_{source}"), {
            "entities": [{"entity": e.name, "rows": rows} for e in entities]})


def test_load_deletes_batch_then_copies_in_one_transaction(s3, settings):
    write_manifests(s3, settings)
    fake = FakeRedshift()
    summary = load_batch(settings, BATCH, s3=s3, connection_factory=lambda _: fake)
    assert fake.committed and not fake.rolled_back
    assert summary["total_rows"] == 2 * len(ALL_ENTITIES)
    copies = [s for s in fake.sql if s.startswith("COPY")]
    assert len(copies) == len(ALL_ENTITIES)
    assert all("FORMAT AS PARQUET" in c and "IAM_ROLE 'arn:aws:iam::1:role/copy'" in c for c in copies)
    assert "raw/mysql/orders/batch_id=20261113T120000Z/" in " ".join(copies)
    # DELETE del mismo lote antes de cada COPY: repetir la carga no duplica
    for e in ALL_ENTITIES:
        idx_del = fake.sql.index(f"DELETE FROM {e.raw_table} WHERE _batch_id = %s")
        assert fake.sql[idx_del + 1].startswith(f"COPY {e.raw_table} ")
    assert s3io.exists(s3, settings.bucket, batches.control_key(BATCH, "load"))


def test_count_mismatch_rolls_back(s3, settings):
    write_manifests(s3, settings)
    fake = FakeRedshift(rows_per_copy={"raw_data.mysql_orders": 1})
    with pytest.raises(RuntimeError, match="COPY cargó 1"):
        load_batch(settings, BATCH, s3=s3, connection_factory=lambda _: fake)
    assert fake.rolled_back and not fake.committed


def test_simulated_failure_rolls_back_after_copy(s3, settings):
    write_manifests(s3, settings)
    fake = FakeRedshift()
    with pytest.raises(RuntimeError, match="FALLO SIMULADO"):
        load_batch(settings, BATCH, s3=s3, connection_factory=lambda _: fake, simulate_failure=True)
    assert any(s.startswith("COPY") for s in fake.sql) and fake.rolled_back and not fake.committed
    assert not s3io.exists(s3, settings.bucket, batches.control_key(BATCH, "load"))


def test_empty_entities_skip_copy(s3, settings):
    write_manifests(s3, settings, rows=0)
    fake = FakeRedshift()
    load_batch(settings, BATCH, s3=s3, connection_factory=lambda _: fake)
    assert not any(s.startswith("COPY") for s in fake.sql)


def test_missing_manifest_fails_before_touching_redshift(s3, settings):
    called = []
    with pytest.raises(RuntimeError, match="falta el manifiesto"):
        load_batch(settings, BATCH, s3=s3, connection_factory=lambda _: called.append(1))
    assert called == []
