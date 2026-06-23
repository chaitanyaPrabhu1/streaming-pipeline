import pyarrow.parquet as pq

import common
import consumer
from conftest import NOW, as_kinesis_event, fake_event


def test_process_dedupes_at_least_once_duplicates():
    # Same event_id delivered twice (at-least-once) -> counted once.
    raw = [fake_event(event_id="dup"), fake_event(event_id="dup")]
    enriched, aggs, dropped, ctx = consumer.process(raw, now=NOW)
    assert len(enriched) == 1
    assert sum(a["event_count"] for a in aggs) == 1


def test_process_drops_invalid():
    raw = [fake_event(), fake_event(event_id="2", timestamp=None)]
    enriched, aggs, dropped, ctx = consumer.process(raw, now=NOW)
    assert len(enriched) == 1
    assert dropped == 1


def test_run_writes_events_and_aggregates(local_backend):
    raw = [
        fake_event(event_id="a", wiki="enwiki"),
        fake_event(event_id="b", wiki="dewiki"),
    ]
    result = consumer.run(
        raw, now=NOW, backend=local_backend, sink=common.LocalParquetSink(local_backend)
    )
    assert result["enriched"] == 2
    assert result["windows"] == 2

    # Aggregate parquet landed under the deterministic, retry-safe key.
    agg_keys = [k for k in local_backend.list_keys("agg/") if k.endswith(".parquet")]
    assert len(agg_keys) == 1
    table = pq.read_table(local_backend.location(agg_keys[0]))
    assert table.num_rows == 2

    # Enriched events landed (Firehose stand-in).
    event_keys = [k for k in local_backend.list_keys("events/") if k.endswith(".parquet")]
    assert len(event_keys) == 1


def test_run_is_idempotent_on_retry(local_backend):
    raw = [fake_event(event_id="a"), fake_event(event_id="b")]
    consumer.run(raw, now=NOW, backend=local_backend, sink=common.LocalParquetSink(local_backend))
    consumer.run(raw, now=NOW, backend=local_backend, sink=common.LocalParquetSink(local_backend))

    # Same batch -> same deterministic keys -> overwritten, not duplicated.
    agg_keys = [k for k in local_backend.list_keys("agg/") if k.endswith(".parquet")]
    event_keys = [k for k in local_backend.list_keys("events/") if k.endswith(".parquet")]
    assert len(agg_keys) == 1
    assert len(event_keys) == 1


def test_handler_decodes_kinesis_and_lands(local_backend, monkeypatch, tmp_path):
    # handler() builds its backend/sink from env -> point them at the local lake.
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("LOCAL_LAKE_DIR", str(tmp_path / "lake"))
    monkeypatch.delenv("FIREHOSE_STREAM", raising=False)  # -> LocalParquetSink

    event = as_kinesis_event([fake_event(event_id="a"), fake_event(event_id="b")])
    result = consumer.handler(event, None)
    assert result["received"] == 2
    assert result["enriched"] == 2
