import datetime as dt

import pyarrow.parquet as pq

import common
from conftest import NOW, as_kinesis_event, fake_event


def test_enrich_flattens_and_computes_bytes_changed():
    rec = common.enrich(fake_event(old=100, new=175), ingested_at=NOW)
    assert rec["event_id"] == "11111111-1111-1111-1111-111111111111"
    assert rec["wiki"] == "enwiki"
    assert rec["event_type"] == "edit"
    assert rec["bytes_changed"] == 75
    assert rec["is_bot"] is False
    assert isinstance(rec["event_time"], dt.datetime)
    assert rec["event_time"] == dt.datetime(2024, 5, 1, 12, 0, 0)


def test_enrich_handles_missing_length():
    rec = common.enrich(fake_event(old=None, new=None), ingested_at=NOW)
    assert rec["bytes_changed"] is None
    assert rec["length_old"] is None


def test_validate_flags_bad_records():
    good = common.enrich(fake_event(), ingested_at=NOW)
    assert common.validate(good) is None
    bad = common.enrich(fake_event(timestamp=None), ingested_at=NOW)
    assert common.validate(bad) == "missing/invalid event_time"


def test_window_start_buckets_out_of_order_to_same_window():
    early = dt.datetime(2024, 5, 1, 12, 0, 5)
    late = dt.datetime(2024, 5, 1, 12, 0, 58)  # arrives later, same minute
    assert common.window_start(early, 60) == common.window_start(late, 60)
    assert common.window_start(early, 60) == dt.datetime(2024, 5, 1, 12, 0, 0)
    # Next minute is a different window.
    assert common.window_start(dt.datetime(2024, 5, 1, 12, 1, 1), 60) == dt.datetime(2024, 5, 1, 12, 1, 0)


def test_aggregate_groups_by_window_and_wiki():
    recs = [
        common.enrich(fake_event(wiki="enwiki", type="edit", old=10, new=20), NOW),
        common.enrich(fake_event(wiki="enwiki", type="new", bot=True, old=0, new=50), NOW),
        common.enrich(fake_event(wiki="dewiki", type="edit", old=80, new=60), NOW),
    ]
    aggs = common.aggregate(recs, batch_id="b1", processed_at=NOW)
    by_wiki = {a["wiki"]: a for a in aggs}
    assert by_wiki["enwiki"]["event_count"] == 2
    assert by_wiki["enwiki"]["edit_count"] == 1
    assert by_wiki["enwiki"]["new_page_count"] == 1
    assert by_wiki["enwiki"]["bot_count"] == 1
    assert by_wiki["enwiki"]["total_bytes_changed"] == 10 + 50
    assert by_wiki["dewiki"]["total_bytes_changed"] == -20
    assert by_wiki["dewiki"]["abs_bytes_changed"] == 20


def test_aggregate_splits_across_windows():
    recs = [
        common.enrich(fake_event(timestamp=1714564800), NOW),  # 12:00:00
        common.enrich(fake_event(event_id="2", timestamp=1714564861), NOW),  # 12:01:01
    ]
    aggs = common.aggregate(recs, batch_id="b1", processed_at=NOW)
    assert len(aggs) == 2  # two distinct minute windows


def test_decode_kinesis_records_roundtrip():
    event = as_kinesis_event([fake_event(), fake_event(event_id="2")])
    decoded = common.decode_kinesis_records(event)
    assert len(decoded) == 2
    assert decoded[0]["wiki"] == "enwiki"


def test_window_context_batch_id_is_deterministic():
    a = common.WindowContext(["x", "y", "z"], NOW)
    b = common.WindowContext(["z", "y", "x"], NOW)  # order-independent
    assert a.batch_id == b.batch_id
    assert a.agg_key() == "agg/dt=2024-05-01/hour=12/agg-" + a.batch_id + ".parquet"


def test_parquet_roundtrip(local_backend):
    recs = [common.enrich(fake_event(), NOW)]
    data = common.records_to_parquet_bytes(recs, common.ENRICHED_FIELDS)
    loc = local_backend.put_bytes("x/e.parquet", data)
    table = pq.read_table(loc)
    assert table.num_rows == 1
    assert "bytes_changed" in table.column_names
