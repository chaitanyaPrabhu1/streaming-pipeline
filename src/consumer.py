"""Consumer Lambda: Kinesis batch -> enrich -> Firehose (events) + S3 (aggregates).

Triggered by a Kinesis event-source mapping. For each batch it:

1. decodes + enriches every record, dropping unusable ones;
2. **de-duplicates by event_id within the batch** (at-least-once delivery means
   Kinesis can hand us the same record twice);
3. delivers the enriched events to Firehose, which converts them to Parquet and
   lands them in the S3 ``events/`` zone;
4. computes **per-(event-time window, wiki) partial aggregates** and writes them
   as Parquet to the S3 ``agg/`` zone under a deterministic, retry-safe key.

Window assignment is by EVENT time, so late / out-of-order events still land in
the correct window. Final per-window totals = SUM of partials in Athena.

Entry points:
- ``handler(event, context)`` — AWS Lambda entry point.
- ``run(...)``                 — plain-Python entry point (local runs / tests).
"""
from __future__ import annotations

import datetime as dt
import json
import logging
from typing import Any

import common

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def process(
    raw_events: list[dict[str, Any]],
    now: dt.datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int, common.WindowContext]:
    """Pure core: raw events -> (enriched, aggregates, dropped, ctx). No I/O."""
    enriched: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    dropped = 0
    for raw in raw_events:
        rec = common.enrich(raw, ingested_at=now)
        if common.validate(rec) is not None:
            dropped += 1
            continue
        if rec["event_id"] in seen_ids:  # at-least-once -> dedupe
            continue
        seen_ids.add(rec["event_id"])
        enriched.append(rec)

    ctx = common.WindowContext(event_ids=seen_ids, processed_at=now)
    aggregates = common.aggregate(enriched, batch_id=ctx.batch_id, processed_at=now)
    return enriched, aggregates, dropped, ctx


def run(
    raw_events: list[dict[str, Any]],
    now: dt.datetime | None = None,
    backend: common.StorageBackend | None = None,
    sink: common.EventSink | None = None,
) -> dict[str, Any]:
    now = now or dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    backend = backend or common.get_backend()
    sink = sink or common.LocalParquetSink(backend)

    enriched, aggregates, dropped, ctx = process(raw_events, now)

    events_location = sink.deliver(enriched, ctx)

    agg_location = None
    if aggregates:
        agg_location = backend.put_bytes(
            ctx.agg_key(), common.records_to_parquet_bytes(aggregates, common.AGG_FIELDS)
        )

    result = {
        "received": len(raw_events),
        "enriched": len(enriched),
        "dropped": dropped,
        "windows": len(aggregates),
        "batch_id": ctx.batch_id,
        "events_location": events_location,
        "agg_location": agg_location,
    }
    logger.info("consumer summary: %s", json.dumps(result))
    return result


def _default_sink(backend: common.StorageBackend) -> common.EventSink:
    """In AWS, deliver enriched events to Firehose; locally, write Parquet."""
    import os

    delivery_stream = os.environ.get("FIREHOSE_STREAM")
    if delivery_stream:
        return common.FirehoseSink(delivery_stream)
    return common.LocalParquetSink(backend)


def handler(event, context):  # noqa: ANN001 - Lambda signature
    backend = common.get_backend()
    raw_events = common.decode_kinesis_records(event or {})
    return run(raw_events, backend=backend, sink=_default_sink(backend))
