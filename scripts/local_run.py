#!/usr/bin/env python3
"""End-to-end local run: live Wikipedia edits -> enrich -> aggregate -> ./.local_lake.

Connects to the real (free, no-auth) Wikimedia EventStreams SSE firehose, pumps
events through the same producer/consumer code that runs in AWS, and writes the
enriched events + per-minute aggregate Parquet to a filesystem "lake" — no AWS
account needed. Prints a throughput number you can quote on a resume.

    make local-run                 # default 1000 events
    MAX_EVENTS=3000 make local-run
"""
from __future__ import annotations

import datetime as dt
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

import common  # noqa: E402
import consumer  # noqa: E402
import producer  # noqa: E402


def main() -> int:
    os.environ.setdefault("STORAGE_BACKEND", "local")
    os.environ.setdefault("LOCAL_LAKE_DIR", str(ROOT / ".local_lake"))
    backend = common.get_backend()
    max_events = int(os.environ.get("MAX_EVENTS", "1000"))
    batch_size = int(os.environ.get("BATCH_SIZE", "200"))

    print(f"== produce ==  streaming up to {max_events} live events from Wikimedia")
    stream = common.LocalStream()
    t0 = time.monotonic()
    producer.run(stream, max_events=max_events)
    elapsed = max(time.monotonic() - t0, 1e-6)
    n = len(stream.records)
    rate = n / elapsed * 60.0
    print(f"   collected {n} events in {elapsed:.1f}s  ->  ~{rate:,.0f} events/min")

    print("== consume ==  enrich + window into mini-batches (simulating Lambda)")
    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    totals = {"enriched": 0, "dropped": 0, "windows": 0}
    for i in range(0, n, batch_size):
        batch = stream.records[i : i + batch_size]
        res = consumer.run(batch, now=now, backend=backend, sink=common.LocalParquetSink(backend))
        for k in totals:
            totals[k] += res[k]
    print(f"   enriched {totals['enriched']}  dropped {totals['dropped']}  "
          f"window-partials {totals['windows']}")

    # Prove the Parquet is valid and show a sample aggregate row.
    import pyarrow.parquet as pq

    agg_keys = [k for k in backend.list_keys(common.AGG_PREFIX) if k.endswith(".parquet")]
    if agg_keys:
        table = pq.read_table(backend.location(agg_keys[0]))
        print(f"   agg parquet verified: {table.num_rows} rows x {table.num_columns} cols")
        top = sorted(table.to_pylist(), key=lambda r: r["event_count"], reverse=True)[:3]
        for r in top:
            print(f"     {r['window_start']}  {r['wiki']:<12} events={r['event_count']} "
                  f"edits={r['edit_count']} bots={r['bot_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
