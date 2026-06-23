"""Shared config, backends, and stream-processing helpers for the pipeline.

The same code runs in AWS (Kinesis / Firehose / S3 via boto3) and locally (an
in-memory stream + a filesystem "lake"), which is what makes ``make local-run``
and the unit tests work without an AWS account.

Three backend families, each with an AWS impl and a local impl:
- StorageBackend  — object storage (S3 / local files)        : land aggregates
- StreamBackend   — the event stream (Kinesis / in-memory)   : producer -> stream
- EventSink       — enriched-event delivery (Firehose / local parquet)

Parquet is written with pyarrow directly. In AWS, pyarrow comes from the managed
"AWSSDKPandas" Lambda layer; locally from requirements-dev.txt.
"""
from __future__ import annotations

import base64
import datetime as dt
import io
import json
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Iterable

# --------------------------------------------------------------------------- #
# Configuration                                                               #
# --------------------------------------------------------------------------- #

SSE_URL = os.environ.get(
    "SSE_URL", "https://stream.wikimedia.org/v2/stream/recentchange"
)
WINDOW_SECONDS = int(os.environ.get("WINDOW_SECONDS", "60"))

EVENTS_PREFIX = os.environ.get("EVENTS_PREFIX", "events")   # enriched events (Firehose lands here)
AGG_PREFIX = os.environ.get("AGG_PREFIX", "agg")            # per-window aggregate partials


def _config_path() -> str:
    here = Path(__file__).resolve().parent
    return str(here.parent / "config" / "stream.json")


def load_config(path: str | None = None) -> dict[str, Any]:
    path = path or os.environ.get("STREAM_CONFIG") or _config_path()
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


# --------------------------------------------------------------------------- #
# Object storage backends (S3 / local)                                        #
# --------------------------------------------------------------------------- #


class StorageBackend(ABC):
    @abstractmethod
    def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str: ...

    @abstractmethod
    def get_bytes(self, key: str) -> bytes: ...

    @abstractmethod
    def list_keys(self, prefix: str) -> list[str]: ...

    @abstractmethod
    def location(self, key: str) -> str: ...


class LocalBackend(StorageBackend):
    def __init__(self, root: str):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        return self.root / key

    def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
        p = self._path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        return self.location(key)

    def get_bytes(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def list_keys(self, prefix: str) -> list[str]:
        base = self._path(prefix)
        if not base.exists():
            return []
        if base.is_file():
            return [prefix]
        return sorted(
            str(p.relative_to(self.root)).replace(os.sep, "/")
            for p in base.rglob("*")
            if p.is_file()
        )

    def location(self, key: str) -> str:
        return str(self._path(key))


class S3Backend(StorageBackend):
    def __init__(self, bucket: str, client: Any | None = None):
        self.bucket = bucket
        if client is None:
            import boto3  # lazy so local runs don't need boto3

            client = boto3.client("s3")
        self.client = client

    def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)
        return self.location(key)

    def get_bytes(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        token: str | None = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.client.list_objects_v2(**kwargs)
            keys.extend(obj["Key"] for obj in resp.get("Contents", []))
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return sorted(keys)

    def location(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"


def get_backend(client: Any | None = None) -> StorageBackend:
    backend = os.environ.get("STORAGE_BACKEND")
    bucket = os.environ.get("DATA_BUCKET")
    if backend == "local" or (backend is None and not bucket):
        return LocalBackend(os.environ.get("LOCAL_LAKE_DIR", "./.local_lake"))
    if not bucket:
        raise RuntimeError("DATA_BUCKET must be set when STORAGE_BACKEND is not 'local'")
    return S3Backend(bucket, client=client)


# --------------------------------------------------------------------------- #
# Stream backends (Kinesis / in-memory) — the producer writes here            #
# --------------------------------------------------------------------------- #


class StreamBackend(ABC):
    @abstractmethod
    def put(self, record: dict[str, Any], partition_key: str) -> None: ...

    def flush(self) -> None:  # noqa: D401 - optional batching hook
        """Flush any buffered records (no-op by default)."""


class LocalStream(StreamBackend):
    """Collects records in memory so a local run can drain and process them."""

    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def put(self, record: dict[str, Any], partition_key: str) -> None:
        self.records.append(record)


class KinesisStream(StreamBackend):
    """Buffers records and PutRecords in batches of up to 500 (Kinesis limit)."""

    def __init__(self, stream_name: str, client: Any | None = None, batch_size: int = 500):
        self.stream_name = stream_name
        self.batch_size = min(batch_size, 500)
        if client is None:
            import boto3

            client = boto3.client("kinesis")
        self.client = client
        self._buf: list[dict[str, Any]] = []

    def put(self, record: dict[str, Any], partition_key: str) -> None:
        self._buf.append(
            {
                "Data": json.dumps(record, separators=(",", ":")).encode("utf-8"),
                "PartitionKey": partition_key,
            }
        )
        if len(self._buf) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._buf:
            return
        self.client.put_records(StreamName=self.stream_name, Records=self._buf)
        self._buf = []


# --------------------------------------------------------------------------- #
# Event sinks (Firehose / local parquet) — the consumer lands enriched events #
# --------------------------------------------------------------------------- #


class EventSink(ABC):
    @abstractmethod
    def deliver(self, records: list[dict[str, Any]], ctx: "WindowContext") -> str | None: ...


class FirehoseSink(EventSink):
    """Delivers JSON records to a Firehose stream, which converts them to Parquet
    and lands them in S3. Firehose owns batching/partitioning/format-conversion."""

    def __init__(self, delivery_stream: str, client: Any | None = None):
        self.delivery_stream = delivery_stream
        if client is None:
            import boto3

            client = boto3.client("firehose")
        self.client = client

    def deliver(self, records: list[dict[str, Any]], ctx: "WindowContext") -> str | None:
        if not records:
            return None
        # Firehose wants newline-delimited JSON; cap at 500 records / request.
        for i in range(0, len(records), 500):
            chunk = records[i : i + 500]
            self.client.put_record_batch(
                DeliveryStreamName=self.delivery_stream,
                Records=[
                    {"Data": (json.dumps(r, separators=(",", ":"), default=_json_default) + "\n").encode("utf-8")}
                    for r in chunk
                ],
            )
        return self.delivery_stream


class LocalParquetSink(EventSink):
    """Stand-in for Firehose when running locally: writes the enriched events to
    a date/hour-partitioned Parquet file in the storage backend."""

    def __init__(self, backend: StorageBackend):
        self.backend = backend

    def deliver(self, records: list[dict[str, Any]], ctx: "WindowContext") -> str | None:
        if not records:
            return None
        key = f"{EVENTS_PREFIX}/dt={ctx.dt}/hour={ctx.hour}/events-{ctx.batch_id}.parquet"
        return self.backend.put_bytes(key, records_to_parquet_bytes(records, ENRICHED_FIELDS))


# --------------------------------------------------------------------------- #
# Schemas                                                                      #
# --------------------------------------------------------------------------- #

# Enriched event (what lands in S3 via Firehose). Stable order = stable schema.
ENRICHED_FIELDS: list[tuple[str, str]] = [
    ("event_id", "string"),
    ("event_time", "timestamp"),
    ("event_type", "string"),
    ("wiki", "string"),
    ("domain", "string"),
    ("title", "string"),
    ("user", "string"),
    ("is_bot", "bool"),
    ("is_minor", "bool"),
    ("namespace", "int64"),
    ("length_old", "int64"),
    ("length_new", "int64"),
    ("bytes_changed", "int64"),
    ("ingested_at", "timestamp"),
]

# Per-window aggregate partial (one row per window_start x wiki, per batch).
AGG_FIELDS: list[tuple[str, str]] = [
    ("window_start", "timestamp"),
    ("wiki", "string"),
    ("event_count", "int64"),
    ("edit_count", "int64"),
    ("new_page_count", "int64"),
    ("bot_count", "int64"),
    ("total_bytes_changed", "int64"),
    ("abs_bytes_changed", "int64"),
    ("batch_id", "string"),
    ("processed_at", "timestamp"),
]


# --------------------------------------------------------------------------- #
# Parsing / enrichment                                                         #
# --------------------------------------------------------------------------- #


def _int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return int(value) if isinstance(value, bool) else None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def enrich(raw: dict[str, Any], ingested_at: dt.datetime) -> dict[str, Any]:
    """Flatten a Wikimedia recent-change event onto the enriched schema.

    Does not validate (see ``validate``). Tolerant of missing fields.
    """
    meta = raw.get("meta") or {}
    length = raw.get("length") or {}
    ts = raw.get("timestamp")
    event_time = _epoch_to_dt(ts)
    old, new = _int(length.get("old")), _int(length.get("new"))
    bytes_changed = (new - old) if (old is not None and new is not None) else None
    return {
        "event_id": meta.get("id") or raw.get("id"),
        "event_time": event_time,
        "event_type": raw.get("type"),
        "wiki": raw.get("wiki") or meta.get("domain"),
        "domain": meta.get("domain") or raw.get("server_name"),
        "title": raw.get("title"),
        "user": raw.get("user"),
        "is_bot": bool(raw.get("bot", False)),
        "is_minor": bool(raw.get("minor", False)),
        "namespace": _int(raw.get("namespace")),
        "length_old": old,
        "length_new": new,
        "bytes_changed": bytes_changed,
        "ingested_at": ingested_at,
    }


def validate(rec: dict[str, Any]) -> str | None:
    """Return a reason string if the enriched record is unusable, else None."""
    if not rec.get("event_id"):
        return "missing event_id"
    if rec.get("event_time") is None:
        return "missing/invalid event_time"
    if not rec.get("wiki"):
        return "missing wiki"
    return None


def _epoch_to_dt(value: Any) -> dt.datetime | None:
    """Wikimedia 'timestamp' is unix seconds (int). Return naive UTC datetime."""
    if value is None:
        return None
    try:
        secs = int(value)
    except (TypeError, ValueError):
        return None
    return dt.datetime.fromtimestamp(secs, tz=dt.timezone.utc).replace(tzinfo=None)


def _json_default(o: Any):
    if isinstance(o, dt.datetime):
        # ISO-8601 so Firehose/Athena read it as a timestamp.
        return o.replace(microsecond=0).isoformat(sep=" ")
    raise TypeError(f"not JSON serializable: {type(o)}")


# --------------------------------------------------------------------------- #
# Windowing + aggregation                                                      #
# --------------------------------------------------------------------------- #


def window_start(event_time: dt.datetime, window_seconds: int = WINDOW_SECONDS) -> dt.datetime:
    """Floor an event time to its tumbling-window start (by EVENT time).

    Bucketing on event time (not arrival time) is what makes late / out-of-order
    events land in the correct window — the whole point of the design.
    """
    epoch = int(event_time.replace(tzinfo=dt.timezone.utc).timestamp())
    floored = epoch - (epoch % window_seconds)
    return dt.datetime.fromtimestamp(floored, tz=dt.timezone.utc).replace(tzinfo=None)


def aggregate(
    records: list[dict[str, Any]],
    batch_id: str,
    processed_at: dt.datetime,
    window_seconds: int = WINDOW_SECONDS,
) -> list[dict[str, Any]]:
    """Roll enriched records into per-(window_start, wiki) partial aggregates.

    These are *partials*: one batch's contribution to each window. Because a
    window's events can arrive across many batches (and late), the final
    per-window totals are produced by SUMming partials in Athena (see
    sql/rollup_view.sql). This pre-aggregate-then-roll-up pattern is correct
    under at-least-once delivery and out-of-order arrival.
    """
    buckets: dict[tuple[dt.datetime, str], dict[str, Any]] = {}
    for r in records:
        key = (window_start(r["event_time"], window_seconds), r["wiki"])
        b = buckets.get(key)
        if b is None:
            b = {
                "window_start": key[0],
                "wiki": key[1],
                "event_count": 0,
                "edit_count": 0,
                "new_page_count": 0,
                "bot_count": 0,
                "total_bytes_changed": 0,
                "abs_bytes_changed": 0,
                "batch_id": batch_id,
                "processed_at": processed_at,
            }
            buckets[key] = b
        b["event_count"] += 1
        if r.get("event_type") == "edit":
            b["edit_count"] += 1
        if r.get("event_type") == "new":
            b["new_page_count"] += 1
        if r.get("is_bot"):
            b["bot_count"] += 1
        delta = r.get("bytes_changed")
        if delta is not None:
            b["total_bytes_changed"] += delta
            b["abs_bytes_changed"] += abs(delta)
    return sorted(buckets.values(), key=lambda b: (b["window_start"], b["wiki"]))


# --------------------------------------------------------------------------- #
# Batch context + Kinesis decoding                                             #
# --------------------------------------------------------------------------- #


class WindowContext:
    """Per-invocation context: a deterministic batch id + partition values.

    The batch id is derived from the event ids in the batch, so a retried
    (at-least-once) invocation writes to the SAME keys and overwrites cleanly
    instead of double-counting.
    """

    def __init__(self, event_ids: Iterable[str], processed_at: dt.datetime):
        import hashlib

        h = hashlib.sha1("|".join(sorted(event_ids)).encode("utf-8")).hexdigest()[:16]
        self.batch_id = h
        self.processed_at = processed_at
        self.dt = processed_at.strftime("%Y-%m-%d")
        self.hour = processed_at.strftime("%H")

    def agg_key(self) -> str:
        # Partitioned by processing date; window_start stays a column so late
        # events still roll up correctly across days.
        return f"{AGG_PREFIX}/dt={self.dt}/hour={self.hour}/agg-{self.batch_id}.parquet"


def decode_kinesis_records(event: dict[str, Any]) -> list[dict[str, Any]]:
    """Decode a Lambda Kinesis event into a list of raw event dicts."""
    out: list[dict[str, Any]] = []
    for rec in event.get("Records", []):
        data = (rec.get("kinesis") or {}).get("data")
        if not data:
            continue
        try:
            out.append(json.loads(base64.b64decode(data).decode("utf-8")))
        except Exception:  # noqa: BLE001 - skip undecodable records
            continue
    return out


# --------------------------------------------------------------------------- #
# Parquet helpers (pyarrow)                                                    #
# --------------------------------------------------------------------------- #


def _pyarrow_schema(fields: list[tuple[str, str]]):
    import pyarrow as pa  # type: ignore

    mapping = {
        "string": pa.string(),
        "int64": pa.int64(),
        "bool": pa.bool_(),
        "timestamp": pa.timestamp("us"),
    }
    return pa.schema([(name, mapping[kind]) for name, kind in fields])


def records_to_parquet_bytes(records: Iterable[dict[str, Any]], fields: list[tuple[str, str]]) -> bytes:
    import pyarrow as pa  # type: ignore
    import pyarrow.parquet as pq  # type: ignore

    schema = _pyarrow_schema(fields)
    columns: dict[str, list[Any]] = {name: [] for name, _ in fields}
    for rec in records:
        for name, _kind in fields:
            columns[name].append(rec.get(name))
    table = pa.table(columns, schema=schema)
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="snappy")
    return sink.getvalue()
