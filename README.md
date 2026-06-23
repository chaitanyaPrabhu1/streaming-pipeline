# Real-Time Streaming Pipeline on AWS (Wikipedia edits)

> Same data-engineering discipline as a batch ELT — but real-time. Ingest a live event stream, process
> it as it arrives, and land it somewhere queryable, with windowed aggregations for near-real-time
> metrics. "Streaming" on a DE resume stands out because most candidates only show batch.

**Resume line**

> Built a real-time streaming pipeline on AWS: ingested the live Wikimedia edit stream through Kinesis,
> processed it with Lambda (enrich + per-minute windowed aggregations), and landed it in S3 as Parquet
> via Firehose for querying in Athena — handling at-least-once delivery and out-of-order events.

---

## Architecture

```
 Wikimedia EventStreams (SSE firehose — every edit on every wiki, free, no auth)
     │
     ▼  producer (always-on task: Fargate/EC2/local) — PutRecords
 ┌─────────────────────────┐
 │  Kinesis Data Stream     │  events  (the ingest buffer)
 └─────────────────────────┘
     │
     ▼  event-source mapping (batch_size + batching window)
 ┌─────────────────────────┐
 │  Lambda consumer         │  parse → enrich → de-dupe (at-least-once)
 │                          │   ├─►  per-(event-time window, wiki) aggregates ─┐
 │                          │   └─►  enriched events ─► Kinesis Firehose        │
 └─────────────────────────┘                               │ JSON→Parquet       │
                                                            ▼                    ▼
                                        S3  events/dt=…/hour=…/*.parquet   S3 agg/dt=…/*.parquet
                                                            │                    │
                                                            ▼                    ▼
                                                 Athena  events  +  agg_partials ─► view edits_per_minute
                                                            (near-real-time metrics)

 CloudWatch alarms: consumer errors + iterator-age (lag behind real time).
```

## What this pipeline does and why

A small **producer** holds open the [Wikimedia EventStreams](https://stream.wikimedia.org) SSE
connection — a constant firehose of every edit across every wiki — and writes each event to a
**Kinesis Data Stream**. (It runs as an always-on task, not a Lambda, because Lambda can't hold a
long-lived streaming connection.)

A **Lambda consumer** is driven by the stream via an event-source mapping. For each micro-batch it:

1. **enriches** every record (flatten, type, compute `bytes_changed`, flag bots/minor edits);
2. **de-duplicates by `event_id`** — Kinesis is *at-least-once*, so the same record can arrive twice;
3. delivers the enriched events to **Kinesis Firehose**, which buffers them, converts **JSON → Parquet**
   (using the Glue table as the schema), and lands Hive-partitioned objects in S3;
4. computes **per-minute windowed aggregates** — bucketed by **event time**, so late / out-of-order
   events still land in the right window — and writes them as Parquet partials to S3.

**Athena** queries both: the raw `events` table and a `edits_per_minute` view that sums the aggregate
partials into final per-window metrics.

## Design choices (own these in an interview)

- **Why bucket by event time, not arrival time?** Networks reorder and delay events. Assigning each
  event to its window by its own `event_time` means a record that shows up 30s late still counts in the
  minute it actually happened — correct windows regardless of arrival order.
- **How is at-least-once delivery handled?** Two layers. Within a batch, we de-dupe by `event_id`.
  Across batches, each aggregate file is written to a **deterministic key derived from the batch's
  event ids**, so a retried invocation overwrites the same object instead of double-counting. The
  aggregates are *partials* (one batch's contribution to a window); the Athena `edits_per_minute` view
  SUMs them, which is correct even when a window's events are spread across many batches.
- **Why Lambda *and* Firehose?** The Lambda enriches and de-dupes *before* landing, and computes the
  aggregates; Firehose then does the heavy, managed work — buffering, **Parquet conversion**, and
  partitioned S3 delivery — that you don't want to hand-roll. Each does what it's best at.
- **Why Parquet + `dt`/`hour` partitions?** Athena bills by bytes scanned. Columnar Parquet plus
  partition pruning (via **partition projection**, so no crawler/`MSCK` needed) keeps near-real-time
  queries fast and cheap.
- **What's the health metric?** **Iterator age** — how far behind the tip of the stream the consumer
  is. It's the single best signal that a streaming consumer is keeping up; there's a CloudWatch alarm
  on it.
- **Batch vs streaming — when would you pick each?** Streaming (this project) fits when freshness is
  measured in seconds and you act on events as they arrive. Batch fits periodic, latency-tolerant
  workloads where simplicity and cost win. This stream updates constantly → streaming.

## Repository layout

```
streaming-pipeline/
├── README.md
├── Makefile                   # one-liners: local-run, test, package, plan, apply
├── requirements-dev.txt
├── config/
│   └── stream.json            # SSE source + window/buffer settings
├── src/
│   ├── common.py              # storage/stream/firehose backends, schema, windowing, aggregation
│   ├── producer.py            # Wikimedia SSE → Kinesis (always-on task)
│   ├── consumer.py            # Lambda: Kinesis batch → enrich → Firehose + agg Parquet
│   └── requirements.txt
├── sql/
│   ├── athena_ddl.sql         # external tables (partition projection)
│   ├── rollup_view.sql        # edits_per_minute = SUM of partials
│   └── sample_queries.sql     # near-real-time queries
├── scripts/
│   └── local_run.py           # live SSE → process → ./.local_lake (no AWS) + throughput
├── terraform/                 # Kinesis, Firehose, Lambda+ESM, Glue tables, Athena, IAM, alarms
└── tests/
    ├── conftest.py
    ├── test_common.py
    └── test_consumer.py
```

## Quickstart

### 1. Run end-to-end locally (no AWS needed)

```bash
make venv
make local-run          # streams ~1000 live Wikipedia edits → ./.local_lake, prints events/min
make test               # unit tests (synthetic events)
```

`make local-run` connects to the real Wikimedia SSE firehose and runs the *same* producer/consumer
code that runs in AWS, writing enriched + aggregate Parquet to a local "lake" so you can inspect it.

### 2. Deploy to AWS

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # set pandas_layer_arn (+ optional alarm_email)
make package                                    # assemble the Lambda zip
terraform init && terraform apply
eval "$(terraform output -raw env_exports)"     # export STREAM_NAME, DATA_BUCKET, etc.
```

This provisions the Kinesis stream, the Firehose delivery stream (with Parquet conversion), the
consumer Lambda + event-source mapping, the Glue database/tables, an Athena workgroup, IAM roles, and
CloudWatch alarms.

### 3. Start the producer + query

```bash
STREAM_NAME=$STREAM_NAME python src/producer.py     # pump live edits into Kinesis
# in Athena (after a minute of buffering):
#   run sql/rollup_view.sql once, then sql/sample_queries.sql
```

## Cost & teardown

Everything is serverless/managed and sized small: Kinesis (1 shard), Lambda (free tier covers it),
Firehose (per-GB, tiny), S3 + Athena (pennies, kept cheap by Parquet + partitions). A billing alarm is
provisioned by Terraform. **Tear down when done:**

```bash
cd terraform && terraform destroy
```

## Headline numbers (fill in after a run)

- Sustained ingest of **~1,500–3,000 events/min** from the live Wikimedia firehose (`make local-run`
  prints your exact rate).
- **Per-minute windowed metrics** in Athena, correct under out-of-order arrival + at-least-once delivery.
- End-to-end **JSON → enrich → Parquet** with date/hour partitioning and partition projection.
