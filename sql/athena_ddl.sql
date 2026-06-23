-- Athena external tables over the S3 zones the pipeline lands.
-- Replace ${DATA_BUCKET} with your bucket (Terraform output `data_bucket`).
-- Partition projection is used so you never have to run MSCK / load partitions.

-- 1) Enriched events (landed by Firehose as Parquet).
CREATE EXTERNAL TABLE IF NOT EXISTS events (
    event_id            string,
    event_time          timestamp,
    event_type          string,
    wiki                string,
    domain              string,
    title               string,
    `user`              string,
    is_bot              boolean,
    is_minor            boolean,
    namespace           bigint,
    length_old          bigint,
    length_new          bigint,
    bytes_changed       bigint,
    ingested_at         timestamp
)
PARTITIONED BY (dt string, hour string)
STORED AS PARQUET
LOCATION 's3://${DATA_BUCKET}/events/'
TBLPROPERTIES (
    'projection.enabled'       = 'true',
    'projection.dt.type'       = 'date',
    'projection.dt.format'     = 'yyyy-MM-dd',
    'projection.dt.range'      = '2024-01-01,NOW',
    'projection.dt.interval'   = '1',
    'projection.dt.interval.unit' = 'DAYS',
    'projection.hour.type'     = 'integer',
    'projection.hour.range'    = '0,23',
    'projection.hour.digits'   = '2',
    'storage.location.template' = 's3://${DATA_BUCKET}/events/dt=${dt}/hour=${hour}/'
);

-- 2) Per-(window, wiki) aggregate PARTIALS written by the consumer Lambda.
--    One row per batch's contribution to a window; roll up with the view below.
CREATE EXTERNAL TABLE IF NOT EXISTS agg_partials (
    window_start         timestamp,
    wiki                 string,
    event_count          bigint,
    edit_count           bigint,
    new_page_count       bigint,
    bot_count            bigint,
    total_bytes_changed  bigint,
    abs_bytes_changed    bigint,
    batch_id             string,
    processed_at         timestamp
)
PARTITIONED BY (dt string, hour string)
STORED AS PARQUET
LOCATION 's3://${DATA_BUCKET}/agg/'
TBLPROPERTIES (
    'projection.enabled'       = 'true',
    'projection.dt.type'       = 'date',
    'projection.dt.format'     = 'yyyy-MM-dd',
    'projection.dt.range'      = '2024-01-01,NOW',
    'projection.dt.interval'   = '1',
    'projection.dt.interval.unit' = 'DAYS',
    'projection.hour.type'     = 'integer',
    'projection.hour.range'    = '0,23',
    'projection.hour.digits'   = '2',
    'storage.location.template' = 's3://${DATA_BUCKET}/agg/dt=${dt}/hour=${hour}/'
);
