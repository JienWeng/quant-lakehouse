-- HiveQL: FRED observations — partition keys only in S3 path (see ingest_fred.py)
CREATE DATABASE IF NOT EXISTS quant_raw;

CREATE EXTERNAL TABLE IF NOT EXISTS quant_raw.fred_observations (
  value DOUBLE,
  realtime_start STRING,
  realtime_end STRING
)
PARTITIONED BY (series_id STRING, dt STRING)
STORED AS PARQUET
LOCATION 's3a://quant-lake/raw/fred'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');
