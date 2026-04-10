-- HiveQL: external table over OKX raw Parquet (Hive 3+ / compatible metastore).
-- After landing new partitions, run: MSCK REPAIR TABLE quant_raw.okx_candles;
CREATE DATABASE IF NOT EXISTS quant_raw;

CREATE EXTERNAL TABLE IF NOT EXISTS quant_raw.okx_candles (
  ts BIGINT,
  `open` DOUBLE,
  high DOUBLE,
  low DOUBLE,
  `close` DOUBLE,
  volume DOUBLE,
  vol_ccy DOUBLE,
  vol_ccy_quote DOUBLE,
  confirm INT
)
PARTITIONED BY (inst_id STRING, dt STRING)
STORED AS PARQUET
LOCATION 's3a://quant-lake/raw/okx'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');
