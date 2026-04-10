-- Silver: OKX candles — partition columns last for Spark CTAS
CREATE OR REPLACE TEMP VIEW v_raw_okx
USING PARQUET
OPTIONS (path 's3a://quant-lake/raw/okx');

CREATE OR REPLACE TABLE silver_okx_daily
USING PARQUET
PARTITIONED BY (inst_id, dt)
LOCATION 's3a://quant-lake/silver/okx_daily'
AS
SELECT
  ts,
  `open`,
  high,
  low,
  `close`,
  volume,
  vol_ccy,
  vol_ccy_quote,
  confirm,
  inst_id,
  dt
FROM v_raw_okx;
