-- Silver: FRED — partition columns last
CREATE OR REPLACE TEMP VIEW v_raw_fred
USING PARQUET
OPTIONS (path 's3a://quant-lake/raw/fred');

CREATE OR REPLACE TABLE silver_fred_daily
USING PARQUET
PARTITIONED BY (series_id, dt)
LOCATION 's3a://quant-lake/silver/fred_daily'
AS
SELECT
  value,
  realtime_start,
  realtime_end,
  series_id,
  dt
FROM v_raw_fred;
