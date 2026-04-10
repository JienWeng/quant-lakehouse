-- Gold: BTC-USDT daily log returns,20-day rolling vol of log returns, joined to VIX
CREATE OR REPLACE TEMP VIEW v_okx USING PARQUET OPTIONS (path 's3a://quant-lake/silver/okx_daily');
CREATE OR REPLACE TEMP VIEW v_fred USING PARQUET OPTIONS (path 's3a://quant-lake/silver/fred_daily');

CREATE OR REPLACE TABLE gold_btc_vix_daily
USING PARQUET
LOCATION 's3a://quant-lake/gold/btc_vix_daily'
AS
WITH base AS (
  SELECT
    dt,
    inst_id,
    ts,
    close,
    LAG(close) OVER (PARTITION BY inst_id ORDER BY ts) AS prev_close
  FROM v_okx
  WHERE inst_id = 'BTC-USDT'
),
rets AS (
  SELECT
    dt,
    inst_id,
    close,
    CASE
      WHEN prev_close IS NULL OR prev_close = 0 THEN CAST(NULL AS DOUBLE)
      ELSE ln(close / prev_close)
    END AS log_ret
  FROM base
),
vix AS (
  SELECT dt, value AS vix_close
  FROM v_fred
  WHERE series_id = 'VIXCLS'
),
rolled AS (
  SELECT
    dt,
    inst_id,
    close,
    log_ret,
    stddev_pop(log_ret) OVER (
      PARTITION BY inst_id
      ORDER BY to_date(dt)
      ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS roll20d_logret_stdev
  FROM rets
)
SELECT
  r.dt,
  r.inst_id,
  r.close AS btc_close_usdt,
  r.log_ret AS btc_log_ret,
  r.roll20d_logret_stdev AS btc_roll20d_logret_vol,
  v.vix_close
FROM rolled r
LEFT JOIN vix v ON v.dt = r.dt;
