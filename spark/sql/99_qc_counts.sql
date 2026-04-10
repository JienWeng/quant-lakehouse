-- Lightweight QC: row counts and date range on gold
CREATE OR REPLACE TEMP VIEW g USING PARQUET OPTIONS (path 's3a://quant-lake/gold/btc_vix_daily');
SELECT
  COUNT(*) AS gold_rows,
  MIN(dt) AS min_dt,
  MAX(dt) AS max_dt,
  SUM(CASE WHEN vix_close IS NULL THEN 1 ELSE 0 END) AS rows_missing_vix
FROM g;
