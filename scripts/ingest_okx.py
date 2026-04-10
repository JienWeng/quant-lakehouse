#!/usr/bin/env python3
"""
Land OKX public candlesticks into MinIO (S3-compatible) as partitioned Parquet.
No API keys required for public market/candles.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from typing import Any

import boto3
import pandas as pd
import requests
from botocore.exceptions import ClientError

OKX_CANDLES = "https://www.okx.com/api/v5/market/candles"


def ensure_bucket(s3: Any, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)


def fetch_candles(inst_id: str, bar: str, limit: int) -> pd.DataFrame:
    r = requests.get(
        OKX_CANDLES,
        params={"instId": inst_id, "bar": bar, "limit": str(limit)},
        timeout=60,
    )
    r.raise_for_status()
    body = r.json()
    if body.get("code") != "0":
        raise RuntimeError(f"OKX error: {body}")
    rows = body.get("data") or []
    # [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
    records = []
    for row in rows:
        ts_ms = int(row[0])
        dt_utc = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date().isoformat()
        records.append(
            {
                "ts": ts_ms,
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "vol_ccy": float(row[6]),
                "vol_ccy_quote": float(row[7]),
                "confirm": int(row[8]) if len(row) > 8 else 1,
                "inst_id": inst_id,
                "dt": dt_utc,
            }
        )
    return pd.DataFrame.from_records(records)


def write_partitions(df: pd.DataFrame, bucket: str, endpoint: str, key: str, secret: str) -> None:
    if df.empty:
        print("No rows from OKX; skipping write.", file=sys.stderr)
        return
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        region_name="us-east-1",
    )
    ensure_bucket(s3, bucket)
    for (inst_id, dt), part in df.groupby(["inst_id", "dt"]):
        # Partition keys live in the path only (cleaner for Hive EXTERNAL + MSCK REPAIR).
        sub = part.drop(columns=["inst_id", "dt"])
        path = f"s3://{bucket}/raw/okx/inst_id={inst_id}/dt={dt}/part-000.parquet"
        sub.to_parquet(
            path,
            index=False,
            storage_options={
                "key": key,
                "secret": secret,
                "client_kwargs": {"endpoint_url": endpoint},
            },
        )
        print(f"Wrote {len(sub)} rows -> {path}")


def main() -> int:
    p = argparse.ArgumentParser(description="Ingest OKX candles to MinIO")
    p.add_argument("--inst-id", default=os.environ.get("OKX_INST_ID", "BTC-USDT"))
    p.add_argument("--bar", default=os.environ.get("OKX_BAR", "1D"))
    p.add_argument("--limit", type=int, default=int(os.environ.get("OKX_LIMIT", "300")))
    args = p.parse_args()

    endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:9000")
    bucket = os.environ.get("S3_BUCKET", "quant-lake")
    key = os.environ.get("AWS_ACCESS_KEY_ID", "minio")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "minio12345")

    df = fetch_candles(args.inst_id, args.bar, args.limit)
    write_partitions(df, bucket, endpoint, key, secret)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
