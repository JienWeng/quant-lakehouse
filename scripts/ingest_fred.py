#!/usr/bin/env python3
"""
Land FRED series observations into MinIO as partitioned Parquet.
Requires FRED_API_KEY (free): https://fred.stlouisfed.org/docs/api/api_key.html
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Any

import boto3
import pandas as pd
import requests
from botocore.exceptions import ClientError

FRED_OBS = "https://api.stlouisfed.org/fred/series/observations"


def ensure_bucket(s3: Any, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)


def fetch_series(series_id: str, api_key: str, observation_start: str) -> pd.DataFrame:
    if not api_key or not str(api_key).strip():
        print(
            "ERROR: FRED_API_KEY is missing or empty.\n"
            " 1. Add to quant-lakehouse/.env: FRED_API_KEY=your_key\n"
            "  2. From that folder run: docker compose up -d\n"
            "  3. Free key: https://fred.stlouisfed.org/docs/api/api_key.html",
            file=sys.stderr,
        )
        raise SystemExit(2)
    r = requests.get(
        FRED_OBS,
        params={
            "series_id": series_id,
            "api_key": api_key,
            "file_type": "json",
            "observation_start": observation_start,
        },
        timeout=120,
    )
    r.raise_for_status()
    body = r.json()
    obs = body.get("observations") or []
    rows = []
    for o in obs:
        val = o.get("value")
        if val in (".", None):
            continue
        rows.append(
            {
                "series_id": series_id,
                "dt": o.get("date"),
                "value": float(val),
                "realtime_start": o.get("realtime_start"),
                "realtime_end": o.get("realtime_end"),
            }
        )
    return pd.DataFrame.from_records(rows)


def write_partitions(df: pd.DataFrame, bucket: str, endpoint: str, key: str, secret: str) -> None:
    if df.empty:
        print("No FRED rows; check series id / API key.", file=sys.stderr)
        return
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        region_name="us-east-1",
    )
    ensure_bucket(s3, bucket)
    for (series_id, dt), part in df.groupby(["series_id", "dt"]):
        sub = part.drop(columns=["series_id", "dt"])
        path = f"s3://{bucket}/raw/fred/series_id={series_id}/dt={dt}/part-000.parquet"
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
    p = argparse.ArgumentParser(description="Ingest FRED series to MinIO")
    p.add_argument(
        "--series",
        default=os.environ.get("FRED_SERIES", "VIXCLS"),
        help="FRED series id (default VIXCLS)",
    )
    p.add_argument(
        "--start",
        default=os.environ.get("FRED_START", "2020-01-01"),
    )
    args = p.parse_args()

    endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:9000")
    bucket = os.environ.get("S3_BUCKET", "quant-lake")
    key = os.environ.get("AWS_ACCESS_KEY_ID", "minio")
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "minio12345")
    api_key = os.environ.get("FRED_API_KEY", "")

    df = fetch_series(args.series, api_key, args.start)
    write_partitions(df, bucket, endpoint, key, secret)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
