#!/usr/bin/env bash
set -euo pipefail
# Run Spark SQL file against MinIO (S3A). Used by Airflow BashOperator.
SQL_FILE="${1:?Usage: run_spark_sql.sh /path/to/file.sql}"
ENDPOINT="${SPARK_S3A_ENDPOINT:-http://minio:9000}"
KEY="${AWS_ACCESS_KEY_ID:-minio}"
SECRET="${AWS_SECRET_ACCESS_KEY:-minio12345}"
BUCKET="${S3_BUCKET:-quant-lake}"

# SQL files use placeholder bucket name "quant-lake"; substitute from env for MinIO paths.
TMP="$(mktemp)"
trap 'rm -f "${TMP}"' EXIT
sed "s|quant-lake|${BUCKET}|g" "${SQL_FILE}" > "${TMP}"

# Image sets JAVA_HOME; allow override if a task env strips it.
if [ -z "${JAVA_HOME:-}" ] && [ -x /opt/java/current/bin/java ]; then
  export JAVA_HOME=/opt/java/current
fi
export JAVA_HOME="${JAVA_HOME:?JAVA_HOME must be set for spark-sql (rebuild image: Dockerfile sets /opt/java/current)}"

exec spark-sql \
  --master 'local[*]' \
  --conf "spark.hadoop.fs.s3a.endpoint=${ENDPOINT}" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.access.key=${KEY}" \
  --conf "spark.hadoop.fs.s3a.secret.key=${SECRET}" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  --conf "spark.sql.warehouse.dir=s3a://${BUCKET}/spark-warehouse" \
  -f "${TMP}"
