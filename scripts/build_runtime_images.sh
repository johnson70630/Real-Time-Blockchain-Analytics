#!/usr/bin/env bash
# Local builds only: no ECR push, AWS login, or deployment.
set -euo pipefail
umask 077
cd "$(dirname "$0")/.."
context_dir="$(mktemp -d "${TMPDIR:-/tmp}/blockchain-image-context.XXXXXX")"
jar_cache="${BLOCKCHAIN_MAVEN_CACHE:-$context_dir/maven-cache}"
python3 - "$context_dir/jars.lock.json" <<'PY'
import sys
from pathlib import Path
from runtime.bundle import _git
from runtime.images import ROOT, _write_new

_write_new(Path(sys.argv[1]), _git(ROOT, "show", "HEAD:deploy/runtime/jars.lock.json"))
PY
python3 deploy/runtime/fetch_jars.py "$context_dir/jars.lock.json" --cache-only "$jar_cache"
python3 -m runtime.images context --jar-cache "$jar_cache" --output "$context_dir/context.tar" > "$context_dir/receipt.json"
source_commit="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["source_commit"])' "$context_dir/receipt.json")"
endpoint="$(docker context inspect --format '{{.Endpoints.docker.Host}}')"
case "$endpoint" in unix:///*) ;; *) echo LOCAL_UNIX_DOCKER_REQUIRED >&2; exit 1 ;; esac
unset DOCKER_HOST DOCKER_CONTEXT DOCKER_TLS_VERIFY DOCKER_CERT_PATH BUILDX_BUILDER BUILDKIT_HOST
docker_local=(docker --host "$endpoint")
echo "BUILD_SOURCE_COMMIT=$source_commit"
echo "BUILD_CONTEXT_RECEIPT=$context_dir/receipt.json"
for role in producer spark; do
  "${docker_local[@]}" build --builder default --platform linux/amd64 \
    --label "org.opencontainers.image.revision=$source_commit" \
    --label "org.opencontainers.image.title=blockchain-runtime-$role" \
    -f "deploy/runtime/Dockerfile.$role" -t "blockchain-pilot-$role:offline" - < "$context_dir/context.tar"
done
"${docker_local[@]}" run --rm --network none --read-only --tmpfs /tmp:size=2g,mode=1777,exec \
  --entrypoint python blockchain-pilot-spark:offline -c \
  'from pyspark.sql import SparkSession; s=SparkSession.builder.master("local[1]").config("spark.ui.enabled","false").getOrCreate(); j=s.sparkContext._jvm; assert s.version=="4.0.1"; assert j.java.lang.System.getProperty("java.specification.version")=="17"; assert j.org.apache.hadoop.util.VersionInfo.getVersion()=="3.4.1"; c=j.java.lang.Thread.currentThread().getContextClassLoader(); [c.loadClass(n) for n in ["org.apache.spark.sql.kafka010.KafkaSourceProvider","org.apache.hadoop.fs.s3a.S3AFileSystem","org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider","software.amazon.awssdk.services.s3.S3Client"]]; s.range(3).write.parquet("/tmp/smoke.parquet"); assert s.read.parquet("/tmp/smoke.parquet").count()==3; s.stop(); print("JAVA_SPARK_S3A_KAFKA_PARQUET_VERIFIED")'
