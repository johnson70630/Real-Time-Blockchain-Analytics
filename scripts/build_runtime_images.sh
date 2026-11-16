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
for role in producer spark kafka; do
  "${docker_local[@]}" build --builder default --platform linux/amd64 \
    --label "org.opencontainers.image.revision=$source_commit" \
    --label "org.opencontainers.image.title=blockchain-runtime-$role" \
    -f "deploy/runtime/Dockerfile.$role" -t "blockchain-pilot-$role:offline" - < "$context_dir/context.tar"
done
tar -xOf "$context_dir/context.tar" deploy/runtime/smoke_producer.py | \
  "${docker_local[@]}" run --rm -i --network none --read-only \
    --cap-drop ALL --security-opt no-new-privileges:true \
    --entrypoint python blockchain-pilot-producer:offline -
expected_ca_sha="$(tar -xOf "$context_dir/context.tar" deploy/runtime/images.lock.json | python3 -c 'import json,sys; print(next(row["trust_store_sha256"] for row in json.load(sys.stdin)["sources"] if row["role"]=="java17"))')"
tar -xOf "$context_dir/context.tar" deploy/runtime/smoke_spark.py | \
  "${docker_local[@]}" run --rm -i --network none --read-only --tmpfs /tmp:size=2g,mode=1777,exec \
    --cap-drop ALL --security-opt no-new-privileges:true \
    --entrypoint python blockchain-pilot-spark:offline - "$expected_ca_sha"
tar -xOf "$context_dir/context.tar" deploy/runtime/smoke_jvm_shadow.py | \
  "${docker_local[@]}" run --rm -i --network none --read-only --tmpfs /tmp:size=2g,mode=1777,exec \
    --cap-drop ALL --security-opt no-new-privileges:true \
    --env PYSPARK_SUBMIT_ARGS=pyspark-shell --entrypoint python blockchain-pilot-spark:offline -
expected_kafka_ca_sha="$(tar -xOf "$context_dir/context.tar" deploy/runtime/images.lock.json | python3 -c 'import json,sys; print(next(row["trust_store_sha256"] for row in json.load(sys.stdin)["sources"] if row["role"]=="kafka_java21"))')"
tar -xOf "$context_dir/context.tar" deploy/runtime/smoke_kafka.sh | \
  "${docker_local[@]}" run --rm -i --network none --read-only \
    --cap-drop ALL --security-opt no-new-privileges:true \
    --entrypoint /bin/sh blockchain-pilot-kafka:offline -s -- "$expected_kafka_ca_sha"
