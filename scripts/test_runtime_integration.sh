#!/usr/bin/env bash
# Deletes only the dedicated, synthetic test project's disposable volumes.
set -euo pipefail
compose=(docker compose -f deploy/runtime/compose.test.yaml)
trap '"${compose[@]}" down --volumes' EXIT
"${compose[@]}" up -d --wait --wait-timeout 180 kafka
"${compose[@]}" run --rm spark-test initial
"${compose[@]}" run --rm spark-test snapshot
"${compose[@]}" stop kafka
"${compose[@]}" run --rm --no-deps inspection-disk-test
"${compose[@]}" up -d --wait --wait-timeout 180 kafka
"${compose[@]}" run --rm spark-test resume
"${compose[@]}" run --rm spark-test snapshot
"${compose[@]}" run --rm spark-test repeat
"${compose[@]}" run --rm spark-test identity-loss
"${compose[@]}" run --rm spark-test loss
