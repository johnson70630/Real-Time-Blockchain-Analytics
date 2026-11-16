#!/bin/sh
# Read-only, network-disabled validation of the derived broker's Java runtime.
set -eu
expected_ca_sha="${1:?committed Java CA-store hash required}"
actual_ca_sha="$(sha256sum "$JAVA_HOME/lib/security/cacerts" | awk '{print $1}')"
test "$actual_ca_sha" = "$expected_ca_sha"
java -version 2>&1 | grep '^openjdk version "21\.0\.13'
test ! -e /opt/kafka/kafka.jsa
test ! -e /opt/kafka/storage.jsa
if grep -q 'SharedArchiveFile=/opt/kafka/' /etc/kafka/docker/launch; then
    exit 1
fi
printf 'KAFKA_JAVA21_TRUST_STORE_VERIFIED sha256=%s\n' "$actual_ca_sha"
