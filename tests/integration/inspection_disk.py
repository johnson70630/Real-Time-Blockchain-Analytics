"""Inspect stopped synthetic Kafka's real files with no network and RO mounts."""

import json
from pathlib import Path

from runtime.inspection_transport import retained_disk_identity
from runtime.recovery import _topic_identity

baseline = json.loads(Path("/test-data/recovery-baseline.json").read_text())["snapshot"]
expected = baseline["scope"]
disk = retained_disk_identity(
    "/kafka", expected["cluster_id"], topic="synthetic-pilot-events"
)
assert disk["topic_id"] == _topic_identity(expected["topic_id"])
assert disk["node_id"] == 1 and disk["partition"] == 0
assert len(disk["metadata_sha256"]) == 2
print("RETAINED_KAFKA_DISK_IDENTITY_VERIFIED_OFFLINE", flush=True)
