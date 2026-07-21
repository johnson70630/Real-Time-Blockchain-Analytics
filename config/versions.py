"""Operational versions attached to pipeline records for reproducibility."""

# Identifies the producer implementation that decoded and emitted an event.
PRODUCER_VERSION = "1.0.0"

# Identifies the shape and meaning of the shared Kafka event envelope.
SCHEMA_VERSION = "1.0.0"

# Identifies the Silver normalization and deduplication logic.
SILVER_JOB_VERSION = "1.0.0"

# Identifies the current Gold aggregation logic.
GOLD_JOB_VERSION = "1.0.0"
