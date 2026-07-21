"""Metadata column contracts shared by pipeline stages and tests."""

# Producer implementation and envelope compatibility identifiers.
PRODUCER_VERSION_FIELDS = (
    "producer_version",
    "schema_version",
)

# Complete producer provenance, including the UTC observation timestamp.
PRODUCER_METADATA_FIELDS = (
    *PRODUCER_VERSION_FIELDS,
    "ingested_at",
)

# Bronze processing time and the Hive partition path containing the record.
BRONZE_METADATA_FIELDS = (
    "bronze_processed_at",
    "bronze_file",
)

# Silver processing time and the normalization job implementation version.
SILVER_METADATA_FIELDS = (
    "silver_processed_at",
    "silver_job_version",
)

# Gold processing time and the aggregation implementation version.
GOLD_METADATA_FIELDS = (
    "gold_processed_at",
    "aggregation_version",
)

# Cumulative contracts make propagation expectations explicit at each layer.
BRONZE_OUTPUT_METADATA_FIELDS = (
    *PRODUCER_METADATA_FIELDS,
    *BRONZE_METADATA_FIELDS,
)
SILVER_OUTPUT_METADATA_FIELDS = (
    *BRONZE_OUTPUT_METADATA_FIELDS,
    *SILVER_METADATA_FIELDS,
)
GOLD_OUTPUT_METADATA_FIELDS = (
    *SILVER_OUTPUT_METADATA_FIELDS,
    *GOLD_METADATA_FIELDS,
)
