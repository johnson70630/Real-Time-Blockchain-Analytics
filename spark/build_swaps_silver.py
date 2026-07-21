import json
import logging
from pathlib import Path
from uuid import uuid4

import duckdb

from config.logging import configure_logging
from config.metadata import (
    BRONZE_OUTPUT_METADATA_FIELDS,
    SILVER_METADATA_FIELDS,
)
from config.settings import (
    BRONZE_OUTPUT_PATH,
    PROJECT_ROOT,
    SILVER_DIR,
    SILVER_OUTPUT_FILE,
    SILVER_PROCESSED_FILES_MANIFEST,
)
from config.versions import SILVER_JOB_VERSION
from spark.parquet import discover_parquet_files, write_relation_atomic

logger = logging.getLogger(__name__)

SILVER_UPSTREAM_COLUMNS = (
    "protocol",
    "chain",
    "event_date",
    "event_type",
    "block_number",
    "transaction_hash",
    "pool_address",
    "log_index",
    "raw_data",
    "raw_topics",
    "kafka_timestamp",
    *BRONZE_OUTPUT_METADATA_FIELDS,
)

SILVER_COLUMNS = (
    *SILVER_UPSTREAM_COLUMNS,
    *SILVER_METADATA_FIELDS,
)

SILVER_OUTPUT_COLUMNS = ("event_id", *SILVER_COLUMNS)

LEGACY_METADATA_FALLBACKS = {
    "producer_version": "'legacy'",
    "schema_version": "'legacy'",
    "bronze_processed_at": "ingested_at",
    "bronze_file": "'legacy'",
    "silver_processed_at": "ingested_at",
    "silver_job_version": "'legacy'",
}


def discover_bronze_files(bronze_root: Path = BRONZE_OUTPUT_PATH) -> list[Path]:
    """Return deterministically sorted Bronze Parquet data files."""
    return discover_parquet_files(bronze_root)


def to_manifest_entry(path: Path, project_root: Path = PROJECT_ROOT) -> str:
    """Represent a file relative to the project root when possible."""
    resolved_path = path.resolve()

    try:
        return resolved_path.relative_to(project_root.resolve()).as_posix()
    except ValueError:
        return resolved_path.as_posix()


def load_processed_files(
    manifest_path: Path = SILVER_PROCESSED_FILES_MANIFEST,
) -> set[str]:
    """Load and validate the processed Bronze file manifest."""
    if not manifest_path.exists():
        return set()

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(
            f"Invalid Silver processed-files manifest at {manifest_path}: {error}"
        ) from error

    if not isinstance(manifest, dict):
        raise ValueError(
            f"Invalid Silver processed-files manifest at {manifest_path}: "
            "expected a JSON object"
        )

    processed_files = manifest.get("processed_files")

    if not isinstance(processed_files, list) or not all(
        isinstance(path, str) and path for path in processed_files
    ):
        raise ValueError(
            f"Invalid Silver processed-files manifest at {manifest_path}: "
            "'processed_files' must be a list of non-empty strings"
        )

    return set(processed_files)


def save_processed_files(
    processed_files: set[str],
    manifest_path: Path = SILVER_PROCESSED_FILES_MANIFEST,
) -> None:
    """Atomically save the processed Bronze file manifest."""
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = manifest_path.with_name(
        f".{manifest_path.name}.{uuid4().hex}.tmp"
    )
    manifest = {"processed_files": sorted(processed_files)}

    try:
        temporary_path.write_text(
            json.dumps(manifest, indent=2) + "\n",
            encoding="utf-8",
        )
        temporary_path.replace(manifest_path)
    finally:
        temporary_path.unlink(missing_ok=True)


def select_unprocessed_files(
    discovered_files: list[Path],
    processed_files: set[str],
    project_root: Path = PROJECT_ROOT,
) -> list[Path]:
    """Select discovered Bronze files that are absent from the manifest."""
    return [
        path
        for path in discovered_files
        if to_manifest_entry(path, project_root) not in processed_files
    ]


def create_compatible_view(
    connection: duckdb.DuckDBPyConnection,
    source_view: str,
    target_view: str,
    required_columns: tuple[str, ...],
) -> None:
    """Project a stable metadata schema, including pre-milestone local files."""
    available_columns = {
        row[0] for row in connection.sql(f"DESCRIBE {source_view}").fetchall()
    }
    projections = []

    for column in required_columns:
        if column in available_columns:
            projections.append(column)
        elif column in LEGACY_METADATA_FALLBACKS:
            projections.append(
                f"{LEGACY_METADATA_FALLBACKS[column]} AS {column}"
            )
        else:
            raise ValueError(
                f"Required column '{column}' is missing from {source_view}"
            )

    connection.sql(
        f"CREATE TEMP VIEW {target_view} AS "
        f"SELECT {', '.join(projections)} FROM {source_view}"
    )


def _silver_merge_query(include_existing_silver: bool) -> str:
    columns = ",\n                    ".join(SILVER_COLUMNS)
    upstream_columns = ",\n                    ".join(SILVER_UPSTREAM_COLUMNS)
    new_bronze_source = f"""
                SELECT
                    {upstream_columns},
                    -- Processing time and job version identify this Silver run.
                    CURRENT_TIMESTAMP AS silver_processed_at,
                    '{SILVER_JOB_VERSION}' AS silver_job_version
                FROM new_bronze
                WHERE event_type = 'swap'
                  AND protocol IS NOT NULL
                  AND chain IS NOT NULL
                  AND event_date IS NOT NULL
                  AND transaction_hash IS NOT NULL
                  AND pool_address IS NOT NULL
                  AND block_number IS NOT NULL
                  AND log_index IS NOT NULL
    """

    if include_existing_silver:
        sources = f"""
                SELECT
                    {columns}
                FROM existing_silver
                UNION ALL
                {new_bronze_source}
        """
    else:
        sources = new_bronze_source

    return f"""
        SELECT
            chain || '-' || transaction_hash || '-'
                || CAST(log_index AS VARCHAR) AS event_id,
            {columns}
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY chain, transaction_hash, log_index
                    ORDER BY kafka_timestamp DESC, ingested_at DESC
                ) AS row_num
            FROM (
                {sources}
            )
        )
        WHERE row_num = 1
    """


def merge_silver_swaps(new_bronze_files: list[Path]) -> tuple[int, int]:
    """Merge new Bronze files into Silver using an atomic output replacement."""
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    silver_exists = SILVER_OUTPUT_FILE.exists()
    connection = duckdb.connect()

    try:
        connection.read_parquet(
            [str(path) for path in new_bronze_files],
            hive_partitioning=True,
            union_by_name=True,
        ).create_view("new_bronze_raw")
        create_compatible_view(
            connection,
            "new_bronze_raw",
            "new_bronze",
            SILVER_UPSTREAM_COLUMNS,
        )

        if silver_exists:
            connection.read_parquet(str(SILVER_OUTPUT_FILE)).create_view(
                "existing_silver_raw"
            )
            create_compatible_view(
                connection,
                "existing_silver_raw",
                "existing_silver",
                SILVER_COLUMNS,
            )
            row_count_before = connection.sql(
                "SELECT COUNT(*) FROM existing_silver"
            ).fetchone()[0]
        else:
            row_count_before = 0

        merged_silver = connection.sql(_silver_merge_query(silver_exists))
        row_count_after = write_relation_atomic(
            connection,
            merged_silver,
            SILVER_OUTPUT_FILE,
        )
    finally:
        connection.close()

    return row_count_before, row_count_after


def build_silver_swaps() -> None:
    discovered_files = discover_bronze_files()
    processed_files = load_processed_files()
    new_bronze_files = select_unprocessed_files(discovered_files, processed_files)

    logger.info("Total Bronze Parquet files discovered: %s", len(discovered_files))
    logger.info(
        "Bronze files already processed: %s",
        len(discovered_files) - len(new_bronze_files),
    )
    logger.info("New Bronze files selected: %s", len(new_bronze_files))

    if not new_bronze_files:
        logger.info("No new Bronze files found; Silver output is unchanged")
        return

    row_count_before, row_count_after = merge_silver_swaps(new_bronze_files)
    logger.info("Silver row count before merge: %s", row_count_before)
    logger.info("Silver row count after merge: %s", row_count_after)

    updated_processed_files = processed_files | {
        to_manifest_entry(path) for path in new_bronze_files
    }
    save_processed_files(updated_processed_files)
    logger.info("Updated Silver manifest: %s", SILVER_PROCESSED_FILES_MANIFEST)
    logger.info("Silver swaps written to: %s", SILVER_OUTPUT_FILE)


def main() -> None:
    configure_logging()
    build_silver_swaps()


if __name__ == "__main__":
    main()
