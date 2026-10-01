"""Shared filesystem helpers for local Parquet pipeline stages."""

import shutil
from pathlib import Path
from uuid import uuid4

import duckdb
from pyspark.sql import DataFrame

StoragePath = str | Path


def is_remote_path(path: StoragePath) -> bool:
    """Return whether a Parquet location is handled by Hadoop object storage."""
    return str(path).startswith("s3a://")


def _remove_path(path: Path) -> None:
    """Remove a local file or directory without assuming its storage shape."""
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
    else:
        path.unlink(missing_ok=True)


def discover_parquet_files(root: Path) -> list[Path]:
    """Return deterministic, non-hidden Parquet files below a root path."""
    if not root.exists():
        return []

    return sorted(
        (
            path
            for path in root.rglob("*.parquet")
            if path.is_file() and not path.name.startswith((".", "_"))
        ),
        key=lambda path: path.as_posix(),
    )


def write_relation_atomic(
    connection: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
    output_path: Path,
) -> int:
    """Write a relation once and atomically replace its final Parquet file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_output = output_path.with_name(
        f".{output_path.stem}.{uuid4().hex}.tmp.parquet"
    )

    try:
        relation.write_parquet(str(temporary_output), overwrite=True)
        row_count = (
            connection.read_parquet(str(temporary_output))
            .count("*")
            .fetchone()[0]
        )
        temporary_output.replace(output_path)
    except Exception:
        temporary_output.unlink(missing_ok=True)
        raise

    return row_count


def write_partitioned_dataset_atomic(
    frame: DataFrame,
    output_path: StoragePath,
    partition_columns: tuple[str, ...],
) -> None:
    """Replace a Parquet dataset, using local atomic renames where available."""
    if is_remote_path(output_path):
        writer = frame.write.mode("overwrite")
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        writer.parquet(str(output_path))
        return

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    token = uuid4().hex
    staging_path = output_path.with_name(f".{output_path.name}.{token}.staging")
    backup_path = output_path.with_name(f".{output_path.name}.{token}.backup")

    try:
        writer = frame.write.mode("overwrite")
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        writer.parquet(str(staging_path))
        if output_path.exists():
            output_path.replace(backup_path)
        staging_path.replace(output_path)
        if backup_path.exists():
            _remove_path(backup_path)
    except Exception:
        if backup_path.exists() and not output_path.exists():
            backup_path.replace(output_path)
        raise
    finally:
        if staging_path.exists():
            _remove_path(staging_path)
