"""Shared filesystem helpers for local Parquet pipeline stages."""

from pathlib import Path
from uuid import uuid4

import duckdb


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
