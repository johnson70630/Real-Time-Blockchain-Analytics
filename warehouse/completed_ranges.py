"""Snowflake persistence for append-only completed source ranges."""

from __future__ import annotations

import os
import re
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from orchestration.aave_forward import CompletedBlockRange
from orchestration.watermark import WatermarkIdentity

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


class CompletedRangeEvidenceError(RuntimeError):
    """Base exception for invalid completed-range evidence."""


class DuplicateCompletedRangeEvidence(CompletedRangeEvidenceError):
    """Raised when one logical interval has multiple physical evidence rows."""


class ConflictingCompletedRangeEvidence(CompletedRangeEvidenceError):
    """Raised when an interval exists with different success metadata."""


def _identifier(value: str, setting: str) -> str:
    normalized = value.strip().upper()
    if not _IDENTIFIER_PATTERN.fullmatch(normalized):
        raise ValueError(f"{setting} must be a valid Snowflake identifier")
    return normalized


@dataclass(frozen=True, slots=True)
class CompletedRangeTableConfig:
    """Non-secret names for the pre-provisioned completed-range ledger."""

    database: str = "BLOCKCHAIN_ANALYTICS"
    schema: str = "CONTROL"
    table: str = "PIPELINE_COMPLETED_RANGES"

    def __post_init__(self) -> None:
        _identifier(self.database, "SNOWFLAKE_DATABASE")
        _identifier(self.schema, "SNOWFLAKE_CONTROL_SCHEMA")
        _identifier(self.table, "SNOWFLAKE_COMPLETED_RANGES_TABLE")

    @classmethod
    def from_env(
        cls,
        environ: Mapping[str, str] | None = None,
    ) -> CompletedRangeTableConfig:
        """Load control-object names without reading credentials."""
        values = os.environ if environ is None else environ
        return cls(
            database=values.get(
                "SNOWFLAKE_DATABASE",
                "BLOCKCHAIN_ANALYTICS",
            ),
            schema=values.get("SNOWFLAKE_CONTROL_SCHEMA", "CONTROL"),
            table=values.get(
                "SNOWFLAKE_COMPLETED_RANGES_TABLE",
                "PIPELINE_COMPLETED_RANGES",
            ),
        )

    @property
    def qualified_table(self) -> str:
        """Return the validated fully qualified table name."""
        return ".".join(
            (
                _identifier(self.database, "SNOWFLAKE_DATABASE"),
                _identifier(self.schema, "SNOWFLAKE_CONTROL_SCHEMA"),
                _identifier(
                    self.table,
                    "SNOWFLAKE_COMPLETED_RANGES_TABLE",
                ),
            )
        )


@dataclass(frozen=True, slots=True)
class CompletedRangeCoverage:
    """Physical and logical coverage diagnostics for a range ledger."""

    physical_rows: int
    logical_ranges: int
    duplicate_groups: int
    highest_contiguous_block: int
    gaps: tuple[tuple[int, int], ...]


def summarize_completed_range_coverage(
    project_start_block: int,
    completed_ranges: tuple[CompletedBlockRange, ...],
) -> CompletedRangeCoverage:
    """Summarize coverage over distinct intervals while surfacing duplicates."""
    if project_start_block < 0:
        raise ValueError("project_start_block must not be negative")
    counts: dict[tuple[int, int], int] = {}
    for item in completed_ranges:
        key = (item.start_block, item.end_block)
        counts[key] = counts.get(key, 0) + 1
    intervals = sorted(counts)
    contiguous_end = project_start_block - 1
    gaps: list[tuple[int, int]] = []
    merged_end = project_start_block - 1
    for start_block, end_block in intervals:
        if end_block < project_start_block:
            continue
        effective_start = max(start_block, project_start_block)
        if effective_start > merged_end + 1:
            gaps.append((merged_end + 1, effective_start - 1))
        merged_end = max(merged_end, end_block)
        if effective_start <= contiguous_end + 1:
            contiguous_end = max(contiguous_end, end_block)
    return CompletedRangeCoverage(
        physical_rows=len(completed_ranges),
        logical_ranges=len(counts),
        duplicate_groups=sum(count > 1 for count in counts.values()),
        highest_contiguous_block=contiguous_end,
        gaps=tuple(gaps),
    )

class SnowflakeCompletedRangeStore:
    """Read and append successfully persisted source block intervals."""

    def __init__(
        self,
        connection: Any,
        config: CompletedRangeTableConfig | None = None,
    ) -> None:
        self.connection = connection
        self.config = config or CompletedRangeTableConfig.from_env()

    @staticmethod
    def _key(identity: WatermarkIdentity) -> tuple[Any, ...]:
        return (
            identity.pipeline_name,
            identity.source_name,
            identity.chain_id,
            identity.watermark_type,
        )

    def list_ranges(
        self,
        identity: WatermarkIdentity,
    ) -> tuple[CompletedBlockRange, ...]:
        """Return all immutable successful intervals for one pipeline."""
        query = f"""
            SELECT start_block, end_block, successful_run_id, completed_at
            FROM {self.config.qualified_table}
            WHERE pipeline_name = %s
              AND source_name = %s
              AND chain_id = %s
              AND watermark_type = %s
            ORDER BY start_block, end_block, completed_at
        """
        with closing(self.connection.cursor()) as cursor:
            cursor.execute(query, self._key(identity))
            rows = cursor.fetchall()
        return tuple(
            CompletedBlockRange(
                start_block=int(row[0]),
                end_block=int(row[1]),
                successful_run_id=str(row[2]),
                completed_at=row[3],
            )
            for row in rows
        )

    def record_range(
        self,
        identity: WatermarkIdentity,
        completed_range: CompletedBlockRange,
    ) -> CompletedBlockRange:
        """Append one range after persistence, ignoring an exact safe retry."""
        self.record_ranges(identity, (completed_range,))
        return completed_range

    def record_ranges(
        self,
        identity: WatermarkIdentity,
        completed_ranges: tuple[CompletedBlockRange, ...],
    ) -> tuple[CompletedBlockRange, ...]:
        """Append a persisted batch atomically, allowing exact safe retries."""
        if not completed_ranges:
            return ()
        for completed_range in completed_ranges:
            self._validate_completed_range(completed_range)
        with closing(self.connection.cursor()) as cursor:
            cursor.execute("BEGIN TRANSACTION")
            try:
                for completed_range in completed_ranges:
                    existing = self._matching_rows(
                        cursor,
                        identity,
                        completed_range,
                    )
                    if len(existing) > 1:
                        raise DuplicateCompletedRangeEvidence(
                            "Multiple physical evidence rows exist for logical "
                            f"range {completed_range.start_block}-"
                            f"{completed_range.end_block}"
                        )
                    if existing:
                        self._assert_identical(completed_range, existing[0])
                        continue
                    cursor.execute(
                        self._insert_query(),
                        self._insert_parameters(identity, completed_range),
                    )
                    if cursor.rowcount != 1:
                        raise RuntimeError(
                            "Unexpected completed-range insert count"
                        )
                cursor.execute("COMMIT")
            except Exception:
                cursor.execute("ROLLBACK")
                raise
        return completed_ranges

    @staticmethod
    def _validate_completed_range(
        completed_range: CompletedBlockRange,
    ) -> None:
        if not completed_range.successful_run_id.strip():
            raise ValueError("successful_run_id must not be empty")
        if completed_range.completed_at is None:
            raise ValueError("completed_at is required")

    def _insert_query(self) -> str:
        return f"""
            INSERT INTO {self.config.qualified_table} (
                pipeline_name,
                source_name,
                chain_id,
                watermark_type,
                start_block,
                end_block,
                successful_run_id,
                completed_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

    def _insert_parameters(
        self,
        identity: WatermarkIdentity,
        completed_range: CompletedBlockRange,
    ) -> tuple[Any, ...]:
        return (
            *self._key(identity),
            completed_range.start_block,
            completed_range.end_block,
            completed_range.successful_run_id,
            completed_range.completed_at,
        )

    def _matching_rows(
        self,
        cursor: Any,
        identity: WatermarkIdentity,
        completed_range: CompletedBlockRange,
    ) -> list[tuple[Any, ...]]:
        query = f"""
            SELECT successful_run_id, completed_at
            FROM {self.config.qualified_table}
            WHERE pipeline_name = %s
              AND source_name = %s
              AND chain_id = %s
              AND watermark_type = %s
              AND start_block = %s
              AND end_block = %s
            ORDER BY completed_at
        """
        cursor.execute(
            query,
            (
                *self._key(identity),
                completed_range.start_block,
                completed_range.end_block,
            ),
        )
        return list(cursor.fetchall())

    @staticmethod
    def _assert_identical(
        candidate: CompletedBlockRange,
        existing: tuple[Any, ...],
    ) -> None:
        run_id, completed_at = existing
        if (
            str(run_id) != candidate.successful_run_id
            or not isinstance(completed_at, datetime)
            or completed_at != candidate.completed_at
        ):
            raise ConflictingCompletedRangeEvidence(
                "Completed range already exists with conflicting success metadata"
            )
