"""Read-only Snowflake queries for the local operational dashboard."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from contextlib import closing
from typing import Any, Protocol

import snowflake.connector
from snowflake.connector import DictCursor

from app.config import AppConfig

OPERATIONS_ROLE = "BLOCKCHAIN_PIPELINE_ROLE"
CONTROL_SCHEMA = "CONTROL"
APPROVED_PIPELINES = (
    "aave_forward",
    "chainlink_forward",
    "uniswap_forward",
    "daily_warehouse_pipeline",
)
APPROVED_RUN_STATUSES = (
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    "NO_OP",
    "SKIPPED",
)


class Cursor(Protocol):
    def execute(
        self,
        query: str,
        params: Mapping[str, Any] | None = None,
    ) -> Cursor: ...

    def fetchone(self) -> Any: ...

    def fetchall(self) -> list[Any]: ...

    def close(self) -> None: ...


class Connection(Protocol):
    def cursor(self, cursor_class: Any = None) -> Cursor: ...

    def close(self) -> None: ...


ConnectionFactory = Callable[..., Connection]


def _secondary_roles_disabled(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        normalized = value.strip()
        if normalized.upper() in {"", "NONE"}:
            return True
        try:
            value = json.loads(normalized)
        except json.JSONDecodeError:
            return False
    if isinstance(value, Mapping):
        return not value.get("roles") and not value.get("value")
    return False


def _lowercase_keys(row: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key).lower(): value for key, value in row.items()}


class SnowflakeOperationsRepository:
    """Read approved CONTROL fields through a separate local-only session."""

    def __init__(
        self,
        config: AppConfig,
        connection_factory: ConnectionFactory | None = None,
    ) -> None:
        if config.is_hosted:
            raise ValueError(
                "Operations data is unavailable in the public hosted application"
            )
        self.config = config
        self._connection_factory = (
            connection_factory or snowflake.connector.connect
        )

    def _connect(self) -> Connection:
        connection = self._connection_factory(
            connection_name=self.config.connection_name,
            role=OPERATIONS_ROLE,
            warehouse=self.config.warehouse,
            database=self.config.database,
            schema=CONTROL_SCHEMA,
            application="DEFI_DATA_LAB_OPERATIONS",
        )
        try:
            with closing(connection.cursor()) as cursor:
                cursor.execute("USE SECONDARY ROLES NONE")
                identity = cursor.execute(
                    "SELECT CURRENT_ROLE(), CURRENT_SECONDARY_ROLES()"
                ).fetchone()
            current_role, secondary_roles = identity
            if (
                str(current_role).upper() != OPERATIONS_ROLE
                or not _secondary_roles_disabled(secondary_roles)
            ):
                raise RuntimeError(
                    "Operations Snowflake session identity validation failed"
                )
        except Exception:
            connection.close()
            raise
        return connection

    def list_alerts(self) -> list[dict[str, Any]]:
        """Return approved lifecycle fields without internal payload data."""
        query = f"""
            SELECT ALERT_KEY, CHECK_NAME, PIPELINE_NAME, REASON_CODE,
                   STATE, SEVERITY, CURRENT_VALUE, WARNING_THRESHOLD,
                   STALE_THRESHOLD, OPENED_AT, LAST_OBSERVED_AT, RESOLVED_AT,
                   OCCURRENCE_COUNT, INCIDENT_GENERATION
            FROM {self.config.database}.{CONTROL_SCHEMA}.ALERT_STATE
            ORDER BY IFF(STATE = 'OPEN', 0, 1),
                     IFF(SEVERITY = 'CRITICAL', 0, 1), ALERT_KEY
        """
        return self._fetch_all(query)

    def list_watermarks(
        self,
        feed_addresses: Sequence[str],
    ) -> list[dict[str, Any]]:
        """Return production block watermarks and configured feed cursors only."""
        normalized_feeds = tuple(
            sorted({address.strip().lower() for address in feed_addresses})
        )
        if any(not address for address in normalized_feeds):
            raise ValueError("feed addresses must not be empty")
        params: dict[str, Any] = {
            "chain_id": 42161,
            "aave_source": "aave_v3_pool",
            "uniswap_source": "uniswap_v3_pools",
            "chainlink_source": "chainlink_feeds",
        }
        feed_filter = "FALSE"
        if normalized_feeds:
            placeholders = []
            for index, address in enumerate(normalized_feeds):
                name = f"feed_{index}"
                placeholders.append(f"%({name})s")
                params[name] = address
            feed_filter = (
                "(PIPELINE_NAME = 'chainlink_forward' "
                "AND WATERMARK_TYPE = 'round_id' "
                f"AND LOWER(SOURCE_NAME) IN ({', '.join(placeholders)}))"
            )
        query = f"""
            SELECT PIPELINE_NAME, SOURCE_NAME, CHAIN_ID, WATERMARK_TYPE,
                   COMMITTED_VALUE, SUCCESSFUL_RUN_ID, UPDATED_AT
            FROM {self.config.database}.{CONTROL_SCHEMA}.PIPELINE_WATERMARKS
            WHERE CHAIN_ID = %(chain_id)s
              AND (
                (PIPELINE_NAME = 'aave_forward'
                 AND SOURCE_NAME = %(aave_source)s
                 AND WATERMARK_TYPE = 'block_number')
                OR (PIPELINE_NAME = 'uniswap_forward'
                    AND SOURCE_NAME = %(uniswap_source)s
                    AND WATERMARK_TYPE = 'block_number')
                OR (PIPELINE_NAME = 'chainlink_forward'
                    AND SOURCE_NAME = %(chainlink_source)s
                    AND WATERMARK_TYPE = 'block_number')
                OR {feed_filter}
              )
            ORDER BY PIPELINE_NAME, WATERMARK_TYPE, SOURCE_NAME
        """
        return self._fetch_all(query, params)

    def list_recent_runs(
        self,
        *,
        pipeline: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return recent safe run metrics with validated optional filters."""
        if pipeline is not None and pipeline not in APPROVED_PIPELINES:
            raise ValueError("pipeline is not approved for operations display")
        if status is not None and status not in APPROVED_RUN_STATUSES:
            raise ValueError("status is not a recognized pipeline run state")
        if not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500")

        filters = [
            "PIPELINE_NAME IN (%(aave)s, %(chainlink)s, "
            "%(uniswap)s, %(warehouse)s)"
        ]
        params: dict[str, Any] = {
            "aave": APPROVED_PIPELINES[0],
            "chainlink": APPROVED_PIPELINES[1],
            "uniswap": APPROVED_PIPELINES[2],
            "warehouse": APPROVED_PIPELINES[3],
            "limit": limit,
        }
        if pipeline is not None:
            filters.append("PIPELINE_NAME = %(pipeline)s")
            params["pipeline"] = pipeline
        if status is not None:
            filters.append("STATUS = %(status)s")
            params["status"] = status
        where_clause = " AND ".join(filters)
        query = f"""
            SELECT PIPELINE_NAME, RUN_ID, RUN_TYPE, STATUS,
                   STARTED_AT, FINISHED_AT, DURATION_SECONDS,
                   WATERMARK_BEFORE, WATERMARK_AFTER, MADE_PROGRESS,
                   RECORDS_READ, RECORDS_WRITTEN, RECORDS_INSERTED,
                   RECORDS_UPDATED, RECORDS_QUARANTINED, RETRY_COUNT,
                   METRIC_DETAILS:dbt_models::NUMBER AS DBT_MODELS,
                   METRIC_DETAILS:dbt_tests::NUMBER AS DBT_TESTS
            FROM {self.config.database}.{CONTROL_SCHEMA}.PIPELINE_RUNS
            WHERE {where_clause}
            ORDER BY STARTED_AT DESC, PIPELINE_NAME, RUN_ID
            LIMIT %(limit)s
        """
        return self._fetch_all(query, params)

    def _fetch_all(
        self,
        query: str,
        params: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        with closing(self._connect()) as connection:
            with closing(connection.cursor(DictCursor)) as cursor:
                cursor.execute(query, params)
                return [_lowercase_keys(row) for row in cursor.fetchall()]
