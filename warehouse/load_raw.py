"""Execute the existing Snowflake RAW landing SQL with key-pair authentication."""

from __future__ import annotations

import argparse
import logging
import os
import re
from collections.abc import Iterable, Mapping, Sequence
from contextlib import closing
from dataclasses import dataclass
from typing import Any

import snowflake.connector
from snowflake.connector.config_manager import CONFIG_MANAGER

from warehouse.snowflake import (
    WAREHOUSE_DATASETS,
    SnowflakeLandingConfig,
    WarehouseDataset,
    generate_dataset_load_sql,
    validate_dataset_paths,
)

LOGGER = logging.getLogger(__name__)
PIPELINE_ROLE = "BLOCKCHAIN_PIPELINE_ROLE"
DEFAULT_CONNECTION_NAME = "blockchain-dev"
DEFAULT_WAREHOUSE = "BLOCKCHAIN_TRANSFORM_WH"
KEY_PAIR_AUTHENTICATOR = "SNOWFLAKE_JWT"


@dataclass(frozen=True)
class LoadSummary:
    """Non-secret execution metrics for one canonical RAW dataset."""

    dataset: str
    copy_files_processed: int
    copy_rows_loaded: int
    merge_inserted: int
    merge_updated: int
    final_rows: int
    duplicate_keys: int


def _connection_definition(connection_name: str) -> Mapping[str, Any]:
    """Return one named connector definition without logging its contents."""
    connections = CONFIG_MANAGER["connections"]
    if connection_name not in connections:
        raise ValueError(
            f"Snowflake connection {connection_name!r} is not configured"
        )
    return connections[connection_name]


def validate_key_pair_connection_config(connection_name: str) -> None:
    """Reject password or non-key-pair Snowflake connection definitions."""
    definition = _connection_definition(connection_name)
    authenticator = str(definition.get("authenticator", "")).upper()
    has_private_key = bool(
        definition.get("private_key_file") or definition.get("private_key")
    )
    if authenticator != KEY_PAIR_AUTHENTICATOR or not has_private_key:
        raise ValueError(
            "Snowflake RAW loading requires SNOWFLAKE_JWT key-pair "
            "authentication"
        )
    if definition.get("password"):
        raise ValueError("Password authentication is not allowed for RAW loading")


def connect_pipeline(
    connection_name: str,
    warehouse: str,
):
    """Connect with the least-privilege pipeline role and disable secondary roles."""
    validate_key_pair_connection_config(connection_name)
    connection = snowflake.connector.connect(
        connection_name=connection_name,
        role=PIPELINE_ROLE,
        warehouse=warehouse,
        application="BLOCKCHAIN_AIRFLOW_RAW_LOADER",
    )
    try:
        with closing(connection.cursor()) as cursor:
            cursor.execute("USE SECONDARY ROLES NONE")
            cursor.execute("SELECT CURRENT_ROLE()")
            current_role = cursor.fetchone()[0]
        if current_role.upper() != PIPELINE_ROLE:
            raise RuntimeError(
                f"Expected Snowflake role {PIPELINE_ROLE}, found {current_role}"
            )
    except Exception:
        connection.close()
        raise
    return connection


def _result_dicts(cursor: Any) -> list[dict[str, Any]]:
    """Return cursor rows keyed by normalized Snowflake result-column names."""
    if not cursor.description:
        return []
    names = [str(column[0]).strip().lower().replace(" ", "_") for column in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def _metric(rows: Sequence[Mapping[str, Any]], *names: str) -> int:
    for row in rows:
        for name in names:
            if name in row and row[name] is not None:
                return int(row[name])
    return 0


def _copy_metrics(rows: Sequence[Mapping[str, Any]]) -> tuple[int, int]:
    if len(rows) == 1 and set(rows[0]) == {"status"}:
        match = re.search(
            r"([0-9]+) files? processed",
            str(rows[0]["status"]),
            flags=re.IGNORECASE,
        )
        return (int(match.group(1)) if match else 0), 0
    file_rows = [row for row in rows if "file" in row or "status" in row]
    files_processed = sum(
        1
        for row in file_rows
        if str(row.get("status", "")).upper() not in {"", "SKIPPED"}
    )
    rows_loaded = sum(int(row.get("rows_loaded") or 0) for row in file_rows)
    return files_processed, rows_loaded


def _qualified(config: SnowflakeLandingConfig, table: str) -> str:
    return f"{config.database}.{config.schema}.{table}"


def _final_metrics(
    connection: Any,
    config: SnowflakeLandingConfig,
    dataset: WarehouseDataset,
) -> tuple[int, int]:
    table = _qualified(config, dataset.table)
    keys = ", ".join(dataset.natural_key)
    sql = (
        "SELECT COUNT(*) AS final_rows, "
        "COUNT(*) - COUNT(DISTINCT CONCAT_WS('|', "
        + keys
        + f")) AS duplicate_keys FROM {table}"
    )
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql)
        row = _result_dicts(cursor)[0]
    return int(row["final_rows"]), int(row["duplicate_keys"])


def execute_dataset_load(
    connection: Any,
    config: SnowflakeLandingConfig,
    dataset: WarehouseDataset,
) -> LoadSummary:
    """Execute generated COPY/MERGE SQL and return safe load metrics."""
    cursors = connection.execute_string(generate_dataset_load_sql(config, dataset))
    results = [_result_dicts(cursor) for cursor in cursors]
    copy_files, copy_rows = _copy_metrics(results[0] if results else [])
    merge_rows = results[2] if len(results) > 2 else []
    inserted = _metric(merge_rows, "number_of_rows_inserted", "rows_inserted")
    updated = _metric(merge_rows, "number_of_rows_updated", "rows_updated")
    final_rows, duplicate_keys = _final_metrics(connection, config, dataset)
    return LoadSummary(
        dataset=dataset.dataset,
        copy_files_processed=copy_files,
        copy_rows_loaded=copy_rows,
        merge_inserted=inserted,
        merge_updated=updated,
        final_rows=final_rows,
        duplicate_keys=duplicate_keys,
    )


def validate_stage_inputs(
    connection: Any,
    config: SnowflakeLandingConfig,
    datasets: Iterable[WarehouseDataset],
) -> dict[str, int]:
    """Check that each canonical stage prefix contains Parquet input files."""
    stage = _qualified(config, config.stage)
    counts: dict[str, int] = {}
    for dataset in datasets:
        with closing(connection.cursor()) as cursor:
            cursor.execute(
                f"LIST @{stage}/{dataset.stage_path} PATTERN='.*[.]parquet$'"
            )
            counts[dataset.dataset] = len(cursor.fetchall())
        if counts[dataset.dataset] == 0:
            raise RuntimeError(
                f"No Parquet inputs found for {dataset.dataset} at its "
                "configured Snowflake stage prefix"
            )
    return counts


def validate_target_tables(
    connection: Any,
    config: SnowflakeLandingConfig,
    datasets: Iterable[WarehouseDataset],
) -> None:
    """Fail before loading when provisioned RAW or landing tables are absent."""
    missing: list[str] = []
    for dataset in datasets:
        for table in (dataset.table, f"{dataset.table}_LANDING"):
            try:
                with closing(connection.cursor()) as cursor:
                    cursor.execute(f"DESC TABLE {_qualified(config, table)}")
            except snowflake.connector.Error:
                missing.append(table)
    if missing:
        names = ", ".join(sorted(missing))
        raise RuntimeError(
            "Required pre-provisioned Snowflake tables are unavailable to "
            f"{PIPELINE_ROLE}: {names}"
        )


def _selected_datasets(names: Sequence[str]) -> tuple[WarehouseDataset, ...]:
    selected = set(names)
    return tuple(
        dataset
        for dataset in WAREHOUSE_DATASETS
        if not selected or dataset.dataset in selected
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        action="append",
        choices=tuple(dataset.dataset for dataset in WAREHOUSE_DATASETS),
        default=[],
        help="load or validate one dataset; may be repeated",
    )
    parser.add_argument(
        "--validate-inputs",
        action="store_true",
        help="list canonical stage prefixes without loading data",
    )
    parser.add_argument(
        "--connection-name",
        default=os.getenv("SNOWFLAKE_CONNECTION_NAME", DEFAULT_CONNECTION_NAME),
    )
    parser.add_argument(
        "--warehouse",
        default=os.getenv("SNOWFLAKE_WAREHOUSE", DEFAULT_WAREHOUSE),
    )
    return parser.parse_args()


def main() -> None:
    """Run selected RAW loads or lightweight stage-input validation."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _parse_args()
    validate_dataset_paths()
    datasets = _selected_datasets(args.dataset)
    config = SnowflakeLandingConfig.from_env()
    with closing(connect_pipeline(args.connection_name, args.warehouse)) as connection:
        if args.validate_inputs:
            validate_target_tables(connection, config, datasets)
            counts = validate_stage_inputs(connection, config, datasets)
            for dataset, file_count in counts.items():
                LOGGER.info(
                    "Validated dataset=%s parquet_files=%s",
                    dataset,
                    file_count,
                )
            return

        for dataset in datasets:
            summary = execute_dataset_load(connection, config, dataset)
            LOGGER.info(
                "Loaded dataset=%s copy_files=%s copy_rows=%s inserted=%s "
                "updated=%s final_rows=%s duplicate_keys=%s",
                summary.dataset,
                summary.copy_files_processed,
                summary.copy_rows_loaded,
                summary.merge_inserted,
                summary.merge_updated,
                summary.final_rows,
                summary.duplicate_keys,
            )
            if summary.duplicate_keys:
                raise RuntimeError(
                    f"Natural-key duplicates found in {summary.dataset}"
                )


if __name__ == "__main__":
    main()
