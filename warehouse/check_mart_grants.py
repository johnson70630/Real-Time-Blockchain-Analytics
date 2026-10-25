"""Read-only validation of the DeFi Data Lab Snowflake access contract."""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable, Mapping
from contextlib import closing
from dataclasses import dataclass
from typing import Any, Protocol

import snowflake.connector
from snowflake.connector import DictCursor

DATABASE = "BLOCKCHAIN_ANALYTICS"
SCHEMA = "MARTS"
ANALYST_ROLE = "BLOCKCHAIN_ANALYST_ROLE"
DBT_ROLE = "BLOCKCHAIN_DBT_ROLE"
TUTORIAL_MARTS = (
    "MART_UNISWAP_SWAP_TUTORIAL",
    "MART_AAVE_LENDING_TUTORIAL",
    "MART_CHAINLINK_ORACLE_TUTORIAL",
    "MART_POINT_IN_TIME_PRICING_TUTORIAL",
)
EXPECTED_RELATIONS = frozenset(
    f"{DATABASE}.{SCHEMA}.{table}" for table in TUTORIAL_MARTS
)


class Cursor(Protocol):
    def execute(self, query: str) -> Cursor: ...

    def fetchone(self) -> Mapping[str, Any] | None: ...

    def fetchall(self) -> list[Mapping[str, Any]]: ...

    def close(self) -> None: ...


class Connection(Protocol):
    def cursor(self, cursor_class: Any = None) -> Cursor: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class MartGrantStatus:
    """Read-only object and grant state for one approved tutorial MART."""

    relation: str
    object_type: str
    row_count: int
    analyst_select: bool


@dataclass(frozen=True)
class GrantBaseline:
    """Validated identity and least-privilege MART grant baseline."""

    current_user: str
    current_role: str
    secondary_roles_disabled: bool
    marts: tuple[MartGrantStatus, ...]
    unexpected_select_grants: int
    future_select_grants: int


def _normalized(row: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key).lower(): value for key, value in row.items()}


def _secondary_roles_disabled(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.upper() in {"", "NONE"}:
            return True
        try:
            value = json.loads(stripped)
        except json.JSONDecodeError:
            return False
    if isinstance(value, Mapping):
        return not value.get("roles") and not value.get("value")
    return False


def _select_relations(rows: Iterable[Mapping[str, Any]]) -> frozenset[str]:
    relations: set[str] = set()
    for raw_row in rows:
        row = _normalized(raw_row)
        if (
            str(row.get("privilege", "")).upper() == "SELECT"
            and str(row.get("granted_on", "")).upper() == "TABLE"
        ):
            relations.add(str(row.get("name", "")).upper())
    return frozenset(relations)


def validate_select_contract(
    role_grants: Iterable[Mapping[str, Any]],
    future_grants: Iterable[Mapping[str, Any]],
) -> tuple[int, int]:
    """Require exactly the four approved table SELECT grants and no future grant."""
    selected_relations = _select_relations(role_grants)
    missing = EXPECTED_RELATIONS - selected_relations
    unexpected = selected_relations - EXPECTED_RELATIONS
    future_select_count = sum(
        str(_normalized(row).get("privilege", "")).upper() == "SELECT"
        for row in future_grants
    )

    if missing:
        raise RuntimeError(
            f"Missing analyst SELECT grants on {len(missing)} tutorial MART(s)"
        )
    if unexpected:
        raise RuntimeError(
            f"Unexpected analyst SELECT grants found on {len(unexpected)} table(s)"
        )
    if future_select_count:
        raise RuntimeError("Unexpected future-table SELECT grant found")
    return len(unexpected), future_select_count


def collect_grant_baseline(connection: Connection) -> GrantBaseline:
    """Collect and validate the access contract without mutating Snowflake data."""
    with closing(connection.cursor(DictCursor)) as cursor:
        cursor.execute("USE SECONDARY ROLES NONE")
        identity = cursor.execute(
            "SELECT CURRENT_USER() AS CURRENT_USER, "
            "CURRENT_ROLE() AS CURRENT_ROLE, "
            "CURRENT_SECONDARY_ROLES() AS SECONDARY_ROLES"
        ).fetchone()
        if identity is None:
            raise RuntimeError("Snowflake session identity was unavailable")
        identity = _normalized(identity)
        current_role = str(identity["current_role"]).upper()
        secondary_roles_disabled = _secondary_roles_disabled(
            identity["secondary_roles"]
        )
        if current_role != DBT_ROLE or not secondary_roles_disabled:
            raise RuntimeError("Snowflake grant-check session identity is unsafe")

        marts: list[MartGrantStatus] = []
        for table in TUTORIAL_MARTS:
            relation = f"{DATABASE}.{SCHEMA}.{table}"
            metadata = cursor.execute(
                "SELECT TABLE_TYPE, ROW_COUNT "
                f"FROM {DATABASE}.INFORMATION_SCHEMA.TABLES "
                f"WHERE TABLE_SCHEMA = '{SCHEMA}' AND TABLE_NAME = '{table}'"
            ).fetchone()
            if metadata is None:
                raise RuntimeError(f"Tutorial MART is missing: {table}")
            metadata = _normalized(metadata)
            exact_count = cursor.execute(
                f"SELECT COUNT(*) AS ROW_COUNT FROM {relation}"
            ).fetchone()
            if exact_count is None:
                raise RuntimeError(f"Tutorial MART row count is unavailable: {table}")
            cursor.execute(f"SHOW GRANTS ON TABLE {relation}")
            object_grants = tuple(_normalized(row) for row in cursor.fetchall())
            analyst_select = any(
                str(row.get("privilege", "")).upper() == "SELECT"
                and str(row.get("grantee_name", "")).upper() == ANALYST_ROLE
                for row in object_grants
            )
            marts.append(
                MartGrantStatus(
                    relation=relation,
                    object_type=str(metadata["table_type"]).upper(),
                    row_count=int(_normalized(exact_count)["row_count"]),
                    analyst_select=analyst_select,
                )
            )

        cursor.execute(f"SHOW GRANTS TO ROLE {ANALYST_ROLE}")
        role_grants = cursor.fetchall()
        cursor.execute(f"SHOW FUTURE GRANTS TO ROLE {ANALYST_ROLE}")
        future_grants = cursor.fetchall()
        unexpected_count, future_count = validate_select_contract(
            role_grants, future_grants
        )

    if not all(mart.analyst_select for mart in marts):
        raise RuntimeError("Analyst SELECT is missing from a tutorial MART")

    return GrantBaseline(
        current_user=str(identity["current_user"]).upper(),
        current_role=current_role,
        secondary_roles_disabled=secondary_roles_disabled,
        marts=tuple(marts),
        unexpected_select_grants=unexpected_count,
        future_select_grants=future_count,
    )


def main() -> None:
    """Validate and print only the approved, non-sensitive access baseline."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--connection", default="blockchain-dev")
    arguments = parser.parse_args()
    connection = snowflake.connector.connect(
        connection_name=arguments.connection,
        role=DBT_ROLE,
        warehouse="BLOCKCHAIN_TRANSFORM_WH",
        database=DATABASE,
        schema=SCHEMA,
    )
    try:
        baseline = collect_grant_baseline(connection)
    finally:
        connection.close()

    secondary_roles = "NONE" if baseline.secondary_roles_disabled else "ENABLED"
    print(
        f"IDENTITY user={baseline.current_user} role={baseline.current_role} "
        f"secondary_roles={secondary_roles}"
    )
    for mart in baseline.marts:
        print(
            f"MART relation={mart.relation} type={mart.object_type} "
            f"rows={mart.row_count} analyst_select={str(mart.analyst_select).lower()}"
        )
    print(f"UNEXPECTED_SELECT_GRANTS={baseline.unexpected_select_grants}")
    print(f"FUTURE_SELECT_GRANTS={baseline.future_select_grants}")


if __name__ == "__main__":
    main()
