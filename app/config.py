"""Validated, non-secret configuration for the DeFi Data Lab."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from dotenv import load_dotenv

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_READ_ONLY_ROLE = "BLOCKCHAIN_ANALYST_ROLE"
_MARTS_SCHEMA = "MARTS"
_PROJECT_ROOT = Path(__file__).resolve().parents[1]

load_dotenv(_PROJECT_ROOT / ".env")


def _identifier(value: str, setting: str) -> str:
    normalized = value.strip().upper()
    if not _IDENTIFIER_PATTERN.fullmatch(normalized):
        raise ValueError(f"{setting} must be a valid Snowflake identifier")
    return normalized


@dataclass(frozen=True)
class AppConfig:
    """Connection and object names for read-only tutorial queries."""

    connection_name: str = "blockchain-dev"
    role: str = _READ_ONLY_ROLE
    warehouse: str = "BLOCKCHAIN_TRANSFORM_WH"
    database: str = "BLOCKCHAIN_ANALYTICS"
    schema: str = _MARTS_SCHEMA

    def __post_init__(self) -> None:
        if not self.connection_name.strip():
            raise ValueError("SNOWFLAKE_APP_CONNECTION_NAME must not be empty")

        for setting, value in (
            ("SNOWFLAKE_APP_ROLE", self.role),
            ("SNOWFLAKE_APP_WAREHOUSE", self.warehouse),
            ("SNOWFLAKE_APP_DATABASE", self.database),
            ("SNOWFLAKE_APP_SCHEMA", self.schema),
        ):
            _identifier(value, setting)

        if self.role.upper() != _READ_ONLY_ROLE:
            raise ValueError(
                "SNOWFLAKE_APP_ROLE must be BLOCKCHAIN_ANALYST_ROLE"
            )
        if self.schema.upper() != _MARTS_SCHEMA:
            raise ValueError("SNOWFLAKE_APP_SCHEMA must be MARTS")

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> AppConfig:
        """Load non-secret settings without requiring a local .env file."""
        values = os.environ if environ is None else environ
        return cls(
            connection_name=values.get(
                "SNOWFLAKE_APP_CONNECTION_NAME", "blockchain-dev"
            ),
            role=values.get("SNOWFLAKE_APP_ROLE", _READ_ONLY_ROLE),
            warehouse=values.get(
                "SNOWFLAKE_APP_WAREHOUSE", "BLOCKCHAIN_TRANSFORM_WH"
            ),
            database=values.get(
                "SNOWFLAKE_APP_DATABASE", "BLOCKCHAIN_ANALYTICS"
            ),
            schema=values.get("SNOWFLAKE_APP_SCHEMA", _MARTS_SCHEMA),
        )

    @property
    def tutorial_mart(self) -> str:
        """Return the validated fully qualified Uniswap tutorial mart."""
        return (
            f"{self.database.upper()}.{self.schema.upper()}."
            "MART_UNISWAP_SWAP_TUTORIAL"
        )

    @property
    def aave_lending_mart(self) -> str:
        """Return the validated fully qualified Aave tutorial mart."""
        return (
            f"{self.database.upper()}.{self.schema.upper()}."
            "MART_AAVE_LENDING_TUTORIAL"
        )
