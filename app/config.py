"""Validated, non-secret configuration for the DeFi Data Lab."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping as MappingABC
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from dotenv import load_dotenv

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_READ_ONLY_ROLE = "BLOCKCHAIN_ANALYST_ROLE"
_SERVICE_USER = "BLOCKCHAIN_APP_USER"
_MARTS_SCHEMA = "MARTS"
_LOCAL_ENVIRONMENT = "local"
_HOSTED_ENVIRONMENT = "hosted"
_VALID_ENVIRONMENTS = frozenset({_LOCAL_ENVIRONMENT, _HOSTED_ENVIRONMENT})
_PROJECT_ROOT = Path(__file__).resolve().parents[1]

load_dotenv(_PROJECT_ROOT / ".env")


def _identifier(value: str, setting: str) -> str:
    normalized = value.strip().upper()
    if not _IDENTIFIER_PATTERN.fullmatch(normalized):
        raise ValueError(f"{setting} must be a valid Snowflake identifier")
    return normalized


def _environment(values: Mapping[str, str]) -> str:
    environment = values.get("DATA_LAB_ENV", _LOCAL_ENVIRONMENT).strip().lower()
    if environment not in _VALID_ENVIRONMENTS:
        raise ValueError("DATA_LAB_ENV must be local or hosted")
    return environment


def _hosted_secret(section: Mapping[str, Any], setting: str) -> str:
    value = section.get(setting)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"Missing required hosted Snowflake setting: snowflake.{setting}"
        )
    return value.strip()


@dataclass(frozen=True)
class AppConfig:
    """Connection and object names for read-only tutorial queries."""

    environment: str = _LOCAL_ENVIRONMENT
    connection_name: str | None = "blockchain-dev"
    account: str | None = None
    user: str | None = None
    role: str = _READ_ONLY_ROLE
    warehouse: str = "BLOCKCHAIN_TRANSFORM_WH"
    database: str = "BLOCKCHAIN_ANALYTICS"
    schema: str = _MARTS_SCHEMA
    private_key_pem: str | None = field(default=None, repr=False)
    private_key_passphrase: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        environment = self.environment.strip().lower()
        if environment not in _VALID_ENVIRONMENTS:
            raise ValueError("DATA_LAB_ENV must be local or hosted")

        if environment == _LOCAL_ENVIRONMENT:
            if self.connection_name is None or not self.connection_name.strip():
                raise ValueError(
                    "SNOWFLAKE_APP_CONNECTION_NAME must not be empty"
                )
        else:
            if self.user is None or self.user.upper() != _SERVICE_USER:
                raise ValueError(
                    "Hosted Snowflake user must be BLOCKCHAIN_APP_USER"
                )
            for name, value in (
                ("account", self.account),
                ("private_key", self.private_key_pem),
                ("private_key_passphrase", self.private_key_passphrase),
            ):
                if value is None or not value.strip():
                    raise ValueError(
                        f"Missing required hosted Snowflake setting: snowflake.{name}"
                    )

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
        """Load local settings without requiring a local .env file."""
        values = os.environ if environ is None else environ
        environment = _environment(values)
        if environment == _HOSTED_ENVIRONMENT:
            raise ValueError(
                "Hosted mode requires Streamlit Secrets; use from_runtime() "
                "or from_sources()"
            )
        return cls(
            environment=environment,
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

    @classmethod
    def from_sources(
        cls,
        environ: Mapping[str, str],
        secrets: Mapping[str, Any] | None,
    ) -> AppConfig:
        """Load an explicit local or hosted configuration source."""
        environment = _environment(environ)
        if environment == _LOCAL_ENVIRONMENT:
            return cls.from_env(environ)
        if secrets is None:
            raise ValueError("Hosted mode requires a [snowflake] secrets section")
        section = secrets.get("snowflake")
        if not isinstance(section, MappingABC):
            raise ValueError("Hosted mode requires a [snowflake] secrets section")
        return cls(
            environment=environment,
            connection_name=None,
            account=_hosted_secret(section, "account"),
            user=_hosted_secret(section, "user"),
            role=_hosted_secret(section, "role"),
            warehouse=_hosted_secret(section, "warehouse"),
            database=_hosted_secret(section, "database"),
            schema=_hosted_secret(section, "schema"),
            private_key_pem=_hosted_secret(section, "private_key"),
            private_key_passphrase=_hosted_secret(
                section, "private_key_passphrase"
            ),
        )

    @classmethod
    def from_runtime(
        cls,
        environ: Mapping[str, str] | None = None,
        secrets: Mapping[str, Any] | None = None,
    ) -> AppConfig:
        """Load local environment values or hosted Streamlit Secrets."""
        values = dict(os.environ if environ is None else environ)
        secret_environment: Any = None
        if secrets is None:
            try:
                import streamlit as st

                secrets = st.secrets
                secret_environment = secrets.get("DATA_LAB_ENV")
            except Exception:
                secrets = None
        else:
            secret_environment = secrets.get("DATA_LAB_ENV")

        if "DATA_LAB_ENV" not in values and secrets is not None:
            if isinstance(secret_environment, str):
                values["DATA_LAB_ENV"] = secret_environment

        environment = _environment(values)
        if environment == _LOCAL_ENVIRONMENT:
            return cls.from_env(values)
        return cls.from_sources(values, secrets)

    @property
    def is_hosted(self) -> bool:
        """Return whether service-user authentication is required."""
        return self.environment.lower() == _HOSTED_ENVIRONMENT

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

    @property
    def chainlink_oracle_mart(self) -> str:
        """Return the validated fully qualified Chainlink tutorial mart."""
        return (
            f"{self.database.upper()}.{self.schema.upper()}."
            "MART_CHAINLINK_ORACLE_TUTORIAL"
        )

    @property
    def point_in_time_pricing_mart(self) -> str:
        """Return the validated fully qualified PIT tutorial mart."""
        return (
            f"{self.database.upper()}.{self.schema.upper()}."
            "MART_POINT_IN_TIME_PRICING_TUTORIAL"
        )
