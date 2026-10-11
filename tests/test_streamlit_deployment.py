"""Focused tests for local and hosted DeFi Data Lab authentication."""

from __future__ import annotations

import importlib.util
import runpy
from typing import Any

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

from app.config import AppConfig
from app.data.snowflake import (
    SnowflakeTutorialRepository,
    _connection_parameters,
    load_hosted_private_key,
)

_HOSTED_ENV = {"DATA_LAB_ENV": "hosted"}
_PASSPHRASE = "test-passphrase-not-a-real-secret"


@pytest.fixture(scope="module")
def encrypted_private_key() -> str:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(
            _PASSPHRASE.encode("utf-8")
        ),
    ).decode("utf-8")


def hosted_secrets(encrypted_private_key: str) -> dict[str, Any]:
    return {
        "snowflake": {
            "account": "test-account",
            "user": "BLOCKCHAIN_APP_USER",
            "role": "BLOCKCHAIN_ANALYST_ROLE",
            "warehouse": "BLOCKCHAIN_TRANSFORM_WH",
            "database": "BLOCKCHAIN_ANALYTICS",
            "schema": "MARTS",
            "private_key": encrypted_private_key,
            "private_key_passphrase": _PASSPHRASE,
        }
    }


def test_local_configuration_preserves_named_connection_behavior() -> None:
    config = AppConfig.from_sources(
        {
            "DATA_LAB_ENV": "local",
            "SNOWFLAKE_APP_CONNECTION_NAME": "blockchain-dev",
        },
        secrets=None,
    )

    assert not config.is_hosted
    assert _connection_parameters(config) == {
        "connection_name": "blockchain-dev",
        "role": "BLOCKCHAIN_ANALYST_ROLE",
        "warehouse": "BLOCKCHAIN_TRANSFORM_WH",
        "database": "BLOCKCHAIN_ANALYTICS",
        "schema": "MARTS",
    }


def test_hosted_configuration_loads_service_user_secrets(
    encrypted_private_key: str,
) -> None:
    config = AppConfig.from_sources(
        _HOSTED_ENV, hosted_secrets(encrypted_private_key)
    )

    assert config.is_hosted
    assert config.connection_name is None
    assert config.account == "test-account"
    assert config.user == "BLOCKCHAIN_APP_USER"
    assert config.role == "BLOCKCHAIN_ANALYST_ROLE"
    assert encrypted_private_key not in repr(config)
    assert _PASSPHRASE not in repr(config)


def test_runtime_detects_explicit_hosted_mode_from_root_secret(
    encrypted_private_key: str,
) -> None:
    secrets = hosted_secrets(encrypted_private_key)
    secrets["DATA_LAB_ENV"] = "hosted"

    config = AppConfig.from_runtime({}, secrets)

    assert config.is_hosted
    assert config.user == "BLOCKCHAIN_APP_USER"


@pytest.mark.parametrize(
    "missing_setting",
    [
        "account",
        "user",
        "role",
        "warehouse",
        "database",
        "schema",
        "private_key",
        "private_key_passphrase",
    ],
)
def test_hosted_configuration_rejects_missing_secrets(
    encrypted_private_key: str, missing_setting: str
) -> None:
    secrets = hosted_secrets(encrypted_private_key)
    del secrets["snowflake"][missing_setting]

    with pytest.raises(ValueError, match=f"snowflake.{missing_setting}"):
        AppConfig.from_sources(_HOSTED_ENV, secrets)


def test_hosted_configuration_requires_explicit_secrets_section() -> None:
    with pytest.raises(ValueError, match=r"\[snowflake\]"):
        AppConfig.from_sources(_HOSTED_ENV, None)


def test_encrypted_private_key_is_decrypted_in_memory(
    encrypted_private_key: str,
) -> None:
    key = load_hosted_private_key(encrypted_private_key, _PASSPHRASE)

    assert isinstance(key, RSAPrivateKey)


def test_private_key_errors_do_not_expose_secret_values() -> None:
    private_material = "sensitive-key-material"
    passphrase = "sensitive-passphrase"

    with pytest.raises(ValueError) as error:
        load_hosted_private_key(private_material, passphrase)

    message = str(error.value)
    assert private_material not in message
    assert passphrase not in message


class HostedCursor:
    def __init__(self, identity: tuple[str, str, str]) -> None:
        self.identity = identity
        self.executions: list[str] = []
        self.closed = False

    def execute(self, query: str, params: Any = None) -> HostedCursor:
        self.executions.append(query)
        return self

    def fetchone(self) -> tuple[str, str, str]:
        return self.identity

    def close(self) -> None:
        self.closed = True


class HostedConnection:
    def __init__(self, cursor: HostedCursor) -> None:
        self.hosted_cursor = cursor
        self.closed = False

    def cursor(self, cursor_class: Any = None) -> HostedCursor:
        return self.hosted_cursor

    def close(self) -> None:
        self.closed = True


def test_hosted_connection_uses_jwt_and_validates_secure_session(
    encrypted_private_key: str,
) -> None:
    config = AppConfig.from_sources(
        _HOSTED_ENV, hosted_secrets(encrypted_private_key)
    )
    cursor = HostedCursor(
        (
            "BLOCKCHAIN_APP_USER",
            "BLOCKCHAIN_ANALYST_ROLE",
            '{"roles":"","value":""}',
        )
    )
    connection = HostedConnection(cursor)
    calls: list[dict[str, Any]] = []

    def factory(**kwargs: Any) -> HostedConnection:
        calls.append(kwargs)
        return connection

    result = SnowflakeTutorialRepository(config, factory)._connect()

    assert result is connection
    assert calls[0]["authenticator"] == "SNOWFLAKE_JWT"
    assert isinstance(calls[0]["private_key"], RSAPrivateKey)
    assert "private_key_passphrase" not in calls[0]
    assert cursor.executions == [
        "USE SECONDARY ROLES NONE",
        "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_SECONDARY_ROLES()",
    ]


@pytest.mark.parametrize(
    "identity",
    [
        (
            "UNEXPECTED_USER",
            "BLOCKCHAIN_ANALYST_ROLE",
            '{"roles":"","value":""}',
        ),
        (
            "BLOCKCHAIN_APP_USER",
            "ACCOUNTADMIN",
            '{"roles":"","value":""}',
        ),
        (
            "BLOCKCHAIN_APP_USER",
            "BLOCKCHAIN_ANALYST_ROLE",
            '{"roles":"ACCOUNTADMIN","value":"ALL"}',
        ),
    ],
)
def test_hosted_connection_rejects_unsafe_session_identity(
    encrypted_private_key: str,
    identity: tuple[str, str, str],
) -> None:
    config = AppConfig.from_sources(
        _HOSTED_ENV, hosted_secrets(encrypted_private_key)
    )
    cursor = HostedCursor(identity)
    connection = HostedConnection(cursor)

    with pytest.raises(RuntimeError, match="identity validation failed"):
        SnowflakeTutorialRepository(
            config, lambda **kwargs: connection
        )._connect()

    assert connection.closed


def test_application_imports_without_loading_hosted_secrets() -> None:
    from app.app import main
    from streamlit_app import main as entrypoint_main

    assert callable(main)
    assert entrypoint_main is main


def test_app_resolves_as_package_from_repository_root() -> None:
    spec = importlib.util.find_spec("app")

    assert spec is not None
    assert spec.submodule_search_locations is not None
    assert spec.origin is not None and spec.origin.endswith("app/__init__.py")


def test_root_streamlit_entrypoint_invokes_application_main(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.app

    calls: list[bool] = []
    monkeypatch.setattr(app.app, "main", lambda: calls.append(True))

    runpy.run_path("streamlit_app.py", run_name="__main__")

    assert calls == [True]
