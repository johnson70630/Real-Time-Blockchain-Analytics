from pathlib import Path

import duckdb

from producer.protocols.evm import first_log_topic, hex_int, hex_string
from spark.parquet import discover_parquet_files, write_relation_atomic


def test_evm_rpc_values_are_normalized() -> None:
    assert hex_string(bytes.fromhex("abcd")) == "0xabcd"
    assert hex_int("0x2a") == 42
    assert first_log_topic({"topics": [bytes.fromhex("abcd")]}) == "0xabcd"
    assert first_log_topic({"topics": []}) is None
    assert first_log_topic({"topics": [object()]}) is None


def test_discover_parquet_files_is_sorted_and_ignores_hidden_files(
    tmp_path: Path,
) -> None:
    nested = tmp_path / "protocol=aave_v3"
    nested.mkdir()
    second = nested / "part-00002.parquet"
    first = nested / "part-00001.parquet"
    hidden = nested / ".temporary.parquet"
    ignored = nested / "notes.txt"
    for path in (second, first, hidden, ignored):
        path.touch()

    assert discover_parquet_files(tmp_path) == [first, second]


def test_write_relation_atomic_replaces_output_and_returns_row_count(
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "dataset.parquet"
    connection = duckdb.connect()

    try:
        row_count = write_relation_atomic(
            connection,
            connection.sql("SELECT * FROM VALUES (1), (2) AS rows(value)"),
            output_path,
        )
        replacement_count = write_relation_atomic(
            connection,
            connection.sql("SELECT 3 AS value"),
            output_path,
        )

        values = connection.read_parquet(str(output_path)).fetchall()
    finally:
        connection.close()

    assert row_count == 2
    assert replacement_count == 1
    assert values == [(3,)]
    assert list(tmp_path.glob("*.tmp.parquet")) == []
