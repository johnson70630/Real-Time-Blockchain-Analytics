import json
from datetime import date
from pathlib import Path

from market_data.models import MarketDataObservation
from market_data.validation import validate_observation_message
from spark.bronze import (
    append_partitioned_parquet,
    hive_partition_path,
)
from spark.market_data_schema import get_market_data_observation_schema
from spark.write_market_data_bronze import validation_error_values

FEED_ADDRESS = "0x" + "10" * 20


def _observation() -> dict:
    return MarketDataObservation.create(
        chain="arbitrum",
        feed_address=FEED_ADDRESS,
        base_asset="ETH",
        quote_asset="USD",
        round_id=2**79 + 123,
        answer_raw=2**200 + 456,
        feed_decimals=8,
        feed_updated_at="2026-07-23T12:00:00+00:00",
        observed_at="2026-07-23T12:00:01+00:00",
        block_number=123456,
        block_timestamp="2026-07-23T12:00:00+00:00",
    ).to_dict()


def test_market_data_schema_preserves_exact_observation_fields() -> None:
    schema = get_market_data_observation_schema()
    fields = {field.name: field.dataType.simpleString() for field in schema}

    assert fields["round_id"] == "string"
    assert fields["answer_raw"] == "string"
    assert fields["feed_decimals"] == "int"
    assert fields["block_number"] == "bigint"
    assert fields["feed_updated_at"] == "timestamp"
    assert "transaction_hash" not in fields
    assert "log_index" not in fields


def test_mocked_kafka_value_parses_without_precision_loss() -> None:
    message = _observation()
    raw_value = json.dumps(message)

    parsed, errors = validate_observation_message(raw_value)

    assert errors == ()
    assert parsed is not None
    assert parsed["round_id"] == message["round_id"]
    assert parsed["answer_raw"] == message["answer_raw"]
    assert isinstance(parsed["round_id"], str)
    assert isinstance(parsed["answer_raw"], str)


def test_incomplete_and_malformed_messages_receive_quarantine_reasons() -> None:
    incomplete = _observation()
    incomplete["feed_address"] = ""
    incomplete.pop("answer_raw")

    parsed, errors = validate_observation_message(json.dumps(incomplete))
    invalid_json, invalid_json_errors = validate_observation_message("{bad")

    assert parsed is not None
    assert "missing_or_empty:feed_address" in errors
    assert "missing_or_empty:answer_raw" in errors
    assert "invalid:observation_id" in errors
    assert invalid_json is None
    assert invalid_json_errors == ("invalid_json",)


def test_spark_validation_adapter_reuses_canonical_message_rules() -> None:
    valid = json.dumps(_observation())
    invalid = _observation()
    invalid["round_id"] = "not-an-integer"

    assert validation_error_values(valid) == []
    assert "invalid:round_id" in validation_error_values(json.dumps(invalid))


def test_partition_generation_uses_chain_and_observation_date(
    tmp_path: Path,
) -> None:
    partition = hive_partition_path(
        tmp_path / "bronze" / "market_data",
        (
            ("chain", "arbitrum"),
            ("observation_date", date(2026, 7, 23)),
        ),
    )

    assert partition == (
        tmp_path
        / "bronze"
        / "market_data"
        / "chain=arbitrum"
        / "observation_date=2026-07-23"
    )


class FakeWriter:
    def __init__(self) -> None:
        self.mode_name = None
        self.partition_columns = None
        self.output_path = None

    def mode(self, name: str):
        self.mode_name = name
        return self

    def partitionBy(self, *columns: str):
        self.partition_columns = columns
        return self

    def parquet(self, output_path: str) -> None:
        self.output_path = output_path


class FakeFrame:
    def __init__(self) -> None:
        self.write = FakeWriter()


def test_bronze_writer_appends_without_deduplication(tmp_path: Path) -> None:
    frame = FakeFrame()
    output_path = tmp_path / "market_data"

    append_partitioned_parquet(
        frame,
        output_path,
        ("chain", "observation_date"),
    )

    assert frame.write.mode_name == "append"
    assert frame.write.partition_columns == ("chain", "observation_date")
    assert frame.write.output_path == str(output_path)
