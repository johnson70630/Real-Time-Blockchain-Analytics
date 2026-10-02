from pathlib import Path
from types import SimpleNamespace

import pytest

from config.storage import DATASET_PATHS, DataLakeLocations, StorageConfig
from spark import session as spark_session
from spark.session import spark_session_options


def test_local_storage_uses_project_data_directory(tmp_path: Path) -> None:
    storage = StorageConfig.from_env(tmp_path, {})
    locations = DataLakeLocations.from_storage(storage)

    assert storage.mode == "local"
    assert locations.get("bronze_events") == str(
        tmp_path / "data" / "bronze" / "swaps"
    )
    assert locations.get("enriched_uniswap_swaps").endswith(
        "data/silver/enriched/uniswap_v3/swaps_enriched.parquet"
    )
    assert locations.get("reference_uniswap_v3_pools").endswith(
        "data/reference/uniswap_v3/pools"
    )


def test_s3_storage_generates_canonical_s3a_locations(tmp_path: Path) -> None:
    storage = StorageConfig.from_env(
        tmp_path,
        {
            "STORAGE_MODE": "s3",
            "DATA_LAKE_BUCKET": "analytics-lake-123",
            "DATA_LAKE_PREFIX": "production/blockchain",
            "AWS_REGION": "us-west-2",
        },
    )
    locations = DataLakeLocations.from_storage(storage)

    assert locations.get("bronze_market_data") == (
        "s3a://analytics-lake-123/production/blockchain/bronze/market_data"
    )
    assert storage.s3_url("silver") == (
        "s3://analytics-lake-123/production/blockchain/silver"
    )
    assert locations.get("state_uniswap_v3_pools") == (
        "s3a://analytics-lake-123/production/blockchain/"
        "state/uniswap_v3/pools_watermark"
    )
    assert locations.get("quarantine_uniswap_v3_pools") == (
        "s3a://analytics-lake-123/production/blockchain/"
        "quarantine/reference/uniswap_v3/pools"
    )


@pytest.mark.parametrize(
    ("environ", "message"),
    [
        ({"STORAGE_MODE": "azure"}, "STORAGE_MODE"),
        ({"STORAGE_MODE": "s3", "AWS_REGION": "us-west-2"}, "BUCKET"),
        (
            {
                "STORAGE_MODE": "s3",
                "DATA_LAKE_BUCKET": "analytics-lake-123",
            },
            "AWS_REGION",
        ),
    ],
)
def test_invalid_storage_configuration_fails_clearly(
    tmp_path: Path,
    environ: dict[str, str],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        StorageConfig.from_env(tmp_path, environ)


def test_every_dataset_resolves_under_one_s3_root(tmp_path: Path) -> None:
    storage = StorageConfig(
        mode="s3",
        project_root=tmp_path,
        bucket="analytics-lake-123",
        prefix="prod",
        aws_region="us-east-1",
    )
    locations = DataLakeLocations.from_storage(storage)

    assert set(locations.datasets) == set(DATASET_PATHS)
    assert all(
        location.startswith("s3a://analytics-lake-123/prod/")
        for location in locations.datasets.values()
    )


def test_spark_s3_options_use_connector_without_credentials(
    tmp_path: Path,
) -> None:
    storage = StorageConfig(
        mode="s3",
        project_root=tmp_path,
        bucket="analytics-lake-123",
        aws_region="us-west-2",
    )

    options = spark_session_options(storage, include_kafka=True)

    assert "hadoop-aws" in options["spark.jars.packages"]
    assert "spark-sql-kafka" in options["spark.jars.packages"]
    assert options["spark.hadoop.fs.s3a.endpoint.region"] == "us-west-2"
    providers = options[
        "spark.hadoop.fs.s3a.aws.credentials.provider"
    ]
    assert (
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
        in providers
    )
    assert "ProfileAWSCredentialsProvider" not in providers
    assert "WebIdentityTokenFileCredentialsProvider" in providers
    assert "IAMInstanceCredentialsProvider" in providers
    assert "spark.hadoop.fs.s3a.auth.profile.name" not in options
    assert not any("secret" in key.lower() or "access.key" in key.lower() for key in options)


def test_local_spark_options_do_not_require_s3_connector(tmp_path: Path) -> None:
    storage = StorageConfig(mode="local", project_root=tmp_path)

    options = spark_session_options(storage)

    assert options == {}


def test_s3_options_keep_iam_provider_without_local_profile(
    tmp_path: Path,
) -> None:
    storage = StorageConfig(
        mode="s3",
        project_root=tmp_path,
        bucket="analytics-lake-123",
        aws_region="us-west-2",
    )

    options = spark_session_options(storage)

    assert "spark.hadoop.fs.s3a.auth.profile.name" not in options
    providers = options["spark.hadoop.fs.s3a.aws.credentials.provider"]
    assert (
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"
        in providers
    )
    assert "ProfileAWSCredentialsProvider" not in providers
    assert "IAMInstanceCredentialsProvider" in options[
        "spark.hadoop.fs.s3a.aws.credentials.provider"
    ]


def test_s3a_classpath_validation_reports_missing_provider(monkeypatch) -> None:
    class Loader:
        def loadClass(self, name: str) -> None:
            if name == "missing.Provider":
                raise RuntimeError("not found")

    loader = Loader()
    thread = SimpleNamespace(getContextClassLoader=lambda: loader)
    java_thread = SimpleNamespace(currentThread=lambda: thread)
    spark = SimpleNamespace(
        sparkContext=SimpleNamespace(
            _jvm=SimpleNamespace(
                java=SimpleNamespace(
                    lang=SimpleNamespace(Thread=java_thread),
                )
            )
        )
    )
    monkeypatch.setattr(
        spark_session,
        "S3A_CREDENTIAL_PROVIDERS",
        "available.Provider,missing.Provider",
    )

    with pytest.raises(RuntimeError, match="missing.Provider"):
        spark_session.validate_s3a_provider_classpath(spark)
