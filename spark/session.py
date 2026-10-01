"""Shared Spark session construction for local and S3-backed jobs."""

from __future__ import annotations

from pyspark.sql import SparkSession

from config.settings import (
    SPARK_KAFKA_CONNECTOR_PACKAGE,
    SPARK_S3_CONNECTOR_PACKAGE,
    STORAGE,
)
from config.storage import StorageConfig

S3A_CREDENTIAL_PROVIDERS = ",".join(
    (
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "software.amazon.awssdk.auth.credentials."
        "EnvironmentVariableCredentialsProvider",
        "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider",
        "software.amazon.awssdk.auth.credentials."
        "WebIdentityTokenFileCredentialsProvider",
        "org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider",
    )
)


def spark_session_options(
    storage: StorageConfig,
    *,
    include_kafka: bool = False,
) -> dict[str, str]:
    """Return deterministic Spark options without creating a JVM session."""
    packages: list[str] = []
    if include_kafka:
        packages.append(SPARK_KAFKA_CONNECTOR_PACKAGE)
    if storage.is_s3:
        packages.append(SPARK_S3_CONNECTOR_PACKAGE)

    options: dict[str, str] = {}
    if packages:
        options["spark.jars.packages"] = ",".join(packages)
    if storage.is_s3:
        options.update(
            {
                "spark.hadoop.fs.s3a.impl": (
                    "org.apache.hadoop.fs.s3a.S3AFileSystem"
                ),
                "spark.hadoop.fs.s3a.endpoint.region": str(storage.aws_region),
                "spark.hadoop.fs.s3a.aws.credentials.provider": (
                    S3A_CREDENTIAL_PROVIDERS
                ),
            }
        )
    return options


def validate_s3a_provider_classpath(spark: SparkSession) -> None:
    """Fail early if a configured S3A credential provider is not loadable."""
    class_loader = (
        spark.sparkContext._jvm.java.lang.Thread.currentThread()
        .getContextClassLoader()
    )
    missing: list[str] = []
    for provider in S3A_CREDENTIAL_PROVIDERS.split(","):
        try:
            class_loader.loadClass(provider)
        except Exception:
            missing.append(provider)
    if missing:
        names = ", ".join(missing)
        raise RuntimeError(f"S3A credential provider classes not found: {names}")


def create_spark_session(
    app_name: str,
    *,
    include_kafka: bool = False,
    storage: StorageConfig = STORAGE,
) -> SparkSession:
    """Create the single configured Spark session used by pipeline jobs.

    S3A uses Hadoop/AWS SDK providers for environment credentials, named
    profiles, and IAM workload roles. No credential value is copied into Spark.
    """
    builder = SparkSession.builder.appName(app_name)
    for key, value in spark_session_options(
        storage,
        include_kafka=include_kafka,
    ).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    if storage.is_s3:
        try:
            validate_s3a_provider_classpath(spark)
        except Exception:
            spark.stop()
            raise
    return spark
