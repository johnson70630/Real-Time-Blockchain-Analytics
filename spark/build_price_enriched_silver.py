"""Build point-in-time Chainlink-enriched Silver event datasets with Spark."""

import logging
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import col, lit, sum as spark_sum, when

from config.logging import configure_logging
from config.settings import (
    ASSET_PRICE_MAPPING_CONFIG,
    DATA_LAKE,
    get_max_price_age_seconds,
)
from config.storage import join_location
from config.versions import PRICE_ENRICHMENT_JOB_VERSION
from spark.parquet import (
    discover_parquet_files,
    is_remote_path,
    write_partitioned_dataset_atomic,
)
from spark.price_enrichment import (
    EnrichmentLeg,
    create_mapping_frames,
    enrich_events,
    invalid_enrichment_condition,
    load_asset_price_config,
    monetary_columns_are_exact,
)
from spark.session import create_spark_session as create_shared_spark_session

logger = logging.getLogger(__name__)

MARKET_DATA_SILVER_DIR = DATA_LAKE.get("silver_chainlink_market_prices")
PRICE_ENRICHMENT_QUARANTINE_DIR = DATA_LAKE.get(
    "quarantine_price_enrichment"
)


@dataclass(frozen=True, slots=True)
class PriceEnrichmentModel:
    """Describe one existing Silver source and its independent asset legs."""

    name: str
    source_path: str | Path
    output_path: str | Path
    legs: tuple[EnrichmentLeg, ...]


@dataclass(frozen=True, slots=True)
class PriceEnrichmentStats:
    """Operational counts for one enriched Silver dataset."""

    source_records: int
    enriched_records: int
    no_asset_mapping_records: int
    no_prior_price_records: int
    stale_price_records: int
    other_unenriched_records: int
    rejected_records: int
    records_written: int


PRICE_ENRICHMENT_MODELS = (
    PriceEnrichmentModel(
        name="uniswap_v3_swaps",
        source_path=DATA_LAKE.get("silver_uniswap_swaps"),
        output_path=DATA_LAKE.get("enriched_uniswap_swaps"),
        legs=(
            EnrichmentLeg(
                name="token0",
                raw_amount_column="amount0_raw",
                uniswap_pool_side="token0",
                absolute_usd_value=True,
            ),
            EnrichmentLeg(
                name="token1",
                raw_amount_column="amount1_raw",
                uniswap_pool_side="token1",
                absolute_usd_value=True,
            ),
        ),
    ),
    PriceEnrichmentModel(
        name="aave_v3_borrow",
        source_path=DATA_LAKE.get("silver_aave_borrows"),
        output_path=DATA_LAKE.get("enriched_aave_borrows"),
        legs=(
            EnrichmentLeg(
                name="",
                source_asset_column="reserve",
                raw_amount_column="amount_raw",
            ),
        ),
    ),
    PriceEnrichmentModel(
        name="aave_v3_repay",
        source_path=DATA_LAKE.get("silver_aave_repays"),
        output_path=DATA_LAKE.get("enriched_aave_repays"),
        legs=(
            EnrichmentLeg(
                name="",
                source_asset_column="reserve",
                raw_amount_column="amount_raw",
            ),
        ),
    ),
    PriceEnrichmentModel(
        name="aave_v3_liquidation",
        source_path=DATA_LAKE.get("silver_aave_liquidations"),
        output_path=DATA_LAKE.get("enriched_aave_liquidations"),
        legs=(
            EnrichmentLeg(
                name="debt",
                source_asset_column="debt_asset",
                raw_amount_column="debt_to_cover_raw",
            ),
            EnrichmentLeg(
                name="collateral",
                source_asset_column="collateral_asset",
                raw_amount_column="liquidated_collateral_amount_raw",
            ),
        ),
    ),
)


def _source_with_compatibility_columns(
    source: DataFrame,
    model: PriceEnrichmentModel,
) -> DataFrame:
    """Add typed nulls for local Silver files created before this milestone."""
    fallbacks = {"block_timestamp": "timestamp"}
    for leg in model.legs:
        fallbacks[leg.raw_amount_column] = "string"
        if leg.source_asset_column is not None:
            fallbacks[leg.source_asset_column] = "string"
        if leg.uniswap_pool_side is not None:
            fallbacks["pool_address"] = "string"

    compatible = source
    for name, data_type in fallbacks.items():
        if name not in compatible.columns:
            compatible = compatible.withColumn(name, lit(None).cast(data_type))
    return compatible


def _status_condition(
    model: PriceEnrichmentModel,
    status: str,
) -> Column:
    condition = lit(False)
    for leg in model.legs:
        condition |= col(f"{leg.prefix}price_enrichment_status") == status
    return condition


def _collect_stats(
    enriched: DataFrame,
    model: PriceEnrichmentModel,
    rejected_count: int,
) -> PriceEnrichmentStats:
    """Collect all operational metrics in one Spark aggregation."""
    enriched_condition = _status_condition(model, "enriched")
    no_mapping = _status_condition(model, "no_asset_mapping")
    no_prior = _status_condition(model, "no_prior_price")
    stale = _status_condition(model, "stale_price")
    other = _status_condition(model, "invalid_event_timestamp") | (
        _status_condition(model, "invalid_amount")
    )
    row = enriched.agg(
        spark_sum(lit(1)).cast("long").alias("source_records"),
        spark_sum(when(enriched_condition, 1).otherwise(0))
        .cast("long")
        .alias("enriched_records"),
        spark_sum(when(no_mapping, 1).otherwise(0))
        .cast("long")
        .alias("no_asset_mapping_records"),
        spark_sum(when(no_prior, 1).otherwise(0))
        .cast("long")
        .alias("no_prior_price_records"),
        spark_sum(when(stale, 1).otherwise(0))
        .cast("long")
        .alias("stale_price_records"),
        spark_sum(when(other, 1).otherwise(0))
        .cast("long")
        .alias("other_unenriched_records"),
    ).first()
    source_records = row["source_records"] or 0
    return PriceEnrichmentStats(
        source_records=source_records,
        enriched_records=row["enriched_records"] or 0,
        no_asset_mapping_records=row["no_asset_mapping_records"] or 0,
        no_prior_price_records=row["no_prior_price_records"] or 0,
        stale_price_records=row["stale_price_records"] or 0,
        other_unenriched_records=row["other_unenriched_records"] or 0,
        rejected_records=rejected_count,
        records_written=source_records - rejected_count,
    )


def _build_model(
    spark: SparkSession,
    prices: DataFrame,
    mappings: DataFrame,
    pools: DataFrame,
    model: PriceEnrichmentModel,
    max_price_age_seconds: int,
) -> PriceEnrichmentStats:
    source = _source_with_compatibility_columns(
        spark.read.parquet(str(model.source_path)),
        model,
    )
    duplicate_key = (
        source.filter(col("event_id").isNotNull())
        .groupBy("event_id")
        .count()
        .filter(col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate_key:
        raise ValueError(f"{model.source_path} contains duplicate event_id values")

    enriched = enrich_events(
        source,
        prices,
        mappings,
        pools,
        model.legs,
        max_price_age_seconds,
        PRICE_ENRICHMENT_JOB_VERSION,
    ).cache()
    try:
        if not monetary_columns_are_exact(enriched):
            raise ValueError(
                f"{model.name} produced FloatType, DoubleType, or another "
                "unexpected monetary type"
            )
        invalid = invalid_enrichment_condition(
            model.legs,
            max_price_age_seconds,
        )
        rejected = enriched.filter(invalid).cache()
        accepted = enriched.filter(~invalid).cache()
        try:
            rejected_count = rejected.count()
            stats = _collect_stats(enriched, model, rejected_count)
            accepted_count = accepted.count()
            if accepted_count != stats.records_written:
                raise ValueError(
                    f"{model.name} accepted count changed during validation"
                )
            write_partitioned_dataset_atomic(accepted, model.output_path, ())
            write_partitioned_dataset_atomic(
                rejected,
                join_location(PRICE_ENRICHMENT_QUARANTINE_DIR, model.name),
                (),
            )
            return stats
        finally:
            accepted.unpersist()
            rejected.unpersist()
    finally:
        enriched.unpersist()


def build_price_enriched_silver(
    spark: SparkSession,
    *,
    models: tuple[PriceEnrichmentModel, ...] = PRICE_ENRICHMENT_MODELS,
    market_price_root: str | Path = MARKET_DATA_SILVER_DIR,
    mapping_path: Path = ASSET_PRICE_MAPPING_CONFIG,
    max_price_age_seconds: int | None = None,
) -> dict[str, PriceEnrichmentStats]:
    """Atomically rebuild enriched Silver outputs from deterministic inputs."""
    max_age = (
        get_max_price_age_seconds()
        if max_price_age_seconds is None
        else max_price_age_seconds
    )
    if max_age <= 0:
        raise ValueError("max_price_age_seconds must be greater than zero")

    price_files = (
        None
        if is_remote_path(market_price_root)
        else discover_parquet_files(Path(market_price_root))
    )
    if price_files == []:
        logger.info(
            "No Silver market price files found; enriched outputs are unchanged"
        )
        return {}

    config = load_asset_price_config(mapping_path)
    mappings, pools = create_mapping_frames(spark, config)
    prices = (
        spark.read.option("basePath", str(market_price_root))
        .parquet(str(market_price_root))
        .cache()
    )
    try:
        stats_by_model = {}
        for model in models:
            if not is_remote_path(model.source_path) and not Path(
                model.source_path
            ).exists():
                logger.info(
                    "Skipping %s; source Silver dataset does not exist: %s",
                    model.name,
                    model.source_path,
                )
                continue
            try:
                stats = _build_model(
                    spark,
                    prices,
                    mappings,
                    pools,
                    model,
                    max_age,
                )
            except Exception:
                logger.exception("Price enrichment failed for %s", model.name)
                raise
            stats_by_model[model.name] = stats
            logger.info(
                "%s source_events_read=%s events_enriched=%s "
                "events_without_mapping=%s events_without_prior_price=%s "
                "events_with_stale_price=%s events_unenriched=%s "
                "rejected_records=%s records_written=%s output=%s",
                model.name,
                stats.source_records,
                stats.enriched_records,
                stats.no_asset_mapping_records,
                stats.no_prior_price_records,
                stats.stale_price_records,
                stats.other_unenriched_records,
                stats.rejected_records,
                stats.records_written,
                model.output_path,
            )
        return stats_by_model
    finally:
        prices.unpersist()


def create_spark_session() -> SparkSession:
    """Create the shared local or S3-enabled Spark session."""
    spark = create_shared_spark_session("BuildPriceEnrichedSilver")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def main() -> None:
    configure_logging()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        build_price_enriched_silver(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
