"""Reusable Spark transformations for point-in-time price enrichment."""

import json
import re
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    abs as spark_abs,
    broadcast,
    col,
    current_timestamp,
    expr,
    length,
    lit,
    lower,
    monotonically_increasing_id,
    regexp_replace,
    round as spark_round,
    row_number,
    trim,
    upper,
    when,
)
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

DECIMAL_PRECISION = 38
AMOUNT_SCALE = 18
USD_SCALE = 8
AMOUNT_TYPE = DecimalType(DECIMAL_PRECISION, AMOUNT_SCALE)
USD_TYPE = DecimalType(DECIMAL_PRECISION, USD_SCALE)

# Narrower operands leave sufficient integral precision when Spark derives the
# multiplication type. The public output is cast back to DECIMAL(38, 8).
CALCULATION_TYPE = DecimalType(28, AMOUNT_SCALE)

_EVM_ADDRESS_PATTERN = re.compile(r"^0x[0-9a-f]{40}$")


@dataclass(frozen=True, slots=True)
class AssetPriceMapping:
    """Map one event asset identity to its Chainlink price asset."""

    chain: str
    event_asset: str
    event_asset_address: str | None
    price_asset: str
    quote_asset: str
    token_decimals: int


@dataclass(frozen=True, slots=True)
class UniswapV3PoolMapping:
    """Identify both token sides of one Uniswap V3 pool."""

    chain: str
    pool_address: str
    token0_asset: str
    token1_asset: str


@dataclass(frozen=True, slots=True)
class AssetPriceConfig:
    """Validated asset aliases and optional Uniswap pool identities."""

    mappings: tuple[AssetPriceMapping, ...]
    uniswap_v3_pools: tuple[UniswapV3PoolMapping, ...]


@dataclass(frozen=True, slots=True)
class EnrichmentLeg:
    """Describe one independently priced token amount in an event."""

    name: str
    raw_amount_column: str
    source_asset_column: str | None = None
    uniswap_pool_side: str | None = None
    absolute_usd_value: bool = False

    @property
    def prefix(self) -> str:
        return f"{self.name}_" if self.name else ""


def _required_string(
    entry: dict,
    field: str,
    context: str,
    *,
    lower_value: bool = False,
) -> str:
    value = entry.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{context}.{field} must be a non-empty string")
    normalized = value.strip()
    return normalized.lower() if lower_value else normalized.upper()


def load_asset_price_config(path: Path) -> AssetPriceConfig:
    """Load and validate the version-controlled enrichment mapping."""
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise ValueError(f"Asset price mapping not found: {path}") from error
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"Invalid asset price mapping at {path}: {error}") from error

    if not isinstance(document, dict):
        raise ValueError("Asset price mapping must be a JSON object")
    raw_mappings = document.get("mappings")
    raw_pools = document.get("uniswap_v3_pools", [])
    if not isinstance(raw_mappings, list) or not raw_mappings:
        raise ValueError("Asset price mapping requires a non-empty 'mappings' list")
    if not isinstance(raw_pools, list):
        raise ValueError("'uniswap_v3_pools' must be a list")

    mappings = []
    mapping_keys: set[tuple[str, str]] = set()
    address_keys: set[tuple[str, str]] = set()
    for index, entry in enumerate(raw_mappings):
        context = f"mappings[{index}]"
        if not isinstance(entry, dict):
            raise ValueError(f"{context} must be an object")
        chain = _required_string(entry, "chain", context, lower_value=True)
        event_asset = _required_string(entry, "event_asset", context)
        price_asset = _required_string(entry, "price_asset", context)
        quote_asset = _required_string(entry, "quote_asset", context)
        address = entry.get("event_asset_address")
        if address is not None:
            if not isinstance(address, str):
                raise ValueError(
                    f"{context}.event_asset_address must be a string or null"
                )
            address = address.strip().lower()
            if not _EVM_ADDRESS_PATTERN.fullmatch(address):
                raise ValueError(
                    f"{context}.event_asset_address must be a 20-byte EVM address"
                )
        decimals = entry.get("token_decimals")
        if not isinstance(decimals, int) or not 0 <= decimals <= AMOUNT_SCALE:
            raise ValueError(
                f"{context}.token_decimals must be an integer from 0 to "
                f"{AMOUNT_SCALE}"
            )
        mapping_key = (chain, event_asset)
        if mapping_key in mapping_keys:
            raise ValueError(f"Duplicate asset mapping: {chain}/{event_asset}")
        mapping_keys.add(mapping_key)
        if address is not None:
            address_key = (chain, address)
            if address_key in address_keys:
                raise ValueError(f"Duplicate asset address mapping: {chain}/{address}")
            address_keys.add(address_key)
        mappings.append(
            AssetPriceMapping(
                chain=chain,
                event_asset=event_asset,
                event_asset_address=address,
                price_asset=price_asset,
                quote_asset=quote_asset,
                token_decimals=decimals,
            )
        )

    pools = []
    pool_keys: set[tuple[str, str]] = set()
    for index, entry in enumerate(raw_pools):
        context = f"uniswap_v3_pools[{index}]"
        if not isinstance(entry, dict):
            raise ValueError(f"{context} must be an object")
        chain = _required_string(entry, "chain", context, lower_value=True)
        pool_address = _required_string(
            entry,
            "pool_address",
            context,
            lower_value=True,
        )
        if not _EVM_ADDRESS_PATTERN.fullmatch(pool_address):
            raise ValueError(f"{context}.pool_address must be a 20-byte EVM address")
        token0_asset = _required_string(entry, "token0_asset", context)
        token1_asset = _required_string(entry, "token1_asset", context)
        for asset in (token0_asset, token1_asset):
            if (chain, asset) not in mapping_keys:
                raise ValueError(
                    f"{context} references unmapped asset {chain}/{asset}"
                )
        pool_key = (chain, pool_address)
        if pool_key in pool_keys:
            raise ValueError(f"Duplicate Uniswap pool mapping: {chain}/{pool_address}")
        pool_keys.add(pool_key)
        pools.append(
            UniswapV3PoolMapping(
                chain=chain,
                pool_address=pool_address,
                token0_asset=token0_asset,
                token1_asset=token1_asset,
            )
        )

    return AssetPriceConfig(tuple(mappings), tuple(pools))


def create_mapping_frames(
    spark: SparkSession,
    config: AssetPriceConfig,
) -> tuple[DataFrame, DataFrame]:
    """Create the small version-controlled mapping DataFrames."""
    mapping_schema = StructType(
        [
            StructField("mapping_chain", StringType(), False),
            StructField("event_asset", StringType(), False),
            StructField("event_asset_address", StringType(), True),
            StructField("price_asset", StringType(), False),
            StructField("quote_asset", StringType(), False),
            StructField("token_decimals", IntegerType(), False),
        ]
    )
    mappings = spark.createDataFrame(
        [
            (
                mapping.chain,
                mapping.event_asset,
                mapping.event_asset_address,
                mapping.price_asset,
                mapping.quote_asset,
                mapping.token_decimals,
            )
            for mapping in config.mappings
        ],
        mapping_schema,
    )
    pool_schema = StructType(
        [
            StructField("pool_chain", StringType(), False),
            StructField("pool_address_key", StringType(), False),
            StructField("pool_side", StringType(), False),
            StructField("pool_event_asset", StringType(), False),
        ]
    )
    pool_rows = [
        (pool.chain, pool.pool_address, side, asset)
        for pool in config.uniswap_v3_pools
        for side, asset in (
            ("token0", pool.token0_asset),
            ("token1", pool.token1_asset),
        )
    ]
    pools = spark.createDataFrame(pool_rows, pool_schema)
    return mappings, pools


def _exact_decimal_expression(
    raw_column: str,
    decimals_column: str,
) -> Column:
    """Scale an integer string to DECIMAL(38, 18) without floating point."""
    return expr(
        f"""
        TRY_CAST(
            CASE
                WHEN {decimals_column} = 0 THEN TRIM(CAST({raw_column} AS STRING))
                WHEN {decimals_column} BETWEEN 1 AND {AMOUNT_SCALE} THEN CONCAT(
                    CASE
                        WHEN STARTSWITH(TRIM(CAST({raw_column} AS STRING)), '-')
                        THEN '-'
                        ELSE ''
                    END,
                    SUBSTRING(
                        LPAD(
                            REGEXP_REPLACE(
                                TRIM(CAST({raw_column} AS STRING)), '^-', ''
                            ),
                            GREATEST(
                                LENGTH(REGEXP_REPLACE(
                                    TRIM(CAST({raw_column} AS STRING)), '^-', ''
                                )),
                                {decimals_column} + 1
                            ),
                            '0'
                        ),
                        1,
                        LENGTH(LPAD(
                            REGEXP_REPLACE(
                                TRIM(CAST({raw_column} AS STRING)), '^-', ''
                            ),
                            GREATEST(
                                LENGTH(REGEXP_REPLACE(
                                    TRIM(CAST({raw_column} AS STRING)), '^-', ''
                                )),
                                {decimals_column} + 1
                            ),
                            '0'
                        )) - {decimals_column}
                    ),
                    '.',
                    RIGHT(
                        LPAD(
                            REGEXP_REPLACE(
                                TRIM(CAST({raw_column} AS STRING)), '^-', ''
                            ),
                            GREATEST(
                                LENGTH(REGEXP_REPLACE(
                                    TRIM(CAST({raw_column} AS STRING)), '^-', ''
                                )),
                                {decimals_column} + 1
                            ),
                            '0'
                        ),
                        {decimals_column}
                    )
                )
            END AS DECIMAL({DECIMAL_PRECISION}, {AMOUNT_SCALE})
        )
        """
    )


def _resolve_asset_mapping(
    event_legs: DataFrame,
    mappings: DataFrame,
    pools: DataFrame,
    leg: EnrichmentLeg,
) -> DataFrame:
    """Attach a normalized event asset and its price-feed alias."""
    if leg.source_asset_column is not None:
        return event_legs.join(
            broadcast(mappings),
            (lower(trim(col("event_chain"))) == col("mapping_chain"))
            & (
                lower(trim(col("source_asset_address")))
                == col("event_asset_address")
            ),
            "left",
        )
    if leg.uniswap_pool_side not in {"token0", "token1"}:
        raise ValueError(f"Enrichment leg {leg.name!r} has no asset identity")
    resolved_pool = event_legs.join(
        broadcast(pools),
        (lower(trim(col("event_chain"))) == col("pool_chain"))
        & (lower(trim(col("source_pool_address"))) == col("pool_address_key"))
        & (col("pool_side") == lit(leg.uniswap_pool_side)),
        "left",
    )
    return resolved_pool.join(
        broadcast(mappings),
        (col("pool_chain") == col("mapping_chain"))
        & (col("pool_event_asset") == col("event_asset")),
        "left",
    )


def _price_candidates(prices: DataFrame) -> DataFrame:
    """Select and normalize only columns required by the temporal join."""
    return prices.select(
        lower(trim(col("chain"))).alias("price_chain"),
        upper(trim(col("base_asset"))).alias("price_base_asset"),
        upper(trim(col("quote_asset"))).alias("price_quote_asset"),
        col("observation_id").alias("price_observation_id"),
        col("feed_address").alias("price_feed_address"),
        col("answer_raw").alias("price_answer_raw"),
        col("feed_decimals").alias("price_feed_decimals"),
        col("price").cast(AMOUNT_TYPE).alias("candidate_price"),
        col("feed_updated_at").cast("timestamp").alias("price_timestamp"),
        col("block_number").alias("price_block_number"),
        col("round_id").cast("string").alias("price_round_id"),
        col("silver_processed_at").cast("timestamp").alias(
            "price_silver_processed_at"
        ),
    ).filter(col("candidate_price") > lit(0).cast(AMOUNT_TYPE))


def enrich_event_leg(
    events: DataFrame,
    prices: DataFrame,
    mappings: DataFrame,
    pools: DataFrame,
    leg: EnrichmentLeg,
    max_price_age_seconds: int,
) -> DataFrame:
    """Enrich one event amount using the latest non-future Chainlink price."""
    if max_price_age_seconds <= 0:
        raise ValueError("max_price_age_seconds must be greater than zero")

    selections = [
        "_enrichment_row_id",
        col("chain").alias("event_chain"),
        col("block_timestamp").cast("timestamp").alias("event_timestamp"),
        col(leg.raw_amount_column).cast("string").alias("raw_amount"),
    ]
    if leg.source_asset_column is not None:
        selections.append(
            col(leg.source_asset_column)
            .cast("string")
            .alias("source_asset_address")
        )
    else:
        selections.append(
            col("pool_address").cast("string").alias("source_pool_address")
        )
    event_legs = events.select(*selections)
    mapped = _resolve_asset_mapping(event_legs, mappings, pools, leg).withColumn(
        "normalized_amount",
        _exact_decimal_expression("raw_amount", "token_decimals"),
    )

    candidates = _price_candidates(prices)
    joined = mapped.join(
        candidates,
        (lower(trim(col("event_chain"))) == col("price_chain"))
        & (col("price_asset") == col("price_base_asset"))
        & (col("quote_asset") == col("price_quote_asset"))
        & (col("price_timestamp") <= col("event_timestamp")),
        "left",
    )
    normalized_round = when(
        regexp_replace(col("price_round_id"), r"^0+", "") == "",
        lit("0"),
    ).otherwise(regexp_replace(col("price_round_id"), r"^0+", ""))
    ordering = Window.partitionBy("_enrichment_row_id").orderBy(
        col("price_timestamp").desc_nulls_last(),
        col("price_block_number").desc_nulls_last(),
        length(normalized_round).desc_nulls_last(),
        normalized_round.desc_nulls_last(),
        col("price_silver_processed_at").desc_nulls_last(),
        col("price_observation_id").desc_nulls_last(),
        col("price_feed_address").desc_nulls_last(),
        col("candidate_price").desc_nulls_last(),
        col("price_answer_raw").desc_nulls_last(),
    )
    latest = (
        joined.withColumn("_price_rank", row_number().over(ordering))
        .filter(col("_price_rank") == 1)
        .drop("_price_rank")
        .withColumn(
            "price_age_seconds",
            col("event_timestamp").cast("long")
            - col("price_timestamp").cast("long"),
        )
    )

    valid_amount = col("normalized_amount").isNotNull()
    calculation_amount = col("normalized_amount").cast(CALCULATION_TYPE)
    if leg.absolute_usd_value:
        valid_amount &= col("normalized_amount") != lit(0).cast(AMOUNT_TYPE)
        calculation_amount = spark_abs(calculation_amount)
    else:
        valid_amount &= col("normalized_amount") > lit(0).cast(AMOUNT_TYPE)
    valid_amount &= calculation_amount.isNotNull()
    status = (
        when(col("event_timestamp").isNull(), "invalid_event_timestamp")
        .when(col("event_asset").isNull(), "no_asset_mapping")
        .when(~valid_amount, "invalid_amount")
        .when(col("price_observation_id").isNull(), "no_prior_price")
        .when(col("price_age_seconds") > max_price_age_seconds, "stale_price")
        .otherwise("enriched")
    )
    classified = latest.withColumn("_enrichment_status", status)
    is_enriched = col("_enrichment_status") == "enriched"
    usd_value = spark_round(
        calculation_amount * col("candidate_price").cast(CALCULATION_TYPE),
        USD_SCALE,
    ).cast(USD_TYPE)
    prefix = leg.prefix
    usd_column = (
        f"{prefix}amount_usd_abs"
        if leg.absolute_usd_value
        else f"{prefix}amount_usd"
    )
    reason = (
        when(is_enriched, lit(None).cast("string"))
        .when(
            col("_enrichment_status") == "invalid_event_timestamp",
            "event block timestamp is missing or invalid",
        )
        .when(
            col("_enrichment_status") == "no_asset_mapping",
            "event asset is absent from the configured mapping",
        )
        .when(
            col("_enrichment_status") == "invalid_amount",
            "raw token amount is invalid or outside calculation precision",
        )
        .when(
            col("_enrichment_status") == "no_prior_price",
            "no matching price exists at or before the event",
        )
        .otherwise("latest prior price exceeds the maximum allowed age")
    )

    return classified.select(
        "_enrichment_row_id",
        col("event_asset").alias(f"{prefix}asset_symbol"),
        col("price_asset").alias(f"{prefix}price_asset"),
        col("quote_asset").alias(f"{prefix}quote_asset"),
        col("token_decimals").alias(f"{prefix}token_decimals"),
        col("normalized_amount").alias(f"{prefix}normalized_amount"),
        when(is_enriched, col("price_answer_raw")).alias(
            f"{prefix}price_answer_raw"
        ),
        when(is_enriched, col("price_feed_decimals")).alias(
            f"{prefix}price_feed_decimals"
        ),
        when(is_enriched, col("candidate_price")).alias(f"{prefix}price_usd"),
        when(is_enriched, usd_value).alias(usd_column),
        when(is_enriched, col("price_observation_id")).alias(
            f"{prefix}matched_observation_id"
        ),
        when(is_enriched, col("price_feed_address")).alias(
            f"{prefix}matched_feed_address"
        ),
        when(is_enriched, col("price_timestamp")).alias(
            f"{prefix}matched_price_timestamp"
        ),
        when(is_enriched, col("price_block_number")).alias(
            f"{prefix}matched_price_block_number"
        ),
        col("price_age_seconds").alias(f"{prefix}price_age_seconds"),
        col("_enrichment_status").alias(f"{prefix}price_enrichment_status"),
        reason.alias(f"{prefix}price_enrichment_reason"),
    )


def enrich_events(
    events: DataFrame,
    prices: DataFrame,
    mappings: DataFrame,
    pools: DataFrame,
    legs: tuple[EnrichmentLeg, ...],
    max_price_age_seconds: int,
    enrichment_job_version: str,
) -> DataFrame:
    """Enrich every configured leg while preserving every source event."""
    enriched = events.withColumn(
        "_enrichment_row_id",
        monotonically_increasing_id(),
    )
    for leg in legs:
        leg_result = enrich_event_leg(
            enriched,
            prices,
            mappings,
            pools,
            leg,
            max_price_age_seconds,
        )
        enriched = enriched.join(
            leg_result,
            "_enrichment_row_id",
            "left",
        )
    return (
        enriched.drop("_enrichment_row_id")
        .withColumn("enrichment_processed_at", current_timestamp())
        .withColumn("enrichment_job_version", lit(enrichment_job_version))
    )


def invalid_enrichment_condition(
    legs: tuple[EnrichmentLeg, ...],
    max_price_age_seconds: int,
) -> Column:
    """Return row-level data-quality checks for enrichment output."""
    invalid = col("event_id").isNull()
    for leg in legs:
        prefix = leg.prefix
        status = col(f"{prefix}price_enrichment_status")
        price = col(f"{prefix}price_usd")
        observation = col(f"{prefix}matched_observation_id")
        timestamp = col(f"{prefix}matched_price_timestamp")
        age = col(f"{prefix}price_age_seconds")
        usd_column = (
            f"{prefix}amount_usd_abs"
            if leg.absolute_usd_value
            else f"{prefix}amount_usd"
        )
        enriched = status == "enriched"
        invalid |= enriched & (
            price.isNull()
            | (price <= lit(0).cast(AMOUNT_TYPE))
            | col(usd_column).isNull()
            | observation.isNull()
            | timestamp.isNull()
            | (timestamp > col("block_timestamp"))
            | age.isNull()
            | (age < 0)
            | (age > max_price_age_seconds)
        )
        invalid |= (~enriched) & (
            col(usd_column).isNotNull()
            | price.isNotNull()
            | observation.isNotNull()
        )
    return invalid


def monetary_columns_are_exact(frame: DataFrame) -> bool:
    """Return whether all derived monetary columns use DecimalType."""
    for field in frame.schema.fields:
        if (
            field.name.endswith(
                ("_normalized_amount", "_price_usd", "_amount_usd")
            )
            or field.name in {"normalized_amount", "price_usd", "amount_usd"}
            or field.name.endswith("_amount_usd_abs")
        ) and not isinstance(field.dataType, DecimalType):
            return False
    return True
