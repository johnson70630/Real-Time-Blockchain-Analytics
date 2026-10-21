"""Central settings for the local blockchain analytics pipeline."""

import os
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv

from config.storage import DataLakeLocations, StorageConfig

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

STORAGE = StorageConfig.from_env(PROJECT_ROOT)
DATA_LAKE = DataLakeLocations.from_storage(STORAGE)


def _project_path(env_name: str, default: str | Path) -> Path:
    """Return an absolute path, resolving relative values from the project root."""
    path = Path(os.getenv(env_name, str(default))).expanduser()
    return path if path.is_absolute() else PROJECT_ROOT / path


def get_required_env(name: str) -> str:
    """Read a required environment variable or raise a clear error."""
    value = os.getenv(name)

    if not value:
        raise ValueError(f"Missing required environment variable: {name}")

    return value


def get_alchemy_websocket_url() -> str:
    """Return the Alchemy WebSocket URL required when starting the producer."""
    return get_required_env("ALCHEMY_WEBSOCKET_URL")


def get_alchemy_rpc_url() -> str:
    """Return the HTTP RPC URL required when starting the market-data poller."""
    return get_required_env("ALCHEMY_RPC_URL")


def get_aave_backfill_rpc_url() -> str:
    """Return the dedicated RPC URL required by Aave bounded backfills."""
    return get_required_env("AAVE_BACKFILL_RPC_URL")


def get_blockchain_recovery_rpc_url() -> str:
    """Return the provider-neutral RPC endpoint for bounded recovery jobs."""
    return get_required_env("BLOCKCHAIN_RECOVERY_RPC_URL")


def get_aave_v3_pool_address() -> str:
    """Return the Pool address required only when Aave V3 is enabled."""
    return get_required_env("AAVE_V3_POOL_ADDRESS")


def get_aave_project_collection_start_at() -> datetime:
    """Return the explicit UTC boundary for supported Aave analytics."""
    raw_value = os.getenv(
        "AAVE_PROJECT_COLLECTION_START_AT",
        "2026-10-01T00:00:00Z",
    )
    try:
        value = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(
            "AAVE_PROJECT_COLLECTION_START_AT must be ISO-8601"
        ) from error
    if value.tzinfo is None:
        raise ValueError(
            "AAVE_PROJECT_COLLECTION_START_AT must include a timezone"
        )
    return value.astimezone(UTC)


def get_aave_project_collection_start_block() -> int:
    """Return the RPC-verified first block in the supported Aave period."""
    raw_value = os.getenv("AAVE_PROJECT_COLLECTION_START_BLOCK", "510496864")
    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError(
            "AAVE_PROJECT_COLLECTION_START_BLOCK must be an integer"
        ) from error
    if value < 0:
        raise ValueError(
            "AAVE_PROJECT_COLLECTION_START_BLOCK must not be negative"
        )
    return value


def get_aave_confirmation_depth() -> int:
    """Return the operational block lag used to avoid the newest L2 blocks."""
    raw_value = os.getenv("AAVE_CONFIRMATION_DEPTH", "1200")
    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError("AAVE_CONFIRMATION_DEPTH must be an integer") from error
    if value < 0:
        raise ValueError("AAVE_CONFIRMATION_DEPTH must not be negative")
    return value


def get_aave_bootstrap_chunk_size() -> int:
    """Return the bounded eth_getLogs span used by Aave bootstrap jobs."""
    raw_value = os.getenv("AAVE_BOOTSTRAP_CHUNK_SIZE", "3000")
    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError("AAVE_BOOTSTRAP_CHUNK_SIZE must be an integer") from error
    if value <= 0:
        raise ValueError("AAVE_BOOTSTRAP_CHUNK_SIZE must be greater than zero")
    return value


def _get_positive_integer_setting(name: str, default: int) -> int:
    raw_value = os.getenv(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def get_aave_bootstrap_min_range_size() -> int:
    """Return the smallest provider range eligible for adaptive retry."""
    return _get_positive_integer_setting("AAVE_BOOTSTRAP_MIN_RANGE_SIZE", 100)


def get_aave_bootstrap_max_range_size() -> int:
    """Return the largest provider range allowed by the active plan."""
    return _get_positive_integer_setting("AAVE_BOOTSTRAP_MAX_RANGE_SIZE", 3_000)


def get_aave_bootstrap_max_range_attempts() -> int:
    """Return the finite per-parent adaptive range-attempt budget."""
    return _get_positive_integer_setting("AAVE_BOOTSTRAP_MAX_RANGE_ATTEMPTS", 64)


def get_aave_forward_max_blocks_per_run() -> int:
    """Return the finite upper bound for one normal forward execution."""
    return _get_positive_integer_setting("AAVE_FORWARD_MAX_BLOCKS_PER_RUN", 600_000)


def get_uniswap_project_collection_start_block() -> int:
    """Return the first supported block for Uniswap source coverage."""
    return _get_positive_integer_setting(
        "UNISWAP_PROJECT_COLLECTION_START_BLOCK",
        510_496_864,
    )


def get_uniswap_confirmation_depth() -> int:
    """Return the operational lag applied to Uniswap recovery scans."""
    raw_value = os.getenv("UNISWAP_CONFIRMATION_DEPTH", "1200")
    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError(
            "UNISWAP_CONFIRMATION_DEPTH must be an integer"
        ) from error
    if value < 0:
        raise ValueError("UNISWAP_CONFIRMATION_DEPTH must not be negative")
    return value


def get_uniswap_recovery_chunk_size() -> int:
    """Return the preferred parent block span for one recovery query group."""
    return _get_positive_integer_setting("UNISWAP_RECOVERY_CHUNK_SIZE", 5_000)


def get_uniswap_recovery_address_batch_size() -> int:
    """Return the maximum canonical pool addresses in one RPC query."""
    return _get_positive_integer_setting(
        "UNISWAP_RECOVERY_ADDRESS_BATCH_SIZE",
        68,
    )


def get_uniswap_recovery_min_range_size() -> int:
    """Return the smallest block range eligible for adaptive splitting."""
    return _get_positive_integer_setting(
        "UNISWAP_RECOVERY_MIN_RANGE_SIZE",
        1_000,
    )


def get_uniswap_recovery_max_range_attempts() -> int:
    """Return the finite adaptive-attempt budget for one parent range."""
    return _get_positive_integer_setting(
        "UNISWAP_RECOVERY_MAX_RANGE_ATTEMPTS",
        64,
    )


def get_uniswap_forward_max_blocks_per_run() -> int:
    """Return the finite cap for one normal Uniswap recovery cycle."""
    return _get_positive_integer_setting(
        "UNISWAP_FORWARD_MAX_BLOCKS_PER_RUN",
        600_000,
    )


def get_uniswap_v3_factory_start_block() -> int:
    """Return the authoritative first block for Factory log extraction."""
    raw_block = os.getenv("UNISWAP_V3_FACTORY_START_BLOCK", "165")
    try:
        block = int(raw_block)
    except ValueError as error:
        raise ValueError(
            "UNISWAP_V3_FACTORY_START_BLOCK must be an integer"
        ) from error
    if block < 0:
        raise ValueError(
            "UNISWAP_V3_FACTORY_START_BLOCK must not be negative"
        )
    return block


def get_chainlink_poll_interval_seconds() -> float:
    """Return the positive polling interval configured for Chainlink feeds."""
    raw_interval = os.getenv("CHAINLINK_POLL_INTERVAL_SECONDS", "30")
    try:
        interval = float(raw_interval)
    except ValueError as error:
        raise ValueError(
            "CHAINLINK_POLL_INTERVAL_SECONDS must be a number"
        ) from error
    if interval <= 0:
        raise ValueError(
            "CHAINLINK_POLL_INTERVAL_SECONDS must be greater than zero"
        )
    return interval


def get_chainlink_confirmation_depth() -> int:
    """Return the Chainlink-specific block lag for frozen RPC snapshots."""
    raw_value = os.getenv("CHAINLINK_CONFIRMATION_DEPTH", "1200")
    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError(
            "CHAINLINK_CONFIRMATION_DEPTH must be an integer"
        ) from error
    if value < 0:
        raise ValueError("CHAINLINK_CONFIRMATION_DEPTH must not be negative")
    return value


def get_chainlink_project_collection_start_block() -> int:
    """Return the RPC-verified first block in the supported price period."""
    raw_value = os.getenv(
        "CHAINLINK_PROJECT_COLLECTION_START_BLOCK",
        "510496864",
    )
    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError(
            "CHAINLINK_PROJECT_COLLECTION_START_BLOCK must be an integer"
        ) from error
    if value < 0:
        raise ValueError(
            "CHAINLINK_PROJECT_COLLECTION_START_BLOCK must not be negative"
        )
    return value


def get_chainlink_forward_max_rounds_per_run() -> int:
    """Return the finite upper bound for one Chainlink forward execution."""
    return _get_positive_integer_setting(
        "CHAINLINK_FORWARD_MAX_ROUNDS_PER_RUN",
        10_000,
    )


def get_max_price_age_seconds() -> int:
    """Return the maximum age accepted for point-in-time price enrichment."""
    raw_age = os.getenv("MAX_PRICE_AGE_SECONDS", "300")
    try:
        max_age = int(raw_age)
    except ValueError as error:
        raise ValueError("MAX_PRICE_AGE_SECONDS must be an integer") from error
    if max_age <= 0:
        raise ValueError("MAX_PRICE_AGE_SECONDS must be greater than zero")
    return max_age


def get_enabled_protocols() -> tuple[str, ...]:
    """Return enabled plugin names, preserving the legacy PROTOCOL setting."""
    configured = os.getenv("ENABLED_PROTOCOLS", "").strip()
    if not configured:
        configured = os.getenv("PROTOCOL", "uniswap_v3")

    protocols = tuple(
        dict.fromkeys(
            protocol.strip()
            for protocol in configured.split(",")
            if protocol.strip()
        )
    )
    if not protocols:
        raise ValueError("At least one protocol must be enabled")
    return protocols


PROTOCOL = os.getenv("PROTOCOL", "uniswap_v3")
ENABLED_PROTOCOLS = get_enabled_protocols()
CHAIN = os.getenv("CHAIN", "arbitrum")
UNISWAP_V3_FACTORY_ADDRESS = os.getenv(
    "UNISWAP_V3_FACTORY_ADDRESS",
    "0x1F98431c8aD98523631AE4a59f267346ea31F984",
)
UNISWAP_V3_FACTORY_START_BLOCK = get_uniswap_v3_factory_start_block()

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "uniswap_v3_swaps")
MARKET_DATA_KAFKA_TOPIC = os.getenv(
    "MARKET_DATA_KAFKA_TOPIC",
    "market_data_observations",
)

CHAINLINK_FEEDS_CONFIG = _project_path(
    "CHAINLINK_FEEDS_CONFIG",
    "config/chainlink_feeds.json",
)
ASSET_PRICE_MAPPING_CONFIG = _project_path(
    "ASSET_PRICE_MAPPING_CONFIG",
    "config/asset_price_mapping.json",
)

BRONZE_OUTPUT_PATH = _project_path("BRONZE_OUTPUT_PATH", "data/bronze/swaps")
MARKET_DATA_BRONZE_OUTPUT_PATH = _project_path(
    "MARKET_DATA_BRONZE_OUTPUT_PATH",
    "data/bronze/market_data",
)
MARKET_DATA_QUARANTINE_PATH = _project_path(
    "MARKET_DATA_QUARANTINE_PATH",
    "data/quarantine/market_data",
)
MARKET_DATA_SILVER_DIR = _project_path(
    "MARKET_DATA_SILVER_DIR",
    "data/silver/market_prices",
)
MARKET_DATA_SILVER_QUARANTINE_PATH = _project_path(
    "MARKET_DATA_SILVER_QUARANTINE_PATH",
    "data/quarantine/market_data_silver",
)

SILVER_DIR = _project_path("SILVER_DIR", "data/silver/swaps")
SILVER_OUTPUT_FILE = _project_path(
    "SILVER_OUTPUT_FILE",
    SILVER_DIR / "swaps_silver.parquet",
)
SILVER_PARQUET_GLOB = SILVER_DIR / "*.parquet"

AAVE_SILVER_DIR = _project_path("AAVE_SILVER_DIR", "data/silver/aave_v3")
AAVE_BORROW_EVENTS_FILE = _project_path(
    "AAVE_BORROW_EVENTS_FILE",
    AAVE_SILVER_DIR / "borrow_events.parquet",
)
AAVE_REPAY_EVENTS_FILE = _project_path(
    "AAVE_REPAY_EVENTS_FILE",
    AAVE_SILVER_DIR / "repay_events.parquet",
)
AAVE_LIQUIDATION_EVENTS_FILE = _project_path(
    "AAVE_LIQUIDATION_EVENTS_FILE",
    AAVE_SILVER_DIR / "liquidation_events.parquet",
)
PRICE_ENRICHED_SILVER_DIR = _project_path(
    "PRICE_ENRICHED_SILVER_DIR",
    "data/silver/enriched",
)
UNISWAP_ENRICHED_SWAPS_FILE = _project_path(
    "UNISWAP_ENRICHED_SWAPS_FILE",
    PRICE_ENRICHED_SILVER_DIR
    / "uniswap_v3"
    / "swaps_enriched.parquet",
)
AAVE_ENRICHED_BORROW_EVENTS_FILE = _project_path(
    "AAVE_ENRICHED_BORROW_EVENTS_FILE",
    PRICE_ENRICHED_SILVER_DIR
    / "aave_v3"
    / "borrow_events_enriched.parquet",
)
AAVE_ENRICHED_REPAY_EVENTS_FILE = _project_path(
    "AAVE_ENRICHED_REPAY_EVENTS_FILE",
    PRICE_ENRICHED_SILVER_DIR
    / "aave_v3"
    / "repay_events_enriched.parquet",
)
AAVE_ENRICHED_LIQUIDATION_EVENTS_FILE = _project_path(
    "AAVE_ENRICHED_LIQUIDATION_EVENTS_FILE",
    PRICE_ENRICHED_SILVER_DIR
    / "aave_v3"
    / "liquidation_events_enriched.parquet",
)
PRICE_ENRICHMENT_QUARANTINE_DIR = _project_path(
    "PRICE_ENRICHMENT_QUARANTINE_DIR",
    "data/quarantine/price_enrichment",
)

STATE_DIR = _project_path("STATE_DIR", "data/state")
SILVER_PROCESSED_FILES_MANIFEST = _project_path(
    "SILVER_PROCESSED_FILES_MANIFEST",
    STATE_DIR / "silver_processed_files.json",
)

GOLD_DIR = _project_path("GOLD_DIR", "data/gold")
GOLD_SWAPS_PER_MINUTE_FILE = _project_path(
    "GOLD_SWAPS_PER_MINUTE_FILE",
    GOLD_DIR / "swaps_per_minute" / "swaps_per_minute.parquet",
)
GOLD_TOP_POOLS_FILE = _project_path(
    "GOLD_TOP_POOLS_FILE",
    GOLD_DIR / "top_pools" / "top_pools.parquet",
)
GOLD_PIPELINE_SUMMARY_FILE = _project_path(
    "GOLD_PIPELINE_SUMMARY_FILE",
    GOLD_DIR / "pipeline_summary" / "pipeline_summary.parquet",
)
GOLD_RECENT_SWAPS_FILE = _project_path(
    "GOLD_RECENT_SWAPS_FILE",
    GOLD_DIR / "recent_swaps" / "recent_swaps.parquet",
)
AAVE_GOLD_DIR = _project_path("AAVE_GOLD_DIR", GOLD_DIR / "aave_v3")
AAVE_DAILY_BORROW_ACTIVITY_FILE = _project_path(
    "AAVE_DAILY_BORROW_ACTIVITY_FILE",
    AAVE_GOLD_DIR / "daily_borrow_activity.parquet",
)
AAVE_DAILY_REPAYMENT_ACTIVITY_FILE = _project_path(
    "AAVE_DAILY_REPAYMENT_ACTIVITY_FILE",
    AAVE_GOLD_DIR / "daily_repayment_activity.parquet",
)
AAVE_DAILY_LIQUIDATION_ACTIVITY_FILE = _project_path(
    "AAVE_DAILY_LIQUIDATION_ACTIVITY_FILE",
    AAVE_GOLD_DIR / "daily_liquidation_activity.parquet",
)
AAVE_DAILY_LENDING_SUMMARY_FILE = _project_path(
    "AAVE_DAILY_LENDING_SUMMARY_FILE",
    AAVE_GOLD_DIR / "daily_lending_summary.parquet",
)
GOLD_SWAPS_PER_MINUTE_GLOB = GOLD_SWAPS_PER_MINUTE_FILE.parent / "*.parquet"
GOLD_TOP_POOLS_GLOB = GOLD_TOP_POOLS_FILE.parent / "*.parquet"
GOLD_PIPELINE_SUMMARY_GLOB = GOLD_PIPELINE_SUMMARY_FILE.parent / "*.parquet"
GOLD_RECENT_SWAPS_GLOB = GOLD_RECENT_SWAPS_FILE.parent / "*.parquet"

SPARK_CHECKPOINT_PATH = _project_path(
    "SPARK_CHECKPOINT_PATH",
    "data/checkpoints/swaps_bronze",
)
MARKET_DATA_BRONZE_CHECKPOINT_PATH = _project_path(
    "MARKET_DATA_BRONZE_CHECKPOINT_PATH",
    "data/checkpoints/market_data_bronze",
)
SPARK_KAFKA_CONNECTOR_PACKAGE = os.getenv(
    "SPARK_KAFKA_CONNECTOR_PACKAGE",
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1",
)
SPARK_S3_CONNECTOR_PACKAGE = os.getenv(
    "SPARK_S3_CONNECTOR_PACKAGE",
    "org.apache.hadoop:hadoop-aws:3.4.1",
)
