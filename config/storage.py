"""Central storage configuration and canonical data-lake locations."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Mapping

STORAGE_MODES = frozenset({"local", "s3"})

# Stable logical dataset names shared by Spark and warehouse landing code.
DATASET_PATHS: dict[str, str] = {
    "bronze_events": "bronze/swaps",
    "bronze_market_data": "bronze/market_data",
    "silver_uniswap_swaps": "silver/swaps/swaps_silver.parquet",
    "silver_aave_borrows": "silver/aave_v3/borrow_events.parquet",
    "silver_aave_repays": "silver/aave_v3/repay_events.parquet",
    "silver_aave_liquidations": "silver/aave_v3/liquidation_events.parquet",
    "silver_chainlink_market_prices": "silver/market_prices",
    "enriched_uniswap_swaps": (
        "silver/enriched/uniswap_v3/swaps_enriched.parquet"
    ),
    "enriched_aave_borrows": (
        "silver/enriched/aave_v3/borrow_events_enriched.parquet"
    ),
    "enriched_aave_repays": (
        "silver/enriched/aave_v3/repay_events_enriched.parquet"
    ),
    "enriched_aave_liquidations": (
        "silver/enriched/aave_v3/liquidation_events_enriched.parquet"
    ),
    "quarantine_market_data": "quarantine/market_data",
    "quarantine_market_data_silver": "quarantine/market_data_silver",
    "quarantine_price_enrichment": "quarantine/price_enrichment",
    "checkpoint_events_bronze": "checkpoints/swaps_bronze",
    "checkpoint_market_data_bronze": "checkpoints/market_data_bronze",
    "gold": "gold",
}


def _clean_relative_path(value: str) -> str:
    """Return a normalized, safe relative object path."""
    normalized = str(PurePosixPath(value.strip().strip("/")))
    if not normalized or normalized == "." or normalized.startswith("../"):
        raise ValueError(f"Invalid data-lake path: {value!r}")
    return normalized


@dataclass(frozen=True)
class StorageConfig:
    """Resolve logical data paths for local development or an S3 data lake."""

    mode: str
    project_root: Path
    bucket: str | None = None
    prefix: str = "blockchain-analytics"
    aws_region: str | None = None

    def __post_init__(self) -> None:
        if self.mode not in STORAGE_MODES:
            expected = ", ".join(sorted(STORAGE_MODES))
            raise ValueError(f"STORAGE_MODE must be one of: {expected}")

        if self.mode == "s3":
            if not self.bucket:
                raise ValueError(
                    "DATA_LAKE_BUCKET is required when STORAGE_MODE=s3"
                )
            if not re.fullmatch(
                r"(?=.{3,63}$)[a-z0-9][a-z0-9.-]*[a-z0-9]",
                self.bucket,
            ):
                raise ValueError("DATA_LAKE_BUCKET is not a valid S3 bucket name")
            if not self.aws_region:
                raise ValueError("AWS_REGION is required when STORAGE_MODE=s3")

        _clean_relative_path(self.prefix)

    @classmethod
    def from_env(
        cls,
        project_root: Path,
        environ: Mapping[str, str] | None = None,
    ) -> StorageConfig:
        """Build storage configuration without contacting AWS."""
        values = os.environ if environ is None else environ
        mode = values.get("STORAGE_MODE", "local").strip().lower()
        return cls(
            mode=mode,
            project_root=project_root.resolve(),
            bucket=values.get("DATA_LAKE_BUCKET", "").strip() or None,
            prefix=(
                values.get("DATA_LAKE_PREFIX", "").strip()
                or "blockchain-analytics"
            ),
            aws_region=values.get("AWS_REGION", "").strip() or None,
        )

    @property
    def is_s3(self) -> bool:
        """Return whether durable storage uses S3."""
        return self.mode == "s3"

    def location(self, relative_path: str) -> str:
        """Resolve one canonical relative path for the configured backend."""
        relative = _clean_relative_path(relative_path)
        if not self.is_s3:
            return str(self.project_root / "data" / relative)

        prefix = _clean_relative_path(self.prefix)
        return f"s3a://{self.bucket}/{prefix}/{relative}"

    def s3_url(self, relative_path: str = "silver") -> str:
        """Return a Snowflake-compatible s3:// URL for an S3 location."""
        if not self.is_s3:
            raise ValueError("An S3 URL requires STORAGE_MODE=s3")
        relative = _clean_relative_path(relative_path)
        prefix = _clean_relative_path(self.prefix)
        return f"s3://{self.bucket}/{prefix}/{relative}"


@dataclass(frozen=True)
class DataLakeLocations:
    """Resolved canonical locations for all persisted pipeline datasets."""

    storage: StorageConfig
    datasets: Mapping[str, str]

    @classmethod
    def from_storage(cls, storage: StorageConfig) -> DataLakeLocations:
        """Resolve every registered dataset from the same storage root."""
        return cls(
            storage=storage,
            datasets={
                name: storage.location(path)
                for name, path in DATASET_PATHS.items()
            },
        )

    def get(self, dataset: str) -> str:
        """Return a registered dataset location with a clear lookup error."""
        try:
            return self.datasets[dataset]
        except KeyError as error:
            raise KeyError(f"Unknown data-lake dataset: {dataset}") from error


def join_location(root: str | Path, *parts: str) -> str | Path:
    """Join child names onto either a local path or an s3a:// URI."""
    clean_parts = [_clean_relative_path(part) for part in parts]
    value = str(root)
    if value.startswith("s3a://"):
        return "/".join((value.rstrip("/"), *clean_parts))
    joined = Path(value).joinpath(*clean_parts)
    return joined if isinstance(root, Path) else str(joined)
