"""Build the immutable Chainlink bootstrap Silver increment."""

from __future__ import annotations

import logging

from config.logging import configure_logging
from spark.build_market_data_silver import build_chainlink_bootstrap_silver_increment
from spark.session import create_spark_session

LOGGER = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    spark = create_spark_session("BuildChainlinkBootstrapSilver")
    spark.sparkContext.setLogLevel("WARN")
    try:
        stats = build_chainlink_bootstrap_silver_increment(spark)
        LOGGER.info(
            "Chainlink bootstrap Silver read=%s new=%s duplicates=%s quarantine=%s",
            stats.bronze_records_read,
            stats.silver_records_written,
            stats.duplicate_records_removed,
            stats.invalid_records,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
