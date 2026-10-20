"""Build one deterministic Chainlink Silver forward increment."""

from __future__ import annotations

import argparse
import logging

from config.logging import configure_logging
from spark.build_market_data_silver import build_market_data_silver_increment
from spark.session import create_spark_session

LOGGER = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-block", type=int, required=True)
    parser.add_argument("--end-block", type=int, required=True)
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = _parse_args()
    spark = create_spark_session("BuildChainlinkSilverIncrement")
    spark.sparkContext.setLogLevel("WARN")
    try:
        stats = build_market_data_silver_increment(
            spark,
            start_block=args.start_block,
            end_block=args.end_block,
        )
        LOGGER.info(
            "Chainlink Silver increment range=%s-%s read=%s wrote=%s "
            "quarantined=%s",
            args.start_block,
            args.end_block,
            stats.bronze_records_read,
            stats.silver_records_written,
            stats.invalid_records,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
