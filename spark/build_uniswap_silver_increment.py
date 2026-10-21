"""Build one Uniswap recovery Silver increment from committed Bronze."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from config.logging import configure_logging
from spark.build_cloud_silver import build_uniswap_silver_increment
from spark.session import create_spark_session

LOGGER = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-block", type=int, required=True)
    parser.add_argument("--end-block", type=int, required=True)
    parser.add_argument("--metrics-output", type=Path)
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = _parse_args()
    spark = create_spark_session("BuildUniswapSilverIncrement")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")
    try:
        count = build_uniswap_silver_increment(
            spark,
            start_block=args.start_block,
            end_block=args.end_block,
        )
        LOGGER.info(
            "Uniswap recovery Silver rows=%s range=%s-%s",
            count,
            args.start_block,
            args.end_block,
        )
        if args.metrics_output is not None:
            args.metrics_output.write_text(
                json.dumps({"silver_records_written": count}),
                encoding="utf-8",
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
