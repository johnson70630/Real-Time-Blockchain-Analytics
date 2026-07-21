"""Build all protocol-specific Gold analytics datasets."""

import logging

from config.logging import configure_logging
from spark.build_aave_gold import build_aave_gold
from spark.build_swaps_gold import main as build_uniswap_gold

logger = logging.getLogger(__name__)


def main() -> None:
    configure_logging()
    build_uniswap_gold()
    build_aave_gold()
    logger.info("All Gold analytics layers complete")


if __name__ == "__main__":
    main()
