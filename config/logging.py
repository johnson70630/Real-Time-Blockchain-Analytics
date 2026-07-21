"""Application logging configuration shared by command-line entry points."""

import logging


def configure_logging() -> None:
    """Configure concise process-wide logging without import-time side effects."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
