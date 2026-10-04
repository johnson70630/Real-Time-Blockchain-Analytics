"""Print Snowflake setup and incremental RAW landing SQL."""

import argparse

from config.settings import STORAGE
from warehouse.snowflake import (
    WAREHOUSE_DATASETS,
    SnowflakeLandingConfig,
    generate_dataset_load_sql,
    generate_setup_sql,
    generate_stage_root_sql,
    validate_dataset_paths,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--setup-only",
        action="store_true",
        help="print external-stage alignment and persistent table DDL only",
    )
    mode.add_argument(
        "--loads-only",
        action="store_true",
        help="print incremental COPY and MERGE statements only",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        choices=tuple(dataset.dataset for dataset in WAREHOUSE_DATASETS),
        help="limit load SQL to one dataset; may be specified repeatedly",
    )
    args = parser.parse_args()
    if args.setup_only and args.dataset:
        parser.error("--dataset cannot be combined with --setup-only")
    return args


def main() -> None:
    """Validate cloud configuration and print executable Snowflake SQL."""
    args = _parse_args()
    config = SnowflakeLandingConfig.from_env()
    validate_dataset_paths()
    sections: list[str] = []
    if not args.loads_only:
        if STORAGE.is_s3:
            sections.append("-- Reuse the external stage at the data-lake root")
            sections.append(generate_stage_root_sql(config, STORAGE))
        sections.append("-- Persistent RAW and landing tables")
        sections.append(generate_setup_sql(config))
    if not args.setup_only:
        selected = set(args.dataset or ())
        for dataset in WAREHOUSE_DATASETS:
            if selected and dataset.dataset not in selected:
                continue
            sections.append(f"-- Incremental load: {dataset.dataset}")
            sections.append(generate_dataset_load_sql(config, dataset))
    print("\n\n".join(sections))


if __name__ == "__main__":
    main()
