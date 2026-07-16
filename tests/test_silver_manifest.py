import json

import pytest

from spark.build_swaps_silver import (
    load_processed_files,
    save_processed_files,
    select_unprocessed_files,
    to_manifest_entry,
)


def test_missing_manifest_returns_empty_processed_set(tmp_path) -> None:
    manifest_path = tmp_path / "state" / "silver_processed_files.json"

    assert load_processed_files(manifest_path) == set()


def test_saved_manifest_can_be_loaded(tmp_path) -> None:
    manifest_path = tmp_path / "state" / "silver_processed_files.json"
    processed_files = {
        "data/bronze/swaps/partition-a.parquet",
        "data/bronze/swaps/partition-b.parquet",
    }

    save_processed_files(processed_files, manifest_path)

    assert load_processed_files(manifest_path) == processed_files


def test_only_unprocessed_files_are_selected(tmp_path) -> None:
    first_file = tmp_path / "bronze" / "part-00001.parquet"
    second_file = tmp_path / "bronze" / "part-00002.parquet"
    discovered_files = [first_file, second_file]
    processed_files = {to_manifest_entry(first_file, tmp_path)}

    selected_files = select_unprocessed_files(
        discovered_files,
        processed_files,
        project_root=tmp_path,
    )

    assert selected_files == [second_file]


def test_corrupted_manifest_raises_clear_error(tmp_path) -> None:
    manifest_path = tmp_path / "state" / "silver_processed_files.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(json.dumps({"processed_files": "not-a-list"}))

    with pytest.raises(
        ValueError,
        match="Invalid Silver processed-files manifest",
    ):
        load_processed_files(manifest_path)
