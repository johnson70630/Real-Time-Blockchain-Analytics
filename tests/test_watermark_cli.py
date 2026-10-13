"""CLI safety contracts for explicit watermark operations."""

from __future__ import annotations

from argparse import Namespace

import pytest

from orchestration.run_watermark import _require_mutation_args


def test_mutations_require_explicit_value_and_run_id() -> None:
    with pytest.raises(ValueError, match="--value and --run-id"):
        _require_mutation_args(Namespace(value=None, run_id=None))
    assert _require_mutation_args(
        Namespace(value=123, run_id="acceptance-test")
    ) == (123, "acceptance-test")
