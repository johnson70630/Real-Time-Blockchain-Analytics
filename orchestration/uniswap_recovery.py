"""Planning and commit-last contracts for Uniswap source-gap recovery."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

from orchestration.aave_forward import (
    CompletedBlockRange,
    highest_contiguous_block,
)
from orchestration.watermark import (
    UNISWAP_FORWARD_WATERMARK,
    WatermarkState,
    WatermarkStore,
    commit_watermark_from_expected,
)
from warehouse.leases import PipelineLeaseIdentity

T = TypeVar("T")
UNISWAP_RECOVERY_CONTRACT_VERSION = "v1"
UNISWAP_FORWARD_LEASE = PipelineLeaseIdentity(
    pipeline_name="uniswap_forward",
    source_name="uniswap_v3_pools",
    chain_id=42161,
)


class UniswapSourceContractChangedError(RuntimeError):
    """Raised when forward recovery no longer matches its accepted contract."""


def source_contract_run_id(
    run_id: str,
    fingerprint: str,
    *,
    contract_version: str = UNISWAP_RECOVERY_CONTRACT_VERSION,
) -> str:
    """Tag a successful run with its accepted pools/topics/query contract."""
    normalized_fingerprint = fingerprint.strip().lower()
    if len(normalized_fingerprint) != 64 or any(
        character not in "0123456789abcdef"
        for character in normalized_fingerprint
    ):
        raise ValueError("Source contract fingerprint must be 64 hexadecimal characters")
    if not run_id.strip() or not contract_version.strip():
        raise ValueError("Run ID and source contract version must not be empty")
    return (
        f"{run_id.strip()}|uniswap_source_contract="
        f"{contract_version.strip()}:{normalized_fingerprint}"
    )


def assert_source_contract_matches(
    state: WatermarkState,
    fingerprint: str,
    *,
    contract_version: str = UNISWAP_RECOVERY_CONTRACT_VERSION,
) -> None:
    """Reject forward work when accepted pools, topics, or query semantics changed."""
    expected_suffix = source_contract_run_id(
        "accepted",
        fingerprint,
        contract_version=contract_version,
    ).removeprefix("accepted")
    if not state.successful_run_id.endswith(expected_suffix):
        raise UniswapSourceContractChangedError(
            "Uniswap source contract differs from the accepted forward watermark; "
            "reaccept and backfill the changed contract before advancing"
        )


@dataclass(frozen=True, slots=True)
class UniswapRecoveryPlan:
    """One finite interval after an explicit committed source position."""

    committed_block: int
    safe_head: int
    start_block: int | None
    end_block: int | None

    @property
    def is_noop(self) -> bool:
        return self.start_block is None


def plan_recovery_increment(
    *,
    committed_block: int,
    safe_head: int,
    max_blocks: int,
) -> UniswapRecoveryPlan:
    """Plan only confirmed blocks immediately after durable source coverage."""
    if committed_block < 0 or safe_head < 0:
        raise ValueError("Block values must not be negative")
    if max_blocks <= 0:
        raise ValueError("max_blocks must be greater than zero")
    if safe_head <= committed_block:
        return UniswapRecoveryPlan(committed_block, safe_head, None, None)
    start_block = committed_block + 1
    return UniswapRecoveryPlan(
        committed_block,
        safe_head,
        start_block,
        min(safe_head, start_block + max_blocks - 1),
    )


def bootstrap_eligible_value(
    *,
    project_start_block: int,
    frozen_safe_block: int,
    completed_ranges: tuple[CompletedBlockRange, ...],
) -> int | None:
    """Return the proposed initial value only for gap-free source coverage."""
    contiguous = highest_contiguous_block(project_start_block, completed_ranges)
    return frozen_safe_block if contiguous >= frozen_safe_block else None


def run_commit_last_pipeline(
    *,
    store: WatermarkStore,
    current: WatermarkState,
    plan: UniswapRecoveryPlan,
    successful_run_id: str,
    contract_fingerprint: str,
    persist_source: Callable[[UniswapRecoveryPlan], T],
    build_silver: Callable[[T], None],
    load_snowflake: Callable[[], None],
    run_dbt_and_quality: Callable[[], None],
) -> WatermarkState:
    """Advance source coverage only after every downstream boundary succeeds."""
    if current.identity != UNISWAP_FORWARD_WATERMARK:
        raise ValueError("Unexpected Uniswap watermark identity")
    assert_source_contract_matches(current, contract_fingerprint)
    if current.committed_value != plan.committed_block:
        raise ValueError("Plan does not start at the supplied watermark")
    if plan.is_noop:
        return current
    assert plan.end_block is not None
    source_result = persist_source(plan)
    build_silver(source_result)
    load_snowflake()
    run_dbt_and_quality()
    return commit_watermark_from_expected(
        store,
        UNISWAP_FORWARD_WATERMARK,
        expected_value=plan.committed_block,
        candidate_value=plan.end_block,
        successful_run_id=source_contract_run_id(
            successful_run_id,
            contract_fingerprint,
        ),
        processing_succeeded=True,
    )
