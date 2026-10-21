"""Correctness contracts for bounded source correction replay."""

from __future__ import annotations

import pytest

from orchestration.correction_replay import (
    DuplicateReplayIdentityError,
    IncompleteReplaySourceError,
    ProcessingMode,
    ReplayCorrectionRequiredError,
    ReplayRecord,
    ReplayRequest,
    ReplayWatermarkChangedError,
    SourceSnapshot,
    execute_replay,
    reconcile_records,
)
from orchestration.run_correction_replay import UniswapReplayLease


def record(identity: str, block: int, fingerprint: str | None = None) -> ReplayRecord:
    return ReplayRecord(identity, block, fingerprint or f"hash-{identity}")


def request(
    mode: ProcessingMode = ProcessingMode.CORRECTION_REPLAY,
) -> ReplayRequest:
    return ReplayRequest("aave", 100, 110, mode)


def test_forward_mode_is_not_accepted_by_replay_entrypoint() -> None:
    with pytest.raises(ValueError, match="existing source-specific"):
        ReplayRequest("aave", 100, 110, ProcessingMode.FORWARD)


def test_replay_request_is_bounded_and_source_limited() -> None:
    assert request().block_count == 11
    with pytest.raises(ValueError, match="inclusive and increasing"):
        ReplayRequest("aave", 110, 100, ProcessingMode.CORRECTION_REPLAY)
    with pytest.raises(ValueError, match="source must be"):
        ReplayRequest("other", 100, 110, ProcessingMode.CORRECTION_REPLAY)


def test_bounded_reconciliation_classifies_every_identity_once() -> None:
    stored = (
        record("unchanged", 101),
        record("changed", 102, "old"),
        record("missing", 103),
    )
    fresh = SourceSnapshot(
        (
            record("unchanged", 101),
            record("changed", 102, "new"),
            record("added", 104),
        ),
        complete=True,
    )
    result = reconcile_records(request(), stored=stored, fresh=fresh)
    assert tuple(item.identity for item in result.unchanged) == ("unchanged",)
    assert tuple(item.identity for item in result.added) == ("added",)
    assert tuple(item.identity for item in result.missing_from_source) == (
        "missing",
    )
    assert tuple(pair[0].identity for pair in result.changed) == ("changed",)
    assert result.requires_canonicality_correction


def test_reconciliation_rejects_partial_or_out_of_scope_source() -> None:
    with pytest.raises(IncompleteReplaySourceError, match="incomplete"):
        reconcile_records(
            request(),
            stored=(),
            fresh=SourceSnapshot((), complete=False),
        )
    with pytest.raises(ValueError, match="outside"):
        reconcile_records(
            request(),
            stored=(),
            fresh=SourceSnapshot((record("outside", 111),), complete=True),
        )


def test_conflicting_duplicate_identity_is_rejected() -> None:
    with pytest.raises(DuplicateReplayIdentityError, match="conflicting"):
        reconcile_records(
            request(),
            stored=(record("same", 101, "a"), record("same", 101, "b")),
            fresh=SourceSnapshot((), complete=True),
        )


def test_historical_replay_is_add_only() -> None:
    states = {"aave": 1_000}
    with pytest.raises(ReplayCorrectionRequiredError, match="add-only"):
        execute_replay(
            request(ProcessingMode.HISTORICAL_REPLAY),
            read_watermarks=lambda: states,
            fetch_fresh=lambda _request: SourceSnapshot((), complete=True),
            load_stored=lambda _request, _fresh: (record("missing", 101),),
        )
    assert states == {"aave": 1_000}


def test_audit_and_deterministic_second_replay_do_not_mutate_state() -> None:
    states = {"aave": 1_000, "uniswap": 2_000, "chainlink": 3_000}
    stored = (record("same", 101),)
    fresh = SourceSnapshot(stored, complete=True)

    def run():
        return execute_replay(
            request(),
            read_watermarks=lambda: states,
            fetch_fresh=lambda _request: fresh,
            load_stored=lambda _request, _fresh: stored,
        )

    first = run()
    second = run()
    assert first.reconciliation == second.reconciliation
    assert not first.applied and not second.applied
    assert states == {"aave": 1_000, "uniswap": 2_000, "chainlink": 3_000}


def test_added_event_applies_once_without_duplicate_business_identity() -> None:
    states = {"aave": 1_000}
    retained: dict[str, ReplayRecord] = {}
    source = SourceSnapshot((record("new", 105),), complete=True)
    downstream_runs = 0

    def apply(result) -> None:
        for item in result.added:
            retained[item.identity] = item

    def refresh() -> None:
        nonlocal downstream_runs
        downstream_runs += 1

    def run():
        return execute_replay(
            request(),
            read_watermarks=lambda: states,
            fetch_fresh=lambda _request: source,
            load_stored=lambda _request, _fresh: tuple(retained.values()),
            apply_reconciliation=apply,
            refresh_downstream=refresh,
        )

    first = run()
    second = run()
    assert first.applied
    assert not second.applied
    assert tuple(retained) == ("new",)
    assert downstream_runs == 1
    assert states["aave"] == 1_000


@pytest.mark.parametrize("failed_stage", ["apply", "downstream"])
def test_failure_never_advances_watermark(failed_stage: str) -> None:
    states = {"aave": 1_000}

    def fail_apply(_result) -> None:
        if failed_stage == "apply":
            raise RuntimeError("apply failed")

    def fail_downstream() -> None:
        if failed_stage == "downstream":
            raise RuntimeError("downstream failed")

    with pytest.raises(RuntimeError, match="failed"):
        execute_replay(
            request(),
            read_watermarks=lambda: states,
            fetch_fresh=lambda _request: SourceSnapshot(
                (record("new", 105),), complete=True
            ),
            load_stored=lambda _request, _fresh: (),
            apply_reconciliation=fail_apply,
            refresh_downstream=fail_downstream,
        )
    assert states == {"aave": 1_000}


def test_watermark_mutation_is_detected() -> None:
    states = {"aave": 1_000}

    def mutate(_result) -> None:
        states["aave"] = 1_001

    with pytest.raises(ReplayWatermarkChangedError, match="watermark"):
        execute_replay(
            request(),
            read_watermarks=lambda: states,
            fetch_fresh=lambda _request: SourceSnapshot(
                (record("new", 105),), complete=True
            ),
            load_stored=lambda _request, _fresh: (),
            apply_reconciliation=mutate,
            refresh_downstream=lambda: None,
        )


def test_uniswap_replay_uses_existing_lease_and_surfaces_conflict() -> None:
    class Store:
        def acquire(self, _identity, _run_id):
            raise RuntimeError("lease held")

    with pytest.raises(RuntimeError, match="lease held"):
        with UniswapReplayLease(Store(), "run-b"):
            raise AssertionError("unreachable")


def test_late_chainlink_window_and_no_future_contract_remain_in_dbt_sql() -> None:
    sql = open(
        "dbt/models/intermediate/pricing/int_defi_event_token_prices.sql",
        encoding="utf-8",
    ).read()
    assert "previous_feed_updated_at" in sql
    assert "events.event_timestamp >= windows.previous_feed_updated_at" in sql
    assert "events.event_timestamp < windows.next_feed_updated_at" in sql
    assert "prices.feed_updated_at <= event_tokens.event_timestamp" in sql
