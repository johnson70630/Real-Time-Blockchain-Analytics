from spark import build_gold


def test_combined_gold_entrypoint_runs_uniswap_and_aave(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(
        build_gold,
        "build_uniswap_gold",
        lambda: calls.append("uniswap"),
    )
    monkeypatch.setattr(
        build_gold,
        "build_aave_gold",
        lambda: calls.append("aave"),
    )

    build_gold.main()

    assert calls == ["uniswap", "aave"]
