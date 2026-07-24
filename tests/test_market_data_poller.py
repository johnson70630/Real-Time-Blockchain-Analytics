from datetime import UTC, datetime

from market_data.feeds import ChainlinkFeed
from market_data.poller import ChainlinkMarketDataPoller
from market_data.rpc import BlockSnapshot, ChainlinkRound

FEED_ADDRESS = "0x" + "10" * 20
SECOND_FEED_ADDRESS = "0x" + "20" * 20
LARGE_ROUND_ID = 2**79 + 123
LARGE_ANSWER = 2**255 - 123
NOW = datetime(2026, 7, 23, 12, 0, tzinfo=UTC)
BLOCK = BlockSnapshot(
    number=123456,
    timestamp="2026-07-23T11:59:59+00:00",
)


def _feed(
    address: str = FEED_ADDRESS,
    base_asset: str = "ETH",
) -> ChainlinkFeed:
    return ChainlinkFeed(
        name=f"{base_asset}/USD",
        feed_address=address,
        base_asset=base_asset,
        quote_asset="USD",
        chain="arbitrum",
    )


class FakePublisher:
    def __init__(self) -> None:
        self.messages = []

    def send(self, message) -> None:
        self.messages.append(message)


class FakeRpc:
    def __init__(
        self,
        rounds: list[ChainlinkRound] | None = None,
        *,
        fail_blocks: int = 0,
    ) -> None:
        self.rounds = rounds or [
            ChainlinkRound(
                round_id=LARGE_ROUND_ID,
                answer_raw=LARGE_ANSWER,
                updated_at="2026-07-23T11:59:58+00:00",
            )
        ]
        self.fail_blocks = fail_blocks
        self.decimal_calls = 0
        self.block_calls = 0
        self.round_calls: list[tuple[str, int]] = []
        self._round_index = 0

    def is_connected(self) -> bool:
        return True

    def get_latest_block(self) -> BlockSnapshot:
        self.block_calls += 1
        if self.block_calls <= self.fail_blocks:
            raise ConnectionError("temporary RPC failure")
        return BLOCK

    def get_feed_decimals(self, _feed_address: str) -> int:
        self.decimal_calls += 1
        return 8

    def get_latest_round(
        self,
        feed_address: str,
        block_number: int,
    ) -> ChainlinkRound:
        self.round_calls.append((feed_address, block_number))
        index = min(self._round_index, len(self.rounds) - 1)
        self._round_index += 1
        return self.rounds[index]


def _poller(
    rpc: FakeRpc,
    publisher: FakePublisher,
    *,
    feeds: tuple[ChainlinkFeed, ...] | None = None,
    sleeper=lambda _seconds: None,
) -> ChainlinkMarketDataPoller:
    return ChainlinkMarketDataPoller(
        rpc=rpc,
        publisher=publisher,
        feeds=feeds or (_feed(),),
        poll_interval_seconds=30,
        clock=lambda: NOW,
        sleeper=sleeper,
    )


def test_decimals_are_cached_once_for_service_lifetime() -> None:
    rpc = FakeRpc()
    poller = _poller(rpc, FakePublisher())

    poller.initialize()
    poller.initialize()
    poller.poll_once()
    poller.poll_once()

    assert rpc.decimal_calls == 1


def test_only_new_rounds_are_published_with_exact_observation_identity() -> None:
    next_round = ChainlinkRound(
        round_id=LARGE_ROUND_ID + 1,
        answer_raw=LARGE_ANSWER - 1,
        updated_at="2026-07-23T12:00:28+00:00",
    )
    rpc = FakeRpc([FakeRpc().rounds[0], FakeRpc().rounds[0], next_round])
    publisher = FakePublisher()
    poller = _poller(rpc, publisher)
    poller.initialize()

    first = poller.poll_once()
    second = poller.poll_once()
    third = poller.poll_once()

    assert first.observations_published == 1
    assert second.unchanged_observations == 1
    assert third.observations_published == 1
    assert len(publisher.messages) == 2

    message = publisher.messages[0]
    assert message["observation_id"] == (
        f"arbitrum:{FEED_ADDRESS.lower()}:{LARGE_ROUND_ID}"
    )
    assert message["protocol"] == "chainlink"
    assert message["event_type"] == "price_update"
    assert message["round_id"] == str(LARGE_ROUND_ID)
    assert message["answer_raw"] == str(LARGE_ANSWER)
    assert isinstance(message["round_id"], str)
    assert isinstance(message["answer_raw"], str)
    assert message["feed_decimals"] == 8
    assert message["block_number"] == BLOCK.number
    assert message["observed_at"] == NOW.isoformat()
    assert message["ingested_at"] == NOW.isoformat()
    assert "transaction_hash" not in message
    assert "log_index" not in message
    assert rpc.round_calls == [(FEED_ADDRESS, BLOCK.number)] * 3


def test_all_feeds_use_the_same_pinned_block() -> None:
    rpc = FakeRpc()
    publisher = FakePublisher()
    feeds = (
        _feed(),
        _feed(SECOND_FEED_ADDRESS, "USDC"),
    )
    poller = _poller(rpc, publisher, feeds=feeds)
    poller.initialize()

    stats = poller.poll_once()

    assert stats.feeds_checked == 2
    assert stats.observations_published == 2
    assert rpc.block_calls == 1
    assert rpc.round_calls == [
        (FEED_ADDRESS, BLOCK.number),
        (SECOND_FEED_ADDRESS, BLOCK.number),
    ]
    assert rpc.decimal_calls == 2


def test_cycle_failure_retries_on_next_interval() -> None:
    rpc = FakeRpc(fail_blocks=1)
    publisher = FakePublisher()
    sleeps = []
    poller = _poller(rpc, publisher, sleeper=sleeps.append)

    poller.run(max_cycles=2)

    assert rpc.block_calls == 2
    assert len(publisher.messages) == 1
    assert sleeps == [30]
    assert rpc.decimal_calls == 1
