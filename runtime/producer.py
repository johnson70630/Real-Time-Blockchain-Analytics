"""Secret-file injection and reconnect policy for the cloud producer only."""

from __future__ import annotations

import json
import logging
import os
import random
import signal
import time
from pathlib import Path
from urllib.parse import urlsplit

from confluent_kafka import KafkaException, Producer
from websocket import WebSocketException

from runtime.contracts import validate_environment

logger = logging.getLogger(__name__)


class ConfirmedKafkaProducer:
    """Acknowledge each event before receiving another source event.

    Idempotence protects librdkafka retries within one producer session. A
    reconnect may still duplicate source events; Bronze keeps Kafka offsets.
    """

    def __init__(self, bootstrap_servers: str, topic: str) -> None:
        self.topic = topic
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "pilot-blockchain-producer",
                "enable.idempotence": True,
                "acks": "all",
                "message.timeout.ms": 30000,
            }
        )

    def send(self, event: dict) -> None:
        delivered = []
        self.producer.produce(
            self.topic,
            key=event.get("transaction_hash", ""),
            value=json.dumps(event),
            on_delivery=lambda error, _message: delivered.append(error),
        )
        remaining = self.producer.flush(35)
        if remaining or not delivered or delivered[0] is not None:
            raise RuntimeError("KAFKA_DELIVERY_NOT_CONFIRMED_RECOVERY_REQUIRED")

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        if self.producer.flush(35):
            raise RuntimeError("KAFKA_UNFLUSHED_MESSAGES_RECOVERY_REQUIRED")


def secret_url(path: Path) -> str:
    value = path.read_text().strip()
    parsed = urlsplit(value)
    if (
        parsed.scheme != "wss"
        or parsed.username
        or parsed.password
        or not (parsed.hostname or "").endswith(".g.alchemy.com")
        or not parsed.path.startswith("/v2/")
        or len(parsed.path) <= 4
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("ALCHEMY_SECRET_MUST_BE_A_WSS_ENDPOINT")
    return value


class RedactEndpoint(logging.Filter):
    def __init__(self, secret: str):
        super().__init__()
        self.secret = secret

    def filter(self, record):
        # Provider errors may embed endpoints or key fragments. Log only fixed
        # messages from our retry handler; also redact full endpoint/key here.
        message = record.getMessage().replace(self.secret, "[redacted]")
        record.msg = message.replace(self.secret.rsplit("/", 1)[-1], "[redacted]")
        record.args = ()
        record.exc_info = None
        record.exc_text = None
        return True


def reconnect(run, *, sleep=time.sleep, jitter=random.uniform) -> None:
    """Reconnect and resubscribe with capped exponential backoff and jitter.

    WebSocket subscriptions have no replay cursor. Every reconnect is a data
    gap candidate and requires bounded chain reconciliation before promotion.
    """
    attempt = 0
    while True:
        try:
            run(kafka_factory=ConfirmedKafkaProducer)
            return  # Graceful signal shutdown returns from the common loop.
        except (
            OSError,
            WebSocketException,
            KafkaException,
            RuntimeError,
            BufferError,
            json.JSONDecodeError,
        ) as error:
            delay = min(60, 2 ** min(attempt, 6)) + jitter(0, 1)
            logger.warning(
                "SOURCE_RECONNECT_RECOVERY_REQUIRED type=%s delay=%.1f",
                type(error).__name__,
                delay,
            )
            attempt += 1
            sleep(delay)


def main() -> int:
    from config.logging import configure_logging
    from producer import run_producer

    configure_logging()
    signal.signal(signal.SIGTERM, run_producer.handle_shutdown_signal)
    try:
        validate_environment(os.environ)
        value = secret_url(Path("/run/secrets/alchemy_websocket_url"))
        for handler in logging.getLogger().handlers:
            handler.addFilter(RedactEndpoint(value))
        # The URL is never an environment variable or a Docker inspect field.
        run_producer.get_alchemy_websocket_url = lambda: value
        reconnect(run_producer.run_producer)
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception as error:
        logger.error("PRODUCER_FAILED type=%s", type(error).__name__)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
