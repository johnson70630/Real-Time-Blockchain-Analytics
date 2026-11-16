"""Synthetic producer/native-library check, supplied over stdin without network."""

import os
import ssl
import sys
from importlib.metadata import version as distribution_version

from confluent_kafka import libversion
from eth_abi import encode
import certifi

from producer import run_producer
from producer.event_handlers import SwapEventHandler
from runtime.producer import ConfirmedKafkaProducer

assert sys.version_info[:2] == (3, 12)
assert os.environ["LC_ALL"] == "C.UTF-8" and os.environ["LANGUAGE"] == ""
assert distribution_version("confluent-kafka") == "2.14.2"
assert distribution_version("aiohttp") == "3.14.5"
assert distribution_version("certifi") == "2026.10.23"
assert distribution_version("urllib3") == "2.8.0"
# Exercise the installed CA bundle, not just the package's version metadata.
ca_subjects = {
    value
    for certificate in ssl.create_default_context(cafile=certifi.where()).get_ca_certs()
    for rdn in certificate["subject"]
    for key, value in rdn
    if key == "commonName"
}
assert "emSign Root CA - G1" in ca_subjects
assert "emSign ECC Root CA - G3" not in ca_subjects
assert "AAA Certificate Services" not in ca_subjects
assert libversion()[1] > 0
assert callable(run_producer.run_producer) and callable(ConfirmedKafkaProducer)

handler = SwapEventHandler()
log = {
    "address": "0x" + "11" * 20,
    "topics": [
        handler.topic,
        "0x" + encode(["address"], ["0x" + "22" * 20]).hex(),
        "0x" + encode(["address"], ["0x" + "33" * 20]).hex(),
    ],
    "data": "0x"
    + encode(
        ["int256", "int256", "uint160", "uint128", "int24"],
        [-10, 20, 2**96, 1000, -120],
    ).hex(),
    "blockNumber": "0x64",
    "transactionHash": "0x" + "ab" * 32,
    "transactionIndex": "0x0",
    "blockHash": "0x" + "cd" * 32,
    "logIndex": "0x0",
    "removed": False,
}
event = handler.build_event(
    log, "2026-01-01T00:00:00+00:00", chain="arbitrum"
).to_dict()
assert event["block_number"] == 100 and event["chain"] == "arbitrum"
assert event["protocol"] == "uniswap_v3" and event["schema_version"]
assert event["payload"]["amount0"] == "-10" and event["payload"]["amount1"] == "20"
print("PRODUCER_NATIVE_ABI_NORMALIZATION_VERIFIED")
print("PRODUCER_PYTHON_SECURITY_AND_CA_BUNDLE_VERIFIED")
