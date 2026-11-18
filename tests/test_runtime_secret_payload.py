"""Secret representation compatibility without AWS, clipboard or credentials."""

import json

import pytest

from runtime.contracts import SECRET_FIELD, SecretPayloadError, resolve_secret_string

URL = "wss://arb-mainnet.g.alchemy.com/v2/fake"


@pytest.mark.parametrize(
    ("payload", "representation"),
    [
        (URL, "BARE_URL"),
        (json.dumps(URL), "JSON_STRING"),
        (json.dumps({SECRET_FIELD: URL}), "JSON_OBJECT"),
        (json.dumps({SECRET_FIELD: URL}, indent=2), "JSON_OBJECT"),
        (" \n" + json.dumps({SECRET_FIELD: URL}) + "\n", "JSON_OBJECT"),
        (
            json.dumps({SECRET_FIELD: URL.replace("/", "\\/")}).replace("\\\\/", "\\/"),
            "JSON_OBJECT",
        ),
    ],
)
def test_supported_representations_preserve_exact_url_and_hide_it_in_repr(
    payload, representation
):
    result = resolve_secret_string(payload)
    assert result.url == URL
    assert result.representation == representation
    assert URL not in repr(result)
    assert "/v2/" not in repr(result)


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {},
        [],
        b"private-placeholder",
        "",
        "x" * 65537,
        "{}",
        "[]",
        json.dumps([URL]),
        json.dumps({"url": URL}),
        json.dumps({SECRET_FIELD: URL, "other": "private-placeholder"}),
        json.dumps({SECRET_FIELD: {"url": URL}}),
        json.dumps({SECRET_FIELD: None}),
        json.dumps({SECRET_FIELD: True}),
        json.dumps(json.dumps(URL)),
        json.dumps({SECRET_FIELD: json.dumps(URL)}),
        "'" + URL + "'",
        '{"private-placeholder":',
        '{"ALCHEMY_WEBSOCKET_URL":"private-placeholder","ALCHEMY_WEBSOCKET_URL":'
        + json.dumps(URL)
        + "}",
    ],
)
def test_unknown_ambiguous_or_recursive_formats_fail_without_input_in_error(payload):
    with pytest.raises(SecretPayloadError) as raised:
        resolve_secret_string(payload)
    assert "private-placeholder" not in str(raised.value)
    assert "alchemy.com" not in str(raised.value)
    assert URL not in repr(raised.value)


@pytest.mark.parametrize(
    "envelope", [lambda x: x, json.dumps, lambda x: json.dumps({SECRET_FIELD: x})]
)
@pytest.mark.parametrize(
    "bad_url",
    [
        " " + URL,
        URL + "\n",
        "\x00" + URL,
        URL + "\x7f",
        URL.replace("wss:", "ws:"),
        URL.replace("arb-mainnet", "eth-mainnet"),
        URL.replace("arb-mainnet", "arb-sepolia"),
        URL.replace(".alchemy.com", ".alchemy.com.evil"),
        URL.replace("wss://", "wss://user@"),
        URL.replace(".com/", ".com:80/"),
        URL.replace(".com/", ".com:wrong/"),
        URL + "?private-placeholder",
        URL + "#private-placeholder",
        URL + "/extra",
        URL.removesuffix("fake"),
    ],
)
def test_each_representation_enforces_the_same_endpoint_rules(envelope, bad_url):
    with pytest.raises(SecretPayloadError, match="^INVALID_ENDPOINT$"):
        resolve_secret_string(envelope(bad_url))
