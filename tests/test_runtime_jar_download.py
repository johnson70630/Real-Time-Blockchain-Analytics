"""Offline cache/integrity/failure tests for build-time public dependencies."""

import hashlib
import io
import urllib.error

import pytest

from deploy.runtime import fetch_jars as jars

DATA = b"synthetic pinned connector"
PIN = hashlib.sha512(DATA).hexdigest()
LOCK = [{"path": "org/example/connector/1/connector-1.jar", "sha512": PIN}]


def test_cache_hit_and_offline_install_make_no_network_request(tmp_path, monkeypatch):
    cache, target = tmp_path / "cache", tmp_path / "installed"
    cache.mkdir()
    (cache / "connector-1.jar").write_bytes(DATA)
    monkeypatch.setattr(jars.urllib.request, "urlopen", lambda *a, **k: pytest.fail("Network forbidden"))
    jars.install(LOCK, target=cache)
    jars.install(LOCK, target=target, cache=cache, offline=True)
    assert (target / "connector-1.jar").read_bytes() == DATA
    assert (target / "connector-1.jar").stat().st_mode & 0o777 == 0o644


def test_cold_cache_downloads_only_fixed_maven_path(tmp_path, monkeypatch):
    calls = []
    def open_url(url, **kwargs):
        calls.append(url)
        assert kwargs == {"timeout": 120}
        return io.BytesIO(DATA)
    monkeypatch.setattr(jars.urllib.request, "urlopen", open_url)
    jars.install(LOCK, target=tmp_path)
    assert calls == ["https://repo.maven.apache.org/maven2/" + LOCK[0]["path"]]
    assert sorted(p.name for p in tmp_path.iterdir()) == ["connector-1.jar"]


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_transient_failure_honors_short_retry_after(tmp_path, monkeypatch, status):
    calls, delays = [], []
    def open_url(*args, **kwargs):
        calls.append(args[0])
        if len(calls) == 1:
            raise urllib.error.HTTPError(args[0], status, "synthetic", {"Retry-After": "5"}, io.BytesIO())
        return io.BytesIO(DATA)
    monkeypatch.setattr(jars.urllib.request, "urlopen", open_url)
    monkeypatch.setattr(jars.time, "sleep", delays.append)
    jars.install(LOCK, target=tmp_path)
    assert len(calls) == 2 and len(set(calls)) == 1
    assert delays == [5]
    assert (tmp_path / "connector-1.jar").read_bytes() == DATA


@pytest.mark.parametrize("status,headers", [(404, {}), (403, {}), (429, {"Retry-After": "60"}), (429, {"Retry-After": "later"})])
def test_permanent_or_long_cooldown_failure_does_not_retry(tmp_path, monkeypatch, status, headers):
    calls = []
    def open_url(*args, **kwargs):
        calls.append(args[0])
        raise urllib.error.HTTPError(args[0], status, "synthetic", headers, io.BytesIO())
    monkeypatch.setattr(jars.urllib.request, "urlopen", open_url)
    monkeypatch.setattr(jars.time, "sleep", lambda _: pytest.fail("Must not wait/retry"))
    with pytest.raises(urllib.error.HTTPError):
        jars.install(LOCK, target=tmp_path)
    assert len(calls) == 1 and not list(tmp_path.iterdir())


def test_retries_are_bounded_and_leave_no_partial_jar(tmp_path, monkeypatch):
    calls, delays = [], []
    def open_url(*args, **kwargs):
        calls.append(args[0])
        raise urllib.error.HTTPError(args[0], 429, "synthetic", {}, io.BytesIO())
    monkeypatch.setattr(jars.urllib.request, "urlopen", open_url)
    monkeypatch.setattr(jars.time, "sleep", delays.append)
    with pytest.raises(urllib.error.HTTPError):
        jars.install(LOCK, target=tmp_path)
    assert len(calls) == 3 and delays == [2, 4]
    assert not list(tmp_path.iterdir())


def test_interrupted_body_is_restarted_without_appending_partial_content(tmp_path, monkeypatch):
    class Interrupted(io.BytesIO):
        def read(self, size=-1):
            if self.tell():
                raise ConnectionResetError("synthetic interruption")
            return super().read(3)
    replies = iter([Interrupted(DATA), io.BytesIO(DATA)])
    monkeypatch.setattr(jars.urllib.request, "urlopen", lambda *a, **k: next(replies))
    monkeypatch.setattr(jars.time, "sleep", lambda _: None)
    jars.install(LOCK, target=tmp_path)
    assert (tmp_path / "connector-1.jar").read_bytes() == DATA
    assert len(list(tmp_path.iterdir())) == 1


def test_checksum_failure_is_terminal_and_never_promoted(tmp_path, monkeypatch):
    calls = []
    def open_url(*args, **kwargs):
        calls.append(args[0])
        return io.BytesIO(b"corrupt")
    monkeypatch.setattr(jars.urllib.request, "urlopen", open_url)
    monkeypatch.setattr(jars.time, "sleep", lambda _: pytest.fail("Integrity failures must not retry"))
    with pytest.raises(ValueError, match="checksum"):
        jars.install(LOCK, target=tmp_path)
    assert len(calls) == 1 and not list(tmp_path.iterdir())


@pytest.mark.parametrize("bad", ["corrupt", "symlink", "missing"])
def test_bad_offline_cache_fails_closed_without_network(tmp_path, monkeypatch, bad):
    cache = tmp_path / "cache"
    cache.mkdir()
    if bad == "corrupt":
        (cache / "connector-1.jar").write_bytes(b"corrupt")
    elif bad == "symlink":
        data = tmp_path / "data"
        data.write_bytes(DATA)
        (cache / "connector-1.jar").symlink_to(data)
    monkeypatch.setattr(jars.urllib.request, "urlopen", lambda *a, **k: pytest.fail("Network forbidden"))
    with pytest.raises(ValueError):
        jars.install(LOCK, target=tmp_path / "target", cache=cache, offline=True)
    assert not list((tmp_path / "target").iterdir())


def test_existing_bundled_conflict_is_not_overwritten(tmp_path, monkeypatch):
    destination = tmp_path / "connector-1.jar"
    destination.write_bytes(b"existing")
    monkeypatch.setattr(jars.urllib.request, "urlopen", lambda *a, **k: pytest.fail("Network forbidden"))
    with pytest.raises(ValueError):
        jars.install(LOCK, target=tmp_path)
    assert destination.read_bytes() == b"existing"
