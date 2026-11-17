"""Read flattened image metadata without extraction or privilege escalation."""

import io
import tarfile

import pytest

from deploy.runtime import check_image_privileges as audit


def image_tar(rows):
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w") as archive:
        for name, mode, kind in rows:
            row = tarfile.TarInfo(name)
            row.mode, row.type = mode, kind
            row.size = 3 if kind == tarfile.REGTYPE else 0
            if kind in (tarfile.SYMTYPE, tarfile.LNKTYPE):
                row.linkname = "/outside/never-read"
            archive.addfile(row, io.BytesIO(b"abc") if row.size else None)
    data.seek(0)
    return data


def test_regular_files_and_hardlinks_are_counted_without_following_links(tmp_path):
    rows = [
        ("usr", 0o755, tarfile.DIRTYPE),
        ("usr/bin/tool", 0o755, tarfile.REGTYPE),
        ("usr/bin/linked", 0o755, tarfile.LNKTYPE),
        ("usr/bin/symlink", 0o777, tarfile.SYMTYPE),
    ]
    assert audit.inspect_export(image_tar(rows)) == {
        "regular_files_and_links": 2, "setid_files": 0,
    }
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("mode", [0o4755, 0o2755, 0o6755])
@pytest.mark.parametrize("kind", [tarfile.REGTYPE, tarfile.LNKTYPE])
def test_either_privilege_bit_on_any_regular_file_or_link_fails(mode, kind):
    with pytest.raises(ValueError, match="^CONTAINER_SETID_BITS_PRESENT$"):
        audit.inspect_export(image_tar([("unusual/renamed-tool", mode, kind)]))


def test_directory_setgid_is_not_an_executable_privilege_bit():
    assert audit.inspect_export(image_tar([
        ("group-directory", 0o2770, tarfile.DIRTYPE),
        ("group-directory/data", 0o640, tarfile.REGTYPE),
    ]))["setid_files"] == 0


@pytest.mark.parametrize("name", ["/absolute", "safe/../traversal"])
def test_unsafe_member_names_fail_without_extraction(name):
    with pytest.raises(ValueError, match="ARCHIVE_INVALID"):
        audit.inspect_export(image_tar([(name, 0o755, tarfile.REGTYPE)]))


def test_duplicate_paths_cannot_conceal_an_earlier_permission_mode():
    with pytest.raises(ValueError, match="ARCHIVE_INVALID"):
        audit.inspect_export(image_tar([
            ("tool", 0o4755, tarfile.REGTYPE),
            ("./tool", 0o755, tarfile.REGTYPE),
        ]))


@pytest.mark.parametrize("data", [b"", b"invalid archive", b"\0" * 10240])
def test_empty_or_invalid_export_never_passes(data):
    with pytest.raises(ValueError, match="ARCHIVE_(INVALID|EMPTY)"):
        audit.inspect_export(io.BytesIO(data))


def test_truncated_file_body_is_rejected():
    data = image_tar([("tool", 0o755, tarfile.REGTYPE)]).getvalue()
    with pytest.raises(ValueError, match="ARCHIVE_INVALID"):
        audit.inspect_export(io.BytesIO(data[:513]))


@pytest.mark.parametrize("limit", ["MAX_MEMBERS", "MAX_BYTES"])
def test_archive_work_limits_fail_closed(monkeypatch, limit):
    monkeypatch.setattr(audit, limit, 0)
    with pytest.raises(ValueError, match="ARCHIVE_(INVALID|TOO_LARGE)"):
        audit.inspect_export(image_tar([("tool", 0o755, tarfile.REGTYPE)]))
