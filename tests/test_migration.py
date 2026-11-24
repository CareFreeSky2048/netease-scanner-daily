import hashlib
import io
import json
import tarfile

import pytest

from prepare_migration import prepare
from verify_migration import extract_verified, verify_archive


def test_migration_active_archive_and_byte_checks(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    active = b"\xef\xbb\xbfartist,track\r\nexample,song\r\n"
    (source / "VA_Tracks_Output.csv").write_bytes(active)
    (source / "VA_Album_Date_Cache.json").write_text('{"1":{"album_name":"test"}}')
    (source / "VA_Scan_State.json").write_text('{"old":true}')
    (source / "netease_api.out.log").write_text("old log\n" * 1000)
    caches = source / "VA_Artist_Caches"
    caches.mkdir()
    (caches / "artists_0123456789abcdef.json").write_text(json.dumps({"count": 1, "artists": [{"id": "1", "name": "test"}]}))
    output = tmp_path / "migration.tar.gz"
    manifest = prepare(source, output)
    assert len(manifest["files"]) == 5
    assert verify_archive(output) == manifest
    target = tmp_path / "target"
    extract_verified(output, target)
    assert (target / "data/VA_Tracks_Output.csv").read_bytes() == active
    assert (target / "data/migration_archive/VA_Scan_State.json").exists()
    assert (target / "data/migration_archive/netease_api.out.log").exists()
    assert (source / "VA_Tracks_Output.csv").read_bytes() == active
    with pytest.raises(ValueError, match="拒绝覆盖"):
        extract_verified(output, target)
    with pytest.raises(ValueError, match="已存在"):
        prepare(source, output)


def test_rejects_traversal_and_tampered_content(tmp_path):
    unsafe = tmp_path / "unsafe.tar.gz"
    with tarfile.open(unsafe, "w:gz") as archive:
        info = tarfile.TarInfo("data/../../outside")
        info.size = 1
        archive.addfile(info, io.BytesIO(b"x"))
    with pytest.raises(ValueError, match="不安全"):
        verify_archive(unsafe)
    bad = tmp_path / "bad.tar.gz"
    manifest = {"files": [{"path": "data/VA_Tracks_Output.csv", "size": 1, "sha256": "0" * 64}]}
    with tarfile.open(bad, "w:gz") as archive:
        for name, data in (("data/VA_Tracks_Output.csv", b"x"), ("MANIFEST.json", json.dumps(manifest).encode())):
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    with pytest.raises(ValueError, match="SHA-256"):
        verify_archive(bad)
