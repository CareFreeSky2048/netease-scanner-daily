import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import re
import shutil
import sqlite3
import subprocess
import tarfile
from contextlib import closing
from datetime import date, timedelta
from pathlib import Path

import pytest
import requests

import build_site
import ci_scan
import engine
import server
import state_store


def _make_sample_data_dir(base: Path, *, num_m3_reports: int = 15) -> Path:
    data = base / "data"
    (data / "VA_Artist_Caches").mkdir(parents=True, exist_ok=True)
    (data / "migration_archive" / "pre_migration").mkdir(parents=True, exist_ok=True)

    csv_rows = [
        ["Artist A", "Track 1", "VA One", "2026-11-24", "https://music.163.com/#/song?id=101", "1"],
        ["Artist B", "Track 2", "VA One", "2026-11-24", "https://music.163.com/#/song?id=102", "1"],
        ["Artist C", "Track 3", "VA One", "2026-11-24", "https://music.163.com/#/song?id=103", "1"],
        ["Artist D", "Track 4", "Solo Album", "2026-11-23", "https://music.163.com/#/song?id=104", "0"],
    ]
    for name in (data / "VA_Tracks_Output.csv", data / "migration_archive" / "pre_migration" / "VA_Tracks_Output.csv"):
        with name.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(state_store.CSV_HEADER)
            w.writerows(csv_rows)

    album_cache = {
        str(9000 + i): {"album_id": str(9000 + i), "album_name": f"Album {i}", "date_str": "2026-11-24", "publish_ts": 1795449600000}
        for i in range(20)
    }
    (data / "VA_Album_Date_Cache.json").write_text(json.dumps(album_cache), encoding="utf-8")
    (data / "VA_Artist_Caches" / "artists_0123456789abcdef.json").write_text(
        json.dumps({"saved_at": 1.0, "count": 2, "meta": {}, "artists": [{"id": "1", "name": "Artist A"}, {"id": "2", "name": "Artist B"}]}),
        encoding="utf-8",
    )
    (data / "VA_Scan_Metrics.json").write_text(json.dumps({"results": 4}), encoding="utf-8")
    (data / "VA_Scan_Tuning_Report.txt").write_text("ok\n", encoding="utf-8")

    store = server.Store(data / "scan_history.sqlite3")
    store.set_value("migration_snapshot_initialized", "2026-10-07T00:00:00+08:00")
    store.set_value("backup_date", "2026-11-24")

    run_ids = []
    with store.connect() as db:
        for i in range(num_m3_reports):
            rid = f"run_{i:03d}"
            run_ids.append(rid)
            day = f"2026-11-{(i % 24) + 1:02d}"
            created_at = f"{day}T00:{i % 60:02d}:00+08:00"
            db.execute(
                "INSERT INTO runs(id,source,schedule_date,schedule_time,status,created_at,started_at,ended_at,params,summary,metrics,error,warning) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    rid,
                    "scheduled",
                    day,
                    f"00:{i % 60:02d}",
                    "success",
                    created_at,
                    created_at,
                    created_at,
                    json.dumps({"scanMode": "m3", "threads": 4}),
                    json.dumps({"artists": 10}),
                    json.dumps({"elapsed_sec": 120.5}),
                    "",
                    "",
                ),
            )
            db.execute(
                "INSERT INTO settings(key,value) VALUES(?,?)",
                (f"m3_match_report:{rid}", json.dumps({"run_id": rid, "blob": "x" * 200})),
            )
            db.execute(
                "INSERT INTO logs(run_id,created_at,level,text) VALUES(?,?,?,?)",
                (rid, created_at, "info", f"log for {rid}"),
            )
        # Put 3 hits on album 5001 (heat 3) and 1 hit on album 5002 (heat 1) on latest run.
        latest_rid = run_ids[-1]
        for idx, artist in enumerate(("Artist A", "Artist B", "Artist C"), 1):
            payload = {
                "name": artist,
                "song_name": f"Track {idx}",
                "album_name": "Hot VA </script><img src=x onerror=alert(1)>",
                "date_str": "2026-11-24",
                "link": f"https://music.163.com/#/song?id={100 + idx}",
                "flag": 1,
                "album_id": "5001",
                "album_count": 3,
            }
            db.execute(
                "INSERT INTO hits VALUES(?,?,?)",
                (latest_rid, payload["link"], json.dumps(payload, ensure_ascii=False)),
            )
        cold_payload = {
            "name": "Artist D",
            "song_name": "Solo Song",
            "album_name": "Cold Single",
            "date_str": "2026-11-24",
            "link": "https://music.163.com/#/song?id=104",
            "flag": 0,
            "album_id": "5002",
            "album_count": 1,
        }
        db.execute(
            "INSERT INTO hits VALUES(?,?,?)",
            (latest_rid, cold_payload["link"], json.dumps(cold_payload, ensure_ascii=False)),
        )
    return data


def test_sqlite_compaction_and_pack_unpack_roundtrip(tmp_path):
    data_dir = _make_sample_data_dir(tmp_path, num_m3_reports=15)
    # Also place sensitive/unrelated files that must NEVER be packed.
    (data_dir / ".server.lock").write_text("", encoding="utf-8")
    (data_dir / "VA_Album_Date_Cache.json.tmp").write_text("junk", encoding="utf-8")
    (data_dir / "backups").mkdir()
    (data_dir / "backups" / "old.tar.gz").write_bytes(b"backup")

    out_dir = tmp_path / "packed"
    manifest = state_store.pack_state(data_dir, out_dir, keep_m3_reports=5, asset_name="scanner-state-20261124-120000.tar.gz")
    assert manifest["compaction"]["kept_m3_reports"] == 5
    assert manifest["compaction"]["pruned_m3_reports"] == 10
    assert manifest["stats"]["csv_rows"] == 4
    assert manifest["stats"]["sqlite_runs"] == 15
    assert manifest["stats"]["sqlite_hits"] == 4
    assert manifest["stats"]["sqlite_integrity_ok"] is True

    archive_path = out_dir / manifest["latest_asset"]
    with tarfile.open(archive_path, "r:gz") as tf:
        names = set(tf.getnames())
    assert ".server.lock" not in str(names)
    assert "backups" not in str(names)
    assert "VA_Album_Date_Cache.json.tmp" not in str(names)

    # Unpack into fresh directory and verify compacted DB still retains all runs/hits/logs + non-M3 settings.
    restored_dir = tmp_path / "restored"
    unpack_res = state_store.unpack_state(archive_path, manifest["sha256"], restored_dir)
    assert unpack_res["stats"] == manifest["stats"]
    assert (restored_dir / state_store.BASELINE_FILENAME).is_file()

    with closing(sqlite3.connect(restored_dir / "scan_history.sqlite3")) as db:
        assert db.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert db.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 15
        assert db.execute("SELECT COUNT(*) FROM hits").fetchone()[0] == 4
        assert db.execute("SELECT COUNT(*) FROM logs").fetchone()[0] == 15
        assert db.execute("SELECT value FROM settings WHERE key='backup_date'").fetchone()[0] == "2026-11-24"
        m3_count = db.execute("SELECT COUNT(*) FROM settings WHERE key LIKE 'm3_match_report:%'").fetchone()[0]
        assert m3_count == 5


def test_unpack_rejects_bad_hash_and_unsafe_paths(tmp_path):
    data_dir = _make_sample_data_dir(tmp_path, num_m3_reports=2)
    out_dir = tmp_path / "packed"
    manifest = state_store.pack_state(data_dir, out_dir, asset_name="scanner-state-20261124-120001.tar.gz")
    archive_path = out_dir / manifest["latest_asset"]

    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        state_store.unpack_state(archive_path, "0" * 64, tmp_path / "bad_hash")

    evil_tar = tmp_path / "evil.tar.gz"
    with tarfile.open(evil_tar, "w:gz") as tf:
        info = tarfile.TarInfo("../evil.txt")
        payload = b"pwned"
        info.size = len(payload)
        tf.addfile(info, io.BytesIO(payload))
    with pytest.raises(ValueError, match="Unsafe"):
        state_store.unpack_state(evil_tar, None, tmp_path / "evil_dest")


def test_regression_guard_blocks_shrinking_state(tmp_path):
    data_dir = _make_sample_data_dir(tmp_path, num_m3_reports=3)
    baseline = state_store.validate_data_dir(data_dir)
    # Truncate CSV rows
    with (data_dir / "VA_Tracks_Output.csv").open("w", encoding="utf-8-sig", newline="") as f:
        csv.writer(f).writerow(state_store.CSV_HEADER)
    with pytest.raises(ValueError, match="csv_rows shrank"):
        state_store.validate_data_dir(data_dir, baseline_stats=baseline)


class _MockResponse:
    def __init__(self, status_code=200, json_data=None, content=b""):
        self.status_code = status_code
        self._json_data = json_data
        self._content = content

    def raise_for_status(self):
        if self.status_code >= 400:
            err = requests.HTTPError(f"HTTP {self.status_code}")
            err.response = self
            raise err

    def json(self):
        return self._json_data

    def iter_content(self, chunk_size=1024):
        yield self._content

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class _FakeGitHubSession:
    def __init__(self):
        self.headers = {}
        self.release_created = False
        self.assets = {}
        self.next_id = 1
        self.ops = []

    def request(self, method, url, **kwargs):
        if method == "GET" and url.endswith("/releases/tags/scanner-state"):
            if not self.release_created:
                return _MockResponse(404, {"message": "Not Found"})
            return _MockResponse(200, self._release_payload())
        if method == "POST" and url.endswith("/releases"):
            self.release_created = True
            return _MockResponse(201, self._release_payload())
        if method == "POST" and "assets?name=" in url:
            name = url.split("name=", 1)[1]
            data = kwargs["data"].read()
            self.ops.append(("upload", name, len(data)))
            aid = self.next_id
            self.next_id += 1
            asset = {
                "id": aid,
                "name": name,
                "size": len(data),
                "url": f"https://api.github.com/repos/u/r/releases/assets/{aid}",
                "created_at": f"2026-11-24T00:00:{aid:02d}Z",
                "_bytes": data,
            }
            self.assets[aid] = asset
            return _MockResponse(201, {k: v for k, v in asset.items() if k != "_bytes"})
        if method == "DELETE" and "/releases/assets/" in url:
            aid = int(url.rsplit("/", 1)[1])
            removed = self.assets.pop(aid, None)
            self.ops.append(("delete", removed["name"] if removed else aid))
            return _MockResponse(204)
        if method == "GET" and "/releases/assets/" in url:
            aid = int(url.rsplit("/", 1)[1])
            return _MockResponse(200, content=self.assets[aid]["_bytes"])
        raise AssertionError(f"Unexpected request: {method} {url}")

    def _release_payload(self):
        return {
            "id": 99,
            "tag_name": "scanner-state",
            "upload_url": "https://uploads.github.com/repos/u/r/releases/99/assets{?name,label}",
            "assets": [{k: v for k, v in a.items() if k != "_bytes"} for a in self.assets.values()],
        }


def test_github_release_save_restore_atomic_and_prune(tmp_path):
    fake_sess = _FakeGitHubSession()
    client = state_store.GitHubReleaseClient("u/r", "tok_test", session=fake_sess)

    # Restore on missing release fails closed by default.
    with pytest.raises(RuntimeError, match="not found"):
        client.restore_from_release(tmp_path / "empty_fail", allow_empty=False)
    empty_res = client.restore_from_release(tmp_path / "empty_ok", allow_empty=True)
    assert empty_res["empty"] is True

    # Seed old daily backup & old state archives to verify retention pruning.
    data_dir = _make_sample_data_dir(tmp_path, num_m3_reports=12)
    fake_sess.release_created = True
    old_day = (state_store.now_cn().date() - timedelta(days=20)).isoformat()
    for stale_name in (
        f"backup-{old_day}.tar.gz",
        "scanner-state-20261101-000000.tar.gz",
        "scanner-state-20261102-000000.tar.gz",
        "scanner-state-20261103-000000.tar.gz",
    ):
        aid = fake_sess.next_id
        fake_sess.next_id += 1
        fake_sess.assets[aid] = {
            "id": aid,
            "name": stale_name,
            "size": 10,
            "url": f"https://api.github.com/repos/u/r/releases/assets/{aid}",
            "created_at": "2026-11-01T00:00:00Z",
            "_bytes": b"0123456789",
        }

    save_res = client.save_to_release(data_dir, keep_m3_reports=5, keep_latest=2, daily_retention=14)
    upload_names = [op[1] for op in fake_sess.ops if op[0] == "upload"]
    # Atomic order: archive uploaded BEFORE state-manifest.json!
    assert upload_names[0] == save_res["latest_asset"]
    assert upload_names.index(save_res["latest_asset"]) < upload_names.index("state-manifest.json")
    assert f"backup-{old_day}.tar.gz" in save_res["pruned"]
    remaining_names = {a["name"] for a in fake_sess.assets.values()}
    assert save_res["latest_asset"] in remaining_names
    assert "state-manifest.json" in remaining_names
    assert f"backup-{state_store.now_cn().date().isoformat()}.tar.gz" in remaining_names

    # Restore into brand new directory and verify contents.
    restored = tmp_path / "restored_from_release"
    restore_res = client.restore_from_release(restored, allow_empty=False)
    assert restore_res["restored"] is True
    assert restore_res["stats"]["sqlite_runs"] == 12
    assert (restored / "VA_Tracks_Output.csv").is_file()


def test_ci_scan_execution_scrubbing_and_slot_dedup(tmp_path, monkeypatch):
    data_dir = tmp_path / "ci_data"
    data_dir.mkdir()
    secret_cookie = "MUSIC_U=super_secret_token_999;"
    cfg_path = ci_scan._prepare_temp_config(
        tmp_path / "secrets_tmp",
        raw_cookie=secret_cookie,
        threads=4,
        recent_limit=50,
        owned_limit=50,
        new_album_probe_limit=20,
        artist_cache_key="",
    )

    monkeypatch.setattr(engine, "SCRIPT_DIR", data_dir)
    monkeypatch.setattr(engine, "CSV_FILE", data_dir / "VA_Tracks_Output.csv")
    monkeypatch.setattr(engine, "ALBUM_CACHE_FILE", data_dir / "VA_Album_Date_Cache.json")
    monkeypatch.setattr(engine, "ARTIST_CACHE_DIR", data_dir / "VA_Artist_Caches")
    (data_dir / "VA_Artist_Caches").mkdir(exist_ok=True)
    monkeypatch.setattr(server, "DATA", data_dir)
    monkeypatch.setattr(server, "verify_login", lambda *a, **kw: None)

    calls = []

    def fake_worker(cookie, source_input, *args, **kwargs):
        calls.append(cookie)
        engine.init_csv_and_seen()
        hit = {
            "name": "CI Artist",
            "song_name": "CI Song",
            "album_name": "CI Album",
            "date_str": "2026-11-24",
            "link": "https://music.163.com/#/song?id=777",
            "flag": 1,
            "album_id": "888",
            "album_count": 4,
        }
        engine.write_hit_if_new(hit)
        engine.emit({"type": "hit", "data": hit})
        engine.emit({"type": "log", "level": "error", "text": f"upstream leak {secret_cookie}"})
        engine.emit({
            "type": "done",
            "results": [hit],
            "summary": {"artists": 1, "unique_albums": 1, "artist_failures": 0, "new_album_probe_failures": 0, "album_errors": 0, "owned_errors": 0},
            "metrics": {"elapsed_sec": 12.0, "results": 1, "csv_skipped": 0},
        })

    runtime = server.Runtime(config=server.Config(cfg_path), db_path=data_dir / "scan_history.sqlite3", worker=fake_worker)
    try:
        out_dir = tmp_path / "artifacts"
        args = ci_scan._parse_args([
            "--data-dir", str(data_dir),
            "--source", "scheduled",
            "--schedule-slot", "00:03",
            "--skip-if-slot-exists",
            "--scan-mode", "regular",
            "--threads", "4",
            "--result-json", str(out_dir / "run_result.json"),
            "--today-csv", str(out_dir / "today.csv"),
            "--run-csv", str(out_dir / "run.csv"),
            "--desktop-zip", str(out_dir / "records-export.zip"),
            "--summary-md", str(out_dir / "summary.md"),
        ])
        rc = ci_scan.execute_scan(runtime, args, data_dir)
        assert rc == 0
        assert len(calls) == 1

        res = json.loads((out_dir / "run_result.json").read_text(encoding="utf-8"))
        assert res["status"] == "success"
        assert res["result_count"] == 1
        assert res["skipped"] is False
        assert (out_dir / "today.csv").is_file()
        assert (out_dir / "run.csv").is_file()
        assert (out_dir / "records-export.zip").is_file()
        summary_text = (out_dir / "summary.md").read_text(encoding="utf-8")
        assert "super_secret_token_999" not in summary_text
        assert "CI Album" in summary_text
        assert "🔥 4" in summary_text

        # Second scheduled invocation on the same day must skip without calling worker.
        args2 = ci_scan._parse_args([
            "--data-dir", str(data_dir),
            "--source", "scheduled",
            "--schedule-slot", "00:03",
            "--skip-if-slot-exists",
            "--result-json", str(out_dir / "run_result2.json"),
            "--summary-md", str(out_dir / "summary2.md"),
        ])
        rc2 = ci_scan.execute_scan(runtime, args2, data_dir)
        assert rc2 == 0
        assert len(calls) == 1
        res2 = json.loads((out_dir / "run_result2.json").read_text(encoding="utf-8"))
        assert res2["skipped"] is True
        assert res2["status"] == "skipped"
    finally:
        runtime.close()


def test_ci_scan_partial_and_failed_exit_codes(tmp_path, monkeypatch):
    data_dir = tmp_path / "ci_fail"
    data_dir.mkdir()
    cfg_path = ci_scan._prepare_temp_config(
        tmp_path / "sec_fail",
        raw_cookie="MUSIC_U=tok_abc;",
        threads=2,
        recent_limit=50,
        owned_limit=50,
        new_album_probe_limit=20,
        artist_cache_key="",
    )
    monkeypatch.setattr(engine, "SCRIPT_DIR", data_dir)
    monkeypatch.setattr(engine, "CSV_FILE", data_dir / "VA_Tracks_Output.csv")
    monkeypatch.setattr(server, "DATA", data_dir)
    monkeypatch.setattr(server, "verify_login", lambda *a, **kw: None)

    def partial_worker(*a, **kw):
        engine.emit({
            "type": "done",
            "results": [],
            "summary": {"artists": 2, "album_errors": 1},
            "metrics": {"elapsed_sec": 3.0},
        })

    def fail_worker(*a, **kw):
        raise RuntimeError("boom")

    rt1 = server.Runtime(config=server.Config(cfg_path), db_path=data_dir / "scan_history.sqlite3", worker=partial_worker)
    try:
        args_partial = ci_scan._parse_args([
            "--data-dir", str(data_dir),
            "--result-json", str(tmp_path / "p.json"),
        ])
        assert ci_scan.execute_scan(rt1, args_partial, data_dir) == 0
        assert json.loads((tmp_path / "p.json").read_text(encoding="utf-8"))["status"] == "partial"
    finally:
        rt1.close()

    rt2 = server.Runtime(config=server.Config(cfg_path), db_path=data_dir / "scan_history.sqlite3", worker=fail_worker)
    try:
        args_fail = ci_scan._parse_args([
            "--data-dir", str(data_dir),
            "--result-json", str(tmp_path / "f1.json"),
            "--fail-on-error",
        ])
        assert ci_scan.execute_scan(rt2, args_fail, data_dir) == 2

        args_nofail = ci_scan._parse_args([
            "--data-dir", str(data_dir),
            "--result-json", str(tmp_path / "f2.json"),
            "--no-fail-on-error",
        ])
        assert ci_scan.execute_scan(rt2, args_nofail, data_dir) == 0
        assert json.loads((tmp_path / "f2.json").read_text(encoding="utf-8"))["status"] == "failed"
    finally:
        rt2.close()


def test_build_site_and_js_syntax(tmp_path):
    data_dir = _make_sample_data_dir(tmp_path, num_m3_reports=5)
    out_dir = tmp_path / "site"
    standalone = tmp_path / "latest-report.html"
    summary = build_site.build_site(data_dir, out_dir, standalone_path=standalone)

    assert summary["days_count"] == 5
    assert summary["runs_count"] == 5
    manifest = json.loads((out_dir / "data" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["latest_day"] == manifest["days"][0]["date"]
    assert manifest["legacy_stats"]["total_tracks"] == 4

    # Latest run had Hot VA (heat 3) before Cold Single (heat 1).
    latest_run_id = manifest["runs"][0]["id"]
    run_doc = json.loads((out_dir / "data" / "runs" / f"{latest_run_id}.json").read_text(encoding="utf-8"))
    assert run_doc["albums"][0]["album_id"] == "5001"
    assert run_doc["albums"][0]["heat"] == 3
    assert run_doc["albums"][1]["album_id"] == "5002"

    # Standalone HTML safely escapes </script> inside embedded JSON.
    standalone_html = standalone.read_text(encoding="utf-8")
    assert "</script><img" not in standalone_html
    assert "\\u003c/script>" in standalone_html

    # Validate inline script syntax using Node.
    html = (out_dir / "index.html").read_text(encoding="utf-8")
    matches = re.findall(r"<script>(.*?)</script>", html, flags=re.DOTALL)
    assert len(matches) == 1
    js_tmp = tmp_path / "viewer_check.mjs"
    js_tmp.write_text(matches[0], encoding="utf-8")
    proc = subprocess.run(["node", "--check", str(js_tmp)], capture_output=True, text=True, check=False)
    assert proc.returncode == 0, proc.stderr


def test_ci_scan_probe_only_scrubs_cookie(tmp_path, monkeypatch):
    import probe as probe_mod

    secret = "MUSIC_U=probe_secret_42;"
    monkeypatch.setenv("MUSIC_COOKIE", secret)
    monkeypatch.setattr(
        probe_mod,
        "probe",
        lambda **kw: {
            "ok": False,
            "elapsed_sec": 0.2,
            "artist_count": 0,
            "checks": [],
            "error": f"bad auth {secret}",
        },
    )
    out = tmp_path / "probe_out"
    rc = ci_scan.main([
        "--data-dir", str(tmp_path / "pdata"),
        "--probe-only",
        "--result-json", str(out / "probe.json"),
        "--summary-md", str(out / "probe.md"),
    ])
    assert rc == 1
    assert "probe_secret_42" not in (out / "probe.json").read_text(encoding="utf-8")
    assert "probe_secret_42" not in (out / "probe.md").read_text(encoding="utf-8")


def test_scan_workflow_respects_github_limits_and_shell_safety():
    workflow_path = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "scan.yml"
    text = workflow_path.read_text(encoding="utf-8")

    # GitHub Actions allows at most 10 workflow_dispatch inputs.
    m = re.search(r"workflow_dispatch:\s*\n\s*inputs:\s*\n(.*?)\npermissions:", text, flags=re.DOTALL)
    assert m is not None
    inputs_block = m.group(1)
    input_names = re.findall(r"^      ([a-z0-9_]+):\s*$", inputs_block, flags=re.MULTILINE)
    assert 1 <= len(input_names) <= 10, f"Found {len(input_names)} dispatch inputs: {input_names}"

    # Concurrency group must prevent parallel state writes without cancelling active scans.
    assert "group: netease-scanner-state" in text
    assert "cancel-in-progress: false" in text

    # No raw ${{ inputs.* }} interpolation inside bash run: blocks.
    run_blocks = re.findall(r"run:\s*\|\s*\n((?:        .*\n)+)", text)
    assert run_blocks
    for block in run_blocks:
        assert "${{ inputs." not in block

    # Transport overrides must be wired for GitHub Actions runners.
    assert "NETEASE_REAL_IP:" in text
    assert "NETEASE_API_CRYPTO:" in text
    assert "NETEASE_API_PROXY:" in text


def test_engine_transport_overrides_and_proxy_redaction(monkeypatch):
    monkeypatch.delenv("NETEASE_REAL_IP", raising=False)
    monkeypatch.delenv("NETEASE_API_CRYPTO", raising=False)
    monkeypatch.delenv("NETEASE_API_PROXY", raising=False)
    monkeypatch.setattr(engine, "_AUTO_REAL_IP", None)
    assert engine.api_transport_overrides() == {}

    monkeypatch.setenv("NETEASE_REAL_IP", "auto")
    monkeypatch.setenv("NETEASE_API_CRYPTO", "eapi")
    monkeypatch.setenv("NETEASE_API_PROXY", "http://user:s3cr3t@1.2.3.4:8080")
    ov1 = engine.api_transport_overrides()
    ov2 = engine.api_transport_overrides()
    assert ov1["realIP"].startswith("116.")
    assert ov1["realIP"] == ov2["realIP"]
    assert ov1["crypto"] == "eapi"
    assert ov1["proxy"] == "http://user:s3cr3t@1.2.3.4:8080"

    monkeypatch.setenv("NETEASE_REAL_IP", "off")
    monkeypatch.setenv("NETEASE_API_CRYPTO", "none")
    monkeypatch.setenv("NETEASE_API_PROXY", "off")
    assert engine.api_transport_overrides() == {}

    scrubbed = engine.redacted_error_text(
        "failed http://alice:topsecret@proxy.example:1080/test?proxy=http://a:b@c&cookie=MUSIC_U=abc123;"
    )
    assert "topsecret" not in scrubbed
    assert "a:b" not in scrubbed
    assert "abc123" not in scrubbed
    assert "http://<redacted>@proxy.example:1080" in scrubbed


def test_verify_login_forwards_transport_overrides(monkeypatch):
    monkeypatch.setenv("NETEASE_REAL_IP", "116.25.10.20")
    monkeypatch.setenv("NETEASE_API_CRYPTO", "eapi")
    monkeypatch.setenv("NETEASE_API_PROXY", "http://127.0.0.1:9000")

    captured = {}

    class _Resp:
        def raise_for_status(self):
            pass

        def json(self):
            return {"data": {"code": 200, "account": {"id": 42}}}

    def fake_post(url, data=None, timeout=None):
        captured["url"] = url
        captured["data"] = dict(data)
        return _Resp()

    monkeypatch.setattr(server.requests, "post", fake_post)
    server.verify_login("MUSIC_U=tok;", "http://127.0.0.1:3000")
    assert captured["data"]["realIP"] == "116.25.10.20"
    assert captured["data"]["crypto"] == "eapi"
    assert captured["data"]["proxy"] == "http://127.0.0.1:9000"


def test_probe_405_details_and_diagnostic_matrix(tmp_path, monkeypatch):
    import probe as probe_mod

    empty_cache = tmp_path / "empty_cache"
    empty_cache.mkdir()
    monkeypatch.setattr(engine, "ARTIST_CACHE_DIR", empty_cache)

    cfg_path = tmp_path / "cfg.json"
    cookie_path = tmp_path / "cookie.txt"
    cookie_path.write_text("MUSIC_U=probe_tok_secret;", encoding="utf-8")
    cfg_path.write_text(json.dumps({"cookie_file": str(cookie_path)}), encoding="utf-8")

    monkeypatch.setenv("NETEASE_REAL_IP", "116.25.146.177")
    monkeypatch.setenv("NETEASE_API_CRYPTO", "eapi")
    monkeypatch.setenv("NETEASE_API_PROXY", "http://u:pw@9.9.9.9:8080")
    monkeypatch.setattr(probe_mod, "verify_login", lambda *a, **kw: None)

    seen_calls = []

    class _FakeHttpResp:
        def __init__(self, status_code, body):
            self.status_code = status_code
            self.ok = 200 <= status_code < 300
            self._body = body

        def raise_for_status(self):
            if self.status_code >= 400:
                raise requests.HTTPError(f"HTTP {self.status_code}")

        def json(self):
            return self._body

    def fake_request(method, url, data=None, params=None, timeout=None):
        payload = data if method == "POST" else params
        seen_calls.append((method, url, dict(payload or {})))
        if url.endswith("/artist/sublist"):
            if payload.get("realIP") and payload.get("crypto") == "eapi":
                # Simulate diagnostic realIP+eapi succeeding while primary probe saw 405
                if len(seen_calls) <= 2:
                    return _FakeHttpResp(405, {"code": 405, "message": "cheating MUSIC_U=probe_tok_secret"})
                return _FakeHttpResp(200, {"code": 200, "data": [{"id": 1, "name": "A"}]})
            return _FakeHttpResp(405, {"code": 405, "message": "blocked"})
        if url.endswith("/artist/songs"):
            return _FakeHttpResp(200, {"code": 200, "songs": [{"al": {"id": 555}}]})
        if url.endswith("/artist/album"):
            return _FakeHttpResp(200, {"code": 200, "hotAlbums": [{"id": 555}]})
        if url.endswith("/album"):
            return _FakeHttpResp(200, {"code": 200, "album": {"id": 555}})
        raise AssertionError(url)

    monkeypatch.setattr(probe_mod.requests, "request", fake_request)
    report = probe_mod.probe(config=server.Config(cfg_path), artist_count=3, api_base="http://127.0.0.1:3000")

    assert report["ok"] is False
    assert "probe_tok_secret" not in report["error"]
    assert "cheating" in report["error"]
    assert report["transport"] == {
        "realIP": "116.25.146.177",
        "crypto": "eapi",
        "proxy_configured": True,
    }
    assert any(
        d["endpoint"] == "/artist/sublist" and d["mode"] == "realIP+eapi" and d["ok"] is True
        for d in report["diagnostics"]
    )
    assert seen_calls[0][2]["realIP"] == "116.25.146.177"
    assert seen_calls[0][2]["crypto"] == "eapi"
    assert seen_calls[0][2]["proxy"] == "http://u:pw@9.9.9.9:8080"

    md = engine.redacted_error_text(ci_scan._build_probe_summary_md(report))
    assert "传输模式诊断矩阵" in md
    assert "realIP+eapi" in md
    assert "代理：`已配置`" in md
    assert "proxy=<redacted>" not in md
    assert "probe_tok_secret" not in md
    assert "pw@" not in md


def test_probe_falls_back_to_cached_artists_on_sublist_405(tmp_path, monkeypatch):
    import probe as probe_mod

    cache_dir = tmp_path / "caches"
    cache_dir.mkdir()
    monkeypatch.setattr(engine, "ARTIST_CACHE_DIR", cache_dir)

    cookie = "MUSIC_U=fallback_tok_888;"
    fp = engine.cookie_fingerprint(cookie)
    engine.save_artist_cache(fp, [{"id": "701", "name": "CachedArtist"}], {"source": "test"})

    cfg_path = tmp_path / "cfg.json"
    cookie_path = tmp_path / "cookie.txt"
    cookie_path.write_text(cookie, encoding="utf-8")
    cfg_path.write_text(json.dumps({"cookie_file": str(cookie_path)}), encoding="utf-8")

    monkeypatch.setenv("NETEASE_REAL_IP", "116.25.146.177")
    monkeypatch.delenv("NETEASE_API_CRYPTO", raising=False)
    monkeypatch.delenv("NETEASE_API_PROXY", raising=False)
    monkeypatch.setattr(probe_mod, "verify_login", lambda *a, **kw: None)

    class _FakeResp:
        def __init__(self, status_code, body):
            self.status_code = status_code
            self.ok = 200 <= status_code < 300
            self._body = body

        def raise_for_status(self):
            if self.status_code >= 400:
                raise requests.HTTPError(f"HTTP {self.status_code}")

        def json(self):
            return self._body

    def fake_request(method, url, data=None, params=None, timeout=None):
        if url.endswith("/artist/sublist"):
            return _FakeResp(405, {"code": 405, "message": "操作频繁"})
        if url.endswith("/artist/songs"):
            return _FakeResp(200, {"code": 200, "songs": [{"al": {"id": 9901}}]})
        if url.endswith("/artist/album"):
            return _FakeResp(200, {"code": 200, "hotAlbums": [{"id": 9901}]})
        if url.endswith("/album"):
            return _FakeResp(200, {"code": 200, "album": {"id": 9901}})
        raise AssertionError(url)

    monkeypatch.setattr(probe_mod.requests, "request", fake_request)
    report = probe_mod.probe(config=server.Config(cfg_path), artist_count=3, api_base="http://127.0.0.1:3000")

    assert report["ok"] is True
    assert report["degraded"] is True
    assert report["artist_source"] == "fallback-cache"
    assert report["artist_cache_key"] == fp
    assert report["artist_count"] == 1
    assert report["diagnostics"] == []
    assert any(c["endpoint"] == "/album" and c["ok"] for c in report["checks"])

    md = engine.redacted_error_text(ci_scan._build_probe_summary_md(report))
    assert "降级：使用艺人缓存" in md
    assert "代理：`未配置`" in md
    assert "fallback_tok_888" not in md


