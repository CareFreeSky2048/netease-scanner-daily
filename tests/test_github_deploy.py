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
import time
from contextlib import closing
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest
import requests

import build_site
import ci_scan
import engine
import patch_api_keepalive
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
    assert "test_fresh_day" in inputs_block

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

    # Dry-run test_fresh_day must NEVER save state or publish to site branch, while keeping status gate.
    save_match = re.search(r"- name: Save compacted state to GitHub Release\s*\n(.*?)(?=\n      - name: |\Z)", text, flags=re.DOTALL)
    assert save_match is not None
    assert "steps.ctx.outputs.action == 'scan'" in save_match.group(1)
    assert "test_fresh_day" not in save_match.group(1)

    publish_match = re.search(r"- name: Publish squashed static viewer to site branch\s*\n(.*?)(?=\n      - name: |\Z)", text, flags=re.DOTALL)
    assert publish_match is not None
    assert "steps.ctx.outputs.action == 'scan' || steps.ctx.outputs.action == 'rebuild_site'" in publish_match.group(1)
    assert "!= 'probe'" not in publish_match.group(1)

    gate_match = re.search(r"- name: Enforce scan status gate\s*\n(.*?)(?=\n      - name: |\Z)", text, flags=re.DOTALL)
    assert gate_match is not None
    assert "steps.ctx.outputs.action == 'test_fresh_day'" in gate_match.group(1)

    # First early-morning slot (00:03) skips new-album directory probe for speed; second slot (04:00) enables it.
    assert 'PROBE_LIMIT="${NEW_ALBUM_PROBE_LIMIT:-20}"' in text
    assert 'PROBE_LIMIT="0"' in text
    assert 'ARGS+=(--new-album-probe-limit "$PROBE_LIMIT")' in text
    slot_block = re.search(
        r'ARGS\+=\(--source scheduled --schedule-slot 04:00 --skip-if-slot-exists\)\s*\n\s*else\s*\n(.*?)\n\s*fi',
        text,
        flags=re.DOTALL,
    )
    assert slot_block is not None
    assert '--schedule-slot 00:03' in slot_block.group(1)
    assert 'PROBE_LIMIT="0"' in slot_block.group(1)
    assert 'SLOW_RETRY_ROUNDS="1"' in slot_block.group(1)

    # External accurate trigger via repository_dispatch must route scan_0003 / scan_0400 cleanly.
    assert "repository_dispatch:" in text
    assert "- scan_0003" in text
    assert "- scan_0400" in text
    assert "EVENT_ACTION: ${{ github.event.action }}" in text
    dispatch_block = re.search(
        r'elif \[ "\$EVENT_NAME" = "repository_dispatch" \]; then\s*\n(.*?)\n\s*else\s*\n\s*ARGS\+=\(--source manual\)',
        text,
        flags=re.DOTALL,
    )
    assert dispatch_block is not None
    assert 'scan_0400' in dispatch_block.group(1)
    assert '--schedule-slot 04:00' in dispatch_block.group(1)
    assert '--schedule-slot 00:03' in dispatch_block.group(1)
    assert 'PROBE_LIMIT="0"' in dispatch_block.group(1)
    assert 'SLOW_RETRY_ROUNDS="1"' in dispatch_block.group(1)
    assert '--slow-retry-rounds "$SLOW_RETRY_ROUNDS"' in text

    # Keep-Alive patcher must run right before starting NeteaseCloudMusicApi.
    start_match = re.search(
        r"- name: Start NeteaseCloudMusicApi service\s*\n(.*?)(?=\n      - name: |\Z)",
        text,
        flags=re.DOTALL,
    )
    assert start_match is not None
    assert "patch_api_keepalive.py" in start_match.group(1)
    assert start_match.group(1).index("patch_api_keepalive.py") < start_match.group(1).index("NeteaseCloudMusicApi/app.js")

    # Email notification step must tolerate missing secrets/SMTP errors cleanly without blocking status gate.
    mail_match = re.search(
        r"- name: Send email notification\s*\n(.*?)(?=\n      - name: |\Z)",
        text,
        flags=re.DOTALL,
    )
    assert mail_match is not None
    assert "notify_email.py" in mail_match.group(1)
    assert "continue-on-error: true" in mail_match.group(1)
    assert "SMTP_USER:" in mail_match.group(1)
    assert "SMTP_PASS:" in mail_match.group(1)
    assert "MAIL_TO:" in mail_match.group(1)
    assert "SITE_URL:" in mail_match.group(1)


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
    monkeypatch.setattr(probe_mod.time, "sleep", lambda *a, **kw: None)

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

    def fake_request(method, url, data=None, params=None, headers=None, proxies=None, timeout=None):
        payload = data if method == "POST" else params
        seen_calls.append((method, url, dict(payload or {})))
        if url == "https://music.163.com/api/artist/sublist":
            return _FakeHttpResp(405, {"code": 405, "message": "blocked"})
        if url.endswith("/artist/sublist"):
            if payload.get("realIP") and payload.get("crypto") == "eapi":
                # Simulate diagnostic realIP+eapi succeeding while primary probe saw 405
                if len(seen_calls) <= 2:
                    return _FakeHttpResp(405, {"code": 405, "message": "cheating MUSIC_U=probe_tok_secret"})
                return _FakeHttpResp(200, {"code": 200, "count": 4700, "data": [{"id": 1, "name": "A"}]})
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
    diag_modes = {d["mode"] for d in report["diagnostics"] if d["endpoint"] == "/artist/sublist"}
    assert {
        "default",
        "realIP",
        "eapi",
        "realIP+eapi",
        "api",
        "realIP+api",
        "linuxapi",
        "realIP+linuxapi",
        "eapi+iphone+er+ct",
        "eapi+android+er+ct",
        "eapi+pc+er+ct",
        "direct-music-api",
    } <= diag_modes
    real_eapi = next(d for d in report["diagnostics"] if d["endpoint"] == "/artist/sublist" and d["mode"] == "realIP+eapi")
    assert real_eapi["ok"] is True
    assert "返回 1 位" in real_eapi["detail"]
    assert seen_calls[0][2]["realIP"] == "116.25.146.177"
    assert seen_calls[0][2]["crypto"] == "eapi"
    assert seen_calls[0][2]["proxy"] == "http://u:pw@9.9.9.9:8080"

    md = engine.redacted_error_text(ci_scan._build_probe_summary_md(report))
    assert "传输模式诊断矩阵" in md
    assert "realIP+eapi" in md
    assert "linuxapi" in md
    assert "direct-music-api" in md
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
    monkeypatch.setattr(probe_mod.time, "sleep", lambda *a, **kw: None)

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

    def fake_request(method, url, data=None, params=None, headers=None, proxies=None, timeout=None):
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
    assert len(report["diagnostics"]) >= 12
    assert all(d["endpoint"] == "/artist/sublist" for d in report["diagnostics"])
    diag_modes = {d["mode"] for d in report["diagnostics"]}
    assert {"api", "linuxapi", "eapi+iphone+er+ct", "direct-music-api"} <= diag_modes
    assert any(c["endpoint"] == "/album" and c["ok"] for c in report["checks"])

    md = engine.redacted_error_text(ci_scan._build_probe_summary_md(report))
    assert "降级：使用艺人缓存" in md
    assert "传输模式诊断矩阵" in md
    assert "linuxapi" in md
    assert "代理：`未配置`" in md
    assert "fallback_tok_888" not in md


def test_build_new_album_relations_no_duplicate_placeholder():
    candidate = {"artist_id": "10", "artist_name": "Artist X", "album_id": "200", "album_name": "Album Z"}
    detail_with_song = {"album_id": "200", "album_name": "Album Z", "songs": [{"id": "555", "name": "Real Song"}]}

    # Already represented via /artist/songs -> must NOT emit 【新专辑】 placeholder.
    existing = {("200", "10", "555")}
    assert engine.build_new_album_relations(candidate, detail_with_song, existing) == []

    # Not in /artist/songs and /album has no songs -> SHOULD still emit fallback placeholder.
    detail_empty = {"album_id": "201", "album_name": "Empty Pre", "songs": []}
    cand_empty = {"artist_id": "10", "artist_name": "Artist X", "album_id": "201", "album_name": "Empty Pre"}
    rels = engine.build_new_album_relations(cand_empty, detail_empty, set())
    assert len(rels) == 1
    assert rels[0]["song_name"] == "【新专辑】Empty Pre"
    assert rels[0]["link"] == "https://music.163.com/#/album?id=201"


def test_scan_summary_lists_all_albums_with_artists_only_and_dedup_placeholders():
    hits = []
    # Build 35 distinct albums (>30!) plus a redundant 【新专辑】 placeholder on album 1.
    for idx in range(1, 36):
        hits.append({
            "name": f"Artist {idx}",
            "song_name": f"SongTitle_{idx}",
            "album_name": f"Album_{idx}",
            "date_str": "2026-11-24",
            "link": f"https://music.163.com/#/song?id={1000 + idx}",
            "album_id": str(2000 + idx),
            "album_count": 2,
        })
    # Duplicate artist on Album_1 + redundant placeholder on Album_1
    hits.append({
        "name": "Artist 1",
        "song_name": "SecondTrack_1",
        "album_name": "Album_1",
        "date_str": "2026-11-24",
        "link": "https://music.163.com/#/song?id=1998",
        "album_id": "2001",
        "album_count": 2,
    })
    hits.append({
        "name": "Artist 1",
        "song_name": "【新专辑】Album_1",
        "album_name": "Album_1",
        "date_str": "2026-11-24",
        "link": "https://music.163.com/#/album?id=2001",
        "album_id": "2001",
        "album_count": 2,
    })
    # Songless album keeps its placeholder
    hits.append({
        "name": "Artist Solo",
        "song_name": "【新专辑】Songless",
        "album_name": "Songless",
        "date_str": "2026-11-24",
        "link": "https://music.163.com/#/album?id=9999",
        "album_id": "9999",
        "album_count": 1,
    })

    run = {"id": "r1", "status": "success", "source": "manual", "params": {"scanMode": "regular", "threads": 8}, "summary": {}, "metrics": {}}
    md = ci_scan._build_scan_summary_md(run, "2026-11-24", hits, hits, lambda s: s)
    assert "本次新增专辑（共 36 张）" in md
    assert "Top 30" not in md
    assert "| 热度 | 专辑 / 企划 | 发行日期 | 命中艺人 |" in md
    assert "示例" not in md
    assert "SongTitle_" not in md
    for idx in range(1, 36):
        assert f"[Album_{idx}]" in md
    assert "[Songless]" in md
    # Artist 1 deduplicated on Album_1 row
    album1_line = next(line for line in md.splitlines() if "[Album_1]" in line)
    assert album1_line.endswith("| Artist 1 |")


def test_build_site_deduplicates_placeholders_and_includes_artists():
    rows = [
        ("https://music.163.com/#/song?id=10", {"name": "Alice", "song_name": "Real1", "album_name": "Mix", "date_str": "2026-11-24", "album_id": "500", "album_count": 2}),
        ("https://music.163.com/#/song?id=11", {"name": "Bob", "song_name": "Real2", "album_name": "Mix", "date_str": "2026-11-24", "album_id": "500", "album_count": 2}),
        ("https://music.163.com/#/album?id=500", {"name": "Alice", "song_name": "【新专辑】Mix", "album_name": "Mix", "date_str": "2026-11-24", "album_id": "500", "album_count": 2}),
        ("https://music.163.com/#/album?id=501", {"name": "Solo", "song_name": "【新专辑】EmptyOnly", "album_name": "EmptyOnly", "date_str": "2026-11-24", "album_id": "501", "album_count": 1}),
    ]
    albums = build_site._group_hits_to_albums(rows)
    by_id = {a["album_id"]: a for a in albums}
    assert by_id["500"]["track_count"] == 2
    assert all(not t["song_name"].startswith("【新专辑】") for t in by_id["500"]["tracks"])
    assert by_id["500"]["artists"] == ["Alice", "Bob"]
    assert by_id["501"]["track_count"] == 1
    assert by_id["501"]["artists"] == ["Solo"]


def test_rollback_today_state_isolates_only_today_and_preserves_history(tmp_path):
    data_dir = tmp_path / "data"
    cache_dir = data_dir / "VA_Artist_Caches"
    cache_dir.mkdir(parents=True)
    artist_cache_file = cache_dir / "artists_92c430701d65a19f.json"
    artist_payload = json.dumps({"count": 2, "artists": [{"id": "1", "name": "A"}, {"id": "2", "name": "B"}]})
    artist_cache_file.write_text(artist_payload, encoding="utf-8")

    csv_file = data_dir / "VA_Tracks_Output.csv"
    with csv_file.open("w", encoding="utf-8-sig", newline="") as handle:
        w = csv.writer(handle)
        w.writerow(state_store.CSV_HEADER)
        w.writerow(["Artist Old", "Old Track", "Old Album", "2026-11-23", "https://music.163.com/#/song?id=101", "0"])
        w.writerow(["Artist S", "Today Sched Track", "Album S", "2026-11-24", "https://music.163.com/#/song?id=201", "1"])
        w.writerow(["Artist M", "Today Manual Track", "Album M", "2026-11-24", "https://music.163.com/#/song?id=202", "1"])

    yest_ts = datetime(2026, 11, 23, 23, 59, 59, tzinfo=ci_scan.TZ).timestamp()
    today_ts = datetime(2026, 11, 24, 0, 5, 0, tzinfo=ci_scan.TZ).timestamp()
    album_cache_file = data_dir / "VA_Album_Date_Cache.json"
    album_cache_file.write_text(
        json.dumps({
            "1001": {"album_id": "1001", "album_name": "Old Album", "cached_at": yest_ts},
            "1002": {"album_id": "1002", "album_name": "Today Album", "cached_at": today_ts},
            "1003": {"album_id": "1003", "album_name": "Legacy Entry"},
        }),
        encoding="utf-8",
    )

    store = server.Store(data_dir / "scan_history.sqlite3")
    store.set_value("backup_date", "2026-11-24")
    with store.connect() as db:
        runs = [
            ("run_yest", "scheduled", "2026-11-23", "00:03", "2026-11-23T00:03:00+08:00", "https://music.163.com/#/song?id=101"),
            ("run_today_sched", "scheduled", "2026-11-24", "00:03", "2026-11-24T00:03:00+08:00", "https://music.163.com/#/song?id=201"),
            ("run_today_man", "manual", None, None, "2026-11-24T10:00:00+08:00", "https://music.163.com/#/song?id=202"),
        ]
        for rid, src, sdate, stime, cat, link in runs:
            db.execute(
                "INSERT INTO runs(id,source,schedule_date,schedule_time,status,created_at,params) VALUES(?,?,?,?,?,?,?)",
                (rid, src, sdate, stime, "success", cat, "{}"),
            )
            db.execute("INSERT INTO hits VALUES(?,?,?)", (rid, link, json.dumps({"link": link})))
            db.execute("INSERT INTO logs(run_id,created_at,level,text) VALUES(?,?,?,?)", (rid, cat, "info", f"log {rid}"))
            db.execute("INSERT INTO settings(key,value) VALUES(?,?)", (f"m3_match_report:{rid}", "{}"))

    res = ci_scan.rollback_today_state(data_dir, "2026-11-24")
    assert res == {
        "date": "2026-11-24",
        "removed_runs": 2,
        "removed_hits": 2,
        "removed_logs": 2,
        "removed_m3_reports": 2,
        "removed_csv_rows": 2,
        "evicted_album_cache": 1,
    }

    with closing(sqlite3.connect(data_dir / "scan_history.sqlite3")) as db:
        assert [r[0] for r in db.execute("SELECT id FROM runs").fetchall()] == ["run_yest"]
        assert [r[0] for r in db.execute("SELECT link FROM hits").fetchall()] == ["https://music.163.com/#/song?id=101"]
        assert [r[0] for r in db.execute("SELECT run_id FROM logs").fetchall()] == ["run_yest"]
        keys = {r[0] for r in db.execute("SELECT key FROM settings").fetchall()}
        assert keys == {"backup_date", "m3_match_report:run_yest"}

    with csv_file.open("r", encoding="utf-8-sig", newline="") as handle:
        csv_rows = list(csv.reader(handle))
    assert len(csv_rows) == 2
    assert csv_rows[1][4] == "https://music.163.com/#/song?id=101"

    album_cache_after = json.loads(album_cache_file.read_text(encoding="utf-8"))
    assert set(album_cache_after.keys()) == {"1001", "1003"}
    assert artist_cache_file.read_text(encoding="utf-8") == artist_payload

    # Idempotent on repeated invocation
    res2 = ci_scan.rollback_today_state(data_dir, "2026-11-24")
    assert res2 == {
        "date": "2026-11-24",
        "removed_runs": 0,
        "removed_hits": 0,
        "removed_logs": 0,
        "removed_m3_reports": 0,
        "removed_csv_rows": 0,
        "evicted_album_cache": 0,
    }


def test_ci_scan_fresh_today_dry_run_end_to_end(tmp_path, monkeypatch):
    data_dir = tmp_path / "fresh_data"
    (data_dir / "VA_Artist_Caches").mkdir(parents=True)
    cfg_path = ci_scan._prepare_temp_config(
        tmp_path / "sec_fresh",
        raw_cookie="MUSIC_U=dryrun_cookie_123;",
        threads=4,
        recent_limit=50,
        owned_limit=50,
        new_album_probe_limit=20,
        artist_cache_key="",
    )

    today_dt = datetime.now(ci_scan.TZ).replace(hour=1, minute=0, second=0, microsecond=0)
    today_str = today_dt.date().isoformat()
    yest_dt = today_dt - timedelta(days=1)
    yest_str = yest_dt.date().isoformat()

    monkeypatch.setattr(engine, "SCRIPT_DIR", data_dir)
    monkeypatch.setattr(engine, "CSV_FILE", data_dir / "VA_Tracks_Output.csv")
    monkeypatch.setattr(engine, "ALBUM_CACHE_FILE", data_dir / "VA_Album_Date_Cache.json")
    monkeypatch.setattr(engine, "ARTIST_CACHE_DIR", data_dir / "VA_Artist_Caches")
    monkeypatch.setattr(server, "DATA", data_dir)
    monkeypatch.setattr(server, "verify_login", lambda *a, **kw: None)

    yest_hit = {
        "name": "Yest Artist",
        "song_name": "Yest Song",
        "album_name": "Yest Album",
        "date_str": yest_str,
        "link": "https://music.163.com/#/song?id=100",
        "flag": 0,
        "album_id": "100",
        "album_count": 1,
    }
    today_hit = {
        "name": "Fresh Artist",
        "song_name": "Fresh Song",
        "album_name": "Fresh Album",
        "date_str": today_str,
        "link": "https://music.163.com/#/song?id=777",
        "flag": 1,
        "album_id": "777",
        "album_count": 3,
    }

    with (data_dir / "VA_Tracks_Output.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        w = csv.writer(handle)
        w.writerow(state_store.CSV_HEADER)
        w.writerow([yest_hit["name"], yest_hit["song_name"], yest_hit["album_name"], yest_hit["date_str"], yest_hit["link"], "0"])
        w.writerow([today_hit["name"], today_hit["song_name"], today_hit["album_name"], today_hit["date_str"], today_hit["link"], "1"])

    (data_dir / "VA_Album_Date_Cache.json").write_text(
        json.dumps({
            "100": {"album_id": "100", "album_name": "Yest Album", "cached_at": yest_dt.timestamp()},
            "777": {"album_id": "777", "album_name": "Fresh Album", "cached_at": today_dt.timestamp()},
        }),
        encoding="utf-8",
    )

    store = server.Store(data_dir / "scan_history.sqlite3")
    with store.connect() as db:
        db.execute(
            "INSERT INTO runs(id,source,schedule_date,schedule_time,status,created_at,params) VALUES(?,?,?,?,?,?,?)",
            ("run_yest", "scheduled", yest_str, "00:03", "success", yest_dt.isoformat(timespec="seconds"), "{}"),
        )
        db.execute("INSERT INTO hits VALUES(?,?,?)", ("run_yest", yest_hit["link"], json.dumps(yest_hit)))
        db.execute(
            "INSERT INTO runs(id,source,schedule_date,schedule_time,status,created_at,params) VALUES(?,?,?,?,?,?,?)",
            ("run_prev_today", "scheduled", today_str, "00:03", "success", today_dt.isoformat(timespec="seconds"), "{}"),
        )
        db.execute("INSERT INTO hits VALUES(?,?,?)", ("run_prev_today", today_hit["link"], json.dumps(today_hit)))

    def fake_worker(*a, **kw):
        engine.init_csv_and_seen()
        emitted = []
        skipped = 0
        for item in (yest_hit, today_hit):
            if engine.write_hit_if_new(item):
                emitted.append(item)
                engine.emit({"type": "hit", "data": item})
            else:
                skipped += 1
        engine.emit({
            "type": "done",
            "results": emitted,
            "summary": {"artists": 2, "unique_albums": 2, "artist_failures": 0, "new_album_probe_failures": 0, "album_errors": 0, "owned_errors": 0},
            "metrics": {"elapsed_sec": 4.2, "results": len(emitted), "csv_skipped": skipped},
        })

    runtime = server.Runtime(config=server.Config(cfg_path), db_path=data_dir / "scan_history.sqlite3", worker=fake_worker)
    try:
        bad_args = ci_scan._parse_args(["--data-dir", str(data_dir), "--fresh-today", "--retry-of", "run_yest"])
        with pytest.raises(ValueError, match="cannot be combined"):
            ci_scan.execute_scan(runtime, bad_args, data_dir)

        out_dir = tmp_path / "fresh_out"
        args = ci_scan._parse_args([
            "--data-dir", str(data_dir),
            "--source", "manual",
            "--scan-mode", "regular",
            "--threads", "4",
            "--fresh-today",
            "--result-json", str(out_dir / "run_result.json"),
            "--today-csv", str(out_dir / "today.csv"),
            "--run-csv", str(out_dir / "run.csv"),
            "--summary-md", str(out_dir / "summary.md"),
        ])
        rc = ci_scan.execute_scan(runtime, args, data_dir)
        assert rc == 0

        res = json.loads((out_dir / "run_result.json").read_text(encoding="utf-8"))
        assert res["dry_run"] is True
        assert res["result_count"] == 1
        assert res["today_total"] == 1
        assert res["fresh_today_rollback"] == {
            "date": today_str,
            "removed_runs": 1,
            "removed_hits": 1,
            "removed_logs": 0,
            "removed_m3_reports": 0,
            "removed_csv_rows": 1,
            "evicted_album_cache": 1,
        }

        summary_text = (out_dir / "summary.md").read_text(encoding="utf-8")
        assert "🧪 网易云新曲扫描试跑报告（模拟今日首次扫描）" in summary_text
        assert "隔离试跑模式（模拟今日尚未扫描）" in summary_text
        assert "未写回" in summary_text
        assert "Fresh Album" in summary_text
        assert "CSV 已去重 1 条" in summary_text
        assert "dryrun_cookie_123" not in summary_text
    finally:
        runtime.close()


def test_scan_worker_skips_new_album_probe_when_limit_zero(tmp_path, monkeypatch):
    data_dir = tmp_path / "fast_engine"
    (data_dir / "VA_Artist_Caches").mkdir(parents=True)

    monkeypatch.setattr(engine, "SCRIPT_DIR", data_dir)
    monkeypatch.setattr(engine, "CSV_FILE", data_dir / "VA_Tracks_Output.csv")
    monkeypatch.setattr(engine, "ALBUM_CACHE_FILE", data_dir / "VA_Album_Date_Cache.json")
    monkeypatch.setattr(engine, "ARTIST_CACHE_DIR", data_dir / "VA_Artist_Caches")
    monkeypatch.setattr(engine, "METRICS_FILE", data_dir / "VA_Scan_Metrics.json")
    monkeypatch.setattr(engine, "REPORT_FILE", data_dir / "VA_Scan_Tuning_Report.txt")
    monkeypatch.setattr(engine, "CONTROL", engine.ControlState(target_threads=2, baseline_threads=2))
    monkeypatch.setattr(engine, "METRICS", engine.Metrics())

    events = []
    monkeypatch.setattr(engine, "emit", lambda msg: events.append(msg))

    now_naive = datetime.now(ci_scan.TZ).replace(tzinfo=None, hour=12, minute=0, second=0, microsecond=0)
    today_str = now_naive.date().isoformat()
    pub_ts = int(datetime.now(ci_scan.TZ).timestamp() * 1000)

    monkeypatch.setattr(
        engine,
        "fetch_recent_songs_for_artist",
        lambda artist, cookie, limit: (
            [
                {
                    "artist_id": artist["id"],
                    "artist_name": artist["name"],
                    "song_id": "8801",
                    "song_name": "Fast Track",
                    "album_id": "5001",
                    "album_name": "Fast Album",
                    "song_publish_ts": pub_ts,
                    "song_artist_ids": [artist["id"]],
                    "song_artist_names": [artist["name"]],
                    "link": "https://music.163.com/#/song?id=8801",
                }
            ],
            {"ok": True, "duration": 0.01},
        ),
    )

    def fail_if_called(*args, **kwargs):
        raise AssertionError("fetch_new_albums_for_artist should not be called when limit == 0")

    monkeypatch.setattr(engine, "fetch_new_albums_for_artist", fail_if_called)
    monkeypatch.setattr(
        engine,
        "fetch_album_detail",
        lambda aid, cookie, need_songs=False, expected_names=None: {
            "album_id": str(aid),
            "album_name": "Fast Album",
            "date_str": today_str,
            "publish_ts": pub_ts,
            "artist_ids": ["1"],
            "artist_names": ["Artist 1"],
        },
    )
    monkeypatch.setattr(
        engine,
        "fetch_owned_album_ids_shallow",
        lambda aid, cookie, limit: ({"5001"}, {"5001": today_str}),
    )

    engine.scan_worker(
        "MUSIC_U=fast_tok;",
        "",
        now_naive - timedelta(days=30),
        now_naive,
        now_naive - timedelta(days=1),
        now_naive + timedelta(days=3),
        50,
        50,
        0,
        artists_override=[{"id": "1", "name": "Artist 1"}],
    )

    logs = [e.get("text", "") for e in events if e.get("type") == "log"]
    assert any("newAlbumProbeLimit=0" in text for text in logs)
    assert any("新专目录兜底已跳过(0)" in text for text in logs)
    done_ev = next(e for e in events if e.get("type") == "done")
    assert done_ev["summary"]["new_album_probe_failures"] == 0
    assert len(done_ev["results"]) == 1
    assert done_ev["results"][0]["song_name"] == "Fast Track"


def test_runtime_new_album_probe_zero_validation_and_total_failure(tmp_path, monkeypatch):
    data_dir = tmp_path / "rt_probe_zero"
    (data_dir / "VA_Artist_Caches").mkdir(parents=True)
    cfg_path = ci_scan._prepare_temp_config(
        tmp_path / "sec_zero",
        raw_cookie="MUSIC_U=tok_zero;",
        threads=4,
        recent_limit=50,
        owned_limit=50,
        new_album_probe_limit=0,
        artist_cache_key="",
    )
    monkeypatch.setattr(engine, "SCRIPT_DIR", data_dir)
    monkeypatch.setattr(engine, "CSV_FILE", data_dir / "VA_Tracks_Output.csv")
    monkeypatch.setattr(server, "DATA", data_dir)
    monkeypatch.setattr(server, "verify_login", lambda *a, **kw: None)

    def all_artist_failed_worker(*args, **kwargs):
        engine.emit({
            "type": "done",
            "results": [],
            "summary": {
                "artists": 5,
                "unique_albums": 0,
                "artist_failures": 5,
                "new_album_probe_failures": 0,
                "album_errors": 0,
                "owned_errors": 0,
            },
            "metrics": {"elapsed_sec": 1.0, "results": 0, "csv_skipped": 0},
        })

    rt = server.Runtime(
        config=server.Config(cfg_path),
        db_path=data_dir / "scan_history.sqlite3",
        worker=all_artist_failed_worker,
    )
    try:
        assert rt.parameters({"newAlbumProbeLimit": 0})["newAlbumProbeLimit"] == 0
        with pytest.raises(ValueError, match="newAlbumProbeLimit"):
            rt.parameters({"newAlbumProbeLimit": -1})

        out_dir = tmp_path / "zero_out"
        args = ci_scan._parse_args([
            "--data-dir", str(data_dir),
            "--source", "scheduled",
            "--schedule-slot", "00:03",
            "--new-album-probe-limit", "0",
            "--result-json", str(out_dir / "run_result.json"),
            "--summary-md", str(out_dir / "summary.md"),
        ])
        rc = ci_scan.execute_scan(rt, args, data_dir)
        assert rc == 2

        res = json.loads((out_dir / "run_result.json").read_text(encoding="utf-8"))
        assert res["status"] == "failed"
        assert "所有艺人采样均失败" in res["error"]
        summary_text = (out_dir / "summary.md").read_text(encoding="utf-8")
        assert "跳过（快速模式）" in summary_text

        md_enabled = ci_scan._build_scan_summary_md(
            {
                "id": "r_on",
                "status": "success",
                "source": "scheduled",
                "schedule_time": "04:00",
                "params": {"scanMode": "regular", "threads": 8, "newAlbumProbeLimit": 20},
                "summary": {},
                "metrics": {},
            },
            "2026-11-24",
            [],
            [],
            lambda s: s,
        )
        assert "开启（深度 20）" in md_enabled
    finally:
        rt.close()


def test_resolve_artist_list_protects_cache_from_incomplete_live_fetch(tmp_path, monkeypatch):
    cache_dir = tmp_path / "artist_caches"
    cache_dir.mkdir(parents=True)
    monkeypatch.setattr(engine, "ARTIST_CACHE_DIR", cache_dir)
    monkeypatch.setattr(engine, "REFRESH_ARTIST_CACHE", True)

    cookie = "MUSIC_U=guard_cookie_123;"
    key = engine.cookie_fingerprint(cookie)
    original_artists = [{"id": str(i), "name": f"Artist {i}"} for i in range(1, 6)]
    engine.save_artist_cache(key, original_artists, {"source": "live-sublist", "count": 5, "complete_confirmed": True})

    events = []
    monkeypatch.setattr(engine, "emit", lambda msg: events.append(msg))

    # 1. Incomplete fetch (e.g. mid-pagination 405) must NOT overwrite existing 5-artist cache.
    def incomplete_fetch(_cookie):
        incomplete_fetch.last_meta = {
            "method": "GET",
            "limit": 500,
            "had_error": True,
            "complete_confirmed": False,
            "server_count": 5,
            "end_reason": "error_offset_500",
        }
        return original_artists[:2]

    monkeypatch.setattr(engine, "fetch_artist_sublist", incomplete_fetch)
    artists, res_key, mode = engine.resolve_artist_list(cookie, "")
    assert res_key == key
    assert mode == "fallback-cache"
    assert len(artists) == 5
    assert len(engine.load_artist_cache_by_key(key)) == 5
    assert engine.load_artist_cache_meta(key).get("complete_confirmed") is True
    assert any("回退缓存" in e.get("text", "") for e in events if e.get("type") == "log")

    # 2. Complete fetch with fewer artists (legitimate unfollow) SHOULD update cache normally.
    def complete_smaller_fetch(_cookie):
        complete_smaller_fetch.last_meta = {
            "method": "GET",
            "limit": 500,
            "had_error": False,
            "complete_confirmed": True,
            "server_count": 3,
            "end_reason": "server_count_reached",
        }
        return original_artists[:3]

    monkeypatch.setattr(engine, "fetch_artist_sublist", complete_smaller_fetch)
    artists2, res_key2, mode2 = engine.resolve_artist_list(cookie, "")
    assert res_key2 == key
    assert mode2 == "live"
    assert len(artists2) == 3
    assert len(engine.load_artist_cache_by_key(key)) == 3


def test_runtime_resolve_falls_back_to_configured_cache_key_on_incomplete_live_fetch(tmp_path, monkeypatch):
    data_dir = tmp_path / "rt_guard"
    cache_dir = data_dir / "VA_Artist_Caches"
    cache_dir.mkdir(parents=True)

    fallback_key = "92c430701d65a19f"
    fallback_artists = [{"id": str(i), "name": f"Migrated {i}"} for i in range(1, 5)]

    monkeypatch.setattr(engine, "SCRIPT_DIR", data_dir)
    monkeypatch.setattr(engine, "CSV_FILE", data_dir / "VA_Tracks_Output.csv")
    monkeypatch.setattr(engine, "ARTIST_CACHE_DIR", cache_dir)
    monkeypatch.setattr(server, "DATA", data_dir)
    monkeypatch.setattr(server, "verify_login", lambda *a, **kw: None)

    engine.save_artist_cache(
        fallback_key,
        fallback_artists,
        {"source": "migration", "count": 4, "complete_confirmed": True},
    )

    cfg_path = ci_scan._prepare_temp_config(
        tmp_path / "sec_guard",
        raw_cookie="MUSIC_U=new_cookie_diff_fp;",
        threads=4,
        recent_limit=50,
        owned_limit=50,
        new_album_probe_limit=20,
        artist_cache_key=fallback_key,
    )

    def partial_live_fetch(_cookie):
        partial_live_fetch.last_meta = {
            "method": "GET",
            "limit": 500,
            "had_error": True,
            "complete_confirmed": False,
            "server_count": 4,
            "end_reason": "error_offset_500",
        }
        return [fallback_artists[0]]

    monkeypatch.setattr(engine, "fetch_artist_sublist", partial_live_fetch)

    observed = {}

    def fake_worker(cookie, source_input, *args, **kwargs):
        resolved_artists, resolved_key, resolved_mode = engine.resolve_artist_list(cookie, source_input)
        observed["artists"] = resolved_artists
        observed["key"] = resolved_key
        observed["mode"] = resolved_mode
        engine.emit({
            "type": "done",
            "results": [],
            "summary": {
                "artists": len(resolved_artists),
                "unique_albums": 0,
                "artist_failures": 0,
                "new_album_probe_failures": 0,
                "album_errors": 0,
                "owned_errors": 0,
            },
            "metrics": {"elapsed_sec": 0.5, "results": 0, "csv_skipped": 0},
        })

    rt = server.Runtime(
        config=server.Config(cfg_path),
        db_path=data_dir / "scan_history.sqlite3",
        worker=fake_worker,
    )
    try:
        out_dir = tmp_path / "guard_out"
        args = ci_scan._parse_args([
            "--data-dir", str(data_dir),
            "--source", "manual",
            "--result-json", str(out_dir / "run_result.json"),
        ])
        rc = ci_scan.execute_scan(rt, args, data_dir)
        assert rc == 0
        assert observed["mode"] == "fallback-cache"
        assert observed["key"] == fallback_key
        assert len(observed["artists"]) == 4

        res = json.loads((out_dir / "run_result.json").read_text(encoding="utf-8"))
        assert res["status"] == "partial"
        assert "回退缓存" in res["warning"]
        assert "complete_confirmed" not in res["warning"]
    finally:
        rt.close()


def test_fetch_artist_sublist_tail_probe_paginates_past_4700(monkeypatch):
    all_artists = [{"id": str(i), "name": f"Artist {i}"} for i in range(1, 4749)]
    calls = []
    events = []

    def fake_api_request(path, params=None, preferred="GET", exact_method=False, **kwargs):
        assert path == "/artist/sublist"
        offset = int(params["offset"])
        limit = int(params["limit"])
        method = preferred
        calls.append((method, offset, limit))
        if limit >= 500 and offset >= 4500:
            return {"code": 200, "count": 4748, "data": [], "hasMore": False}
        page = all_artists[offset : offset + limit]
        return {
            "code": 200,
            "count": 4748,
            "data": page,
            "hasMore": (offset + len(page)) < len(all_artists),
        }

    monkeypatch.setattr(engine, "api_request", fake_api_request)
    monkeypatch.setattr(engine, "emit", lambda msg: events.append(msg))
    monkeypatch.setattr(engine.time, "sleep", lambda *a, **kw: None)

    artists = engine.fetch_artist_sublist("MUSIC_U=tail_test;")
    assert len(artists) == 4748
    assert len({a["id"] for a in artists}) == 4748
    meta = engine.fetch_artist_sublist.last_meta
    assert meta["complete_confirmed"] is True
    assert meta["end_reason"] == "tail_probe_server_count_reached"
    # 9 full pages (0..4000) + 1 empty page at 4500 + 2 tail pages (4500/200, 4700/48)
    assert len(calls) == 12
    assert calls[-2:] == [("GET", 4500, 200), ("GET", 4700, 48)]
    assert any("尾页补偿探测已补齐" in e.get("text", "") for e in events if e.get("type") == "log")


def test_fetch_artist_sublist_tail_probe_stops_cleanly_on_eof_short_count(monkeypatch):
    all_artists = [{"id": str(i), "name": f"Artist {i}"} for i in range(1, 4747)]
    calls = []

    def fake_api_request(path, params=None, preferred="GET", exact_method=False, **kwargs):
        offset = int(params["offset"])
        limit = int(params["limit"])
        calls.append((preferred, offset, limit))
        if preferred == "GET" and limit >= 500 and offset >= 4500:
            return {"code": 200, "count": 4748, "data": []}
        if preferred == "POST":
            return {"code": 200, "count": 4748, "data": all_artists[:500]}
        page = all_artists[offset : offset + limit]
        return {"code": 200, "count": 4748, "data": page}

    monkeypatch.setattr(engine, "api_request", fake_api_request)
    monkeypatch.setattr(engine, "emit", lambda msg: None)
    monkeypatch.setattr(engine.time, "sleep", lambda *a, **kw: None)

    artists = engine.fetch_artist_sublist("MUSIC_U=tail_short;")
    assert len(artists) == 4746
    assert len({a["id"] for a in artists}) == 4746
    meta = engine.fetch_artist_sublist.last_meta
    assert meta["complete_confirmed"] is False
    assert meta["end_reason"] == "tail_probe_partial"
    assert ("GET", 4500, 200) in calls
    assert ("GET", 4700, 48) in calls
    assert ("GET", 4746, 2) in calls


def test_album_cache_recheck_narrowed_to_3_days():
    now_cn = datetime.now(engine.CN_TZ)
    now_ts = time.time()

    ts_1d = int((now_cn - timedelta(days=1)).timestamp() * 1000)
    ts_5d = int((now_cn - timedelta(days=5)).timestamp() * 1000)

    assert engine.album_cache_needs_recheck({"publish_ts": ts_1d, "date_str": "recent", "cached_at": now_ts - 13 * 3600}) is True
    assert engine.album_cache_needs_recheck({"publish_ts": ts_1d, "date_str": "recent", "cached_at": now_ts - 2 * 3600}) is False
    assert engine.album_cache_needs_recheck({"publish_ts": ts_5d, "date_str": "older", "cached_at": now_ts - 13 * 3600}) is False
    assert engine.album_cache_needs_recheck({"publish_ts": 0, "date_str": "未知日期", "cached_at": now_ts - 7 * 3600}) is True


def test_scan_worker_skips_owned_verify_for_seen_and_new_only_artists(tmp_path, monkeypatch):
    data_dir = tmp_path / "skip_verify"
    (data_dir / "VA_Artist_Caches").mkdir(parents=True)

    monkeypatch.setattr(engine, "SCRIPT_DIR", data_dir)
    monkeypatch.setattr(engine, "CSV_FILE", data_dir / "VA_Tracks_Output.csv")
    monkeypatch.setattr(engine, "ALBUM_CACHE_FILE", data_dir / "VA_Album_Date_Cache.json")
    monkeypatch.setattr(engine, "ARTIST_CACHE_DIR", data_dir / "VA_Artist_Caches")
    monkeypatch.setattr(engine, "METRICS_FILE", data_dir / "VA_Scan_Metrics.json")
    monkeypatch.setattr(engine, "REPORT_FILE", data_dir / "VA_Scan_Tuning_Report.txt")
    monkeypatch.setattr(engine, "CONTROL", engine.ControlState(target_threads=2, baseline_threads=2))
    monkeypatch.setattr(engine, "METRICS", engine.Metrics())

    events = []
    monkeypatch.setattr(engine, "emit", lambda msg: events.append(msg))

    now_cn = datetime.now(engine.CN_TZ).replace(hour=12, minute=0, second=0, microsecond=0)
    now_naive = now_cn.replace(tzinfo=None)
    ext_cn = now_cn - timedelta(days=10)
    future_cn = now_cn + timedelta(days=1)
    ext_date = ext_cn.date().isoformat()
    future_date = future_cn.date().isoformat()
    ext_ts = int(ext_cn.timestamp() * 1000)
    future_ts = int(future_cn.timestamp() * 1000)

    # Pre-seed Artist 2's song on external album 8001 in CSV so it is already seen.
    engine.init_csv_and_seen()
    assert engine.write_hit_if_new({
        "name": "Artist 2",
        "song_name": "Seen Ext Song",
        "album_name": "Shared Ext VA",
        "date_str": ext_date,
        "link": "https://music.163.com/#/song?id=9102",
        "flag": 0,
    }) is True

    songs_by_artist = {
        "1": [{
            "artist_id": "1",
            "artist_name": "Artist 1",
            "song_id": "9101",
            "song_name": "Unseen Ext Song",
            "album_id": "8001",
            "album_name": "Shared Ext VA",
            "song_publish_ts": ext_ts,
            "song_artist_ids": ["1"],
            "song_artist_names": ["Artist 1"],
            "link": "https://music.163.com/#/song?id=9101",
        }],
        "2": [{
            "artist_id": "2",
            "artist_name": "Artist 2",
            "song_id": "9102",
            "song_name": "Seen Ext Song",
            "album_id": "8001",
            "album_name": "Shared Ext VA",
            "song_publish_ts": ext_ts,
            "song_artist_ids": ["2"],
            "song_artist_names": ["Artist 2"],
            "link": "https://music.163.com/#/song?id=9102",
        }],
        "3": [{
            "artist_id": "3",
            "artist_name": "Artist 3",
            "song_id": "9103",
            "song_name": "Today New Song",
            "album_id": "8002",
            "album_name": "Today Single",
            "song_publish_ts": future_ts,
            "song_artist_ids": ["3"],
            "song_artist_names": ["Artist 3"],
            "link": "https://music.163.com/#/song?id=9103",
        }],
    }
    monkeypatch.setattr(
        engine,
        "fetch_recent_songs_for_artist",
        lambda artist, cookie, limit: (songs_by_artist[artist["id"]], {"ok": True, "duration": 0.01}),
    )
    monkeypatch.setattr(
        engine,
        "fetch_album_detail",
        lambda aid, cookie, need_songs=False, expected_names=None: {
            "8001": {
                "album_id": "8001",
                "album_name": "Shared Ext VA",
                "date_str": ext_date,
                "publish_ts": ext_ts,
                "artist_ids": [],
                "artist_names": [],
            },
            "8002": {
                "album_id": "8002",
                "album_name": "Today Single",
                "date_str": future_date,
                "publish_ts": future_ts,
                "artist_ids": ["3"],
                "artist_names": ["Artist 3"],
            },
        }[str(aid)],
    )

    verified_ids = []

    def fake_owned(aid, cookie, limit):
        verified_ids.append(str(aid))
        return set(), {}

    monkeypatch.setattr(engine, "fetch_owned_album_ids_shallow", fake_owned)

    engine.scan_worker(
        "MUSIC_U=skip_verify_tok;",
        "",
        now_naive - timedelta(days=30),
        now_naive,
        now_naive - timedelta(days=1),
        now_naive + timedelta(days=3),
        50,
        50,
        0,
        artists_override=[
            {"id": "1", "name": "Artist 1"},
            {"id": "2", "name": "Artist 2"},
            {"id": "3", "name": "Artist 3"},
        ],
    )

    # Only Artist 1 (unseen external candidate) requires owned_verify; Artists 2 & 3 are skipped.
    assert verified_ids == ["1"]
    done_ev = next(e for e in events if e.get("type") == "done")
    assert done_ev["summary"]["verify_artists"] == 1
    assert done_ev["metrics"]["csv_skipped"] == 1
    assert {h["song_name"] for h in done_ev["results"]} == {"Unseen Ext Song", "Today New Song"}
    # Heat for Shared Ext VA must still include Artist 2 even though owned_verify was skipped for Artist 2.
    assert done_ev["summary"]["album_artist_counts"]["8001"] == 2
    ext_hit = next(h for h in done_ev["results"] if h["song_name"] == "Unseen Ext Song")
    assert ext_hit["album_count"] == 2
    assert any("跳过已入库/新专免验证艺人 2 位" in e.get("text", "") for e in events if e.get("type") == "log")


def test_m3_two_round_split_window_and_second_round_recent_limit(tmp_path, monkeypatch):
    data_dir = tmp_path / "m3_split"
    cache_dir = data_dir / "VA_Artist_Caches"
    cache_dir.mkdir(parents=True)

    m3_file = tmp_path / "m3_names.json"
    m3_file.write_text(
        json.dumps({"artists": [{"m3_artist_id": "m3_alpha", "name": "Alpha"}]}, ensure_ascii=False),
        encoding="utf-8",
    )

    monkeypatch.setattr(engine, "SCRIPT_DIR", data_dir)
    monkeypatch.setattr(engine, "CSV_FILE", data_dir / "VA_Tracks_Output.csv")
    monkeypatch.setattr(engine, "ARTIST_CACHE_DIR", cache_dir)
    monkeypatch.setattr(server, "DATA", data_dir)
    monkeypatch.setattr(server, "verify_login", lambda *a, **kw: None)

    cfg_path = ci_scan._prepare_temp_config(
        tmp_path / "sec_m3_split",
        raw_cookie="MUSIC_U=m3_split_tok;",
        threads=4,
        recent_limit=50,
        owned_limit=50,
        new_album_probe_limit=20,
        artist_cache_key="",
    )
    cfg_data = json.loads(cfg_path.read_text(encoding="utf-8"))
    cfg_data["m3_artist_file"] = str(m3_file)
    cfg_path.write_text(json.dumps(cfg_data), encoding="utf-8")

    followed = [{"id": "1", "name": "Alpha"}, {"id": "2", "name": "NonM3"}]
    monkeypatch.setattr(engine, "resolve_artist_list", lambda *a: (followed, "1234567890abcdef", "live"))

    calls = []

    def round_worker(*args, artists_override=None, artist_source_label="", **kwargs):
        calls.append({
            "label": artist_source_label,
            "artist_ids": [a["id"] for a in artists_override],
            "ext_start": args[2].date().isoformat(),
            "ext_end": args[3].date().isoformat(),
            "new_start": args[4].date().isoformat(),
            "new_end": args[5].date().isoformat(),
            "recent_limit": args[6],
        })
        engine.emit({
            "type": "done",
            "results": [],
            "summary": {
                "artists": len(artists_override),
                "relations": 0,
                "unique_albums": 0,
                "date_hit_albums": 0,
                "verify_artists": 0,
                "artist_failures": 0,
                "new_album_probe_failures": 0,
                "album_errors": 0,
                "owned_errors": 0,
                "round_artist_names": [a["name"] for a in artists_override],
            },
            "metrics": {"elapsed_sec": 0.2, "results": 0, "csv_skipped": 0},
        })

    rt = server.Runtime(
        config=server.Config(cfg_path),
        db_path=data_dir / "scan_history.sqlite3",
        worker=round_worker,
    )
    rt.use_m3_rounds = True
    try:
        dec10_params = rt.parameters({"scanMode": "auto"}, date(2026, 12, 10))
        dec26_params = rt.parameters({"scanMode": "auto"}, date(2026, 12, 26))
        assert dec10_params["scanMode"] == "m3"
        assert dec26_params["scanMode"] == "regular"

        with rt.lock:
            run_id = rt.store.create(dec10_params, source="manual")
            rt._launch(run_id, dec10_params)
        rt.thread.join()

        assert len(calls) == 2
        r1, r2 = calls
        assert r1["artist_ids"] == ["1"]
        assert r1["ext_start"] == "2026-10-24"
        assert r1["ext_end"] == "2026-12-10"
        assert r1["new_start"] == "2026-10-24"
        assert r1["new_end"] == "2026-12-13"
        assert r1["recent_limit"] == 50

        assert r2["artist_ids"] == ["2"]
        assert r2["ext_start"] == "2026-11-10"
        assert r2["ext_end"] == "2026-12-10"
        assert r2["new_start"] == "2026-12-09"
        assert r2["new_end"] == "2026-12-13"
        assert r2["recent_limit"] == 30
        assert rt.store.get(run_id)["status"] == "success"
    finally:
        rt.close()


def test_patch_api_keepalive_factory_inline_and_idempotent(tmp_path):
    factory_js = tmp_path / "factory_request.js"
    factory_js.write_text(
        "const http = require('http')\n"
        "const https = require('https')\n"
        "const createHttpAgent = () => new http.Agent({ keepAlive: true })\n"
        "const createHttpsAgent = () => new https.Agent({ keepAlive: true })\n"
        "module.exports = { httpAgent: createHttpAgent(), httpsAgent: createHttpsAgent() }\n",
        encoding="utf-8",
    )
    assert patch_api_keepalive.patch_request_js(factory_js) == "patched"
    patched_text = factory_js.read_text(encoding="utf-8")
    assert patch_api_keepalive.MARKER in patched_text
    assert "_sharedHttpAgent" in patched_text
    assert "_sharedHttpsAgent" in patched_text
    assert "() => _sharedHttpsAgent" in patched_text
    assert patch_api_keepalive.patch_request_js(factory_js) == "already-patched"
    proc1 = subprocess.run(["node", "--check", str(factory_js)], capture_output=True, text=True, check=False)
    assert proc1.returncode == 0, proc1.stderr

    inline_js = tmp_path / "inline_request.js"
    inline_js.write_text(
        "const http = require('http')\n"
        "const https = require('https')\n"
        "const settings = { httpAgent: new http.Agent({ keepAlive: true }), httpsAgent: new https.Agent({ keepAlive: true }) }\n"
        "module.exports = settings\n",
        encoding="utf-8",
    )
    assert patch_api_keepalive.patch_request_js(inline_js) == "patched"
    inline_text = inline_js.read_text(encoding="utf-8")
    assert patch_api_keepalive.MARKER in inline_text
    assert "httpAgent: _sharedHttpAgent" in inline_text
    assert "httpsAgent: _sharedHttpsAgent" in inline_text
    proc2 = subprocess.run(["node", "--check", str(inline_js)], capture_output=True, text=True, check=False)
    assert proc2.returncode == 0, proc2.stderr

    missing = tmp_path / "does_not_exist.js"
    assert patch_api_keepalive.patch_request_js(missing) == "skipped"


def test_session_bypasses_apicache_and_album_405_switches_immediately(tmp_path, monkeypatch):
    monkeypatch.setattr(engine, "ALBUM_CACHE_FILE", tmp_path / "VA_Album_Date_Cache.json")
    monkeypatch.setattr(engine, "ALBUM_CACHE", {})
    monkeypatch.setattr(engine, "METRICS", engine.Metrics())
    engine._ALBUM_PACER.reset()
    with engine.METHOD_CACHE_LOCK:
        saved_methods = dict(engine.METHOD_CACHE)
        engine.METHOD_CACHE.clear()
    had_session = hasattr(engine._thread_local, "session")
    prev_session = getattr(engine._thread_local, "session", None)
    if had_session:
        delattr(engine._thread_local, "session")

    try:
        sess = engine.get_session()
        assert sess.headers.get("X-Apicache-Bypass") == "true"

        post_calls = []
        get_calls = []
        sleep_calls = []

        class FakeResp:
            def __init__(self, status_code, payload):
                self.status_code = status_code
                self._payload = payload

            def json(self):
                return self._payload

            def raise_for_status(self):
                if self.status_code >= 400:
                    raise requests.HTTPError(f"HTTP {self.status_code}")

        def fake_post(url, data=None, timeout=None):
            post_calls.append((url, dict(data or {})))
            return FakeResp(405, {"code": 405, "msg": "Method Not Allowed"})

        def fake_get(url, params=None, timeout=None):
            get_calls.append((url, dict(params or {})))
            return FakeResp(
                200,
                {
                    "code": 200,
                    "album": {
                        "id": 555,
                        "name": "Album 555",
                        "publishTime": 1700000000000,
                        "artists": [],
                    },
                    "songs": [],
                },
            )

        monkeypatch.setattr(sess, "post", fake_post)
        monkeypatch.setattr(sess, "get", fake_get)
        monkeypatch.setattr(engine.time, "sleep", lambda s: sleep_calls.append(s))

        detail = engine.fetch_album_detail("555", "cookie-val")
        assert detail["album_id"] == "555"
        assert detail["album_name"] == "Album 555"
        assert len(post_calls) == 1  # POST tried first, NOT retried twice after 405
        assert len(get_calls) == 1
        assert sleep_calls == []
        with engine.METHOD_CACHE_LOCK:
            assert engine.METHOD_CACHE.get("/album") == "GET"
    finally:
        engine._ALBUM_PACER.reset()
        with engine.METHOD_CACHE_LOCK:
            engine.METHOD_CACHE.clear()
            engine.METHOD_CACHE.update(saved_methods)
        if had_session:
            engine._thread_local.session = prev_session
        elif hasattr(engine._thread_local, "session"):
            delattr(engine._thread_local, "session")


def test_phase2_prefilters_album_cache_hits_before_thread_pool(tmp_path, monkeypatch):
    data_dir = tmp_path / "prefilter"
    (data_dir / "VA_Artist_Caches").mkdir(parents=True)

    monkeypatch.setattr(engine, "SCRIPT_DIR", data_dir)
    monkeypatch.setattr(engine, "CSV_FILE", data_dir / "VA_Tracks_Output.csv")
    monkeypatch.setattr(engine, "ALBUM_CACHE_FILE", data_dir / "VA_Album_Date_Cache.json")
    monkeypatch.setattr(engine, "ARTIST_CACHE_DIR", data_dir / "VA_Artist_Caches")
    monkeypatch.setattr(engine, "METRICS_FILE", data_dir / "VA_Scan_Metrics.json")
    monkeypatch.setattr(engine, "REPORT_FILE", data_dir / "VA_Scan_Tuning_Report.txt")
    monkeypatch.setattr(engine, "ALBUM_CACHE", {})
    monkeypatch.setattr(engine, "_ALBUM_CACHE_DIRTY", False)
    monkeypatch.setattr(engine, "_ALBUM_CACHE_GEN", 0)
    monkeypatch.setattr(engine, "_ALBUM_CACHE_LOADED_FILE", None)
    monkeypatch.setattr(engine, "_SEEN_LOADED_CSV", None)
    monkeypatch.setattr(engine, "CONTROL", engine.ControlState(target_threads=2, baseline_threads=2))
    monkeypatch.setattr(engine, "METRICS", engine.Metrics())

    events = []
    monkeypatch.setattr(engine, "emit", lambda msg: events.append(msg))

    now_cn = datetime.now(engine.CN_TZ).replace(hour=12, minute=0, second=0, microsecond=0)
    now_naive = now_cn.replace(tzinfo=None)
    old_cn = now_cn - timedelta(days=10)
    old_date = old_cn.date().isoformat()
    old_ts = int(old_cn.timestamp() * 1000)

    engine.ALBUM_CACHE_FILE.write_text(
        json.dumps(
            {
                "200": {
                    "album_id": "200",
                    "album_name": "Cached VA",
                    "date_str": old_date,
                    "publish_ts": old_ts,
                    "artist_ids": [],
                    "artist_names": [],
                    "size": 2,
                    "cached_at": time.time(),
                }
            }
        ),
        encoding="utf-8",
    )

    rels = [
        {
            "artist_id": "1",
            "artist_name": "Artist 1",
            "song_id": "1001",
            "song_name": "Cached Song",
            "album_id": "200",
            "album_name": "Cached VA",
            "song_publish_ts": old_ts,
            "song_artist_ids": ["1"],
            "song_artist_names": ["Artist 1"],
            "link": "https://music.163.com/#/song?id=1001",
        },
        {
            "artist_id": "1",
            "artist_name": "Artist 1",
            "song_id": "1002",
            "song_name": "Uncached Song",
            "album_id": "201",
            "album_name": "Uncached VA",
            "song_publish_ts": old_ts,
            "song_artist_ids": ["1"],
            "song_artist_names": ["Artist 1"],
            "link": "https://music.163.com/#/song?id=1002",
        },
    ]
    monkeypatch.setattr(
        engine,
        "fetch_recent_songs_for_artist",
        lambda artist, cookie, limit: (rels, {"ok": True, "duration": 0.01}),
    )
    monkeypatch.setattr(
        engine,
        "fetch_owned_album_ids_shallow",
        lambda artist_id, cookie, limit: (set(), {}),
    )
    album_api_calls = []

    def fake_api_request(path, params=None, **kwargs):
        if path == "/album":
            aid = str(params["id"])
            album_api_calls.append(aid)
            return {
                "code": 200,
                "album": {"id": int(aid), "name": "Uncached VA", "publishTime": old_ts, "artists": []},
                "songs": [],
            }
        raise AssertionError(f"Unexpected api_request: {path}")

    monkeypatch.setattr(engine, "api_request", fake_api_request)

    pool_calls = []
    orig_pool = engine.run_dynamic_pool

    def spy_pool(items, func, phase_label, progress_cb=None):
        pool_calls.append((phase_label, list(items)))
        return orig_pool(items, func, phase_label, progress_cb)

    monkeypatch.setattr(engine, "run_dynamic_pool", spy_pool)

    artists = [{"id": "1", "name": "Artist 1"}]
    engine.scan_worker(
        scan_cookie="cookie",
        artist_source_input="",
        external_start_dt=now_naive - timedelta(days=30),
        external_end_dt=now_naive,
        new_start_dt=now_naive - timedelta(days=1),
        new_end_dt=now_naive + timedelta(days=2),
        recent_limit=30,
        owned_verify_limit=50,
        new_album_probe_limit=0,
        artists_override=artists,
    )

    album_pool_batches = [items for phase, items in pool_calls if phase == "album_detail_pool"]
    assert album_pool_batches == [["201"]]
    assert album_api_calls == ["201"]
    assert engine.METRICS.album_cache_hit >= 1
    assert engine.METRICS.album_total == 2
    assert engine.METRICS.album_done == 2
    done_ev = next(e for e in events if e.get("type") == "done")
    assert {h["album_id"] for h in done_ev["results"]} == {"200", "201"}

    # Second run: both 200 and 201 are cached -> album_detail_pool skips ThreadPoolExecutor completely.
    pool_calls.clear()
    album_api_calls.clear()
    events.clear()
    monkeypatch.setattr(engine, "METRICS", engine.Metrics())
    engine.scan_worker(
        scan_cookie="cookie",
        artist_source_input="",
        external_start_dt=now_naive - timedelta(days=30),
        external_end_dt=now_naive,
        new_start_dt=now_naive - timedelta(days=1),
        new_end_dt=now_naive + timedelta(days=2),
        recent_limit=30,
        owned_verify_limit=50,
        new_album_probe_limit=0,
        artists_override=artists,
    )
    assert [phase for phase, _ in pool_calls if phase == "album_detail_pool"] == []
    assert album_api_calls == []
    assert engine.METRICS.album_total == 2
    assert engine.METRICS.album_done == 2
    assert engine.METRICS.album_cache_hit == 2


def test_album_cache_dirty_flag_and_round2_skips_redundant_disk_reload(tmp_path, monkeypatch):
    data_dir = tmp_path / "dirty_flag"
    data_dir.mkdir(parents=True)
    cache_file = data_dir / "VA_Album_Date_Cache.json"
    csv_file = data_dir / "VA_Tracks_Output.csv"

    monkeypatch.setattr(engine, "CSV_FILE", csv_file)
    monkeypatch.setattr(engine, "ALBUM_CACHE_FILE", cache_file)
    monkeypatch.setattr(engine, "ALBUM_CACHE", {})
    monkeypatch.setattr(engine, "_ALBUM_CACHE_DIRTY", False)
    monkeypatch.setattr(engine, "_ALBUM_CACHE_GEN", 0)
    monkeypatch.setattr(engine, "_ALBUM_CACHE_LOADED_FILE", None)
    monkeypatch.setattr(engine, "_SEEN_LOADED_CSV", None)
    monkeypatch.setattr(engine, "METRICS", engine.Metrics())

    cache_file.write_text(
        json.dumps(
            {
                "300": {
                    "album_id": "300",
                    "album_name": "Existing 300",
                    "date_str": "2026-11-01",
                    "publish_ts": 1793462400000,
                    "cached_at": time.time(),
                }
            }
        ),
        encoding="utf-8",
    )

    engine.load_album_cache()

    dump_calls = 0
    orig_dump = engine.json.dump

    def spy_dump(*args, **kwargs):
        nonlocal dump_calls
        dump_calls += 1
        return orig_dump(*args, **kwargs)

    monkeypatch.setattr(engine.json, "dump", spy_dump)

    # Clean cache -> force=False skips rewriting disk
    assert engine.save_album_cache(force=False) is False
    assert dump_calls == 0

    # Fetching a new album marks ALBUM_CACHE dirty
    monkeypatch.setattr(
        engine,
        "api_request",
        lambda path, params=None, **kwargs: {
            "code": 200,
            "album": {"id": 301, "name": "New 301", "publishTime": 1793462400000, "artists": []},
            "songs": [],
        },
    )
    engine.fetch_album_detail("301", "cookie")
    assert engine.save_album_cache(force=False) is True
    assert dump_calls == 1
    saved = json.loads(cache_file.read_text(encoding="utf-8"))
    assert set(saved.keys()) == {"300", "301"}

    # Clean again after save -> second save(force=False) skips disk write
    assert engine.save_album_cache(force=False) is False
    assert dump_calls == 1

    # Round 2 (reset_seen=False / force=False) skips redundant CSV & album cache disk reloads
    engine.init_csv_and_seen(reset_current_scan=True)
    engine.write_hit_if_new({
        "name": "Artist A",
        "song_name": "Track A",
        "album_name": "Existing 300",
        "date_str": "2026-11-01",
        "link": "https://music.163.com/#/song?id=7001",
        "flag": 0,
    })

    csv_reads = 0
    json_loads = 0
    orig_csv_reader = engine.csv.reader
    orig_json_load = engine.json.load

    def spy_csv_reader(*args, **kwargs):
        nonlocal csv_reads
        csv_reads += 1
        return orig_csv_reader(*args, **kwargs)

    def spy_json_load(*args, **kwargs):
        nonlocal json_loads
        json_loads += 1
        return orig_json_load(*args, **kwargs)

    monkeypatch.setattr(engine.csv, "reader", spy_csv_reader)
    monkeypatch.setattr(engine.json, "load", spy_json_load)

    engine.init_csv_and_seen(reset_current_scan=False)
    engine.load_album_cache(force=False)
    assert csv_reads == 0
    assert json_loads == 0
    assert "https://music.163.com/#/song?id=7001" in engine.SEEN_LINKS
    assert set(engine.ALBUM_CACHE.keys()) == {"300", "301"}

    # Explicit reload (reset_current_scan=True / force=True) still reloads from disk
    engine.init_csv_and_seen(reset_current_scan=True)
    engine.load_album_cache(force=True)
    assert csv_reads == 1
    assert json_loads == 1


def test_scan_summary_renders_endpoint_and_phase_perf_section():
    run = {
        "id": "r_perf",
        "status": "success",
        "source": "manual",
        "params": {"scanMode": "m3", "threads": 7, "newAlbumProbeLimit": 20},
        "summary": {"artists": 4749, "unique_albums": 47621},
        "metrics": {
            "started_at": 1000.0,
            "ended_at": 2036.0,
            "elapsed_sec": 1036.0,
            "album_cache_hit": 48654,
            "album_cache_miss": 859,
            "album_cache_recheck": 0,
            "album_id_mismatch": 194,
            "album_name_mismatch": 0,
            "endpoints": {
                "/artist/songs": {"count": 4749, "ok": 4749, "status_405": 0, "status_460": 0, "timeout": 0, "conn_error": 0, "error": 0, "avg_ms": 420.4, "p95_ms": 594.1},
                "/album": {"count": 1686, "ok": 1053, "status_405": 632, "status_460": 0, "timeout": 0, "conn_error": 0, "error": 1, "avg_ms": 252.8, "p95_ms": 445.2},
            },
            "events": [
                {"ts": 1015.8, "phase": "resolve_artist_list"},
                {"ts": 1016.0, "phase": "sample_recent_songs"},
                {"ts": 1049.0, "phase": "new_album_probe"},
                {"ts": 1094.0, "phase": "album_detail_pool"},
                {"ts": 1134.0, "phase": "owned_verify"},
                {"ts": 1137.0, "phase": "emit_results"},
                {"ts": 1139.0, "phase": "resolve_artist_list"},
                {"ts": 1140.0, "phase": "sample_recent_songs"},
                {"ts": 1408.0, "phase": "new_album_probe"},
                {"ts": 1881.0, "phase": "album_detail_pool"},
                {"ts": 1982.0, "phase": "owned_verify"},
                {"ts": 2032.0, "phase": "emit_results"},
            ],
        },
    }
    md = ci_scan._build_scan_summary_md(run, "2026-11-25", [], [], lambda s: s)
    assert "### 性能与接口统计" in md
    assert "**专辑缓存**：命中 48654 · 未命中 859 · 复查 0 · ID 错配 194" in md
    assert "**分阶段耗时**：" in md
    assert "解析艺人列表 17.0s" in md
    assert "采样 recent 曲目 301.0s" in md
    assert "查询专辑详情 141.0s" in md
    assert "| 接口 | 请求数 | 成功 | 405 | 其他错误 | 平均耗时 | P95 |" in md
    assert "| `/artist/songs` | 4749 | 4749 | 0 | 0 | 420.4 ms | 594.1 ms |" in md
    assert "| `/album` | 1686 | 1053 | 632 | 1 | 252.8 ms | 445.2 ms |" in md

    empty_md = ci_scan._build_scan_summary_md(
        {"id": "r_empty", "status": "success", "source": "manual", "params": {}, "summary": {}, "metrics": {}},
        "2026-11-25",
        [],
        [],
        lambda s: s,
    )
    assert "### 性能与接口统计" not in empty_md


def test_dual_cookie_uses_main_for_sublist_and_alt_for_scanning_and_scrubs_both(tmp_path, monkeypatch):
    data_dir = tmp_path / "dual_cookie"
    cache_dir = data_dir / "VA_Artist_Caches"
    cache_dir.mkdir(parents=True)

    main_raw = "MUSIC_U=main_secret_token_111;"
    alt_raw = "MUSIC_U=alt_secret_token_222;"

    monkeypatch.setattr(engine, "SCRIPT_DIR", data_dir)
    monkeypatch.setattr(engine, "CSV_FILE", data_dir / "VA_Tracks_Output.csv")
    monkeypatch.setattr(engine, "ALBUM_CACHE_FILE", data_dir / "VA_Album_Date_Cache.json")
    monkeypatch.setattr(engine, "ARTIST_CACHE_DIR", cache_dir)
    monkeypatch.setattr(engine, "METRICS_FILE", data_dir / "VA_Scan_Metrics.json")
    monkeypatch.setattr(engine, "REPORT_FILE", data_dir / "VA_Scan_Tuning_Report.txt")
    monkeypatch.setattr(engine, "ALBUM_CACHE", {})
    monkeypatch.setattr(engine, "_ALBUM_CACHE_DIRTY", False)
    monkeypatch.setattr(engine, "_ALBUM_CACHE_GEN", 0)
    monkeypatch.setattr(engine, "_ALBUM_CACHE_LOADED_FILE", None)
    monkeypatch.setattr(engine, "_SEEN_LOADED_CSV", None)
    monkeypatch.setattr(server, "DATA", data_dir)

    m3_file = tmp_path / "m3_names.json"
    m3_file.write_text(
        json.dumps({"artists": [{"m3_artist_id": "m3_alpha", "name": "Alpha"}]}, ensure_ascii=False),
        encoding="utf-8",
    )

    single_cfg_path = ci_scan._prepare_temp_config(
        tmp_path / "single_sec",
        raw_cookie=main_raw,
        threads=2,
        recent_limit=50,
        owned_limit=50,
        new_album_probe_limit=20,
        artist_cache_key="",
    )
    single_cfg = server.Config(single_cfg_path)
    assert single_cfg.cookie() == main_raw
    assert single_cfg.scan_cookie() == main_raw

    dual_cfg_path = ci_scan._prepare_temp_config(
        tmp_path / "dual_sec",
        raw_cookie=main_raw,
        raw_scan_cookie=alt_raw,
        threads=2,
        recent_limit=50,
        owned_limit=50,
        new_album_probe_limit=20,
        artist_cache_key="",
    )
    cfg_data = json.loads(dual_cfg_path.read_text(encoding="utf-8"))
    cfg_data["m3_artist_file"] = str(m3_file)
    dual_cfg_path.write_text(json.dumps(cfg_data), encoding="utf-8")

    dual_cfg = server.Config(dual_cfg_path)
    assert dual_cfg.cookie() == main_raw
    assert dual_cfg.scan_cookie() == alt_raw

    verified_cookies = []
    monkeypatch.setattr(server, "verify_login", lambda ck, *a, **kw: verified_cookies.append(ck))

    now_cn = datetime.now(engine.CN_TZ).replace(hour=12, minute=0, second=0, microsecond=0)
    today_date = now_cn.date()
    ext_date_ts = int((now_cn - timedelta(days=10)).timestamp() * 1000)

    api_cookie_log = []
    scrubbed_during_run = []

    def fake_api_request(path, params=None, **kwargs):
        params = dict(params or {})
        api_cookie_log.append((path, params.get("cookie")))
        if not scrubbed_during_run:
            scrubbed_during_run.append(
                rt.scrub(f"err main=main_secret_token_111 alt=alt_secret_token_222 full={alt_raw} fullm={main_raw}")
            )
            engine.emit(
                {
                    "type": "log",
                    "level": "info",
                    "text": f"trace main=main_secret_token_111 alt=alt_secret_token_222",
                }
            )
        if path == "/artist/sublist":
            return {
                "code": 200,
                "data": [{"id": 1, "name": "Alpha"}, {"id": 2, "name": "Beta"}],
                "count": 2,
                "hasMore": False,
            }
        if path == "/artist/songs":
            aid = str(params["id"])
            return {
                "code": 200,
                "songs": [
                    {
                        "id": 9000 + int(aid),
                        "name": f"Track {aid}",
                        "al": {"id": 8000 + int(aid), "name": f"VA {aid}"},
                        "ar": [{"id": int(aid), "name": "Alpha" if aid == "1" else "Beta"}],
                        "publishTime": ext_date_ts,
                    }
                ],
            }
        if path == "/artist/album":
            return {"code": 200, "hotAlbums": [], "more": False}
        if path == "/album":
            aid = int(params["id"])
            return {
                "code": 200,
                "album": {
                    "id": aid,
                    "name": f"VA {aid - 8000}",
                    "publishTime": ext_date_ts,
                    "artists": [],
                    "size": 1,
                },
                "songs": [],
            }
        raise AssertionError(f"Unexpected path: {path}")

    monkeypatch.setattr(engine, "api_request", fake_api_request)

    rt = server.Runtime(config=dual_cfg, db_path=data_dir / "scan_history.sqlite3")
    try:
        # 1. Regular mode
        reg_params = rt.parameters(
            {
                "scanMode": "regular",
                "threads": 2,
                "recentLimit": 50,
                "ownedLimit": 50,
                "newAlbumProbeLimit": 20,
            },
            today_date,
        )
        with rt.lock:
            reg_id = rt.store.create(reg_params, source="manual")
            rt._launch(reg_id, reg_params)
        rt.thread.join()
        assert rt.store.get(reg_id)["status"] == "success"

        assert verified_cookies == [alt_raw]
        sublist_cookies = {ck for p, ck in api_cookie_log if p == "/artist/sublist"}
        scan_cookies = {ck for p, ck in api_cookie_log if p in {"/artist/songs", "/artist/album", "/album"}}
        assert sublist_cookies == {main_raw}
        assert scan_cookies == {alt_raw}
        assert {p for p, _ in api_cookie_log} == {"/artist/sublist", "/artist/songs", "/artist/album", "/album"}

        main_key = engine.cookie_fingerprint(main_raw)
        alt_key = engine.cookie_fingerprint(alt_raw)
        assert (cache_dir / f"artists_{main_key}.json").is_file()
        assert not (cache_dir / f"artists_{alt_key}.json").exists()

        # Both tokens are scrubbed during run and never reach stored logs
        assert len(scrubbed_during_run) == 1
        assert "main_secret_token_111" not in scrubbed_during_run[0]
        assert "alt_secret_token_222" not in scrubbed_during_run[0]
        all_logs_text = "\n".join(row["text"] for row in rt.store.logs(reg_id))
        assert "main_secret_token_111" not in all_logs_text
        assert "alt_secret_token_222" not in all_logs_text

        # 2. M3 two-round mode (remove main artist cache & CSV first so live sublist + album verify run cleanly)
        (cache_dir / f"artists_{main_key}.json").unlink()
        if engine.CSV_FILE.exists():
            engine.CSV_FILE.unlink()
        engine.ALBUM_CACHE.clear()
        if engine.ALBUM_CACHE_FILE.exists():
            engine.ALBUM_CACHE_FILE.unlink()
        api_cookie_log.clear()
        verified_cookies.clear()

        m3_params = rt.parameters(
            {
                "externalStartDate": (today_date - timedelta(days=20)).isoformat(),
                "externalEndDate": today_date.isoformat(),
                "newStartDate": (today_date - timedelta(days=1)).isoformat(),
                "newEndDate": today_date.isoformat(),
                "scanMode": "m3",
                "threads": 2,
                "recentLimit": 50,
                "ownedLimit": 50,
                "newAlbumProbeLimit": 20,
            },
            today_date,
        )
        with rt.lock:
            m3_id = rt.store.create(m3_params, source="manual")
            rt._launch(m3_id, m3_params)
        rt.thread.join()
        assert rt.store.get(m3_id)["status"] == "success"
        assert verified_cookies == [alt_raw]
        assert {ck for p, ck in api_cookie_log if p == "/artist/sublist"} == {main_raw}
        assert {ck for p, ck in api_cookie_log if p in {"/artist/songs", "/artist/album", "/album"}} == {alt_raw}

        md = ci_scan._build_scan_summary_md(rt.store.get(reg_id), today_date.isoformat(), [], [], rt.scrub, dual_cookie=True)
        assert "双 Cookie（大号拉艺人 / 小号扫描）" in md
        assert "main_secret_token_111" not in md
        assert "alt_secret_token_222" not in md
    finally:
        rt.close()


def test_probe_dual_cookie_separates_sublist_and_scan_cookies_and_scrubs(tmp_path, monkeypatch):
    import probe as probe_mod

    main_raw = "MUSIC_U=probe_main_777;"
    alt_raw = "MUSIC_U=probe_alt_888;"
    cfg_path = ci_scan._prepare_temp_config(
        tmp_path / "probe_dual_sec",
        raw_cookie=main_raw,
        raw_scan_cookie=alt_raw,
        threads=2,
        recent_limit=50,
        owned_limit=50,
        new_album_probe_limit=20,
        artist_cache_key="",
    )
    cfg = server.Config(cfg_path)

    login_checks = []
    monkeypatch.setattr(probe_mod, "verify_login", lambda ck, base=None: login_checks.append(ck))

    req_log = []

    class FakeResp:
        def __init__(self, payload):
            self.status_code = 200
            self.ok = True
            self._payload = payload

        def json(self):
            return self._payload

        def raise_for_status(self):
            return None

    def fake_request(method, url, data=None, params=None, headers=None, timeout=None, **kwargs):
        req_params = dict(data if method == "POST" else params or {})
        endpoint = "/" + url.split("http://127.0.0.1:3000/")[-1]
        req_log.append((endpoint, req_params.get("cookie"), dict(headers or {})))
        if endpoint == "/artist/sublist":
            return FakeResp({"code": 200, "data": [{"id": 1, "name": "Artist 1"}], "count": 1})
        if endpoint == "/artist/songs":
            return FakeResp({"code": 200, "songs": [{"id": 10, "al": {"id": 500}}]})
        if endpoint == "/artist/album":
            return FakeResp({"code": 200, "hotAlbums": [{"id": 500}]})
        if endpoint == "/album":
            return FakeResp({"code": 200, "album": {"id": 500, "name": "VA 500"}})
        raise AssertionError(f"Unexpected endpoint: {endpoint}")

    monkeypatch.setattr(probe_mod.requests, "request", fake_request)

    report = probe_mod.probe(config=cfg, artist_count=1, api_base="http://127.0.0.1:3000")
    assert report["ok"] is True
    assert report["cookie_mode"] == "dual"
    assert login_checks == [alt_raw, main_raw]
    assert {ck for ep, ck, _ in req_log if ep == "/artist/sublist"} == {main_raw}
    assert {ck for ep, ck, _ in req_log if ep != "/artist/sublist"} == {alt_raw}
    assert all(hdrs.get("X-Apicache-Bypass") == "true" for _, _, hdrs in req_log)

    scrubbed = probe_mod._scrub_text("bad main=probe_main_777 alt=probe_alt_888", (main_raw, alt_raw))
    assert "probe_main_777" not in scrubbed
    assert "probe_alt_888" not in scrubbed

    md = ci_scan._build_probe_summary_md(report)
    assert "双 Cookie（大号拉艺人 / 小号扫描）" in md


def test_album_pacer_throttles_405_and_recovers_without_redundant_method_switch(tmp_path, monkeypatch):
    # 1. Adaptive backoff & recovery behavior
    pacer = engine._AlbumPacer(max_concurrency=2, base_interval_sec=0.1, max_interval_sec=1.0, cooldown_sec=0.05)
    assert pacer.current_interval == 0.1
    stretched = pacer.on_rate_limit()
    assert stretched == pytest.approx(0.26)
    for _ in range(10):
        pacer.on_success()
    assert pacer.current_interval < stretched
    assert pacer.current_interval >= pacer.base_interval
    pacer.reset()
    assert pacer.current_interval == 0.1
    assert pacer._next_allowed == 0.0
    assert pacer._ok_streak == 0

    # 2. Proven POST method encountering transient 405 cools down & retries POST in-place without falling over to GET
    test_pacer = engine._AlbumPacer(max_concurrency=2, base_interval_sec=0.0, max_interval_sec=0.2, cooldown_sec=0.01)
    monkeypatch.setattr(engine, "_ALBUM_PACER", test_pacer)
    monkeypatch.setattr(engine, "ALBUM_CACHE_FILE", tmp_path / "VA_Album_Date_Cache.json")
    monkeypatch.setattr(engine, "ALBUM_CACHE", {})
    monkeypatch.setattr(engine, "METRICS", engine.Metrics())

    with engine.METHOD_CACHE_LOCK:
        saved_methods = dict(engine.METHOD_CACHE)
        engine.METHOD_CACHE["/album"] = "POST"

    methods_called = []

    def fake_api_request(path, params=None, preferred=None, exact_method=False, **kwargs):
        assert path == "/album"
        assert exact_method is True
        methods_called.append(preferred)
        if len(methods_called) == 1:
            raise engine.ApiError("/album", "405 rate limit", 405)
        return {
            "code": 200,
            "album": {
                "id": 777,
                "name": "Album 777",
                "publishTime": 1700000000000,
                "artists": [],
            },
            "songs": [],
        }

    monkeypatch.setattr(engine, "api_request", fake_api_request)
    try:
        detail = engine.fetch_album_detail("777", "cookie-val")
        assert detail["album_id"] == "777"
        assert detail["album_name"] == "Album 777"
        assert methods_called == ["POST", "POST"]
        assert test_pacer._sem._value == 2
    finally:
        with engine.METHOD_CACHE_LOCK:
            engine.METHOD_CACHE.clear()
            engine.METHOD_CACHE.update(saved_methods)


def test_notify_email_infers_smtp_and_builds_html_attachments_and_scrubs(tmp_path, monkeypatch):
    import notify_email

    assert notify_email.infer_smtp_config("sender@gmail.com") == ("smtp.gmail.com", 465)
    assert notify_email.infer_smtp_config("user@qq.com") == ("smtp.qq.com", 465)
    assert notify_email.infer_smtp_config("user@163.com") == ("smtp.163.com", 465)
    assert notify_email.infer_smtp_config("user@outlook.com") == ("smtp-mail.outlook.com", 587)
    assert notify_email.infer_smtp_config("custom@example.com", "smtp.example.org", "2525") == ("smtp.example.org", 2525)
    with pytest.raises(ValueError):
        notify_email.infer_smtp_config("user@unknown-domain.example")

    data_dir = tmp_path / "mail_data"
    data_dir.mkdir(parents=True)
    store = server.Store(data_dir / "scan_history.sqlite3")
    run_id = store.create(
        {
            "scanMode": "m3",
            "threads": 7,
            "externalStartDate": "2026-10-24",
            "externalEndDate": "2026-11-25",
            "newStartDate": "2026-11-24",
            "newEndDate": "2026-11-28",
        },
        source="scheduled",
        schedule_time="04:00",
        schedule_date="2026-11-25",
    )
    store.update(
        run_id,
        status="partial",
        summary={"artists": 4700, "unique_albums": 47124, "album_errors": 0},
        metrics={
            "elapsed_sec": 600.0,
            "csv_skipped": 100,
            "album_cache_hit": 48000,
            "album_cache_miss": 800,
            "endpoints": {"/album": {"count": 800, "status_405": 0}},
        },
        warning="fallback MUSIC_U=secret_ck_999; pass=app-pass-secret-xyz",
    )
    store.hit(
        run_id,
        {
            "name": "Alice",
            "song_name": "Track One",
            "album_name": "Shared VA",
            "date_str": "2026-11-25",
            "link": "https://music.163.com/#/song?id=10",
            "flag": 1,
            "album_id": "500",
            "album_count": 3,
        },
    )
    store.hit(
        run_id,
        {
            "name": "Alice",
            "song_name": "【新专辑】Shared VA",
            "album_name": "Shared VA",
            "date_str": "2026-11-25",
            "link": "https://music.163.com/#/album?id=500",
            "flag": 1,
            "album_id": "500",
            "album_count": 3,
        },
    )
    store.hit(
        run_id,
        {
            "name": "Bob",
            "song_name": "Solo EP",
            "album_name": "Solo Release",
            "date_str": "2026-11-24",
            "link": "https://music.163.com/#/song?id=11",
            "flag": 1,
            "album_id": "501",
            "album_count": 1,
        },
    )

    art_dir = tmp_path / "artifacts"
    art_dir.mkdir(parents=True)
    result_json = art_dir / "run_result.json"
    result_json.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "status": "partial",
                "skipped": False,
                "dry_run": False,
                "date": "2026-11-25",
                "source": "scheduled",
                "schedule_slot": "04:00",
                "result_count": 2,
                "today_total": 5,
                "elapsed_sec": 600.0,
                "warning": "fallback MUSIC_U=secret_ck_999; pass=app-pass-secret-xyz",
                "error": "",
            }
        ),
        encoding="utf-8",
    )
    html_file = art_dir / "latest-report.html"
    html_file.write_text("<html><body>Offline Report</body></html>", encoding="utf-8")
    today_csv = art_dir / "today.csv"
    today_csv.write_text("Artist,Track,Album,Date,Link,Flag\nAlice,Track One,Shared VA,2026-11-25,https://music.163.com/#/song?id=10,1\n", encoding="utf-8-sig")
    run_csv = art_dir / "run.csv"
    run_csv.write_text("Artist,Track,Album,Date,Link,Flag\nAlice,Track One,Shared VA,2026-11-25,https://music.163.com/#/song?id=10,1\n", encoding="utf-8-sig")

    sent = {}

    class DummySSLServer:
        def __init__(self, host, port, timeout=30, context=None):
            sent["host"] = host
            sent["port"] = port

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def login(self, user, pwd):
            sent["login"] = (user, pwd)

        def send_message(self, msg):
            sent["msg"] = msg

    monkeypatch.setattr(notify_email.smtplib, "SMTP_SSL", DummySSLServer)
    monkeypatch.setenv("SMTP_USER", "sender@gmail.com")
    monkeypatch.setenv("SMTP_PASS", "app-pass-secret-xyz")
    monkeypatch.delenv("MAIL_TO", raising=False)
    monkeypatch.delenv("SMTP_HOST", raising=False)
    monkeypatch.delenv("SMTP_PORT", raising=False)
    monkeypatch.delenv("MAIL_ATTACH_CSV", raising=False)

    rc = notify_email.main([
        "--data-dir", str(data_dir),
        "--result-json", str(result_json),
        "--run-csv", str(run_csv),
        "--today-csv", str(today_csv),
        "--standalone-html", str(html_file),
        "--run-url", "https://github.com/owner/repo/actions/runs/12345",
        "--site-url", "https://viewer.example.com",
    ])
    assert rc == 0
    assert sent["host"] == "smtp.gmail.com"
    assert sent["port"] == 465
    assert sent["login"] == ("sender@gmail.com", "app-pass-secret-xyz")

    msg = sent["msg"]
    assert msg["To"] == "sender@gmail.com"
    assert "[网易云新曲] · ⚠️ 告警 2026-11-25 (04:00) 新增 2 条（2 张专辑）" == msg["Subject"]

    plain_part = msg.get_body(preferencelist=("plain",)).get_content()
    html_part = msg.get_body(preferencelist=("html",)).get_content()
    assert "https://music.163.com/#/album?id=500" in html_part
    assert "https://github.com/owner/repo/actions/runs/12345" in html_part
    assert "https://viewer.example.com" in plain_part and "https://viewer.example.com" in html_part
    assert "overflow-x:auto" not in html_part
    assert "<table" not in html_part
    assert "Shared VA" in html_part
    assert "Alice" in html_part
    assert "Solo Release" in html_part
    assert "Bob" in html_part
    assert "app-pass-secret-xyz" not in plain_part and "app-pass-secret-xyz" not in html_part
    assert "secret_ck_999" not in plain_part and "secret_ck_999" not in html_part

    att_names = [part.get_filename() for part in msg.iter_attachments()]
    assert "netease-report-2026-11-25.html" in att_names
    assert not any(name.endswith(".csv") for name in att_names)

    # Explicit MAIL_ATTACH_CSV=1 re-enables CSV attachments when requested
    sent.clear()
    monkeypatch.setenv("MAIL_ATTACH_CSV", "1")
    rc_csv = notify_email.main([
        "--data-dir", str(data_dir),
        "--result-json", str(result_json),
        "--run-csv", str(run_csv),
        "--today-csv", str(today_csv),
        "--standalone-html", str(html_file),
    ])
    assert rc_csv == 0
    att_names_csv = [part.get_filename() for part in sent["msg"].iter_attachments()]
    assert "netease-today-2026-11-25.csv" in att_names_csv
    assert "netease-run-2026-11-25-0400.csv" in att_names_csv


def test_notify_email_skip_guards_and_starttls_and_dry_run_prefix(tmp_path, monkeypatch):
    import notify_email

    art_dir = tmp_path / "mail_guards"
    art_dir.mkdir(parents=True)
    result_json = art_dir / "run_result.json"

    def write_result(payload):
        result_json.write_text(json.dumps(payload), encoding="utf-8")

    smtp_calls = []

    class DummySTARTTLSServer:
        def __init__(self, host, port, timeout=30):
            smtp_calls.append({"mode": "starttls", "host": host, "port": port, "tls": False})

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def ehlo(self):
            return (250, b"OK")

        def starttls(self, context=None):
            smtp_calls[-1]["tls"] = True

        def login(self, user, pwd):
            smtp_calls[-1]["login"] = (user, pwd)

        def send_message(self, msg):
            smtp_calls[-1]["msg"] = msg

    monkeypatch.setattr(notify_email.smtplib, "SMTP", DummySTARTTLSServer)

    # 1. Missing SMTP secrets -> graceful skip
    monkeypatch.delenv("SMTP_USER", raising=False)
    monkeypatch.delenv("SMTP_PASS", raising=False)
    write_result({"status": "success", "skipped": False, "result_count": 1, "today_total": 1, "date": "2026-11-25"})
    assert notify_email.main(["--result-json", str(result_json)]) == 0
    assert smtp_calls == []

    # Configure Outlook SMTP (587 STARTTLS) + explicit MAIL_TO
    monkeypatch.setenv("SMTP_USER", "sender@outlook.com")
    monkeypatch.setenv("SMTP_PASS", "outlook-pass")
    monkeypatch.setenv("MAIL_TO", "dest1@example.com, dest2@example.com")

    # 2. Skipped slot -> skip
    write_result({"status": "skipped", "skipped": True, "result_count": 0, "today_total": 0, "date": "2026-11-25"})
    assert notify_email.main(["--result-json", str(result_json)]) == 0
    assert smtp_calls == []

    # 3. MAIL_SEND_EMPTY=0 skips clean empty run, but still sends warning/partial alert
    monkeypatch.setenv("MAIL_SEND_EMPTY", "0")
    write_result({"status": "success", "skipped": False, "result_count": 0, "today_total": 2, "date": "2026-11-25", "warning": "", "error": ""})
    assert notify_email.main(["--result-json", str(result_json)]) == 0
    assert smtp_calls == []

    write_result({"status": "partial", "skipped": False, "result_count": 0, "today_total": 2, "date": "2026-11-25", "schedule_slot": "00:03", "warning": "sublist 301 fallback"})
    assert notify_email.main(["--result-json", str(result_json)]) == 0
    assert len(smtp_calls) == 1
    assert smtp_calls[0]["host"] == "smtp-mail.outlook.com"
    assert smtp_calls[0]["port"] == 587
    assert smtp_calls[0]["tls"] is True
    assert smtp_calls[0]["msg"]["To"] == "dest1@example.com, dest2@example.com"
    assert "⚠️ 告警" in smtp_calls[0]["msg"]["Subject"]

    # 4. Dry-run toggle and subject prefix
    smtp_calls.clear()
    monkeypatch.setenv("MAIL_SEND_EMPTY", "1")
    monkeypatch.setenv("MAIL_ON_DRY_RUN", "0")
    write_result({"status": "success", "skipped": False, "dry_run": True, "result_count": 1, "today_total": 1, "date": "2026-11-25", "source": "manual"})
    assert notify_email.main(["--result-json", str(result_json)]) == 0
    assert smtp_calls == []

    monkeypatch.setenv("MAIL_ON_DRY_RUN", "1")
    assert notify_email.main(["--result-json", str(result_json)]) == 0
    assert len(smtp_calls) == 1
    assert smtp_calls[0]["msg"]["Subject"].startswith("[🧪 试跑·网易云新曲]")


def test_slow_retry_round_limit_and_ci_scan_flag(tmp_path, monkeypatch):
    import slow_retry

    assert slow_retry.effective_delays(1) == (30.0,)
    assert slow_retry.effective_delays(0) == ()
    assert slow_retry.effective_delays(None) == slow_retry.DELAYS

    monkeypatch.setattr(engine, "CONTROL", engine.ControlState(target_threads=2, baseline_threads=2))
    monkeypatch.setattr(engine, "METRICS", engine.Metrics())
    logs = []
    monkeypatch.setattr(engine, "emit", logs.append)
    monkeypatch.setattr(slow_retry, "DELAYS", (0.0, 0.0, 0.0))
    monkeypatch.setattr(slow_retry, "SPACING", 0.0)
    monkeypatch.setattr(slow_retry, "MAX_ROUNDS", 1)

    budget = slow_retry.RetryBudget()
    calls = []
    outcomes = list(slow_retry.retry_failed(["item1"], lambda x: (calls.append(x), False)[1], bool, "/album", str, budget))
    assert calls == ["item1"]
    assert len(outcomes) == 1
    assert budget.attempted == 1
    assert budget.stages["/album"]["remaining"] == 1
    assert any("第 1/1 轮" in msg.get("text", "") for msg in logs if msg.get("type") == "log")

    # Verify ci_scan CLI flag sets slow_retry.MAX_ROUNDS during execute_scan and restores it afterward.
    parsed = ci_scan._parse_args(["--data-dir", ".", "--slow-retry-rounds", "1"])
    assert parsed.slow_retry_rounds == 1

    data_dir = tmp_path / "retry_ci"
    data_dir.mkdir(parents=True)
    cfg_path = ci_scan._prepare_temp_config(
        tmp_path / "sec_retry",
        raw_cookie="MUSIC_U=retry_tok_123;",
        threads=2,
        recent_limit=50,
        owned_limit=50,
        new_album_probe_limit=0,
        artist_cache_key="",
    )
    monkeypatch.setattr(engine, "SCRIPT_DIR", data_dir)
    monkeypatch.setattr(engine, "CSV_FILE", data_dir / "VA_Tracks_Output.csv")
    monkeypatch.setattr(server, "DATA", data_dir)
    monkeypatch.setattr(server, "verify_login", lambda *a, **kw: None)

    observed_rounds = []

    def check_worker(*a, **kw):
        observed_rounds.append(slow_retry.MAX_ROUNDS)
        engine.emit({
            "type": "done",
            "results": [],
            "summary": {"artists": 1},
            "metrics": {"elapsed_sec": 0.1},
        })

    monkeypatch.setattr(slow_retry, "MAX_ROUNDS", None)
    rt = server.Runtime(config=server.Config(cfg_path), db_path=data_dir / "scan_history.sqlite3", worker=check_worker)
    try:
        args = ci_scan._parse_args([
            "--data-dir", str(data_dir),
            "--slow-retry-rounds", "1",
            "--result-json", str(tmp_path / "res.json"),
        ])
        assert ci_scan.execute_scan(rt, args, data_dir) == 0
        assert observed_rounds == [1]
        assert slow_retry.MAX_ROUNDS is None
    finally:
        rt.close()


def test_album_auto_skipped_after_three_failed_workflows_and_outage_guarded(tmp_path, monkeypatch):
    import slow_retry

    data_dir = tmp_path / "auto_skip"
    cache_dir = data_dir / "VA_Artist_Caches"
    cache_dir.mkdir(parents=True)

    monkeypatch.setattr(engine, "SCRIPT_DIR", data_dir)
    monkeypatch.setattr(engine, "CSV_FILE", data_dir / "VA_Tracks_Output.csv")
    monkeypatch.setattr(engine, "ALBUM_CACHE_FILE", data_dir / "VA_Album_Date_Cache.json")
    monkeypatch.setattr(engine, "ARTIST_CACHE_DIR", cache_dir)
    monkeypatch.setattr(engine, "METRICS_FILE", data_dir / "VA_Scan_Metrics.json")
    monkeypatch.setattr(engine, "REPORT_FILE", data_dir / "VA_Scan_Tuning_Report.txt")
    monkeypatch.setattr(engine, "ALBUM_CACHE", {})
    monkeypatch.setattr(engine, "_ALBUM_CACHE_DIRTY", False)
    monkeypatch.setattr(engine, "_ALBUM_CACHE_GEN", 0)
    monkeypatch.setattr(engine, "_ALBUM_CACHE_LOADED_FILE", None)
    monkeypatch.setattr(engine, "_SEEN_LOADED_CSV", None)
    monkeypatch.setattr(engine, "DYNAMIC_SKIP_ALBUM_IDS", set())
    monkeypatch.setattr(engine, "REFRESH_ARTIST_CACHE", False)
    monkeypatch.setattr(server, "DATA", data_dir)
    monkeypatch.setattr(server, "verify_login", lambda *a, **kw: None)
    monkeypatch.setattr(slow_retry, "DELAYS", (0.0,))
    monkeypatch.setattr(slow_retry, "SPACING", 0.0)

    cfg_path = ci_scan._prepare_temp_config(
        tmp_path / "sec_autoskip",
        raw_cookie="MUSIC_U=autoskip_tok_123;",
        threads=2,
        recent_limit=50,
        owned_limit=50,
        new_album_probe_limit=0,
        artist_cache_key="",
    )
    cfg = server.Config(cfg_path)
    today_date = datetime.now(engine.CN_TZ).date()
    today_ts = int(datetime(today_date.year, today_date.month, today_date.day, 12, tzinfo=engine.CN_TZ).timestamp() * 1000)

    album_api_calls = []

    def fake_api_request(path, params=None, **kwargs):
        params = dict(params or {})
        if path == "/artist/sublist":
            return {"code": 200, "data": [{"id": 1, "name": "Artist 1"}], "count": 1, "hasMore": False}
        if path == "/artist/songs":
            return {
                "code": 200,
                "songs": [
                    {"id": 1001, "name": "Good Track", "al": {"id": 200, "name": "Good Album"}, "ar": [{"id": 1, "name": "Artist 1"}]},
                    {"id": 1002, "name": "Ghost Track", "al": {"id": 410860166, "name": "Ghost Album"}, "ar": [{"id": 1, "name": "Artist 1"}]},
                ],
            }
        if path == "/artist/album":
            return {"code": 200, "hotAlbums": [], "more": False}
        if path == "/album":
            aid = str(params["id"])
            album_api_calls.append(aid)
            if aid == "410860166":
                raise engine.ApiError("/album", "返回码 404 [nonexistent]")
            return {"code": 200, "album": {"id": 200, "name": "Good Album", "publishTime": today_ts, "artists": []}, "songs": []}
        raise AssertionError(path)

    monkeypatch.setattr(engine, "api_request", fake_api_request)

    db_path = data_dir / "scan_history.sqlite3"
    rt = server.Runtime(config=cfg, db_path=db_path)
    try:
        reg_params = rt.parameters({"scanMode": "regular", "threads": 2, "newAlbumProbeLimit": 0}, today_date)

        # Workflows 1 & 2: 410860166 fails -> recorded in tracker, not skipped yet
        for expected_count in (1, 2):
            with rt.lock:
                rid = rt.store.create(reg_params, source="manual")
                rt._launch(rid, reg_params)
            rt.thread.join()
            run_rec = rt.store.get(rid)
            assert run_rec["status"] == "partial"
            assert run_rec["summary"]["album_errors"] == 1
            tracker = rt.store.load_album_failure_tracker()
            assert tracker["410860166"]["count"] == expected_count
            assert tracker["410860166"]["skipped"] is False
            assert "410860166" in album_api_calls

        # Workflow 3: 3rd failure marks 410860166 as skipped going forward
        album_api_calls.clear()
        with rt.lock:
            rid3 = rt.store.create(reg_params, source="manual")
            rt._launch(rid3, reg_params)
        rt.thread.join()
        assert rt.store.get(rid3)["status"] == "partial"
        tracker3 = rt.store.load_album_failure_tracker()
        assert tracker3["410860166"]["count"] == 3
        assert tracker3["410860166"]["skipped"] is True
        assert "410860166" in rt.store.skipped_album_ids()
        assert any("410860166" in row["text"] and "自动标记为不存在专辑" in row["text"] for row in rt.store.logs(rid3))

        # Workflow 4: 410860166 is completely skipped before /album & never triggers slow_retry
        album_api_calls.clear()
        with rt.lock:
            rid4 = rt.store.create(reg_params, source="manual")
            rt._launch(rid4, reg_params)
        rt.thread.join()
        run4 = rt.store.get(rid4)
        assert run4["status"] == "success"
        assert run4["summary"]["album_errors"] == 0
        assert run4["summary"]["slow_retry"]["attempted"] == 0
        assert "410860166" not in album_api_calls
        assert engine.DYNAMIC_SKIP_ALBUM_IDS == set()

        # Recovery clears consecutive streak before threshold
        rt.store.record_album_workflow_outcomes("rec_1", ["999001"], [])
        assert rt.store.load_album_failure_tracker()["999001"]["count"] == 1
        rt.store.record_album_workflow_outcomes("rec_2", [], ["999001"])
        assert "999001" not in rt.store.load_album_failure_tracker()

        # Outage guard ignores large mass failures (>50)
        mass_ids = [str(700000 + i) for i in range(55)]
        assert rt.store.record_album_workflow_outcomes("outage_run", mass_ids, []) == []
        assert "700000" not in rt.store.load_album_failure_tracker()

        # state_store.compact_sqlite preserves album_failure_tracker across pruning
        rt.store.set_json("m3_match_report:old_1", {"a": 1})
        rt.store.set_json("m3_match_report:old_2", {"a": 2})
        compacted_db = tmp_path / "compacted.sqlite3"
        state_store.compact_sqlite(db_path, compacted_db, keep_m3_reports=1)
        assert server.Store(compacted_db).skipped_album_ids() == {"410860166"}
    finally:
        rt.close()













