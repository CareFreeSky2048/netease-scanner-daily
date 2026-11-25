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
from datetime import date, datetime, timedelta
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






