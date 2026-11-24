import csv
import io
import json
import queue
import sqlite3
import threading
import weakref
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

import engine
import server
import slow_retry
from server import Broadcast, BusyError, Config, Runtime, Store, TZ, create_app, default_dates


@pytest.fixture
def environment(tmp_path, monkeypatch):
    monkeypatch.setattr(slow_retry, 'DELAYS', (0, 0, 0))
    monkeypatch.setattr(slow_retry, 'SPACING', 0)
    for name, value in {
        "SCRIPT_DIR": tmp_path,
        "CSV_FILE": tmp_path / "VA_Tracks_Output.csv",
        "ARTIST_CACHE_DIR": tmp_path / "VA_Artist_Caches",
        "ALBUM_CACHE_FILE": tmp_path / "VA_Album_Date_Cache.json",
        "METRICS_FILE": tmp_path / "VA_Scan_Metrics.json",
        "REPORT_FILE": tmp_path / "VA_Scan_Tuning_Report.txt",
    }.items():
        monkeypatch.setattr(engine, name, value)
    monkeypatch.setattr(server, "DATA", tmp_path)
    engine.ARTIST_CACHE_DIR.mkdir()
    engine.ALBUM_CACHE.clear()
    engine.ARTIST_ALBUM_PAGE_CACHE.clear()
    engine.SEEN_LINKS.clear()
    engine.SEEN_SONG_IDS.clear()
    cookie = tmp_path / "cookie.txt"
    cookie.write_text("test-secret-credential", encoding="utf-8")
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"schedule_enabled": False, "cookie_file": str(cookie)}), encoding="utf-8")
    monkeypatch.setattr(server, "verify_login", lambda *args: None)
    return tmp_path, Config(config)


def done_worker(*args):
    engine.emit({"type": "done", "results": [], "summary": {"artists": 1}})


def wait_for(runtime):
    runtime.thread.join(timeout=5)
    assert not runtime.thread.is_alive()
    assert runtime.current is None


def enable(config):
    value = json.loads(config.path.read_text())
    value["schedule_enabled"] = True
    config.path.write_text(json.dumps(value))


@pytest.mark.parametrize("day,expected", [(date(2026, 3, 31), "2026-02-28"), (date(2024, 3, 31), "2024-02-29"),
                                           (date(2026, 1, 1), "2025-12-01"), (date(2026, 10, 7), "2026-09-07")])
def test_calendar_month_dates(day, expected):
    params = default_dates(day)
    assert params["externalStartDate"] == expected
    assert params["externalEndDate"] == day.isoformat()


def test_m3_cutoff_and_explicit_mode_dates(environment):
    _, config = environment
    runtime = Runtime(config, worker=done_worker)
    before = runtime.parameters({"scanMode": "auto"}, date(2026, 11, 25))
    after = runtime.parameters({"scanMode": "auto"}, date(2026, 11, 26))
    manual = runtime.parameters({"scanMode": "m3"}, date(2026, 12, 1))
    intersection = runtime.parameters({"scanMode": "m3_intersection"}, date(2026, 12, 1))
    custom = runtime.parameters({"scanMode": "m3", "externalStartDate": "2026-10-30"}, date(2026, 12, 1))
    assert before["scanMode"] == "m3" and before["externalStartDate"] == "2026-10-24"
    assert after["scanMode"] == "regular"
    assert manual["scanMode"] == "m3" and manual["externalStartDate"] == "2026-10-24"
    assert manual["newStartDate"] == "2026-11-30"
    assert intersection["scanMode"] == "m3_intersection" and intersection["externalStartDate"] == "2026-10-24"
    assert intersection["newStartDate"] == "2026-10-24" and intersection["newEndDate"] == "2026-12-04"
    assert custom["externalStartDate"] == "2026-10-30"
    custom_intersection = runtime.parameters({"scanMode": "m3_intersection", "newStartDate": "2026-10-30"}, date(2026, 12, 1))
    assert custom_intersection["newStartDate"] == "2026-10-30"
    regular = runtime.parameters({"scanMode": "regular"}, date(2026, 12, 1))
    assert regular["newStartDate"] == "2026-11-30"
    runtime.close()


@pytest.mark.parametrize("scan_mode", ["m3", "m3_intersection"])
def test_m3_scan_runs_ordered_rounds_merges_heat_and_saves_match_report(environment, monkeypatch, scan_mode):
    root, config = environment
    m3_file = root / "m3_names.json"
    m3_file.write_text(json.dumps({"artists": [{"m3_artist_id": "artist_alpha", "name": "Alpha"}]}, ensure_ascii=False), encoding="utf-8")
    config_data = json.loads(config.path.read_text(encoding="utf-8"))
    config_data["m3_artist_file"] = str(m3_file)
    config.path.write_text(json.dumps(config_data), encoding="utf-8")
    followed = [{"id": "1", "name": "alpha"}, {"id": "2", "name": "Beta"}]
    monkeypatch.setattr(engine, "resolve_artist_list", lambda *args: (followed, "1234567890abcdef", "live"))
    groups = []

    def round_worker(*args, artists_override=None, artist_source_label="", **kwargs):
        groups.append(([artist["id"] for artist in artists_override], artist_source_label,
                       [value.isoformat() for value in args[2:6]], kwargs))
        artist = artists_override[0]
        hit = {"name": artist["name"], "song_name": f"song-{artist['id']}",
               "album_name": "shared", "date_str": "2026-10-25", "link": f"https://music.163.com/#/song?id={artist['id']}",
               "flag": 0, "album_id": "900", "album_count": 1}
        engine.emit({"type": "done", "results": [hit], "summary": {
            "artists": 1, "relations": 1, "unique_albums": 1, "date_hit_albums": 1, "verify_artists": 1,
            "artist_failures": 0, "new_album_probe_failures": 0, "album_errors": 0, "owned_errors": 0,
            "album_artist_counts": {"900": 1}, "album_artist_names": {"900": [artist["name"]]},
            "unique_album_ids": ["900"], "date_hit_album_ids": ["900"],
            "verify_artist_ids": [artist["id"]], "round_artist_names": [artist["name"]],
            "failure_details": [], "slow_retry": {"attempted": 0, "recovered": 0},
        }})

    monkeypatch.setattr(server, "verify_login", lambda *args: None)
    runtime = Runtime(config, worker=round_worker)
    # The worker is injected for this integration test, but the test explicitly
    # enables M3 orchestration to exercise two-round behavior without HTTP calls.
    runtime.use_m3_rounds = True
    run_id = runtime.start({"scanMode": scan_mode})
    wait_for(runtime)
    run = runtime.store.get(run_id)
    hits = runtime.store.hits(run_id)
    intersection_only = scan_mode == "m3_intersection"
    expected_count = 1 if intersection_only else 2
    assert [group[0] for group in groups] == ([["1"]] if intersection_only else [["1"], ["2"]])
    if intersection_only:
        assert groups[0][2][2] == "2026-10-24T00:00:00"
    else:
        assert groups[0][2][2] == "2026-10-24T00:00:00"
        assert groups[1][2][2] == f"{run['params']['newStartDate']}T00:00:00"
    assert groups[0][3]["finalize"] is intersection_only and groups[0][3]["reset_seen"] is True
    if intersection_only:
        assert groups[0][1] == "M3-intersection"
    else:
        assert groups[1][3]["finalize"] is True and groups[1][3]["reset_seen"] is False
    assert run["status"] == "success"
    assert run["summary"]["artists"] == expected_count and run["summary"]["unique_albums"] == 1
    assert run["summary"]["album_artist_counts"] == {"900": expected_count}
    assert {hit["album_count"] for hit in hits} == {expected_count}
    assert len(hits) == expected_count
    assert run["summary"]["m3_scan"]["match_count"] == 1
    assert run["summary"]["m3_scan"]["matched_account_count"] == 1
    assert run["summary"]["m3_scan"]["intersection_only"] is intersection_only
    assert run["summary"]["m3_scan"]["second_round_count"] == (0 if intersection_only else 1)
    assert run["summary"]["m3_scan"]["excluded_count"] == (1 if intersection_only else 0)
    assert run["metrics"]["artist_total"] == expected_count
    client = create_app(runtime).test_client()
    report = client.get(f"/api/runs/{run_id}/m3").json
    assert report["summary"]["matched"] == 1
    assert report["intersection_only"] is intersection_only
    assert len(report["rounds"]) == expected_count
    assert report["matches"][0]["m3_artist_id"] == "artist_alpha"
    assert report["unmatched_followed"][0]["netease_name"] == "Beta"
    csv_report = client.get(f"/api/runs/{run_id}/m3.csv")
    assert csv_report.status_code == 200
    assert ("未纳入交集·无匹配" if intersection_only else "第二轮·未匹配") in csv_report.get_data(as_text=True)
    assert "M3 艺人 ID" in csv_report.get_data(as_text=True)
    assert "artist_alpha" in csv_report.get_data(as_text=True)
    runtime.close()


@pytest.mark.parametrize("has_match", [True, False])
def test_m3_intersection_excludes_ambiguous_and_unmatched_without_fallback(environment, monkeypatch, has_match):
    root, config = environment
    m3_file = root / "m3_names.json"
    m3_file.write_text(json.dumps({"artists": [
        {"m3_artist_id": "alpha", "name": "ALPHA"},
        {"m3_artist_id": "echo", "name": "Echo"}]}), encoding="utf-8")
    value = json.loads(config.path.read_text())
    value["m3_artist_file"] = str(m3_file)
    config.path.write_text(json.dumps(value), encoding="utf-8")
    followed = [{"id": "2", "name": "Echo"}, {"id": "3", "name": "ECHO"},
                {"id": "4", "name": "Unmatched"}]
    if has_match:
        followed.insert(0, {"id": "1", "name": "alpha"})
    monkeypatch.setattr(engine, "resolve_artist_list", lambda *args: (followed, "1234567890abcdef", "live"))
    calls = []
    def worker(*args, artists_override=None, **kwargs):
        calls.append(artists_override)
        engine.emit({"type": "done", "results": [], "summary": {"artists": len(artists_override)}})
    runtime = Runtime(config, worker=worker)
    runtime.use_m3_rounds = True
    client = create_app(runtime).test_client()
    reply = client.post("/api/start", json={"scanMode": "m3_intersection"}, headers={"X-Scanner-Request": "1"})
    assert reply.status_code == 200
    wait_for(runtime)
    run_id = reply.json["run_id"]
    run = runtime.store.get(run_id)
    assert calls == ([[followed[0]]] if has_match else [])
    assert run["status"] == ("success" if has_match else "failed")
    if has_match:
        assert run["summary"]["artists"] == 1
        assert run["summary"]["m3_scan"]["ambiguous_count"] == 2
        assert run["summary"]["m3_scan"]["excluded_count"] == 3
    else:
        assert "没有可扫描的关注艺人" in run["error"]
    csv_report = client.get(f"/api/runs/{run_id}/m3.csv").get_data(as_text=True)
    assert "未纳入交集·歧义" in csv_report and "未纳入交集·无匹配" in csv_report
    assert "第二轮" not in csv_report
    runtime.close()


def test_m3_intersection_scans_confirmed_duplicate_accounts_as_one_logical_artist(environment, monkeypatch):
    root, config = environment
    m3_file = root / "m3_names.json"
    m3_file.write_text(json.dumps({"artists": [{"m3_artist_id": "ariiol-m3", "name": "ariiol"}]}), encoding="utf-8")
    value = json.loads(config.path.read_text(encoding="utf-8"))
    value["m3_artist_file"] = str(m3_file)
    config.path.write_text(json.dumps(value), encoding="utf-8")
    followed = [{"id": "122335233", "name": "ariiol"},
                {"id": "35882993", "name": "ariiol"}]
    monkeypatch.setattr(engine, "resolve_artist_list", lambda *args: (followed, "1234567890abcdef", "live"))
    scanned = []

    def worker(*args, artists_override=None, **kwargs):
        scanned.extend(artists_override)
        logical_names = sorted({artist.get("m3_logical_artist", artist["name"])
                                for artist in artists_override})
        engine.emit({"type": "done", "results": [], "summary": {
            "artists": len(artists_override), "album_artist_counts": {"900": len(logical_names)},
            "album_artist_names": {"900": logical_names},
        }})

    monkeypatch.setattr(server, "verify_login", lambda *args: None)
    runtime = Runtime(config, worker=worker)
    runtime.use_m3_rounds = True
    client = create_app(runtime).test_client()
    reply = client.post("/api/start", json={"scanMode": "m3_intersection"},
                        headers={"X-Scanner-Request": "1"})
    assert reply.status_code == 200
    wait_for(runtime)

    run = runtime.store.get(reply.json["run_id"])
    report = client.get(f"/api/runs/{reply.json['run_id']}/m3").json
    assert [artist["id"] for artist in scanned] == ["122335233", "35882993"]
    assert {artist["m3_logical_artist"] for artist in scanned} == {"ariiol"}
    assert run["summary"]["m3_scan"]["match_count"] == 1
    assert run["summary"]["m3_scan"]["matched_account_count"] == 2
    assert run["summary"]["album_artist_counts"] == {"900": 1}
    assert report["summary"]["matched"] == 1
    assert report["summary"]["matched_accounts"] == 2
    assert {match["logical_artist_id"] for match in report["matches"]} == {"ariiol"}
    runtime.close()


def test_m3_intersection_available_in_manual_defaults_but_rejected_as_schedule_default(environment):
    _, config = environment
    runtime = Runtime(config, worker=done_worker)
    client = create_app(runtime).test_client()
    status = client.get("/api/status").json
    assert status["mode_defaults"]["m3_intersection"]["scanMode"] == "m3_intersection"
    assert status["scan_mode"] == "auto"
    assert '<option value="m3_intersection">仅 M3 交集（手动）</option>' in client.get("/").get_data(as_text=True)
    runtime.close()
    value = json.loads(config.path.read_text())
    value["scan_mode"] = "m3_intersection"
    config.path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="scan_mode"):
        config.read()


def test_api_records_results_without_browser_and_reloads(environment):
    root, config = environment
    hit = {"name": "艺人", "song_name": "曲目", "album_name": "合辑", "date_str": "2026-10-07",
           "link": "https://music.163.com/#/song?id=1", "flag": 1}
    def worker(*args):
        engine.emit({"type": "hit", "data": hit})
        engine.emit({"type": "done", "results": [hit], "summary": {"artists": 1}})
    runtime = Runtime(config, worker=worker)
    client = create_app(runtime).test_client()
    reply = client.post("/api/start", json={}, headers={"X-Scanner-Request": "1"})
    assert reply.status_code == 200
    run_id = reply.json["run_id"]
    wait_for(runtime)
    fresh = Store(root / "scan_history.sqlite3")
    assert fresh.get(run_id)["status"] == "success"
    assert fresh.hits(run_id) == [hit]
    result = client.get(f"/api/runs/{run_id}").json
    assert result["run"]["ended_at"]
    assert result["results"][0]["song_name"] == "曲目"
    assert client.get(f"/api/runs/{run_id}/csv").data.startswith(b"\xef\xbb\xbf")
    assert "test-secret-credential" not in json.dumps(result)
    runtime.close()


def test_today_results_use_task_day_not_release_date(environment, monkeypatch):
    _, config = environment
    runtime = Runtime(config, worker=done_worker)
    client = create_app(runtime).test_client()
    def add_run(day, source, created_at, status, song_id):
        monkeypatch.setattr(server, 'now_iso', lambda: created_at)
        run_id = runtime.store.create(default_dates(date.fromisoformat(day)), source,
                                      day if source == 'scheduled' else None, status)
        hit = {'name': '艺人', 'song_name': f'曲目{song_id}', 'album_name': '合辑',
               'date_str': '2026-09-30', 'link': f'https://music.163.com/#/song?id={song_id}', 'flag': 1}
        runtime.store.hit(run_id, hit)
        return run_id
    # A queued scheduled scan retains its assigned day even if it starts late.
    scheduled = add_run('2026-10-07', 'scheduled', '2026-10-08T00:01:00+08:00', 'partial', 1)
    manual = add_run('2026-10-07', 'manual', '2026-10-07T23:59:00+08:00', 'success', 2)
    add_run('2026-10-06', 'scheduled', '2026-10-07T00:01:00+08:00', 'success', 3)
    add_run('2026-10-08', 'manual', '2026-10-08T00:01:00+08:00', 'success', 4)
    runtime.store.update(scheduled, warning='关注缓存刷新失败，回退缓存', summary={'artist_failures': 1})
    result = client.get('/api/today?date=2026-10-07&limit=1').json
    assert result['date'] == '2026-10-07' and result['timezone'] == 'Asia/Shanghai'
    assert result['total'] == 2 and len(result['results']) == 1
    assert {run['id'] for run in result['runs']} == {scheduled, manual}
    assert any(run['status'] == 'partial' and run['warning'] for run in result['runs'])
    page2 = client.get('/api/today?date=2026-10-07&limit=1&offset=1').json
    assert page2['results'][0]['link'] != result['results'][0]['link']
    response = client.get('/api/today/csv?date=2026-10-07')
    rows = list(csv.reader(io.StringIO(response.data.decode('utf-8-sig'))))
    assert len(rows) == 3
    assert 'scan-2026-10-07.csv' in response.headers['Content-Disposition']
    assert {row[4] for row in rows[1:]} == {hit['link'] for hit in runtime.store.day_hits('2026-10-07')}
    runtime.close()


def test_today_empty_failure_and_server_timezone(environment, monkeypatch):
    _, config = environment
    runtime = Runtime(config, worker=done_worker)
    client = create_app(runtime).test_client()
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            # UTC Oct 6 is already Oct 7 in Beijing.
            return datetime(2026, 10, 6, 16, 4, tzinfo=timezone.utc).astimezone(tz)
    monkeypatch.setattr(server, 'datetime', FixedDatetime)
    empty = client.get('/api/today').json
    assert empty == {'date': '2026-10-07', 'timezone': 'Asia/Shanghai', 'runs': [], 'total': 0,
                     'results': [], 'albums': [], 'total_albums': 0}
    run_id = runtime.store.create(default_dates(date(2026, 10, 7)), 'scheduled', '2026-10-07', 'failed')
    runtime.store.update(run_id, error='Cookie 已失效')
    failed = client.get('/api/today').json
    assert failed['total'] == 0 and failed['runs'][0]['error'] == 'Cookie 已失效'
    assert failed['runs'][0]['status'] == 'failed'
    for path in ('/api/today?date=bad', '/api/today/csv?date=2026-02-30', '/api/today?offset=bad'):
        assert client.get(path).status_code == 400
    runtime.close()


def test_album_result_view_pages_albums_and_expands_tracks(environment):
    root, config = environment
    runtime = Runtime(config, worker=done_worker)
    run_id = runtime.store.create({}, status="success")
    rows = [
        {"name": "Artist A", "song_name": "Track 1", "album_name": "Shared Album", "album_id": "100",
         "album_count": 3, "date_str": "2026-11-15", "link": "https://music.163.com/#/song?id=1"},
        {"name": "Artist B", "song_name": "Track 2", "album_name": "Shared Album", "album_id": "100",
         "album_count": 3, "date_str": "2026-11-14", "link": "https://music.163.com/#/song?id=2"},
        {"name": "Artist C", "song_name": "Other Track", "album_name": "Other Album", "album_id": "200",
         "album_count": 1, "date_str": "2026-11-13", "link": "https://music.163.com/#/song?id=3"},
    ]
    runtime.store.update(run_id, status="success", ended_at=server.now_iso(), summary={})
    for row in rows:
        runtime.store.hit(run_id, row)
    client = create_app(runtime).test_client()

    page = client.get(f"/api/runs/{run_id}?album_limit=1&album_offset=0").json
    assert page["total_albums"] == 2
    assert page["albums"][0]["album_name"] == "Shared Album"
    assert page["albums"][0]["album_id"] == "100"
    assert page["albums"][0]["track_count"] == 2
    expanded = client.get("/api/album-tracks", query_string={
        "source": "run", "run_id": run_id, "album": "100"}).json
    assert [track["song_name"] for track in expanded["results"]] == ["Track 1", "Track 2"]

    today = datetime.now(TZ).date().isoformat()
    scheduled_id = runtime.store.create({}, source="scheduled", schedule_date=today,
                                        schedule_time="00:03", status="success")
    runtime.store.update(scheduled_id, status="success", ended_at=server.now_iso(), summary={})
    runtime.store.hit(scheduled_id, {**rows[0], "name": "Scheduled A", "song_name": "Scheduled Track A",
                                     "link": "https://music.163.com/#/song?id=4"})
    runtime.store.hit(scheduled_id, {**rows[2], "name": "Scheduled B", "song_name": "Scheduled Track B",
                                     "link": "https://music.163.com/#/song?id=5"})
    album_a = client.get("/api/album-tracks", query_string={
        "source": "today", "date": today, "album": "100"}).json
    album_b = client.get("/api/album-tracks", query_string={
        "source": "today", "date": today, "album": "200"}).json
    assert {track["song_name"] for track in album_a["results"]} == {"Track 1", "Track 2", "Scheduled Track A"}
    assert {track["song_name"] for track in album_b["results"]} == {"Other Track", "Scheduled Track B"}
    runtime.close()


def test_record_export_whitelist_bytes_snapshot_and_cleanup(environment):
    root, config = environment
    import hashlib
    from zipfile import ZipFile
    records = {'VA_Tracks_Output.csv': b'\xef\xbb\xbforiginal\r\n',
               'VA_Album_Date_Cache.json': b'{"1": {}}',
               'VA_Scan_Metrics.json': b'{}', 'VA_Scan_Tuning_Report.txt': '报告\r\n'.encode(),
               'VA_Artist_Caches/artists_1234567890abcdef.json': b'{"artists": []}'}
    for name, data in records.items():
        (root / name).write_bytes(data)
    for name in ['cookie.txt', 'VA_State.json', 'my_artists.json', 'VA_Artist_Caches/cookie.json']:
        (root / name).write_text('private-not-exported')
    runtime = Runtime(config, worker=done_worker)
    client = create_app(runtime).test_client()
    assert client.post('/api/records/export', json={}).status_code == 403
    response = client.post('/api/records/export', json={}, headers={'X-Scanner-Request': '1'})
    assert response.status_code == 200
    snapshot = response.json
    assert {f['path'] for f in snapshot['files']} == set(records)
    token = snapshot['export_id']
    # Subsequent writes to live data do not change this export.
    (root / 'VA_Tracks_Output.csv').write_bytes(b'next-scan')
    for file in snapshot['files']:
        data = client.get(f"/api/records/{token}/file/{file['path']}").data
        assert data == records[file['path']]
        assert hashlib.sha256(data).hexdigest() == file['sha256']
    archive_response = client.get(f'/api/records/{token}/zip')
    with ZipFile(io.BytesIO(archive_response.data)) as archive:
        assert set(archive.namelist()) == set(records) | {'MANIFEST.json'}
        assert json.loads(archive.read('MANIFEST.json'))['files'] == snapshot['files']
        assert archive.read('VA_Tracks_Output.csv') == records['VA_Tracks_Output.csv']
    for name in ('cookie.txt', 'VA_State.json', '../cookie.txt', 'VA_Artist_Caches/cookie.json'):
        assert client.get(f'/api/records/{token}/file/{name}').status_code == 404
    # Close streaming responses so Windows can delete their temporary files.
    response.close(); archive_response.close()
    assert client.post(f'/api/records/{token}/release', json={}, headers={'X-Scanner-Request': '1'}).status_code == 200
    assert client.get(f'/api/records/{token}/zip').status_code == 404
    assert (root / 'cookie.txt').read_text() == 'private-not-exported'
    runtime.close()


def test_record_export_refuses_running_or_waiting_scans(environment):
    root, config = environment
    (root / 'VA_Tracks_Output.csv').write_text('record')
    runtime = Runtime(config, worker=done_worker)
    client = create_app(runtime).test_client()
    runtime.current = 'running-test'
    assert client.post('/api/records/export', json={}, headers={'X-Scanner-Request': '1'}).status_code == 409
    runtime.current = None
    runtime.store.create(default_dates(date(2026, 10, 7)))
    assert client.post('/api/records/export', json={}, headers={'X-Scanner-Request': '1'}).status_code == 409
    assert not runtime.record_exports.entries
    runtime.close()


def test_record_export_expiry_limit_and_symlink_protection(tmp_path, monkeypatch):
    from record_export import ExportLimitError, RecordExports
    import record_export
    (tmp_path / 'VA_Tracks_Output.csv').write_text('record')
    exports = RecordExports(tmp_path, ttl=10, capacity=1)
    clock = [0]
    monkeypatch.setattr(record_export.time, 'monotonic', lambda: clock[0])
    token = exports.create('test')['export_id']
    with pytest.raises(ExportLimitError):
        exports.create('test')
    clock[0] = 11
    assert exports.file(token, 'VA_Tracks_Output.csv') is None
    # Deterministic link refusal without requiring Windows symlink privileges.
    original = Path.is_symlink
    monkeypatch.setattr(Path, 'is_symlink', lambda p: p.name == 'VA_Tracks_Output.csv' or original(p))
    with pytest.raises(ValueError):
        exports.create('test')
    assert not exports.entries
    exports.close()


@pytest.mark.parametrize('limit', [2, 4, 8])
def test_small_host_limits_default_manual_and_live_concurrency(environment, monkeypatch, limit):
    _, config = environment
    monkeypatch.setenv('NETEASE_MAX_THREADS', str(limit))
    runtime = Runtime(config, worker=done_worker)
    client = create_app(runtime).test_client()
    headers = {'X-Scanner-Request': '1'}
    status = client.get('/api/status').json
    default_threads = min(5, limit)
    assert status['thread_limit'] == limit and status['defaults']['threads'] == default_threads
    assert [status['defaults'][key] for key in ('recentLimit', 'ownedLimit', 'newAlbumProbeLimit')] == [50, 50, 20]
    assert client.post('/api/start', json={'threads': limit + 1}, headers=headers).status_code == 400
    response = client.post('/api/start', json={}, headers=headers)
    assert response.status_code == 200
    wait_for(runtime)
    assert runtime.store.get(response.json['run_id'])['params']['threads'] == default_threads
    response = client.post('/api/start', json={'threads': limit}, headers=headers)
    assert response.status_code == 200
    wait_for(runtime)
    assert runtime.store.get(response.json['run_id'])['params']['threads'] == limit
    runtime.current = response.json['run_id']
    assert client.post('/api/control', json={'action': 'threads', 'value': limit + 1}, headers=headers).status_code == 400
    assert client.post('/api/control', json={'action': 'threads', 'value': limit}, headers=headers).status_code == 200
    assert client.post('/api/control', json={'action': 'threads', 'value': 1}, headers=headers).status_code == 200
    runtime.current = None
    runtime.close()


def test_0003_and_single_catchup_per_shanghai_day(environment):
    _, config = environment
    runtime = Runtime(config, worker=done_worker)
    enable(config)
    runtime.tick(datetime(2026, 10, 7, 0, 2, 59, tzinfo=TZ), do_backup=False)
    assert not runtime.store.list()
    runtime.tick(datetime(2026, 10, 6, 16, 3, tzinfo=timezone.utc), do_backup=False)
    wait_for(runtime)
    assert runtime.store.list()[0]["schedule_date"] == "2026-10-07"
    runtime.tick(datetime(2026, 10, 7, 16, tzinfo=TZ), do_backup=False)
    assert len(runtime.store.list()) == 1
    runtime.tick(datetime(2026, 10, 8, 16, tzinfo=TZ), do_backup=False)
    wait_for(runtime)
    assert len(runtime.store.list()) == 2
    runtime.close()


def enable_dual_schedule(config, starts=None):
    value=json.loads(config.path.read_text())
    value.update(schedule_enabled=True,schedule_times=['00:03','04:00'],schedule_start_dates=starts or {},threads=4)
    config.path.write_text(json.dumps(value))


def enable_m3_daytime_schedule(config, starts=None):
    enable_dual_schedule(config, starts)
    value = json.loads(config.path.read_text())
    slots = [f'{hour:02d}:{minute:02d}' for hour in range(12, 17) for minute in range(0, 60, 15)
             if (hour, minute) <= (16, 30)]
    value.update(schedule_times=['00:03', '04:00', *slots],
                 schedule_modes={slot: 'm3_intersection' for slot in slots},
                 schedule_end_dates={slot: '2026-11-25' for slot in slots})
    config.path.write_text(json.dumps(value))


def test_m3_daytime_exact_times_in_shanghai_and_inclusive_cutoff(environment):
    _, config = environment
    enable_m3_daytime_schedule(config)
    runtime = Runtime(config, worker=done_worker)
    # Keep all slots queued as if a manual scan were still running.
    runtime.current = 'manual-running'
    runtime.tick(datetime(2026, 11, 25, 11, 59, 59, tzinfo=TZ), do_backup=False)
    assert {r['schedule_time'] for r in runtime.store.list()} == {'00:03', '04:00'}
    runtime.tick(datetime(2026, 11, 25, 4, 0, tzinfo=timezone.utc), do_backup=False)
    noon = next(r for r in runtime.store.list() if r['schedule_time'] == '12:00')
    assert noon['status'] == 'queued' and noon['schedule_date'] == '2026-11-25'
    assert noon['params']['scanMode'] == 'm3_intersection'
    assert noon['params']['externalStartDate'] == '2026-10-24'
    assert noon['params']['externalEndDate'] == '2026-11-25'
    assert noon['params']['newStartDate'] == '2026-10-24'
    assert noon['params']['threads'] == 4
    runtime.tick(datetime(2026, 11, 25, 16, 29, 59, tzinfo=TZ), do_backup=False)
    assert len(runtime.store.list()) == 20
    runtime.tick(datetime(2026, 11, 25, 8, 30, tzinfo=timezone.utc), do_backup=False)
    runtime.tick(datetime(2026, 11, 25, 23, 59, tzinfo=TZ), do_backup=False)
    runs = runtime.store.day_runs('2026-11-25')
    assert len(runs) == 21
    assert {r['schedule_time']: r['params']['scanMode'] for r in runs if r['schedule_time'] in {'00:03', '04:00'}} == {
        '00:03': 'm3', '04:00': 'm3'}
    assert all(r['params']['scanMode'] == 'm3_intersection' for r in runs
               if r['schedule_time'] not in {'00:03', '04:00'})
    runtime.current = None
    runtime.close()


def test_every_quarter_hour_creates_one_task_without_duplicates(environment):
    _, config = environment
    enable_m3_daytime_schedule(config)
    runtime = Runtime(config, worker=done_worker)
    runtime.current = 'manual-running'
    for index, slot in enumerate(config.read()['schedule_times'][2:]):
        hour, minute = map(int, slot.split(':'))
        instant = datetime(2026, 10, 28, hour, minute, tzinfo=TZ)
        runtime.tick(instant - timedelta(seconds=1), do_backup=False)
        assert len(runtime.store.day_runs('2026-10-28')) == 2 + index
        runtime.tick(instant, do_backup=False)
        runtime.tick(instant, do_backup=False)
        runs = runtime.store.day_runs('2026-10-28')
        assert len(runs) == 3 + index
        assert sum(r['schedule_time'] == slot for r in runs) == 1
    runtime.tick(datetime(2026, 10, 28, 16, 45, tzinfo=TZ), do_backup=False)
    assert len(runtime.store.day_runs('2026-10-28')) == 21
    runtime.current = None
    runtime.close()


def test_m3_daytime_stops_after_nov25_and_does_not_backfill_previous_day(environment):
    _, config = environment
    enable_m3_daytime_schedule(config)
    runtime = Runtime(config, worker=done_worker)
    runtime.current = 'manual-running'
    runtime.tick(datetime(2026, 11, 26, 18, tzinfo=TZ), do_backup=False)
    runs = runtime.store.list()
    assert len(runs) == 2
    assert {r['schedule_time'] for r in runs} == {'00:03', '04:00'}
    assert all(r['schedule_date'] == '2026-11-26' and r['params']['scanMode'] == 'regular' for r in runs)
    runtime.current = None
    runtime.close()


def test_m3_daytime_queue_keeps_mode_and_window_and_restart_does_not_duplicate(environment):
    _, config = environment
    enable_m3_daytime_schedule(config)
    runtime = Runtime(config, worker=done_worker)
    runtime.current = 'manual-running'
    runtime.tick(datetime(2026, 10, 27, 16, 30, tzinfo=TZ), do_backup=False)
    queued = next(r for r in runtime.store.pending() if r['schedule_time'] == '12:00')
    runtime.current = None
    runtime.close()
    runtime = Runtime(config, worker=done_worker)
    for _ in range(21):
        runtime.tick(datetime(2026, 10, 27, 20, tzinfo=TZ), do_backup=False)
        wait_for(runtime)
    runs = runtime.store.day_runs('2026-10-27')
    assert len(runs) == 21 and all(r['status'] == 'success' for r in runs)
    finished = runtime.store.get(queued['id'])
    assert finished['params'] == queued['params']
    assert finished['params']['scanMode'] == 'm3_intersection'
    assert finished['params']['newStartDate'] == '2026-10-24'
    runtime.tick(datetime(2026, 10, 27, 23, tzinfo=TZ), do_backup=False)
    assert len(runtime.store.list()) == 21
    status = create_app(runtime).test_client().get('/api/status').json
    assert status['schedule_modes'] == config.read()['schedule_modes']
    assert status['schedule_end_dates'] == config.read()['schedule_end_dates']
    assert status['schedule'] == '00:03 / 04:00 / 12:00–16:30（每15分钟）'
    assert [s['mode'] for s in status['schedule_details'][:2]] == ['auto', 'auto']
    assert all(s['mode'] == 'm3_intersection' for s in status['schedule_details'][2:])
    assert all(s['active'] for s in server.schedule_details(config.read(), date(2026, 11, 25)))
    assert [s['active'] for s in server.schedule_details(config.read(), date(2026, 11, 26))] == [True, True] + [False] * 19
    runtime.close()


def test_m3_daytime_start_date_prevents_immediate_catchup(environment):
    _, config = environment
    starts = {f'{hour:02d}:{minute:02d}': '2026-10-27'
              for hour in range(12, 17) for minute in range(0, 60, 15)
              if (hour, minute) <= (16, 30)}
    enable_m3_daytime_schedule(config, starts)
    runtime = Runtime(config, worker=done_worker)
    runtime.current = 'manual-running'
    runtime.tick(datetime(2026, 10, 26, 20, tzinfo=TZ), do_backup=False)
    assert len(runtime.store.list()) == 2
    runtime.tick(datetime(2026, 10, 27, 12, tzinfo=TZ), do_backup=False)
    assert {r['schedule_time'] for r in runtime.store.day_runs('2026-10-27')} == {'00:03', '04:00', '12:00'}
    assert all(r['status'] == 'interrupted' for r in runtime.store.day_runs('2026-10-26'))
    runtime.current = None
    runtime.close()


def test_all_four_cron_jobs_registered(environment, monkeypatch):
    from apscheduler.schedulers.background import BackgroundScheduler
    _, config = environment
    enable_m3_daytime_schedule(config)
    runtime = Runtime(config, worker=done_worker)
    monkeypatch.setattr(runtime, 'safe_tick', lambda: None)
    monkeypatch.setattr(BackgroundScheduler, 'start', lambda self: None)
    runtime.start_scheduler()
    jobs = {job.id: job for job in runtime.scheduler.get_jobs()}
    for slot in ('0003', '0400', '1200', '1630'):
        assert f'daily-scan-{slot}' in jobs
        assert str(jobs[f'daily-scan-{slot}'].trigger.timezone) == 'Asia/Shanghai'
    runtime.close()


@pytest.mark.parametrize('key,value', [
    ('schedule_modes', []), ('schedule_modes', {'12:00': 'unknown'}),
    ('schedule_modes', {'12:00': []}), ('schedule_modes', {'12:0': 'm3_intersection'}),
    ('schedule_end_dates', []), ('schedule_end_dates', {'24:00': '2026-11-25'}),
    ('schedule_end_dates', {'12:00': '2026-02-30'}),
    ('schedule_end_dates', {'12:00': '20261125'}), ('schedule_end_dates', {'12:00': None})])
def test_invalid_per_slot_schedule_settings_rejected(environment, key, value):
    _, config = environment
    data = json.loads(config.path.read_text())
    data[key] = value
    config.path.write_text(json.dumps(data))
    with pytest.raises(ValueError):
        config.read()


def test_dual_schedule_exact_times_independent_slots_and_today(environment):
    _,config=environment
    enable_dual_schedule(config)
    runtime=Runtime(config,worker=done_worker)
    runtime.tick(datetime(2026,10,7,0,2,59,tzinfo=TZ),do_backup=False)
    assert not runtime.store.list()
    runtime.tick(datetime(2026,10,6,16,3,tzinfo=timezone.utc),do_backup=False)
    wait_for(runtime)
    first=runtime.store.list()[0]
    assert first['schedule_date']=='2026-10-07' and first['schedule_time']=='00:03'
    runtime.store.update(first['id'],status='failed')
    runtime.tick(datetime(2026,10,7,3,59,59,tzinfo=TZ),do_backup=False)
    assert len(runtime.store.list())==1
    runtime.tick(datetime(2026,10,6,20,0,tzinfo=timezone.utc),do_backup=False)
    wait_for(runtime)
    runs=runtime.store.day_runs('2026-10-07')
    assert len(runs)==2 and {r['schedule_time'] for r in runs}=={'00:03','04:00'}
    assert all(r['params']['threads']==4 for r in runs)
    second=next(r for r in runs if r['schedule_time']=='04:00')
    hit={'name':'艺人','song_name':'曲目','album_name':'合辑','date_str':'2026-09-30','link':'https://music.163.com/#/song?id=1'}
    runtime.store.hit(second['id'],hit)
    result=create_app(runtime).test_client().get('/api/today?date=2026-10-07').json
    assert len(result['runs'])==2 and result['total']==1 and result['results']==[hit]
    runtime.tick(datetime(2026,10,7,20,tzinfo=TZ),do_backup=False)
    assert len(runtime.store.list())==2
    with pytest.raises(sqlite3.IntegrityError):
        runtime.store.create(first['params'],'scheduled','2026-10-07',schedule_time='04:00')
    runtime.close()


def test_new_slot_activation_starts_tomorrow_without_today_catchup(environment):
    _,config=environment
    enable_dual_schedule(config,{'04:00':'2026-10-08'})
    runtime=Runtime(config,worker=done_worker)
    runtime.store.create(runtime.parameters(),'scheduled','2026-10-07',status='partial')
    runtime.tick(datetime(2026,10,7,20,tzinfo=TZ),do_backup=False)
    assert len(runtime.store.list())==1
    runtime.tick(datetime(2026,10,8,0,3,tzinfo=TZ),do_backup=False)
    wait_for(runtime)
    assert len(runtime.store.day_runs('2026-10-08'))==1
    runtime.tick(datetime(2026,10,8,4,tzinfo=TZ),do_backup=False)
    wait_for(runtime)
    assert len(runtime.store.day_runs('2026-10-08'))==2
    status=create_app(runtime).test_client().get('/api/status').json
    assert status['schedule_times']==['00:03','04:00']
    assert status['schedule_start_dates']=={'04:00':'2026-10-08'}
    runtime.close()


def test_dual_schedule_queues_second_slot_and_retains_original_window(environment):
    _,config=environment
    enable_dual_schedule(config)
    release=threading.Event()
    def worker(*args):
        assert release.wait(5)
        done_worker()
    runtime=Runtime(config,worker=worker)
    runtime.tick(datetime(2026,10,7,0,3,tzinfo=TZ),do_backup=False)
    runtime.tick(datetime(2026,10,7,4,tzinfo=TZ),do_backup=False)
    queued=runtime.store.pending()[0]
    assert queued['schedule_time']=='04:00' and queued['params']['newStartDate']=='2026-10-06'
    release.set()
    wait_for(runtime)
    runtime.tick(datetime(2026,10,7,6,tzinfo=TZ),do_backup=False)
    wait_for(runtime)
    assert runtime.store.get(queued['id'])['status']=='success'
    assert len(runtime.store.list())==2
    runtime.close()


def test_restart_catches_only_missing_slot_and_does_not_retry_interrupted_slot(environment):
    _,config=environment
    enable_dual_schedule(config)
    runtime=Runtime(config,worker=done_worker)
    first=runtime.store.create(runtime.parameters(),'scheduled','2026-10-07',status='running')
    runtime.store.recover()
    runtime.tick(datetime(2026,10,7,5,tzinfo=TZ),do_backup=False)
    wait_for(runtime)
    assert runtime.store.get(first)['status']=='interrupted'
    assert len(runtime.store.day_runs('2026-10-07'))==2
    runtime.tick(datetime(2026,10,7,6,tzinfo=TZ),do_backup=False)
    assert len(runtime.store.list())==2
    runtime.close()


def legacy_schedule_db(path):
    with sqlite3.connect(path) as db:
        db.executescript("""CREATE TABLE runs (
            id TEXT PRIMARY KEY,source TEXT NOT NULL,schedule_date TEXT UNIQUE,status TEXT NOT NULL,
            created_at TEXT NOT NULL,started_at TEXT,ended_at TEXT,params TEXT NOT NULL,
            summary TEXT NOT NULL DEFAULT '{}',metrics TEXT NOT NULL DEFAULT '{}',
            error TEXT NOT NULL DEFAULT '',warning TEXT NOT NULL DEFAULT '');
            CREATE TABLE hits(run_id TEXT NOT NULL,link TEXT NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(run_id,link));
            CREATE TABLE logs(seq INTEGER PRIMARY KEY AUTOINCREMENT,run_id TEXT NOT NULL,created_at TEXT NOT NULL,level TEXT NOT NULL,text TEXT NOT NULL);
            CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);""")
        db.execute("INSERT INTO runs(id,source,schedule_date,status,created_at,params) VALUES('old','scheduled','2026-10-07','partial','2026-10-07T16:19:30+08:00',?)",(json.dumps(default_dates(date(2026,10,7))),))
        db.execute("INSERT INTO hits VALUES('old','link',?)",(json.dumps({'link':'link','album_count':3}),))
        db.execute("INSERT INTO logs(run_id,created_at,level,text) VALUES('old','2026-10-07T18:22:05+08:00','error','old error')")
        db.execute("INSERT INTO settings VALUES('backup_date','2026-10-07')")


def test_schedule_schema_migration_preserves_history_and_allows_second_slot(tmp_path):
    path=tmp_path/'old.sqlite3'
    legacy_schedule_db(path)
    store=Store(path)
    assert store.get('old')['schedule_time']=='00:03' and store.get('old')['status']=='partial'
    assert store.hits('old')==[{'link':'link','album_count':3}]
    assert store.logs('old')[0]['text']=='old error' and store.value('backup_date')=='2026-10-07'
    store.create(default_dates(date(2026,10,7)),'scheduled','2026-10-07',schedule_time='04:00')
    assert len(Store(path).day_runs('2026-10-07'))==2


def test_schedule_schema_migration_rolls_back_atomically(tmp_path,monkeypatch):
    path=tmp_path/'old.sqlite3'
    legacy_schedule_db(path)
    migrate=Store._migrate_schedule_slots
    def fail_rename(db):
        db.set_authorizer(lambda action,*args:sqlite3.SQLITE_DENY if action==sqlite3.SQLITE_ALTER_TABLE else sqlite3.SQLITE_OK)
        migrate(db)
    with monkeypatch.context() as context:
        context.setattr(Store,'_migrate_schedule_slots',staticmethod(fail_rename))
        with pytest.raises(sqlite3.DatabaseError):
            Store(path)
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT id FROM runs').fetchall()==[('old',)]
        assert 'schedule_time' not in {row[1] for row in db.execute('PRAGMA table_info(runs)')}
        assert not db.execute("SELECT 1 FROM sqlite_master WHERE name='runs_with_slots'").fetchone()
    assert Store(path).get('old')['schedule_time']=='00:03'


@pytest.mark.parametrize('times,starts', [(['04:0'],{}),(['24:00'],{}),(['00:03','00:03'],{}),([],{}),(['04:00'],{'04:00':'20261008'}),(['04:00'],{'04:00':'2026-02-30'})])
def test_invalid_schedule_configuration_is_rejected(environment,times,starts):
    _,config=environment
    value=json.loads(config.path.read_text())
    value.update(schedule_times=times,schedule_start_dates=starts)
    config.path.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        config.read()


def test_manual_conflict_queues_original_window(environment):
    _, config = environment
    release = threading.Event()
    def worker(*args):
        assert release.wait(5)
        done_worker()
    runtime = Runtime(config, worker=worker)
    runtime.start()
    enable(config)
    runtime.tick(datetime(2026, 10, 7, 0, 3, tzinfo=TZ), do_backup=False)
    pending = runtime.store.pending()
    assert len(pending) == 1
    assert pending[0]["params"]["newStartDate"] == "2026-10-06"
    with pytest.raises(BusyError):
        runtime.start()
    release.set()
    wait_for(runtime)
    runtime.tick(datetime(2026, 10, 7, 1, tzinfo=TZ), do_backup=False)
    wait_for(runtime)
    assert runtime.store.get(pending[0]["id"])["status"] == "success"
    runtime.close()


def test_scheduled_concurrency_snapshot_and_live_adjustment(environment, monkeypatch):
    _, config = environment
    monkeypatch.setenv('NETEASE_MAX_THREADS', '8')
    value = json.loads(config.path.read_text())
    value.update(schedule_enabled=True, threads=4)
    config.path.write_text(json.dumps(value))
    release = threading.Event()
    def worker(*args):
        assert release.wait(5)
        done_worker()
    runtime = Runtime(config, worker=worker)
    client = create_app(runtime).test_client()
    runtime.tick(datetime(2026, 10, 7, 16, tzinfo=TZ), do_backup=False)
    run = runtime.store.list()[0]
    assert run['source'] == 'scheduled' and run['params']['threads'] == 4
    assert engine.CONTROL.snapshot()['target_threads'] == 4
    value['threads'] = 6
    config.path.write_text(json.dumps(value))
    status = client.get('/api/status').json
    assert status['defaults']['threads'] == 6
    assert status['current']['params']['threads'] == 4
    assert status['state']['target_threads'] == 4
    response = client.post('/api/control', json={'action':'threads', 'value':5}, headers={'X-Scanner-Request':'1'})
    assert response.status_code == 200
    assert config.read()['threads'] == 6
    release.set()
    wait_for(runtime)
    first = runtime.store.get(run['id'])
    assert first['params']['threads'] == 4 and first['metrics']['control']['target_threads'] == 5
    logs = client.get(f"/api/runs/{run['id']}").json['logs']
    assert any('任务创建时并发 4' in row['text'] for row in logs)
    assert any('由 4 调整为 5' in row['text'] for row in logs)
    runtime.tick(datetime(2026, 10, 8, 0, 3, tzinfo=TZ), do_backup=False)
    wait_for(runtime)
    assert runtime.store.list()[0]['params']['threads'] == 6
    runtime.close()


def test_queued_scheduled_threads_keep_creation_snapshot(environment, monkeypatch):
    _, config = environment
    monkeypatch.setenv('NETEASE_MAX_THREADS', '8')
    release = threading.Event()
    def worker(*args):
        assert release.wait(5)
        done_worker()
    runtime = Runtime(config, worker=worker)
    runtime.start({'threads':2})
    value = json.loads(config.path.read_text())
    value.update(schedule_enabled=True, threads=4)
    config.path.write_text(json.dumps(value))
    runtime.tick(datetime(2026, 10, 7, 0, 3, tzinfo=TZ), do_backup=False)
    queued = runtime.store.pending()[0]
    assert queued['params']['threads'] == 4
    value['threads'] = 6
    config.path.write_text(json.dumps(value))
    release.set()
    wait_for(runtime)
    runtime.tick(datetime(2026, 10, 7, 1, tzinfo=TZ), do_backup=False)
    wait_for(runtime)
    finished = runtime.store.get(queued['id'])
    assert finished['params']['threads'] == finished['metrics']['control']['target_threads'] == 4
    runtime.close()


@pytest.mark.parametrize('candidate_source', ['recent', 'probe', 'both'])
def test_nonexistent_album_is_silently_excluded_before_detail_and_retry(environment, monkeypatch, candidate_source):
    _, config = environment
    publish_ts = int(datetime(2026, 10, 7, tzinfo=TZ).timestamp() * 1000)
    good_song = {'id': 100, 'name': '曲目', 'al': {'id': 200, 'name': '专辑'}, 'ar': [{'id': 1, 'name': '艺人'}]}
    bad_song = {'id': 101, 'name': '无效候选', 'al': {'id': 141201113, 'name': '不存在'}, 'ar': [{'id': 1, 'name': '艺人'}]}
    bad_album = {'id': 141201113, 'name': '不存在', 'publishTime': publish_ts}
    calls = []
    def api(path, params=None, **kwargs):
        if path == '/artist/sublist':
            return {'code': 200, 'data': [{'id': 1, 'name': '艺人'}], 'count': 1, 'hasMore': False}
        if path == '/artist/songs':
            return {'code': 200, 'songs': [good_song] + ([bad_song] if candidate_source in {'recent', 'both'} else [])}
        if path == '/artist/album':
            return {'code': 200, 'hotAlbums': [bad_album] if candidate_source in {'probe', 'both'} else []}
        if path == '/album':
            calls.append(str(params['id']))
            assert str(params['id']) == '200'
            return {'code': 200, 'album': {'id': 200, 'name': '专辑', 'publishTime': publish_ts}, 'songs': [good_song]}
        raise AssertionError(path)
    monkeypatch.setattr(engine, 'api_request', api)
    # Assert even the cache/detail helper is never called for the excluded ID.
    fetch = engine.fetch_album_detail
    detail_calls = []
    def checked_detail(album_id, *args, **kwargs):
        detail_calls.append(str(album_id))
        assert str(album_id) != '141201113'
        return fetch(album_id, *args, **kwargs)
    monkeypatch.setattr(engine, 'fetch_album_detail', checked_detail)
    runtime = Runtime(config)
    run_id = runtime.start({**default_dates(date(2026, 10, 7)), 'scanMode': 'regular', 'debugAlbum': '141201113'})
    wait_for(runtime)
    run = runtime.store.get(run_id)
    assert run['status'] == 'success' and run['result_count'] == 1
    assert calls == detail_calls == ['200']
    assert run['summary']['unique_albums'] == 1
    assert run['summary']['album_errors'] == run['summary']['slow_retry']['attempted'] == 0
    assert run['summary']['failure_details'] == []
    assert not any(key.startswith('skipped_album') for key in run['summary'])
    assert not any('141201113' in row['text'] for row in runtime.store.logs(run_id))
    runtime.close()


def test_album_failure_details_are_persisted_and_scrubbed(environment, monkeypatch):
    _, config = environment
    song = {'id':100,'name':'曲目','al':{'id':200,'name':'专辑'},'ar':[{'id':1,'name':'艺人'}]}
    def api(path, params=None, **kwargs):
        if path == '/artist/sublist':
            return {'code':200,'data':[{'id':1,'name':'艺人'}],'count':1,'hasMore':False}
        if path == '/artist/songs':
            return {'code':200,'songs':[song]}
        if path == '/artist/album':
            return {'code':200,'hotAlbums':[]}
        raise AssertionError(path)
    def album(*args, **kwargs):
        raise ValueError('album unavailable test-secret-credential')
    monkeypatch.setattr(engine, 'api_request', api)
    monkeypatch.setattr(engine, 'fetch_album_detail', album)
    runtime = Runtime(config)
    run_id = runtime.start(default_dates(date(2026,10,7)))
    wait_for(runtime)
    result = create_app(runtime).test_client().get(f'/api/runs/{run_id}').json
    assert result['run']['status'] == 'partial' and not result['results']
    detail = result['run']['summary']['failure_details'][0]
    assert detail['stage'] == '/album' and detail['item_id'] == '200'
    assert 'album unavailable' in detail['error']
    assert any('ID 200' in row['text'] for row in result['logs'] if row['level'] == 'error')
    assert 'test-secret-credential' not in json.dumps(result)
    assert result['run']['summary']['slow_retry']['attempted'] == 3
    assert result['run']['summary']['slow_retry']['recovered'] == 0
    runtime.close()


@pytest.mark.parametrize("scan_mode", ["regular", "m3_intersection"])
def test_full_retry_preserves_old_window_and_uses_current_defaults(environment, monkeypatch, scan_mode):
    _, config = environment
    monkeypatch.setenv('NETEASE_MAX_THREADS', '8')
    value=json.loads(config.path.read_text())
    value['threads']=4
    config.path.write_text(json.dumps(value))
    runtime=Runtime(config,worker=done_worker)
    client=create_app(runtime).test_client()
    headers={'X-Scanner-Request':'1'}
    params=runtime.parameters({"scanMode":scan_mode},day=date(2026,1,1))
    params.update(threads=2,recentLimit=70)
    parent=runtime.store.create(params,source='scheduled',schedule_date='2026-01-01',status='partial')
    response=client.post(f'/api/runs/{parent}/retry',json={},headers=headers)
    assert response.status_code==200
    wait_for(runtime)
    child=runtime.store.get(response.json['run_id'])
    assert child['params']['retry_of']==parent
    assert child['params']['scanMode']==scan_mode
    assert child['params']['threads']==4 and child['params']['recentLimit']==70
    for key in server.DATE_KEYS:
        assert child['params'][key]==params[key]
    assert runtime.store.get(parent)['status']=='partial'
    assert len(runtime.store.list())==2
    assert client.post('/api/runs/missing/retry',json={},headers=headers).status_code==404
    assert client.post(f"/api/runs/{child['id']}/retry",json={},headers=headers).status_code==400
    runtime.current=child['id']
    assert client.post(f'/api/runs/{parent}/retry',json={},headers=headers).status_code==409
    runtime.current=None
    runtime.close()


def test_real_engine_recovers_album_failure_without_resampling_artists(environment, monkeypatch):
    _,config=environment
    publish_ts=int(datetime(2026,10,7,tzinfo=TZ).timestamp()*1000)
    song={'id':100,'name':'曲目','al':{'id':200,'name':'专辑'},'ar':[{'id':1,'name':'艺人'}]}
    calls=[]
    def api(path,params=None,**kwargs):
        calls.append(path)
        if path=='/artist/sublist':
            return {'code':200,'data':[{'id':1,'name':'艺人'}],'count':1,'hasMore':False}
        if path=='/artist/songs':
            return {'code':200,'songs':[song]}
        if path=='/artist/album':
            return {'code':200,'hotAlbums':[]}
        if path=='/album':
            if calls.count('/album')<=2:
                raise engine.ApiError('/album','temporarily unavailable')
            return {'code':200,'album':{'id':200,'name':'专辑','publishTime':publish_ts},'songs':[song]}
        raise AssertionError(path)
    monkeypatch.setattr(engine,'api_request',api)
    runtime=Runtime(config)
    run_id=runtime.start(default_dates(date(2026,10,7)))
    wait_for(runtime)
    run=runtime.store.get(run_id)
    assert run['status']=='success' and run['result_count']==1
    assert run['summary']['album_errors']==0
    assert run['summary']['slow_retry']['recovered']==1
    assert calls.count('/artist/songs')==1
    assert runtime.store.hits(run_id)[0]['album_count']==1
    second=runtime.start(default_dates(date(2026,10,7)))
    wait_for(runtime)
    assert runtime.store.get(second)['result_count']==0
    runtime.close()


@pytest.mark.parametrize('stage',['recent','probe','owned'])
def test_real_engine_recovers_artist_stage_failures(environment,monkeypatch,stage):
    _,config=environment
    publish_ts=int(datetime(2026,10,7,tzinfo=TZ).timestamp()*1000)
    song={'id':100,'name':'曲目','al':{'id':200,'name':'专辑'},'ar':[{'id':1,'name':'艺人'}],'publishTime':publish_ts}
    album={'id':200,'name':'专辑','publishTime':publish_ts}
    calls=[]
    failed=False
    def api(path,params=None,**kwargs):
        nonlocal failed
        calls.append((path,params.get('limit')))
        match=(stage=='recent' and path=='/artist/songs') or (stage=='probe' and path=='/artist/album' and params['limit']==20) or (stage=='owned' and path=='/artist/album' and params['limit']==50)
        if match and not failed:
            failed=True
            return {'code':500}
        if path=='/artist/sublist':
            return {'code':200,'data':[{'id':1,'name':'艺人'}],'count':1,'hasMore':False}
        if path=='/artist/songs':
            return {'code':200,'songs':[song]}
        if path=='/artist/album':
            return {'code':200,'hotAlbums':[album]}
        if path=='/album':
            return {'code':200,'album':album,'songs':[song]}
        raise AssertionError(path)
    monkeypatch.setattr(engine,'api_request',api)
    runtime=Runtime(config)
    run_id=runtime.start(default_dates(date(2026,10,7)))
    wait_for(runtime)
    run=runtime.store.get(run_id)
    assert failed and run['status']=='success' and run['result_count']==1
    assert run['summary']['slow_retry']['recovered']==1
    assert not any(run['summary'][k] for k in server.FAILURE_KEYS)
    assert {h['album_count'] for h in runtime.store.hits(run_id)}=={1}
    assert sum(p=='/artist/songs' for p,_ in calls)==(2 if stage=='recent' else 1)
    runtime.close()


def test_stale_pending_not_backfilled_and_running_recovers(environment):
    root, config = environment
    runtime = Runtime(config, worker=done_worker)
    old = runtime.store.create(default_dates(date(2026, 10, 6)), "scheduled", "2026-10-06")
    running = runtime.store.create(default_dates(date(2026, 10, 6)), status="running")
    runtime.store.recover()
    assert runtime.store.get(running)["status"] == "interrupted"
    runtime.tick(datetime(2026, 10, 7, 0, 2, tzinfo=TZ), do_backup=False)
    assert runtime.store.get(old)["status"] == "interrupted"
    assert runtime.current is None
    runtime.close()


def test_broadcast_two_clients_and_bounded_backlog():
    bus = Broadcast(capacity=2)
    a, b = bus.subscribe(), bus.subscribe()
    bus.publish({"type": "hit", "run_id": "id"})
    assert a.get_nowait() == b.get_nowait()
    for i in range(1000):
        bus.publish({"type": "progress", "current": i, "run_id": "id"})
    assert a.qsize() == b.qsize() == 2
    assert any(a.get_nowait()["type"] == "resync" for _ in range(2))
    bus.unsubscribe(a)
    bus.unsubscribe(b)
    bus.publish({"type": "hit"})
    assert not bus.clients


def test_expired_cookie_is_failed_not_empty_success(environment, monkeypatch):
    _, config = environment
    def expired(*args):
        raise ValueError("Cookie 已失效")
    monkeypatch.setattr(server, "verify_login", expired)
    runtime = Runtime(config, worker=done_worker)
    run_id = runtime.start()
    wait_for(runtime)
    record = runtime.store.get(run_id)
    assert record["status"] == "failed"
    assert "失效" in record["error"]
    runtime.close()


@pytest.mark.parametrize("mode,expected", [("partial", "partial"), ("fatal", "failed"), ("stop", "stopped"), ("unexpected", "partial")])
def test_failure_status_and_redaction(environment, mode, expected):
    root, config = environment
    def worker(*args):
        if mode == "fatal":
            raise RuntimeError("failed MUSIC_U=test-secret-credential;")
        if mode == "stop":
            engine.CONTROL.stop()
            return
        if mode == "unexpected":
            engine.emit({"type": "log", "level": "error", "text": "任务异常：test-secret-credential"})
        engine.emit({"type": "done", "results": [], "summary": {"artists": 2, "album_errors": 1 if mode == "partial" else 0}})
    runtime = Runtime(config, worker=worker)
    run_id = runtime.start()
    wait_for(runtime)
    record = runtime.store.get(run_id)
    assert record["status"] == expected
    assert "test-secret-credential" not in json.dumps(record)
    assert "test-secret-credential" not in json.dumps(runtime.store.logs(run_id))
    runtime.close()


def test_validation_and_csrf_do_not_acquire_scan_lock(environment):
    _, config = environment
    runtime = Runtime(config, worker=done_worker)
    client = create_app(runtime).test_client()
    assert client.post("/api/start", json={}).status_code == 403
    for payload in ([], {"recentLimit": "bad"}, {"externalStartDate": "2026-13-01"}, {"externalStartDate": "2027-01-01"}):
        assert client.post("/api/start", json=payload, headers={"X-Scanner-Request": "1"}).status_code == 400
    assert runtime.current is None
    assert not runtime.store.list()
    assert client.get("/api/runs?offset=bad").status_code == 400
    runtime.close()


def test_legacy_csv_byte_preservation_and_pagination(environment):
    _, config = environment
    content = "\ufeff" + ",".join(server.CSV_HEADER) + "\r\n艺人,曲目,合辑,2026-10-07,https://music.163.com/#/song?id=1,1\r\n"
    engine.CSV_FILE.write_bytes(content.encode("utf-8"))
    runtime = Runtime(config, worker=done_worker)
    client = create_app(runtime).test_client()
    assert client.get("/api/legacy/csv").data == content.encode("utf-8")
    assert client.get("/api/legacy").json["results"][0]["name"] == "艺人"
    assert client.get("/api/legacy?offset=1").json["results"] == []
    assert client.get("/api/pre-migration/csv").data == content.encode("utf-8")
    # Later appends and restarts must not change the migration-time snapshot.
    with engine.CSV_FILE.open("a", encoding="utf-8") as f:
        f.write("后续艺人,新曲,新专,2026-10-08,https://music.163.com/#/song?id=2,1\r\n")
    fresh = Runtime(config, worker=done_worker)
    client = create_app(fresh).test_client()
    assert client.get("/api/pre-migration").json["total"] == 1
    assert client.get("/api/legacy").json["total"] == 2
    assert client.get("/api/pre-migration/csv").data == content.encode("utf-8")
    fresh.close()
    runtime.close()


def test_completion_storage_failure_releases_lock_and_blocks_new_scans(environment, monkeypatch):
    _, config = environment
    runtime = Runtime(config, worker=done_worker)
    update = runtime.store.update
    def unavailable(run_id, **fields):
        if "ended_at" in fields:
            raise sqlite3.OperationalError("disk full")
        return update(run_id, **fields)
    monkeypatch.setattr(runtime.store, "update", unavailable)
    runtime.start()
    wait_for(runtime)
    assert runtime.closed and runtime.persistence_error
    assert not engine.is_server_scanning
    with pytest.raises(BusyError):
        runtime.start()
    status = create_app(runtime).test_client().get("/api/status").json
    assert status["accepting_scans"] is False
    assert "写入失败" in status["scheduler_error"]
    runtime.close()


def test_backup_restores_readable_database(environment):
    root, config = environment
    runtime = Runtime(config, worker=done_worker)
    run_id = runtime.start()
    wait_for(runtime)
    runtime.backup(date(2026, 10, 7), 14)
    import tarfile
    with tarfile.open(root / "backups/data-2026-10-07.tar.gz") as archive:
        data = archive.extractfile("data/scan_history.sqlite3").read()
    recovered = root / "restored.sqlite3"
    recovered.write_bytes(data)
    assert Store(recovered).get(run_id)["status"] == "success"
    assert runtime.store.value("backup_date") == "2026-10-07"
    runtime.close()


def test_real_engine_preserves_dedupe_and_album_cache(environment, monkeypatch):
    root, config = environment
    params = default_dates(date(2026, 10, 7))
    publish_ts = int(datetime(2026, 10, 7, tzinfo=TZ).timestamp() * 1000)
    song = {"id": 100, "name": "新曲", "al": {"id": 200, "name": "新专"}, "ar": [{"id": 1, "name": "艺人"}], "publishTime": publish_ts}
    album = {"id": 200, "name": "新专", "publishTime": publish_ts, "artists": [{"id": 1, "name": "艺人"}], "size": 1}
    calls = []
    def api(path, params=None, **kwargs):
        calls.append(path)
        if path == "/artist/sublist":
            return {"code": 200, "data": [{"id": 1, "name": "艺人"}], "count": 1, "hasMore": False}
        if path == "/artist/songs":
            return {"code": 200, "songs": [song]}
        if path == "/artist/album":
            return {"code": 200, "hotAlbums": [album]}
        if path == "/album":
            return {"code": 200, "album": album, "songs": [song]}
        raise AssertionError(path)
    monkeypatch.setattr(engine, "api_request", api)
    runtime = Runtime(config)
    run1 = runtime.start(params)
    wait_for(runtime)
    assert runtime.store.get(run1)["status"] == "success"
    assert runtime.store.get(run1)["summary"]["relations"] == 1
    assert not engine.ARTIST_ALBUM_PAGE_CACHE
    # Do not emit duplicate album-level placeholder when the artist's track
    # in this album is already represented by recent-song relations.
    assert runtime.store.get(run1)["result_count"] == 1
    assert {hit["link"] for hit in runtime.store.hits(run1)} == {
        "https://music.163.com/#/song?id=100"}
    original_csv = engine.CSV_FILE.read_bytes()
    assert json.loads(engine.ALBUM_CACHE_FILE.read_text())["200"]["album_id"] == "200"
    calls.clear()
    run2 = runtime.start(params)
    wait_for(runtime)
    assert runtime.store.get(run2)["status"] == "success"
    assert runtime.store.get(run2)["result_count"] == 0
    assert engine.CSV_FILE.read_bytes() == original_csv
    assert "/artist/sublist" not in calls  # Already refreshed today.
    assert runtime.store.get(run2)["metrics"]["album_cache_hit"] >= 1
    runtime.close()


def test_compact_artist_pages_preserve_new_release_ownership_and_failure(environment, monkeypatch):
    publish_ts = int(datetime(2026, 10, 7, tzinfo=TZ).timestamp() * 1000)
    raw = {"code": 200, "artist": {"biography": "unused" * 1000}, "more": False,
           "hotAlbums": [{"id": 200, "name": "新专", "publishTime": publish_ts,
                          "description": "unused" * 1000, "artists": [{"id": 1}]}]}
    calls = []
    def api(path, params=None, **kwargs):
        calls.append(params["limit"])
        return raw
    monkeypatch.setattr(engine, "api_request", api)
    found, info = engine.fetch_new_albums_for_artist(
        {"id": "1", "name": "艺人"}, "cookie", 1, datetime(2026, 10, 6), datetime(2026, 10, 8))
    assert info["ok"] and found[0]["album_name"] == "新专"
    owned, dates = engine.fetch_owned_album_ids_shallow("1", "cookie", 1)
    assert owned == {"200"} and dates == {"200": "2026-10-07"}
    assert calls == [1]  # Reuse the compact page without another request.
    page = next(iter(engine.ARTIST_ALBUM_PAGE_CACHE.values()))
    assert len(json.dumps(page)) < len(json.dumps(raw)) / 20
    assert set(page["hotAlbums"][0]) == {"id", "name", "publishTime"}
    assert "description" in raw["hotAlbums"][0]  # Do not mutate the API response.
    engine.fetch_owned_album_ids_shallow("1", "cookie", 50)
    assert calls == [1, 50]  # Ownership still requests the configured depth.
    raw["code"] = 500
    found, info = engine.fetch_new_albums_for_artist(
        {"id": "2", "name": "艺人"}, "cookie", 1, datetime(2026, 10, 6), datetime(2026, 10, 8))
    assert not info["ok"] and not found and "500" in info["error"]


def test_dynamic_pool_streams_and_releases_prior_results(environment, monkeypatch):
    monkeypatch.setattr(engine, "CONTROL", engine.ControlState(target_threads=1, baseline_threads=1))
    monkeypatch.setattr(engine, "METRICS", engine.Metrics())
    class Result:
        pass
    calls = []
    def task(item):
        calls.append(item)
        return Result()
    outputs = iter(engine.run_dynamic_pool(list(range(4)), task, "test"))
    first = next(outputs)
    first_ref = weakref.ref(first)
    assert calls == [0]  # The whole phase has not been evaluated yet.
    del first
    next(outputs)
    next(outputs)
    assert first_ref() is None  # Earlier results do not stay in a pool-wide list.
    assert len(list(outputs)) == 1
    assert calls == [0, 1, 2, 3]


def test_scan_releases_artist_pages_on_error(environment, monkeypatch):
    monkeypatch.setattr(engine, "emit", lambda message: None)
    monkeypatch.setattr(engine, "CONTROL", engine.ControlState(target_threads=1))
    monkeypatch.setattr(engine, "METRICS", engine.Metrics())
    def fail(*args):
        engine.ARTIST_ALBUM_PAGE_CACHE[("cookie", "1", "page")] = {"code": 200}
        raise RuntimeError("test failure")
    monkeypatch.setattr(engine, "resolve_artist_list", fail)
    day = datetime(2026, 10, 7)
    engine.scan_worker("cookie", "", day, day, day, day, 50, 50, 20)
    assert not engine.ARTIST_ALBUM_PAGE_CACHE


def test_memory_changes_keep_date_ownership_filters_and_partial_failure(environment, monkeypatch):
    _, config = environment
    params = default_dates(date(2026, 10, 7))
    albums = {}
    for aid, day in ((200, (2026, 10, 7)), (300, (2026, 9, 9)),
                     (400, (2026, 8, 1)), (900, (2026, 9, 9))):
        albums[aid] = {"id": aid, "name": str(aid),
                       "publishTime": int(datetime(*day, tzinfo=TZ).timestamp() * 1000),
                       "artists": [{"id": 1, "name": "艺人"}], "description": "unused" * 1000}
    songs = [{"id": aid + 1000, "name": str(aid), "al": {"id": aid, "name": str(aid)},
              "ar": [{"id": 1, "name": "艺人"}], "publishTime": album["publishTime"]}
             for aid, album in albums.items()]
    def api(path, params=None, **kwargs):
        if path == "/artist/sublist":
            return {"code": 200, "data": [{"id": 1, "name": "艺人"}, {"id": 2, "name": "失败艺人"}],
                    "count": 2, "hasMore": False}
        if path in ("/artist/songs", "/artist/album") and str(params["id"]) == "2":
            return {"code": 500}
        if path == "/artist/songs":
            return {"code": 200, "songs": songs}
        if path == "/artist/album":
            return {"code": 200, "hotAlbums": [albums[200], albums[300]]}
        if path == "/album":
            aid = int(params["id"])
            return {"code": 200, "album": albums[aid], "songs": [s for s in songs if s["al"]["id"] == aid]}
        raise AssertionError(path)
    monkeypatch.setattr(engine, "api_request", api)
    runtime = Runtime(config)
    run_id = runtime.start(params)
    wait_for(runtime)
    run = runtime.store.get(run_id)
    assert run["status"] == "partial"
    assert run["summary"]["relations"] == 4
    assert run["summary"]["artist_failures"] == run["summary"]["new_album_probe_failures"] == 1
    assert {hit["link"] for hit in runtime.store.hits(run_id)} == {
        "https://music.163.com/#/song?id=1200",
        "https://music.163.com/#/song?id=1900"}
    assert not engine.ARTIST_ALBUM_PAGE_CACHE
    runtime.close()


def test_heat_uses_distinct_candidate_artists_before_historical_dedupe(environment, monkeypatch):
    _, config = environment
    publish_ts = int(datetime(2026, 9, 10, tzinfo=TZ).timestamp() * 1000)
    artists = [{"id": i, "name": f"艺人{i}"} for i in (1, 2, 3)]
    songs = {i: [{"id": 100 + i, "name": f"曲目{i}", "al": {"id": 200, "name": "共享合辑"},
                  "ar": [{"id": i, "name": f"艺人{i}"}], "publishTime": publish_ts}] for i in (1, 2, 3)}
    songs[1].append({**songs[1][0], "id": 111, "name": "同艺人另一曲"})
    engine.init_csv_and_seen()
    engine.write_hit_if_new({"name": "艺人3", "song_name": "曲目3", "album_name": "共享合辑",
                             "date_str": "2026-09-10", "link": "https://music.163.com/#/song?id=103", "flag": 0})
    def api(path, params=None, **kwargs):
        if path == "/artist/sublist":
            return {"code": 200, "data": artists, "count": 3, "hasMore": False}
        if path == "/artist/songs":
            return {"code": 200, "songs": songs[int(params["id"])]}
        if path == "/artist/album":
            return {"code": 200, "hotAlbums": []}
        if path == "/album":
            return {"code": 200, "album": {"id": 200, "name": "共享合辑", "publishTime": publish_ts}}
        raise AssertionError(path)
    monkeypatch.setattr(engine, "api_request", api)
    runtime = Runtime(config)
    run_id = runtime.start(default_dates(date(2026, 10, 7)))
    wait_for(runtime)
    run = runtime.store.get(run_id)
    assert run["status"] == "success" and run["summary"]["relations"] == 4
    assert run["result_count"] == 3 and run["metrics"]["csv_skipped"] == 1
    hits = Store(runtime.store.path).hits(run_id)
    assert {hit["album_count"] for hit in hits} == {3}
    assert {hit["link"] for hit in hits} == {
        "https://music.163.com/#/song?id=101", "https://music.163.com/#/song?id=102",
        "https://music.163.com/#/song?id=111"}
    with engine.CSV_FILE.open(encoding="utf-8-sig", newline="") as f:
        assert {len(row) for row in csv.reader(f)} == {6}
    runtime.close()


def test_existing_heat_results_sort_before_paging_and_export_without_rescan(environment, monkeypatch):
    root, config = environment
    monkeypatch.setattr(server, "now_iso", lambda: "2026-10-07T12:00:00+08:00")
    runtime = Runtime(config, worker=done_worker)
    run_id = runtime.store.create(default_dates(date(2026, 10, 7)), status="success")
    other = runtime.store.create(default_dates(date(2026, 10, 7)), status="success")
    base = {"name": "艺人", "song_name": "曲目", "album_name": "专辑", "flag": 0}
    for i in range(205):
        runtime.store.hit(run_id, {**base, "link": f"https://music.163.com/#/song?id={i}",
                                   "date_str": "2026-10-07", "album_count": 1})
    hot = {**base, "link": "https://music.163.com/#/song?id=900", "date_str": "2026-09-10", "album_count": 4}
    hotter = {**base, "link": "https://music.163.com/#/song?id=901", "date_str": "2026-09-09", "album_count": 5}
    unknown = {**base, "link": "https://music.163.com/#/song?id=902", "date_str": "2026-10-08"}
    runtime.store.hit(run_id, hot)
    runtime.store.hit(run_id, unknown)
    runtime.store.hit(other, hotter)
    client = create_app(runtime).test_client()
    assert Store(root / "scan_history.sqlite3").hits(run_id, limit=1) == [hot]
    assert client.get(f"/api/runs/{run_id}?limit=1").json["results"] == [hot]
    assert client.get("/api/today?date=2026-10-07&limit=1").json["results"] == [hotter]
    assert client.get("/api/today?date=2026-10-07&limit=1&offset=1").json["results"] == [hot]
    assert runtime.store.hits(run_id, limit=1, offset=206) == [unknown]
    for path, expected in ((f"/api/runs/{run_id}/csv", hot), ("/api/today/csv?date=2026-10-07", hotter)):
        rows = list(csv.reader(io.StringIO(client.get(path).data.decode("utf-8-sig"))))
        assert rows[0][-1] == "热度（同专辑命中艺人数）"
        assert rows[1][4] == expected["link"] and rows[1][-1] == str(expected["album_count"])
        assert rows[-1][-1] == ""  # Missing historical values remain unknown.
    legacy = b'\xef\xbb\xbfArtist,Track,Album,Date,Link,Flag\r\na,b,c,2026-09-01,https://music.163.com/#/song?id=9,0\r\n'
    engine.CSV_FILE.write_bytes(legacy)
    assert client.get("/api/legacy/csv").data == legacy
    assert "album_count" not in client.get("/api/legacy").json["results"][0]
    assert "热度" in client.get("/").data.decode("utf-8")
    runtime.close()
