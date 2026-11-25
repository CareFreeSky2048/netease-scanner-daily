"""Single-process web service, durable runs and daily Shanghai-time scheduling."""
from __future__ import annotations

import atexit
import calendar
import csv
import hashlib
import io
import json
import logging
import os
import queue
import re
import shutil
import sqlite3
import tarfile
import threading
import uuid
from collections import deque
from contextlib import closing
from datetime import date, datetime, time, timedelta
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests
from flask import Flask, Response, jsonify, render_template, request, send_file

import engine
from m3_mode import load_m3_artist_names, partition_m3_followed_artists
from record_export import ExportLimitError, RecordExports

TZ = ZoneInfo("Asia/Shanghai")
ROOT = Path(__file__).resolve().parent
DATA = engine.SCRIPT_DIR
CSV_HEADER = ["艺人 (Artist)", "单曲名 (Track)", "企划/专辑名 (Album/VA)",
              "发行日期 (Release Date)", "直达链接", "新歌标记 (Flag)"]
DATE_KEYS = ("externalStartDate", "externalEndDate", "newStartDate", "newEndDate")
FAILURE_KEYS = ("artist_failures", "new_album_probe_failures", "album_errors", "owned_errors")
SCAN_MODES = {"auto", "regular", "m3", "m3_intersection"}
CONFIG_SCAN_MODES = {"auto", "regular", "m3"}
M3_DEFAULT_UNTIL = date(2026, 11, 25)
M3_EXTERNAL_START_DATE = date(2026, 10, 24)


def now_iso():
    return datetime.now(TZ).isoformat(timespec="seconds")


def default_dates(day: date):
    year, month = (day.year - 1, 12) if day.month == 1 else (day.year, day.month - 1)
    start = date(year, month, min(day.day, calendar.monthrange(year, month)[1]))
    return dict(zip(DATE_KEYS, (start.isoformat(), day.isoformat(),
                               (day - timedelta(days=1)).isoformat(),
                               (day + timedelta(days=3)).isoformat())))


def schedule_details(config, day):
    today = day.isoformat()
    return [{"time": slot,
             "mode": config["schedule_modes"].get(slot, config["scan_mode"]),
             "start_date": config["schedule_start_dates"].get(slot),
             "end_date": config["schedule_end_dates"].get(slot),
             "active": (today >= config["schedule_start_dates"].get(slot, today) and
                        today <= config["schedule_end_dates"].get(slot, today))}
            for slot in config["schedule_times"]]


def format_schedule_times(times):
    """Keep dense quarter-hour schedules readable in the status header."""
    minutes = [int(slot[:2]) * 60 + int(slot[3:]) for slot in times]
    labels = []
    index = 0
    while index < len(times):
        end = index + 1
        while end < len(times) and minutes[end] - minutes[end - 1] == 15:
            end += 1
        if end - index >= 3:
            labels.append(f"{times[index]}–{times[end - 1]}（每15分钟）")
        else:
            labels.extend(times[index:end])
        index = end
    return " / ".join(labels)


class Config:
    def __init__(self, path=None):
        self.path = Path(path or os.getenv("NETEASE_CONFIG_FILE", str(ROOT / "config.json")))

    def thread_limit(self):
        limit = int(os.getenv('NETEASE_MAX_THREADS', engine.MANUAL_THREAD_MAX))
        if not 1 <= limit <= engine.MANUAL_THREAD_MAX:
            raise ValueError('NETEASE_MAX_THREADS 配置有误')
        return limit

    def read(self):
        defaults = {"schedule_enabled": False, "recentLimit": 50, "ownedLimit": 50,
                    "newAlbumProbeLimit": 20, "threads": 5, "backup_retention_days": 14,
                    "schedule_times": ["00:03"], "schedule_start_dates": {},
                    "schedule_modes": {}, "schedule_end_dates": {},
                    "cookie_file": str(ROOT / "secrets" / "music_cookie.txt"),
                    "scan_cookie_file": "",
                    "artist_cache_key": "", "scan_mode": "auto",
                    "m3_artist_file": str(ROOT / "m3_artist_names.json"),
                    "m3_default_until": M3_DEFAULT_UNTIL.isoformat(),
                    "m3_external_start_date": M3_EXTERNAL_START_DATE.isoformat(),
                    "m3_second_round_recent_limit": 0}
        if self.path.exists():
            data = json.loads(self.path.read_text(encoding="utf-8-sig"))
            if not isinstance(data, dict):
                raise ValueError("配置必须是 JSON 对象")
            defaults.update(data)
        if not isinstance(defaults["schedule_enabled"], bool):
            raise ValueError("schedule_enabled 必须为 true 或 false")
        times = defaults["schedule_times"]
        if (not isinstance(times, list) or not 1 <= len(times) <= 24 or
                any(not isinstance(t, str) or not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", t) for t in times) or
                len(times) != len(set(times))):
            raise ValueError("schedule_times 必须是无重复的 HH:MM 时间列表")
        defaults["schedule_times"] = sorted(times)
        for key in ("schedule_start_dates", "schedule_end_dates", "schedule_modes"):
            mapping = defaults[key]
            if not isinstance(mapping, dict):
                raise ValueError(f"{key} 必须是时间与设置的映射")
            for slot, value in mapping.items():
                if not isinstance(slot, str) or not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", slot):
                    raise ValueError(f"{key} 的时间必须使用 HH:MM")
                if key == "schedule_modes":
                    if not isinstance(value, str) or value not in SCAN_MODES:
                        raise ValueError("定时时段模式必须为 auto、regular、m3 或 m3_intersection")
                elif not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
                    raise ValueError(f"{key} 的日期必须使用 YYYY-MM-DD")
        for key, lo, hi in (("recentLimit", 10, 200), ("ownedLimit", 10, 200),
                            ("newAlbumProbeLimit", 0, 100), ("threads", 1, engine.MANUAL_THREAD_MAX),
                            ("backup_retention_days", 1, 365)):
            defaults[key] = int(defaults[key])
            if not lo <= defaults[key] <= hi:
                raise ValueError(f"配置 {key} 必须在 {lo} 至 {hi} 之间")
        defaults['threads'] = min(defaults['threads'], self.thread_limit())
        if defaults.get("scan_mode") not in CONFIG_SCAN_MODES:
            raise ValueError("scan_mode 必须为 auto、regular 或 m3")
        try:
            cutoff = str(defaults.get("m3_default_until"))
            if date.fromisoformat(cutoff).isoformat() != cutoff:
                raise ValueError
        except Exception:
            raise ValueError("m3_default_until 必须使用 YYYY-MM-DD")
        try:
            m3_start = str(defaults.get("m3_external_start_date"))
            if date.fromisoformat(m3_start).isoformat() != m3_start:
                raise ValueError
        except Exception:
            raise ValueError("m3_external_start_date 必须使用 YYYY-MM-DD")
        if not isinstance(defaults.get("m3_artist_file"), str) or not defaults["m3_artist_file"].strip():
            raise ValueError("m3_artist_file 必须为文件路径")
        second_recent = int(defaults.get("m3_second_round_recent_limit") or 0)
        if second_recent != 0 and not 10 <= second_recent <= 200:
            raise ValueError("m3_second_round_recent_limit 必须为 0 或在 10 至 200 之间")
        defaults["m3_second_round_recent_limit"] = second_recent
        if not isinstance(defaults.get("scan_cookie_file", ""), str):
            raise ValueError("scan_cookie_file 必须为文件路径")
        defaults["scan_cookie_file"] = defaults.get("scan_cookie_file", "").strip()
        return defaults

    def cookie(self, config=None):
        config = config or self.read()
        path = Path(config["cookie_file"])
        if not path.is_absolute():
            path = self.path.parent / path
        raw = path.read_text(encoding="utf-8-sig").strip() if path.is_file() else ""
        if not raw or raw.startswith("REPLACE_"):
            raise ValueError("尚未配置有效 Cookie，请更新服务器凭据文件")
        return engine.normalize_cookie(raw)

    def scan_cookie(self, config=None):
        config = config or self.read()
        scan_path_raw = str(config.get("scan_cookie_file") or "").strip()
        if not scan_path_raw:
            return self.cookie(config)
        path = Path(scan_path_raw)
        if not path.is_absolute():
            path = self.path.parent / path
        raw = path.read_text(encoding="utf-8-sig").strip() if path.is_file() else ""
        if not raw or raw.startswith("REPLACE_"):
            raise ValueError("尚未配置有效扫描小号 Cookie，请更新扫描凭据文件")
        return engine.normalize_cookie(raw)


class Store:
    ALBUM_KEY_SQL = "COALESCE(NULLIF(CAST(json_extract(payload,'$.album_id') AS TEXT),''),'name:'||COALESCE(NULLIF(json_extract(payload,'$.album_name'),''),'link:'||link))"

    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY, source TEXT NOT NULL, schedule_date TEXT, schedule_time TEXT,
                    status TEXT NOT NULL, created_at TEXT NOT NULL, started_at TEXT,
                    ended_at TEXT, params TEXT NOT NULL, summary TEXT NOT NULL DEFAULT '{}',
                    metrics TEXT NOT NULL DEFAULT '{}', error TEXT NOT NULL DEFAULT '',
                    warning TEXT NOT NULL DEFAULT '', UNIQUE(schedule_date, schedule_time));
                CREATE TABLE IF NOT EXISTS hits (
                    run_id TEXT NOT NULL, link TEXT NOT NULL, payload TEXT NOT NULL,
                    PRIMARY KEY(run_id, link));
                CREATE TABLE IF NOT EXISTS logs (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL,
                    created_at TEXT NOT NULL, level TEXT NOT NULL, text TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            """)
            self._migrate_schedule_slots(db)

    @staticmethod
    def _migrate_schedule_slots(db):
        if "schedule_time" in {row[1] for row in db.execute("PRAGMA table_info(runs)")}:
            return
        # Replace the old one-run-per-day constraint atomically, keeping IDs and
        # all history. Existing scheduled runs belong to the original 00:03 slot.
        db.execute("BEGIN IMMEDIATE")
        db.execute("""CREATE TABLE runs_with_slots (
            id TEXT PRIMARY KEY, source TEXT NOT NULL, schedule_date TEXT, schedule_time TEXT,
            status TEXT NOT NULL, created_at TEXT NOT NULL, started_at TEXT, ended_at TEXT,
            params TEXT NOT NULL, summary TEXT NOT NULL DEFAULT '{}', metrics TEXT NOT NULL DEFAULT '{}',
            error TEXT NOT NULL DEFAULT '', warning TEXT NOT NULL DEFAULT '',
            UNIQUE(schedule_date, schedule_time))""")
        columns = "id,source,schedule_date,status,created_at,started_at,ended_at,params,summary,metrics,error,warning"
        db.execute(f"INSERT INTO runs_with_slots({columns},schedule_time) SELECT {columns},"
                   "CASE WHEN source='scheduled' THEN '00:03' ELSE NULL END FROM runs")
        db.execute("DROP TABLE runs")
        db.execute("ALTER TABLE runs_with_slots RENAME TO runs")

    def connect(self):
        # Connection context commits/rolls back; closing also releases the file handle.
        class Connection(sqlite3.Connection):
            def __exit__(self, *args):
                try:
                    return super().__exit__(*args)
                finally:
                    self.close()
        db = sqlite3.connect(self.path, timeout=30, factory=Connection)
        db.row_factory = sqlite3.Row
        return db

    def create(self, params, source="manual", schedule_date=None, status="queued", schedule_time="00:03"):
        run_id = uuid.uuid4().hex
        with self.connect() as db:
            db.execute("INSERT INTO runs(id,source,schedule_date,schedule_time,status,created_at,params) VALUES(?,?,?,?,?,?,?)",
                       (run_id, source, schedule_date, schedule_time if source == "scheduled" else None,
                        status, now_iso(), json.dumps(params, ensure_ascii=False)))
        return run_id

    def update(self, run_id, **fields):
        allowed = {"status", "started_at", "ended_at", "summary", "metrics", "error", "warning"}
        if not fields or not set(fields) <= allowed:
            raise ValueError("invalid run update")
        values = [json.dumps(v, ensure_ascii=False) if isinstance(v, dict) else v for v in fields.values()]
        with self.connect() as db:
            db.execute("UPDATE runs SET " + ",".join(f"{k}=?" for k in fields) + " WHERE id=?", values + [run_id])

    @staticmethod
    def decode(row):
        if row is None:
            return None
        result = dict(row)
        for key in ("params", "summary", "metrics"):
            result[key] = json.loads(result[key])
        return result

    def get(self, run_id):
        with self.connect() as db:
            row = db.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
            result = self.decode(row)
            if result:
                result["result_count"] = db.execute("SELECT COUNT(*) FROM hits WHERE run_id=?", (run_id,)).fetchone()[0]
            return result

    def list(self, limit=50, offset=0):
        with self.connect() as db:
            rows = db.execute("SELECT r.*, (SELECT COUNT(*) FROM hits h WHERE h.run_id=r.id) AS result_count "
                              "FROM runs r ORDER BY created_at DESC,rowid DESC LIMIT ? OFFSET ?", (limit, offset)).fetchall()
            return [self.decode(row) for row in rows]

    def hits(self, run_id, limit=200, offset=0):
        with self.connect() as db:
            rows = db.execute("SELECT payload FROM hits WHERE run_id=? ORDER BY "
                              "COALESCE(CAST(json_extract(payload,'$.album_count') AS INTEGER),0) DESC,"
                              "json_extract(payload,'$.date_str') DESC,link "
                              "LIMIT ? OFFSET ?", (run_id, limit, offset)).fetchall()
            return [json.loads(row[0]) for row in rows]

    def day_runs(self, day):
        with self.connect() as db:
            rows = db.execute("SELECT r.*, (SELECT COUNT(*) FROM hits h WHERE h.run_id=r.id) AS result_count "
                              "FROM runs r WHERE schedule_date=? OR "
                              "(source='manual' AND substr(created_at,1,10)=?) ORDER BY created_at DESC,rowid DESC",
                              (day, day)).fetchall()
            return [self.decode(row) for row in rows]

    def day_hits(self, day, limit=200, offset=0):
        with self.connect() as db:
            rows = db.execute("SELECT h.payload FROM hits h JOIN runs r ON h.run_id=r.id "
                              "WHERE r.schedule_date=? OR (r.source='manual' AND substr(r.created_at,1,10)=?) "
                              "ORDER BY COALESCE(CAST(json_extract(h.payload,'$.album_count') AS INTEGER),0) DESC,"
                              "json_extract(h.payload,'$.date_str') DESC,r.created_at DESC,h.link LIMIT ? OFFSET ?",
                              (day, day, limit, offset)).fetchall()
            return [json.loads(row[0]) for row in rows]

    def album_page(self, where, args, limit=50, offset=0):
        if where not in {"h.run_id=?", "r.schedule_date=? OR (r.source='manual' AND substr(r.created_at,1,10)=?)"}:
            raise ValueError("invalid album query")
        with self.connect() as db:
            grouped = ("SELECT " + self.ALBUM_KEY_SQL + " AS album_key,"
                       "MAX(json_extract(payload,'$.album_name')) AS album_name,"
                       "MAX(json_extract(payload,'$.album_id')) AS album_id,"
                       "MAX(CAST(json_extract(payload,'$.album_count') AS INTEGER)) AS heat,"
                       "MAX(json_extract(payload,'$.date_str')) AS latest_date,COUNT(*) AS track_count "
                       "FROM hits h JOIN runs r ON h.run_id=r.id WHERE " + where + " GROUP BY album_key")
            total = db.execute("SELECT COUNT(*) FROM (" + grouped + ")", args).fetchone()[0]
            rows = db.execute("SELECT * FROM (" + grouped + ") ORDER BY COALESCE(heat,0) DESC,"
                              "latest_date DESC,album_name COLLATE NOCASE,album_key LIMIT ? OFFSET ?",
                              (*args, limit, offset)).fetchall()
            return [dict(row) for row in rows], total

    def album_tracks(self, where, args, album_key):
        if where not in {"h.run_id=?", "r.schedule_date=? OR (r.source='manual' AND substr(r.created_at,1,10)=?)"}:
            raise ValueError("invalid album query")
        with self.connect() as db:
            rows = db.execute("SELECT h.payload FROM hits h JOIN runs r ON h.run_id=r.id WHERE (" + where +
                              ") AND " + self.ALBUM_KEY_SQL + "=? ORDER BY "
                              "json_extract(h.payload,'$.date_str') DESC,json_extract(h.payload,'$.name'),"
                              "json_extract(h.payload,'$.song_name') COLLATE NOCASE,h.link",
                              (*args, album_key)).fetchall()
            return [json.loads(row[0]) for row in rows]

    def hit(self, run_id, payload):
        with self.connect() as db:
            db.execute("INSERT OR REPLACE INTO hits VALUES(?,?,?)",
                       (run_id, payload["link"], json.dumps(payload, ensure_ascii=False)))

    def log(self, run_id, level, text):
        with self.connect() as db:
            db.execute("INSERT INTO logs(run_id,created_at,level,text) VALUES(?,?,?,?)", (run_id, now_iso(), level, text))
            db.execute("DELETE FROM logs WHERE run_id=? AND seq NOT IN "
                       "(SELECT seq FROM logs WHERE run_id=? ORDER BY seq DESC LIMIT 1000)", (run_id, run_id))

    def logs(self, run_id):
        with self.connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM logs WHERE run_id=? ORDER BY seq DESC LIMIT 100", (run_id,)).fetchall()][::-1]

    def value(self, key):
        with self.connect() as db:
            row = db.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
            return row[0] if row else None

    def set_value(self, key, value):
        with self.connect() as db:
            db.execute("INSERT OR REPLACE INTO settings VALUES(?,?)", (key, value))

    def set_json(self, key, value):
        self.set_value(key, json.dumps(value, ensure_ascii=False, separators=(",", ":")))

    def get_json(self, key):
        value = self.value(key)
        return json.loads(value) if value else None

    def update_album_heat(self, run_id, album_counts):
        with self.connect() as db:
            rows = db.execute("SELECT link,payload FROM hits WHERE run_id=?", (run_id,)).fetchall()
            for row in rows:
                payload = json.loads(row["payload"])
                album_id = str(payload.get("album_id") or "")
                if album_id in album_counts:
                    payload["album_count"] = int(album_counts[album_id])
                    db.execute("UPDATE hits SET payload=? WHERE run_id=? AND link=?",
                               (json.dumps(payload, ensure_ascii=False), run_id, row["link"]))

    def recover(self):
        with self.connect() as db:
            db.execute("UPDATE runs SET status='interrupted',ended_at=?,error='服务重启，任务中断；已写入的数据保留' "
                       "WHERE status='running'", (now_iso(),))

    def scheduled_exists(self, day, slot="00:03"):
        with self.connect() as db:
            return db.execute("SELECT 1 FROM runs WHERE schedule_date=? AND schedule_time=?", (day, slot)).fetchone() is not None

    def pending(self):
        with self.connect() as db:
            return [self.decode(row) for row in db.execute("SELECT * FROM runs WHERE status='queued' ORDER BY created_at,rowid").fetchall()]


class Broadcast:
    def __init__(self, capacity=128):
        self.capacity = capacity
        self.lock = threading.Lock()
        self.clients = set()

    def subscribe(self):
        client = queue.Queue(maxsize=self.capacity)
        with self.lock:
            self.clients.add(client)
        return client

    def unsubscribe(self, client):
        with self.lock:
            self.clients.discard(client)

    def publish(self, message):
        with self.lock:
            for client in self.clients:
                try:
                    client.put_nowait(message)
                except queue.Full:
                    # A slow browser can reload durable state; producer never blocks.
                    try:
                        client.get_nowait()
                    except queue.Empty:
                        pass  # Consumer may have drained it after put_nowait.
                    client.put_nowait({"type": "resync", "run_id": message.get("run_id")})


class BusyError(Exception):
    pass


class Runtime:
    def __init__(self, config=None, db_path=None, worker=None):
        self.config = config or Config()
        self.store = Store(db_path or DATA / "scan_history.sqlite3")
        # Freeze the imported CSV before any server scan can append to it.
        if not self.store.value("migration_snapshot_initialized"):
            if engine.CSV_FILE.exists() and not self.store.list(limit=1):
                snapshot = DATA / "migration_archive" / "pre_migration" / "VA_Tracks_Output.csv"
                snapshot.parent.mkdir(parents=True, exist_ok=True)
                if not snapshot.exists():
                    shutil.copy2(engine.CSV_FILE, snapshot)
            self.store.set_value("migration_snapshot_initialized", now_iso())
        self.store.recover()
        self.lock = threading.RLock()
        self.record_exports = RecordExports(DATA)
        self.current = None
        self.thread = None
        self.events = Broadcast()
        self.worker = worker or engine.scan_worker
        self.use_m3_rounds = worker is None
        self.scheduler = None
        self.tokens = []
        self.done_message = None
        self.fatal = ""
        self.warnings = []
        self.task_errors = 0
        self.closed = False
        self.shutdown_complete = False
        self.scheduler_error = ""
        self.persistence_error = ""
        engine.emit = self.emit
        engine.ARTIST_CACHE_TTL_HOURS = 24
        self._original_resolve = engine.resolve_artist_list
        # The original engine emits detailed fallback warnings through emit().

    def scrub(self, value):
        if isinstance(value, dict):
            return {k: self.scrub(v) for k, v in value.items() if k not in {"scanCookie", "cookie"}}
        if isinstance(value, list):
            return [self.scrub(v) for v in value]
        if not isinstance(value, str):
            return value
        for token in sorted(self.tokens, key=len, reverse=True):
            if token:
                value = value.replace(token, "<redacted>").replace(quote(token, safe=""), "<redacted>")
        return engine.redacted_error_text(value)

    def emit(self, message):
        message = self.scrub(message)
        run_id = self.current
        if not run_id:
            return
        message["run_id"] = run_id
        kind = message.get("type")
        if kind == "hit":
            self.store.hit(run_id, message["data"])
        elif kind == "done":
            self.done_message = message
            for failure in message.get("summary", {}).get("failure_details", [])[:20]:
                self.store.log(run_id, "error", f"{failure['stage']} · ID {failure['item_id']}：{failure['error']}")
            for hit in message.get("results", []):
                self.store.hit(run_id, hit)
            # Terminal event is sent only after final metrics and status are persisted.
            return
        elif kind == "error":
            self.fatal = message.get("text", "扫描异常")
            self.store.log(run_id, "error", self.fatal)
        elif kind == "log":
            text = message.get("text", "")
            self.store.log(run_id, message.get("level", "info"), text)
            if "任务异常" in text:
                self.task_errors += 1
            if "回退缓存" in text or "缓存清理" in text:
                self.warnings.append(text)
        if message.get("metrics"):
            self.store.update(run_id, metrics=message["metrics"])
        self.events.publish(message)

    def parameters(self, payload=None, day=None):
        config = self.config.read()
        task_day = day or datetime.now(TZ).date()
        params = default_dates(task_day)
        params.update({key: config[key] for key in ("recentLimit", "ownedLimit", "newAlbumProbeLimit", "threads")})
        payload = payload or {}
        if not isinstance(payload, dict):
            raise ValueError("请求必须是 JSON 对象")
        requested_mode = payload.get("scanMode", config.get("scan_mode", "auto"))
        if requested_mode not in SCAN_MODES:
            raise ValueError("scanMode 必须为 auto、regular、m3 或 m3_intersection")
        cutoff = date.fromisoformat(config["m3_default_until"])
        params["scanMode"] = ("m3" if task_day <= cutoff else "regular") if requested_mode == "auto" else requested_mode
        # Credentials never enter run parameters; server file is the sole source.
        for key in (*DATE_KEYS, "recentLimit", "ownedLimit", "newAlbumProbeLimit", "threads", "debugAlbum"):
            if key in payload:
                params[key] = payload[key]
        if params["scanMode"] in {"m3", "m3_intersection"} and "externalStartDate" not in payload:
            # Historical scheduled runs and date-specific tests can predate
            # the configured M3 start. Never construct a reversed window.
            params["externalStartDate"] = min(config["m3_external_start_date"], params["externalEndDate"])
        if params["scanMode"] == "m3_intersection" and "newStartDate" not in payload:
            params["newStartDate"] = min(config["m3_external_start_date"], params["newEndDate"])
        for key in DATE_KEYS:
            if not isinstance(params[key], str):
                raise ValueError("日期必须使用 YYYY-MM-DD")
            date.fromisoformat(params[key])
        if params["externalStartDate"] > params["externalEndDate"] or params["newStartDate"] > params["newEndDate"]:
            raise ValueError("开始日期不能晚于结束日期")
        for key, lo, hi in (("recentLimit", 10, 200), ("ownedLimit", 10, 200), ("newAlbumProbeLimit", 0, 100),
                            ("threads", 1, self.config.thread_limit())):
            params[key] = int(params[key])
            if not lo <= params[key] <= hi:
                raise ValueError(f"{key} 必须在 {lo} 至 {hi} 之间")
        params["debugAlbum"] = engine.parse_debug_album_id(params.get("debugAlbum", ""))
        return params

    def start(self, payload=None, retry_of=None):
        params = self.parameters(payload)
        if retry_of:
            params["retry_of"] = retry_of
        # Check configuration before creating a manual run; expiry is checked in worker.
        self.config.cookie()
        with self.lock:
            if self.current or self.store.pending() or self.closed:
                raise BusyError("已有扫描任务运行或等待中")
            run_id = self.store.create(params)
            self._launch(run_id, params)
            return run_id

    def retry(self, run_id):
        previous = self.store.get(run_id)
        if previous is None:
            raise KeyError(run_id)
        if previous["status"] not in {"partial", "failed", "stopped", "interrupted"}:
            raise ValueError("仅部分完成、失败、停止或中断的任务可补扫")
        # Preserve old windows/depths, using today's saved concurrency.
        keys = (*DATE_KEYS, "recentLimit", "ownedLimit", "newAlbumProbeLimit", "debugAlbum", "scanMode")
        return self.start({key: previous["params"][key] for key in keys if key in previous["params"]}, retry_of=run_id)

    def _launch(self, run_id, params):
        self.store.update(run_id, status="running", started_at=now_iso())
        run = self.store.get(run_id)
        source = f"自动扫描 {run['schedule_time']}" if run["source"] == "scheduled" else "手动扫描"
        if params.get("retry_of"):
            source = f"补扫（原任务 {params['retry_of']}）"
        self.store.log(run_id, "info", f"{source}启动：任务创建时并发 {params['threads']}；"
                       f"外部合辑 {params['externalStartDate']} 至 {params['externalEndDate']}；"
                       f"新专／新歌 {params['newStartDate']} 至 {params['newEndDate']}。"
                       "运行中的任务保留创建时参数，修改默认配置仅影响之后创建的任务。")
        self.current = run_id
        self.done_message = None
        self.round_messages = []
        self.m3_report = None
        self.fatal = ""
        self.warnings = []
        self.task_errors = 0
        engine.CONTROL = engine.ControlState(target_threads=params["threads"], baseline_threads=params["threads"])
        engine.METRICS = engine.Metrics()
        engine.is_server_scanning = True
        # Artist album-page cache is per-run, unlike durable date cache.
        engine.ARTIST_ALBUM_PAGE_CACHE.clear()
        self.thread = threading.Thread(target=self._run, args=(run_id, params), name="scan-worker", daemon=True)
        self.thread.start()

    @staticmethod
    def _merge_round_summaries(messages, total_artists):
        summaries = [message.get("summary", {}) for message in messages]
        merged = {}
        count_keys = set().union(*(summary.keys() for summary in summaries))
        special = {"failure_details", "round_artist_names", "album_artist_names",
                   "album_artist_counts", "slow_retry", "unique_album_ids",
                   "date_hit_album_ids", "verify_artist_ids"}
        for key in count_keys - special:
            values = [summary.get(key) for summary in summaries if key in summary]
            if values and all(isinstance(value, (int, float)) and not isinstance(value, bool)
                              for value in values):
                merged[key] = sum(values)
            elif values:
                merged[key] = values[-1]

        merged["artists"] = total_artists
        for source_key, target_key in (("unique_album_ids", "unique_albums"),
                                       ("date_hit_album_ids", "date_hit_albums"),
                                       ("verify_artist_ids", "verify_artists")):
            ids = {str(item) for summary in summaries for item in summary.get(source_key, [])}
            if ids:
                merged[target_key] = len(ids)
        merged["round_artist_names"] = sorted({
            str(name) for summary in summaries for name in summary.get("round_artist_names", [])
        })

        album_artist_names = {}
        for summary in summaries:
            for album_id, names in summary.get("album_artist_names", {}).items():
                album_artist_names.setdefault(str(album_id), set()).update(
                    str(name) for name in names if name
                )
        if album_artist_names:
            merged["album_artist_counts"] = {
                album_id: len(names) for album_id, names in album_artist_names.items()
            }
        else:
            # Compatibility for test and older injected workers without the
            # per-album name sets needed for an exact cross-round union.
            counts = {}
            for summary in summaries:
                for album_id, count in summary.get("album_artist_counts", {}).items():
                    counts[str(album_id)] = counts.get(str(album_id), 0) + int(count)
            merged["album_artist_counts"] = counts
        merged.pop("album_artist_names", None)

        retry_totals = {}
        for summary in summaries:
            for key, value in summary.get("slow_retry", {}).items():
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    retry_totals[key] = retry_totals.get(key, 0) + value
                elif key not in retry_totals:
                    retry_totals[key] = value
        if retry_totals:
            merged["slow_retry"] = retry_totals

        # Preserve exact set cardinalities for fields emitted by current workers;
        # older/injected workers retain additive values above.
        for key in ("unique_albums", "date_hit_albums", "verify_artists"):
            if key not in merged:
                merged[key] = sum(summary.get(key, 0) for summary in summaries)

        failures = []
        seen_failures = set()
        for summary in summaries:
            for failure in summary.get("failure_details", []):
                if not isinstance(failure, dict):
                    continue
                key = (failure.get("stage"), failure.get("item_id"), failure.get("error"))
                if key not in seen_failures:
                    failures.append(failure)
                    seen_failures.add(key)
        merged["failure_details"] = failures
        return merged

    def _run(self, run_id, params):
        completed_messages = []
        round_errors = []
        round_statuses = []
        m3_report = None
        try:
            config = self.config.read()
            artist_cookie = self.config.cookie(config)
            scan_cookie = self.config.scan_cookie(config)
            dual_cookie = scan_cookie != artist_cookie
            tokens = []
            for ck in (artist_cookie, scan_cookie):
                if ck:
                    tokens.append(ck)
                    bare = ck.removeprefix("MUSIC_U=").rstrip(";")
                    if bare:
                        tokens.append(bare)
            self.tokens = list(dict.fromkeys(tokens))
            verify_login(scan_cookie)
            if dual_cookie:
                self.emit({"type": "log", "level": "info", "text": "双 Cookie 模式已启用：大号用于拉取关注艺人列表，小号用于执行歌曲与专辑扫描。"})
            # Refresh once per Shanghai calendar day, including legacy caches that
            # were copied preserving a recent mtime. A failed refresh stays visible.
            cache_key = engine.cookie_fingerprint(artist_cookie)
            today = datetime.now(TZ).date().isoformat()
            meta = engine.load_artist_cache_meta(cache_key)
            engine.REFRESH_ARTIST_CACHE = meta.get("server_refresh_date") != today
            fallback_key = str(config.get("artist_cache_key", "")).strip()
            if fallback_key and not re.fullmatch(r"[0-9a-f]{16}", fallback_key):
                raise ValueError("artist_cache_key 必须为迁移缓存的 16 位指纹")
            artist_source_arg = artist_cookie if dual_cookie else ""
            def resolve(scan_cookie_arg, source_input):
                try:
                    artists, key, mode = self._original_resolve(scan_cookie_arg, source_input or artist_source_arg)
                except Exception:
                    artists = engine.load_artist_cache_by_key(fallback_key) if fallback_key else None
                    if not artists:
                        raise
                    key, mode = fallback_key, "fallback-cache"
                    self.emit({"type": "log", "level": "error", "text": "实时拉取失败，回退缓存至已配置的迁移艺人列表"})
                if mode == "live":
                    meta = engine.load_artist_cache_meta(key)
                    live_meta = dict(getattr(engine.fetch_artist_sublist, "last_meta", {}) or meta)
                    if engine.is_artist_sublist_incomplete(artists, live_meta) and fallback_key and fallback_key != key:
                        fallback_artists = engine.load_artist_cache_by_key(fallback_key)
                        if fallback_artists and len(fallback_artists) > len(artists):
                            self.emit({
                                "type": "log",
                                "level": "error",
                                "text": (
                                    f"实时拉取艺人列表不完整（仅 {len(artists)} 位），"
                                    f"已回退缓存至已配置的迁移艺人列表 {fallback_key}（{len(fallback_artists)} 位）。"
                                ),
                            })
                            return fallback_artists, fallback_key, "fallback-cache"
                    if meta.get("complete_confirmed") is False:
                        warning = "关注艺人列表的完整性未确认，缓存可能不完整"
                        self.warnings.append(warning)
                        self.emit({"type": "log", "level": "error", "text": warning})
                    meta["server_refresh_date"] = today
                    engine.save_artist_cache(key, artists, meta)
                return artists, key, mode
            engine.resolve_artist_list = resolve
            dates = [datetime.combine(date.fromisoformat(params[k]), time.min) for k in DATE_KEYS]
            dates[1] = dates[1].replace(hour=23, minute=59, second=59, microsecond=999999)
            dates[3] = dates[3].replace(hour=23, minute=59, second=59, microsecond=999999)
            # Test/injected workers are deliberately kept on the single-round
            # path; the production engine is the only worker that understands
            # the M3 round arguments and owns the shared CSV state.
            if params.get("scanMode") in {"m3", "m3_intersection"} and self.use_m3_rounds:
                intersection_only = params["scanMode"] == "m3_intersection"
                artists, artist_key, artist_source_mode = engine.resolve_artist_list(scan_cookie, artist_source_arg)
                if not artists:
                    raise ValueError("关注艺人列表为空，M3 扫描无法继续")
                config = self.config.read()
                m3_path = Path(config["m3_artist_file"])
                if not m3_path.is_absolute():
                    m3_path = self.config.path.parent / m3_path
                m3_names = load_m3_artist_names(m3_path)
                prioritized, remaining, match_summary = partition_m3_followed_artists(artists, m3_names)
                m3_bytes = m3_path.read_bytes()
                m3_report = {
                    "schema_version": 1,
                    "run_id": run_id,
                    "scan_mode": params["scanMode"],
                    "intersection_only": intersection_only,
                    "created_at": now_iso(),
                    "artist_source": artist_source_mode,
                    "artist_cache_key": artist_key,
                    "m3_artist_file": m3_path.name,
                    "m3_artist_sha256": hashlib.sha256(m3_bytes).hexdigest(),
                    "m3_artist_records": len(m3_names),
                    "summary": {key: value for key, value in match_summary.items()
                                if key not in {"matches", "ambiguous_matches", "unmatched_matches"}},
                    "matches": match_summary["matches"],
                    "ambiguous_matches": match_summary["ambiguous_matches"],
                    "unmatched_followed": match_summary["unmatched_matches"],
                }
                self.m3_report = m3_report
                self.store.set_json(f"m3_match_report:{run_id}", m3_report)
                self.store.log(run_id, "info",
                               f"M3 名称配对完成：关注艺人 {match_summary['followed']} 位；"
                               f"M3 优先轮 {len(prioritized)} 位（同名 {match_summary['exact_matches']}，"
                               f"大小写折叠 {match_summary['case_insensitive_matches']}，"
                               f"Unicode/空格归一化 {match_summary['other_normalized_matches']}，"
                               f"标点归一化 {match_summary['punctuation_matches']}，"
                               f"使用别名 {match_summary['alias_matches']}）；"
                               + (f"本次仅扫描交集，排除其余 {len(remaining)} 位关注艺人"
                                  f"（含歧义 {match_summary['ambiguous']} 位），不执行第二轮。"
                                  if intersection_only else
                                  f"歧义 {match_summary['ambiguous']} 位留在第二轮；其余关注艺人 {len(remaining)} 位。"))
                groups = ([("仅 M3 交集扫描", prioritized)] if intersection_only else
                          [("M3 匹配艺人优先轮", prioritized), ("其余关注艺人第二轮", remaining)])
                if not any(group for _, group in groups):
                    raise ValueError("M3 配对后没有可扫描的关注艺人；交集模式只扫描唯一、明确匹配项")
                scan_artist_count = len(prioritized) if intersection_only else len(artists)
                engine.METRICS.artist_progress_total = scan_artist_count
                engine.METRICS.artist_total = scan_artist_count
                offset = 0
                last_nonempty_index = max(index for index, (_, group) in enumerate(groups) if group)
                scan_started = False
                for index, (round_name, group) in enumerate(groups):
                    if not group:
                        round_statuses.append({"name": round_name, "artists": 0,
                                               "status": "skipped_empty", "error": "该轮没有艺人"})
                        continue
                    if engine.CONTROL.snapshot()["stop_requested"]:
                        round_statuses.append({"name": round_name, "artists": len(group),
                                               "status": "skipped_stop", "error": "扫描已停止，未开始此轮"})
                        break
                    engine.METRICS.artist_progress_offset = offset
                    self.done_message = None
                    self.fatal = ""
                    final_round = index == last_nonempty_index
                    end_day_defaults = default_dates(date.fromisoformat(params["externalEndDate"]))
                    round_dates = list(dates)
                    round_recent_limit = params["recentLimit"]
                    if (params.get("scanMode") == "m3" and index == 0
                            and not params.get("retry_of")
                            and params.get("newStartDate") == end_day_defaults["newStartDate"]):
                        m3_new_start = min(config["m3_external_start_date"], params["newEndDate"])
                        round_dates[2] = datetime.combine(date.fromisoformat(m3_new_start), time.min)
                    elif params.get("scanMode") == "m3" and index == 1:
                        m3_ext_default = min(config["m3_external_start_date"], params["externalEndDate"])
                        if not params.get("retry_of") and params.get("externalStartDate") == m3_ext_default:
                            round_dates[0] = datetime.combine(
                                date.fromisoformat(end_day_defaults["externalStartDate"]), time.min
                            )
                        second_round_recent = int(config.get("m3_second_round_recent_limit") or 0)
                        if second_round_recent > 0:
                            round_recent_limit = min(params["recentLimit"], second_round_recent)
                    self.store.log(
                        run_id,
                        "info",
                        f"开始 {round_name}：{len(group)} 位艺人（外部合辑 {round_dates[0]:%Y-%m-%d}~{round_dates[1]:%Y-%m-%d}，"
                        f"新专/新歌 {round_dates[2]:%Y-%m-%d}~{round_dates[3]:%Y-%m-%d}，recent{round_recent_limit}）。",
                    )
                    source_label = "M3-intersection" if intersection_only else f"M3-{index + 1}"
                    self.worker(scan_cookie, artist_source_arg, *round_dates, round_recent_limit, params["ownedLimit"],
                                params["newAlbumProbeLimit"], params["debugAlbum"],
                                artists_override=group, artist_source_label=source_label,
                                finalize=final_round, reset_seen=not scan_started,
                                include_merge_details=True)
                    scan_started = True
                    message = self.done_message
                    round_error = self.fatal
                    if message:
                        completed_messages.append(message)
                        round_summary = message.get("summary", {})
                        failures = any(round_summary.get(key, 0) for key in FAILURE_KEYS)
                        round_statuses.append({"name": round_name, "artists": len(group),
                                               "status": "partial" if failures or round_error else "success",
                                               "error": round_error})
                    else:
                        round_error = round_error or f"{round_name} 未能正常完成"
                        round_errors.append(f"{round_name}：{round_error}")
                        round_statuses.append({"name": round_name, "artists": len(group),
                                               "status": "failed", "error": round_error})
                    if round_error:
                        self.fatal = ""
                    offset += len(group)
                self.m3_report["rounds"] = round_statuses
                self.m3_report["summary"]["completed_rounds"] = sum(
                    item["status"] in {"success", "partial"} for item in round_statuses)
                self.store.set_json(f"m3_match_report:{run_id}", self.m3_report)
                if completed_messages:
                    merged_results = {}
                    for message in completed_messages:
                        for hit in message.get("results", []):
                            if hit.get("link"):
                                merged_results.setdefault(hit["link"], dict(hit))
                    self.done_message = dict(completed_messages[-1])
                    self.done_message["results"] = list(merged_results.values())
                    self.done_message["summary"] = self._merge_round_summaries(completed_messages, scan_artist_count)
                    if round_errors:
                        self.done_message["summary"]["m3_round_errors"] = len(round_errors)
                        self.done_message["summary"]["failure_details"] = list(
                            self.done_message["summary"].get("failure_details", [])) + [
                                {"stage": "M3 scan round", "item_id": str(i + 1), "error": error}
                                for i, error in enumerate(round_errors[:20])]
                    self.done_message["summary"]["m3_scan"] = {
                        "match_count": match_summary["matched"],
                        "matched_account_count": len(prioritized),
                        "second_round_count": 0 if intersection_only else len(remaining),
                        "ambiguous_count": match_summary["ambiguous"],
                        "intersection_only": intersection_only,
                        "excluded_count": len(remaining) if intersection_only else 0,
                        "rounds": round_statuses,
                    }
                    album_counts = self.done_message["summary"].get("album_artist_counts", {})
                    for hit in self.done_message["results"]:
                        album_id = str(hit.get("album_id") or "")
                        if album_id in album_counts:
                            hit["album_count"] = album_counts[album_id]
                    self.done_message["results"].sort(
                        key=lambda hit: (hit.get("album_count", 0), hit.get("date_str", "")), reverse=True)
                    self.store.update_album_heat(run_id, album_counts)
                else:
                    self.fatal = "M3 扫描没有任何一轮正常完成"
                if round_errors:
                    self.warnings.append("M3 扫描有轮次未正常完成，结果可能不完整")
            else:
                self.worker(scan_cookie, artist_source_arg, *dates, params["recentLimit"], params["ownedLimit"],
                            params["newAlbumProbeLimit"], params["debugAlbum"])
        except Exception as exc:
            self.fatal = self.scrub(str(exc))
            self.store.log(run_id, "error", self.fatal)
            logging.error("扫描任务 %s 失败：%s", run_id, self.fatal)
        finally:
            engine.resolve_artist_list = self._original_resolve
            engine.METRICS.finish()
            metrics = self.scrub(engine.METRICS.snapshot())
            summary = (self.done_message or {}).get("summary", {})
            summary["task_errors"] = self.task_errors
            stopped = engine.CONTROL.snapshot()["stop_requested"]
            if self.fatal:
                status = "failed"
            elif stopped:
                status = "stopped"
            elif not self.done_message:
                status = "failed"
                self.fatal = "扫描未正常完成，请查看日志"
            elif (summary.get("artists", 0) > 0 and
                  summary.get("artist_failures", 0) >= summary["artists"] and
                  (params.get("newAlbumProbeLimit", 20) <= 0 or
                   summary.get("new_album_probe_failures", 0) >= summary["artists"])):
                status = "failed"
                self.fatal = (
                    "所有艺人采样均失败"
                    if params.get("newAlbumProbeLimit", 20) <= 0
                    else "所有艺人采样及新专探测均失败"
                )
            elif any(summary.get(k, 0) for k in (*FAILURE_KEYS, "task_errors", "m3_round_errors")) or self.warnings:
                status = "partial"
            else:
                status = "success"
            try:
                self.store.update(run_id, status=status, ended_at=now_iso(), summary=summary,
                                  metrics=metrics, error=self.fatal, warning="\n".join(self.warnings[:10]))
                self.events.publish({"type": "finished", "run_id": run_id, "status": status, "metrics": metrics})
            except Exception as exc:
                # Fail closed if completion cannot be recorded; do not start more
                # tasks while durable storage is unhealthy or claim success.
                self.persistence_error = "任务记录写入失败，已停止接受新扫描；请检查磁盘与数据库后重启服务"
                self.closed = True
                logging.error("%s (%s)", self.persistence_error, type(exc).__name__)
                self.events.publish({"type": "storage_error", "run_id": run_id, "text": self.persistence_error})
            finally:
                with self.lock:
                    self.current = None
                    engine.is_server_scanning = False
                    self.tokens = []

    def tick(self, now=None, do_backup=True):
        if self.closed:
            return
        now = now or datetime.now(TZ)
        now = now.astimezone(TZ)
        config = self.config.read()
        with self.lock:
            today = now.date().isoformat()
            if config["schedule_enabled"]:
                for slot in config["schedule_times"]:
                    hour, minute = map(int, slot.split(":"))
                    first_day = config["schedule_start_dates"].get(slot, today)
                    last_day = config["schedule_end_dates"].get(slot)
                    if (today >= first_day and (last_day is None or today <= last_day) and
                            now.time().replace(tzinfo=None) >= time(hour, minute)):
                        if not self.store.scheduled_exists(today, slot):
                            mode = config["schedule_modes"].get(slot, config["scan_mode"])
                            self.store.create(self.parameters({"scanMode": mode}, day=now.date()),
                                              "scheduled", today, schedule_time=slot)
            pending = self.store.pending()
            for run in pending:
                if run["schedule_date"] and run["schedule_date"] < today:
                    self.store.update(run["id"], status="interrupted", ended_at=now_iso(), error="跨日后不补跑旧定时任务")
            if not self.current and not self.closed:
                pending = self.store.pending()
                if pending:
                    self._launch(pending[0]["id"], pending[0]["params"])
                elif do_backup and now.time().replace(tzinfo=None) >= time(3):
                    self.backup(now.date(), config["backup_retention_days"])

    def safe_tick(self):
        try:
            self.tick()
            self.scheduler_error = ""
        except Exception as exc:
            # Config errors must be observable even with no active run.
            self.scheduler_error = "调度或备份异常，请检查服务器配置与日志"
            logging.error("%s (%s)", self.scheduler_error, type(exc).__name__)

    def start_scheduler(self):
        from apscheduler.schedulers.background import BackgroundScheduler
        self.scheduler = BackgroundScheduler(timezone=TZ)
        for slot in self.config.read()["schedule_times"]:
            hour, minute = map(int, slot.split(":"))
            self.scheduler.add_job(self.safe_tick, "cron", hour=hour, minute=minute,
                                   id=f"daily-scan-{slot.replace(':', '')}", max_instances=1, coalesce=True)
        self.scheduler.add_job(self.safe_tick, "interval", seconds=30, id="recover-and-backup", max_instances=1, coalesce=True)
        self.scheduler.start()
        self.safe_tick()

    def backup(self, day, retention):
        key = "backup_date"
        if self.store.value(key) == day.isoformat():
            return
        backup_dir = DATA / "backups"
        backup_dir.mkdir(exist_ok=True)
        db_copy = backup_dir / "snapshot.sqlite3"
        with closing(sqlite3.connect(self.store.path)) as source, closing(sqlite3.connect(db_copy)) as dest:
            source.backup(dest)
        target = backup_dir / f"data-{day.isoformat()}.tar.gz"
        tmp = target.with_suffix(".tmp")
        try:
            with tarfile.open(tmp, "w:gz") as archive:
                archive.add(db_copy, arcname="data/scan_history.sqlite3")
                for path in DATA.iterdir():
                    if path.name in {"VA_Tracks_Output.csv", "VA_Album_Date_Cache.json", "VA_Artist_Caches",
                                     "VA_Scan_Metrics.json", "VA_Scan_Tuning_Report.txt"}:
                        archive.add(path, arcname=f"data/{path.name}")
            tmp.replace(target)
            self.store.set_value(key, day.isoformat())
            cutoff = day - timedelta(days=retention)
            for old in backup_dir.glob("data-????-??-??.tar.gz"):
                if date.fromisoformat(old.name[5:15]) < cutoff:
                    old.unlink()
        finally:
            db_copy.unlink(missing_ok=True)
            tmp.unlink(missing_ok=True)

    def close(self):
        if self.shutdown_complete:
            return
        self.shutdown_complete = True
        self.closed = True
        if self.scheduler and self.scheduler.running:
            self.scheduler.shutdown(wait=False)
        if self.current:
            engine.CONTROL.stop()
            if self.thread:
                self.thread.join(timeout=25)
        self.record_exports.close()


def verify_login(cookie, api_base=None):
    try:
        payload_data = {
            "cookie": cookie,
            "timestamp": int(datetime.now().timestamp() * 1000),
            **engine.api_transport_overrides(),
        }
        response = requests.post((api_base or engine.API_BASE) + "/login/status",
                                 data=payload_data, timeout=8)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        # requests errors may contain a full URL; expose only a generic diagnosis.
        raise ValueError(f"网易云 API 登录检查失败（{type(exc).__name__}）") from None
    data = payload.get("data", payload)
    if data.get("code") != 200 or not (data.get("account") or {}).get("id"):
        raise ValueError("Cookie 已失效或尚未登录，请更新服务器凭据文件")


def csv_response(rows, filename, include_heat=False):
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(CSV_HEADER + (["热度（同专辑命中艺人数）"] if include_heat else []))
    for hit in rows:
        keys = ("name", "song_name", "album_name", "date_str", "link", "flag") + (("album_count",) if include_heat else ())
        writer.writerow([hit.get(key, "") for key in keys])
    return Response(buf.getvalue().encode("utf-8-sig"), mimetype="text/csv",
                    headers={"Content-Disposition": f'attachment; filename="{filename}"'})


def create_app(runtime=None):
    runtime = runtime or Runtime()
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 16384
    app.extensions["scanner"] = runtime

    @app.before_request
    def protect_writes():
        # Public authentication terminates at Caddy; app and API ports are private.
        # Reject cross-site writes even when browser Basic Auth is cached.
        if request.method == "POST" and request.headers.get("X-Scanner-Request") != "1":
            return jsonify(status="error", msg="缺少请求校验标识"), 403

    @app.after_request
    def headers(response):
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        return response

    @app.get("/")
    def index():
        return render_template("index.html")

    @app.get("/health")
    def health():
        return jsonify(status="ok")

    @app.get("/api/status")
    def status():
        config = runtime.config.read()
        try:
            runtime.config.cookie(config)
            configured = True
        except (OSError, ValueError):
            configured = False
        return jsonify(current=runtime.store.get(runtime.current) if runtime.current else None,
                       pending=runtime.store.pending(), state=engine.CONTROL.snapshot() if runtime.current else None,
                       metrics=engine.METRICS.snapshot() if runtime.current else None,
                       defaults=runtime.parameters(), cookie_configured=configured,
                       scan_mode=config["scan_mode"],
                       regular_defaults=default_dates(datetime.now(TZ).date()),
                       m3_external_start_date=config["m3_external_start_date"],
                       mode_defaults={
                           "auto": runtime.parameters({"scanMode": "auto"}),
                           "regular": runtime.parameters({"scanMode": "regular"}),
                           "m3": runtime.parameters({"scanMode": "m3"}),
                           "m3_intersection": runtime.parameters({"scanMode": "m3_intersection"}),
                       },
                       schedule_enabled=config["schedule_enabled"], schedule=format_schedule_times(config["schedule_times"]),
                       schedule_times=config["schedule_times"], schedule_start_dates=config["schedule_start_dates"], timezone="Asia/Shanghai",
                       schedule_modes=config["schedule_modes"], schedule_end_dates=config["schedule_end_dates"],
                       schedule_details=schedule_details(config, datetime.now(TZ).date()),
                       scheduler_error=runtime.persistence_error or runtime.scheduler_error,
                       accepting_scans=not runtime.closed,
                       thread_limit=runtime.config.thread_limit(),
                       backup_date=runtime.store.value("backup_date"))

    @app.post("/api/start")
    def start():
        try:
            payload = request.get_json(silent=True)
            if not isinstance(payload, dict):
                raise ValueError("请提交 JSON 对象")
            return jsonify(status="ok", run_id=runtime.start(payload))
        except BusyError as exc:
            return jsonify(status="error", msg=str(exc)), 409
        except (ValueError, TypeError, OSError):
            return jsonify(status="error", msg="参数或凭据配置有误，请检查日期、数量及服务器 Cookie 文件"), 400

    @app.post("/api/control")
    def control():
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify(status="error", msg="请提交 JSON 对象"), 400
        with runtime.lock:
            if not runtime.current:
                return jsonify(status="error", msg="当前没有运行中的扫描"), 409
            try:
                action = payload.get("action")
                if action == "threads":
                    threads = int(payload.get("value"))
                    if not 1 <= threads <= runtime.config.thread_limit():
                        raise ValueError('并发超过服务器配置上限')
                    previous = engine.CONTROL.snapshot()["target_threads"]
                    engine.CONTROL.set_threads_manual(threads)
                    runtime.emit({"type": "log", "level": "info", "text":
                                  f"本次任务运行并发由 {previous} 调整为 {threads}；定时默认并发未修改。"})
                elif action in {"pause", "resume", "stop"}:
                    getattr(engine.CONTROL, action)()
                else:
                    raise ValueError("未知控制动作")
            except (ValueError, TypeError):
                return jsonify(status="error", msg="控制参数错误"), 400
            state = engine.CONTROL.snapshot()
            runtime.emit({"type": "state", "state": state, "metrics": engine.METRICS.snapshot()})
            return jsonify(status="ok", state=state, metrics=engine.METRICS.snapshot())

    @app.post("/api/runs/<run_id>/retry")
    def retry(run_id):
        try:
            return jsonify(status="ok", run_id=runtime.retry(run_id))
        except KeyError:
            return jsonify(status="error", msg="任务不存在"), 404
        except BusyError as exc:
            return jsonify(status="error", msg=str(exc)), 409
        except (ValueError, TypeError, OSError) as exc:
            return jsonify(status="error", msg=runtime.scrub(str(exc))), 400

    def paging():
        return max(1, min(int(request.args.get("limit", 200)), 500)), max(0, int(request.args.get("offset", 0)))

    def album_paging():
        return max(1, min(int(request.args.get("album_limit", 50)), 100)), max(0, int(request.args.get("album_offset", 0)))

    def csv_album_data(path, album_key=None, limit=50, offset=0):
        groups = {}
        tracks = []
        if path.exists():
            with engine.CSV_LOCK, path.open(newline="", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) < 6:
                        continue
                    item = dict(zip(("name", "song_name", "album_name", "date_str", "link", "flag"), row[:6]))
                    key = f"name:{item['album_name']}" if item["album_name"] else f"link:{item['link']}"
                    group = groups.setdefault(key, {"album_key": key, "album_name": item["album_name"],
                                                     "album_id": "", "heat": None, "latest_date": item["date_str"],
                                                     "track_count": 0})
                    group["track_count"] += 1
                    group["latest_date"] = max(group["latest_date"], item["date_str"])
                    if album_key == key:
                        tracks.append(item)
        ordered = sorted(groups.values(), key=lambda item: (item["latest_date"], item["album_name"].casefold()), reverse=True)
        if album_key is not None:
            return tracks
        return ordered[offset:offset + limit], len(ordered)

    @app.get("/api/runs")
    def runs():
        limit, offset = paging()
        return jsonify(runs=runtime.store.list(limit, offset))

    @app.post('/api/records/export')
    def export_records():
        # Hold the same lock as manual launch and scheduler while making a copy.
        with runtime.lock:
            if runtime.current or runtime.store.pending() or runtime.closed:
                return jsonify(msg='请等待扫描结束后再回传记录'), 409
            try:
                return jsonify(runtime.record_exports.create(now_iso()))
            except ExportLimitError as exc:
                return jsonify(msg=str(exc)), 409
            except (OSError, ValueError):
                return jsonify(msg='记录导出失败，请检查磁盘空间、记录文件或稍后重试'), 409

    @app.get('/api/records/<token>/file/<path:name>')
    def export_file(token, name):
        path = runtime.record_exports.file(token, name)
        if path is None:
            return jsonify(msg='回传包已过期或文件不存在，请重新导出'), 404
        return send_file(path, mimetype='application/octet-stream')

    @app.get('/api/records/<token>/zip')
    def export_zip(token):
        path = runtime.record_exports.archive(token)
        if path is None:
            return jsonify(msg='回传包已过期，请重新导出'), 404
        return send_file(path, mimetype='application/zip', as_attachment=True,
                         download_name=f'netease-records-{token[:8]}.zip')

    @app.post('/api/records/<token>/release')
    def release_export(token):
        runtime.record_exports.release(token)
        return jsonify(status='ok')

    def requested_day():
        day = request.args.get("date", datetime.now(TZ).date().isoformat())
        return date.fromisoformat(day).isoformat()

    @app.get("/api/today")
    def today():
        day = requested_day()
        limit, offset = paging()
        album_limit, album_offset = album_paging()
        daily_runs = runtime.store.day_runs(day)
        albums, album_total = runtime.store.album_page(
            "r.schedule_date=? OR (r.source='manual' AND substr(r.created_at,1,10)=?)", (day, day),
            album_limit, album_offset)
        return jsonify(date=day, timezone="Asia/Shanghai", runs=daily_runs,
                       total=sum(run["result_count"] for run in daily_runs),
                       albums=albums, total_albums=album_total,
                       results=runtime.store.day_hits(day, limit, offset))

    @app.get("/api/today/csv")
    def today_csv():
        day = requested_day()
        return csv_response(runtime.store.day_hits(day, limit=-1), f"scan-{day}.csv", include_heat=True)

    @app.get("/api/runs/<run_id>")
    def run(run_id):
        result = runtime.store.get(run_id)
        if not result:
            return jsonify(msg="任务不存在"), 404
        limit, offset = paging()
        album_limit, album_offset = album_paging()
        albums, album_total = runtime.store.album_page("h.run_id=?", (run_id,), album_limit, album_offset)
        return jsonify(run=result, results=runtime.store.hits(run_id, limit, offset), logs=runtime.store.logs(run_id),
                       albums=albums, total_albums=album_total)

    @app.get("/api/album-tracks")
    def album_tracks():
        source = request.args.get("source", "")
        key = request.args.get("album", "")
        if not key:
            return jsonify(msg="缺少专辑标识"), 400
        if source == "today":
            day = requested_day()
            rows = runtime.store.album_tracks(
                "r.schedule_date=? OR (r.source='manual' AND substr(r.created_at,1,10)=?)", (day, day), key)
        elif source == "run":
            run_id = request.args.get("run_id", "")
            if not runtime.store.get(run_id):
                return jsonify(msg="任务不存在"), 404
            rows = runtime.store.album_tracks("h.run_id=?", (run_id,), key)
        elif source in {"legacy", "pre-migration"}:
            path = engine.CSV_FILE if source == "legacy" else DATA / "migration_archive" / "pre_migration" / "VA_Tracks_Output.csv"
            rows = csv_album_data(path, album_key=key)
        else:
            return jsonify(msg="不支持的结果来源"), 400
        return jsonify(results=rows)

    @app.get("/api/runs/<run_id>/csv")
    def run_csv(run_id):
        if not runtime.store.get(run_id):
            return jsonify(msg="任务不存在"), 404
        return csv_response(runtime.store.hits(run_id, limit=-1), f"scan-{run_id}.csv", include_heat=True)

    @app.get("/api/runs/<run_id>/m3")
    def run_m3(run_id):
        if not runtime.store.get(run_id):
            return jsonify(msg="任务不存在"), 404
        report = runtime.store.get_json(f"m3_match_report:{run_id}")
        if report is None:
            return jsonify(msg="该任务没有 M3 配对报告"), 404
        return jsonify(report)

    @app.get("/api/runs/<run_id>/m3.csv")
    def run_m3_csv(run_id):
        if not runtime.store.get(run_id):
            return jsonify(msg="任务不存在"), 404
        report = runtime.store.get_json(f"m3_match_report:{run_id}")
        if report is None:
            return jsonify(msg="该任务没有 M3 配对报告"), 404
        buf = io.StringIO(newline="")
        writer = csv.writer(buf)
        writer.writerow(["类别", "网易云艺人 ID", "网易云艺人", "M3 艺人 ID", "M3 艺人", "匹配标签", "匹配来源", "匹配方式", "网易云候选 ID / 名称", "M3 候选 ID / 艺人", "原因"])
        ambiguous_label = "未纳入交集·歧义" if report.get("intersection_only") else "第二轮·歧义"
        unmatched_label = "未纳入交集·无匹配" if report.get("intersection_only") else "第二轮·未匹配"
        for item in report.get("matches", []):
            writer.writerow(["M3交集" if report.get("intersection_only") else "第一轮", item.get("netease_id", ""), item.get("netease_name", ""),
                             item.get("m3_artist_id", ""), item.get("m3_name", ""),
                             item.get("matched_label", ""), item.get("match_source", ""),
                             item.get("match_kind", ""), "", "", ""])
        for item in report.get("ambiguous_matches", []):
            netease_candidates = " / ".join(
                f"{candidate['id']}:{candidate['name']}" for candidate in item.get("netease_candidates", []))
            m3_candidates = item.get("m3_candidate_records", [])
            m3_candidate_text = " / ".join(
                f"{candidate['m3_artist_id']}:{candidate['name']}" if candidate.get("m3_artist_id")
                else candidate["name"] for candidate in m3_candidates)
            if not m3_candidate_text:
                m3_candidate_text = " / ".join(item.get("m3_candidates", []))
            writer.writerow([ambiguous_label, item.get("netease_id", ""), item.get("netease_name", ""), "", "", "", "", "",
                             netease_candidates, m3_candidate_text, item.get("reason", "")])
        for item in report.get("unmatched_followed", []):
            writer.writerow([unmatched_label, item.get("netease_id", ""), item.get("netease_name", ""), "", "", "", "", "", "", "", item.get("reason", "")])
        return Response(buf.getvalue().encode("utf-8-sig"), mimetype="text/csv",
                        headers={"Content-Disposition": f'attachment; filename="m3-match-{run_id}.csv"'})

    @app.get("/api/legacy")
    def legacy():
        return csv_page(engine.CSV_FILE)

    @app.get("/api/pre-migration")
    def pre_migration():
        return csv_page(DATA / "migration_archive" / "pre_migration" / "VA_Tracks_Output.csv")

    def csv_page(path):
        limit, offset = paging()
        album_limit, album_offset = album_paging()
        rows, count = [], 0
        groups = {}
        if path.exists():
            with engine.CSV_LOCK, path.open(newline="", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) < 6:
                        continue
                    item = dict(zip(("name", "song_name", "album_name", "date_str", "link", "flag"), row[:6]))
                    key = f"name:{item['album_name']}" if item["album_name"] else f"link:{item['link']}"
                    group = groups.setdefault(key, {"album_key": key, "album_name": item["album_name"],
                                                     "album_id": "", "heat": None, "latest_date": item["date_str"],
                                                     "track_count": 0})
                    group["track_count"] += 1
                    group["latest_date"] = max(group["latest_date"], item["date_str"])
                    if offset <= count < offset + limit:
                        rows.append(item)
                    count += 1
        albums = sorted(groups.values(), key=lambda item: (item["latest_date"], item["album_name"].casefold()), reverse=True)
        total_albums = len(albums)
        albums = albums[album_offset:album_offset + album_limit]
        return jsonify(results=rows, total=count, albums=albums, total_albums=total_albums)

    @app.get("/api/legacy/csv")
    def legacy_csv():
        if not engine.CSV_FILE.exists():
            return csv_response([], "VA_Tracks_Output.csv")
        with engine.CSV_LOCK:
            content = engine.CSV_FILE.read_bytes()
        return Response(content, mimetype="text/csv", headers={"Content-Disposition": 'attachment; filename="VA_Tracks_Output.csv"'})

    @app.get("/api/pre-migration/csv")
    def pre_migration_csv():
        path = DATA / "migration_archive" / "pre_migration" / "VA_Tracks_Output.csv"
        if not path.exists():
            return jsonify(msg="没有迁移前 CSV 快照"), 404
        return send_file(path, mimetype="text/csv", as_attachment=True, download_name="VA_Tracks_Output.csv")

    @app.get("/api/stream")
    def stream():
        client = runtime.events.subscribe()
        def generate():
            try:
                yield "data: " + json.dumps({"type": "resync", "run_id": runtime.current}) + "\n\n"
                while True:
                    try:
                        message = client.get(timeout=20)
                        yield "data: " + json.dumps(message, ensure_ascii=False) + "\n\n"
                    except queue.Empty:
                        yield ": keep-alive\n\n"
            finally:
                runtime.events.unsubscribe(client)
        return Response(generate(), mimetype="text/event-stream", headers={"X-Accel-Buffering": "no"})

    @app.errorhandler(ValueError)
    @app.errorhandler(TypeError)
    def invalid(_):
        return jsonify(msg="请求参数或服务器配置有误"), 400

    @app.errorhandler(500)
    def internal(_):
        return jsonify(msg="服务器错误，请检查服务日志"), 500

    return app


if __name__ == "__main__":
    from waitress import serve
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # Process lock protects SQLite recovery and engine globals across accidental
    # duplicate launches on the same mounted data directory (Linux deployment).
    import fcntl
    process_lock = (DATA / ".server.lock").open("a")
    try:
        fcntl.flock(process_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit("同一数据目录已有服务器进程运行")
    runtime = Runtime()
    import signal
    def terminate(signum, frame):
        runtime.close()
        raise SystemExit(0)
    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
    atexit.register(runtime.close)
    runtime.start_scheduler()
    serve(create_app(runtime), host="0.0.0.0", port=5000, threads=16, channel_timeout=120)
