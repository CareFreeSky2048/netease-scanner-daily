# =============================================================================
#  网易云 VA 合辑扫描器 - Album Candidate Pool 全量版
# =============================================================================
#  目标：只保留“全量扫描”入口；删除 recent100 / 快速粗扫 / 账号测试等实验功能。
#  核心算法：
#    1) 覆盖全部关注艺人，每位艺人只采样 recent N 首（默认 50，网易云艺人页默认视角）
#    2) 全局汇总 album_id 候选池，同一 album 只查一次详情
#    3) 先按 album publishTime 过滤目标日期区间
#    4) 只对日期命中的候选 album 所涉及艺人做 shallow owned-album 验证
#    5) 输出规则固定为二元并集：
#       - 外部合辑/企划：album_id 不在该艺人 owned_ids 中，且发行日期落在“外部合辑日期范围”
#         默认最近一个月。
#       - 新专/新 EP/新曲所在专辑：发行日期落在“新专/新歌日期范围”，默认昨天到未来 3 天，
#         不管是否属于该艺人 owned_ids 都输出。
#       两套日期窗口彼此独立，不再用一个 start/end 同时控制两个目标。
#
#  保留功能：
#    - 支持单大号模式 / 大小号模式（默认单大号；大小号用于大号艺人列表 + 小号扫描 Cookie 解耦）
#    - 艺人列表按来源账号缓存
#    - album 日期跨运行缓存
#    - CSV 历史去重，CSV 列结构不变
#    - 暂停 / 继续 / 停止
#    - 前端实时显示并可手动调节后续任务投递并发，默认并发 4
#    - 405 GET/POST fallback，但不做自动升线程
#    - /artist/sublist 分页去重与重复页保护，避免艺人列表虚高
# =============================================================================

import concurrent.futures
import csv
import hashlib
import json
import logging
import os
import queue
import random
import re
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import requests
from slow_retry import RetryBudget, retry_failed
from flask import Flask, Response, jsonify, render_template_string, request

# ---------------------------- 基础路径与配置 ----------------------------

SCRIPT_DIR = Path(os.getenv("NETEASE_DATA_DIR", str(Path(__file__).resolve().parent / "data"))).resolve()
SCRIPT_DIR.mkdir(parents=True, exist_ok=True)
CSV_FILE = SCRIPT_DIR / "VA_Tracks_Output.csv"
ARTIST_CACHE_DIR = SCRIPT_DIR / "VA_Artist_Caches"
ALBUM_CACHE_FILE = SCRIPT_DIR / "VA_Album_Date_Cache.json"
METRICS_FILE = SCRIPT_DIR / "VA_Scan_Metrics.json"
REPORT_FILE = SCRIPT_DIR / "VA_Scan_Tuning_Report.txt"
API_ERR_LOG = SCRIPT_DIR / "netease_api.err.log"
API_OUT_LOG = SCRIPT_DIR / "netease_api.out.log"

ARTIST_CACHE_DIR.mkdir(exist_ok=True)

API_BASE = os.getenv("NETEASE_API_BASE", "http://127.0.0.1:3000").rstrip("/")
FLASK_HOST = "127.0.0.1"
FLASK_PORT = int(os.getenv("NETEASE_UI_PORT", "5000"))

DEFAULT_RECENT_LIMIT = int(os.getenv("NETEASE_RECENT_LIMIT", "50"))
DEFAULT_OWNED_VERIFY_LIMIT = int(os.getenv("NETEASE_OWNED_VERIFY_LIMIT", "50"))
DEFAULT_NEW_ALBUM_PROBE_LIMIT = int(os.getenv("NETEASE_NEW_ALBUM_PROBE_LIMIT", "20"))
ALBUM_CACHE_RECENT_RECHECK_HOURS = float(os.getenv("NETEASE_ALBUM_CACHE_RECENT_RECHECK_HOURS", "12"))
ALBUM_CACHE_UNKNOWN_RECHECK_HOURS = float(os.getenv("NETEASE_ALBUM_CACHE_UNKNOWN_RECHECK_HOURS", "6"))
DEFAULT_THREADS = int(os.getenv("NETEASE_SCANNER_THREADS", "5"))
MANUAL_THREAD_MAX = int(os.getenv("NETEASE_MANUAL_THREAD_MAX", "16"))
API_TIMEOUT = float(os.getenv("NETEASE_API_TIMEOUT", "8"))
API_RETRIES = int(os.getenv("NETEASE_API_RETRIES", "1"))
ALBUM_MISMATCH_RETRIES = int(os.getenv("NETEASE_ALBUM_MISMATCH_RETRIES", "3"))
ALLOW_ALBUM_FALLBACK_FROM_RECENT = os.getenv("NETEASE_ALLOW_ALBUM_FALLBACK_FROM_RECENT", "1") != "0"
# User-confirmed nonexistent upstream albums never enter detail/retry queues.
SKIP_ALBUM_DETAIL_IDS = frozenset({"141201113"})
ARTIST_SUBLIST_SUSPECT_CAP = max(1000, int(os.getenv("NETEASE_ARTIST_SUBLIST_SUSPECT_CAP", "4500")))
SUBLIST_LIMIT = max(20, min(int(os.getenv("NETEASE_SUBLIST_LIMIT", "500")), 500))
SUBLIST_MAX_PAGES = max(1, int(os.getenv("NETEASE_SUBLIST_MAX_PAGES", "100")))
SUBLIST_MAX_ARTISTS = max(SUBLIST_LIMIT, int(os.getenv("NETEASE_SUBLIST_MAX_ARTISTS", "20000")))
ARTIST_CACHE_TTL_HOURS = float(os.getenv("NETEASE_ARTIST_CACHE_TTL_HOURS", "720"))
REFRESH_ARTIST_CACHE = os.getenv("NETEASE_REFRESH_ARTIST_CACHE", "0") == "1"
HIDE_EXISTING_CSV_HITS = os.getenv("NETEASE_HIDE_EXISTING_CSV_HITS", "1") != "0"
AUTO_START_API = os.getenv("NETEASE_AUTO_START_API", "1") != "0"

CN_TZ = timezone(timedelta(hours=8))

# ---------------------------- Flask / SSE ----------------------------

app = Flask(__name__)
message_queue: "queue.Queue[Dict[str, Any]]" = queue.Queue()
SERVER_SCAN_LOCK = threading.Lock()
is_server_scanning = False

# ---------------------------- 控制状态 ----------------------------

@dataclass
class ControlState:
    target_threads: int = max(1, min(DEFAULT_THREADS, MANUAL_THREAD_MAX))
    baseline_threads: int = max(1, min(DEFAULT_THREADS, MANUAL_THREAD_MAX))
    manual_thread_max: int = MANUAL_THREAD_MAX
    paused: bool = False
    stop_requested: bool = False
    reason: str = "initial"
    lock: threading.Lock = field(default_factory=threading.Lock)
    pause_cond: threading.Condition = field(default_factory=lambda: threading.Condition(threading.Lock()))

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "target_threads": self.target_threads,
                "baseline_threads": self.baseline_threads,
                "manual_thread_max": self.manual_thread_max,
                "paused": self.paused,
                "stop_requested": self.stop_requested,
                "reason": self.reason,
            }

    def set_threads_manual(self, value: int) -> int:
        value = int(value)
        value = max(1, min(value, self.manual_thread_max))
        with self.lock:
            self.target_threads = value
            self.reason = f"manual:{value}"
        self.resume()
        return value

    def reduce_threads(self, value: int, reason: str) -> int:
        value = max(1, min(int(value), self.manual_thread_max))
        with self.lock:
            if value < self.target_threads:
                self.target_threads = value
                self.reason = reason
            return self.target_threads

    def pause(self) -> None:
        with self.lock:
            self.paused = True
            self.reason = "paused"

    def resume(self) -> None:
        with self.lock:
            self.paused = False
            self.reason = "resumed/manual"
        with self.pause_cond:
            self.pause_cond.notify_all()

    def stop(self) -> None:
        with self.lock:
            self.stop_requested = True
            self.paused = False
            self.reason = "stop_requested"
        with self.pause_cond:
            self.pause_cond.notify_all()

CONTROL = ControlState()

# ---------------------------- 度量 ----------------------------

class Metrics:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.started_at = time.time()
        self.ended_at: Optional[float] = None
        self.phase = "idle"
        self.phase_started_at = self.started_at
        self.phase_done = 0
        self.phase_total = 0
        self.endpoint: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "count": 0, "ok": 0, "status_405": 0, "status_460": 0,
            "timeout": 0, "conn_error": 0, "error": 0, "durations": []
        })
        self.artist_done = 0
        self.artist_total = 0
        # Optional offsets let the server run several artist groups as one
        # durable scan while keeping the recent-sampling progress cumulative.
        self.artist_progress_offset = 0
        self.artist_progress_total: Optional[int] = None
        self.artist_failed = 0
        self.album_done = 0
        self.album_total = 0
        self.verify_done = 0
        self.verify_total = 0
        self.results = 0
        self.csv_skipped = 0
        self.album_cache_hit = 0
        self.album_cache_miss = 0
        self.album_cache_recheck = 0
        self.album_cache_corrupt = 0
        self.album_id_mismatch = 0
        self.album_name_mismatch = 0
        self.album_detail_fallback_from_recent = 0
        self.album_fallback_no_date = 0
        self.new_album_probe_failed = 0
        self.album_name_disagreement = 0
        self.slow_artists: List[Dict[str, Any]] = []
        self.events: deque = deque(maxlen=200)
        self.last_autosave = 0.0

    def set_phase(self, phase: str) -> None:
        with self.lock:
            self.phase = phase
            self.phase_started_at = time.time()
            self.phase_done = 0
            self.phase_total = 0
            self.events.append({"ts": time.time(), "phase": phase})

    def set_phase_progress(self, done: int, total: int) -> None:
        with self.lock:
            self.phase_done = max(0, int(done))
            self.phase_total = max(0, int(total))

    def record_endpoint(self, path: str, duration: float, ok: bool, error_kind: Optional[str] = None, status_code: Optional[int] = None) -> None:
        with self.lock:
            rec = self.endpoint[path]
            rec["count"] += 1
            rec["durations"].append(duration)
            if ok:
                rec["ok"] += 1
            else:
                if status_code == 405:
                    rec["status_405"] += 1
                elif status_code == 460:
                    rec["status_460"] += 1
                elif error_kind in ("timeout", "conn_error", "error"):
                    rec[error_kind] += 1
                else:
                    rec["error"] += 1

    def inc(self, name: str, n: int = 1) -> None:
        with self.lock:
            if hasattr(self, name):
                setattr(self, name, getattr(self, name) + n)

    def add_slow_artist(self, artist_name: str, duration: float, details: Dict[str, Any]) -> None:
        with self.lock:
            self.slow_artists.append({"artist": artist_name, "duration": duration, **details})
            self.slow_artists.sort(key=lambda x: x.get("duration", 0), reverse=True)
            self.slow_artists = self.slow_artists[:50]

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            now = time.time()
            elapsed = max(0.001, (self.ended_at or now) - self.started_at)
            endpoints: Dict[str, Any] = {}
            for path, rec in self.endpoint.items():
                durs = sorted(rec.get("durations", []))
                p95 = durs[int(len(durs) * 0.95) - 1] if durs else 0
                avg = sum(durs) / len(durs) if durs else 0
                endpoints[path] = {
                    k: v for k, v in rec.items() if k != "durations"
                }
                endpoints[path].update({
                    "avg_ms": round(avg * 1000, 1),
                    "p95_ms": round(p95 * 1000, 1),
                    "max_ms": round((max(durs) if durs else 0) * 1000, 1),
                })
            artists_per_min = round(self.artist_done / elapsed * 60, 2)
            remaining = max(0, self.artist_total - self.artist_done)
            eta_min = round(remaining / artists_per_min, 1) if artists_per_min > 0 else None
            phase_elapsed = max(0.001, now - self.phase_started_at)
            phase_per_min = round(self.phase_done / phase_elapsed * 60, 2)
            phase_remaining = max(0, self.phase_total - self.phase_done)
            phase_eta_min = round(phase_remaining / phase_per_min, 1) if phase_per_min > 0 and self.phase_total > 0 else None
            phase_display = {
                "idle": "等待",
                "resolve_artist_list": "解析艺人列表",
                "sample_recent_songs": "采样 recent 曲目",
                "new_album_probe": "兜底扫描新专目录",
                "album_detail_pool": "查询 album 日期候选池",
                "owned_verify": "验证 album 归属",
                "emit_results": "比对筛选结果",
                "slow_retry": "失败对象低速补扫（单线程）",
            }.get(self.phase, self.phase)
            phase_unit = {
                "sample_recent_songs": "位",
                "new_album_probe": "位",
                "album_detail_pool": "album",
                "owned_verify": "位",
                "emit_results": "条",
            }.get(self.phase, "项")
            return {
                "started_at": self.started_at,
                "ended_at": self.ended_at,
                "elapsed_sec": round(elapsed, 2),
                "phase": self.phase,
                "phase_display": phase_display,
                "phase_unit": phase_unit,
                "phase_done": self.phase_done,
                "phase_total": self.phase_total,
                "phase_elapsed_sec": round(phase_elapsed, 2),
                "phase_per_min": phase_per_min,
                "phase_eta_min": phase_eta_min,
                "artist_done": self.artist_done,
                "artist_total": self.artist_total,
                "artist_failed": self.artist_failed,
                "album_done": self.album_done,
                "album_total": self.album_total,
                "verify_done": self.verify_done,
                "verify_total": self.verify_total,
                "results": self.results,
                "csv_skipped": self.csv_skipped,
                "album_cache_hit": self.album_cache_hit,
                "album_cache_miss": self.album_cache_miss,
                "album_cache_recheck": self.album_cache_recheck,
                "album_cache_corrupt": self.album_cache_corrupt,
                "album_id_mismatch": self.album_id_mismatch,
                "album_name_mismatch": self.album_name_mismatch,
                "album_detail_fallback_from_recent": self.album_detail_fallback_from_recent,
                "album_fallback_no_date": self.album_fallback_no_date,
                "new_album_probe_failed": self.new_album_probe_failed,
                "album_name_disagreement": self.album_name_disagreement,
                "artists_per_min": artists_per_min,
                "eta_min": eta_min,
                "endpoints": endpoints,
                "slow_artists": list(self.slow_artists[:20]),
                "control": CONTROL.snapshot(),
                "events": list(self.events),
            }

    def finish(self) -> None:
        with self.lock:
            self.ended_at = time.time()

METRICS = Metrics()

# ---------------------------- 工具函数 ----------------------------

def emit(msg: Dict[str, Any]) -> None:
    message_queue.put(msg)


def redacted_error_text(text: str) -> str:
    # 避免 Cookie 或代理凭据明文进入前端日志或报告
    text = re.sub(r"MUSIC_U%3D[^&\s]+", "MUSIC_U=<redacted>", text)
    text = re.sub(r"MUSIC_U=[^;\s]+;?", "MUSIC_U=<redacted>;", text)
    text = re.sub(r"(https?|socks5h?)://[^/\s@]+@", r"\1://<redacted>@", text, flags=re.IGNORECASE)
    text = re.sub(r"proxy=[^&\s]+", "proxy=<redacted>", text, flags=re.IGNORECASE)
    return text


def normalize_cookie(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    # 允许粘贴纯 MUSIC_U 值、MUSIC_U=xxx、或完整 Cookie
    if "MUSIC_U=" in raw:
        # 如果是完整 Cookie，只保留 MUSIC_U，减少日志/请求体体积
        m = re.search(r"MUSIC_U=([^;\s]+)", raw)
        if m:
            return f"MUSIC_U={m.group(1)};"
        return raw if raw.endswith(";") else raw + ";"
    return f"MUSIC_U={raw};"


def cookie_fingerprint(cookie: str) -> str:
    return hashlib.sha256(cookie.encode("utf-8")).hexdigest()[:16]


def looks_like_cache_key(value: str) -> bool:
    return bool(re.fullmatch(r"[0-9a-fA-F]{16}", (value or "").strip()))


def parse_cn_date_from_ms(ts: Optional[int]) -> Tuple[str, Optional[datetime]]:
    if not ts:
        return "未知日期", None
    try:
        dt = datetime.fromtimestamp(int(ts) / 1000, tz=CN_TZ).replace(tzinfo=None)
        return dt.strftime("%Y-%m-%d"), dt
    except Exception:
        return "未知日期", None


def safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def parse_debug_album_id(value: Any) -> str:
    """从 album URL / 纯数字中提取用于漏扫诊断的 album_id。为空则不诊断。"""
    text = str(value or "").strip()
    if not text:
        return ""
    patterns = [r"[?&#]id=(\d+)", r"/album(?:\?id=|/)(\d+)", r"^(\d+)$"]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            return m.group(1)
    m = re.search(r"(\d{4,})", text)
    return m.group(1) if m else ""


def wait_if_paused_or_stopped() -> bool:
    while True:
        snap = CONTROL.snapshot()
        if snap["stop_requested"]:
            return False
        if not snap["paused"]:
            return True
        with CONTROL.pause_cond:
            CONTROL.pause_cond.wait(timeout=0.5)

# ---------------------------- CSV / 缓存 ----------------------------

CSV_LOCK = threading.Lock()
SEEN_LINKS: set = set()
SEEN_SONG_IDS: set = set()
# Links emitted during the current logical scan are kept separate from the
# historical CSV set.  A multi-round scan can therefore share the same CSV
# de-duplication state without re-reading or appending the same song twice.
CURRENT_SCAN_LINKS: set = set()
SEEN_LOCK = threading.Lock()

ALBUM_CACHE_LOCK = threading.Lock()
ALBUM_CACHE: Dict[str, Dict[str, Any]] = {}
ARTIST_ALBUM_CACHE_LOCK = threading.Lock()
ARTIST_ALBUM_PAGE_CACHE: Dict[Tuple[str, str], Dict[str, Any]] = {}


def init_csv_and_seen(reset_current_scan: bool = True) -> None:
    with CSV_LOCK:
        if not CSV_FILE.exists():
            with CSV_FILE.open("w", newline="", encoding="utf-8-sig") as f:
                csv.writer(f).writerow([
                    "艺人 (Artist)", "单曲名 (Track)", "企划/专辑名 (Album/VA)",
                    "发行日期 (Release Date)", "直达链接", "新歌标记 (Flag)"
                ])
        with SEEN_LOCK:
            SEEN_LINKS.clear()
            SEEN_SONG_IDS.clear()
            if reset_current_scan:
                CURRENT_SCAN_LINKS.clear()
            with CSV_FILE.open("r", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) >= 5:
                        link = row[4]
                        SEEN_LINKS.add(link)
                        m = re.search(r"id=(\d+)", link or "")
                        if m:
                            SEEN_SONG_IDS.add(m.group(1))


def write_hit_if_new(hit: Dict[str, Any]) -> bool:
    link = hit.get("link", "")
    with SEEN_LOCK:
        if link in SEEN_LINKS:
            METRICS.inc("csv_skipped")
            return False
        SEEN_LINKS.add(link)
        m = re.search(r"id=(\d+)", link or "")
        if m:
            SEEN_SONG_IDS.add(m.group(1))
    with CSV_LOCK:
        with CSV_FILE.open("a", newline="", encoding="utf-8-sig") as f:
            csv.writer(f).writerow([
                hit.get("name", ""), hit.get("song_name", ""), hit.get("album_name", ""),
                hit.get("date_str", ""), hit.get("link", ""), hit.get("flag", 0)
            ])
    return True


def is_valid_album_cache_entry(album_id: str, entry: Any) -> bool:
    """校验 album cache，防止历史版本/异常写入造成 album_id -> album_name 错配。"""
    if not isinstance(entry, dict):
        return False
    expected = str(album_id)
    actual = entry.get("album_id") or entry.get("id")
    if actual is not None and str(actual) != expected:
        return False
    # 老缓存可能没有 album_id，但 key 本身可信；至少要求有日期或名字，避免空壳污染。
    if not entry.get("album_name") and not entry.get("date_str") and not entry.get("publish_ts"):
        return False
    return True


def load_album_cache() -> None:
    global ALBUM_CACHE
    if not ALBUM_CACHE_FILE.exists():
        ALBUM_CACHE = {}
        return
    try:
        with ALBUM_CACHE_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            cleaned = {}
            dropped = 0
            for k, v in data.items():
                key = str(k)
                if is_valid_album_cache_entry(key, v):
                    if isinstance(v, dict):
                        v = dict(v)
                        v["album_id"] = key
                    cleaned[key] = v
                else:
                    dropped += 1
            ALBUM_CACHE = cleaned
            if dropped:
                emit({"type": "log", "level": "error", "text": f"album 缓存清理：丢弃 {dropped} 条 album_id 不匹配或空壳记录。"})
        else:
            ALBUM_CACHE = {}
    except Exception:
        ALBUM_CACHE = {}


def save_album_cache() -> None:
    tmp = ALBUM_CACHE_FILE.with_suffix(".json.tmp")
    with ALBUM_CACHE_LOCK:
        data = dict(ALBUM_CACHE)
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    tmp.replace(ALBUM_CACHE_FILE)


def artist_cache_path(key: str) -> Path:
    return ARTIST_CACHE_DIR / f"artists_{key}.json"


def normalize_artist_list(raw_artists: Any) -> List[Dict[str, Any]]:
    """清洗艺人列表：按 id 去重，避免重复分页/旧缓存污染后续扫描。"""
    unique: List[Dict[str, Any]] = []
    seen = set()
    if not isinstance(raw_artists, list):
        return unique
    for a in raw_artists:
        if not isinstance(a, dict):
            continue
        artist_id = a.get("id")
        name = a.get("name")
        if artist_id is None or not name:
            continue
        sid = str(artist_id)
        if sid in seen:
            continue
        unique.append({"id": sid, "name": str(name)})
        seen.add(sid)
    return unique


def load_artist_cache_by_key(key: str) -> Optional[List[Dict[str, Any]]]:
    path = artist_cache_path(key)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        artists = payload.get("artists") if isinstance(payload, dict) else None
        unique = normalize_artist_list(artists)
        if unique:
            saved_count = payload.get("count") if isinstance(payload, dict) else None
            if saved_count and int(saved_count) != len(unique):
                emit({"type": "log", "level": "info", "text": f"艺人缓存 {key} 已去重：{saved_count} → {len(unique)} 位。"})
            return unique
    except Exception:
        return None
    return None


def load_artist_cache_meta(key: str) -> Dict[str, Any]:
    path = artist_cache_path(key)
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        meta = payload.get("meta") if isinstance(payload, dict) else None
        return meta if isinstance(meta, dict) else {}
    except Exception:
        return {}


def save_artist_cache(key: str, artists: List[Dict[str, Any]], meta: Dict[str, Any]) -> None:
    path = artist_cache_path(key)
    unique = normalize_artist_list(artists)
    payload = {"saved_at": time.time(), "count": len(unique), "meta": meta, "artists": unique}
    tmp = path.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def is_artist_cache_fresh(key: str) -> bool:
    path = artist_cache_path(key)
    if not path.exists():
        return False
    age_hours = (time.time() - path.stat().st_mtime) / 3600
    return age_hours <= ARTIST_CACHE_TTL_HOURS

# ---------------------------- API 请求 ----------------------------

_thread_local = threading.local()
METHOD_CACHE_LOCK = threading.Lock()
METHOD_CACHE: Dict[str, str] = {
    "/artist/sublist": "POST",
    "/artist/songs": "GET",
    "/artist/album": "GET",
    "/album": "GET",
}
_CHINA_IP_PREFIXES = (
    "116.25", "116.76", "116.77", "116.78", "116.79", "116.80",
    "116.81", "116.82", "116.83", "116.84", "116.85", "116.86",
    "116.87", "116.88", "116.89", "116.90", "116.91", "116.92",
    "116.93", "116.94",
)
_AUTO_REAL_IP: Optional[str] = None
_TRANSPORT_LOCK = threading.Lock()


def api_transport_overrides() -> Dict[str, str]:
    """Read optional NeteaseCloudMusicApi transport overrides from environment variables."""
    global _AUTO_REAL_IP
    overrides: Dict[str, str] = {}
    real_ip = os.getenv("NETEASE_REAL_IP", "").strip()
    if real_ip and real_ip.lower() not in {"none", "off"}:
        if real_ip.lower() == "auto":
            with _TRANSPORT_LOCK:
                if _AUTO_REAL_IP is None:
                    prefix = random.choice(_CHINA_IP_PREFIXES)
                    _AUTO_REAL_IP = f"{prefix}.{random.randint(1, 254)}.{random.randint(1, 254)}"
                overrides["realIP"] = _AUTO_REAL_IP
        else:
            overrides["realIP"] = real_ip
    crypto = os.getenv("NETEASE_API_CRYPTO", "").strip()
    if crypto and crypto.lower() not in {"none", "off"}:
        overrides["crypto"] = crypto
    proxy = os.getenv("NETEASE_API_PROXY", "").strip()
    if proxy and proxy.lower() not in {"none", "off"}:
        overrides["proxy"] = proxy
    return overrides


def get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        sess = requests.Session()
        sess.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"
        })
        _thread_local.session = sess
    return _thread_local.session


class ApiError(Exception):
    def __init__(self, path: str, message: str, status_code: Optional[int] = None):
        super().__init__(f"API 请求失败 {path}: {message}")
        self.path = path
        self.status_code = status_code


class AlbumIdMismatchError(ApiError):
    """专辑详情接口返回了与请求不同的 album_id。"""


class AlbumNameMismatchError(ApiError):
    """专辑详情接口返回的专辑名与采样关系明显冲突。"""


def api_request(path: str, params: Optional[Dict[str, Any]] = None, preferred: Optional[str] = None, timeout: Optional[float] = None, exact_method: bool = False) -> Dict[str, Any]:
    if not wait_if_paused_or_stopped():
        raise ApiError(path, "scan stopped")
    params = dict(params or {})
    params.setdefault("timestamp", int(time.time() * 1000))
    for k, v in api_transport_overrides().items():
        params.setdefault(k, v)
    timeout = timeout or API_TIMEOUT
    sess = get_session()
    with METHOD_CACHE_LOCK:
        method = preferred or METHOD_CACHE.get(path, "GET")
    methods = [method] if exact_method else [method] + (["POST" if method == "GET" else "GET"] if path in ("/artist/sublist", "/artist/songs", "/artist/album", "/album") else [])
    last_err: Optional[Exception] = None

    for attempt in range(API_RETRIES + 1):
        for m in methods:
            if not wait_if_paused_or_stopped():
                raise ApiError(path, "scan stopped")
            start = time.time()
            try:
                url = API_BASE + path
                if m == "POST":
                    res = sess.post(url, data=params, timeout=timeout)
                else:
                    res = sess.get(url, params=params, timeout=timeout)
                duration = time.time() - start
                if res.status_code == 405:
                    METRICS.record_endpoint(path, duration, ok=False, status_code=405)
                    detail = "405 Method Not Allowed"
                    try:
                        err_body = res.json()
                        err_msg = err_body.get("message") or err_body.get("msg")
                        if err_msg:
                            detail = f"405: {redacted_error_text(str(err_msg))}"
                    except Exception:
                        pass
                    last_err = ApiError(path, detail, 405)
                    continue
                res.raise_for_status()
                data = res.json()
                code = data.get("code")
                if code == 405:
                    METRICS.record_endpoint(path, duration, ok=False, status_code=405)
                    err_msg = data.get("message") or data.get("msg")
                    detail = f"API code 405: {redacted_error_text(str(err_msg))}" if err_msg else "API code 405"
                    last_err = ApiError(path, detail, 405)
                    continue
                if code == 460:
                    METRICS.record_endpoint(path, duration, ok=False, status_code=460)
                    raise ApiError(path, "网易云风控 460", 460)
                # 某些接口 code 不一定为 200，但这里仅严格处理常用接口
                METRICS.record_endpoint(path, duration, ok=True)
                with METHOD_CACHE_LOCK:
                    METHOD_CACHE[path] = m
                return data
            except requests.Timeout as e:
                METRICS.record_endpoint(path, time.time() - start, ok=False, error_kind="timeout")
                last_err = e
            except requests.ConnectionError as e:
                METRICS.record_endpoint(path, time.time() - start, ok=False, error_kind="conn_error")
                last_err = e
            except ApiError:
                raise
            except Exception as e:
                METRICS.record_endpoint(path, time.time() - start, ok=False, error_kind="error")
                last_err = e
        if attempt < API_RETRIES:
            time.sleep(0.8 + attempt * 0.8 + random.random() * 0.5)
    # 保留 405 等结构化信息，让上层可以选择性地切换方法，避免每次
    # album 请求都重复走一轮必然失败的 GET。
    if isinstance(last_err, ApiError):
        raise last_err
    raise ApiError(path, redacted_error_text(str(last_err)))

# ---------------------------- 网易云数据获取 ----------------------------

def fetch_artist_sublist(source_cookie: str) -> List[Dict[str, Any]]:
    """拉取关注艺人列表，带可验证分页。

    修复重点：不要把“第一页 500 条”误认为完整列表。某些本地 API
    组合下，POST 会返回 200 但 offset 没生效，导致 offset=500 仍然
    返回第一页；上一版的重复页保护会在这种情况下停在 500。

    现在采用多计划拉取：
      1) 优先 GET + limit=500，确保 offset 位于 URL query，最容易验证分页。
      2) GET 遇到 405 或不可用时，切 POST。
      3) 如果 500 分页疑似只返回一页或重复页，再降到 100 重试。
    返回使用“新增 artist_id 最多且没有明显分页异常”的结果。
    """

    def page_signature(page: List[Dict[str, Any]]) -> Tuple[str, ...]:
        # 只取前 20 个 id 做重复页签名即可；完整重复用 new_count 兜底。
        sig = []
        for a in page[:20]:
            if isinstance(a, dict) and a.get("id"):
                sig.append(str(a["id"]))
        return tuple(sig)

    def fetch_with_plan(method: str, page_limit: int) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        artists: List[Dict[str, Any]] = []
        seen = set()
        seen_pages = set()
        offset = 0
        page_no = 0
        server_count: Optional[int] = None
        end_reason = "unknown"
        had_repeat = False
        had_error = False
        last_error = ""

        while True:
            if CONTROL.snapshot()["stop_requested"]:
                end_reason = "stopped"
                break
            if page_no >= SUBLIST_MAX_PAGES:
                end_reason = f"max_pages_{SUBLIST_MAX_PAGES}"
                break
            if len(artists) >= SUBLIST_MAX_ARTISTS:
                end_reason = f"max_artists_{SUBLIST_MAX_ARTISTS}"
                break

            try:
                data = api_request("/artist/sublist", {
                    "limit": page_limit,
                    "offset": offset,
                    "cookie": source_cookie,
                }, preferred=method, exact_method=True)
            except Exception as e:
                had_error = True
                last_error = redacted_error_text(str(e))
                end_reason = f"api_error_{method}_{page_limit}: {last_error}"
                break

            if data.get("code") != 200:
                had_error = True
                last_error = f"返回码 {data.get('code')}"
                end_reason = f"bad_code_{data.get('code')}"
                break

            if server_count is None:
                for k in ("count", "total", "size"):
                    v = data.get(k)
                    if isinstance(v, int) and v > 0:
                        server_count = v
                        break

            page = data.get("data") or []
            if not isinstance(page, list) or not page:
                end_reason = f"empty_page_offset_{offset}"
                break

            sig = page_signature(page)
            before = len(artists)
            for a in page:
                if not isinstance(a, dict):
                    continue
                if a.get("id") and a.get("name"):
                    sid = str(a["id"])
                    if sid not in seen:
                        artists.append({"id": sid, "name": str(a["name"])})
                        seen.add(sid)

            new_count = len(artists) - before
            total_hint = f" / 服务器count≈{server_count}" if server_count else ""
            emit({"type": "log", "level": "info", "text": f"艺人列表[{method}, limit={page_limit}] offset={offset}：本页 {len(page)} 条，新增 {new_count} 位，累计 {len(artists)} 位{total_hint}。"})

            if sig and sig in seen_pages:
                had_repeat = True
                end_reason = f"repeat_page_signature_offset_{offset}"
                break
            if sig:
                seen_pages.add(sig)
            if new_count == 0:
                had_repeat = True
                end_reason = f"no_new_ids_offset_{offset}"
                break

            # 如果服务器明确给了总数，以总数为准。
            if server_count and len(artists) >= server_count:
                end_reason = "server_count_reached"
                break

            # hasMore/more 有时缺失；只有明确 False 才停。
            if data.get("hasMore") is False or data.get("more") is False:
                end_reason = "server_has_more_false"
                break

            # 页不足说明正常结束。
            if len(page) < page_limit:
                end_reason = "short_page"
                break

            offset += page_limit
            page_no += 1
            time.sleep(0.18 if page_limit <= 100 else 0.25)

        meta = {
            "method": method,
            "limit": page_limit,
            "count": len(artists),
            "server_count": server_count,
            "end_reason": end_reason,
            "had_repeat": had_repeat,
            "had_error": had_error,
            "last_error": last_error,
            "suspicious_single_full_page": (len(artists) == page_limit and page_no == 0 and end_reason == "server_has_more_false"),
            "suspicious_cap_boundary": (len(artists) >= ARTIST_SUBLIST_SUSPECT_CAP and not server_count),
            # 空页本身只说明本次分页到头；当数量恰好撞上可疑边界时，必须把
            # “已读完接口返回”与“已确认账号全量”区分开，避免误报为完整列表。
            "complete_confirmed": bool(
                (server_count and len(artists) >= server_count)
                or end_reason in ("short_page", "server_has_more_false") and len(artists) < ARTIST_SUBLIST_SUSPECT_CAP
            ),
        }
        return artists, meta

    def probe_missing_tail(base_artists: List[Dict[str, Any]], base_meta: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """补探大页边界遗漏的尾页。

        网易云某些代理/版本在 limit=500 且 offset=4500 时会错误返回空页，
        但同一 offset 用较小 limit 仍能返回尾部记录。只在服务器总数明确大于
        当前数量时触发，最多尝试少量请求，以控制速度与风控成本。
        """
        server_count = base_meta.get("server_count")
        if not isinstance(server_count, int) or server_count <= len(base_artists):
            return base_artists, base_meta
        if not str(base_meta.get("end_reason", "")).startswith("empty_page_offset_"):
            return base_artists, base_meta
        try:
            boundary_offset = int(str(base_meta["end_reason"]).rsplit("_", 1)[-1])
        except Exception:
            return base_artists, base_meta

        merged = list(base_artists)
        seen = {str(a.get("id")) for a in merged if isinstance(a, dict) and a.get("id")}
        deficit = max(1, server_count - len(merged))
        limits = []
        for value in (deficit, 100, 200, 50, 39):
            value = max(1, min(200, int(value)))
            if value not in limits:
                limits.append(value)
        attempts = []
        for tail_limit in limits:
            try:
                data = api_request("/artist/sublist", {
                    "limit": tail_limit,
                    "offset": boundary_offset,
                    "cookie": source_cookie,
                }, preferred=method, exact_method=True)
                page = data.get("data") or []
                gained = 0
                for a in page if isinstance(page, list) else []:
                    if not isinstance(a, dict) or not a.get("id") or not a.get("name"):
                        continue
                    sid = str(a["id"])
                    if sid not in seen:
                        merged.append({"id": sid, "name": str(a["name"])})
                        seen.add(sid)
                        gained += 1
                attempts.append({"limit": tail_limit, "returned": len(page) if isinstance(page, list) else 0, "gained": gained})
                emit({"type": "log", "level": "info", "text": f"尾页补偿探测 offset={boundary_offset}, limit={tail_limit}：返回 {len(page) if isinstance(page, list) else 0} 条，新增 {gained} 位，累计 {len(merged)} 位 / 服务器count≈{server_count}。"})
                if len(merged) >= server_count:
                    emit({"type": "log", "level": "success", "text": f"尾页补偿探测已补齐：累计 {len(merged)} 位，达到服务器count≈{server_count}。"})
                    break
                if gained > 0:
                    # 已命中尾部时，后续更小 limit 只会重复同一尾页。
                    break
            except Exception as e:
                attempts.append({"limit": tail_limit, "error": redacted_error_text(str(e))})

        if len(merged) > len(base_artists):
            base_meta = dict(base_meta)
            base_meta.update({
                "count": len(merged),
                "tail_probe": attempts,
                "tail_probe_offset": boundary_offset,
                "complete_confirmed": len(merged) >= server_count,
                "end_reason": "tail_probe_server_count_reached" if len(merged) >= server_count else "tail_probe_partial",
            })
        else:
            base_meta = dict(base_meta)
            base_meta["tail_probe"] = attempts
        return merged, base_meta

    # 默认计划：GET 先行。因为 POST 在某些本地 API 状态下会 200 但 offset 不生效。
    forced = os.getenv("NETEASE_SUBLIST_METHOD", "").strip().upper()
    limits = []
    try:
        env_lim = int(os.getenv("NETEASE_SUBLIST_LIMIT", str(SUBLIST_LIMIT)))
        if env_lim > 0:
            limits.append(env_lim)
    except Exception:
        pass
    for x in (500, 200, 100):
        if x not in limits:
            limits.append(x)

    methods = [forced] if forced in ("GET", "POST") else ["GET", "POST"]
    plans = []
    for lim in limits:
        for m in methods:
            plans.append((m, lim))

    best_artists: List[Dict[str, Any]] = []
    best_meta: Dict[str, Any] = {}
    tried = []

    for method, lim in plans:
        emit({"type": "log", "level": "info", "text": f"开始拉取艺人列表分页计划：{method}, limit={lim}"})
        artists, meta = fetch_with_plan(method, lim)
        if method == "GET" and lim >= 500:
            artists, meta = probe_missing_tail(artists, meta)
        tried.append(meta)
        if len(artists) > len(best_artists):
            best_artists = artists
            best_meta = meta

        # 只拿到完整一页 500/200/100 且服务器说没有更多，这是可疑的；继续试其他计划。
        suspicious = meta.get("suspicious_single_full_page") or meta.get("suspicious_cap_boundary") or (meta.get("had_repeat") and len(artists) <= lim)
        complete_by_server = meta.get("server_count") and len(artists) >= int(meta["server_count"])
        # 多页并不等于完整：服务端若已报告总数 4539，而当前只有 4500，
        # 不能因为分页过程看起来正常就提前缓存。
        known_deficit = bool(meta.get("server_count") and len(artists) < int(meta["server_count"]))
        healthy_multi_page = len(artists) > lim and not meta.get("had_repeat") and not known_deficit
        healthy_short_end = meta.get("end_reason") in ("short_page", "empty_page_offset_0") and len(artists) > 0

        if complete_by_server or healthy_multi_page or (healthy_short_end and meta.get("complete_confirmed")):
            emit({"type": "log", "level": "success", "text": f"艺人列表分页计划可用：{method}, limit={lim}，共 {len(artists)} 位，结束原因：{meta.get('end_reason')}。"})
            fetch_artist_sublist.last_meta = dict(meta)
            return normalize_artist_list(artists)

        if suspicious:
            end_reason = str(meta.get("end_reason") or "")
            if end_reason.startswith("empty_page_offset_") and len(artists) > 0:
                deficit = int(meta["server_count"]) - len(artists) if meta.get("server_count") else 0
                note = f"服务器总数还差约 {deficit} 位，当前结果不能视为完整。" if deficit > 0 else "当前没有可用的服务器总数，完整性未确认。"
                emit({"type": "log", "level": "warning", "text": f"艺人列表到达空页边界：{method}, limit={lim}，已取得 {len(artists)} 位，原因：{end_reason}。{note}"})
            else:
                emit({"type": "log", "level": "error", "text": f"艺人列表分页计划疑似不完整：{method}, limit={lim}，只得到 {len(artists)} 位，原因：{end_reason}；继续尝试其他分页方式。"})
        else:
            emit({"type": "log", "level": "info", "text": f"艺人列表分页计划结束：{method}, limit={lim}，得到 {len(artists)} 位，原因：{meta.get('end_reason')}。"})

    if best_artists:
        deficit = int(best_meta["server_count"]) - len(best_artists) if best_meta.get("server_count") else 0
        boundary_note = f"；服务器总数约 {best_meta['server_count']}，仍缺 {deficit} 位" if deficit > 0 else ("；数量正好达到 4500，无法确认账号是否还有更多关注艺人" if len(best_artists) == ARTIST_SUBLIST_SUSPECT_CAP else "")
        emit({"type": "log", "level": "warning", "text": f"所有分页计划均未确认完整，采用数量最多的一组：{best_meta.get('method')}, limit={best_meta.get('limit')}，共 {len(best_artists)} 位{boundary_note}。这不是本地缓存容量限制；若账号实际关注数更大，需要导入外部艺人列表或使用可返回总数的接口。"})
        # 供 resolve_artist_list() 写入缓存；调用方仍会使用这组结果继续本次扫描。
        fetch_artist_sublist.last_meta = dict(best_meta)
        return normalize_artist_list(best_artists)

    raise RuntimeError(f"无法拉取艺人列表；尝试计划：{tried}")


def resolve_artist_list(scan_cookie: str, source_input: str) -> Tuple[List[Dict[str, Any]], str, str]:
    source_input = (source_input or "").strip()
    if looks_like_cache_key(source_input):
        key = source_input.lower()
        cached = load_artist_cache_by_key(key)
        if cached is None:
            raise RuntimeError(f"找不到艺人列表缓存：{key}")
        return cached, key, "cache-key"

    source_cookie = normalize_cookie(source_input) if source_input else scan_cookie
    key = cookie_fingerprint(source_cookie)

    cache_meta = load_artist_cache_meta(key)
    cache_complete = cache_meta.get("complete_confirmed") is not False
    if not REFRESH_ARTIST_CACHE and is_artist_cache_fresh(key) and cache_complete:
        cached = load_artist_cache_by_key(key)
        if cached:
            emit({"type": "log", "level": "success", "text": f"已读取艺人缓存 {key}，共 {len(cached)} 位。"})
            return cached, key, "cache"

    try:
        artists = fetch_artist_sublist(source_cookie)
        # 将最后一次分页计划的完整性写入缓存，避免把接口边界造成的
        # 4500 条结果在下次运行中当成已确认全量而永久复用。
        save_meta = dict(getattr(fetch_artist_sublist, "last_meta", {}) or {})
        save_meta.update({"source": "live-sublist", "count": len(artists)})
        save_artist_cache(key, artists, save_meta)
        emit({"type": "log", "level": "success", "text": f"艺人列表已缓存为 {key}，共 {len(artists)} 位。"})
        return artists, key, "live"
    except Exception as e:
        cached = load_artist_cache_by_key(key)
        if cached:
            emit({"type": "log", "level": "error", "text": f"实时拉取艺人列表失败，回退缓存 {key}：{redacted_error_text(str(e))}"})
            return cached, key, "fallback-cache"
        raise


def fetch_recent_songs_for_artist(artist: Dict[str, str], cookie: str, recent_limit: int) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    t0 = time.time()
    artist_id = artist["id"]
    artist_name = artist["name"]
    try:
        data = api_request("/artist/songs", {
            "id": artist_id,
            "limit": recent_limit,
            "offset": 0,
            "order": "time",
            "cookie": cookie,
        }, preferred="GET")
        if data.get("code") != 200:
            raise ApiError("/artist/songs", f"返回码 {data.get('code')}")
        songs = data.get("songs") or []
        relations: List[Dict[str, Any]] = []
        skipped_seen = 0
        for song in songs:
            song_id = str(song.get("id") or "")
            if not song_id:
                continue
            link = f"https://music.163.com/#/song?id={song_id}"
            # 不在采样阶段按 CSV 跳过：否则历史记录会影响 album 候选池，造成隔天扫描的隐性遗漏。
            # 去重只在最终输出阶段执行。
            al = song.get("al") or {}
            album_id = al.get("id")
            if not album_id:
                continue
            ars = song.get("ar") or []
            relations.append({
                "artist_id": artist_id,
                "artist_name": artist_name,
                "song_id": song_id,
                "song_name": song.get("name") or "",
                "album_id": str(album_id),
                "album_name": al.get("name") or "",
                "song_publish_ts": song.get("publishTime") or song.get("publish_time") or 0,
                "song_publish_time": song.get("publishTime") or song.get("publish_time") or 0,
                "song_artist_ids": [str(x.get("id")) for x in ars if x.get("id")],
                "song_artist_names": [x.get("name") for x in ars if x.get("name")],
                "link": link,
            })
        dur = time.time() - t0
        if dur > 2.0:
            METRICS.add_slow_artist(artist_name, dur, {"stage": "recent_songs", "relations": len(relations), "skipped_seen": skipped_seen})
        return relations, {"ok": True, "duration": dur, "skipped_seen": skipped_seen}
    except Exception as e:
        dur = time.time() - t0
        METRICS.inc("artist_failed")
        return [], {"ok": False, "duration": dur, "error": redacted_error_text(str(e))}


def extract_album_artists(album_obj: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    ids: List[str] = []
    names: List[str] = []
    for key in ("artists", "artist"):
        val = album_obj.get(key)
        if isinstance(val, list):
            for a in val:
                if isinstance(a, dict):
                    if a.get("id"):
                        ids.append(str(a.get("id")))
                    if a.get("name"):
                        names.append(a.get("name"))
        elif isinstance(val, dict):
            if val.get("id"):
                ids.append(str(val.get("id")))
            if val.get("name"):
                names.append(val.get("name"))
    # 去重保序
    ids = list(dict.fromkeys(ids))
    names = list(dict.fromkeys(names))
    return ids, names


def compact_album_songs(raw_songs: Any) -> List[Dict[str, Any]]:
    compact: List[Dict[str, Any]] = []
    if not isinstance(raw_songs, list):
        return compact
    for song in raw_songs:
        if not isinstance(song, dict):
            continue
        sid = song.get("id")
        if not sid:
            continue
        ars = song.get("ar") or song.get("artists") or []
        artist_ids: List[str] = []
        artist_names: List[str] = []
        if isinstance(ars, list):
            for a in ars:
                if isinstance(a, dict):
                    if a.get("id"):
                        artist_ids.append(str(a.get("id")))
                    if a.get("name"):
                        artist_names.append(a.get("name"))
        compact.append({
            "id": str(sid),
            "name": song.get("name") or "",
            "artist_ids": list(dict.fromkeys(artist_ids)),
            "artist_names": list(dict.fromkeys(artist_names)),
        })
    return compact


def normalize_album_name(value: Any) -> str:
    """用于交叉校验的专辑名规范化，不改变最终展示文本。"""
    text = str(value or "").strip().casefold()
    return re.sub(r"[\W_]+", "", text, flags=re.UNICODE)


def album_name_matches_expected(actual: Any, expected_names: Optional[Iterable[str]]) -> bool:
    actual_key = normalize_album_name(actual)
    expected = {normalize_album_name(x) for x in (expected_names or []) if normalize_album_name(x)}
    if not actual_key or not expected:
        return True
    return actual_key in expected


def album_cache_needs_recheck(entry: Dict[str, Any]) -> bool:
    """临近当前日期或未知日期的 album cache 需要周期性复查，避免当天新专 publishTime/API 可见性滞后造成漏扫。"""
    now = datetime.now(CN_TZ).replace(tzinfo=None)
    cached_at = safe_int(entry.get("cached_at"), 0)
    age_hours = (time.time() - cached_at) / 3600 if cached_at else 999999
    publish_ts = entry.get("publish_ts")
    if not publish_ts or entry.get("date_str") == "未知日期":
        return age_hours >= ALBUM_CACHE_UNKNOWN_RECHECK_HOURS
    _, dt = parse_cn_date_from_ms(publish_ts)
    if not dt:
        return age_hours >= ALBUM_CACHE_UNKNOWN_RECHECK_HOURS
    if abs((now - dt).days) <= 14:
        return age_hours >= ALBUM_CACHE_RECENT_RECHECK_HOURS
    return False


def fetch_album_detail(album_id: str, cookie: str, need_songs: bool = False, expected_names: Optional[Iterable[str]] = None) -> Dict[str, Any]:
    """严格获取 album 详情。

    NeteaseCloudMusicApi 在高并发/缓存异常时偶尔会出现：请求 A album_id，返回 B album.id。
    旧逻辑直接抛错，导致整张合辑在日期过滤阶段消失。这里改成：
    1) 缓存校验；
    2) GET/POST/无 cookie 多路径强制重试；
    3) 仍错配才上抛，让上层可用 recent 关系做 fallback。
    """
    album_id = str(album_id)
    with ALBUM_CACHE_LOCK:
        cached = ALBUM_CACHE.get(album_id)
        if cached is not None:
            if is_valid_album_cache_entry(album_id, cached):
                if (not need_songs or cached.get("songs")) and not album_cache_needs_recheck(cached) and album_name_matches_expected(cached.get("album_name"), expected_names):
                    METRICS.inc("album_cache_hit")
                    return cached
                METRICS.inc("album_cache_recheck")
            else:
                ALBUM_CACHE.pop(album_id, None)
                METRICS.inc("album_cache_corrupt")
    METRICS.inc("album_cache_miss")

    def request_once(method: str, ck: str, label: str) -> Dict[str, Any]:
        params = {"id": album_id}
        if ck:
            params["cookie"] = ck
        # 加一个额外随机项，尽量绕开本地/上游缓存错串。
        params["_album_fix_nonce"] = f"{int(time.time()*1000)}_{random.randint(1000,9999)}"
        data = api_request("/album", params, preferred=method, exact_method=True, timeout=max(API_TIMEOUT, 10))
        if data.get("code") != 200:
            raise ApiError("/album", f"返回码 {data.get('code')} [{label}]")
        album = data.get("album") or {}
        returned_id = album.get("id")
        if returned_id is not None and str(returned_id) != album_id:
            METRICS.inc("album_id_mismatch")
            raise AlbumIdMismatchError("/album", f"album_id 错配：请求 {album_id}，返回 {returned_id} [{label}]")
        if expected_names and not album_name_matches_expected(album.get("name"), expected_names):
            METRICS.inc("album_name_mismatch")
            raise AlbumNameMismatchError("/album", f"album_name 疑似错配：请求 {album_id}，返回 {album.get('name')} [{label}]")
        return data

    last_err: Optional[Exception] = None
    data: Optional[Dict[str, Any]] = None
    with METHOD_CACHE_LOCK:
        preferred_method = METHOD_CACHE.get("/album", "GET")
    method_order = [preferred_method] + (["POST"] if preferred_method == "GET" else ["GET"])
    mismatch_seen = False
    for round_no in range(max(1, ALBUM_MISMATCH_RETRIES)):
        round_mismatch = False
        for method in method_order:
            try:
                data = request_once(method, cookie, f"strict-{method.lower()}-{round_no+1}")
                break
            except (AlbumIdMismatchError, AlbumNameMismatchError) as e:
                last_err = e
                round_mismatch = True
                mismatch_seen = True
            except ApiError as e:
                last_err = e
        if data is not None:
            break
        if round_mismatch:
            try:
                data = request_once(method_order[0], "", f"strict-{method_order[0].lower()}-nocookie-{round_no+1}")
            except (AlbumIdMismatchError, AlbumNameMismatchError) as e:
                last_err = e
                mismatch_seen = True
            except ApiError as e:
                last_err = e
        if data is not None:
            break
        if not mismatch_seen:
            break
        time.sleep(0.15 + random.random() * 0.25)
    if data is None:
        raise ApiError("/album", redacted_error_text(str(last_err)))

    album = data.get("album") or {}
    date_str, dt = parse_cn_date_from_ms(album.get("publishTime"))
    artist_ids, artist_names = extract_album_artists(album)
    payload = {
        "album_id": album_id,
        "album_name": album.get("name") or "",
        "date_str": date_str,
        "publish_ts": album.get("publishTime"),
        "artist_ids": artist_ids,
        "artist_names": artist_names,
        "size": album.get("size"),
        "cached_at": time.time(),
    }
    if need_songs:
        payload["songs"] = compact_album_songs(data.get("songs") or album.get("songs") or [])
    with ALBUM_CACHE_LOCK:
        old = ALBUM_CACHE.get(album_id)
        if old and isinstance(old, dict) and old.get("songs") and not payload.get("songs"):
            payload["songs"] = old.get("songs")
        ALBUM_CACHE[album_id] = payload
    return payload


def build_album_detail_fallback_from_relations(album_id: str, rels: List[Dict[str, Any]], new_candidates: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """当 /album 详情错配/失败时，尝试用采样关系构造保守 album 详情。

    只作为召回兜底：优先使用 /artist/songs 中的 album_name 与 song.publishTime。
    如果没有可用日期，则返回 None，因为无法安全进入日期窗口。
    """
    if not ALLOW_ALBUM_FALLBACK_FROM_RECENT:
        return None
    album_id = str(album_id)
    names: List[str] = []
    publish_candidates: List[int] = []
    artist_ids: List[str] = []
    artist_names: List[str] = []
    for r in rels or []:
        if r.get("album_name"):
            names.append(str(r.get("album_name")))
        ts = safe_int(r.get("song_publish_ts") or r.get("song_publish_time") or 0, 0)
        if ts:
            publish_candidates.append(ts)
        artist_ids.append(str(r.get("artist_id")))
        artist_names.append(str(r.get("artist_name")))
    for c in new_candidates or []:
        if c.get("album_name"):
            names.append(str(c.get("album_name")))
        ts = safe_int(c.get("publish_ts") or c.get("publishTime") or 0, 0)
        if ts:
            publish_candidates.append(ts)
        artist_ids.append(str(c.get("artist_id", "")))
        artist_names.append(str(c.get("artist_name", "")))
    if not publish_candidates:
        METRICS.inc("album_fallback_no_date")
        return None
    # 对同一 album 的歌曲，publishTime 理论上应一致；取最大值更有利于当天新专同步延迟场景。
    publish_ts = max(publish_candidates)
    date_str, dt = parse_cn_date_from_ms(publish_ts)
    name = names[0] if names else f"album:{album_id}"
    payload = {
        "album_id": album_id,
        "album_name": name,
        "date_str": date_str,
        "publish_ts": publish_ts,
        "artist_ids": list(dict.fromkeys([x for x in artist_ids if x and x != "None"])),
        "artist_names": list(dict.fromkeys([x for x in artist_names if x and x != "None"])),
        "size": None,
        "cached_at": time.time(),
        "fallback_from_recent": True,
    }
    METRICS.inc("album_detail_fallback_from_recent")
    return payload


def fetch_artist_album_page(artist_id: str, cookie: str, limit: int) -> Dict[str, Any]:
    key = (cookie_fingerprint(cookie), str(artist_id), "page")
    with ARTIST_ALBUM_CACHE_LOCK:
        cached = ARTIST_ALBUM_PAGE_CACHE.get(key)
        if cached is not None and len(cached.get("hotAlbums") or []) >= limit:
            return cached
    data = api_request("/artist/album", {
        "id": artist_id,
        "limit": limit,
        "offset": 0,
        "cookie": cookie,
    }, preferred="GET")
    # Server memory: retain only fields used by new-release and ownership checks.
    data = {
        "code": data.get("code"),
        "hotAlbums": [{field: album.get(field) for field in ("id", "name", "publishTime")}
                      for album in data.get("hotAlbums") or []],
    }
    with ARTIST_ALBUM_CACHE_LOCK:
        old = ARTIST_ALBUM_PAGE_CACHE.get(key)
        if old is None or len(data.get("hotAlbums") or []) >= len(old.get("hotAlbums") or []):
            ARTIST_ALBUM_PAGE_CACHE[key] = data
    return data


def fetch_owned_album_ids_shallow(artist_id: str, cookie: str, limit: int) -> Tuple[set, Dict[str, str]]:
    data = fetch_artist_album_page(artist_id, cookie, limit)
    if data.get("code") != 200:
        raise ApiError("/artist/album", f"返回码 {data.get('code')}")
    owned_ids = set()
    owned_dates: Dict[str, str] = {}
    for a in data.get("hotAlbums") or []:
        if a.get("id"):
            aid = str(a.get("id"))
            owned_ids.add(aid)
            date_str, _ = parse_cn_date_from_ms(a.get("publishTime"))
            if date_str != "未知日期":
                owned_dates[aid] = date_str
    return owned_ids, owned_dates


def fetch_new_albums_for_artist(artist: Dict[str, str], cookie: str, limit: int, new_start_dt: datetime, new_end_dt: datetime) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """从艺人专辑页浅层兜底新专，解决新专已上架但尚未同步进 /artist/songs recent 的漏扫。"""
    t0 = time.time()
    artist_id = artist["id"]
    artist_name = artist["name"]
    try:
        data = fetch_artist_album_page(artist_id, cookie, limit)
        if data.get("code") != 200:
            raise ApiError("/artist/album", f"返回码 {data.get('code')}")
        found: List[Dict[str, Any]] = []
        for a in data.get("hotAlbums") or []:
            aid = a.get("id")
            if not aid:
                continue
            date_str, dt = parse_cn_date_from_ms(a.get("publishTime"))
            if dt and new_start_dt <= dt <= new_end_dt:
                found.append({
                    "artist_id": artist_id,
                    "artist_name": artist_name,
                    "album_id": str(aid),
                    "album_name": a.get("name") or "",
                    "date_str": date_str,
                    "publish_ts": a.get("publishTime"),
                    "source": "artist_album_new_probe",
                })
        dur = time.time() - t0
        if dur > 2.0:
            METRICS.add_slow_artist(artist_name, dur, {"stage": "new_album_probe", "found": len(found)})
        return found, {"ok": True, "duration": dur, "found": len(found)}
    except Exception as e:
        dur = time.time() - t0
        METRICS.inc("new_album_probe_failed")
        return [], {"ok": False, "duration": dur, "error": redacted_error_text(str(e))}


def build_new_album_relations(candidate: Dict[str, Any], detail: Dict[str, Any], existing_keys: set) -> List[Dict[str, Any]]:
    """把专辑页发现的新专转换为输出关系；优先展开 album songs，必要时输出 album-level 占位行。"""
    artist_id = str(candidate.get("artist_id") or "")
    artist_name = candidate.get("artist_name") or ""
    album_id = str(candidate.get("album_id") or detail.get("album_id") or "")
    album_name = candidate.get("album_name") or detail.get("album_name") or ""
    rels: List[Dict[str, Any]] = []
    songs = detail.get("songs") or []
    for s in songs:
        sid = str(s.get("id") or "")
        if not sid:
            continue
        # 这是艺人专辑页发现的新专：默认输出专辑内曲目；如果歌曲 artist 信息明确且不含该艺人，仍保留，避免制作人/合作专辑漏曲。
        key = (album_id, artist_id, sid)
        if key in existing_keys:
            continue
        existing_keys.add(key)
        rels.append({
            "artist_id": artist_id,
            "artist_name": artist_name,
            "song_id": sid,
            "song_name": s.get("name") or "",
            "album_id": album_id,
            "album_name": album_name,
            "song_artist_ids": s.get("artist_ids") or [],
            "song_artist_names": s.get("artist_names") or [],
            "link": f"https://music.163.com/#/song?id={sid}",
            "source": "artist_album_new_probe",
        })
    already_represented = any(k[0] == album_id and k[1] == artist_id for k in existing_keys)
    if not rels and not already_represented:
        synthetic_id = f"album-{album_id}-{artist_id}"
        key = (album_id, artist_id, synthetic_id)
        if key not in existing_keys:
            existing_keys.add(key)
            rels.append({
                "artist_id": artist_id,
                "artist_name": artist_name,
                "song_id": synthetic_id,
                "song_name": f"【新专辑】{album_name}",
                "album_id": album_id,
                "album_name": album_name,
                "song_artist_ids": [artist_id] if artist_id else [],
                "song_artist_names": [artist_name] if artist_name else [],
                "link": f"https://music.163.com/#/album?id={album_id}",
                "source": "artist_album_new_probe",
            })
    return rels

# ---------------------------- 动态并发任务池 ----------------------------

def run_dynamic_pool(items: List[Any], func: Callable[[Any], Any], phase_label: str, progress_cb: Optional[Callable[[int, int, Any], None]] = None) -> Iterable[Any]:
    # Server memory: stream completed results instead of retaining a whole phase.
    total = len(items)
    idx = 0
    done = 0
    futures: Dict[concurrent.futures.Future, Any] = {}
    max_workers = CONTROL.manual_thread_max
    METRICS.set_phase(phase_label)
    METRICS.set_phase_progress(0, total)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        while (idx < total or futures) and not CONTROL.snapshot()["stop_requested"]:
            if not wait_if_paused_or_stopped():
                break
            target = CONTROL.snapshot()["target_threads"]
            while idx < total and len(futures) < target and not CONTROL.snapshot()["paused"] and not CONTROL.snapshot()["stop_requested"]:
                item = items[idx]
                idx += 1
                futures[executor.submit(func, item)] = item
            if not futures:
                time.sleep(0.1)
                continue
            finished, _ = concurrent.futures.wait(futures.keys(), timeout=0.5, return_when=concurrent.futures.FIRST_COMPLETED)
            for fut in finished:
                item = futures.pop(fut)
                done += 1
                METRICS.set_phase_progress(done, total)
                try:
                    res = fut.result()
                    yield res
                except Exception as e:
                    emit({"type": "log", "level": "error", "text": f"{phase_label} 任务异常：{redacted_error_text(str(e))}"})
                    yield e
                if progress_cb:
                    progress_cb(done, total, item)
        # stop requested: 不再提交新任务，等待已提交任务自然收尾一小段；requests 有 timeout

# ---------------------------- 扫描主流程：Album Candidate Pool ----------------------------

def save_metrics_and_report(reason: str) -> None:
    snap = METRICS.snapshot()
    snap["save_reason"] = reason
    with METRICS_FILE.open("w", encoding="utf-8") as f:
        json.dump(snap, f, ensure_ascii=False, indent=2)

    lines: List[str] = []
    lines.append("网易云扫描调参报告")
    lines.append("=" * 40)
    lines.append(f"保存原因：{reason}")
    lines.append(f"阶段：{snap['phase']}")
    lines.append(f"耗时：{snap['elapsed_sec']} 秒")
    lines.append(f"当前阶段进度：{snap['phase_done']} / {snap['phase_total']}，速度 {snap['phase_per_min']} {snap['phase_unit']}/分钟，ETA {snap['phase_eta_min'] if snap['phase_eta_min'] is not None else '-'} 分钟")
    lines.append(f"艺人进度：{snap['artist_done']} / {snap['artist_total']}，速度 {snap['artists_per_min']} 位/分钟")
    lines.append(f"Album 详情：{snap['album_done']} / {snap['album_total']}")
    lines.append(f"Owned 验证：{snap['verify_done']} / {snap['verify_total']}")
    lines.append(f"结果：{snap['results']}，CSV 跳过：{snap['csv_skipped']}")
    lines.append(f"Album 缓存命中/未命中：{snap['album_cache_hit']} / {snap['album_cache_miss']}")
    lines.append("")
    lines.append("Endpoint 统计：")
    for path, rec in snap.get("endpoints", {}).items():
        lines.append(
            f"- {path}: count={rec.get('count')} ok={rec.get('ok')} 405={rec.get('status_405')} "
            f"460={rec.get('status_460')} timeout={rec.get('timeout')} conn={rec.get('conn_error')} "
            f"avg={rec.get('avg_ms')}ms p95={rec.get('p95_ms')}ms max={rec.get('max_ms')}ms"
        )
    lines.append("")
    lines.append("经验判断：")
    endpoints = snap.get("endpoints", {})
    total_405 = sum(v.get("status_405", 0) for v in endpoints.values())
    total_req = sum(v.get("count", 0) for v in endpoints.values())
    if total_req:
        rate_405 = total_405 / total_req
        lines.append(f"- 405 比例：{rate_405:.3%}")
        if rate_405 > 0.01:
            lines.append("- 405 已偏高：下次应降低对应 endpoint 或全局投递并发，不应自动升线程。")
        else:
            lines.append("- 405 较低：当前瓶颈更可能是候选 album 数量、album cache 命中率或 owned 验证。")
    if endpoints:
        slow = sorted(endpoints.items(), key=lambda kv: kv[1].get("p95_ms", 0), reverse=True)[:3]
        lines.append("- p95 最慢端点：" + ", ".join([f"{k}({v.get('p95_ms')}ms)" for k, v in slow]))
    if snap.get("slow_artists"):
        lines.append("")
        lines.append("慢艺人 Top：")
        for x in snap["slow_artists"][:10]:
            lines.append(f"- {x.get('artist')}: {x.get('duration'):.2f}s stage={x.get('stage')} details={x}")

    with REPORT_FILE.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def autosave_if_needed(force: bool = False, reason: str = "autosave") -> None:
    now = time.time()
    if force or now - METRICS.last_autosave > 20:
        try:
            save_album_cache()
            save_metrics_and_report(reason)
            METRICS.last_autosave = now
        except Exception:
            pass


def scan_worker(scan_cookie: str, artist_source_input: str, external_start_dt: datetime, external_end_dt: datetime, new_start_dt: datetime, new_end_dt: datetime, recent_limit: int, owned_verify_limit: int, new_album_probe_limit: int, debug_album_id: str = "", artists_override: Optional[List[Dict[str, Any]]] = None, artist_source_label: str = "", finalize: bool = True, reset_seen: bool = True, include_merge_details: bool = False) -> None:
    global is_server_scanning
    all_results: List[Dict[str, Any]] = []
    retry_budget = RetryBudget()
    stopped = False
    if str(debug_album_id) in SKIP_ALBUM_DETAIL_IDS:
        debug_album_id = ""
    try:
        init_csv_and_seen(reset_current_scan=reset_seen)
        load_album_cache()
        CONTROL.resume()
        METRICS.set_phase("resolve_artist_list")
        emit({"type": "log", "level": "info", "text": "正在解析艺人列表来源：可使用大号 Cookie 或缓存指纹。"})
        if artists_override is None:
            artists, artist_cache_key, artist_source_mode = resolve_artist_list(scan_cookie, artist_source_input)
        else:
            artists = normalize_artist_list(artists_override)
            artist_cache_key = artist_source_label or "server-partition"
            artist_source_mode = "override"
        if not artists:
            emit({"type": "error", "text": "艺人列表为空，无法扫描。"})
            return

        if METRICS.artist_progress_total is None:
            METRICS.artist_total = len(artists)
        else:
            METRICS.artist_total = METRICS.artist_progress_total
        probe_label = f"新专目录兜底{new_album_probe_limit}" if new_album_probe_limit > 0 else "新专目录兜底已跳过(0)"
        emit({"type": "log", "level": "success", "text": f"全量扫描启动：艺人 {len(artists)} 位；recent{recent_limit}；Owned验证深度{owned_verify_limit}；{probe_label}；外部合辑范围 {external_start_dt:%Y-%m-%d}~{external_end_dt:%Y-%m-%d}；新专/新歌范围 {new_start_dt:%Y-%m-%d}~{new_end_dt:%Y-%m-%d}；艺人列表来源 {artist_source_mode}:{artist_cache_key}。"})
        if debug_album_id:
            emit({"type": "log", "level": "info", "text": f"漏扫诊断开启：album_id={debug_album_id}"})
        emit({"type": "state", "state": CONTROL.snapshot(), "metrics": METRICS.snapshot()})

        # 阶段 1：采样所有艺人 recentN 曲目，只收集关系，不做 album/owned 查询
        artist_relations: List[Dict[str, Any]] = []
        artist_failures: List[Dict[str, Any]] = []

        def sample_task(artist: Dict[str, str]) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, str]]:
            rels, info = fetch_recent_songs_for_artist(artist, scan_cookie, recent_limit)
            return rels, info, artist

        def sample_progress(done: int, total: int, item: Any) -> None:
            METRICS.artist_done = METRICS.artist_progress_offset + done
            if done % 10 == 0 or done == total:
                emit({"type": "progress", "phase": "采样 recent 曲目", "current": done, "total": total, "state": CONTROL.snapshot(), "metrics": METRICS.snapshot()})
                autosave_if_needed(reason="sampling")

        sample_outputs = run_dynamic_pool(artists, sample_task, "sample_recent_songs", sample_progress)
        for out in sample_outputs:
            if isinstance(out, Exception):
                continue
            rels, info, artist = out
            if info.get("ok"):
                artist_relations.extend(rels)
            else:
                artist_failures.append({"artist": artist, "error": info.get("error")})
        sample_errors = {x["artist"]["id"]: x for x in artist_failures}
        for artist, out in retry_failed([x["artist"] for x in artist_failures], sample_task,
                                        lambda x: x[1].get("ok"), "/artist/songs", lambda x: x["id"], retry_budget):
            if isinstance(out, Exception):
                sample_errors[artist["id"]]["error"] = redacted_error_text(str(out))
            elif out[1].get("ok"):
                artist_relations.extend(out[0])
                sample_errors.pop(artist["id"], None)
            else:
                sample_errors[artist["id"]]["error"] = out[1].get("error")
        artist_failures = list(sample_errors.values())
        METRICS.artist_failed += len(artist_failures)
        if CONTROL.snapshot()["stop_requested"]:
            stopped = True
            emit({"type": "log", "level": "error", "text": "已停止：recent 曲目采样阶段结束前收到停止信号。"})
            return

        # 阶段 1b：专门兜底“新专/新 EP”：艺人专辑页可能早于 /artist/songs 同步。
        new_album_candidates: List[Dict[str, Any]] = []
        new_album_probe_failures: List[Dict[str, Any]] = []

        if new_album_probe_limit > 0:
            def new_album_task(artist: Dict[str, str]) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, str]]:
                found, info = fetch_new_albums_for_artist(artist, scan_cookie, new_album_probe_limit, new_start_dt, new_end_dt)
                return found, info, artist

            def new_album_progress(done: int, total: int, item: Any) -> None:
                if done % 20 == 0 or done == total:
                    emit({"type": "progress", "phase": "兜底扫描新专目录", "current": done, "total": total, "state": CONTROL.snapshot(), "metrics": METRICS.snapshot()})
                    autosave_if_needed(reason="new_album_probe")

            new_album_outputs = run_dynamic_pool(artists, new_album_task, "new_album_probe", new_album_progress)
            for out in new_album_outputs:
                if isinstance(out, Exception):
                    continue
                found, info, artist = out
                if info.get("ok"):
                    new_album_candidates.extend(found)
                else:
                    new_album_probe_failures.append({"artist": artist, "error": info.get("error")})
            probe_errors = {x["artist"]["id"]: x for x in new_album_probe_failures}
            for artist, out in retry_failed([x["artist"] for x in new_album_probe_failures], new_album_task,
                                            lambda x: x[1].get("ok"), "/artist/album 新专", lambda x: x["id"], retry_budget):
                if isinstance(out, Exception):
                    probe_errors[artist["id"]]["error"] = redacted_error_text(str(out))
                elif out[1].get("ok"):
                    new_album_candidates.extend(out[0])
                    probe_errors.pop(artist["id"], None)
                else:
                    probe_errors[artist["id"]]["error"] = out[1].get("error")
            new_album_probe_failures = list(probe_errors.values())
            METRICS.new_album_probe_failed += len(new_album_probe_failures)
            if CONTROL.snapshot()["stop_requested"]:
                stopped = True
                emit({"type": "log", "level": "error", "text": "已停止：新专目录兜底阶段结束前收到停止信号。"})
                return
        else:
            emit({"type": "log", "level": "info", "text": "已跳过新专目录兜底阶段（newAlbumProbeLimit=0），直接进入 album 日期候选池。"})

        # 构造全局 album 候选池：recent 曲目 + 新专目录兜底 album
        by_album: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for r in artist_relations:
            by_album[r["album_id"]].append(r)
        new_album_by_id: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for c in new_album_candidates:
            new_album_by_id[c["album_id"]].append(c)
        album_candidate_ids = list(dict.fromkeys(list(by_album.keys()) + list(new_album_by_id.keys())))
        album_ids = [aid for aid in album_candidate_ids if str(aid) not in SKIP_ALBUM_DETAIL_IDS]
        METRICS.album_total = len(album_ids)
        emit({"type": "log", "level": "success", "text": f"recent 曲目采样完成：关系 {len(artist_relations)} 条；新专目录兜底命中 {len(new_album_candidates)} 条；唯一 album {len(album_ids)} 个；recent失败艺人 {len(artist_failures)} 位；新专兜底失败艺人 {len(new_album_probe_failures)} 位。"})
        if debug_album_id:
            dbg_rels = by_album.get(debug_album_id, [])
            dbg_new = new_album_by_id.get(debug_album_id, [])
            sample_names = ", ".join([f"{x.get('artist_name')}《{x.get('song_name')}》" for x in dbg_rels[:8]]) or "无"
            emit({"type": "log", "level": "info", "text": f"[漏扫诊断] album {debug_album_id}：recent采样关系 {len(dbg_rels)} 条；新专目录候选 {len(dbg_new)} 条；样例：{sample_names}"})

        # 阶段 2：全局 album_id 去重查详情，先按日期过滤
        album_details: Dict[str, Dict[str, Any]] = {}
        album_errors: Dict[str, str] = {}

        def album_task(album_id: str) -> Tuple[str, Optional[Dict[str, Any]], Optional[str]]:
            try:
                expected_names = [r.get("album_name") for r in by_album.get(str(album_id), [])]
                expected_names.extend(c.get("album_name") for c in new_album_by_id.get(str(album_id), []))
                detail = fetch_album_detail(album_id, scan_cookie, need_songs=(str(album_id) in new_album_by_id), expected_names=expected_names)
                return album_id, detail, None
            except Exception as e:
                return album_id, None, redacted_error_text(str(e))

        def album_progress(done: int, total: int, item: Any) -> None:
            METRICS.album_done = done
            if done % 20 == 0 or done == total:
                emit({"type": "progress", "phase": "查询 album 日期候选池", "current": done, "total": total, "state": CONTROL.snapshot(), "metrics": METRICS.snapshot()})
                autosave_if_needed(reason="album_pool")

        album_outputs = run_dynamic_pool(album_ids, album_task, "album_detail_pool", album_progress)
        for out in album_outputs:
            if isinstance(out, Exception):
                continue
            aid, detail, err = out
            if detail:
                album_details[aid] = detail
            elif err:
                # /album 偶发返回错 album_id 时，用 recent 采样中的 song.publishTime + al.name 做保守兜底，
                # 避免整张当天合辑因为详情接口串包而漏掉。
                fallback = build_album_detail_fallback_from_relations(aid, by_album.get(aid, []), new_album_by_id.get(aid, []))
                if fallback:
                    album_details[aid] = fallback
                    album_errors[aid] = f"{err}；已用recent采样fallback"
                else:
                    album_errors[aid] = err
        for aid, out in retry_failed(list(album_errors), album_task, lambda x: bool(x[1]),
                                     "/album", str, retry_budget):
            if not isinstance(out, Exception) and out[1]:
                album_details[aid] = out[1]
                album_errors.pop(aid, None)
            else:
                err = redacted_error_text(str(out)) if isinstance(out, Exception) else out[2] or "unknown"
                album_errors[aid] = err + ("；已用recent采样fallback" if aid in album_details else "")
        save_album_cache()
        if CONTROL.snapshot()["stop_requested"]:
            stopped = True
            emit({"type": "log", "level": "error", "text": "已停止：album 候选池阶段收到停止信号。"})
            return

        date_hit_albums: Dict[str, Dict[str, Any]] = {}
        external_range_albums: set = set()
        new_release_albums: set = set()
        for aid, detail in album_details.items():
            _, dt = parse_cn_date_from_ms(detail.get("publish_ts"))
            if not dt:
                continue
            in_external_range = external_start_dt <= dt <= external_end_dt
            in_new_release_window = new_start_dt <= dt <= new_end_dt
            if in_external_range or in_new_release_window:
                date_hit_albums[aid] = detail
            if in_external_range:
                external_range_albums.add(aid)
            if in_new_release_window:
                new_release_albums.add(aid)
        emit({"type": "log", "level": "success", "text": f"日期过滤完成：待验证 album {len(date_hit_albums)} 个；外部合辑日期范围命中 {len(external_range_albums)} 个；新专/新歌窗口命中 {len(new_release_albums)} 个；无关 album {len(album_details)-len(date_hit_albums)} 个。"})
        if debug_album_id:
            d = album_details.get(debug_album_id)
            err = album_errors.get(debug_album_id)
            if d:
                emit({"type": "log", "level": "info", "text": f"[漏扫诊断] album {debug_album_id}：album详情成功；name={d.get('album_name')}；date={d.get('date_str')}；fallback={bool(d.get('fallback_from_recent'))}；external命中={debug_album_id in external_range_albums}；new命中={debug_album_id in new_release_albums}；date_hit={debug_album_id in date_hit_albums}"})
            else:
                emit({"type": "log", "level": "error", "text": f"[漏扫诊断] album {debug_album_id}：album详情未成功；error={err or '未进入album候选池/未返回详情'}"})

        # 阶段 3：只对日期命中 album 涉及的艺人做 shallow owned 验证。
        # 新专本身不依赖 owned 验证，但外部合辑检测仍需要 owned_ids；为了保持结果列表一致，统一验证候选涉及艺人。
        verify_artist_map: Dict[str, Dict[str, Any]] = {}
        candidate_relations: List[Dict[str, Any]] = []
        existing_relation_keys = set()
        for aid in date_hit_albums.keys():
            for r in by_album.get(aid, []):
                key = (r["album_id"], r["artist_id"], r["song_id"])
                existing_relation_keys.add(key)
                candidate_relations.append(r)
                if r["artist_id"] not in verify_artist_map:
                    verify_artist_map[r["artist_id"]] = {"id": r["artist_id"], "name": r["artist_name"]}
            # 新专窗口内的 album 如果尚未同步进 /artist/songs，则从 /artist/album + /album songs 构造关系。
            if aid in new_release_albums:
                detail = date_hit_albums.get(aid, {})
                for c in new_album_by_id.get(aid, []):
                    for r in build_new_album_relations(c, detail, existing_relation_keys):
                        candidate_relations.append(r)
                        if r["artist_id"] not in verify_artist_map:
                            verify_artist_map[r["artist_id"]] = {"id": r["artist_id"], "name": r["artist_name"]}
        verify_artists = list(verify_artist_map.values())
        # Server memory: only date-matching relations are needed from this point.
        relation_count = len(artist_relations)
        artist_relations.clear()
        by_album.clear()
        new_album_candidates.clear()
        new_album_by_id.clear()
        METRICS.verify_total = len(verify_artists)
        emit({"type": "log", "level": "info", "text": f"进入归属验证：候选关系 {len(candidate_relations)} 条，需验证艺人 {len(verify_artists)} 位。"})
        if debug_album_id:
            dbg_cands = [x for x in candidate_relations if x.get('album_id') == debug_album_id]
            sample = ", ".join([f"{x.get('artist_name')}《{x.get('song_name')}》" for x in dbg_cands[:8]]) or "无"
            emit({"type": "log", "level": "info", "text": f"[漏扫诊断] album {debug_album_id}：进入最终候选关系 {len(dbg_cands)} 条；样例：{sample}"})

        owned_map: Dict[str, set] = {}
        owned_errors: Dict[str, str] = {}

        def verify_task(artist: Dict[str, str]) -> Tuple[str, Optional[set], Optional[str]]:
            t0 = time.time()
            try:
                owned, _ = fetch_owned_album_ids_shallow(artist["id"], scan_cookie, owned_verify_limit)
                dur = time.time() - t0
                if dur > 2.0:
                    METRICS.add_slow_artist(artist["name"], dur, {"stage": "owned_verify", "owned_count": len(owned)})
                return artist["id"], owned, None
            except Exception as e:
                dur = time.time() - t0
                METRICS.add_slow_artist(artist["name"], dur, {"stage": "owned_verify_failed", "error": redacted_error_text(str(e))})
                return artist["id"], None, redacted_error_text(str(e))

        def verify_progress(done: int, total: int, item: Any) -> None:
            METRICS.verify_done = done
            if done % 10 == 0 or done == total:
                emit({"type": "progress", "phase": "验证 album 归属", "current": done, "total": total, "state": CONTROL.snapshot(), "metrics": METRICS.snapshot()})
                autosave_if_needed(reason="owned_verify")

        verify_outputs = run_dynamic_pool(verify_artists, verify_task, "owned_verify", verify_progress)
        for out in verify_outputs:
            if isinstance(out, Exception):
                continue
            artist_id, owned, err = out
            if owned is not None:
                owned_map[artist_id] = owned
            else:
                owned_errors[artist_id] = err or "unknown"

        for artist, out in retry_failed([x for x in verify_artists if x["id"] in owned_errors], verify_task,
                                        lambda x: x[1] is not None, "/artist/album 归属", lambda x: x["id"], retry_budget):
            if not isinstance(out, Exception) and out[1] is not None:
                owned_map[artist["id"]] = out[1]
                owned_errors.pop(artist["id"], None)
            else:
                owned_errors[artist["id"]] = redacted_error_text(str(out)) if isinstance(out, Exception) else out[2] or "unknown"
        if CONTROL.snapshot()["stop_requested"]:
            stopped = True
            emit({"type": "log", "level": "error", "text": "已停止：owned 验证阶段收到停止信号。"})
            return

        # 阶段 4：输出结果。固定输出“外部合辑/企划”与“新专/新歌窗口内的任何专辑”的并集。
        METRICS.set_phase("emit_results")
        METRICS.set_phase_progress(0, len(candidate_relations))
        album_artists_hit: Dict[str, set] = defaultdict(set)
        logical_artist_by_id = {
            str(artist.get("id")): artist.get("m3_logical_artist")
            for artist in artists if artist.get("m3_logical_artist")
        }
        for r in candidate_relations:
            logical_name = logical_artist_by_id.get(str(r["artist_id"])) or r["artist_name"]
            album_artists_hit[r["album_id"]].add(logical_name)

        debug_reasons = defaultdict(int)
        debug_outputs = 0
        compare_total = len(candidate_relations)
        compare_done = 0
        for r in candidate_relations:
            if not wait_if_paused_or_stopped():
                stopped = True
                return
            compare_done += 1
            METRICS.set_phase_progress(compare_done, compare_total)
            if compare_done % 50 == 0 or compare_done == compare_total:
                emit({"type": "progress", "phase": "比对筛选结果", "current": compare_done, "total": compare_total, "state": CONTROL.snapshot(), "metrics": METRICS.snapshot()})
            aid = r["album_id"]
            detail = date_hit_albums.get(aid)
            if not detail:
                if debug_album_id and aid == debug_album_id:
                    debug_reasons["no_detail"] += 1
                continue
            owned_ids = owned_map.get(r["artist_id"], set())
            owned_failed = r["artist_id"] in owned_errors
            # 外部合辑检测仍要求 owned 验证成功；新专/新歌窗口不依赖 owned 结果，避免 MitiS 这类自有新专被过滤。
            is_external_compilation = (aid in external_range_albums) and (not owned_failed) and (aid not in owned_ids)
            is_new_release = aid in new_release_albums
            if not (is_external_compilation or is_new_release):
                if debug_album_id and aid == debug_album_id:
                    if aid not in external_range_albums and aid not in new_release_albums:
                        debug_reasons["date_window_not_hit"] += 1
                    elif owned_failed:
                        debug_reasons["owned_verify_failed"] += 1
                    elif aid in owned_ids and aid not in new_release_albums:
                        debug_reasons["owned_album_filtered"] += 1
                    else:
                        debug_reasons["rule_filtered"] += 1
                continue
            link = r["link"]
            with SEEN_LOCK:
                if HIDE_EXISTING_CSV_HITS and link in SEEN_LINKS:
                    METRICS.inc("csv_skipped")
                    if debug_album_id and aid == debug_album_id:
                        debug_reasons["csv_link_skipped"] += 1
                    continue
            _, dt = parse_cn_date_from_ms(detail.get("publish_ts"))
            flag_val = 1 if dt and dt >= (datetime.now(CN_TZ).replace(tzinfo=None) - timedelta(days=1)) else 0
            # 展示用专辑名优先使用 /artist/songs 中与该歌曲绑定的 al.name。
            # /album 详情只用于日期过滤；这样即使历史 album cache 曾错配，也不会把别的合辑名显示到正确歌曲上。
            relation_album_name = r.get("album_name") or ""
            detail_album_name = detail.get("album_name") or ""
            if relation_album_name and detail_album_name and relation_album_name != detail_album_name:
                METRICS.inc("album_name_disagreement")
            hit = {
                "name": r["artist_name"],
                "song_name": r["song_name"],
                "album_name": relation_album_name or detail_album_name,
                "date_str": detail.get("date_str") or "未知日期",
                "link": link,
                "flag": flag_val,
                "album_id": aid,
                "album_count": len(album_artists_hit.get(aid, set())),
            }
            # The first round writes the new row.  A later round may encounter
            # the same link; it remains a historical duplicate and is skipped,
            # while the round summary still contributes to merged album heat.
            if write_hit_if_new(hit):
                with SEEN_LOCK:
                    CURRENT_SCAN_LINKS.add(link)
                if debug_album_id and aid == debug_album_id:
                    debug_outputs += 1
                all_results.append(hit)
                METRICS.inc("results")
                emit({"type": "hit", "data": hit})

        if debug_album_id:
            emit({"type": "log", "level": "info", "text": f"[漏扫诊断] album {debug_album_id}：最终输出 {debug_outputs} 条；过滤原因 {dict(debug_reasons)}"})

        # 按 album_count 回填与排序
        for h in all_results:
            h["album_count"] = len(album_artists_hit.get(h.get("album_id"), set()))
        all_results.sort(key=lambda x: (x.get("date_str", ""), x.get("album_count", 0)), reverse=True)
        emit({
            "type": "done",
            "results": all_results,
            "summary": {
                "artists": len(artists),
                "relations": relation_count,
                "unique_albums": len(album_ids),
                "date_hit_albums": len(date_hit_albums),
                "verify_artists": len(verify_artists),
                "artist_failures": len(artist_failures),
                "new_album_probe_failures": len(new_album_probe_failures),
                "album_errors": len(album_errors),
                "owned_errors": len(owned_errors),
                "slow_retry": retry_budget.snapshot(),
                "album_artist_counts": {aid: len(names) for aid, names in album_artists_hit.items()},
                **({"album_artist_names": {aid: sorted(names) for aid, names in album_artists_hit.items()},
                    "unique_album_ids": list(album_ids),
                    "date_hit_album_ids": list(date_hit_albums),
                    "verify_artist_ids": [str(artist["id"]) for artist in verify_artists],
                    }
                   if include_merge_details else {}),
                "round_artist_names": sorted({str(a.get("name", "")) for a in artists}),
                "failure_details": (
                    [{"stage": "/album", "item_id": aid, "error": err} for aid, err in list(album_errors.items())[:20]] +
                    [{"stage": "owned_verify", "item_id": aid, "error": err} for aid, err in list(owned_errors.items())[:20]] +
                    [{"stage": "recent_songs", "item_id": x["artist"]["id"], "error": x.get("error") or "unknown"} for x in artist_failures[:20]] +
                    [{"stage": "new_album_probe", "item_id": x["artist"]["id"], "error": x.get("error") or "unknown"} for x in new_album_probe_failures[:20]]
                )[:20],
            },
            "metrics": METRICS.snapshot(),
        })

    except Exception as e:
        err = redacted_error_text(str(e))
        emit({"type": "error", "text": f"服务端异常：{err}"})
        logging.error("扫描失败：%s", err)
    finally:
        if finalize:
            METRICS.finish()
            autosave_if_needed(force=True, reason="stopped" if stopped else "finished_or_error")
        # Server memory: this cache is useful only while a scan is running.
        with ARTIST_ALBUM_CACHE_LOCK:
            ARTIST_ALBUM_PAGE_CACHE.clear()
        if finalize:
            with SERVER_SCAN_LOCK:
                is_server_scanning = False

