"""Headless CLI runner for GitHub Actions wrapping server.Runtime without modifying scan logic."""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import sys
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Shanghai")
ROOT = Path(__file__).resolve().parent


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Headless Netease scanner runner for CI/GitHub Actions.")
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--api-base", default="http://127.0.0.1:3000")
    parser.add_argument("--max-threads", type=int, default=16)
    parser.add_argument("--cookie-env", default="MUSIC_COOKIE")
    parser.add_argument("--artist-cache-key", default=None)

    parser.add_argument("--source", choices=("manual", "scheduled"), default="manual")
    parser.add_argument("--schedule-slot", default="00:03")
    parser.add_argument("--skip-if-slot-exists", action="store_true")

    parser.add_argument("--scan-mode", choices=("auto", "regular", "m3", "m3_intersection"), default="auto")
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--external-start-date", default="")
    parser.add_argument("--external-end-date", default="")
    parser.add_argument("--new-start-date", default="")
    parser.add_argument("--new-end-date", default="")
    parser.add_argument("--recent-limit", type=int, default=50)
    parser.add_argument("--owned-limit", type=int, default=50)
    parser.add_argument("--new-album-probe-limit", type=int, default=20)
    parser.add_argument("--debug-album", default="")
    parser.add_argument("--retry-of", default="")

    parser.add_argument("--probe-only", action="store_true")
    parser.add_argument("--probe-artists", type=int, default=3)

    parser.add_argument("--result-json", type=Path, default=None)
    parser.add_argument("--today-csv", type=Path, default=None)
    parser.add_argument("--run-csv", type=Path, default=None)
    parser.add_argument("--m3-csv", type=Path, default=None)
    parser.add_argument("--desktop-zip", type=Path, default=None)
    parser.add_argument("--summary-md", type=Path, default=None)
    parser.add_argument("--fail-on-error", action=argparse.BooleanOptionalAction, default=True)
    return parser.parse_args(list(argv) if argv is not None else None)


def _write_csv(rows: List[Dict[str, Any]], dest: Path, include_heat: bool = True) -> None:
    import server

    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(server.CSV_HEADER + (["热度（同专辑命中艺人数）"] if include_heat else []))
        keys = ("name", "song_name", "album_name", "date_str", "link", "flag") + (
            ("album_count",) if include_heat else ()
        )
        for hit in rows:
            writer.writerow([hit.get(key, "") for key in keys])


def _write_m3_csv(report: Dict[str, Any], dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "类别",
                "网易云艺人 ID",
                "网易云艺人",
                "M3 艺人 ID",
                "M3 艺人",
                "匹配标签",
                "匹配来源",
                "匹配方式",
                "网易云候选 ID / 名称",
                "M3 候选 ID / 艺人",
                "原因",
            ]
        )
        ambiguous_label = "未纳入交集·歧义" if report.get("intersection_only") else "第二轮·歧义"
        unmatched_label = "未纳入交集·无匹配" if report.get("intersection_only") else "第二轮·未匹配"
        for item in report.get("matches", []):
            writer.writerow(
                [
                    "M3交集" if report.get("intersection_only") else "第一轮",
                    item.get("netease_id", ""),
                    item.get("netease_name", ""),
                    item.get("m3_artist_id", ""),
                    item.get("m3_name", ""),
                    item.get("matched_label", ""),
                    item.get("match_source", ""),
                    item.get("match_kind", ""),
                    "",
                    "",
                    "",
                ]
            )
        for item in report.get("ambiguous_matches", []):
            netease_candidates = " / ".join(
                f"{candidate['id']}:{candidate['name']}"
                for candidate in item.get("netease_candidates", [])
            )
            m3_candidates = item.get("m3_candidate_records", [])
            m3_candidate_text = " / ".join(
                f"{candidate['m3_artist_id']}:{candidate['name']}"
                if candidate.get("m3_artist_id")
                else candidate["name"]
                for candidate in m3_candidates
            )
            if not m3_candidate_text:
                m3_candidate_text = " / ".join(item.get("m3_candidates", []))
            writer.writerow(
                [
                    ambiguous_label,
                    item.get("netease_id", ""),
                    item.get("netease_name", ""),
                    "",
                    "",
                    "",
                    "",
                    "",
                    netease_candidates,
                    m3_candidate_text,
                    item.get("reason", ""),
                ]
            )
        for item in report.get("unmatched_followed", []):
            writer.writerow(
                [
                    unmatched_label,
                    item.get("netease_id", ""),
                    item.get("netease_name", ""),
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    item.get("reason", ""),
                ]
            )


def _md_escape(text: Any) -> str:
    return str(text or "").replace("|", "\\|").replace("\r", " ").replace("\n", " ")


MAX_SUMMARY_BYTES = 900_000


def _is_album_placeholder(hit: Dict[str, Any]) -> bool:
    return (
        str(hit.get("song_name") or "").startswith("【新专辑】")
        and "/album?id=" in str(hit.get("link") or "")
    )


def _group_albums(hits: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    groups: Dict[str, Dict[str, Any]] = {}
    for hit in hits:
        album_id = str(hit.get("album_id") or "").strip()
        album_name = str(hit.get("album_name") or "").strip()
        key = album_id or (f"name:{album_name}" if album_name else f"link:{hit.get('link', '')}")
        group = groups.setdefault(
            key,
            {
                "album_id": album_id,
                "album_name": album_name or "（未命名专辑）",
                "heat": 0,
                "latest_date": "",
                "tracks": [],
            },
        )
        heat_val = hit.get("album_count")
        try:
            heat_int = int(heat_val) if heat_val is not None and heat_val != "" else 0
        except (TypeError, ValueError):
            heat_int = 0
        group["heat"] = max(group["heat"], heat_int)
        date_str = str(hit.get("date_str") or "")
        if date_str > group["latest_date"]:
            group["latest_date"] = date_str
        group["tracks"].append(hit)

    for group in groups.values():
        tracks = group["tracks"]
        if any(not _is_album_placeholder(t) for t in tracks):
            tracks = [t for t in tracks if not _is_album_placeholder(t)]
            group["tracks"] = tracks
        seen_artists: set[str] = set()
        artists: List[str] = []
        for t in tracks:
            name = str(t.get("name") or "").strip()
            if name and name not in seen_artists:
                seen_artists.add(name)
                artists.append(name)
        group["artists"] = artists

    return sorted(
        groups.values(),
        key=lambda g: (g["heat"], g["latest_date"], g["album_name"].casefold()),
        reverse=True,
    )


def _build_scan_summary_md(
    run: Dict[str, Any],
    today_str: str,
    run_hits: List[Dict[str, Any]],
    day_hits: List[Dict[str, Any]],
    scrub_fn: Any,
) -> str:
    status = run.get("status", "unknown")
    badges = {
        "success": "✅ 成功 (success)",
        "partial": "⚠️ 部分完成 (partial)",
        "failed": "❌ 失败 (failed)",
        "stopped": "⏹️ 已停止 (stopped)",
        "interrupted": "⚠️ 已中断 (interrupted)",
    }
    badge = badges.get(status, status)
    params = run.get("params") or {}
    summary = run.get("summary") or {}
    metrics = run.get("metrics") or {}
    elapsed_min = round(float(metrics.get("elapsed_sec") or 0) / 60.0, 1)

    lines = [
        f"## 网易云新曲扫描报告 · {today_str}",
        "",
        f"**状态**：{badge}",
        "",
        "| 项目 | 值 |",
        "| --- | --- |",
        f"| Run ID | `{run.get('id', '')}` |",
        f"| 来源 | `{run.get('source', '')}`"
        + (f" (`{run.get('schedule_time')}`)" if run.get("schedule_time") else "")
        + " |",
        f"| 扫描模式 | `{params.get('scanMode', '')}` |",
        f"| 并发线程 | `{params.get('threads', '')}` |",
        f"| 外部合辑窗口 | `{params.get('externalStartDate', '')}` ~ `{params.get('externalEndDate', '')}` |",
        f"| 新专/新歌窗口 | `{params.get('newStartDate', '')}` ~ `{params.get('newEndDate', '')}` |",
        f"| 耗时 | {elapsed_min} 分钟 |",
        f"| 本次新增 | **{len(run_hits)}** 条（CSV 已去重 {metrics.get('csv_skipped', 0)} 条） |",
        f"| 今日累计新增 | **{len(day_hits)}** 条 |",
        f"| 覆蓋艺人 / 候选专辑 | {summary.get('artists', 0)} 位 / {summary.get('unique_albums', 0)} 张 |",
        f"| 失败计数 | 艺人 {summary.get('artist_failures', 0)} · 探测 {summary.get('new_album_probe_failures', 0)} · 专辑 {summary.get('album_errors', 0)} · 归属 {summary.get('owned_errors', 0)} · 异常 {summary.get('task_errors', 0)} |",
        "",
    ]

    if run.get("error"):
        lines.extend(["### 错误信息", "", f"> ❌ {scrub_fn(run['error'])}", ""])
    if run.get("warning"):
        lines.extend(["### 告警信息", "", f"> ⚠️ {scrub_fn(run['warning'])}", ""])

    display_hits = run_hits if run_hits else day_hits
    albums = _group_albums(display_hits)
    display_label = (
        f"本次新增专辑（共 {len(albums)} 张）"
        if run_hits
        else f"今日累计新增专辑（共 {len(albums)} 张）"
    )
    if albums:
        lines.extend(
            [
                f"### {display_label}",
                "",
                "| 热度 | 专辑 / 企划 | 发行日期 | 命中艺人 |",
                "| --- | --- | --- | --- |",
            ]
        )
        used_bytes = len(("\n".join(lines) + "\n").encode("utf-8"))
        truncated_count = 0
        for idx, album in enumerate(albums):
            heat = album["heat"]
            heat_str = f"🔥 {heat}" if heat >= 3 else (str(heat) if heat > 0 else "—")
            title = _md_escape(album["album_name"])
            if re.fullmatch(r"\d+", album["album_id"]):
                title_cell = f"[{title}](https://music.163.com/#/album?id={album['album_id']})"
            else:
                title_cell = title
            unique_artists = album.get("artists") or []
            if len(unique_artists) > 30:
                artists_cell = (
                    "、".join(_md_escape(a) for a in unique_artists[:30])
                    + f" 等共 {len(unique_artists)} 位"
                )
            else:
                artists_cell = "、".join(_md_escape(a) for a in unique_artists) or "—"
            row_line = f"| {heat_str} | {title_cell} | `{_md_escape(album['latest_date'])}` | {artists_cell} |"
            row_bytes = len((row_line + "\n").encode("utf-8"))
            if used_bytes + row_bytes > MAX_SUMMARY_BYTES:
                truncated_count = len(albums) - idx
                break
            lines.append(row_line)
            used_bytes += row_bytes
        if truncated_count > 0:
            lines.extend(
                [
                    "",
                    f"> ℹ️ 新增专辑数量较多，已截断剩余 {truncated_count} 张专辑以避免超出 GitHub Summary 容量上限；完整结果请下载 Artifacts CSV 或查看静态网页。",
                ]
            )
    else:
        lines.append("本次扫描及今日暂无新增曲目。")

    text = "\n".join(lines) + "\n"
    return scrub_fn(text)


def _build_probe_summary_md(report: Dict[str, Any]) -> str:
    if report.get("ok") and report.get("degraded"):
        status_line = "✅ 接口探测通过（降级：使用艺人缓存）"
    elif report.get("ok"):
        status_line = "✅ 接口探测通过"
    else:
        status_line = "❌ 接口探测失败"
    transport = report.get("transport") or {}
    transport_desc = (
        f"realIP=`{transport.get('realIP') or '未设置'}` · "
        f"crypto=`{transport.get('crypto') or '默认'}` · "
        f"代理：`{'已配置' if transport.get('proxy_configured') else '未配置'}`"
    )
    lines = [
        "## 网易云 API 探测报告",
        "",
        f"**结果**：{status_line}（耗时 {report.get('elapsed_sec', 0)} 秒，采样艺人 {report.get('artist_count', 0)} 位）",
        "",
        f"**传输参数**：{transport_desc}",
        "",
        "| 接口 | 状态 | 耗时 (ms) | 说明 |",
        "| --- | --- | --- | --- |",
    ]
    for chk in report.get("checks", []):
        status_cell = "✅" if chk.get("ok") else "⚠️ (已回退缓存)"
        lines.append(
            f"| `{chk.get('endpoint', '')}` | {status_cell} | {chk.get('elapsed_ms', '—')} | {_md_escape(chk.get('detail') or '—')} |"
        )
    if report.get("warning"):
        lines.extend(["", f"> ⚠️ {_md_escape(report['warning'])}"])
    if report.get("error"):
        lines.extend(["", f"> ❌ {_md_escape(report['error'])}"])
    diagnostics = report.get("diagnostics") or []
    if diagnostics:
        lines.extend(
            [
                "",
                "### 传输模式诊断矩阵（自动对比）",
                "",
                "| 接口 | 模式 | HTTP | 返回码 | 结果 | 说明 | 耗时 (ms) |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for item in diagnostics:
            lines.append(
                f"| `{_md_escape(item.get('endpoint'))}` | `{_md_escape(item.get('mode'))}` | "
                f"{item.get('http_status') or '—'} | {item.get('api_code') or '—'} | "
                f"{'✅' if item.get('ok') else '❌'} | {_md_escape(item.get('detail') or '—')} | "
                f"{item.get('elapsed_ms', '—')} |"
            )
    return "\n".join(lines) + "\n"


def _prepare_temp_config(
    temp_dir: Path,
    raw_cookie: str,
    threads: int,
    recent_limit: int,
    owned_limit: int,
    new_album_probe_limit: int,
    artist_cache_key: str,
) -> Path:
    import engine

    normalized = engine.normalize_cookie(raw_cookie)
    if not normalized or normalized.startswith("REPLACE_"):
        raise ValueError("MUSIC_COOKIE is empty or placeholder")

    temp_dir.mkdir(parents=True, exist_ok=True)
    cookie_path = temp_dir / "music_cookie.txt"
    cookie_path.write_text(normalized, encoding="utf-8")
    try:
        os.chmod(cookie_path, 0o600)
    except OSError:
        pass

    cfg = {
        "schedule_enabled": False,
        "cookie_file": str(cookie_path),
        "recentLimit": recent_limit,
        "ownedLimit": owned_limit,
        "newAlbumProbeLimit": new_album_probe_limit,
        "threads": threads,
        "backup_retention_days": 14,
        "artist_cache_key": artist_cache_key.strip(),
        "scan_mode": "auto",
        "m3_artist_file": str(ROOT / "m3_artist_names.json"),
        "m3_default_until": "2026-11-25",
        "m3_external_start_date": "2026-10-24",
    }
    cfg_path = temp_dir / "config.json"
    cfg_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    return cfg_path


def _write_json(path: Optional[Path], payload: Dict[str, Any]) -> None:
    if not path:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_text(path: Optional[Path], text: str) -> None:
    if not path:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def execute_scan(
    runtime: Any,
    args: argparse.Namespace,
    data_dir: Path,
) -> int:
    import engine
    import server

    today_str = datetime.now(TZ).date().isoformat()

    if args.source == "scheduled" and args.skip_if_slot_exists:
        if runtime.store.scheduled_exists(today_str, args.schedule_slot):
            msg = f"时段 {today_str} {args.schedule_slot} 今日已存在任务记录，跳过重复执行。"
            print(f"[ci_scan] {msg}", flush=True)
            day_hits = runtime.store.day_hits(today_str, limit=-1)
            if args.today_csv:
                _write_csv(day_hits, args.today_csv, include_heat=True)
            _write_json(
                args.result_json,
                {
                    "run_id": None,
                    "status": "skipped",
                    "skipped": True,
                    "date": today_str,
                    "schedule_slot": args.schedule_slot,
                    "result_count": 0,
                    "today_total": len(day_hits),
                    "elapsed_sec": 0,
                    "warning": "",
                    "error": "",
                },
            )
            _write_text(
                args.summary_md,
                f"## 网易云新曲扫描报告 · {today_str}\n\n⏭️ {msg}\n\n今日累计新增：**{len(day_hits)}** 条。\n",
            )
            return 0

    payload: Dict[str, Any] = {
        "scanMode": args.scan_mode,
        "threads": args.threads,
        "recentLimit": args.recent_limit,
        "ownedLimit": args.owned_limit,
        "newAlbumProbeLimit": args.new_album_probe_limit,
    }
    if args.external_start_date.strip():
        payload["externalStartDate"] = args.external_start_date.strip()
    if args.external_end_date.strip():
        payload["externalEndDate"] = args.external_end_date.strip()
    if args.new_start_date.strip():
        payload["newStartDate"] = args.new_start_date.strip()
    if args.new_end_date.strip():
        payload["newEndDate"] = args.new_end_date.strip()
    if args.debug_album.strip():
        payload["debugAlbum"] = args.debug_album.strip()

    client_queue = runtime.events.subscribe()
    stop_monitor = threading.Event()
    last_progress = {"phase": None, "ts": 0.0}

    def monitor_events() -> None:
        while not stop_monitor.is_set() or not client_queue.empty():
            try:
                msg = client_queue.get(timeout=1.0)
            except Exception:
                continue
            kind = msg.get("type")
            if kind == "progress":
                phase = msg.get("phase", "")
                cur = msg.get("current", 0)
                tot = msg.get("total", 0)
                now = time.monotonic()
                if phase != last_progress["phase"] or cur == tot or (now - last_progress["ts"]) >= 30.0:
                    last_progress["phase"] = phase
                    last_progress["ts"] = now
                    m = msg.get("metrics") or {}
                    print(
                        f"[progress] {phase} {cur}/{tot} | hits={m.get('results', 0)} "
                        f"dedup={m.get('csv_skipped', 0)} elapsed={round(float(m.get('elapsed_sec', 0)) / 60, 1)}m",
                        flush=True,
                    )
            elif kind == "log":
                level = msg.get("level", "info")
                text = runtime.scrub(msg.get("text", ""))
                if level == "error" or level == "success" or "开始" in text or "配对完成" in text:
                    print(f"[{level}] {text}", flush=True)
            elif kind in ("error", "storage_error", "finished"):
                print(f"[{kind}] {runtime.scrub(json.dumps(msg, ensure_ascii=False))}", flush=True)

    monitor_thread = threading.Thread(target=monitor_events, name="ci-event-monitor", daemon=True)
    monitor_thread.start()

    try:
        runtime.config.cookie()
        if args.retry_of.strip():
            run_id = runtime.retry(args.retry_of.strip())
        else:
            params = runtime.parameters(payload)
            with runtime.lock:
                if runtime.current or runtime.store.pending() or runtime.closed:
                    raise server.BusyError("已有扫描任务运行或等待中")
                run_id = runtime.store.create(
                    params,
                    source=args.source,
                    schedule_date=today_str if args.source == "scheduled" else None,
                    schedule_time=args.schedule_slot,
                )
                runtime._launch(run_id, params)

        if runtime.thread:
            runtime.thread.join()
    finally:
        stop_monitor.set()
        monitor_thread.join(timeout=5)
        runtime.events.unsubscribe(client_queue)

    run = runtime.store.get(run_id)
    if run is None:
        raise RuntimeError(f"Run {run_id} disappeared from store")

    run_hits = runtime.store.hits(run_id, limit=-1)
    day_hits = runtime.store.day_hits(today_str, limit=-1)

    if args.today_csv:
        _write_csv(day_hits, args.today_csv, include_heat=True)
    if args.run_csv:
        _write_csv(run_hits, args.run_csv, include_heat=True)
    if args.m3_csv:
        m3_report = runtime.store.get_json(f"m3_match_report:{run_id}")
        if m3_report is not None:
            _write_m3_csv(m3_report, args.m3_csv)

    if args.desktop_zip:
        try:
            export_meta = runtime.record_exports.create(server.now_iso())
            export_id = export_meta["export_id"]
            try:
                zip_src = runtime.record_exports.archive(export_id)
                if zip_src and zip_src.is_file():
                    args.desktop_zip.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(zip_src, args.desktop_zip)
            finally:
                runtime.record_exports.release(export_id)
        except Exception as exc:
            print(f"[ci_scan] Warning: desktop ZIP export skipped ({type(exc).__name__})", file=sys.stderr)

    def scrub_all(text: str) -> str:
        return engine.redacted_error_text(runtime.scrub(text))

    summary_md = _build_scan_summary_md(run, today_str, run_hits, day_hits, scrub_all)
    _write_text(args.summary_md, summary_md)

    metrics = run.get("metrics") or {}
    result_payload = {
        "run_id": run_id,
        "status": run.get("status"),
        "skipped": False,
        "date": today_str,
        "source": run.get("source"),
        "schedule_slot": run.get("schedule_time"),
        "result_count": len(run_hits),
        "today_total": len(day_hits),
        "elapsed_sec": metrics.get("elapsed_sec", 0),
        "warning": scrub_all(run.get("warning") or ""),
        "error": scrub_all(run.get("error") or ""),
    }
    _write_json(args.result_json, result_payload)

    status = run.get("status")
    if status == "partial":
        print(f"::warning::Scan {run_id} finished with partial status: {result_payload['warning']}")
    if status in ("failed", "interrupted") and args.fail_on_error:
        return 2
    return 0


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    data_dir = args.data_dir.resolve()
    data_dir.mkdir(parents=True, exist_ok=True)

    # Set environment variables BEFORE importing engine/server so SCRIPT_DIR and API_BASE bind cleanly.
    os.environ["NETEASE_DATA_DIR"] = str(data_dir)
    os.environ["NETEASE_API_BASE"] = args.api_base.rstrip("/")
    os.environ["NETEASE_AUTO_START_API"] = "0"
    os.environ["NETEASE_MAX_THREADS"] = str(args.max_threads)

    import engine
    import probe
    import server

    raw_cookie = os.getenv(args.cookie_env, "")
    if not raw_cookie.strip():
        raise SystemExit(f"Environment variable {args.cookie_env} is empty or not set.")

    artist_cache_key = (
        args.artist_cache_key
        if args.artist_cache_key is not None
        else os.getenv("ARTIST_CACHE_KEY", "")
    )

    with tempfile.TemporaryDirectory(prefix="netease-ci-secret-") as tmp_secret:
        cfg_path = _prepare_temp_config(
            Path(tmp_secret),
            raw_cookie=raw_cookie,
            threads=args.threads,
            recent_limit=args.recent_limit,
            owned_limit=args.owned_limit,
            new_album_probe_limit=args.new_album_probe_limit,
            artist_cache_key=artist_cache_key,
        )

        if args.probe_only:
            cfg = server.Config(cfg_path)
            report = probe.probe(config=cfg, artist_count=args.probe_artists, api_base=args.api_base)
            raw_norm = engine.normalize_cookie(raw_cookie)
            token_vals = [t for t in (raw_cookie.strip(), raw_norm, raw_norm.removeprefix("MUSIC_U=").rstrip(";")) if t]

            def _scrub_probe(text: str) -> str:
                for tok in sorted(token_vals, key=len, reverse=True):
                    text = text.replace(tok, "<redacted>")
                return engine.redacted_error_text(text)

            if report.get("error"):
                report["error"] = _scrub_probe(str(report["error"]))
            result_payload = {
                "run_id": None,
                "status": "success" if report.get("ok") else "failed",
                "skipped": False,
                "probe": report,
            }
            _write_json(args.result_json, result_payload)
            _write_text(args.summary_md, _scrub_probe(_build_probe_summary_md(report)))
            print(_scrub_probe(json.dumps(report, ensure_ascii=False, indent=2)), flush=True)
            return 0 if report.get("ok") else 1

        runtime = server.Runtime(config=server.Config(cfg_path), db_path=data_dir / "scan_history.sqlite3")
        try:
            return execute_scan(runtime, args, data_dir)
        finally:
            runtime.close()


if __name__ == "__main__":
    raise SystemExit(main())
