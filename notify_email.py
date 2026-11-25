"""Send SMTP summary emails (HTML digest + optional report/CSV attachments) for CI scans."""
from __future__ import annotations

import argparse
import csv
import html
import json
import os
import re
import smtplib
import sqlite3
import ssl
import sys
import time
from contextlib import closing
from datetime import datetime
from email.message import EmailMessage
from email.utils import formatdate
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Shanghai")

MAX_INLINE_ALBUMS = 200
MAX_INLINE_ARTISTS = 25
MAX_HTML_ATTACHMENT_BYTES = 15 * 1024 * 1024
MAX_TOTAL_ATTACHMENT_BYTES = 18 * 1024 * 1024

DOMAIN_SMTP_DEFAULTS: Dict[str, Tuple[str, int]] = {
    "gmail.com": ("smtp.gmail.com", 465),
    "googlemail.com": ("smtp.gmail.com", 465),
    "qq.com": ("smtp.qq.com", 465),
    "foxmail.com": ("smtp.qq.com", 465),
    "163.com": ("smtp.163.com", 465),
    "126.com": ("smtp.126.com", 465),
    "outlook.com": ("smtp-mail.outlook.com", 587),
    "hotmail.com": ("smtp-mail.outlook.com", 587),
    "live.com": ("smtp-mail.outlook.com", 587),
    "office365.com": ("smtp-mail.outlook.com", 587),
}


def infer_smtp_config(smtp_user: str, smtp_host: str = "", smtp_port: str = "") -> Tuple[str, int]:
    """Resolve SMTP host and port from explicit overrides or sender email domain."""
    host = (smtp_host or "").strip()
    port_raw = str(smtp_port or "").strip()

    if host:
        if port_raw:
            port = int(port_raw)
            if not 1 <= port <= 65535:
                raise ValueError(f"Invalid SMTP_PORT: {port_raw}")
            return host, port
        return host, 465

    user = (smtp_user or "").strip()
    if "@" not in user:
        raise ValueError("Unable to infer SMTP_HOST from SMTP_USER; please configure vars.SMTP_HOST")
    domain = user.rsplit("@", 1)[-1].lower().strip()
    if domain not in DOMAIN_SMTP_DEFAULTS:
        raise ValueError(f"Unsupported email domain '{domain}'; please configure vars.SMTP_HOST")
    default_host, default_port = DOMAIN_SMTP_DEFAULTS[domain]
    if port_raw:
        port = int(port_raw)
        if not 1 <= port <= 65535:
            raise ValueError(f"Invalid SMTP_PORT: {port_raw}")
        return default_host, port
    return default_host, default_port


def _env_flag(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    val = raw.strip().lower()
    if val in {"0", "false", "no", "off", "disabled"}:
        return False
    if val in {"1", "true", "yes", "on", "enabled"}:
        return True
    return default


def _parse_recipients(raw_mail_to: str, fallback_sender: str) -> List[str]:
    parts = [p.strip() for p in re.split(r"[;,]+", raw_mail_to or "") if p.strip()]
    if parts:
        return list(dict.fromkeys(parts))
    sender = (fallback_sender or "").strip()
    return [sender] if sender else []


def _scrub_text(text: Any, secret_tokens: Iterable[str] = ()) -> str:
    val = str(text or "")
    tokens = [str(t) for t in secret_tokens if t and str(t).strip()]
    for tok in sorted(set(tokens), key=len, reverse=True):
        val = val.replace(tok, "<redacted>")
    val = re.sub(r"MUSIC_U=[^;\s\"']+", "MUSIC_U=<redacted>", val)
    try:
        import engine

        val = engine.redacted_error_text(val)
    except Exception:
        pass
    return val


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
        key = album_id if album_id else f"name:{album_name.casefold()}"
        if key not in groups:
            groups[key] = {
                "album_key": key,
                "album_id": album_id,
                "album_name": album_name or "未知专辑",
                "heat": 0,
                "latest_date": str(hit.get("date_str") or ""),
                "tracks": [],
                "artists": [],
            }
        group = groups[key]
        group["heat"] = max(group["heat"], int(hit.get("album_count") or 0))
        date_str = str(hit.get("date_str") or "")
        if date_str > group["latest_date"]:
            group["latest_date"] = date_str
        group["tracks"].append(hit)

    for group in groups.values():
        tracks = group["tracks"]
        real_tracks = [t for t in tracks if not _is_album_placeholder(t)]
        if real_tracks:
            tracks = real_tracks
            group["tracks"] = real_tracks
        artists: List[str] = []
        seen_artists = set()
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


def _read_csv_hits(csv_path: Optional[Path]) -> List[Dict[str, Any]]:
    if not csv_path or not csv_path.is_file():
        return []
    hits: List[Dict[str, Any]] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        next(reader, None)
        for row in reader:
            if len(row) < 6:
                continue
            link = row[4]
            album_match = re.search(r"/album\?id=(\d+)", link or "")
            album_count = 0
            if len(row) >= 7 and str(row[6]).strip().isdigit():
                album_count = int(str(row[6]).strip())
            hits.append(
                {
                    "name": row[0],
                    "song_name": row[1],
                    "album_name": row[2],
                    "date_str": row[3],
                    "link": link,
                    "flag": int(row[5]) if str(row[5]).strip().isdigit() else 0,
                    "album_id": album_match.group(1) if album_match else "",
                    "album_count": album_count,
                }
            )
    return hits


def _csv_has_data_rows(csv_path: Optional[Path]) -> bool:
    if not csv_path or not csv_path.is_file():
        return False
    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            next(reader, None)
            return next(reader, None) is not None
    except Exception:
        return False


def _load_hits_and_run_meta(
    data_dir: Optional[Path],
    result: Dict[str, Any],
    run_csv: Optional[Path],
    today_csv: Optional[Path],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    run_meta: Dict[str, Any] = {}
    run_hits: List[Dict[str, Any]] = []
    day_hits: List[Dict[str, Any]] = []

    db_file = (data_dir / "scan_history.sqlite3") if data_dir else None
    run_id = result.get("run_id")
    today_str = str(result.get("date") or "")

    if db_file and db_file.is_file():
        try:
            with closing(sqlite3.connect(f"file:{db_file}?mode=ro", uri=True, timeout=15)) as db:
                db.row_factory = sqlite3.Row
                if run_id:
                    row = db.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
                    if row is not None:
                        run_meta = dict(row)
                        for key in ("params", "summary", "metrics"):
                            try:
                                run_meta[key] = json.loads(run_meta.get(key) or "{}")
                            except Exception:
                                run_meta[key] = {}
                    rows = db.execute(
                        "SELECT payload FROM hits WHERE run_id=? ORDER BY "
                        "COALESCE(CAST(json_extract(payload,'$.album_count') AS INTEGER),0) DESC, "
                        "json_extract(payload,'$.date_str') DESC, link",
                        (run_id,),
                    ).fetchall()
                    run_hits = [json.loads(r[0]) for r in rows]
                if today_str:
                    rows = db.execute(
                        "SELECT h.payload FROM hits h JOIN runs r ON h.run_id=r.id "
                        "WHERE r.schedule_date=? OR (r.source='manual' AND substr(r.created_at,1,10)=?) "
                        "ORDER BY COALESCE(CAST(json_extract(h.payload,'$.album_count') AS INTEGER),0) DESC, "
                        "json_extract(h.payload,'$.date_str') DESC, r.created_at DESC, h.link",
                        (today_str, today_str),
                    ).fetchall()
                    day_hits = [json.loads(r[0]) for r in rows]
        except Exception:
            pass

    if not run_hits and run_csv:
        run_hits = _read_csv_hits(run_csv)
    if not day_hits and today_csv:
        day_hits = _read_csv_hits(today_csv)

    return run_meta, run_hits, day_hits


def build_email_message(
    *,
    sender: str,
    recipients: Sequence[str],
    result: Dict[str, Any],
    run_meta: Dict[str, Any],
    run_hits: List[Dict[str, Any]],
    day_hits: List[Dict[str, Any]],
    run_url: str = "",
    standalone_html: Optional[Path] = None,
    run_csv: Optional[Path] = None,
    today_csv: Optional[Path] = None,
    attach_html: bool = True,
    attach_csv: bool = True,
    secret_tokens: Iterable[str] = (),
) -> EmailMessage:
    status = str(result.get("status") or "unknown")
    dry_run = bool(result.get("dry_run"))
    today_str = str(result.get("date") or datetime.now(TZ).date().isoformat())
    source = str(result.get("source") or "manual")
    slot = str(result.get("schedule_slot") or ("手动" if source == "manual" else source))
    warning = _scrub_text(result.get("warning") or run_meta.get("warning") or "", secret_tokens).strip()
    error = _scrub_text(result.get("error") or run_meta.get("error") or "", secret_tokens).strip()

    params = run_meta.get("params") or {}
    summary = run_meta.get("summary") or {}
    metrics = run_meta.get("metrics") or {}
    elapsed_sec = result.get("elapsed_sec")
    if elapsed_sec is None:
        elapsed_sec = metrics.get("elapsed_sec") or 0
    elapsed_min = round(float(elapsed_sec or 0) / 60.0, 1)

    result_count = int(result.get("result_count") if result.get("result_count") is not None else len(run_hits))
    today_total = int(result.get("today_total") if result.get("today_total") is not None else len(day_hits))
    csv_skipped = int(metrics.get("csv_skipped") or 0)

    display_hits = run_hits if run_hits else day_hits
    albums = _group_albums(display_hits)
    run_albums = _group_albums(run_hits) if run_hits else []
    shown_albums = albums[:MAX_INLINE_ALBUMS]

    prefix = "[🧪 试跑·网易云新曲]" if dry_run else "[网易云新曲]"
    if status in {"failed", "interrupted", "unknown"}:
        alert_tag = " · ❌ 失败"
    elif status == "partial" or warning or error:
        alert_tag = " · ⚠️ 告警"
    else:
        alert_tag = ""
    if result_count > 0:
        count_summary = f"新增 {result_count} 条（{len(run_albums)} 张专辑）"
    else:
        count_summary = f"无新增（今日累计 {today_total} 条）"
    subject = f"{prefix}{alert_tag} {today_str} ({slot}) {count_summary}"

    status_badges = {
        "success": "✅ 成功 (success)",
        "partial": "⚠️ 部分完成 (partial)",
        "failed": "❌ 失败 (failed)",
        "stopped": "⏹️ 已停止 (stopped)",
        "interrupted": "⚠️ 已中断 (interrupted)",
    }
    status_label = status_badges.get(status, status)

    scan_mode = str(params.get("scanMode") or "auto")
    threads = str(params.get("threads") or "—")
    ext_win = (
        f"{params.get('externalStartDate', '')} ~ {params.get('externalEndDate', '')}"
        if params.get("externalStartDate")
        else "—"
    )
    new_win = (
        f"{params.get('newStartDate', '')} ~ {params.get('newEndDate', '')}"
        if params.get("newStartDate")
        else "—"
    )
    failure_line = (
        f"艺人 {summary.get('artist_failures', 0)} · "
        f"探测 {summary.get('new_album_probe_failures', 0)} · "
        f"专辑 {summary.get('album_errors', 0)} · "
        f"归属 {summary.get('owned_errors', 0)} · "
        f"异常 {summary.get('task_errors', 0)}"
    )

    # Plain-text body
    plain_lines = [
        f"{'🧪 网易云新曲试跑简报' if dry_run else '网易云新曲每日简报'} · {today_str} ({slot})",
        f"状态：{status_label}",
        f"耗时：{elapsed_min} 分钟 | 模式：{scan_mode} | 线程：{threads}",
        f"本次新增：{result_count} 条（CSV 去重 {csv_skipped} 条） | 今日累计：{today_total} 条",
        f"覆盖艺人 / 候选专辑：{summary.get('artists', 0)} 位 / {summary.get('unique_albums', 0)} 张",
        f"失败计数：{failure_line}",
    ]
    if error:
        plain_lines.extend(["", f"错误：{error}"])
    if warning:
        plain_lines.extend(["", f"告警：{warning}"])
    if run_url:
        plain_lines.extend(["", f"Actions 运行详情：{run_url}"])
    if shown_albums:
        list_label = f"本次新增专辑（{len(albums)} 张）" if run_hits else f"今日累计新增专辑（{len(albums)} 张）"
        plain_lines.extend(["", f"--- {list_label} ---"])
        for album in shown_albums:
            heat = album["heat"]
            heat_str = f"🔥{heat}" if heat >= 3 else (f"[{heat}]" if heat > 0 else "[-]")
            artists = album.get("artists") or []
            artist_text = "、".join(artists[:MAX_INLINE_ARTISTS])
            if len(artists) > MAX_INLINE_ARTISTS:
                artist_text += f" 等共 {len(artists)} 位"
            url = f"https://music.163.com/#/album?id={album['album_id']}" if re.fullmatch(r"\d+", album["album_id"]) else ""
            plain_lines.append(f"{heat_str} 《{album['album_name']}》 ({album['latest_date']}) - {artist_text or '—'} {url}".strip())
        if len(albums) > len(shown_albums):
            plain_lines.append(f"... 正文仅展示前 {len(shown_albums)} 张专辑，完整结果请查看附件 HTML 或 CSV。")
    else:
        plain_lines.extend(["", "本次扫描及今日暂无新增曲目。"])
    plain_text = "\n".join(plain_lines) + "\n"

    # HTML body
    alert_blocks: List[str] = []
    if dry_run:
        alert_blocks.append(
            '<div style="margin:0 0 14px;padding:10px 14px;border-radius:8px;background:#eef6ff;border:1px solid #bfdbfe;color:#1e40af;font-size:13px;">'
            "🧪 <strong>隔离试跑模式</strong>：结果仅用于验证，未写回生产 Release 与静态站点。"
            "</div>"
        )
    if error:
        alert_blocks.append(
            '<div style="margin:0 0 14px;padding:10px 14px;border-radius:8px;background:#fef2f2;border:1px solid #fecaca;color:#991b1b;font-size:13px;word-break:break-word;">'
            f"❌ <strong>错误：</strong>{html.escape(error)}"
            "</div>"
        )
    if warning:
        alert_blocks.append(
            '<div style="margin:0 0 14px;padding:10px 14px;border-radius:8px;background:#fffbeb;border:1px solid #fde68a;color:#92400e;font-size:13px;word-break:break-word;">'
            f"⚠️ <strong>告警：</strong>{html.escape(warning)}"
            "</div>"
        )

    album_rows_html: List[str] = []
    for album in shown_albums:
        heat = album["heat"]
        if heat >= 3:
            heat_cell = f'<span style="display:inline-block;padding:2px 8px;border-radius:999px;background:#fef2f2;color:#b91c1c;font-weight:700;font-size:12px;">🔥 {heat}</span>'
        elif heat > 0:
            heat_cell = f'<span style="color:#475569;font-weight:600;font-size:12px;">{heat}</span>'
        else:
            heat_cell = '<span style="color:#94a3b8;">—</span>'

        title_esc = html.escape(album["album_name"])
        if re.fullmatch(r"\d+", album["album_id"]):
            title_cell = (
                f'<a href="https://music.163.com/#/album?id={album["album_id"]}" '
                'style="color:#1d4ed8;text-decoration:none;font-weight:600;" target="_blank">'
                f"{title_esc}</a>"
            )
        else:
            title_cell = f'<span style="font-weight:600;">{title_esc}</span>'

        artists = album.get("artists") or []
        if len(artists) > MAX_INLINE_ARTISTS:
            artists_text = "、".join(html.escape(a) for a in artists[:MAX_INLINE_ARTISTS]) + f" 等共 {len(artists)} 位"
        else:
            artists_text = "、".join(html.escape(a) for a in artists) or "—"

        album_rows_html.append(
            "<tr>"
            f'<td style="padding:8px 10px;border-bottom:1px solid #e2e8f0;text-align:center;white-space:nowrap;">{heat_cell}</td>'
            f'<td style="padding:8px 10px;border-bottom:1px solid #e2e8f0;line-height:1.4;">{title_cell}</td>'
            f'<td style="padding:8px 10px;border-bottom:1px solid #e2e8f0;white-space:nowrap;color:#475569;font-family:monospace;font-size:12px;">{html.escape(album["latest_date"])}</td>'
            f'<td style="padding:8px 10px;border-bottom:1px solid #e2e8f0;color:#334155;font-size:13px;line-height:1.45;">{artists_text}</td>'
            "</tr>"
        )

    if shown_albums:
        list_heading = (
            f"本次新增专辑（共 {len(albums)} 张）"
            if run_hits
            else f"今日累计新增专辑（共 {len(albums)} 张）"
        )
        trunc_note = ""
        if len(albums) > len(shown_albums):
            trunc_note = (
                f'<p style="margin:10px 0 0;color:#64748b;font-size:12px;">'
                f"ℹ️ 正文仅展示前 {len(shown_albums)} 张专辑以避免邮件被截断，完整结果请打开附件离线报告或 CSV。"
                "</p>"
            )
        albums_section = (
            f'<h3 style="margin:20px 0 10px;font-size:15px;color:#0f172a;">{html.escape(list_heading)}</h3>'
            '<div style="overflow-x:auto;border:1px solid #e2e8f0;border-radius:8px;">'
            '<table style="width:100%;border-collapse:collapse;background:#ffffff;font-size:13px;">'
            '<thead><tr style="background:#f8fafc;color:#475569;text-align:left;font-size:12px;">'
            '<th style="padding:8px 10px;border-bottom:1px solid #e2e8f0;text-align:center;width:58px;">热度</th>'
            '<th style="padding:8px 10px;border-bottom:1px solid #e2e8f0;">专辑 / 企划</th>'
            '<th style="padding:8px 10px;border-bottom:1px solid #e2e8f0;width:96px;">发行日期</th>'
            '<th style="padding:8px 10px;border-bottom:1px solid #e2e8f0;">命中艺人</th>'
            "</tr></thead>"
            f"<tbody>{''.join(album_rows_html)}</tbody>"
            "</table></div>"
            f"{trunc_note}"
        )
    else:
        albums_section = '<p style="margin:16px 0;color:#64748b;font-size:14px;">本次扫描及今日暂无新增曲目。</p>'

    endpoints = metrics.get("endpoints") or {}
    album_ep = endpoints.get("/album") or {}
    perf_bits: List[str] = []
    if metrics.get("album_cache_hit") is not None or metrics.get("album_cache_miss") is not None:
        perf_bits.append(
            f"专辑缓存命中 {int(metrics.get('album_cache_hit') or 0)} / 未命中 {int(metrics.get('album_cache_miss') or 0)}"
        )
    if album_ep:
        perf_bits.append(
            f"/album 请求 {int(album_ep.get('count') or 0)}（405: {int(album_ep.get('status_405') or 0)}）"
        )
    perf_footer = (
        f'<p style="margin:12px 0 0;color:#64748b;font-size:12px;">性能概况：{" · ".join(perf_bits)}</p>'
        if perf_bits
        else ""
    )

    run_link_html = (
        f'<a href="{html.escape(run_url)}" style="display:inline-block;margin-top:12px;padding:7px 14px;border-radius:6px;background:#0f172a;color:#ffffff;text-decoration:none;font-size:12px;font-weight:600;" target="_blank">查看 GitHub Actions 运行详情</a>'
        if run_url
        else ""
    )

    html_body = (
        '<!doctype html><html><body style="margin:0;padding:16px;background:#f1f5f9;font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',\'PingFang SC\',\'Microsoft YaHei\',sans-serif;color:#0f172a;">'
        '<div style="max-width:860px;margin:0 auto;background:#ffffff;border-radius:12px;padding:20px 22px;border:1px solid #e2e8f0;">'
        f'<h2 style="margin:0 0 12px;font-size:18px;color:#0f172a;">{"🧪 网易云新曲试跑简报" if dry_run else "网易云新曲扫描简报"} · {html.escape(today_str)} <span style="font-size:13px;font-weight:500;color:#64748b;">({html.escape(slot)})</span></h2>'
        f"{''.join(alert_blocks)}"
        '<table style="width:100%;border-collapse:collapse;font-size:13px;background:#f8fafc;border-radius:8px;overflow:hidden;border:1px solid #e2e8f0;">'
        f'<tr><td style="padding:7px 12px;border-bottom:1px solid #e2e8f0;color:#475569;width:140px;">状态</td><td style="padding:7px 12px;border-bottom:1px solid #e2e8f0;font-weight:600;">{html.escape(status_label)}</td></tr>'
        f'<tr><td style="padding:7px 12px;border-bottom:1px solid #e2e8f0;color:#475569;">本次新增 / 今日累计</td><td style="padding:7px 12px;border-bottom:1px solid #e2e8f0;"><strong>{result_count}</strong> 条（CSV 去重 {csv_skipped} 条） / 今日累计 <strong>{today_total}</strong> 条</td></tr>'
        f'<tr><td style="padding:7px 12px;border-bottom:1px solid #e2e8f0;color:#475569;">模式 / 线程 / 耗时</td><td style="padding:7px 12px;border-bottom:1px solid #e2e8f0;"><code>{html.escape(scan_mode)}</code> · {html.escape(threads)} 线程 · {elapsed_min} 分钟</td></tr>'
        f'<tr><td style="padding:7px 12px;border-bottom:1px solid #e2e8f0;color:#475569;">外部 / 新专窗口</td><td style="padding:7px 12px;border-bottom:1px solid #e2e8f0;"><code>{html.escape(ext_win)}</code> / <code>{html.escape(new_win)}</code></td></tr>'
        f'<tr><td style="padding:7px 12px;border-bottom:1px solid #e2e8f0;color:#475569;">覆蓋艺人 / 专辑</td><td style="padding:7px 12px;border-bottom:1px solid #e2e8f0;">{int(summary.get("artists", 0))} 位 / {int(summary.get("unique_albums", 0))} 张</td></tr>'
        f'<tr><td style="padding:7px 12px;color:#475569;">失败计数</td><td style="padding:7px 12px;">{html.escape(failure_line)}</td></tr>'
        "</table>"
        f"{run_link_html}"
        f"{albums_section}"
        f"{perf_footer}"
        "</div></body></html>"
    )

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg["Date"] = formatdate(localtime=True)
    msg.set_content(plain_text, subtype="plain", charset="utf-8")
    msg.add_alternative(html_body, subtype="html", charset="utf-8")

    safe_date = re.sub(r"[^0-9A-Za-z_-]+", "-", today_str) or "scan"
    safe_slot = re.sub(r"[^0-9A-Za-z_-]+", "", slot.replace(":", "")) or "run"
    attached_bytes = 0

    if attach_html and standalone_html and standalone_html.is_file():
        size = standalone_html.stat().st_size
        if size <= MAX_HTML_ATTACHMENT_BYTES and attached_bytes + size <= MAX_TOTAL_ATTACHMENT_BYTES:
            msg.add_attachment(
                standalone_html.read_bytes(),
                maintype="text",
                subtype="html",
                filename=f"netease-report-{safe_date}.html",
            )
            attached_bytes += size

    if attach_csv and today_csv and _csv_has_data_rows(today_csv):
        size = today_csv.stat().st_size
        if attached_bytes + size <= MAX_TOTAL_ATTACHMENT_BYTES:
            msg.add_attachment(
                today_csv.read_bytes(),
                maintype="text",
                subtype="csv",
                filename=f"netease-today-{safe_date}.csv",
            )
            attached_bytes += size

    if attach_csv and run_csv and result_count > 0 and result_count != today_total and _csv_has_data_rows(run_csv):
        size = run_csv.stat().st_size
        if attached_bytes + size <= MAX_TOTAL_ATTACHMENT_BYTES:
            msg.add_attachment(
                run_csv.read_bytes(),
                maintype="text",
                subtype="csv",
                filename=f"netease-run-{safe_date}-{safe_slot}.csv",
            )
            attached_bytes += size

    return msg


def send_email(msg: EmailMessage, smtp_host: str, smtp_port: int, smtp_user: str, smtp_pass: str) -> None:
    """Deliver EmailMessage over SSL (port 465) or STARTTLS."""
    ctx = ssl.create_default_context()
    if int(smtp_port) == 465:
        with smtplib.SMTP_SSL(smtp_host, int(smtp_port), timeout=30, context=ctx) as server:
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
    else:
        with smtplib.SMTP(smtp_host, int(smtp_port), timeout=30) as server:
            server.ehlo()
            server.starttls(context=ctx)
            server.ehlo()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send Netease scanner summary email via SMTP.")
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--result-json", type=Path, required=True)
    parser.add_argument("--run-csv", type=Path, default=None)
    parser.add_argument("--today-csv", type=Path, default=None)
    parser.add_argument("--standalone-html", type=Path, default=None)
    parser.add_argument("--run-url", default="")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)

    smtp_user = os.getenv("SMTP_USER", "").strip()
    smtp_pass = os.getenv("SMTP_PASS", "").strip()
    if not smtp_user or not smtp_pass:
        print("[notify_email] SMTP_USER or SMTP_PASS is not set; skipping email notification.", flush=True)
        return 0

    recipients = _parse_recipients(os.getenv("MAIL_TO", ""), smtp_user)
    if not recipients:
        print("[notify_email] No email recipients configured; skipping email notification.", flush=True)
        return 0

    if args.result_json.is_file():
        result = json.loads(args.result_json.read_text(encoding="utf-8"))
    else:
        result = {
            "run_id": None,
            "status": "failed",
            "skipped": False,
            "dry_run": False,
            "date": datetime.now(TZ).date().isoformat(),
            "source": "scheduled",
            "schedule_slot": "",
            "result_count": 0,
            "today_total": 0,
            "elapsed_sec": 0,
            "warning": "",
            "error": "Scan finished without generating run_result.json.",
        }

    if result.get("skipped"):
        print("[notify_email] Scan slot was skipped; skipping email notification.", flush=True)
        return 0

    dry_run = bool(result.get("dry_run"))
    if dry_run and not _env_flag("MAIL_ON_DRY_RUN", default=True):
        print("[notify_email] Dry-run scan and MAIL_ON_DRY_RUN=0; skipping email notification.", flush=True)
        return 0

    status = str(result.get("status") or "unknown")
    result_count = int(result.get("result_count") or 0)
    has_alert = bool(
        status != "success"
        or str(result.get("warning") or "").strip()
        or str(result.get("error") or "").strip()
    )
    if not _env_flag("MAIL_SEND_EMPTY", default=True) and result_count == 0 and not has_alert:
        print("[notify_email] MAIL_SEND_EMPTY=0 with 0 new hits and no alert; skipping email notification.", flush=True)
        return 0

    try:
        smtp_host, smtp_port = infer_smtp_config(
            smtp_user,
            os.getenv("SMTP_HOST", ""),
            os.getenv("SMTP_PORT", ""),
        )
        run_meta, run_hits, day_hits = _load_hits_and_run_meta(
            args.data_dir,
            result,
            args.run_csv,
            args.today_csv,
        )
        msg = build_email_message(
            sender=smtp_user,
            recipients=recipients,
            result=result,
            run_meta=run_meta,
            run_hits=run_hits,
            day_hits=day_hits,
            run_url=args.run_url.strip(),
            standalone_html=args.standalone_html,
            run_csv=args.run_csv,
            today_csv=args.today_csv,
            attach_html=_env_flag("MAIL_ATTACH_HTML", default=True),
            attach_csv=_env_flag("MAIL_ATTACH_CSV", default=True),
            secret_tokens=(smtp_pass,),
        )
        t0 = time.monotonic()
        send_email(msg, smtp_host, smtp_port, smtp_user, smtp_pass)
        elapsed = round(time.monotonic() - t0, 2)
        print(
            f"[notify_email] Email sent via {smtp_host}:{smtp_port} to {len(recipients)} recipient(s) in {elapsed}s.",
            flush=True,
        )
        return 0
    except Exception as exc:
        safe_err = _scrub_text(f"{type(exc).__name__}: {exc}", (smtp_pass,))
        print(f"[notify_email] Warning: failed to send email ({safe_err})", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
