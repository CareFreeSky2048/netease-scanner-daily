"""Generate a static web viewer (and optional single-file offline HTML) from scanner data."""
from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Shanghai")
SENSITIVE_RE = re.compile(r"(MUSIC_U\s*=\s*)[^;\s&\"']+", re.IGNORECASE)


def _scrub_text(value: Any) -> str:
    if value is None:
        return ""
    return SENSITIVE_RE.sub(r"\1<redacted>", str(value))


def _album_key(link: str, payload: Dict[str, Any]) -> str:
    album_id = str(payload.get("album_id") or "").strip()
    if album_id:
        return album_id
    album_name = str(payload.get("album_name") or "").strip()
    if album_name:
        return f"name:{album_name}"
    return f"link:{link}"


def _is_album_placeholder(hit: Dict[str, Any]) -> bool:
    return (
        str(hit.get("song_name") or "").startswith("【新专辑】")
        and "/album?id=" in str(hit.get("link") or "")
    )


def _finalize_album_group(group: Dict[str, Any]) -> None:
    tracks = group["tracks"]
    if any(not _is_album_placeholder(t) for t in tracks):
        tracks = [t for t in tracks if not _is_album_placeholder(t)]
        group["tracks"] = tracks
        group["track_count"] = len(tracks)
    # Match Store.album_tracks: date_str DESC, name ASC, song_name NOCASE ASC, link ASC
    tracks.sort(key=lambda t: t["link"])
    tracks.sort(key=lambda t: t["song_name"].casefold())
    tracks.sort(key=lambda t: t["name"])
    tracks.sort(key=lambda t: t["date_str"], reverse=True)
    seen_artists: set[str] = set()
    artists: List[str] = []
    for t in tracks:
        name = str(t.get("name") or "").strip()
        if name and name not in seen_artists:
            seen_artists.add(name)
            artists.append(name)
    group["artists"] = artists


def _group_hits_to_albums(rows: List[Tuple[str, Dict[str, Any]]]) -> List[Dict[str, Any]]:
    groups: Dict[str, Dict[str, Any]] = {}
    for link, payload in rows:
        key = _album_key(link, payload)
        album_name = str(payload.get("album_name") or "")
        album_id = str(payload.get("album_id") or "")
        date_str = str(payload.get("date_str") or "")
        raw_heat = payload.get("album_count")
        heat: Optional[int]
        try:
            heat = int(raw_heat) if raw_heat is not None and raw_heat != "" else None
        except (TypeError, ValueError):
            heat = None

        group = groups.get(key)
        if group is None:
            group = {
                "album_key": key,
                "album_name": album_name,
                "album_id": album_id,
                "heat": heat,
                "latest_date": date_str,
                "track_count": 0,
                "tracks": [],
            }
            groups[key] = group
        else:
            if album_name > group["album_name"]:
                group["album_name"] = album_name
            if album_id > group["album_id"]:
                group["album_id"] = album_id
            if heat is not None and (group["heat"] is None or heat > group["heat"]):
                group["heat"] = heat
            if date_str > group["latest_date"]:
                group["latest_date"] = date_str

        group["track_count"] += 1
        group["tracks"].append(
            {
                "name": str(payload.get("name") or ""),
                "song_name": str(payload.get("song_name") or ""),
                "album_name": album_name,
                "date_str": date_str,
                "link": str(payload.get("link") or link),
                "flag": payload.get("flag", 0),
                "album_id": album_id,
                "album_count": heat,
            }
        )

    for group in groups.values():
        _finalize_album_group(group)

    ordered = list(groups.values())
    ordered.sort(key=lambda g: g["album_key"])
    ordered.sort(key=lambda g: (g["album_name"] or "").casefold())
    ordered.sort(key=lambda g: g["latest_date"], reverse=True)
    ordered.sort(key=lambda g: g["heat"] or 0, reverse=True)
    return ordered


def _load_sqlite_data(
    db_path: Path,
) -> Tuple[List[Dict[str, Any]], Dict[str, List[Tuple[str, Dict[str, Any]]]], Dict[str, List[Dict[str, Any]]], set[str]]:
    if not db_path.is_file():
        return [], {}, {}, set()

    with closing(sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30)) as db:
        db.row_factory = sqlite3.Row
        run_rows = db.execute(
            "SELECT * FROM runs ORDER BY created_at DESC, rowid DESC"
        ).fetchall()
        hits_rows = db.execute("SELECT run_id, link, payload FROM hits").fetchall()
        log_rows = db.execute(
            "SELECT run_id, created_at, level, text FROM logs ORDER BY seq ASC"
        ).fetchall()
        m3_keys = {
            row[0][len("m3_match_report:") :]
            for row in db.execute(
                "SELECT key FROM settings WHERE key LIKE 'm3_match_report:%'"
            ).fetchall()
        }

    hits_by_run: Dict[str, List[Tuple[str, Dict[str, Any]]]] = {}
    for row in hits_rows:
        payload = json.loads(row["payload"])
        hits_by_run.setdefault(row["run_id"], []).append((row["link"], payload))

    logs_by_run: Dict[str, List[Dict[str, Any]]] = {}
    for row in log_rows:
        bucket = logs_by_run.setdefault(row["run_id"], [])
        bucket.append(
            {
                "created_at": row["created_at"],
                "level": row["level"],
                "text": _scrub_text(row["text"]),
            }
        )
    for rid, items in logs_by_run.items():
        if len(items) > 100:
            logs_by_run[rid] = items[-100:]

    runs: List[Dict[str, Any]] = []
    for row in run_rows:
        params = json.loads(row["params"]) if row["params"] else {}
        summary = json.loads(row["summary"]) if row["summary"] else {}
        metrics = json.loads(row["metrics"]) if row["metrics"] else {}
        run_id = row["id"]
        run_hits = hits_by_run.get(run_id, [])
        runs.append(
            {
                "id": run_id,
                "source": row["source"],
                "schedule_date": row["schedule_date"],
                "schedule_time": row["schedule_time"],
                "status": row["status"],
                "created_at": row["created_at"],
                "started_at": row["started_at"],
                "ended_at": row["ended_at"],
                "params": params,
                "summary": {
                    k: summary[k]
                    for k in (
                        "artists",
                        "unique_albums",
                        "date_hit_albums",
                        "verify_artists",
                        "artist_failures",
                        "new_album_probe_failures",
                        "album_errors",
                        "owned_errors",
                        "task_errors",
                        "m3_round_errors",
                        "slow_retry",
                        "m3_scan",
                    )
                    if k in summary
                },
                "metrics": {
                    k: metrics[k]
                    for k in ("elapsed_sec", "results", "csv_skipped", "artist_total")
                    if k in metrics
                },
                "error": _scrub_text(row["error"]),
                "warning": _scrub_text(row["warning"]),
                "result_count": len(run_hits),
                "has_m3_report": run_id in m3_keys,
            }
        )

    return runs, hits_by_run, logs_by_run, m3_keys


def _load_legacy_csv(csv_path: Path, max_albums: int = 500) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    stats = {"total_tracks": 0, "total_albums": 0, "latest_date": ""}
    if not csv_path.is_file():
        return stats, {"stats": stats, "albums": []}

    groups: Dict[str, Dict[str, Any]] = {}
    total_tracks = 0
    latest_overall = ""
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        next(reader, None)
        for row in reader:
            if len(row) < 6:
                continue
            total_tracks += 1
            name, song_name, album_name, date_str, link, flag = row[:6]
            if date_str > latest_overall:
                latest_overall = date_str
            key = f"name:{album_name}" if album_name else f"link:{link}"
            group = groups.get(key)
            if group is None:
                group = {
                    "album_key": key,
                    "album_name": album_name,
                    "album_id": "",
                    "heat": None,
                    "latest_date": date_str,
                    "track_count": 0,
                    "tracks": [],
                }
                groups[key] = group
            elif date_str > group["latest_date"]:
                group["latest_date"] = date_str
            group["track_count"] += 1
            group["tracks"].append(
                {
                    "name": name,
                    "song_name": song_name,
                    "album_name": album_name,
                    "date_str": date_str,
                    "link": link,
                    "flag": flag,
                    "album_id": "",
                    "album_count": None,
                }
            )

    stats = {
        "total_tracks": total_tracks,
        "total_albums": len(groups),
        "latest_date": latest_overall,
    }
    ordered = list(groups.values())
    ordered.sort(key=lambda g: (g["album_name"] or "").casefold())
    ordered.sort(key=lambda g: g["latest_date"], reverse=True)
    top_albums = ordered[:max_albums]
    for g in top_albums:
        _finalize_album_group(g)
    return stats, {"stats": stats, "albums": top_albums}


def _render_html(embedded_payload: Optional[Dict[str, Any]] = None) -> str:
    embedded_block = ""
    if embedded_payload is not None:
        raw_json = json.dumps(embedded_payload, ensure_ascii=False, separators=(",", ":"))
        safe_json = raw_json.replace("<", "\\u003c")
        embedded_block = f'<script type="application/json" id="embedded-data">{safe_json}</script>\n'

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>网易云扫描记录 · 静态查看页</title>
<style>
:root{{font-family:system-ui,-apple-system,"Microsoft YaHei",sans-serif;color:#172332;background:#f3f5f7;font-size:15px}}
*{{box-sizing:border-box}}body{{margin:0}}
main{{max-width:1360px;margin:auto;padding:28px 20px}}
header{{display:flex;justify-content:space-between;gap:20px;align-items:start;margin-bottom:20px;flex-wrap:wrap}}
h1{{font-size:26px;letter-spacing:-.5px;margin:0 0 6px}}
h2{{font-size:17px;margin:0 0 14px}}
p{{margin:6px 0;line-height:1.65}}
.muted{{color:#677487;font-size:13px}}
.card{{background:#fff;border:1px solid #dde3e9;border-radius:12px;padding:20px;margin:14px 0}}
.tag{{font-size:12px;border:1px solid #cbd8df;background:#edf4f5;color:#31546b;padding:5px 11px;border-radius:20px;white-space:nowrap}}
.tabs{{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px}}
button,.button{{font:inherit;display:inline-flex;align-items:center;gap:6px;padding:9px 15px;border-radius:7px;border:1px solid #ccd4de;background:#fff;color:#263548;cursor:pointer;text-decoration:none}}
button.active,button.primary{{background:#183f50;color:#fff;border-color:#183f50}}
button:disabled{{opacity:.45;cursor:default}}
input,select{{font:inherit;border:1px solid #ccd4de;border-radius:6px;padding:8px 10px;background:#fff;color:#172332;min-width:0}}
.filters{{display:flex;gap:12px;flex-wrap:wrap;align-items:center;justify-content:space-between;margin-bottom:14px}}
.filter-group{{display:flex;gap:10px;flex-wrap:wrap;align-items:center;flex:1 1 320px}}
.filter-group input[type=search]{{flex:1 1 220px;max-width:380px}}
.check-label{{display:inline-flex;align-items:center;gap:6px;font-size:13px;color:#3d4d61;cursor:pointer;user-select:none}}
.layout{{display:grid;grid-template-columns:310px minmax(0,1fr);gap:20px}}
.layout.single{{grid-template-columns:minmax(0,1fr)}}
.run-list{{max-height:640px;overflow:auto;padding-right:2px}}
.run{{display:block;text-align:left;width:100%;border:1px solid #e1e6ec;border-radius:8px;margin-bottom:8px;padding:11px 12px;font-size:13px;line-height:1.55;background:#fff}}
.run.selected{{background:#eff5f7;border-color:#719bac}}
.run small{{display:block;color:#677487;margin-top:2px}}
.notice{{padding:11px 14px;border-radius:7px;background:#fff2db;color:#795718;line-height:1.6;white-space:pre-line;margin:10px 0}}
.notice:empty{{display:none}}
.toolbar{{display:flex;justify-content:space-between;gap:12px;align-items:start;flex-wrap:wrap}}
.album-list{{border-top:1px solid #e3e8ed;margin-top:10px}}
.album{{margin:0;border-bottom:1px solid #e3e8ed}}
.album summary{{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:10px;align-items:center;padding:14px 8px;list-style:none;color:#172332;cursor:pointer}}
.album summary::-webkit-details-marker{{display:none}}
.album summary::before{{content:'+';grid-column:2;grid-row:1;color:#557181;font-size:18px;text-align:center;width:20px}}
.album[open] summary::before{{content:'−'}}
.album-title{{grid-column:1;grid-row:1;font-weight:600;overflow-wrap:anywhere}}
.album-title a{{color:#215a71;text-decoration:underline;text-decoration-color:#a8c1cb;text-underline-offset:3px}}
.album-title a:hover{{color:#163f50;text-decoration-color:currentColor}}
.album-meta{{grid-column:1/-1;color:#677487;font-size:13px}}
.album-tracks{{padding:0 8px 12px;overflow-x:auto}}
table{{border-collapse:collapse;width:100%;font-size:13px}}
th,td{{padding:9px 7px;text-align:left;border-bottom:1px solid #e8ecf0;vertical-align:top;overflow-wrap:anywhere}}
th{{color:#637287;white-space:nowrap;font-weight:500}}
td a{{color:#215a71;white-space:nowrap}}
.pagination{{display:flex;justify-content:flex-end;gap:10px;align-items:center;margin-top:16px;flex-wrap:wrap}}
#logs{{max-height:240px;overflow:auto;white-space:pre-wrap;font-size:12px;color:#596679;line-height:1.75;background:#f7f9fb;padding:12px;border-radius:7px}}
.empty{{padding:20px 8px;color:#677487}}
@media(max-width:900px){{
  .layout{{grid-template-columns:1fr}}
  .run-list{{max-height:240px}}
  main{{padding:18px 12px}}
  .card{{padding:15px}}
}}
</style>
</head>
<body>
<main>
  <header>
    <div>
      <h1>网易云新曲扫描记录</h1>
      <p class="muted" id="subtitle">GitHub Actions 自动扫描 · 历史缓存与专辑热度静态查看</p>
    </div>
    <span class="tag" id="updatedTag">正在加载数据</span>
  </header>

  <section class="card">
    <div class="tabs" role="tablist">
      <button id="tabDay" class="active" role="tab">按日扫描结果</button>
      <button id="tabRuns" role="tab">单次任务历史</button>
      <button id="tabLegacy" role="tab">近期累计曲库</button>
    </div>

    <div class="filters">
      <div class="filter-group">
        <select id="daySelect" aria-label="选择日期"></select>
        <input id="searchBox" type="search" placeholder="搜索艺人、单曲或专辑名">
        <label class="check-label"><input id="hotOnly" type="checkbox"> 仅看高热度 (🔥 ≥ 3)</label>
      </div>
      <div>
        <button id="downloadCsv">下载当前 CSV</button>
      </div>
    </div>

    <div id="contentLayout" class="layout single">
      <aside id="runSidebar" hidden>
        <div class="run-list" id="runList"></div>
      </aside>
      <div>
        <div class="toolbar">
          <div>
            <h2 id="viewTitle">扫描结果</h2>
            <p id="viewSummary" class="muted"></p>
          </div>
        </div>
        <div id="viewWarning" class="notice"></div>
        <div class="album-list" id="albumList"></div>
        <div class="pagination">
          <button id="prevPage">上一页</button>
          <span id="pageInfo" class="muted"></span>
          <button id="nextPage">下一页</button>
        </div>
        <details id="logsBox" hidden style="margin-top:16px">
          <summary style="cursor:pointer;color:#596679">本次扫描日志</summary>
          <div id="logs"></div>
        </details>
      </div>
    </div>
  </section>
</main>
{embedded_block}<script>
const $ = id => document.getElementById(id);
const statusNames = {{queued:'等待',running:'扫描中',success:'完成',partial:'部分完成',failed:'失败',stopped:'已停止',interrupted:'已中断'}};
const modeNames = {{auto:'自动',regular:'常规',m3:'M3 特供双轮',m3_intersection:'仅 M3 交集'}};
const PAGE_SIZE = 50;

let manifest = null;
let embedded = null;
let mode = 'day';
let selectedDay = '';
let selectedRunId = '';
let pageOffset = 0;
let currentAlbums = [];
let currentFiltered = [];
const cache = {{days: new Map(), runs: new Map(), legacy: null}};

function loadEmbedded() {{
  const el = $('embedded-data');
  if (!el) return null;
  try {{ return JSON.parse(el.textContent); }} catch (_) {{ return null; }}
}}

async function fetchJson(relPath) {{
  const res = await fetch(relPath, {{cache: 'no-store'}});
  if (!res.ok) throw new Error(`读取 ${{relPath}} 失败 (${{res.status}})`);
  return res.json();
}}

async function getDayData(day) {{
  if (cache.days.has(day)) return cache.days.get(day);
  if (embedded?.days?.[day]) {{ cache.days.set(day, embedded.days[day]); return embedded.days[day]; }}
  if (embedded) throw new Error(`离线单文件仅内嵌最近天数数据，未包含 ${{day}}`);
  const data = await fetchJson(`./data/days/${{encodeURIComponent(day)}}.json`);
  cache.days.set(day, data);
  return data;
}}

async function getRunData(runId) {{
  if (cache.runs.has(runId)) return cache.runs.get(runId);
  if (embedded?.runs?.[runId]) {{ cache.runs.set(runId, embedded.runs[runId]); return embedded.runs[runId]; }}
  if (embedded) throw new Error('离线单文件未内嵌该历史任务，请查看在线站点');
  const data = await fetchJson(`./data/runs/${{encodeURIComponent(runId)}}.json`);
  cache.runs.set(runId, data);
  return data;
}}

async function getLegacyData() {{
  if (cache.legacy) return cache.legacy;
  if (embedded?.legacy_recent) {{ cache.legacy = embedded.legacy_recent; return cache.legacy; }}
  if (embedded) throw new Error('离线单文件未内嵌累计曲库');
  const data = await fetchJson('./data/legacy_recent.json');
  cache.legacy = data;
  return data;
}}

function formatTime(val) {{
  if (!val) return '—';
  try {{
    return new Intl.DateTimeFormat('zh-CN', {{
      timeZone:'Asia/Shanghai', dateStyle:'short', timeStyle:'medium', hour12:false
    }}).format(new Date(val));
  }} catch (_) {{ return String(val); }}
}}

function heatLabel(v) {{
  const n = Number(v);
  return v !== null && v !== undefined && v !== '' && Number.isSafeInteger(n) && n > 0
    ? (n >= 3 ? `🔥 ${{n}}` : String(n))
    : '—';
}}

function sourceLabel(r) {{
  const base = r.params?.retry_of ? '补扫' : r.source === 'scheduled' ? `自动 ${{r.schedule_time || '00:03'}}` : '手动扫描';
  return `${{base}} · ${{modeNames[r.params?.scanMode] || r.params?.scanMode || '常规'}}`;
}}

function filterAlbums() {{
  const q = $('searchBox').value.trim().toLowerCase();
  const hotOnly = $('hotOnly').checked;
  currentFiltered = currentAlbums.filter(album => {{
    if (hotOnly && (Number(album.heat) || 0) < 3) return false;
    if (!q) return true;
    if ((album.album_name || '').toLowerCase().includes(q)) return true;
    return (album.tracks || []).some(t =>
      (t.name || '').toLowerCase().includes(q) || (t.song_name || '').toLowerCase().includes(q)
    );
  }});
}}

function buildTrackTable(tracks) {{
  const table = document.createElement('table');
  const thead = table.createTHead().insertRow();
  for (const label of ['艺人', '曲目', '发行日期', '链接']) {{
    const th = document.createElement('th');
    th.textContent = label;
    thead.append(th);
  }}
  const tbody = table.createTBody();
  for (const row of tracks) {{
    const tr = tbody.insertRow();
    for (const k of ['name', 'song_name', 'date_str']) {{
      const td = tr.insertCell();
      td.textContent = row[k] || '';
    }}
    const linkTd = tr.insertCell();
    if (/^https?:\\/\\/(music\\.163\\.com|y\\.music\\.163\\.com)(\\/|$)/i.test(row.link || '')) {{
      const a = document.createElement('a');
      a.href = row.link;
      a.target = '_blank';
      a.rel = 'noopener noreferrer';
      a.textContent = '网易云 ↗';
      linkTd.append(a);
    }}
  }}
  return table;
}}

function renderAlbums(emptyText) {{
  filterAlbums();
  const total = currentFiltered.length;
  if (pageOffset >= total) pageOffset = 0;
  const slice = currentFiltered.slice(pageOffset, pageOffset + PAGE_SIZE);
  const frag = document.createDocumentFragment();
  if (!slice.length) {{
    const p = document.createElement('p');
    p.className = 'empty';
    p.textContent = emptyText || '没有匹配的记录';
    frag.append(p);
  }}
  for (const album of slice) {{
    const details = document.createElement('details');
    details.className = 'album';
    const summary = document.createElement('summary');
    const title = document.createElement('span');
    title.className = 'album-title';
    const aid = String(album.album_id || '');
    if (/^\\d+$/.test(aid)) {{
      const a = document.createElement('a');
      a.href = `https://music.163.com/#/album?id=${{encodeURIComponent(aid)}}`;
      a.target = '_blank';
      a.rel = 'noopener noreferrer';
      a.textContent = album.album_name || '（未命名专辑）';
      a.addEventListener('click', ev => ev.stopPropagation());
      title.append(a);
    }} else {{
      title.textContent = album.album_name || '（未命名专辑）';
    }}
    const meta = document.createElement('span');
    meta.className = 'album-meta';
    const artistList = (album.artists && album.artists.length)
      ? album.artists
      : Array.from(new Set((album.tracks || []).map(t => t.name).filter(Boolean)));
    const artistText = artistList.length
      ? `艺人：${{artistList.slice(0, 8).join('、')}}${{artistList.length > 8 ? ` 等共 ${{artistList.length}} 位` : ''}} · `
      : '';
    meta.textContent = `${{heatLabel(album.heat)}} 热度 · ${{artistText}}${{album.track_count}} 首曲目 · 最近发行 ${{album.latest_date || '—'}}`;
    summary.append(title, meta);
    details.append(summary);
    details.addEventListener('toggle', () => {{
      if (!details.open || details.dataset.rendered) return;
      const box = document.createElement('div');
      box.className = 'album-tracks';
      box.append(buildTrackTable(album.tracks || []));
      details.append(box);
      details.dataset.rendered = '1';
    }});
    frag.append(details);
  }}
  $('albumList').replaceChildren(frag);
  $('pageInfo').textContent = `${{total ? pageOffset + 1 : 0}}–${{Math.min(pageOffset + PAGE_SIZE, total)}} / ${{total}} 张专辑`;
  $('prevPage').disabled = pageOffset === 0;
  $('nextPage').disabled = pageOffset + PAGE_SIZE >= total;
  $('downloadCsv').disabled = total === 0;
}}

function downloadCurrentCsv() {{
  const header = ['艺人 (Artist)', '单曲名 (Track)', '企划/专辑名 (Album/VA)', '发行日期 (Release Date)', '直达链接', '新歌标记 (Flag)', '热度（同专辑命中艺人数）'];
  const esc = v => {{
    const s = String(v ?? '');
    return /[",\\r\\n]/.test(s) ? `"${{s.replace(/"/g, '""')}}"` : s;
  }};
  const lines = [header.map(esc).join(',')];
  for (const album of currentFiltered) {{
    for (const t of (album.tracks || [])) {{
      lines.push([t.name, t.song_name, t.album_name, t.date_str, t.link, t.flag ?? 0, t.album_count ?? album.heat ?? ''].map(esc).join(','));
    }}
  }}
  const blob = new Blob(['\\uFEFF' + lines.join('\\r\\n')], {{type: 'text/csv;charset=utf-8'}});
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = mode === 'day' ? `scan-${{selectedDay || 'day'}}.csv` : mode === 'run' ? `scan-${{selectedRunId.slice(0, 8)}}.csv` : 'legacy-recent.csv';
  document.body.append(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 30000);
}}

function renderRunList() {{
  const frag = document.createDocumentFragment();
  for (const r of (manifest.runs || [])) {{
    const btn = document.createElement('button');
    btn.className = 'run' + (selectedRunId === r.id ? ' selected' : '');
    const t = document.createElement('div');
    t.textContent = `${{formatTime(r.started_at || r.created_at)}} · ${{statusNames[r.status] || r.status}}`;
    const s = document.createElement('small');
    const mins = r.metrics?.elapsed_sec ? `${{Math.round(r.metrics.elapsed_sec / 60)}}m · ` : '';
    s.textContent = `${{sourceLabel(r)}} · ${{mins}}新增 ${{r.result_count}} 条`;
    btn.append(t, s);
    btn.onclick = () => selectRun(r.id);
    frag.append(btn);
  }}
  $('runList').replaceChildren(frag);
}}

async function selectDay(day) {{
  selectedDay = day;
  pageOffset = 0;
  $('daySelect').value = day;
  $('logsBox').hidden = true;
  $('viewWarning').textContent = '';
  try {{
    const data = await getDayData(day);
    currentAlbums = data.albums || [];
    const runs = data.runs || [];
    const counts = {{}};
    runs.forEach(r => counts[r.status] = (counts[r.status] || 0) + 1);
    const statusText = Object.entries(counts).map(([k, v]) => `${{statusNames[k] || k}} ${{v}} 次`).join(' · ');
    $('viewTitle').textContent = `今日 / 按日扫描结果 · ${{data.date}}`;
    $('viewSummary').textContent = `${{runs.length}} 次任务 · 新增 ${{data.total_tracks}} 首 · ${{data.total_albums}} 张专辑` + (statusText ? ` · ${{statusText}}` : '');
    $('viewWarning').textContent = runs
      .map(r => [r.error, r.warning].filter(Boolean).join('；'))
      .filter(Boolean)
      .join('\\n');
    renderAlbums(runs.length ? '当日已完成扫描，暂无新增记录' : '当日没有扫描记录');
  }} catch (err) {{
    currentAlbums = [];
    $('viewWarning').textContent = err.message;
    renderAlbums('无法加载当日数据');
  }}
}}

async function selectRun(runId) {{
  selectedRunId = runId;
  pageOffset = 0;
  renderRunList();
  $('logsBox').hidden = false;
  try {{
    const data = await getRunData(runId);
    const r = data.run;
    currentAlbums = data.albums || [];
    $('viewTitle').textContent = `${{sourceLabel(r)}} · ${{formatTime(r.started_at || r.created_at)}}`;
    const mins = r.metrics?.elapsed_sec ? ` · 耗时 ${{Math.round(r.metrics.elapsed_sec / 60)}} 分钟` : '';
    $('viewSummary').textContent = `${{statusNames[r.status] || r.status}} · 新增 ${{data.total_tracks}} 首 · 创建时 ${{r.params?.threads ?? '—'}} 线程${{mins}}`;
    $('viewWarning').textContent = [r.error, r.warning].filter(Boolean).join('；');
    $('logs').textContent = (data.logs || []).map(l => `${{formatTime(l.created_at)}} [${{l.level}}] ${{l.text}}`).join('\\n') || '无日志';
    renderAlbums('该次任务无新增记录');
  }} catch (err) {{
    currentAlbums = [];
    $('viewWarning').textContent = err.message;
    $('logs').textContent = '';
    renderAlbums('无法加载任务详情');
  }}
}}

async function selectLegacy() {{
  pageOffset = 0;
  $('logsBox').hidden = true;
  $('viewWarning').textContent = '';
  try {{
    const data = await getLegacyData();
    currentAlbums = data.albums || [];
    const st = data.stats || {{}};
    $('viewTitle').textContent = '近期累计曲库（VA_Tracks_Output.csv）';
    $('viewSummary').textContent = `总累计 ${{st.total_tracks || 0}} 首 · ${{st.total_albums || 0}} 张专辑（静态页展示最近 ${{currentAlbums.length}} 张）`;
    renderAlbums('暂无累计曲库记录');
  }} catch (err) {{
    currentAlbums = [];
    $('viewWarning').textContent = err.message;
    renderAlbums('无法加载累计曲库');
  }}
}}

function switchTab(nextMode) {{
  mode = nextMode;
  $('tabDay').classList.toggle('active', mode === 'day');
  $('tabRuns').classList.toggle('active', mode === 'run');
  $('tabLegacy').classList.toggle('active', mode === 'legacy');
  $('daySelect').hidden = mode !== 'day';
  $('runSidebar').hidden = mode !== 'run';
  $('contentLayout').classList.toggle('single', mode !== 'run');
  if (mode === 'day') selectDay(selectedDay || manifest.latest_day || '');
  else if (mode === 'run') selectRun(selectedRunId || manifest.runs?.[0]?.id || '');
  else selectLegacy();
}}

async function init() {{
  embedded = loadEmbedded();
  manifest = embedded ? embedded.manifest : await fetchJson('./data/manifest.json');
  $('updatedTag').textContent = `更新时间 ${{formatTime(manifest.generated_at)}}`;
  if (embedded) $('subtitle').textContent += '（离线单文件模式）';

  const dayFrag = document.createDocumentFragment();
  for (const d of (manifest.days || [])) {{
    const opt = document.createElement('option');
    opt.value = d.date;
    opt.textContent = `${{d.date}} · ${{d.run_count}} 次任务 · ${{d.hit_count}} 首`;
    dayFrag.append(opt);
  }}
  if (!(manifest.days || []).length) {{
    const opt = document.createElement('option');
    opt.value = '';
    opt.textContent = '暂无按日记录';
    dayFrag.append(opt);
  }}
  $('daySelect').replaceChildren(dayFrag);
  selectedDay = manifest.latest_day || manifest.days?.[0]?.date || '';
  selectedRunId = manifest.runs?.[0]?.id || '';

  $('tabDay').onclick = () => switchTab('day');
  $('tabRuns').onclick = () => switchTab('run');
  $('tabLegacy').onclick = () => switchTab('legacy');
  $('daySelect').onchange = () => selectDay($('daySelect').value);
  $('searchBox').oninput = () => {{ pageOffset = 0; renderAlbums(); }};
  $('hotOnly').onchange = () => {{ pageOffset = 0; renderAlbums(); }};
  $('prevPage').onclick = () => {{ pageOffset = Math.max(0, pageOffset - PAGE_SIZE); renderAlbums(); $('albumList').scrollIntoView({{block:'start'}}); }};
  $('nextPage').onclick = () => {{ pageOffset += PAGE_SIZE; renderAlbums(); $('albumList').scrollIntoView({{block:'start'}}); }};
  $('downloadCsv').onclick = downloadCurrentCsv;

  switchTab(selectedDay ? 'day' : 'run');
}}

init().catch(err => {{
  $('viewWarning').textContent = `初始化失败：${{err.message}}`;
}});
</script>
</body>
</html>
"""


def build_site(
    data_dir: Path,
    out_dir: Path,
    standalone_path: Optional[Path] = None,
    max_runs: int = 200,
    max_run_details: int = 120,
    standalone_days: int = 14,
    standalone_runs: int = 40,
    legacy_max_albums: int = 500,
) -> Dict[str, Any]:
    data_dir = Path(data_dir).resolve()
    out_dir = Path(out_dir).resolve()
    data_out = out_dir / "data"
    days_out = data_out / "days"
    runs_out = data_out / "runs"
    days_out.mkdir(parents=True, exist_ok=True)
    runs_out.mkdir(parents=True, exist_ok=True)

    # Clear stale day/run JSONs when rebuilding into an existing directory.
    for stale in list(days_out.glob("*.json")) + list(runs_out.glob("*.json")):
        stale.unlink()

    runs, hits_by_run, logs_by_run, _ = _load_sqlite_data(data_dir / "scan_history.sqlite3")
    legacy_stats, legacy_payload = _load_legacy_csv(
        data_dir / "VA_Tracks_Output.csv", max_albums=legacy_max_albums
    )

    runs_by_day: Dict[str, List[Dict[str, Any]]] = {}
    for run in runs:
        day = run.get("schedule_date") or (
            run["created_at"][:10] if run.get("created_at") else ""
        )
        if not day:
            continue
        runs_by_day.setdefault(day, []).append(run)

    day_summaries: List[Dict[str, Any]] = []
    all_day_data: Dict[str, Dict[str, Any]] = {}
    for day in sorted(runs_by_day.keys(), reverse=True):
        day_runs = runs_by_day[day]
        combined_hits: List[Tuple[str, Dict[str, Any]]] = []
        for r in day_runs:
            combined_hits.extend(hits_by_run.get(r["id"], []))
        albums = _group_hits_to_albums(combined_hits)
        cleaned_tracks = sum(a["track_count"] for a in albums)
        day_doc = {
            "date": day,
            "timezone": "Asia/Shanghai",
            "runs": day_runs,
            "total_tracks": cleaned_tracks,
            "total_albums": len(albums),
            "albums": albums,
        }
        all_day_data[day] = day_doc
        (days_out / f"{day}.json").write_text(
            json.dumps(day_doc, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        day_summaries.append(
            {
                "date": day,
                "run_count": len(day_runs),
                "hit_count": cleaned_tracks,
                "album_count": len(albums),
                "statuses": sorted({r["status"] for r in day_runs}),
            }
        )

    runs_for_manifest = runs[:max_runs]
    detail_limit = max(max_runs, max_run_details)
    all_run_data: Dict[str, Dict[str, Any]] = {}
    for idx, r in enumerate(runs[:detail_limit]):
        rid = r["id"]
        run_hits = hits_by_run.get(rid, [])
        albums = _group_hits_to_albums(run_hits)
        run_doc = {
            "run": r,
            "logs": logs_by_run.get(rid, []),
            "total_tracks": sum(a["track_count"] for a in albums),
            "total_albums": len(albums),
            "albums": albums,
        }
        if idx < standalone_runs:
            all_run_data[rid] = run_doc
        (runs_out / f"{rid}.json").write_text(
            json.dumps(run_doc, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )

    generated_at = datetime.now(TZ).isoformat(timespec="seconds")
    latest_day = day_summaries[0]["date"] if day_summaries else ""
    manifest = {
        "schema": 1,
        "generated_at": generated_at,
        "timezone": "Asia/Shanghai",
        "latest_day": latest_day,
        "days": day_summaries,
        "runs": runs_for_manifest,
        "legacy_stats": legacy_stats,
    }
    (data_out / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    (data_out / "legacy_recent.json").write_text(
        json.dumps(legacy_payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    (out_dir / "index.html").write_text(_render_html(embedded_payload=None), encoding="utf-8")

    if standalone_path is not None:
        standalone_path = Path(standalone_path).resolve()
        standalone_path.parent.mkdir(parents=True, exist_ok=True)
        trimmed_days = day_summaries[:standalone_days]
        trimmed_runs = runs[:standalone_runs]
        embedded = {
            "manifest": {
                **manifest,
                "days": trimmed_days,
                "runs": trimmed_runs,
            },
            "days": {d["date"]: all_day_data[d["date"]] for d in trimmed_days},
            "runs": {r["id"]: all_run_data[r["id"]] for r in trimmed_runs if r["id"] in all_run_data},
            "legacy_recent": {
                "stats": legacy_stats,
                "albums": legacy_payload["albums"][:200],
            },
        }
        standalone_path.write_text(_render_html(embedded_payload=embedded), encoding="utf-8")

    return {
        "generated_at": generated_at,
        "latest_day": latest_day,
        "days_count": len(day_summaries),
        "runs_count": len(runs_for_manifest),
        "legacy_stats": legacy_stats,
        "out_dir": str(out_dir),
        "standalone_path": str(standalone_path) if standalone_path else None,
    }


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Build static HTML viewer from scanner state.")
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--standalone", type=Path, default=None)
    args = parser.parse_args(list(argv) if argv is not None else None)
    summary = build_site(args.data_dir, args.out_dir, standalone_path=args.standalone)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
