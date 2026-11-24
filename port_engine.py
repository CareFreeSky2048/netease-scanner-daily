"""Reproduce the server engine from the unmodified desktop scanner."""
from pathlib import Path

root = Path(__file__).resolve().parent
source = root.parent / "netease_scanner_dual_date_album_mismatch_fix.py"
text = source.read_text(encoding="utf-8")
text = text.split("# ---------------------------- Web UI ----------------------------")[0]
start = text.index("# ---------------------------- 依赖自检")
end = text.index("import requests", start)
text = text[:start] + text[end:]
text = text.replace(
    "SCRIPT_DIR = Path(__file__).resolve().parent",
    'SCRIPT_DIR = Path(os.getenv("NETEASE_DATA_DIR", str(Path(__file__).resolve().parent / "data"))).resolve()\n'
    'SCRIPT_DIR.mkdir(parents=True, exist_ok=True)',
)
text = text.replace("datetime.now() - timedelta(days=1)", "datetime.now(CN_TZ).replace(tzinfo=None) - timedelta(days=1)")
text = text.replace("traceback.print_exc()", 'logging.error("扫描失败：%s", err)')
# The server owns API lifetime; remove desktop process management entirely.
text = text.split("# ---------------------------- API 服务自检/启动 ----------------------------")[0]
# Stop must also work while emitting the final result rows.
text = text.replace("        for r in candidate_relations:\n            compare_done += 1", "        for r in candidate_relations:\n            if not wait_if_paused_or_stopped():\n                stopped = True\n                return\n            compare_done += 1")
for unused in ("import atexit\n", "import subprocess\n", "import sys\n", "import traceback\n", "import webbrowser\n"):
    text = text.replace(unused, "")
# Reproduce server-only memory changes without editing the original scanner.
memory_changes = [
    ('    with ARTIST_ALBUM_CACHE_LOCK:\n        old = ARTIST_ALBUM_PAGE_CACHE.get(key)',
     '    # Server memory: retain only fields used by new-release and ownership checks.\n'
     '    data = {\n'
     '        "code": data.get("code"),\n'
     '        "hotAlbums": [{field: album.get(field) for field in ("id", "name", "publishTime")}\n'
     '                      for album in data.get("hotAlbums") or []],\n'
     '    }\n'
     '    with ARTIST_ALBUM_CACHE_LOCK:\n        old = ARTIST_ALBUM_PAGE_CACHE.get(key)'),
    ('progress_cb: Optional[Callable[[int, int, Any], None]] = None) -> List[Any]:\n    results: List[Any] = []',
     'progress_cb: Optional[Callable[[int, int, Any], None]] = None) -> Iterable[Any]:\n'
     '    # Server memory: stream completed results instead of retaining a whole phase.'),
    ('                    results.append(res)', '                    yield res'),
    ('                    results.append(e)\n', ''),
    ('emit({"type": "log", "level": "error", "text": f"{phase_label} 任务异常：{redacted_error_text(str(e))}"})',
     'emit({"type": "log", "level": "error", "text": f"{phase_label} 任务异常：{redacted_error_text(str(e))}"})\n'
     '                    yield e'),
    ('    return results\n', ''),
    ('        verify_artists = list(verify_artist_map.values())',
     '        verify_artists = list(verify_artist_map.values())\n'
     '        # Server memory: only date-matching relations are needed from this point.\n'
     '        relation_count = len(artist_relations)\n'
     '        artist_relations.clear()\n'
     '        by_album.clear()\n'
     '        new_album_candidates.clear()\n'
     '        new_album_by_id.clear()'),
    ('"relations": len(artist_relations),', '"relations": relation_count,'),
    ('        autosave_if_needed(force=True, reason="stopped" if stopped else "finished_or_error")',
     '        autosave_if_needed(force=True, reason="stopped" if stopped else "finished_or_error")\n'
     '        # Server memory: this cache is useful only while a scan is running.\n'
     '        with ARTIST_ALBUM_CACHE_LOCK:\n'
     '            ARTIST_ALBUM_PAGE_CACHE.clear()'),
]
for old, new in memory_changes:
    if text.count(old) != 1:
        raise ValueError(f"Server memory transformation must match exactly once: {old!r}")
    text = text.replace(old, new, 1)
# Preserve bounded failure details so a partial run can be diagnosed afterwards.
old = '                "owned_errors": len(owned_errors),'
new = old + '''
                "failure_details": (
                    [{"stage": "/album", "item_id": aid, "error": err} for aid, err in list(album_errors.items())[:20]] +
                    [{"stage": "owned_verify", "item_id": aid, "error": err} for aid, err in list(owned_errors.items())[:20]] +
                    [{"stage": "recent_songs", "item_id": x["artist"]["id"], "error": x.get("error") or "unknown"} for x in artist_failures[:20]] +
                    [{"stage": "new_album_probe", "item_id": x["artist"]["id"], "error": x.get("error") or "unknown"} for x in new_album_probe_failures[:20]]
                )[:20],'''
if text.count(old) != 1:
    raise ValueError("Server failure transformation must match exactly once")
text = text.replace(old, new, 1)
# Retry only failed objects before their downstream filtering stage consumes them.
retry_changes = [
    ('import requests\n', 'import requests\nfrom slow_retry import RetryBudget, retry_failed\n'),
    ('    all_results: List[Dict[str, Any]] = []\n    stopped = False',
     '    all_results: List[Dict[str, Any]] = []\n    retry_budget = RetryBudget()\n    stopped = False'),
    ('        if CONTROL.snapshot()["stop_requested"]:\n            stopped = True\n            emit({"type": "log", "level": "error", "text": "已停止：recent 曲目采样阶段结束前收到停止信号。"})',
     '''        sample_errors = {x["artist"]["id"]: x for x in artist_failures}
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
        METRICS.artist_failed = len(artist_failures)
        if CONTROL.snapshot()["stop_requested"]:
            stopped = True
            emit({"type": "log", "level": "error", "text": "已停止：recent 曲目采样阶段结束前收到停止信号。"})'''),
    ('        if CONTROL.snapshot()["stop_requested"]:\n            stopped = True\n            emit({"type": "log", "level": "error", "text": "已停止：新专目录兜底阶段结束前收到停止信号。"})',
     '''        probe_errors = {x["artist"]["id"]: x for x in new_album_probe_failures}
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
        METRICS.new_album_probe_failed = len(new_album_probe_failures)
        if CONTROL.snapshot()["stop_requested"]:
            stopped = True
            emit({"type": "log", "level": "error", "text": "已停止：新专目录兜底阶段结束前收到停止信号。"})'''),
    ('        save_album_cache()\n        if CONTROL.snapshot()["stop_requested"]:',
     '''        for aid, out in retry_failed(list(album_errors), album_task, lambda x: bool(x[1]),
                                     "/album", str, retry_budget):
            if not isinstance(out, Exception) and out[1]:
                album_details[aid] = out[1]
                album_errors.pop(aid, None)
            else:
                err = redacted_error_text(str(out)) if isinstance(out, Exception) else out[2] or "unknown"
                album_errors[aid] = err + ("；已用recent采样fallback" if aid in album_details else "")
        save_album_cache()
        if CONTROL.snapshot()["stop_requested"]:'''),
    ('        if CONTROL.snapshot()["stop_requested"]:\n            stopped = True\n            emit({"type": "log", "level": "error", "text": "已停止：owned 验证阶段收到停止信号。"})',
     '''        for artist, out in retry_failed([x for x in verify_artists if x["id"] in owned_errors], verify_task,
                                        lambda x: x[1] is not None, "/artist/album 归属", lambda x: x["id"], retry_budget):
            if not isinstance(out, Exception) and out[1] is not None:
                owned_map[artist["id"]] = out[1]
                owned_errors.pop(artist["id"], None)
            else:
                owned_errors[artist["id"]] = redacted_error_text(str(out)) if isinstance(out, Exception) else out[2] or "unknown"
        if CONTROL.snapshot()["stop_requested"]:
            stopped = True
            emit({"type": "log", "level": "error", "text": "已停止：owned 验证阶段收到停止信号。"})'''),
    ('                "failure_details": (', '                "slow_retry": retry_budget.snapshot(),\n                "failure_details": ('),
    ('                "emit_results": "比对筛选结果",', '                "emit_results": "比对筛选结果",\n                "slow_retry": "失败对象低速补扫（单线程）",'),
]
for old, new in retry_changes:
    if text.count(old) != 1:
        raise ValueError(f"Slow retry transformation must match exactly once: {old!r}")
    text = text.replace(old, new, 1)
(root / "engine.py").write_text(text, encoding="utf-8")
