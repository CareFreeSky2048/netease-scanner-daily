"""Small live API check; no full scan and no writes to scanner data."""
import argparse
import json
import time

import requests

import engine
from server import Config, verify_login

DIAGNOSTIC_REAL_IP = "116.25.146.177"


def _scrub_text(text, cookie, proxy=""):
    result = str(text or "")
    for token in (cookie, cookie.removeprefix("MUSIC_U=").rstrip(";"), proxy):
        if token:
            result = result.replace(token, "<redacted>")
    return engine.redacted_error_text(result)


def _extract_error_detail(response, scrub_fn):
    parts = [f"HTTP {response.status_code}"]
    try:
        body = response.json()
        if isinstance(body, dict):
            if body.get("code") is not None:
                parts.append(f"code={body.get('code')}")
            msg = body.get("message") or body.get("msg")
            if msg:
                parts.append(f"msg={msg}")
    except Exception:
        pass
    return scrub_fn(" · ".join(parts))


def _load_fallback_artists(config, cookie, count):
    candidates = [engine.cookie_fingerprint(cookie)]
    try:
        cfg_key = str(config.read().get("artist_cache_key") or "").strip().lower()
        if cfg_key and cfg_key not in candidates:
            candidates.append(cfg_key)
    except Exception:
        pass
    try:
        if engine.ARTIST_CACHE_DIR.is_dir():
            files = sorted(
                engine.ARTIST_CACHE_DIR.glob("artists_*.json"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            for path in files:
                key = path.stem.removeprefix("artists_")
                if key not in candidates:
                    candidates.append(key)
    except Exception:
        pass
    for key in candidates:
        artists = engine.load_artist_cache_by_key(key)
        if artists:
            return artists[:count], key
    return [], ""


def _resolve_sample_artist_id(config, cookie=""):
    if cookie:
        artists, _ = _load_fallback_artists(config, cookie, 1)
        if artists:
            return str(artists[0]["id"])
    try:
        cfg_key = str(config.read().get("artist_cache_key") or "").strip()
        if cfg_key:
            artists = engine.load_artist_cache_by_key(cfg_key)
            if artists:
                return str(artists[0]["id"])
    except Exception:
        pass
    try:
        if engine.ARTIST_CACHE_DIR.is_dir():
            files = sorted(
                engine.ARTIST_CACHE_DIR.glob("artists_*.json"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            for path in files:
                artists = engine.load_artist_cache_by_key(path.stem.removeprefix("artists_"))
                if artists:
                    return str(artists[0]["id"])
    except Exception:
        pass
    return "6452"


def _run_diagnostics(base, cookie, config, overrides, scrub_fn):
    """Read-only sweep across transport modes when primary probe fails."""
    results = []
    proxy_extra = {"proxy": overrides["proxy"]} if overrides.get("proxy") else {}
    sample_artist_id = _resolve_sample_artist_id(config, cookie)
    sample_album_id = None

    modes = (
        ("default", {}),
        ("realIP", {"realIP": DIAGNOSTIC_REAL_IP}),
        ("eapi", {"crypto": "eapi"}),
        ("realIP+eapi", {"realIP": DIAGNOSTIC_REAL_IP, "crypto": "eapi"}),
    )

    def test_one(endpoint, mode_name, base_params, mode_params):
        start = time.monotonic()
        req_params = {
            **base_params,
            **mode_params,
            **proxy_extra,
            "cookie": cookie,
            "timestamp": int(time.time() * 1000),
        }
        entry = {
            "endpoint": endpoint,
            "mode": mode_name,
            "http_status": None,
            "api_code": None,
            "ok": False,
            "detail": "",
            "elapsed_ms": 0,
        }
        parsed = None
        try:
            # Bust NeteaseCloudMusicApi's 2-minute URL apicache across diagnostic modes.
            resp = requests.request(
                "POST",
                base + endpoint,
                params={"_diag": f"{mode_name}_{int(time.time() * 1000)}"},
                data=req_params,
                timeout=8,
            )
            entry["http_status"] = resp.status_code
            try:
                body = resp.json()
                if isinstance(body, dict):
                    parsed = body
                    entry["api_code"] = body.get("code")
                    msg = body.get("message") or body.get("msg") or ""
                    if msg:
                        entry["detail"] = scrub_fn(str(msg))
            except Exception:
                pass
            entry["ok"] = resp.ok and isinstance(parsed, dict) and parsed.get("code") == 200
            if not entry["detail"] and not entry["ok"]:
                entry["detail"] = f"HTTP {resp.status_code}"
        except Exception as exc:
            entry["detail"] = scrub_fn(type(exc).__name__)
        entry["elapsed_ms"] = round((time.monotonic() - start) * 1000)
        results.append(entry)
        return parsed

    for mode_name, mode_params in modes:
        test_one("/artist/sublist", mode_name, {"limit": 3, "offset": 0}, mode_params)

    for mode_name, mode_params in (modes[0], modes[3]):
        songs_body = test_one(
            "/artist/songs",
            mode_name,
            {"id": sample_artist_id, "limit": 3, "offset": 0, "order": "time"},
            mode_params,
        )
        if not sample_album_id and isinstance(songs_body, dict):
            songs = songs_body.get("songs") or []
            if songs and isinstance(songs[0], dict):
                sample_album_id = (songs[0].get("al") or {}).get("id")

        albums_body = test_one(
            "/artist/album",
            mode_name,
            {"id": sample_artist_id, "limit": 3, "offset": 0},
            mode_params,
        )
        if not sample_album_id and isinstance(albums_body, dict):
            albums = albums_body.get("hotAlbums") or []
            if albums and isinstance(albums[0], dict):
                sample_album_id = albums[0].get("id")

    album_target = str(sample_album_id or "18905")
    for mode_name, mode_params in (modes[0], modes[3]):
        test_one("/album", mode_name, {"id": album_target}, mode_params)

    return results


def probe(config=None, artist_count=3, api_base=None):
    config = config or Config()
    cookie = config.cookie()
    base = (api_base or engine.API_BASE).rstrip("/")
    overrides = engine.api_transport_overrides()
    proxy_val = overrides.get("proxy", "")

    def scrub_fn(text):
        return _scrub_text(text, cookie, proxy_val)

    report = {
        "api_base": base,
        "started_at": time.time(),
        "transport": {
            "realIP": overrides.get("realIP", ""),
            "crypto": overrides.get("crypto", ""),
            "proxy_configured": bool(proxy_val),
        },
        "checks": [],
        "diagnostics": [],
        "degraded": False,
        "warning": "",
        "ok": False,
    }

    def get(path, params):
        start = time.monotonic()
        # POST keeps credentials out of URL/access logs. Try GET only on 405,
        # matching the scanner's existing endpoint compatibility behaviour.
        req_params = {
            **params,
            **overrides,
            "cookie": cookie,
            "timestamp": int(time.time() * 1000),
        }
        last_405 = ""
        for method in ("POST", "GET"):
            response = requests.request(
                method,
                base + path,
                data=req_params if method == "POST" else None,
                params=req_params if method == "GET" else None,
                timeout=8,
            )
            if response.status_code == 405:
                last_405 = _extract_error_detail(response, scrub_fn)
                continue
            response.raise_for_status()
            data = response.json()
            if data.get("code") == 405:
                last_405 = _extract_error_detail(response, scrub_fn)
                continue
            if data.get("code") != 200:
                msg = data.get("message") or data.get("msg") or ""
                suffix = f" ({scrub_fn(msg)})" if msg else ""
                raise ValueError(f"{path} 返回码 {data.get('code')}{suffix}")
            report["checks"].append(
                {
                    "endpoint": path,
                    "ok": True,
                    "elapsed_ms": round((time.monotonic() - start) * 1000),
                }
            )
            return data
        raise ValueError(f"{path} GET/POST 均返回 405（{last_405 or '405'}）")

    try:
        verify_login(cookie, base)
        report["checks"].append({"endpoint": "/login/status", "ok": True})
        try:
            artists = get("/artist/sublist", {"limit": artist_count, "offset": 0}).get("data", [])[:artist_count]
            report["artist_source"] = "live"
        except ValueError as sublist_err:
            fallback_artists, fallback_key = _load_fallback_artists(config, cookie, artist_count)
            if not fallback_artists:
                raise
            detail_msg = scrub_fn(str(sublist_err))
            report["checks"].append({"endpoint": "/artist/sublist", "ok": False, "detail": detail_msg})
            report["degraded"] = True
            report["warning"] = (
                f"实时 /artist/sublist 失败（{detail_msg}），"
                f"已自动回退艺人缓存 {fallback_key} 继续验证歌曲与专辑接口。"
            )
            report["artist_source"] = "fallback-cache"
            report["artist_cache_key"] = fallback_key
            artists = fallback_artists

        if not artists:
            raise ValueError("关注艺人列表为空，无法验证歌曲与专辑接口")
        for artist in artists:
            artist_id = str(artist["id"])
            songs = get("/artist/songs", {"id": artist_id, "limit": 10, "offset": 0, "order": "time"}).get("songs", [])
            albums = get("/artist/album", {"id": artist_id, "limit": 10, "offset": 0}).get("hotAlbums", [])
            album_id = (songs[0].get("al") or {}).get("id") if songs else None
            album_id = album_id or (albums[0].get("id") if albums else None)
            if album_id:
                album = get("/album", {"id": album_id}).get("album", {})
                if str(album.get("id")) != str(album_id):
                    raise ValueError("专辑接口返回 ID 不匹配，需检查 API 或缓存")
        if not any(c["endpoint"] == "/album" for c in report["checks"]):
            raise ValueError("样本中没有可验证的专辑，请扩大样本")
        report["ok"] = True
        report["artist_count"] = len(artists)
    except Exception as exc:
        # Exception messages from requests may include cookies/proxy creds.
        # Only explicitly created validation errors are shown, scrubbed.
        report["error"] = (
            scrub_fn(str(exc))
            if type(exc) is ValueError
            else f"接口请求失败（{type(exc).__name__}）"
        )
        report["diagnostics"] = _run_diagnostics(base, cookie, config, overrides, scrub_fn)
    report["elapsed_sec"] = round(time.time() - report["started_at"], 2)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--artists", type=int, default=3)
    args = parser.parse_args()
    if not 1 <= args.artists <= 10:
        parser.error("样本数必须为 1 至 10")
    result = probe(artist_count=args.artists)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result["ok"] else 1)
