"""Small live API check; no full scan and no writes to scanner data."""
import argparse
import json
import time

import requests

import engine
from server import Config, verify_login


def probe(config=None, artist_count=3, api_base=None):
    config = config or Config()
    cookie = config.cookie()
    base = api_base or engine.API_BASE
    report = {"api_base": base, "started_at": time.time(), "checks": [], "ok": False}

    def get(path, params):
        start = time.monotonic()
        # POST keeps credentials out of URL/access logs. Try GET only on 405,
        # matching the scanner's existing endpoint compatibility behaviour.
        params = {**params, "cookie": cookie, "timestamp": int(time.time() * 1000)}
        for method in ("POST", "GET"):
            response = requests.request(method, base + path, data=params if method == "POST" else None,
                                        params=params if method == "GET" else None, timeout=8)
            if response.status_code == 405:
                continue
            response.raise_for_status()
            data = response.json()
            if data.get("code") == 405:
                continue
            if data.get("code") != 200:
                raise ValueError(f"{path} 返回码 {data.get('code')}")
            report["checks"].append({"endpoint": path, "ok": True, "elapsed_ms": round((time.monotonic() - start) * 1000)})
            return data
        raise ValueError(f"{path} GET/POST 均返回 405")

    try:
        verify_login(cookie, base)
        report["checks"].append({"endpoint": "/login/status", "ok": True})
        artists = get("/artist/sublist", {"limit": artist_count, "offset": 0}).get("data", [])[:artist_count]
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
        # Exception messages from requests may include cookies. Only explicitly
        # created validation errors are shown, with the credential removed.
        report["error"] = (str(exc).replace(cookie, "<redacted>").replace(cookie.removeprefix("MUSIC_U=").rstrip(";"), "<redacted>")
                           if type(exc) is ValueError else f"接口请求失败（{type(exc).__name__}）")
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
