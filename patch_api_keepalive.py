"""Idempotent patcher for NeteaseCloudMusicApi util/request.js to reuse HTTPS Keep-Alive sockets."""
from __future__ import annotations

import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable, Optional

MARKER = "/* NETEASE_KEEPALIVE_PATCHED */"

_SHARED_AGENTS = (
    f"{MARKER}\n"
    "const _sharedHttpAgent = new http.Agent({ keepAlive: true, maxSockets: 32, maxFreeSockets: 16, keepAliveMsecs: 15000 })\n"
    "const _sharedHttpsAgent = new https.Agent({ keepAlive: true, maxSockets: 32, maxFreeSockets: 16, keepAliveMsecs: 15000 })"
)

_FACTORY_PATTERN = re.compile(
    r"^[ \t]*const[ \t]+createHttpAgent[ \t]*=[ \t]*\(\)[ \t]*=>[ \t]*new[ \t]+http\.Agent\([^;\r\n]+\);?\r?\n"
    r"[ \t]*const[ \t]+createHttpsAgent[ \t]*=[ \t]*\(\)[ \t]*=>[ \t]*new[ \t]+https\.Agent\([^;\r\n]+\);?",
    flags=re.MULTILINE,
)

_HTTPS_REQUIRE_PATTERN = re.compile(
    r"^[ \t]*const[ \t]+https[ \t]*=[ \t]*require\(['\"]https['\"]\);?",
    flags=re.MULTILINE,
)


def _node_syntax_ok(js_path: Path) -> bool:
    node_bin = shutil.which("node")
    if not node_bin:
        return True
    proc = subprocess.run([node_bin, "--check", str(js_path)], capture_output=True, text=True, check=False)
    return proc.returncode == 0


def patch_request_js(path: Path) -> str:
    """Patch util/request.js so HTTP/HTTPS Keep-Alive agents are singletons instead of per-request instances."""
    target = Path(path)
    if not target.is_file():
        return "skipped"

    raw = target.read_text(encoding="utf-8")
    if MARKER in raw:
        return "already-patched"

    newline = "\r\n" if "\r\n" in raw else "\n"
    text = raw.replace("\r\n", "\n")
    patched: Optional[str] = None

    if _FACTORY_PATTERN.search(text):
        replacement = (
            f"{_SHARED_AGENTS}\n"
            "const createHttpAgent = () => _sharedHttpAgent\n"
            "const createHttpsAgent = () => _sharedHttpsAgent"
        )
        patched = _FACTORY_PATTERN.sub(lambda _m: replacement, text, count=1)
    elif (
        _HTTPS_REQUIRE_PATTERN.search(text)
        and "new http.Agent" in text
        and "new https.Agent" in text
    ):
        updated = _HTTPS_REQUIRE_PATTERN.sub(
            lambda m: f"{m.group(0)}\n{_SHARED_AGENTS}", text, count=1
        )
        updated = re.sub(r"new[ \t]+http\.Agent\([^)]*\)", "_sharedHttpAgent", updated)
        updated = re.sub(r"new[ \t]+https\.Agent\([^)]*\)", "_sharedHttpsAgent", updated)
        if updated != text:
            patched = updated

    if not patched or patched == text:
        return "skipped"

    out_text = patched.replace("\n", newline)
    tmp_path = target.with_name(f".{target.stem}.keepalive.tmp{target.suffix or '.js'}")
    try:
        tmp_path.write_text(out_text, encoding="utf-8")
        if not _node_syntax_ok(tmp_path):
            tmp_path.unlink(missing_ok=True)
            return "skipped"
        tmp_path.replace(target)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    return "patched"


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        return 0
    target = Path(args[0])
    try:
        status = patch_request_js(target)
        print(f"[keepalive] {status}: {target}", flush=True)
    except Exception as exc:
        print(f"[keepalive] warning ({type(exc).__name__}): {exc}", file=sys.stderr, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
