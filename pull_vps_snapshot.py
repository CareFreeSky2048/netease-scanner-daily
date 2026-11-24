"""Read-only zero-disk/zero-RAM streaming snapshot puller from an existing VPS deployment."""
from __future__ import annotations

import argparse
import hashlib
import json
import shlex
import shutil
import subprocess
import tarfile
import time
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo

from build_site import build_site
from state_store import pack_state, sha256_file

TZ = ZoneInfo("Asia/Shanghai")


def _ssh_base(port: int, user: str, host: str) -> list[str]:
    return [
        "ssh",
        "-p",
        str(port),
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=15",
        f"{user}@{host}",
    ]


def _run_ssh(port: int, user: str, host: str, remote_cmd: str) -> str:
    proc = subprocess.run(
        [*_ssh_base(port, user, host), remote_cmd],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"SSH command failed (exit {proc.returncode}): {proc.stderr.strip() or proc.stdout.strip()}"
        )
    return proc.stdout


def _check_vps_idle(port: int, user: str, host: str) -> Dict[str, Any]:
    script = (
        "cid=$(docker ps -q --filter label=com.docker.compose.project=netease-scanner "
        "--filter label=com.docker.compose.service=scanner | head -n1); "
        "if [ -z \"$cid\" ]; then echo '{\"idle\":true,\"container\":null}'; exit 0; fi; "
        "docker exec \"$cid\" python -c \"import json,urllib.request; "
        "s=json.load(urllib.request.urlopen('http://127.0.0.1:5000/api/status',timeout=20)); "
        "print(json.dumps({'idle': (not s.get('current')) and (not s.get('pending')), "
        "'current': (s.get('current') or {}).get('id'), 'pending': len(s.get('pending') or [])}))\""
    )
    out = _run_ssh(port, user, host, script).strip().splitlines()[-1]
    return json.loads(out)


def _snapshot_signatures(
    port: int, user: str, host: str, remote_dir: str
) -> List[Dict[str, Any]]:
    py_code = (
        "import json, re, sys; from pathlib import Path; "
        "root = Path(sys.argv[1]); data = root / 'data'; pat = re.compile(r'artists_[0-9a-f]{16}\\.json'); "
        "cands = ["
        "data / 'VA_Tracks_Output.csv', "
        "data / 'VA_Album_Date_Cache.json', "
        "data / 'VA_Scan_Metrics.json', "
        "data / 'VA_Scan_Tuning_Report.txt', "
        "data / 'scan_history.sqlite3', "
        "data / 'scan_history.sqlite3-wal', "
        "data / 'scan_history.sqlite3-shm', "
        "data / 'migration_archive' / 'pre_migration' / 'VA_Tracks_Output.csv'"
        "]; "
        "ac = data / 'VA_Artist_Caches'; "
        "cands += sorted(p for p in ac.iterdir() if p.is_file() and pat.fullmatch(p.name)) if ac.is_dir() else []; "
        "out = []; "
        "[out.append({'rel': p.relative_to(root).as_posix(), 'size': p.stat().st_size, 'mtime_ns': p.stat().st_mtime_ns}) "
        "for p in cands if p.is_file() and not p.is_symlink()]; "
        "out.sort(key=lambda x: x['rel']); "
        "print(json.dumps(out))"
    )
    cmd = f"python3 -c {shlex.quote(py_code)} {shlex.quote(remote_dir.rstrip('/'))}"
    out = _run_ssh(port, user, host, cmd).strip().splitlines()[-1]
    return json.loads(out)


def _stream_tar_over_ssh(
    port: int,
    user: str,
    host: str,
    remote_dir: str,
    rel_paths: List[str],
    dest_tar: Path,
) -> str:
    remote_tar_cmd = (
        f"tar -C {shlex.quote(remote_dir.rstrip('/'))} -czf - -- "
        + " ".join(shlex.quote(rel) for rel in rel_paths)
    )
    dest_tar.parent.mkdir(parents=True, exist_ok=True)
    tmp_tar = dest_tar.with_name(f".{dest_tar.name}.part")
    digest = hashlib.sha256()
    try:
        proc = subprocess.Popen(
            [*_ssh_base(port, user, host), remote_tar_cmd],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert proc.stdout is not None and proc.stderr is not None
        with tmp_tar.open("wb") as out_f:
            while True:
                chunk = proc.stdout.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
                out_f.write(chunk)
        stderr_bytes = proc.stderr.read()
        rc = proc.wait()
        if rc != 0:
            raise RuntimeError(
                f"SSH tar stream failed (exit {rc}): {stderr_bytes.decode('utf-8', errors='replace').strip()}"
            )
        tmp_tar.replace(dest_tar)
        return digest.hexdigest()
    except Exception:
        tmp_tar.unlink(missing_ok=True)
        raise


def _safe_extract_raw(archive_path: Path, raw_data_dir: Path) -> None:
    raw_data_dir.mkdir(parents=True, exist_ok=True)
    base_resolved = raw_data_dir.resolve()
    with tarfile.open(archive_path, "r:gz") as archive:
        for member in archive.getmembers():
            p = PurePosixPath(member.name)
            if p.is_absolute() or ".." in p.parts or "\\" in member.name:
                raise ValueError(f"Unsafe archive entry: {member.name}")
            if not member.isfile():
                continue
            rel = p.relative_to("data") if p.parts and p.parts[0] == "data" else p
            dest = (base_resolved / rel.as_posix()).resolve()
            if not dest.is_relative_to(base_resolved):
                raise ValueError(f"Path escape in archive: {member.name}")
            dest.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(member) as src, dest.open("wb") as out_f:
                while True:
                    chunk = src.read(1024 * 1024)
                    if not chunk:
                        break
                    out_f.write(chunk)


def _wait_for_idle(port: int, user: str, host: str, deadline: float, wait_minutes: int) -> None:
    consecutive_errors = 0
    while True:
        try:
            status = _check_vps_idle(port, user, host)
            consecutive_errors = 0
        except Exception as exc:
            consecutive_errors += 1
            if consecutive_errors >= 5 or time.monotonic() >= deadline:
                raise
            print(
                f"[pull] Transient poll error ({consecutive_errors}/5): {exc}; retrying in 15s...",
                flush=True,
            )
            time.sleep(15)
            continue
        if status.get("idle"):
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"VPS scanner stayed busy past {wait_minutes} minutes: {status}"
            )
        print(
            f"[pull] VPS scan active (current={status.get('current')}, pending={status.get('pending')}); waiting 15s...",
            flush=True,
        )
        time.sleep(15)


def pull_snapshot(
    host: str,
    port: int,
    user: str,
    remote_dir: str,
    out_dir: Path,
    wait_minutes: int = 15,
    keep_m3_reports: int = 10,
    download_only: bool = False,
) -> Dict[str, Any]:
    out_dir = Path(out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + max(0, wait_minutes) * 60

    stamp = datetime.now(TZ).strftime("%Y%m%d-%H%M%S")
    local_tar = out_dir / f"netease-vps-snapshot-{stamp}.tar.gz"
    local_sha = out_dir / f"netease-vps-snapshot-{stamp}.tar.gz.sha256"

    actual_sha: Optional[str] = None
    for attempt in range(1, 4):
        _wait_for_idle(port, user, host, deadline, wait_minutes)
        pre_sigs = _snapshot_signatures(port, user, host, remote_dir)
        if not pre_sigs:
            raise RuntimeError(f"No active scanner data files found under {remote_dir}/data")
        rel_paths = [item["rel"] for item in pre_sigs]
        total_raw_bytes = sum(int(item["size"]) for item in pre_sigs)
        print(
            f"[pull] Attempt {attempt}/3: streaming {len(rel_paths)} files ({total_raw_bytes / 1048576:.1f} MiB raw) directly over SSH...",
            flush=True,
        )
        actual_sha = _stream_tar_over_ssh(port, user, host, remote_dir, rel_paths, local_tar)
        post_sigs = _snapshot_signatures(port, user, host, remote_dir)
        post_status = _check_vps_idle(port, user, host)
        if post_sigs == pre_sigs and post_status.get("idle"):
            break
        local_tar.unlink(missing_ok=True)
        actual_sha = None
        if attempt == 3 or time.monotonic() >= deadline:
            raise RuntimeError("VPS scanner modified data mid-stream across 3 attempts.")
        print("[pull] VPS modified data mid-stream; waiting for next idle window...", flush=True)
        time.sleep(15)

    assert actual_sha is not None
    local_sha.write_text(f"{actual_sha}  {local_tar.name}\n", encoding="ascii")
    print(
        f"[pull] Snapshot streamed cleanly: {local_tar.name} ({local_tar.stat().st_size / 1048576:.2f} MiB compressed)",
        flush=True,
    )

    if download_only:
        return {
            "snapshot_tar": str(local_tar),
            "snapshot_sha256": actual_sha,
        }

    return process_local_snapshot(
        local_tar,
        out_dir=out_dir,
        expected_sha=actual_sha,
        keep_m3_reports=keep_m3_reports,
    )


def process_local_snapshot(
    archive_path: Path,
    out_dir: Path,
    expected_sha: Optional[str] = None,
    keep_m3_reports: int = 10,
) -> Dict[str, Any]:
    archive_path = Path(archive_path).resolve()
    out_dir = Path(out_dir).resolve()
    actual_sha = sha256_file(archive_path)
    if expected_sha and actual_sha != expected_sha.lower():
        raise ValueError(f"Snapshot SHA-256 mismatch: {actual_sha} != {expected_sha}")

    raw_data_dir = out_dir / "raw_data"
    if raw_data_dir.exists():
        shutil.rmtree(raw_data_dir)
    _safe_extract_raw(archive_path, raw_data_dir)

    github_state_dir = out_dir / "github_state"
    pack_manifest = pack_state(
        raw_data_dir,
        github_state_dir,
        keep_m3_reports=keep_m3_reports,
    )

    preview_dir = out_dir / "preview_site"
    standalone_html = out_dir / "latest-report.html"
    site_summary = build_site(raw_data_dir, preview_dir, standalone_path=standalone_html)

    return {
        "snapshot_tar": str(archive_path),
        "snapshot_sha256": actual_sha,
        "raw_data_dir": str(raw_data_dir),
        "github_state_dir": str(github_state_dir),
        "pack_manifest": pack_manifest,
        "site_summary": site_summary,
    }


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Pull live VPS scanner state into local backup + GitHub Release bundle.")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=22)
    parser.add_argument("--user", default="root")
    parser.add_argument("--remote-dir", default="/opt/netease-scanner")
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--wait-minutes", type=int, default=15)
    parser.add_argument("--keep-m3-reports", type=int, default=10)
    parser.add_argument("--download-only", action="store_true")
    parser.add_argument("--process-tar", type=Path, default=None)
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.process_tar:
        sha_file = Path(f"{args.process_tar}.sha256")
        expected = sha_file.read_text(encoding="ascii").split()[0] if sha_file.is_file() else None
        result = process_local_snapshot(
            args.process_tar,
            out_dir=args.out_dir,
            expected_sha=expected,
            keep_m3_reports=args.keep_m3_reports,
        )
    else:
        if not args.host:
            parser.error("--host is required unless --process-tar is supplied")
        result = pull_snapshot(
            host=args.host,
            port=args.port,
            user=args.user,
            remote_dir=args.remote_dir,
            out_dir=args.out_dir,
            wait_minutes=args.wait_minutes,
            keep_m3_reports=args.keep_m3_reports,
            download_only=args.download_only,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
