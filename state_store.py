"""Compact, integrity-checked state persistence via GitHub Releases."""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import re
import shutil
import sqlite3
import sys
import tarfile
import tempfile
import time
from contextlib import closing
from datetime import date, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests

TZ = ZoneInfo("Asia/Shanghai")
CACHE_PATTERN = re.compile(r"artists_[0-9a-f]{16}\.json")
STATE_ARCHIVE_PATTERN = re.compile(r"scanner-state-(\d{8}-\d{6})(?:-\d+)?\.tar\.gz")
DAILY_BACKUP_PATTERN = re.compile(r"backup-(\d{4}-\d{2}-\d{2})\.tar\.gz")
CSV_HEADER = [
    "艺人 (Artist)",
    "单曲名 (Track)",
    "企划/专辑名 (Album/VA)",
    "发行日期 (Release Date)",
    "直达链接",
    "新歌标记 (Flag)",
]
ACTIVE_ROOT_FILES = (
    "VA_Tracks_Output.csv",
    "VA_Album_Date_Cache.json",
    "VA_Scan_Metrics.json",
    "VA_Scan_Tuning_Report.txt",
)
PRE_MIGRATION_REL = "migration_archive/pre_migration/VA_Tracks_Output.csv"
BASELINE_FILENAME = ".state_baseline.json"
MANIFEST_ASSET = "state-manifest.json"
ARCHIVE_MANIFEST = "STATE_MANIFEST.json"
DEFAULT_TAG = "scanner-state"


def now_cn() -> datetime:
    return datetime.now(TZ)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_allowed_relpath(rel_posix: str) -> bool:
    if rel_posix in ACTIVE_ROOT_FILES or rel_posix in {"scan_history.sqlite3", PRE_MIGRATION_REL}:
        return True
    parts = PurePosixPath(rel_posix).parts
    if len(parts) == 2 and parts[0] == "VA_Artist_Caches" and CACHE_PATTERN.fullmatch(parts[1]):
        return True
    return False


def compact_sqlite(src_db: Path, dst_db: Path, keep_m3_reports: int = 10) -> Dict[str, Any]:
    """Copy SQLite safely via backup API, prune old M3 match report blobs, and VACUUM."""
    src_db = Path(src_db)
    dst_db = Path(dst_db)
    if not src_db.is_file():
        raise FileNotFoundError(f"SQLite database not found: {src_db}")
    if keep_m3_reports < 0:
        raise ValueError("keep_m3_reports must be >= 0")

    dst_db.parent.mkdir(parents=True, exist_ok=True)
    dst_db.unlink(missing_ok=True)

    with closing(sqlite3.connect(src_db, timeout=30)) as source:
        with closing(sqlite3.connect(dst_db, timeout=30)) as dest:
            source.backup(dest)

    with closing(sqlite3.connect(dst_db, timeout=30)) as db:
        tables = {
            row[0]
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        pruned_reports = 0
        kept_reports = 0
        if "settings" in tables:
            if "runs" in tables:
                rows = db.execute(
                    """
                    SELECT s.key
                    FROM settings s
                    LEFT JOIN runs r
                      ON r.id = substr(s.key, length('m3_match_report:') + 1)
                    WHERE s.key LIKE 'm3_match_report:%'
                    ORDER BY COALESCE(r.created_at, '') DESC, s.key DESC
                    """
                ).fetchall()
            else:
                rows = db.execute(
                    "SELECT key FROM settings WHERE key LIKE 'm3_match_report:%' ORDER BY key DESC"
                ).fetchall()
            all_keys = [row[0] for row in rows]
            keep_set = set(all_keys[:keep_m3_reports]) if keep_m3_reports > 0 else set()
            drop_keys = [k for k in all_keys if k not in keep_set]
            kept_reports = len(keep_set)
            pruned_reports = len(drop_keys)
            if drop_keys:
                with db:
                    db.executemany("DELETE FROM settings WHERE key=?", [(k,) for k in drop_keys])
        # Ensure WAL is merged and deleted, then compact free pages.
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        db.execute("PRAGMA journal_mode=DELETE")
        db.execute("VACUUM")
        integrity = db.execute("PRAGMA integrity_check").fetchall()
        if integrity != [("ok",)]:
            raise ValueError(f"SQLite integrity check failed after compaction: {integrity}")

    for suffix in ("-wal", "-shm"):
        Path(str(dst_db) + suffix).unlink(missing_ok=True)

    return {"kept_m3_reports": kept_reports, "pruned_m3_reports": pruned_reports}


def _count_csv_rows(csv_path: Path) -> int:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header is None:
            raise ValueError(f"CSV file is empty: {csv_path}")
        if header[:6] != CSV_HEADER:
            raise ValueError(f"Unexpected CSV header in {csv_path}: {header}")
        return sum(1 for row in reader if any(cell != "" for cell in row))


def validate_data_dir(data_dir: Path, baseline_stats: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Inspect active data files, check internal consistency, and enforce non-regression."""
    data_dir = Path(data_dir)
    stats: Dict[str, Any] = {
        "csv_rows": 0,
        "pre_migration_csv_rows": 0,
        "album_cache_entries": 0,
        "artist_cache_files": 0,
        "sqlite_runs": 0,
        "sqlite_hits": 0,
        "sqlite_logs": 0,
        "sqlite_settings": 0,
        "sqlite_integrity_ok": True,
    }

    csv_path = data_dir / "VA_Tracks_Output.csv"
    if csv_path.is_file():
        stats["csv_rows"] = _count_csv_rows(csv_path)

    pre_csv = data_dir / PRE_MIGRATION_REL
    if pre_csv.is_file():
        stats["pre_migration_csv_rows"] = _count_csv_rows(pre_csv)

    album_cache_path = data_dir / "VA_Album_Date_Cache.json"
    if album_cache_path.is_file():
        with album_cache_path.open("r", encoding="utf-8") as handle:
            album_data = json.load(handle)
        if not isinstance(album_data, dict):
            raise ValueError("VA_Album_Date_Cache.json must contain a JSON object")
        stats["album_cache_entries"] = len(album_data)

    artist_dir = data_dir / "VA_Artist_Caches"
    if artist_dir.is_dir():
        count = 0
        for entry in sorted(artist_dir.iterdir()):
            if entry.is_file() and CACHE_PATTERN.fullmatch(entry.name):
                with entry.open("r", encoding="utf-8") as handle:
                    payload = json.load(handle)
                if not isinstance(payload, dict) or not isinstance(payload.get("artists"), list):
                    raise ValueError(f"Invalid artist cache payload: {entry.name}")
                count += 1
        stats["artist_cache_files"] = count

    db_path = data_dir / "scan_history.sqlite3"
    if db_path.is_file():
        with closing(sqlite3.connect(db_path, timeout=30)) as db:
            integrity = db.execute("PRAGMA integrity_check").fetchall()
            stats["sqlite_integrity_ok"] = integrity == [("ok",)]
            if not stats["sqlite_integrity_ok"]:
                raise ValueError(f"SQLite integrity check failed: {integrity}")
            tables = {
                row[0]
                for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            if "runs" in tables:
                stats["sqlite_runs"] = int(db.execute("SELECT COUNT(*) FROM runs").fetchone()[0])
            if "hits" in tables:
                stats["sqlite_hits"] = int(db.execute("SELECT COUNT(*) FROM hits").fetchone()[0])
            if "logs" in tables:
                stats["sqlite_logs"] = int(db.execute("SELECT COUNT(*) FROM logs").fetchone()[0])
            if "settings" in tables:
                stats["sqlite_settings"] = int(db.execute("SELECT COUNT(*) FROM settings").fetchone()[0])

    if baseline_stats:
        if not stats["sqlite_integrity_ok"]:
            raise ValueError("Regression check failed: SQLite integrity is not ok")
        if stats["csv_rows"] < int(baseline_stats.get("csv_rows", 0)):
            raise ValueError(
                f"Regression check failed: csv_rows shrank ({stats['csv_rows']} < {baseline_stats['csv_rows']})"
            )
        if stats["sqlite_runs"] < int(baseline_stats.get("sqlite_runs", 0)):
            raise ValueError(
                f"Regression check failed: sqlite_runs shrank ({stats['sqlite_runs']} < {baseline_stats['sqlite_runs']})"
            )
        if stats["sqlite_hits"] < int(baseline_stats.get("sqlite_hits", 0)):
            raise ValueError(
                f"Regression check failed: sqlite_hits shrank ({stats['sqlite_hits']} < {baseline_stats['sqlite_hits']})"
            )
        min_album_entries = int(int(baseline_stats.get("album_cache_entries", 0)) * 0.9)
        if stats["album_cache_entries"] < min_album_entries:
            raise ValueError(
                f"Regression check failed: album_cache_entries dropped sharply "
                f"({stats['album_cache_entries']} < {min_album_entries})"
            )

    return stats


def _collect_active_sources(data_dir: Path) -> List[Tuple[Path, str]]:
    items: List[Tuple[Path, str]] = []
    for name in ACTIVE_ROOT_FILES:
        path = data_dir / name
        if path.is_symlink():
            raise ValueError(f"Refusing symlink in data_dir: {name}")
        if path.is_file():
            items.append((path, name))

    artist_dir = data_dir / "VA_Artist_Caches"
    if artist_dir.is_symlink():
        raise ValueError("Refusing symlink for VA_Artist_Caches")
    if artist_dir.is_dir():
        for entry in sorted(artist_dir.iterdir()):
            if entry.is_symlink():
                raise ValueError(f"Refusing symlink in VA_Artist_Caches: {entry.name}")
            if entry.is_file() and CACHE_PATTERN.fullmatch(entry.name):
                items.append((entry, f"VA_Artist_Caches/{entry.name}"))

    pre_csv = data_dir / PRE_MIGRATION_REL
    if pre_csv.is_symlink():
        raise ValueError("Refusing symlink for pre_migration CSV")
    if pre_csv.is_file():
        items.append((pre_csv, PRE_MIGRATION_REL))

    return sorted(items, key=lambda pair: pair[1])


def load_baseline_stats(data_dir: Path) -> Optional[Dict[str, Any]]:
    path = Path(data_dir) / BASELINE_FILENAME
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def write_baseline_stats(data_dir: Path, stats: Dict[str, Any]) -> None:
    path = Path(data_dir) / BASELINE_FILENAME
    path.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")


def pack_state(
    data_dir: Path,
    out_dir: Path,
    keep_m3_reports: int = 10,
    asset_name: Optional[str] = None,
    baseline_stats: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Compact SQLite, validate integrity, and write tar.gz + sha256 + state-manifest.json."""
    data_dir = Path(data_dir).resolve()
    out_dir = Path(out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if baseline_stats is None:
        baseline_stats = load_baseline_stats(data_dir)

    stamp = now_cn().strftime("%Y%m%d-%H%M%S")
    archive_name = asset_name or f"scanner-state-{stamp}.tar.gz"
    if "/" in archive_name or "\\" in archive_name:
        raise ValueError("asset_name must be a bare filename")

    with tempfile.TemporaryDirectory(prefix="netease-state-pack-") as tmp_root:
        staging_data = Path(tmp_root) / "data"
        staging_data.mkdir(parents=True, exist_ok=True)

        sources = _collect_active_sources(data_dir)
        for src_path, rel_posix in sources:
            dest = staging_data / rel_posix
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_path, dest)

        db_src = data_dir / "scan_history.sqlite3"
        compaction_info: Dict[str, Any] = {"kept_m3_reports": 0, "pruned_m3_reports": 0}
        if db_src.is_symlink():
            raise ValueError("Refusing symlink for scan_history.sqlite3")
        if db_src.is_file():
            compaction_info = compact_sqlite(
                db_src, staging_data / "scan_history.sqlite3", keep_m3_reports=keep_m3_reports
            )

        stats = validate_data_dir(staging_data, baseline_stats=baseline_stats)

        staged_files: List[Tuple[Path, str]] = []
        for path in sorted(staging_data.rglob("*")):
            if path.is_file():
                rel = path.relative_to(staging_data).as_posix()
                if not _is_allowed_relpath(rel):
                    raise ValueError(f"Unexpected file in staging directory: {rel}")
                staged_files.append((path, rel))

        if not staged_files:
            raise ValueError(f"No active state files found to pack in {data_dir}")

        file_records: List[Dict[str, Any]] = []
        for path, rel in staged_files:
            file_records.append(
                {
                    "path": f"data/{rel}",
                    "rel_path": rel,
                    "size": path.stat().st_size,
                    "sha256": sha256_file(path),
                }
            )

        created_at = now_cn().isoformat(timespec="seconds")
        internal_manifest = {
            "schema": 1,
            "created_at": created_at,
            "compaction": compaction_info,
            "stats": stats,
            "files": file_records,
        }

        archive_path = out_dir / archive_name
        tmp_archive = out_dir / f".{archive_name}.tmp"
        try:
            with tmp_archive.open("wb") as raw_out:
                with gzip.GzipFile(filename="", mode="wb", compresslevel=6, mtime=0, fileobj=raw_out) as gz_out:
                    with tarfile.open(fileobj=gz_out, mode="w") as archive:
                        for path, rel in staged_files:
                            arcname = f"data/{rel}"
                            info = archive.gettarinfo(str(path), arcname=arcname)
                            info.uid = 0
                            info.gid = 0
                            info.uname = ""
                            info.gname = ""
                            info.mode = 0o644
                            with path.open("rb") as src_stream:
                                archive.addfile(info, src_stream)

                        manifest_bytes = json.dumps(
                            internal_manifest, ensure_ascii=False, indent=2
                        ).encode("utf-8")
                        m_info = tarfile.TarInfo(ARCHIVE_MANIFEST)
                        m_info.size = len(manifest_bytes)
                        m_info.mode = 0o644
                        m_info.mtime = int(time.time())
                        archive.addfile(m_info, io.BytesIO(manifest_bytes))

            archive_sha = sha256_file(tmp_archive)
            archive_size = tmp_archive.stat().st_size
            # Verify readable tar.gz before finalizing.
            _verify_archive_contents(tmp_archive)
            tmp_archive.replace(archive_path)
        finally:
            tmp_archive.unlink(missing_ok=True)

        sha_path = out_dir / f"{archive_name}.sha256"
        sha_path.write_text(f"{archive_sha}  {archive_name}\n", encoding="ascii")

        release_manifest = {
            "schema": 1,
            "created_at": created_at,
            "latest_asset": archive_name,
            "sha256": archive_sha,
            "size": archive_size,
            "compaction": compaction_info,
            "stats": stats,
            "files": file_records,
        }
        manifest_path = out_dir / MANIFEST_ASSET
        manifest_path.write_text(
            json.dumps(release_manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return release_manifest


def _validate_archive_members(archive: tarfile.TarFile) -> set[str]:
    seen: set[str] = set()
    for member in archive.getmembers():
        name = member.name
        path = PurePosixPath(name)
        if (
            name in seen
            or path.is_absolute()
            or ".." in path.parts
            or "\\" in name
            or not member.isfile()
        ):
            raise ValueError(f"Unsafe or non-regular archive entry: {name}")
        if name != ARCHIVE_MANIFEST:
            if not name.startswith("data/"):
                raise ValueError(f"Unexpected archive path outside data/: {name}")
            rel = name[len("data/"):]
            if not _is_allowed_relpath(rel):
                raise ValueError(f"Disallowed archive entry: {name}")
        seen.add(name)
    return seen


def _verify_archive_contents(archive_path: Path) -> Dict[str, Any]:
    with tarfile.open(archive_path, "r:gz") as archive:
        names = _validate_archive_members(archive)
        if ARCHIVE_MANIFEST not in names:
            raise ValueError(f"Missing {ARCHIVE_MANIFEST} in archive")
        stream = archive.extractfile(ARCHIVE_MANIFEST)
        if stream is None:
            raise ValueError(f"Could not read {ARCHIVE_MANIFEST}")
        manifest = json.load(stream)
        expected = {entry["path"] for entry in manifest["files"]}
        if names != expected | {ARCHIVE_MANIFEST} or len(expected) != len(manifest["files"]):
            raise ValueError("Archive entries do not match STATE_MANIFEST.json")
        for record in manifest["files"]:
            member = archive.getmember(record["path"])
            if member.size != record["size"]:
                raise ValueError(f"Size mismatch in archive for {record['path']}")
            digest = hashlib.sha256()
            with archive.extractfile(member) as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(chunk)
            if digest.hexdigest() != record["sha256"]:
                raise ValueError(f"SHA-256 mismatch in archive for {record['path']}")
        return manifest


def unpack_state(
    archive_path: Path,
    sha256_expected: Optional[str],
    data_dir: Path,
) -> Dict[str, Any]:
    """Verify archive SHA-256 and per-file hashes, then atomically populate data_dir."""
    archive_path = Path(archive_path).resolve()
    data_dir = Path(data_dir).resolve()
    if not archive_path.is_file():
        raise FileNotFoundError(f"Archive not found: {archive_path}")

    if sha256_expected:
        expected_norm = sha256_expected.strip().lower()
        actual_sha = sha256_file(archive_path)
        if actual_sha != expected_norm:
            raise ValueError(
                f"Archive SHA-256 mismatch: expected {expected_norm}, got {actual_sha}"
            )

    with tempfile.TemporaryDirectory(prefix="netease-state-unpack-") as tmp_root:
        tmp_data = Path(tmp_root) / "data"
        tmp_data.mkdir(parents=True, exist_ok=True)
        with tarfile.open(archive_path, "r:gz") as archive:
            names = _validate_archive_members(archive)
            if ARCHIVE_MANIFEST not in names:
                raise ValueError(f"Missing {ARCHIVE_MANIFEST} in archive")
            stream = archive.extractfile(ARCHIVE_MANIFEST)
            if stream is None:
                raise ValueError(f"Could not read {ARCHIVE_MANIFEST}")
            manifest = json.load(stream)
            expected = {entry["path"] for entry in manifest["files"]}
            if names != expected | {ARCHIVE_MANIFEST} or len(expected) != len(manifest["files"]):
                raise ValueError("Archive contents do not match STATE_MANIFEST.json")

            for record in manifest["files"]:
                member = archive.getmember(record["path"])
                if member.size != record["size"]:
                    raise ValueError(f"Archive entry size mismatch: {record['path']}")
                rel = record["path"][len("data/"):]
                dest = (tmp_data / rel).resolve()
                if not dest.is_relative_to(tmp_data.resolve()):
                    raise ValueError(f"Path traversal detected: {record['path']}")
                dest.parent.mkdir(parents=True, exist_ok=True)
                digest = hashlib.sha256()
                with archive.extractfile(member) as source, dest.open("wb") as out_f:
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        digest.update(chunk)
                        out_f.write(chunk)
                if digest.hexdigest() != record["sha256"]:
                    raise ValueError(f"Extracted SHA-256 mismatch: {record['path']}")

        stats = validate_data_dir(tmp_data)

        data_dir.mkdir(parents=True, exist_ok=True)
        for suffix in ("-wal", "-shm"):
            Path(str(data_dir / "scan_history.sqlite3") + suffix).unlink(missing_ok=True)

        for record in manifest["files"]:
            rel = record["path"][len("data/"):]
            src = tmp_data / rel
            dest = (data_dir / rel).resolve()
            if not dest.is_relative_to(data_dir):
                raise ValueError(f"Unsafe destination path: {rel}")
            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp_dest = dest.with_name(f".{dest.name}.unpack.tmp")
            shutil.copy2(src, tmp_dest)
            tmp_dest.replace(dest)

        write_baseline_stats(data_dir, stats)
        return {"manifest": manifest, "stats": stats}


class GitHubReleaseClient:
    """Minimal GitHub Releases REST client backed by requests.Session."""

    API_BASE = "https://api.github.com"

    def __init__(
        self,
        repo: str,
        token: str,
        tag: str = DEFAULT_TAG,
        session: Optional[requests.Session] = None,
    ) -> None:
        if not repo or "/" not in repo:
            raise ValueError("repo must be in 'owner/repo' format")
        if not token:
            raise ValueError("GitHub token is required")
        self.repo = repo
        self.tag = tag
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "netease-scanner-state-store/1.0",
            }
        )

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        kwargs.setdefault("timeout", 60)
        last_exc: Optional[Exception] = None
        for attempt in range(3):
            try:
                resp = self.session.request(method, url, **kwargs)
                if resp.status_code in (502, 503, 504) and attempt < 2:
                    time.sleep(2 * (attempt + 1))
                    continue
                return resp
            except requests.RequestException as exc:
                last_exc = exc
                if attempt < 2:
                    time.sleep(2 * (attempt + 1))
                    continue
                raise
        raise last_exc  # pragma: no cover

    def get_release(self) -> Optional[Dict[str, Any]]:
        url = f"{self.API_BASE}/repos/{self.repo}/releases/tags/{self.tag}"
        resp = self._request("GET", url)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def ensure_release(self) -> Dict[str, Any]:
        release = self.get_release()
        if release is not None:
            return release
        url = f"{self.API_BASE}/repos/{self.repo}/releases"
        resp = self._request(
            "POST",
            url,
            json={
                "tag_name": self.tag,
                "name": "Scanner Persistent State",
                "body": "Automated persistent cache & SQLite state storage for Netease Music Scanner.",
                "draft": False,
                "prerelease": True,
            },
        )
        if resp.status_code == 422:
            # Handle race condition if another workflow created it concurrently.
            release = self.get_release()
            if release is not None:
                return release
        resp.raise_for_status()
        return resp.json()

    def _download_asset(self, asset: Dict[str, Any], dest: Path) -> None:
        download_url = asset.get("url") or asset.get("browser_download_url")
        if not download_url:
            raise ValueError(f"Asset missing download URL: {asset.get('name')}")
        headers = {"Accept": "application/octet-stream"} if asset.get("url") else {}
        dest.parent.mkdir(parents=True, exist_ok=True)
        with self._request("GET", download_url, headers=headers, stream=True, timeout=180) as resp:
            resp.raise_for_status()
            with dest.open("wb") as out_f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        out_f.write(chunk)

    def _upload_asset(
        self, release: Dict[str, Any], path: Path, content_type: str = "application/octet-stream"
    ) -> Dict[str, Any]:
        upload_template = release.get("upload_url") or (
            f"https://uploads.github.com/repos/{self.repo}/releases/{release['id']}/assets{{?name,label}}"
        )
        upload_base = upload_template.split("{", 1)[0]
        local_size = path.stat().st_size
        url = f"{upload_base}?name={path.name}"
        for attempt in range(3):
            try:
                with path.open("rb") as stream:
                    resp = self._request(
                        "POST",
                        url,
                        headers={"Content-Type": content_type},
                        data=stream,
                        timeout=300,
                    )
                if resp.status_code in (502, 503, 504) and attempt < 2:
                    time.sleep(2 * (attempt + 1))
                    continue
                resp.raise_for_status()
                uploaded = resp.json()
                if int(uploaded.get("size", -1)) != local_size:
                    raise ValueError(
                        f"Uploaded size mismatch for {path.name}: expected {local_size}, got {uploaded.get('size')}"
                    )
                return uploaded
            except requests.RequestException:
                if attempt < 2:
                    time.sleep(2 * (attempt + 1))
                    continue
                raise
        raise RuntimeError(f"Failed to upload asset {path.name}")  # pragma: no cover

    def _delete_asset(self, asset: Dict[str, Any]) -> None:
        asset_url = asset.get("url") or f"{self.API_BASE}/repos/{self.repo}/releases/assets/{asset['id']}"
        resp = self._request("DELETE", asset_url)
        if resp.status_code != 404:
            resp.raise_for_status()

    def restore_from_release(self, data_dir: Path, allow_empty: bool = False) -> Dict[str, Any]:
        data_dir = Path(data_dir).resolve()
        release = self.get_release()
        if release is None or not release.get("assets"):
            if allow_empty:
                data_dir.mkdir(parents=True, exist_ok=True)
                print(
                    f"[state_store] Warning: Release '{self.tag}' missing or empty; initializing empty data_dir.",
                    file=sys.stderr,
                )
                return {"restored": False, "empty": True}
            raise RuntimeError(
                f"Persistent state release '{self.tag}' not found or has no assets; "
                "refusing to run without deduplication state (pass --allow-empty to override)."
            )

        assets = {asset["name"]: asset for asset in release.get("assets", [])}
        with tempfile.TemporaryDirectory(prefix="netease-state-restore-") as tmp_dir:
            tmp_path = Path(tmp_dir)
            target_asset_name: Optional[str] = None
            expected_sha: Optional[str] = None

            if MANIFEST_ASSET in assets:
                manifest_file = tmp_path / MANIFEST_ASSET
                self._download_asset(assets[MANIFEST_ASSET], manifest_file)
                manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
                target_asset_name = manifest.get("latest_asset")
                expected_sha = manifest.get("sha256")
            elif "scanner-state-latest.tar.gz" in assets:
                target_asset_name = "scanner-state-latest.tar.gz"
                sha_asset = assets.get("scanner-state-latest.tar.gz.sha256")
                if sha_asset:
                    sha_file = tmp_path / "latest.sha256"
                    self._download_asset(sha_asset, sha_file)
                    expected_sha = sha_file.read_text(encoding="ascii").split()[0]
            else:
                candidates = sorted(
                    name for name in assets if STATE_ARCHIVE_PATTERN.fullmatch(name)
                )
                if candidates:
                    target_asset_name = candidates[-1]

            if not target_asset_name or target_asset_name not in assets:
                if allow_empty:
                    data_dir.mkdir(parents=True, exist_ok=True)
                    print(
                        "[state_store] Warning: No usable state archive in release; initializing empty data_dir.",
                        file=sys.stderr,
                    )
                    return {"restored": False, "empty": True}
                raise RuntimeError(
                    f"Target state archive '{target_asset_name}' missing in release '{self.tag}'."
                )

            archive_local = tmp_path / target_asset_name
            self._download_asset(assets[target_asset_name], archive_local)
            unpacked = unpack_state(archive_local, expected_sha, data_dir)
            return {
                "restored": True,
                "empty": False,
                "asset": target_asset_name,
                "sha256": expected_sha,
                "stats": unpacked["stats"],
            }

    def upload_packed_dir(
        self,
        pack_dir: Path,
        keep_latest: int = 3,
        daily_retention: int = 14,
    ) -> Dict[str, Any]:
        """Atomically publish pre-packed state assets from pack_dir to GitHub Release."""
        pack_dir = Path(pack_dir).resolve()
        manifest_path = pack_dir / MANIFEST_ASSET
        if not manifest_path.is_file():
            raise FileNotFoundError(f"Missing {MANIFEST_ASSET} in {pack_dir}")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        archive_name = manifest["latest_asset"]
        archive_path = pack_dir / archive_name
        if not archive_path.is_file():
            raise FileNotFoundError(f"Missing archive {archive_name} in {pack_dir}")
        if sha256_file(archive_path) != manifest["sha256"]:
            raise ValueError("Packed archive SHA-256 does not match state-manifest.json")

        release = self.ensure_release()
        existing_assets = list(release.get("assets", []))
        existing_by_name = {asset["name"]: asset for asset in existing_assets}

        # Avoid same-second name collisions against existing assets on the release.
        if archive_name in existing_by_name:
            base = archive_name.removesuffix(".tar.gz")
            suffix = 1
            while f"{base}-{suffix}.tar.gz" in existing_by_name:
                suffix += 1
            new_name = f"{base}-{suffix}.tar.gz"
            new_path = pack_dir / new_name
            shutil.copy2(archive_path, new_path)
            archive_name = new_name
            archive_path = new_path
            manifest["latest_asset"] = new_name
            manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

        # Step 1: Upload new unique timestamped archive FIRST.
        uploaded_archive = self._upload_asset(release, archive_path, "application/gzip")

        # Step 2: Ensure today's Beijing daily backup asset exists.
        today_cn = now_cn().date()
        daily_name = f"backup-{today_cn.isoformat()}.tar.gz"
        daily_uploaded = False
        if daily_name not in existing_by_name:
            daily_copy = pack_dir / daily_name
            try:
                shutil.copy2(archive_path, daily_copy)
                self._upload_asset(release, daily_copy, "application/gzip")
                daily_uploaded = True
            finally:
                daily_copy.unlink(missing_ok=True)

        # Step 3: Atomically swap state-manifest.json AFTER archive is safely verified.
        if MANIFEST_ASSET in existing_by_name:
            self._delete_asset(existing_by_name[MANIFEST_ASSET])
        self._upload_asset(release, manifest_path, "application/json")

        # Step 4: Prune old state archives & expired daily backups (never delete newly uploaded).
        refreshed = self.get_release() or release
        pruned: List[str] = []
        state_archives = [
            asset
            for asset in refreshed.get("assets", [])
            if STATE_ARCHIVE_PATTERN.fullmatch(asset["name"])
        ]
        state_archives.sort(
            key=lambda a: (a.get("created_at", ""), a["name"]),
            reverse=True,
        )
        kept = 0
        for asset in state_archives:
            if asset["name"] == archive_name:
                kept += 1
                continue
            if kept < max(1, keep_latest):
                kept += 1
                continue
            self._delete_asset(asset)
            pruned.append(asset["name"])

        cutoff = today_cn - timedelta(days=daily_retention)
        for asset in refreshed.get("assets", []):
            match = DAILY_BACKUP_PATTERN.fullmatch(asset["name"])
            if match:
                backup_day = date.fromisoformat(match.group(1))
                if backup_day < cutoff:
                    self._delete_asset(asset)
                    pruned.append(asset["name"])

        return {
            "tag": self.tag,
            "latest_asset": uploaded_archive["name"],
            "size": uploaded_archive["size"],
            "sha256": manifest["sha256"],
            "daily_backup": daily_name if daily_uploaded else None,
            "pruned": pruned,
            "stats": manifest["stats"],
        }

    def save_to_release(
        self,
        data_dir: Path,
        keep_m3_reports: int = 10,
        keep_latest: int = 3,
        daily_retention: int = 14,
    ) -> Dict[str, Any]:
        with tempfile.TemporaryDirectory(prefix="netease-state-save-") as tmp_dir:
            pack_dir = Path(tmp_dir)
            pack_state(data_dir, pack_dir, keep_m3_reports=keep_m3_reports)
            return self.upload_packed_dir(
                pack_dir, keep_latest=keep_latest, daily_retention=daily_retention
            )


def _client_from_args(args: argparse.Namespace) -> GitHubReleaseClient:
    repo = args.repo or os.getenv("GITHUB_REPOSITORY", "")
    token = os.getenv(args.token_env, "")
    return GitHubReleaseClient(repo=repo, token=token, tag=args.tag)


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Manage compacted scanner state & GitHub Releases.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_pack = sub.add_parser("pack", help="Compact and pack data_dir into out_dir.")
    p_pack.add_argument("--data-dir", type=Path, required=True)
    p_pack.add_argument("--out-dir", type=Path, required=True)
    p_pack.add_argument("--keep-m3-reports", type=int, default=10)
    p_pack.add_argument("--asset-name", default=None)

    p_unpack = sub.add_parser("unpack", help="Verify and unpack archive into data_dir.")
    p_unpack.add_argument("--archive", type=Path, required=True)
    p_unpack.add_argument("--sha256", default=None)
    p_unpack.add_argument("--data-dir", type=Path, required=True)

    for name in ("restore", "save", "bootstrap-upload"):
        sp = sub.add_parser(name)
        sp.add_argument("--repo", default=None, help="GitHub owner/repo (defaults to GITHUB_REPOSITORY).")
        sp.add_argument("--tag", default=DEFAULT_TAG)
        sp.add_argument("--token-env", default="GITHUB_TOKEN")
        if name in ("restore", "save"):
            sp.add_argument("--data-dir", type=Path, required=True)
        if name == "restore":
            sp.add_argument("--allow-empty", action="store_true")
        if name == "save":
            sp.add_argument("--keep-m3-reports", type=int, default=10)
            sp.add_argument("--keep-latest", type=int, default=3)
            sp.add_argument("--daily-retention", type=int, default=14)
        if name == "bootstrap-upload":
            sp.add_argument("--pack-dir", type=Path, required=True)
            sp.add_argument("--keep-latest", type=int, default=3)
            sp.add_argument("--daily-retention", type=int, default=14)

    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.cmd == "pack":
        result = pack_state(
            args.data_dir,
            args.out_dir,
            keep_m3_reports=args.keep_m3_reports,
            asset_name=args.asset_name,
        )
    elif args.cmd == "unpack":
        expected = args.sha256
        if not expected:
            sidecar = Path(f"{args.archive}.sha256")
            if sidecar.is_file():
                expected = sidecar.read_text(encoding="ascii").split()[0]
        result = unpack_state(args.archive, expected, args.data_dir)
    elif args.cmd == "restore":
        client = _client_from_args(args)
        result = client.restore_from_release(args.data_dir, allow_empty=args.allow_empty)
    elif args.cmd == "save":
        client = _client_from_args(args)
        result = client.save_to_release(
            args.data_dir,
            keep_m3_reports=args.keep_m3_reports,
            keep_latest=args.keep_latest,
            daily_retention=args.daily_retention,
        )
    elif args.cmd == "bootstrap-upload":
        client = _client_from_args(args)
        result = client.upload_packed_dir(
            args.pack_dir,
            keep_latest=args.keep_latest,
            daily_retention=args.daily_retention,
        )
    else:  # pragma: no cover
        parser.error(f"Unknown command: {args.cmd}")
        return 2

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
