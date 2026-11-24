"""Copy a stable, hash-verified snapshot; never modify source music records."""
import argparse
import hashlib
import io
import json
import tarfile
from datetime import datetime, timezone
from pathlib import Path

ACTIVE = {"VA_Tracks_Output.csv", "VA_Album_Date_Cache.json", "VA_Scan_Metrics.json", "VA_Scan_Tuning_Report.txt"}


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sources(source):
    files = []
    for path in source.iterdir():
        if path.is_file() and (path.name.startswith("VA_") or path.name in {"my_artists.json", "netease_api.out.log", "netease_api.err.log"}):
            if path.name.endswith(".tmp"):
                continue
            target = f"data/{path.name}" if path.name in ACTIVE else f"data/migration_archive/{path.name}"
            files.append((path, target))
    cache = source / "VA_Artist_Caches"
    if cache.is_dir():
        files.extend((path, "data/" + path.relative_to(source).as_posix()) for path in cache.rglob("*")
                     if path.is_file() and not path.name.endswith(".tmp"))
    return sorted(files, key=lambda item: item[1])


def prepare(source, output):
    source, output = Path(source).resolve(), Path(output).resolve()
    if output.exists():
        raise ValueError("迁移包已存在；请使用新的文件名，避免覆盖旧快照")
    files = sources(source)
    if not files:
        raise ValueError("未找到记录文件")
    output.parent.mkdir(parents=True, exist_ok=True)
    manifest = {"created_at": datetime.now(timezone.utc).isoformat(), "files": [], "artist_caches": []}
    print(f"正在核对并压缩 {len(files)} 个文件，共 {sum(p.stat().st_size for p, _ in files):,} 字节", flush=True)
    for path, target in files:
        stat = path.stat()
        record = {"source_name": path.relative_to(source).as_posix(), "path": target,
                  "size": stat.st_size, "mtime_ns": stat.st_mtime_ns, "sha256": sha256(path)}
        manifest["files"].append(record)
        if path.parent.name == "VA_Artist_Caches" and path.suffix == ".json":
            data = json.loads(path.read_text(encoding="utf-8-sig"))
            manifest["artist_caches"].append({"key": path.stem.removeprefix("artists_"), "count": data.get("count"),
                                              "saved_at": data.get("saved_at")})
    tmp = output.with_suffix(output.suffix + ".tmp")
    try:
        with tarfile.open(tmp, "w:gz", compresslevel=6) as archive:
            for path, target in files:
                archive.add(path, arcname=target, recursive=False)
            # Compare all source hashes again after all files have been copied.
            for (path, _), record in zip(files, manifest["files"]):
                stat = path.stat()
                if stat.st_size != record["size"] or stat.st_mtime_ns != record["mtime_ns"] or sha256(path) != record["sha256"]:
                    raise ValueError(f"复制期间文件发生变化：{record['source_name']}。请停止本地扫描后重试")
            content = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")
            info = tarfile.TarInfo("MANIFEST.json")
            info.size = len(content)
            info.mode = 0o600
            archive.addfile(info, io.BytesIO(content))
        # Verify actual archive contents, not merely the source files.
        from verify_migration import verify_archive
        verify_archive(tmp)
        tmp.replace(output)
        sidecar = output.with_name(output.name + ".sha256")
        sidecar.write_text(f"{sha256(output)}  {output.name}\n", encoding="ascii")
        output.with_name(output.name + ".manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"迁移包已验证：{output.name}，压缩后 {output.stat().st_size:,} 字节", flush=True)
        return manifest
    finally:
        tmp.unlink(missing_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--confirm-idle", action="store_true", help="确认本地扫描已停止后才能生成快照")
    args = parser.parse_args()
    if not args.confirm_idle:
        parser.error("请先停止本地扫描，再传入 --confirm-idle；生成过程还会二次核对源文件是否改变")
    prepare(args.source, args.output)
