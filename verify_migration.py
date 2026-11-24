"""Verify and safely extract the migration snapshot without overwriting data."""
import argparse
import hashlib
import json
import tarfile
from pathlib import Path, PurePosixPath


def validate_members(archive):
    seen = set()
    for member in archive.getmembers():
        path = PurePosixPath(member.name)
        if (member.name in seen or path.is_absolute() or ".." in path.parts or "\\" in member.name or
                not member.isfile() or (member.name != "MANIFEST.json" and not member.name.startswith("data/"))):
            raise ValueError("迁移包含不安全或重复的路径")
        seen.add(member.name)
    return seen


def verify_archive(path):
    with tarfile.open(path, "r:gz") as archive:
        names = validate_members(archive)
        stream = archive.extractfile("MANIFEST.json")
        if stream is None:
            raise ValueError("缺少文件清单")
        manifest = json.load(stream)
        expected = {entry["path"] for entry in manifest["files"]}
        if names != expected | {"MANIFEST.json"} or len(expected) != len(manifest["files"]):
            raise ValueError("文件清单与迁移包不一致")
        for record in manifest["files"]:
            member = archive.getmember(record["path"])
            if member.size != record["size"]:
                raise ValueError(f"文件大小不符：{record['path']}")
            digest = hashlib.sha256()
            with archive.extractfile(member) as source:
                for chunk in iter(lambda: source.read(4 * 1024 * 1024), b""):
                    digest.update(chunk)
            if digest.hexdigest() != record["sha256"]:
                raise ValueError(f"SHA-256 不符：{record['path']}")
        return manifest


def extract_verified(archive_path, target):
    target = Path(target).resolve()
    if (target / "data").exists() and any((target / "data").iterdir()):
        raise ValueError("目标 data 目录已有数据，拒绝覆盖；请停止服务并使用空目录")
    manifest = verify_archive(archive_path)
    target.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive_path, "r:gz") as archive:
        validate_members(archive)
        for member in archive.getmembers():
            if member.name == "MANIFEST.json":
                continue
            dest = target / member.name
            dest.parent.mkdir(parents=True, exist_ok=True)
            if not dest.resolve().is_relative_to(target) or dest.exists():
                raise ValueError("目标路径不安全或已存在")
            with archive.extractfile(member) as source, dest.open("xb") as output:
                for chunk in iter(lambda: source.read(4 * 1024 * 1024), b""):
                    output.write(chunk)
            import os
            os.utime(dest, (member.mtime, member.mtime))
        (target / "data" / "MIGRATION_MANIFEST.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    # Check the destination bytes after extraction as well.
    for record in manifest["files"]:
        digest = hashlib.sha256()
        with (target / record["path"]).open("rb") as file:
            for chunk in iter(lambda: file.read(4 * 1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != record["sha256"]:
            raise ValueError(f"落地文件校验失败：{record['path']}")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path)
    parser.add_argument("--extract-to", type=Path)
    args = parser.parse_args()
    result = extract_verified(args.archive, args.extract_to) if args.extract_to else verify_archive(args.archive)
    print(f"SHA-256 校验通过：{len(result['files'])} 个文件")
