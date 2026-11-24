"""Build a curated source ZIP; never include actual records or credentials."""
import hashlib
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

ROOT = Path(__file__).resolve().parent
FILES = [
    "engine.py", "server.py", "m3_mode.py", "m3_artist_names.json", "slow_retry.py", "record_export.py", "probe.py", "port_engine.py", "requirements.txt",
    "Dockerfile", "Dockerfile.api", "api-package.json", "api-package-lock.json",
    "compose.yaml", "compose.tunnel.yaml", "Caddyfile", "Caddyfile.tunnel",
    "compose.low-resource.yaml", "compose-small.sh", "LOW_RESOURCE.md",
    "setup_swap.sh", "update_m3_schedule.py",
    ".env.example", ".env.tunnel.example", ".dockerignore", ".gitignore",
    "config.example.json", "secrets/music_cookie.example.txt", "templates/index.html",
    "deploy.sh", "prepare_migration.py", "verify_migration.py", "check_local_idle.ps1",
    "verify_containers.ps1", "tests/conftest.py", "tests/test_server.py", "tests/test_m3_mode.py",
    "tests/test_migration.py", "tests/test_slow_retry.py", "tests/test_schedule_update.py", "tests/test_record_sync.cjs", "static/records.js", "README.md", "VALIDATION.md", "package_release.py",
]


if __name__ == "__main__":
    target = ROOT / "netease-server-code.zip"
    with ZipFile(target, "w", ZIP_DEFLATED, compresslevel=9) as archive:
        for name in FILES:
            path = ROOT / name
            # Linux shell scripts must have LF even when generated on Windows.
            if name.endswith(".sh"):
                archive.writestr(name, path.read_bytes().replace(b"\r\n", b"\n"))
            else:
                archive.write(path, name)
    with ZipFile(target) as archive:
        assert archive.testzip() is None
        assert set(archive.namelist()) == set(FILES)
    digest = hashlib.sha256(target.read_bytes()).hexdigest()
    target.with_name(target.name + ".sha256").write_text(f"{digest}  {target.name}\n", encoding="ascii")
    print(f"Source ZIP verified: {len(FILES)} files, {target.stat().st_size:,} bytes, SHA-256 {digest}")
