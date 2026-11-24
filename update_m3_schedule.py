"""Idle-only update for the existing tunnel/small-host deployment."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import subprocess
from contextlib import closing
from copy import deepcopy
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zipfile import ZipFile
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Shanghai")
CUTOFF = date(2026, 11, 25)
SLOTS = tuple(
    f"{hour:02d}:{minute:02d}"
    for hour in range(12, 17)
    for minute in range(0, 60, 15)
    if (hour, minute) <= (16, 30)
)
FILES = ("engine.py", "server.py", "templates/index.html")
STATUS_CODE = """import json,urllib.request
s=json.load(urllib.request.urlopen('http://127.0.0.1:5000/api/status',timeout=15))
keys=('current','pending','schedule_enabled','schedule_times','schedule_start_dates',
      'schedule_end_dates','schedule_modes','defaults','scan_mode','thread_limit',
      'scheduler_error','accepting_scans')
print(json.dumps({k:s.get(k) for k in keys}))
"""


def updated_config(config, now):
    """Preserve existing settings; never catch up a newly introduced past slot."""
    result = deepcopy(config)
    now = now.astimezone(TZ)
    previous_slots = result.get("schedule_times", ["00:03"])
    starts = result.setdefault("schedule_start_dates", {})
    ends = result.setdefault("schedule_end_dates", {})
    modes = result.setdefault("schedule_modes", {})
    for slot in SLOTS:
        already_configured = (slot in previous_slots and modes.get(slot) == "m3_intersection"
                              and ends.get(slot) == CUTOFF.isoformat())
        if not already_configured:
            hour, minute = map(int, slot.split(":"))
            first = now.date() + timedelta(days=now.time().replace(tzinfo=None) >= time(hour, minute))
            # Keep a future user-defined activation; retries must not postpone it.
            starts[slot] = max(starts.get(slot, first.isoformat()), first.isoformat())
        modes[slot] = "m3_intersection"
        ends[slot] = CUTOFF.isoformat()
    result["schedule_times"] = sorted(set(previous_slots) | set(SLOTS))
    return result


def replace_bytes(path, content, metadata):
    temporary = path.with_name(path.name + ".m3-update.tmp")
    try:
        temporary.write_bytes(content)
        if hasattr(os, "chown"):
            os.chown(temporary, metadata.st_uid, metadata.st_gid)
        temporary.chmod(metadata.st_mode & 0o777)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--check", action="store_true")
    group.add_argument("--apply", action="store_true")
    parser.add_argument("--root", type=Path, default=Path("/opt/netease-scanner"))
    args = parser.parse_args()
    root = args.root.resolve()
    compose = ["bash", "compose-small.sh"]

    def command(*parts, capture=False):
        value = subprocess.run(parts, cwd=root, check=True, text=True,
                               stdout=subprocess.PIPE if capture else None)
        return value.stdout.strip() if capture else None

    def container():
        return command(*compose, "ps", "-q", "scanner", capture=True)

    def status():
        return json.loads(command("docker", "exec", container(), "python", "-c", STATUS_CODE, capture=True))

    def require_idle(value):
        if value["current"] or value["pending"]:
            raise RuntimeError("A scan is active or queued; wait until it finishes, then rerun.")
        if value["scheduler_error"] or not value["accepting_scans"]:
            raise RuntimeError("Service needs attention; update refused.")

    before = status()
    require_idle(before)
    bundle = root / "netease-server-code.zip"
    expected = (root / "netease-server-code.zip.sha256").read_text(encoding="ascii").split()[0]
    if hashlib.sha256(bundle.read_bytes()).hexdigest() != expected:
        raise RuntimeError("Source ZIP SHA-256 mismatch")
    with ZipFile(bundle) as archive:
        reviewed = {name: archive.read(name) for name in FILES}
    for name in ("engine.py", "server.py"):
        compile(reviewed[name], name, "exec")
    config_path = root / "config/config.json"
    original_config = config_path.read_bytes()
    config = json.loads(original_config.decode("utf-8-sig"))
    proposed = updated_config(config, datetime.now(TZ))
    print("PREFLIGHT_OK", json.dumps({key: proposed.get(key) for key in
          ("schedule_enabled", "schedule_times", "schedule_modes", "schedule_start_dates",
           "schedule_end_dates", "threads")}), flush=True)
    if args.check:
        return
    stamp = datetime.now(TZ).strftime("%Y%m%d-%H%M%S-%f")
    backup = root / "code_backups" / f"m3-daytime-before-{stamp}"
    backup.mkdir(parents=True, mode=0o700)
    originals = {name: ((root / name).read_bytes(), (root / name).stat()) for name in FILES}
    config_stat = config_path.stat()
    for name, (content, _) in originals.items():
        destination = backup / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(content)
    (backup / "config.json").write_bytes(original_config)
    (backup / "config.json").chmod(0o600)
    old_image = command("docker", "inspect", "--format", "{{.Image}}", container(), capture=True)
    image_name = command("docker", "inspect", "--format", "{{.Config.Image}}", container(), capture=True)
    rollback_tag = f"netease-scanner-m3-daytime-rollback:{stamp}"
    command("docker", "image", "tag", old_image, rollback_tag)
    stopped = False
    activated = False
    try:
        # The running container uses its image, so building does not interrupt it.
        for name, content in reviewed.items():
            replace_bytes(root / name, content, originals[name][1])
        command(*compose, "build", "scanner")
        require_idle(status())
        if config_path.read_bytes() != original_config:
            raise RuntimeError("Configuration changed during build; rerun update.")
        command(*compose, "stop", "-t", "40", "scanner")
        stopped = True
        database = root / "data/scan_history.sqlite3"
        with closing(sqlite3.connect(database)) as source:
            if source.execute("SELECT 1 FROM runs WHERE status IN ('running','queued') LIMIT 1").fetchone():
                raise RuntimeError("A scan started before shutdown; update refused.")
            with closing(sqlite3.connect(backup / "scan_history.sqlite3")) as destination:
                source.backup(destination)
        (backup / "scan_history.sqlite3").chmod(0o600)
        proposed = updated_config(config, datetime.now(TZ))
        # Validate against the newly built image before replacing live config.
        staged = root / "config/.m3-daytime-validation.json"
        try:
            replace_bytes(staged, (json.dumps(proposed, ensure_ascii=False, indent=2) + "\n").encode(), config_stat)
            command(*compose, "run", "--rm", "--no-deps", "--entrypoint", "python",
                    "scanner", "-c", "from server import Config; Config('/config/.m3-daytime-validation.json').read(); print('CONFIG_OK')")
        finally:
            staged.unlink(missing_ok=True)
        replace_bytes(config_path, (json.dumps(proposed, ensure_ascii=False, indent=2) + "\n").encode(), config_stat)
        activated = True
        command(*compose, "up", "-d", "--no-deps", "--no-build", "--wait", "--wait-timeout", "90", "scanner")
        after = status()
        for key in ("schedule_times", "schedule_modes", "schedule_start_dates", "schedule_end_dates"):
            if after[key] != proposed[key]:
                raise RuntimeError(f"Live setting mismatch: {key}")
        for key in ("scan_mode", "schedule_enabled", "thread_limit"):
            if after[key] != before[key]:
                raise RuntimeError(f"Existing setting changed: {key}")
        for key in ("threads", "recentLimit", "ownedLimit", "newAlbumProbeLimit"):
            if after["defaults"][key] != before["defaults"][key]:
                raise RuntimeError(f"Existing scan depth/concurrency changed: {key}")
        if after["scheduler_error"] or not after["accepting_scans"]:
            raise RuntimeError("Updated service reported an error")
        verify = "import hashlib; from pathlib import Path; expected=%r; assert all(hashlib.sha256((Path('/app')/n).read_bytes()).hexdigest()==d for n,d in expected.items())"
        command("docker", "exec", container(), "python", "-c", verify %
                {name: hashlib.sha256(content).hexdigest() for name, content in reviewed.items()})
        print("DEPLOYMENT_OK", json.dumps({k: after[k] for k in
              ("schedule_times", "schedule_modes", "schedule_start_dates", "schedule_end_dates", "schedule_enabled")}), flush=True)
        print("BACKUPS", backup, rollback_tag, flush=True)
    except BaseException:
        if activated:
            # Startup can legitimately catch up an existing original slot.
            # Never stop it or overwrite records to roll back a validation error.
            print("Service restarted; inspect it before rollback. Backups:", backup, rollback_tag, flush=True)
            raise
        for name, (content, metadata) in originals.items():
            replace_bytes(root / name, content, metadata)
        replace_bytes(config_path, original_config, config_stat)
        command("docker", "image", "tag", rollback_tag, image_name)
        if stopped:
            command(*compose, "up", "-d", "--no-deps", "--no-build", "scanner")
        print("ROLLED_BACK", backup, flush=True)
        raise


if __name__ == "__main__":
    main()
