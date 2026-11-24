"""Idle-only, byte-preserving exports of active desktop-compatible records."""
import hashlib
import json
import re
import shutil
import tempfile
import threading
import time
import uuid
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

ACTIVE_NAMES = ('VA_Tracks_Output.csv', 'VA_Album_Date_Cache.json',
                'VA_Scan_Metrics.json', 'VA_Scan_Tuning_Report.txt')
CACHE_PATTERN = re.compile(r'artists_[0-9a-f]{16}\.json')


def digest(path):
    sha = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            sha.update(block)
    return sha.hexdigest()


class ExportLimitError(ValueError):
    pass


class RecordExports:
    def __init__(self, data, ttl=3600, capacity=3):
        self.data = Path(data)
        self.ttl, self.capacity = ttl, capacity
        self.lock = threading.RLock()
        self.temp = None
        self.entries = {}

    def release(self, token):
        with self.lock:
            entry = self.entries.pop(token, None)
            if entry:
                shutil.rmtree(entry['folder'])

    def prune(self):
        for token, entry in list(self.entries.items()):
            if time.monotonic() - entry['time'] > self.ttl:
                self.release(token)

    def create(self, created_at):
        with self.lock:
            self.prune()
            if len(self.entries) >= self.capacity:
                raise ExportLimitError('已有多个回传包正在使用，请完成下载或稍后重试')
            if self.temp is None:
                self.temp = tempfile.TemporaryDirectory(prefix='netease-record-export-')
            token = uuid.uuid4().hex
            folder = Path(self.temp.name) / token
            folder.mkdir()
            try:
                paths = [self.data / name for name in ACTIVE_NAMES if (self.data / name).exists()]
                cache = self.data / 'VA_Artist_Caches'
                if cache.is_symlink():
                    raise ValueError('缓存目录不能是符号链接')
                if cache.is_dir():
                    paths += sorted(p for p in cache.iterdir() if CACHE_PATTERN.fullmatch(p.name))
                if not paths:
                    raise ValueError('当前没有可回传的记录文件')
                manifest = {'schema': 1, 'created_at': created_at, 'files': []}
                for path in paths:
                    if path.is_symlink() or not path.is_file():
                        raise ValueError('记录文件类型不正确')
                    relative = path.relative_to(self.data).as_posix()
                    dest = folder / relative
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    before = path.stat()
                    shutil.copyfile(path, dest)
                    sha = digest(dest)
                    after = path.stat()
                    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns) or digest(path) != sha:
                        raise ValueError('导出期间记录发生变化，请稍后重试')
                    manifest['files'].append({'path': relative, 'size': dest.stat().st_size, 'sha256': sha})
                (folder / 'MANIFEST.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
                self.entries[token] = {'folder': folder, 'manifest': manifest, 'time': time.monotonic()}
                return {'export_id': token, **manifest}
            except Exception:
                shutil.rmtree(folder)
                raise

    def file(self, token, name):
        with self.lock:
            self.prune()
            entry = self.entries.get(token)
            if not entry or name not in {f['path'] for f in entry['manifest']['files']}:
                return None
            return entry['folder'] / name

    def archive(self, token):
        with self.lock:
            self.prune()
            entry = self.entries.get(token)
            if not entry:
                return None
            target = entry['folder'] / 'records.zip'
            if not target.exists():
                with ZipFile(target, 'w', ZIP_DEFLATED, compresslevel=3) as archive:
                    archive.write(entry['folder'] / 'MANIFEST.json', 'MANIFEST.json')
                    for file in entry['manifest']['files']:
                        archive.write(entry['folder'] / file['path'], file['path'])
            return target

    def close(self):
        with self.lock:
            if self.temp:
                self.temp.cleanup()
                self.temp = None
            self.entries.clear()
