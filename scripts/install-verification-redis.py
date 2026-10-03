"""Install one hash-verified development Redis artifact inside this project."""
import hashlib
import json
from pathlib import Path
import stat
from urllib.request import Request, urlopen
from uuid import uuid4
import zipfile

ROOT = Path(__file__).resolve().parents[1]
LOCAL = ROOT / '.local'
URL = 'https://github.com/redis-windows/redis-windows/releases/download/8.10.2/Redis-8.10.2-Windows-x64-cygwin.zip'
EXPECTED_SIZE = 14_790_472
EXPECTED_SHA = '6de5cc7f5adbf97b5928b13766383d4ad424626ef3d8b313ffff12d820ec6fc1'
ARCHIVE = LOCAL / 'Redis-8.10.2-Windows-x64-cygwin.zip'
TARGET = LOCAL / 'redis-8.10.2'


def main():
    LOCAL.mkdir(exist_ok=True)
    if not ARCHIVE.exists():
        temporary = LOCAL / ('redis-download-' + uuid4().hex + '.part')
        with urlopen(Request(URL, headers={'User-Agent': 'NOS-local-verification'}), timeout=30) as response, temporary.open('xb') as output:
            remaining = EXPECTED_SIZE + 1
            while remaining:
                chunk = response.read(min(1_048_576, remaining))
                if not chunk:
                    break
                output.write(chunk)
                remaining -= len(chunk)
        assert temporary.stat().st_size == EXPECTED_SIZE, 'Redis artifact size mismatch'
        with temporary.open('rb') as source:
            assert hashlib.file_digest(source, 'sha256').hexdigest() == EXPECTED_SHA, 'Redis artifact digest mismatch'
        temporary.rename(ARCHIVE)
    assert ARCHIVE.stat().st_size == EXPECTED_SIZE
    with ARCHIVE.open('rb') as source:
        assert hashlib.file_digest(source, 'sha256').hexdigest() == EXPECTED_SHA
    if not TARGET.exists():
        with zipfile.ZipFile(ARCHIVE) as archive:
            entries = archive.infolist()
            assert len(entries) <= 256 and sum(item.file_size for item in entries) <= 150_000_000
            for item in entries:
                assert (TARGET / item.filename).resolve().is_relative_to(TARGET.resolve()), 'Unsafe archive path'
                assert stat.S_IFMT(item.external_attr >> 16) != stat.S_IFLNK, 'Archive symlink is unsupported'
            archive.extractall(TARGET)
    # A verified cached archive must not bless changed executables/DLLs in an
    # existing directory. Refuse changes rather than overwriting user files.
    with zipfile.ZipFile(ARCHIVE) as archive:
        for item in archive.infolist():
            if not item.is_dir():
                destination = (TARGET / item.filename).resolve()
                assert destination.is_relative_to(TARGET.resolve()) and destination.is_file()
                with archive.open(item) as expected, destination.open('rb') as actual:
                    assert hashlib.file_digest(expected, 'sha256').digest() == hashlib.file_digest(actual, 'sha256').digest(), 'Extracted Redis file differs from verified archive'
    servers = list(TARGET.rglob('redis-server.exe'))
    assert len(servers) == 1 and servers[0].resolve().is_relative_to(LOCAL.resolve())
    result = {'project': str(ROOT), 'version': '8.10.2', 'source': URL, 'sha256': EXPECTED_SHA,
              'size': EXPECTED_SIZE, 'server': str(servers[0].resolve()),
              'scope': 'Portable local verification only; no service or production deployment'}
    (LOCAL / 'verification-redis-artifact.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
