"""Own one loopback verification server; never stop/clear an unrelated Redis."""
import json
from pathlib import Path
import subprocess
import socket
import sys
import time

import redis

ROOT = Path(__file__).resolve().parents[1]
LOCAL = ROOT / '.local'
DATA = LOCAL / 'verification-redis-data'
CONFIG = LOCAL / 'verification-redis.conf'
PORT = 16379


def cygwin(path):
    absolute = path.resolve().as_posix()
    assert absolute[1:3] == ':/'
    return '/cygdrive/' + absolute[0].lower() + absolute[2:]


def owned_client():
    client = redis.Redis(host='127.0.0.1', port=PORT, socket_timeout=2, socket_connect_timeout=2, decode_responses=True)
    try:
        info = client.info('server')
    except (redis.ConnectionError, redis.TimeoutError):
        client.close()
        # Some Windows hosts time out even for unused loopback ports. A
        # successful temporary bind proves it is free; an occupied unknown
        # port must never be adopted or stopped.
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', PORT))
        return None
    if info.get('config_file', '').lower() != cygwin(CONFIG).lower():
        client.close()
        raise RuntimeError('Redis port belongs to another config; no mutation permitted')
    if client.config_get('dir').get('dir', '').lower() != cygwin(DATA).lower():
        client.close()
        raise RuntimeError('Redis data directory is not project owned')
    if info['redis_version'] != '8.10.2':
        raise RuntimeError('Unexpected verification Redis version')
    return client


def main():
    command = sys.argv[1] if len(sys.argv) == 2 else 'status'
    if command not in {'start', 'stop', 'status'}:
        raise ValueError('Choose start, stop or status')
    client = owned_client()
    if command == 'stop':
        if client is not None:
            client.shutdown(nosave=True)
            client.close()
        print(json.dumps({'running': False, 'port': PORT, 'scope': 'owned verification server'}))
        return
    if command == 'start' and client is None:
        artifact = json.loads((LOCAL / 'verification-redis-artifact.json').read_text())
        executable = Path(artifact['server']).resolve()
        assert executable.is_relative_to(LOCAL.resolve()) and artifact['project'] == str(ROOT)
        DATA.mkdir(exist_ok=True)
        marker = DATA / 'OWNER.json'
        if marker.exists():
            assert json.loads(marker.read_text()) == {'project': str(ROOT), 'role': 'verification-redis'}
        else:
            marker.write_text(json.dumps({'project': str(ROOT), 'role': 'verification-redis'}))
        config = '\n'.join([
            'bind 127.0.0.1', f'port {PORT}', 'protected-mode yes', 'databases 16',
            f'dir "{cygwin(DATA)}"', f'logfile "{cygwin(LOCAL / "verification-redis.log")}"',
            'appendonly yes', 'appendfsync always', 'save ""', 'daemonize no',
            'maxmemory 128mb', 'maxmemory-policy noeviction',
        ]) + '\n'
        if CONFIG.exists() and CONFIG.read_text() != config:
            raise RuntimeError('Existing verification config differs; preserve it for review')
        CONFIG.write_text(config)
        with (LOCAL / 'verification-redis-start-error.log').open('ab') as errors:
            subprocess.Popen([str(executable), cygwin(CONFIG)], cwd=executable.parent,
                             stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=errors,
                             creationflags=subprocess.CREATE_NO_WINDOW)
        deadline = time.monotonic() + 12
        while client is None and time.monotonic() < deadline:
            time.sleep(.1)
            client = owned_client()
        if client is None:
            raise RuntimeError('Owned Redis did not start; inspect private startup logs')
    print(json.dumps({'running': client is not None, 'port': PORT,
                      'version': client.info('server')['redis_version'] if client else None,
                      'data_directory': str(DATA), 'scope': 'owned verification server'}))
    if client:
        client.close()


if __name__ == '__main__':
    main()
