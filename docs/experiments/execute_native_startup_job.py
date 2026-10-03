"""Declared one-page native startup -> actual Redis worker -> M2 HTTP.

Non-truncating project-owned stores; no periodic acquisition or budget reset.
"""
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
import importlib.util
import json
import os
from pathlib import Path
import secrets
import re
import socket
import subprocess
import sys
from threading import Thread
import time
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
NOS = ROOT / '.local/nos-integration'
sys.path[:0] = [str(NOS / 'M1/src'), str(NOS / 'M2/src')]

import psycopg
from pydantic import SecretStr
import uvicorn
from nos_m2.api import create_app
from nos_m2.service import M2Service
from nos_m2.settings import Settings
from xingestion.capabilities import CapabilityPlanner, CapabilityRequest, LinkedInCompanyFeedInput
from xingestion.capability_dispatch import queue_capability_request
from xingestion.capability_ids import CapabilityId
from xingestion.config import load_app_config
from xingestion.dispatch import RedisOutboxDispatcher
from xingestion.linkedin.bootstrap import build_linkedin_runtime
from xingestion.migrations import PostgresMigrationRunner
from xingestion.releases import ReleaseStore
from xingestion.tasks import TaskState
from xingestion.workers.worker_app import build_worker

TARGET = 'https://www.linkedin.com/company/linkedin/posts/'
COMPANY = 'urn:li:fsd_company:1337'
REQUEST_URN = 'urn:li:fsd_organizationalPage:1337'
DATABASE = 'nos_linkedin_startup_jobs'
TASK_DSN = 'postgresql://nos_test@127.0.0.1:15432/' + DATABASE


def main():
    spec = importlib.util.spec_from_file_location('startup_redis_owner', ROOT / 'scripts/local-verification-redis.py')
    owner = importlib.util.module_from_spec(spec); spec.loader.exec_module(owner)
    checked = owner.owned_client()
    if checked is None: raise RuntimeError('owned_redis_unavailable')
    checked.close()
    with psycopg.connect('postgresql://nos_test@127.0.0.1:15432/postgres', autocommit=True) as connection:
        if Path(connection.execute('SHOW data_directory').fetchone()[0]).resolve() != ROOT / '.local/verification-postgres':
            raise RuntimeError('unexpected_verification_cluster')
        if not connection.execute('SELECT 1 FROM pg_database WHERE datname=%s', (DATABASE,)).fetchone():
            connection.execute('CREATE DATABASE nos_linkedin_startup_jobs')
    # Resume only a pre-dispatch harness failure with its unchanged declaration.
    # Never replace/refund a consumed allowance or recreate source work.
    resume = None
    if len(sys.argv) == 3 and sys.argv[1] == '--resume':
        receipt_path = Path(sys.argv[2]).resolve()
        if not receipt_path.is_relative_to(ROOT / 'docs/results'):
            raise ValueError('resume_receipt_outside_project')
        resume = json.loads(receipt_path.read_text(encoding='utf-8'))
        if not re.fullmatch(r'native-startup-[a-f0-9]{32}', resume['experiment_id']) or resume.get('root_job_id'):
            raise ValueError('resume_requires_same_pre_dispatch_experiment')
        import sqlite3
        with sqlite3.connect(ROOT / '.local/linkedin-live-capture.sqlite') as connection:
            if connection.execute('SELECT consumed_at FROM capture_allowance').fetchone()[0] is not None:
                raise ValueError('consumed_allowance_cannot_resume_source')
    elif len(sys.argv) != 1:
        raise ValueError('unsupported_experiment_arguments')
    if resume is None and (ROOT / '.local/linkedin-live-capture.sqlite').exists():
        raise RuntimeError('declared_profile_allowance_already_exists_do_not_reset')
    experiment = resume['experiment_id'] if resume else 'native-startup-' + uuid4().hex
    data = ROOT / '.local' / experiment
    if resume:
        if not data.is_dir(): raise ValueError('original_experiment_directory_absent')
    else:
        data.mkdir()
    sink_path = data / 'm2.sqlite'
    token = secrets.token_urlsafe(32)
    previous = os.environ.get('LINKEDIN_M2_API_TOKEN')
    os.environ['LINKEDIN_M2_API_TOKEN'] = token
    service = M2Service(Settings(provider='unavailable', database_url='sqlite:///' + sink_path.as_posix(),
        api_token=SecretStr(token), api_request_log_path=str(data / 'm2-request-metadata.jsonl')))
    server = None; thread = None; selected = None; listener = socket.socket()
    started = time.monotonic()
    result = {'experiment_id': experiment, 'target': TARGET, 'company_urn': COMPANY,
        'feed_request_urn': REQUEST_URN, 'task_database': DATABASE,
        'evidence_class': 'allowlisted_source_projection', 'started_at': datetime.now(UTC).isoformat(),
        'source_root_revision': subprocess.check_output(['git', '-C', str(NOS), 'rev-parse', 'HEAD'], text=True).strip()}
    try:
        listener.bind(('127.0.0.1', 0)); listener.listen(10)
        endpoint = f'http://127.0.0.1:{listener.getsockname()[1]}/v1/linkedin/results'
        server = uvicorn.Server(uvicorn.Config(create_app(service=service), log_level='critical', lifespan='off'))
        thread = Thread(target=server.run, kwargs={'sockets': [listener]}, daemon=True); thread.start()
        deadline = time.monotonic() + 5
        while not server.started and time.monotonic() < deadline: time.sleep(.01)
        if not server.started: raise RuntimeError('owned_m2_http_unavailable')
        path = data / 'live-manifest.json'
        manifest = json.loads(path.read_text(encoding='utf-8')) if resume else dict(company_url=TARGET, feed_publisher_id=COMPANY, feed_request_urn=REQUEST_URN,
            evidence_class='allowlisted_source_projection', capture_budget=1, native_read_budget=12,
            expires_at=(datetime.now(UTC) + timedelta(minutes=10)).isoformat(), project_root=str(ROOT),
            nos_source_root=str(ROOT.parent / 'NOS-V1'),
            probe_sha256=sha256((ROOT / 'scripts/probe-linkedin-company.cjs').read_bytes()).hexdigest())
        if resume is None: path.write_text(json.dumps(manifest), encoding='utf-8')
        stream = 'linkedin-native-startup:' + experiment
        config = replace(load_app_config(NOS / 'M1'), data_dir=data, sqlite_path=data / 'worker.sqlite',
            raw_evidence_dir=data / 'raw', secret_dir=data / 'secrets', session_registry_path=None,
            console_auth_file_path=None, default_credential_ref='env:NOS_LINKEDIN_NO_X_AUTH,NOS_LINKEDIN_NO_X_CT0,NOS_LINKEDIN_NO_X_BEARER',
            postgres_dsn=TASK_DSN, postgres_pool_min_size=1, postgres_pool_max_size=5,
            redis_url='redis://127.0.0.1:16379/15', redis_stream_key=stream,
            redis_consumer_group=experiment, redis_consumer_name='startup-worker', redis_claim_min_idle_ms=300000,
            linkedin_replay_path=None, linkedin_live_path=path, linkedin_m2_ingest_url=endpoint,
            pool_enabled=False, recapture_enabled=False)
        ReleaseStore(config.sqlite_path).approve_release('xrev-all-capabilities-merged-2026-08-22-1',
            reason='isolated_linkedin_startup_metadata_selection_not_x_source_verification')
        selected = build_worker(config=config, root=NOS / 'M1')
        selected.ledger.pool.wait(5)
        PostgresMigrationRunner(selected.ledger.pool, NOS / 'M1/src/xingestion/migrations/postgres_sql').apply()
        with selected.ledger.pool.connection() as connection:
            if connection.execute('SELECT COUNT(*) AS active FROM capability_tasks WHERE state NOT IN (%s,%s)',
                                  (TaskState.DONE.value, TaskState.DEAD_LETTER.value)).fetchone()['active']:
                raise RuntimeError('another_owned_startup_task_is_active')
        queued = queue_capability_request(ledger=selected.ledger, planner=CapabilityPlanner(None),
            capability_request=CapabilityRequest(CapabilityId.LINKEDIN_COMPANY_FEED, 1,
                LinkedInCompanyFeedInput(TARGET, COMPANY)), idempotency_key='northbound:local-startup:' + experiment,
            max_active_tasks_per_capability=1)
        if queued.task is None: raise RuntimeError('startup_job_backpressure')
        task = queued.task; result['root_job_id'] = task.task_id
        dispatched = RedisOutboxDispatcher(ledger=selected.ledger, redis_client=selected.redis_client, stream_key=stream).dispatch_once()
        if not dispatched.dispatched or dispatched.task_id != task.task_id: raise RuntimeError('unexpected_startup_outbox_delivery')
        outcome = selected.process_one()
        result['task_state'] = outcome.state.value if outcome.state else None
        if outcome.state == TaskState.DONE:
            wire = selected.linkedin_runtime.result_for_task(selected.ledger.get_task(task.task_id))
            result.update(status='current_bounded_startup_integrated', admitted=len(wire['items']),
                          coverage=wire['coverage'], expires_at=wire['expires_at'])
            # Factory reconstruction and duplicate queue delivery must never
            # launch another capture. This is resource reopen, not process death.
            selected.linkedin_runtime = build_linkedin_runtime(config)
            selected.redis_client.xadd(stream, {'task_id': task.task_id})
            if selected.process_one().state != TaskState.DONE: raise RuntimeError('startup_duplicate_recovery_failed')
            recovered = selected.linkedin_runtime.result_for_task(selected.ledger.get_task(task.task_id))
            if recovered != wire: raise RuntimeError('startup_recovered_result_changed')
            selected.linkedin_runtime.deliver(recovered)
            result['reopened_duplicate_items'] = len(wire['items'])
        else:
            result['status'] = 'stopped_or_failed_no_source_retry'
        with selected.linkedin_runtime.journal.connect() as connection:
            result['source_attempts'] = [dict(row) for row in connection.execute('SELECT attempt_id, outcome, body_sha256, captured_at, expires_at FROM responses')]
        import sqlite3
        with sqlite3.connect(ROOT / '.local/linkedin-live-capture.sqlite') as connection:
            result['capture_metadata'] = [dict(zip(('attempt_id','captured_at','native_sha','projection_sha','native_reads'), row))
                                        for row in connection.execute('SELECT * FROM capture_metadata')] if connection.execute("SELECT 1 FROM sqlite_master WHERE name='capture_metadata'").fetchone() else []
        result['pending_messages'] = selected.redis_client.xpending(stream, experiment)['pending']
        result['security_hold'] = (ROOT / '.local/linkedin-acquisition-hold.json').exists()
        result['source_journal'] = str((data / 'linkedin-source.sqlite').relative_to(ROOT))
        result['sink_database'] = str(sink_path.relative_to(ROOT))
        return result
    finally:
        if selected:
            selected.ledger.pool.close(); selected.redis_client.close()
        if server: server.should_exit = True
        if thread: thread.join(5)
        listener.close(); service.close()
        if previous is None: os.environ.pop('LINKEDIN_M2_API_TOKEN', None)
        else: os.environ['LINKEDIN_M2_API_TOKEN'] = previous
        result['finished_at'] = datetime.now(UTC).isoformat()
        result['elapsed_seconds'] = round(time.monotonic() - started, 3)
        result['limits'] = ['One declared first page; no continuation/termination/cadence claim',
            'Projected bytes; distinct discarded native-body digest',
            'Configured factory/actual Redis/M2 HTTP; reconstruction is same-process resource reopen',
            'Retention ingress_only; derived analysis disabled; no allowance reset']
        suffix = '-resume-' + uuid4().hex if resume else ''
        (ROOT / 'docs/results' / (experiment + suffix + '.json')).write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')


if __name__ == '__main__':
    print(json.dumps(main(), indent=2))
