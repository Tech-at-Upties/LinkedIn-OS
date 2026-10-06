"""Declared one-page native startup -> actual Redis worker -> M2 HTTP.

Non-truncating project-owned stores; no periodic acquisition or budget reset.
"""
from dataclasses import replace
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from hashlib import sha256
import importlib.util
import json
import os
from pathlib import Path
import secrets
import re
import socket
import sqlite3
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


def _read_json(path):
    if not path.is_file() or path.stat().st_size > 100_000:
        raise ValueError('bounded_existing_declaration_required')
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result: raise ValueError('duplicate_declaration_key')
            result[key] = value
        return result
    value = json.loads(path.read_bytes(), object_pairs_hook=unique,
        parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite_declaration')))
    if type(value) is not dict:
        raise ValueError('declaration_object_required')
    return value


def select_declaration(args, *, grant_reader=None, clock=None):
    """Validate explicit selection before any service or experiment mutation."""
    # Resume only a pre-dispatch harness failure with its unchanged declaration.
    # Never replace/refund a consumed allowance or recreate source work.
    resume = None
    declared_path = None
    grant_path = None
    grant = None
    reference = None
    requested_count = None
    max_pages = 1
    now = (clock or (lambda: datetime.now(UTC)))()
    if len(args) in (2, 6) and args[0] == '--declared-grant':
        grant_path = Path(args[1]).resolve()
        if not grant_path.is_relative_to(ROOT / '.local'):
            raise ValueError('installed_grant_reference_outside_project')
        reference = _read_json(grant_path)
        if reference.get('project_root') != str(ROOT):
            raise ValueError('installed_grant_project_changed')
        if grant_reader is None:
            from xingestion.linkedin.live import GrantedBrowserCapture
            grant_reader = GrantedBrowserCapture
        selected = grant_reader(reference)
        grant = selected.manifest
        if grant.get('kind') == 'linkedin-observation-grant/3':
            flags = dict(zip(args[2::2], args[3::2]))
            if (len(args) != 6 or set(flags) != {'--requested-count', '--max-pages'}
                    or any(not re.fullmatch(r'[1-9][0-9]{0,4}', value) for value in flags.values())):
                raise ValueError('explicit_batch_count_and_page_budget_required')
            requested_count, max_pages = int(flags['--requested-count']), int(flags['--max-pages'])
            if not 2 <= max_pages <= grant['batch_page_budget']:
                raise ValueError('batch_job_budget_exceeds_grant')
            LinkedInCompanyFeedInput(grant['company_url'], grant['feed_publisher_id'],
                page_mode=grant['page_mode'], requested_count=requested_count, max_pages=max_pages).validate()
        elif len(args) != 2:
            raise ValueError('batch_count_flags_require_batch_grant')
        data = selected.source_journal.parent
        if (selected.source_journal.name != 'linkedin-source.sqlite' or not selected.source_journal.is_file()
                or not data.is_relative_to(ROOT / '.local') or not (data / 'm2.sqlite').is_file()):
            raise ValueError('original_grant_source_and_sink_required')
        if (grant['company_url'] != TARGET or grant['evidence_class'] != 'allowlisted_source_projection'
                or selected.expires_at <= now
                or selected.expires_at - now > timedelta(minutes=10)):
            raise ValueError('current_bounded_installed_grant_required')
        selected.consumer._pins(grant)
        if (ROOT / '.local/linkedin-acquisition-hold.json').exists():
            raise ValueError('installed_grant_security_hold')
        with sqlite3.connect(selected.source_journal.as_uri() + '?mode=ro', uri=True) as connection:
            state = connection.execute("SELECT generation,stopped FROM route_state WHERE scope='linkedin.company-feed'").fetchone()
            authority = connection.execute('SELECT authority_id FROM owned_sink_authority WHERE singleton=1').fetchone()
            if state != (reference['source_generation'], 0) or authority != (reference['authority_id'],):
                raise ValueError('installed_grant_source_context_changed')
            if connection.execute("SELECT 1 FROM responses WHERE scope='linkedin.company-feed' AND outcome IN ('dispatch_unknown','captured','classification_expired') LIMIT 1").fetchone():
                raise ValueError('installed_grant_source_outcome_unresolved')
        with sqlite3.connect(selected.budget_path.as_uri() + '?mode=ro', uri=True) as connection:
            slot = connection.execute('SELECT active_grant_id,profile,source_journal,authority_id FROM capture_profile_slot WHERE singleton=1').fetchone()
            stored = connection.execute('SELECT declaration_sha FROM capture_grants WHERE grant_id=?', (selected.grant_id,)).fetchone()
            consumed = connection.execute('SELECT 1 FROM capture_consumptions WHERE grant_id=?', (selected.grant_id,)).fetchone()
            if (slot != (selected.grant_id, str(selected.profile), str(selected.source_journal), reference['authority_id'])
                    or stored != (selected.declaration_sha,) or consumed):
                raise ValueError('installed_active_unconsumed_grant_required')
            if (connection.execute("SELECT 1 FROM sqlite_master WHERE name='capture_interventions'").fetchone()
                    and connection.execute('SELECT 1 FROM capture_interventions LIMIT 1').fetchone()):
                raise ValueError('installed_grant_security_hold')
    elif len(args) == 2 and args[0] == '--declared-manifest':
        declared_path = Path(args[1]).resolve()
        if (not declared_path.is_relative_to(ROOT / '.local') or not declared_path.is_file()
                or declared_path.name == 'live-manifest.json'):
            raise ValueError('explicit_successor_manifest_required')
        declaration = _read_json(declared_path)
        if Path(declaration['source_journal']).resolve().parent != declared_path.parent:
            raise ValueError('successor_must_retain_original_source_directory')
        digest = sha256(json.dumps(declaration, sort_keys=True, allow_nan=False).encode()).hexdigest()
        with sqlite3.connect((ROOT / '.local/linkedin-live-capture.sqlite').as_uri() + '?mode=ro', uri=True) as connection:
            row = connection.execute('SELECT declaration_sha, consumed_at FROM capture_allowance').fetchone()
            if row != (digest, None):
                raise ValueError('explicit_unconsumed_successor_declaration_required')
    elif len(args) == 2 and args[0] == '--resume':
        receipt_path = Path(args[1]).resolve()
        if not receipt_path.is_relative_to(ROOT / 'docs/results'):
            raise ValueError('resume_receipt_outside_project')
        resume = _read_json(receipt_path)
        if not re.fullmatch(r'native-startup-[a-f0-9]{32}', resume['experiment_id']) or resume.get('root_job_id'):
            raise ValueError('resume_requires_same_pre_dispatch_experiment')
        with sqlite3.connect((ROOT / '.local/linkedin-live-capture.sqlite').as_uri() + '?mode=ro', uri=True) as connection:
            if connection.execute('SELECT consumed_at FROM capture_allowance').fetchone()[0] is not None:
                raise ValueError('consumed_allowance_cannot_resume_source')
    elif args:
        raise ValueError('unsupported_experiment_arguments')
    if grant is None and resume is None and declared_path is None and (ROOT / '.local/linkedin-live-capture.sqlite').exists():
        raise RuntimeError('declared_profile_allowance_already_exists_do_not_reset')
    experiment = resume['experiment_id'] if resume else 'native-startup-' + uuid4().hex
    data = data if grant else declared_path.parent if declared_path else ROOT / '.local' / experiment
    if resume or declared_path or grant:
        if not data.is_dir(): raise ValueError('original_experiment_directory_absent')
    return {'resume': resume, 'declared_path': declared_path, 'grant_path': grant_path,
            'grant': grant, 'reference': reference, 'experiment': experiment, 'data': data,
            'requested_count': requested_count, 'max_pages': max_pages}


def collect_capture_metadata(connection, *, grant_id=None):
    """Read only metadata for this grant, preserving unrelated capture history."""
    result = {}
    parameters = ()
    predicate = ''
    if grant_id is not None:
        parameters = (grant_id,)
        predicate = ' WHERE attempt_id IN (SELECT attempt_id FROM capture_consumptions WHERE grant_id=?)'
        result['grant_consumption'] = [dict(zip(('grant_id', 'attempt_id', 'task_id', 'run_id', 'consumed_at'), row))
            for row in connection.execute('SELECT grant_id,attempt_id,task_id,run_id,consumed_at FROM capture_consumptions WHERE grant_id=?', parameters)]
    for key, columns in (
            ('capture_metadata', ('attempt_id', 'captured_at', 'native_sha', 'projection_sha', 'native_reads')),
            ('initial_capture_provenance', ('attempt_id', 'navigation_sha', 'wrapper_sha', 'decoded_sha', 'metadata'))):
        result[key] = [dict(zip(columns, row)) for row in connection.execute(
            'SELECT ' + ','.join(columns) + ' FROM ' + key + predicate, parameters)] if connection.execute(
                "SELECT 1 FROM sqlite_master WHERE name=?", (key,)).fetchone() else []
    return result


def page_delivery_wires(recovered):
    """Recover each original source attempt as a separately bounded HTTP page."""
    groups = {}
    for item in recovered['items']:
        attempt = item.get('acquisition', {}).get('source_attempt_id')
        if not isinstance(attempt, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,200}', attempt):
            raise ValueError('recovered_source_attempt_required')
        groups.setdefault(attempt, []).append(item)
    for items in groups.values():
        if len(items) > 10:
            raise ValueError('recovered_source_page_exceeds_http_bound')
        result = deepcopy(recovered)
        result['items'] = deepcopy(items)
        paging = items[0]['source_fields']['page_paging']
        if any(item['source_fields']['page_paging'] != paging for item in items):
            raise ValueError('recovered_source_page_scope_changed')
        result['coverage'] = {'state': 'partial', 'source_complete': None, 'ordering': 'relevance',
            'paging': deepcopy(paging), 'issues': deepcopy(items[0]['coverage'].get('gaps', [])),
            'limitations': deepcopy(items[0]['coverage'].get('limitations', []))}
        yield result


def main():
    selection = select_declaration(sys.argv[1:])
    resume, declared_path, grant_path, grant, experiment, data = (
        selection[key] for key in ('resume', 'declared_path', 'grant_path', 'grant', 'experiment', 'data'))
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
    if not (resume or declared_path or grant): data.mkdir()
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
    if grant:
        result.update(grant_id=grant['grant_id'], declaration_sha256=selection['reference']['declaration_sha256'], page_mode=grant.get('page_mode', 'api_following'),
            target=grant['company_url'], company_urn=grant['feed_publisher_id'], feed_request_urn=grant['feed_request_urn'])
        if selection['requested_count'] is not None:
            result.update(requested_count=selection['requested_count'], max_pages=selection['max_pages'])
    try:
        listener.bind(('127.0.0.1', 0)); listener.listen(10)
        endpoint = f'http://127.0.0.1:{listener.getsockname()[1]}/v1/linkedin/results'
        server = uvicorn.Server(uvicorn.Config(create_app(service=service), log_level='critical', lifespan='auto'))
        thread = Thread(target=server.run, kwargs={'sockets': [listener]}, daemon=True); thread.start()
        deadline = time.monotonic() + 5
        while not server.started and time.monotonic() < deadline: time.sleep(.01)
        if not server.started: raise RuntimeError('owned_m2_http_unavailable')
        path = grant_path or declared_path or data / 'live-manifest.json'
        manifest = grant or (json.loads(path.read_text(encoding='utf-8')) if resume or declared_path else dict(company_url=TARGET, feed_publisher_id=COMPANY, feed_request_urn=REQUEST_URN,
            evidence_class='allowlisted_source_projection', capture_budget=1, native_read_budget=12,
            expires_at=(datetime.now(UTC) + timedelta(minutes=10)).isoformat(), project_root=str(ROOT),
            nos_source_root=str(ROOT.parent / 'NOS-V1'),
            probe_sha256=sha256((ROOT / 'scripts/probe-linkedin-company.cjs').read_bytes()).hexdigest()))
        if resume is None and declared_path is None and grant is None: path.write_text(json.dumps(manifest), encoding='utf-8')
        stream = 'linkedin-native-startup:' + experiment
        config = replace(load_app_config(NOS / 'M1'), data_dir=data, sqlite_path=data / 'worker.sqlite',
            raw_evidence_dir=data / 'raw', secret_dir=data / 'secrets', session_registry_path=None,
            console_auth_file_path=None, default_credential_ref='env:NOS_LINKEDIN_NO_X_AUTH,NOS_LINKEDIN_NO_X_CT0,NOS_LINKEDIN_NO_X_BEARER',
            postgres_dsn=TASK_DSN, postgres_pool_min_size=1, postgres_pool_max_size=5,
            redis_url='redis://127.0.0.1:16379/15', redis_stream_key=stream,
            redis_consumer_group=experiment, redis_consumer_name='startup-worker', redis_claim_min_idle_ms=300000,
            linkedin_replay_path=None, linkedin_live_path=None if grant else path,
            linkedin_grant_path=grant_path, linkedin_m2_ingest_url=endpoint,
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
            capability_request=CapabilityRequest(CapabilityId.LINKEDIN_COMPANY_FEED, 2 if selection['requested_count'] is not None else 1,
                LinkedInCompanyFeedInput(manifest['company_url'], manifest['feed_publisher_id'],
                    page_mode=manifest.get('page_mode', 'api_following'), requested_count=selection['requested_count'],
                    max_pages=selection['max_pages'])), idempotency_key='northbound:local-startup:' + experiment,
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
            if selection['requested_count'] is not None:
                result['collection'] = wire['coverage']
            # Factory reconstruction and duplicate queue delivery must never
            # launch another capture. This is resource reopen, not process death.
            selected.linkedin_runtime = build_linkedin_runtime(config)
            selected.redis_client.xadd(stream, {'task_id': task.task_id})
            if selected.process_one().state != TaskState.DONE: raise RuntimeError('startup_duplicate_recovery_failed')
            recovered = selected.linkedin_runtime.result_for_task(selected.ledger.get_task(task.task_id))
            if recovered != wire: raise RuntimeError('startup_recovered_result_changed')
            for page_wire in page_delivery_wires(recovered):
                selected.linkedin_runtime.deliver(page_wire)
            result['reopened_duplicate_items'] = len(wire['items'])
        else:
            result['status'] = 'stopped_or_failed_no_source_retry'
        with selected.linkedin_runtime.journal.connect() as connection:
            result['source_attempts'] = [dict(row) for row in connection.execute('SELECT attempt_id, outcome, body_sha256, captured_at, expires_at FROM responses')]
        with sqlite3.connect((ROOT / '.local/linkedin-live-capture.sqlite').as_uri() + '?mode=ro', uri=True) as connection:
            result.update(collect_capture_metadata(connection, grant_id=grant['grant_id'] if grant else None))
        if grant and grant.get('kind') == 'linkedin-observation-grant/3' and result.get('grant_consumption'):
            spec = importlib.util.spec_from_file_location('batch_semantic_metadata', ROOT / 'docs/experiments/verify_batch_semantics.py')
            verifier = importlib.util.module_from_spec(spec); spec.loader.exec_module(verifier)
            try:
                result['semantic_metadata'] = verifier.summarize_batch(data / 'linkedin-source.sqlite',
                    result['grant_consumption'][0]['attempt_id'])
            except ValueError:
                result['semantic_metadata_status'] = 'retention_classification_or_validation_ineligible'
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
        result['limits'] = [('One declared bounded source batch; no termination/cadence claim'
                            if selection['requested_count'] is not None else 'One declared first page; no continuation/termination/cadence claim'),
            'Projected bytes; distinct discarded native-body digest',
            'Configured factory/actual Redis/M2 HTTP; reconstruction is same-process resource reopen',
            'Retention ingress_only; derived analysis disabled; no allowance reset']
        suffix = '-resume-' + uuid4().hex if resume else ''
        (ROOT / 'docs/results' / (experiment + suffix + '.json')).write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')


if __name__ == '__main__':
    print(json.dumps(main(), indent=2))
