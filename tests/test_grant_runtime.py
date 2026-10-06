"""Synthetic installed grants through M1 runtime; no browser or shared services."""
from contextlib import closing, contextmanager
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import UTC, datetime, timedelta
import json
import os
import socket
import sqlite3
from threading import Barrier, Thread
import time
from types import SimpleNamespace

import pytest

from test_initial_company_page import (PROJECT, TARGET, REQUEST_URN, COMPANY,
    project_manifest, install_capture_spy)
from nos_linkedin.acquisition import Journal
from nos_linkedin.pages import PageRunFailure
from xingestion.capabilities import CapabilityRequest, LinkedInCompanyFeedInput, LinkedInCompanyPlan
from xingestion.capability_ids import CapabilityId
from xingestion.linkedin.bootstrap import build_linkedin_runtime
from xingestion.linkedin.grants import GrantIssuer, declaration_sha, RECIPE
from xingestion.linkedin.live import BoundedBrowserCapture
from xingestion.tasks import TaskState
from test_job_runtime import ledger
from test_job_bootstrap import m2
from test_nos_integration import store
from test_redis_job_runtime import queue, dispatcher


@pytest.fixture
def installed(project_manifest, monkeypatch):
    root, live = project_manifest
    journal = Journal(root / '.local/linkedin-source.sqlite')
    journal.generation('linkedin.company-feed')
    with journal.connect() as connection:
        connection.execute('CREATE TABLE owned_sink_authority(singleton INTEGER PRIMARY KEY,authority_id TEXT)')
        connection.execute("INSERT INTO owned_sink_authority VALUES(1,'li-authority-runtime-synthetic')")
    budget = root / '.local/linkedin-live-capture.sqlite'
    with closing(sqlite3.connect(budget)):
        pass
    issuer = GrantIssuer(project_root=root, source_journal=journal.path,
        authority_id='li-authority-runtime-synthetic', source_generation=0)
    issuer.initialize()
    manifest = dict(kind='linkedin-observation-grant/2', page_mode='initial_document',
        grant_id='initial-grant-synthetic', previous_grant_id=None, workload_id='one-observation-synthetic',
        workload_position=0, declared_at=(datetime.now(UTC)-timedelta(seconds=1)).isoformat(),
        reason='explicit_observation', project_root=str(root), profile=str(issuer.profile),
        source_journal=str(journal.path), authority_id=issuer.authority, source_generation=0,
        company_url=TARGET, feed_publisher_id=COMPANY, feed_request_urn=REQUEST_URN,
        recipe=RECIPE, start=0, count=3, probe_sha256=live['probe_sha256'], helper_sha256=live['helper_sha256'],
        expires_at=(datetime.now(UTC)+timedelta(minutes=9)).isoformat(), capture_budget=1, native_read_budget=12,
        evidence_class='allowlisted_source_projection')
    workload = dict(workload_id=manifest['workload_id'], declaration_shas=[declaration_sha(manifest)], native_read_budget=12)
    issuer.declare(manifest, workload=workload)
    reference = dict(kind='installed-observation-grant/1', project_root=str(root), source_journal=str(journal.path),
        authority_id=issuer.authority, source_generation=0, grant_id=manifest['grant_id'],
        declaration_sha256=declaration_sha(manifest), nos_source_root=live['nos_source_root'])
    path = root / '.local/installed-reference.json'
    path.write_text(json.dumps(reference))
    config = SimpleNamespace(linkedin_grant_path=path, linkedin_live_path=None, linkedin_replay_path=None,
        data_dir=root / '.local', linkedin_m2_ingest_url='http://127.0.0.1:1/v1/linkedin/results')
    monkeypatch.setenv('LINKEDIN_M2_API_TOKEN', 'synthetic-unused-http-token')
    return SimpleNamespace(root=root, journal=journal, budget=budget, issuer=issuer,
                           manifest=manifest, reference=reference, path=path, config=config, live=live)


class SyntheticLedger:
    """Instrumented task guard; real PostgreSQL ledger belongs to root's lane."""
    def __init__(self, task_id='runtime-initial-synthetic'):
        payload = LinkedInCompanyFeedInput(TARGET, COMPANY, page_mode='initial_document')
        self.task = SimpleNamespace(task_id=task_id, capability_id=CapabilityId.LINKEDIN_COMPANY_FEED,
            state=TaskState.ENQUEUED, lease_token='synthetic-task-token', delivery_generation=0,
            attempt_count=1, max_attempts=2,
            request_json=CapabilityRequest(CapabilityId.LINKEDIN_COMPANY_FEED, 1, payload).public_dict(),
            plan_json=LinkedInCompanyPlan(page_mode='initial_document').public_dict())
        self.depth, self.cancelled = 0, False
    def acquire_execution_lease(self, task_id, **kwargs):
        assert task_id == self.task.task_id
        self.task.state = TaskState.RUNNING
        return self.task
    def get_task(self, task_id):
        return self.task
    @contextmanager
    def execution_guard(self, task_id, **kwargs):
        if self.cancelled:
            raise ValueError('synthetic-task-cancelled')
        assert task_id == self.task.task_id and kwargs['lease_token'] == self.task.lease_token
        self.depth += 1
        try:
            yield
        finally:
            self.depth -= 1
    def transition_task(self, task_id, **kwargs):
        self.task.state = kwargs['to_state']
        self.task.result_json = kwargs.get('result_json', {})
        return self.task


def runtime_for(case, deliveries):
    runtime = build_linkedin_runtime(case.config)
    runtime.deliver = deliveries.append
    # Acknowledgements are offline echoes, never HTTP requests.
    runtime.synchronize_fence = lambda value: value
    runtime.synchronize_task_fence = lambda value: value
    return runtime


def consumptions(case):
    with closing(sqlite3.connect(case.budget)) as connection:
        return connection.execute('SELECT grant_id,attempt_id,task_id,run_id FROM capture_consumptions').fetchall()


def test_actual_runtime_commits_grant_before_child_under_source_and_task_guards(installed, monkeypatch):
    case, calls, deliveries = installed, [], []
    ledger = SyntheticLedger()
    observed = []
    def at_child(receipt):
        rows = consumptions(case)
        assert len(rows) == 1 and rows[0][2:] == (ledger.task.task_id, ledger.task.task_id)
        assert ledger.depth == 1
        with closing(sqlite3.connect(case.journal.path, timeout=0)) as contender:
            with pytest.raises(sqlite3.OperationalError, match='locked'):
                contender.execute('BEGIN IMMEDIATE')
        # Read-only access sees the prior committed admission despite the writer.
        with closing(sqlite3.connect(case.journal.path)) as reader:
            assert reader.execute('SELECT outcome FROM responses WHERE attempt_id=?', (rows[0][1],)).fetchone() == ('dispatch_unknown',)
        observed.append(rows[0])
    install_capture_spy(monkeypatch, calls, mutate=at_child)
    runtime = runtime_for(case, deliveries)
    result = runtime.process_task(ledger=ledger, task=ledger.task, owner='synthetic-owner', lease_seconds=60)
    assert result.state == TaskState.DONE
    assert len(calls) == len(observed) == len(deliveries) == 1
    assert deliveries[0]['items'][0]['source_fields']['source_provenance']['probe_sha256'] == case.manifest['probe_sha256']
    assert result.result_json['result_mode'] == 'retained_source_journal' and 'items' not in result.result_json
    with closing(sqlite3.connect(case.budget)) as connection:
        assert not connection.execute("SELECT 1 FROM sqlite_master WHERE name='capture_allowance'").fetchone()


@pytest.mark.parametrize('fault', ['pin', 'deadline', 'source_stop', 'task_stop', 'other_unknown'])
def test_grant_runtime_fences_before_consumption_and_child(installed, monkeypatch, fault):
    case, calls = installed, []
    install_capture_spy(monkeypatch, calls)
    runtime = runtime_for(case, [])
    ledger = SyntheticLedger()
    if fault == 'pin':
        (case.root / 'scripts/company-bootstrap.cjs').write_text('synthetic changed pin')
    elif fault == 'deadline':
        runtime.send.expires_at = datetime.now(UTC)-timedelta(seconds=1)
    elif fault == 'source_stop':
        case.journal.stop('linkedin.company-feed', 'operator_stop')
    elif fault == 'task_stop':
        ledger.cancelled = True
    else:
        with case.journal.connect() as connection:
            connection.execute('INSERT INTO responses VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                ('other-unknown','linkedin.company-feed',0,None,None,None,'allowlisted_source_projection',None,
                 datetime.now(UTC).timestamp()+600,None,'dispatch_unknown'))
    result = runtime.process_task(ledger=ledger, task=ledger.task, owner='synthetic-owner', lease_seconds=60)
    assert result.state == TaskState.DEAD_LETTER
    assert calls == [] and consumptions(case) == []


def test_delivery_replay_and_completed_pull_use_retained_initial_provenance_without_grant_consumption(installed, monkeypatch):
    case, calls, deliveries = installed, [], []
    install_capture_spy(monkeypatch, calls)
    runtime = runtime_for(case, deliveries)
    ledger = SyntheticLedger()
    def ambiguous_delivery(wire):
        deliveries.append(wire)
        raise RuntimeError('synthetic lost acknowledgement')
    runtime.deliver = ambiguous_delivery
    assert runtime.process_task(ledger=ledger, task=ledger.task, owner='first', lease_seconds=60).state == TaskState.RETRY_SCHEDULED
    original = consumptions(case)
    with case.journal.connect() as connection:
        connection.execute('UPDATE company_runs SET lease_until=0')
    reopened = runtime_for(case, deliveries)
    ledger.task.attempt_count = 2
    ledger.task.lease_token = 'synthetic-recovery-token'
    assert reopened.process_task(ledger=ledger, task=ledger.task, owner='second', lease_seconds=60).state == TaskState.DONE
    assert len(calls) == 1 and consumptions(case) == original
    assert len(deliveries) == 2
    first = deliveries[0]['items'][0]['source_fields']['source_provenance']
    assert all(item['source_fields']['source_provenance'] == first for wire in deliveries for item in wire['items'])
    assert reopened.result_for_task(ledger.task)['items'][0]['source_fields']['source_provenance'] == first
    with case.journal.connect() as connection:
        connection.execute('UPDATE responses SET expires_at=0')
    with pytest.raises(PageRunFailure):
        reopened.result_for_task(ledger.task)
    with case.journal.connect() as connection:
        assert connection.execute('SELECT body FROM responses').fetchone()[0] is None
    assert len(calls) == 1 and consumptions(case) == original


def test_crash_after_committed_consumption_keeps_unknown_attempt_and_occupied_slot(installed, monkeypatch):
    case, calls = installed, []
    launches = []
    def simulated_death(receipt):
        launches.append(True)
        assert len(consumptions(case)) == 1
        raise SystemExit('synthetic owner death before response commit')
    install_capture_spy(monkeypatch, calls, mutate=simulated_death)
    runtime = runtime_for(case, [])
    ledger = SyntheticLedger()
    with pytest.raises(SystemExit):
        runtime.process_task(ledger=ledger, task=ledger.task, owner='first', lease_seconds=60)
    original = consumptions(case)
    with case.journal.connect() as connection:
        assert connection.execute('SELECT outcome,body FROM responses').fetchone()[:] == ('dispatch_unknown', None)
        connection.execute('UPDATE company_runs SET lease_until=0')
    with closing(sqlite3.connect(case.budget)) as connection:
        assert connection.execute('SELECT active_grant_id FROM capture_profile_slot').fetchone()[0] == case.manifest['grant_id']
    reopened = runtime_for(case, [])
    assert reopened.process_task(ledger=ledger, task=ledger.task, owner='second', lease_seconds=60).state == TaskState.DEAD_LETTER
    assert consumptions(case) == original and len(launches) == 1


@pytest.mark.parametrize('fault', ['extra_field', 'digest', 'journal', 'authority', 'live_also', 'replay_also'])
def test_installed_reference_configuration_never_registers_or_rebinds_grant(installed, monkeypatch, fault):
    case, calls = installed, []
    install_capture_spy(monkeypatch, calls)
    if fault == 'extra_field':
        case.reference['manifest'] = case.manifest
    elif fault == 'digest':
        case.reference['declaration_sha256'] = 'e'*64
    elif fault == 'journal':
        case.config.data_dir = case.root / '.local/other-data'
    elif fault == 'authority':
        case.reference['authority_id'] = 'wrong-authority'
    elif fault == 'live_also':
        case.config.linkedin_live_path = case.path
    else:
        case.config.linkedin_replay_path = case.path
    case.path.write_text(json.dumps(case.reference))
    before = case.budget.read_bytes()
    with pytest.raises(ValueError):
        build_linkedin_runtime(case.config)
    assert case.budget.read_bytes() == before and calls == []


def test_legacy_live_registration_refuses_after_explicit_migration_marker(installed):
    # Migration itself is verified in WR077. This synthetic selected marker
    # isolates the ordinary runtime registration guard from operator issuance.
    with closing(sqlite3.connect(installed.budget)) as connection, connection:
        connection.execute('INSERT INTO capture_legacy_imports VALUES(1,?,?,?,?)',
            ('a'*64, 'synthetic-legacy-id', datetime.now(UTC).isoformat(), '{}'))
    before = installed.budget.read_bytes()
    with pytest.raises(ValueError):
        BoundedBrowserCapture(installed.live)
    assert installed.budget.read_bytes() == before


def test_competing_actual_runtimes_share_one_committed_consumption_and_one_launch(installed, monkeypatch):
    calls, deliveries = [], []
    install_capture_spy(monkeypatch, calls)
    runtimes = [runtime_for(installed, deliveries), runtime_for(installed, deliveries)]
    ledgers = [SyntheticLedger('competing-initial-a'), SyntheticLedger('competing-initial-b')]
    barrier = Barrier(2)
    def competitor(index):
        barrier.wait(5)
        return runtimes[index].process_task(ledger=ledgers[index], task=ledgers[index].task,
            owner='synthetic-owner', lease_seconds=60).state
    with ThreadPoolExecutor(max_workers=2) as workers:
        outcomes = list(workers.map(competitor, range(2)))
    assert sorted(outcomes) == sorted([TaskState.DONE, TaskState.DEAD_LETTER])
    assert len(calls) == len(deliveries) == len(consumptions(installed)) == 1


@pytest.mark.parametrize('fault', ['timeout', 'invalid_receipt', 'security'])
def test_child_failure_preserves_consumed_grant_and_never_launches_on_recovery(installed, monkeypatch, fault):
    calls, launches = [], []
    def fail(receipt):
        launches.append(True)
        if fault == 'timeout':
            raise TimeoutError('synthetic child deadline')
        if fault == 'invalid_receipt':
            receipt['session_verified'] = False
        else:
            receipt['session_verified'] = False
            receipt['stopped'] = {'reason': 'challenge_path'}
    install_capture_spy(monkeypatch, calls, mutate=fail)
    runtime, ledger = runtime_for(installed, []), SyntheticLedger()
    assert runtime.process_task(ledger=ledger, task=ledger.task, owner='first', lease_seconds=60).state == TaskState.DEAD_LETTER
    before = consumptions(installed)
    assert len(before) == len(launches) == 1
    reopened = runtime_for(installed, [])
    assert reopened.process_task(ledger=ledger, task=ledger.task, owner='recovery', lease_seconds=60).state == TaskState.DEAD_LETTER
    assert len(launches) == 1 and consumptions(installed) == before
    with closing(sqlite3.connect(installed.budget)) as connection:
        assert connection.execute('SELECT active_grant_id FROM capture_profile_slot').fetchone()[0] == installed.manifest['grant_id']
    if fault == 'security':
        with installed.journal.connect() as connection:
            assert connection.execute('SELECT stopped,reason FROM route_state').fetchone()[:] == (1, 'challenge')


def test_normal_worker_factory_loads_installed_grant_without_x_credentials_or_source_launch(installed, monkeypatch):
    from xingestion.config import load_app_config
    from xingestion.releases import ReleaseStore
    from xingestion.workers import worker_app
    calls = []
    install_capture_spy(monkeypatch, calls)
    for key in list(os.environ):
        if key.startswith('XINGESTION_') or key in {'X_AUTH_TOKEN', 'X_CT0', 'X_BEARER'}:
            monkeypatch.delenv(key)
    monkeypatch.setenv('XINGESTION_DATA_DIR', str(installed.root / '.local'))
    monkeypatch.setenv('XINGESTION_LINKEDIN_GRANT_PATH', str(installed.path))
    monkeypatch.setenv('XINGESTION_LINKEDIN_M2_INGEST_URL', installed.config.linkedin_m2_ingest_url)
    config = load_app_config(PROJECT / '.local/nos-integration/M1')
    assert config.linkedin_grant_path == installed.path
    assert config.linkedin_live_path is config.linkedin_replay_path is None
    ReleaseStore(config.sqlite_path).approve_release(
        'xrev-all-capabilities-merged-2026-08-22-1', reason='synthetic_installed_grant_factory')
    pool, queue = SimpleNamespace(), SimpleNamespace()
    # Keep the real factory, SQLite stores, release resolution and LocalWorker.
    # The pool and Redis constructors are inert because no service is authorized.
    monkeypatch.setattr(worker_app, 'ConnectionPool', lambda *args, **kwargs: pool)
    monkeypatch.setattr(worker_app.redis.Redis, 'from_url', lambda *args, **kwargs: queue)
    before = installed.budget.read_bytes()
    worker = worker_app.build_worker(config=config, root=PROJECT / '.local/nos-integration/M1')
    assert worker.linkedin_runtime.send.grant_id == installed.manifest['grant_id']
    assert worker.linkedin_runtime.journal.path == installed.journal.path
    assert worker.auth.missing_fields() and worker.redis_client is queue
    assert calls == [] and installed.budget.read_bytes() == before


def test_installed_grant_actual_factory_queue_m2_http_and_retained_duplicate(installed, monkeypatch, ledger, queue, m2, store):
    """Designated service worker runs this case sequentially on owned services."""
    exercise_installed_grant_factory_queue_m2_http(installed, monkeypatch, ledger, queue, m2, store)


def exercise_installed_grant_factory_queue_m2_http(installed, monkeypatch, ledger, queue, m2, store,
        *, install_spy=install_capture_spy, requested_count=None, max_pages=1, expected_items=3, expected_attempts=1):
    """Shared single- and batch-grant configured seam, with owned service fixtures."""
    import uvicorn
    from xingestion.capabilities import CapabilityPlanner
    from xingestion.capability_dispatch import queue_capability_request
    from xingestion.config import load_app_config
    from xingestion.releases import ReleaseStore
    from xingestion.workers.worker_app import build_worker
    calls = []
    install_spy(monkeypatch, calls)
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen(10)
    server = uvicorn.Server(uvicorn.Config(m2[0], log_level='critical', lifespan='auto'))
    thread = Thread(target=server.run, kwargs={'sockets': [listener]}, daemon=True)
    thread.start()
    worker = reopened = None
    try:
        deadline = time.monotonic() + 5
        while not server.started and time.monotonic() < deadline:
            time.sleep(.01)
        assert server.started
        task_dsn = os.environ['XINGESTION_TEST_POSTGRES_DSN']
        for key in list(os.environ):
            if key.startswith('XINGESTION_') and key != 'XINGESTION_TEST_POSTGRES_DSN' or key in {'X_AUTH_TOKEN', 'X_CT0', 'X_BEARER'}:
                monkeypatch.delenv(key)
        for key, value in {
            'XINGESTION_DATA_DIR': str(installed.journal.path.parent),
            'XINGESTION_POSTGRES_DSN': task_dsn,
            'XINGESTION_REDIS_URL': 'redis://127.0.0.1:16379/15',
            'XINGESTION_REDIS_STREAM': queue[1],
            'XINGESTION_REDIS_CONSUMER_GROUP': 'installed-grant-verification',
            'XINGESTION_LINKEDIN_GRANT_PATH': str(installed.path),
            'XINGESTION_LINKEDIN_M2_INGEST_URL': f'http://127.0.0.1:{listener.getsockname()[1]}/v1/linkedin/results',
            'LINKEDIN_M2_API_TOKEN': 'local-http-test-token',
        }.items():
            monkeypatch.setenv(key, value)
        config = load_app_config(PROJECT / '.local/nos-integration/M1')
        ReleaseStore(config.sqlite_path).approve_release(
            'xrev-all-capabilities-merged-2026-08-22-1', reason='synthetic_installed_grant_actual_queue')
        worker = build_worker(config=config, root=PROJECT / '.local/nos-integration/M1')
        assert worker.auth.missing_fields()
        payload = LinkedInCompanyFeedInput(TARGET, COMPANY, page_mode='initial_document',
            requested_count=requested_count, max_pages=max_pages)
        task = queue_capability_request(ledger=ledger, planner=CapabilityPlanner(None),
            capability_request=CapabilityRequest(CapabilityId.LINKEDIN_COMPANY_FEED,
                1 if requested_count is None else 2, payload),
            idempotency_key='synthetic-installed-grant-queue', max_active_tasks_per_capability=2).task
        assert dispatcher(ledger, queue).dispatch_once().dispatched
        assert worker.process_one().state == TaskState.DONE
        original = consumptions(installed)
        assert len(calls) == len(original) == 1
        assert len(store.list_evidence()) == expected_items
        assert queue[0].xpending(queue[1], config.redis_consumer_group)['pending'] == 0
        reopened = build_worker(config=config, root=PROJECT / '.local/nos-integration/M1')
        wire = reopened.linkedin_runtime.result_for_task(ledger.get_task(task.task_id))
        assert len({item['acquisition']['source_attempt_id'] for item in wire['items']}) == expected_attempts
        if requested_count is not None:
            assert wire['coverage']['requested_count'] == wire['coverage']['returned_count'] == requested_count
            assert wire['coverage']['fulfilled'] and wire['coverage']['source_complete'] is None
        # M2 ingress accepts one source attempt per POST. Reconstruct the
        # acknowledged page subsets rather than POST the merged pull result.
        from nos_linkedin.parser import parse_company_feed
        from xingestion.linkedin.northbound import serialize_company_page
        from xingestion.linkedin.pages import select_company_items
        acknowledgements = []
        attempts = dict.fromkeys(item['acquisition']['source_attempt_id'] for item in wire['items'])
        current_task = ledger.get_task(task.task_id)
        for attempt_id in attempts:
            members = [item for item in wire['items'] if item['acquisition']['source_attempt_id'] == attempt_id]
            with reopened.linkedin_runtime.journal.connect() as connection:
                record = connection.execute('SELECT * FROM responses WHERE attempt_id=?', (attempt_id,)).fetchone()
            parsed = parse_company_feed(json.loads(record['body']), feed_publisher_id=COMPANY,
                observed_at=datetime.fromtimestamp(record['captured_at'], UTC),
                evidence_class=reopened.linkedin_runtime.evidence_class)
            page_wire = select_company_items(serialize_company_page(parsed, job_id=task.task_id),
                [item['item_id'] for item in members])
            page_wire = reopened.linkedin_runtime._wire(page_wire, task.task_id, attempt_id,
                members[0]['acquisition']['page_ordinal'], current_task.delivery_generation)
            response = reopened.linkedin_runtime.deliver(page_wire)
            acknowledgements.extend(response['receipts'])
        assert all(row['duplicate_delivery'] for row in acknowledgements)
        assert len(acknowledgements) == expected_items
        assert len(calls) == 1 and consumptions(installed) == original
        assert len(store.list_evidence()) == expected_items
        for item in wire['items']:
            assert len(store.list_observations('linkedin:' + item['item_id'])) == 1
    finally:
        for selected in (worker, reopened):
            if selected is not None:
                selected.ledger.pool.close()
                selected.redis_client.close()
        server.should_exit = True
        thread.join(5)
        listener.close()
