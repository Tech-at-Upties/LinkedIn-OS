"""Declared browser startup with controlled subprocesses; no LinkedIn reads."""
from datetime import UTC, datetime, timedelta
from hashlib import sha256
import json
import multiprocessing
import os
from pathlib import Path
import socket
import sqlite3
from threading import Barrier, Thread
import time
from types import SimpleNamespace

import pytest
import uvicorn

from test_parser import COMPANY, envelope
from test_job_bootstrap import m2
from test_job_runtime import ledger, create
from test_nos_integration import store, convert
from test_redis_job_runtime import queue, dispatcher, selected_worker
from xingestion.linkedin.bootstrap import build_linkedin_runtime
from xingestion.linkedin.live import BoundedBrowserCapture
from xingestion.capabilities import LinkedInCompanyFeedInput
from nos_linkedin.parser import RECIPE

TARGET = 'https://www.linkedin.com/company/linkedin/posts/'
ROOT = Path(__file__).resolve().parents[1]


def configuration(tmp_path, endpoint='http://127.0.0.1:1/v1/linkedin/results'):
    project = tmp_path / 'project'
    (project / '.local/linkedin-test-browser').mkdir(parents=True)
    (project / 'scripts').mkdir()
    script = project / 'scripts/probe-linkedin-company.cjs'
    script.write_bytes((ROOT / 'scripts/probe-linkedin-company.cjs').read_bytes())
    nos = tmp_path / 'nos'
    (nos / 'M3/node_modules/playwright').mkdir(parents=True)
    value = dict(company_url=TARGET, feed_publisher_id=COMPANY,
        feed_request_urn='urn:li:fsd_organizationalPage:1337',
        evidence_class='allowlisted_source_projection', capture_budget=1, native_read_budget=12,
        expires_at=(datetime.now(UTC) + timedelta(minutes=30)).isoformat(),
        project_root=str(project), nos_source_root=str(nos), probe_sha256=sha256(script.read_bytes()).hexdigest())
    path = tmp_path / 'live.json'
    path.write_text(json.dumps(value), encoding='utf-8')
    return SimpleNamespace(linkedin_live_path=path, data_dir=tmp_path, linkedin_m2_ingest_url=endpoint), value


def controlled_receipt():
    body = envelope()
    return dict(target=TARGET, evidence_class='allowlisted_source_projection',
        session_verified=True, stopped=None, account_writes_blocked=True, original_bodies_retained=False,
        native_reads_admitted=2, native_read_budget=12,
        responses=[dict(operation_id='voyagerOrganizationDashCompanies.synthetic', status=200,
            origin='https://www.linkedin.com', representation={'source_native_ids': [COMPANY]}),
        dict(operation_id='voyagerFeedDashOrganizationalPageUpdates.synthetic', status=200,
            request_parameters={'fields': {'count': 10, 'start': 3, 'organizationalPageUrn':
                {'present': True, 'value_sha256': sha256(b'urn:li:fsd_organizationalPage:1337').hexdigest()}}},
            origin='https://www.linkedin.com', method='GET', observed_at=datetime.now(UTC).isoformat(),
            body_sha256='a' * 64, content_type='application/vnd.linkedin.normalized+json+2.1',
            representation=dict(collection_projection={'status': 'captured', 'recipe': RECIPE,
                'fields': body['data']['data'][RECIPE]}, duplicate_entity_ids=[], traversal_bounded=False,
                projected_graph=body['included']))])


def spy(monkeypatch, calls, change=None):
    def run(args, **kwargs):
        calls.append((args, kwargs))
        value = controlled_receipt()
        if change:
            change(value)
        return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode())
    monkeypatch.setattr('xingestion.linkedin.live.subprocess.run', run)


def invoke(capture, attempt='source-one'):
    return capture(LinkedInCompanyFeedInput(TARGET, COMPANY), SimpleNamespace(start=3, count=10, attempt_id=attempt))


def test_single_capture_survives_reopen_and_different_journal(tmp_path, monkeypatch):
    config, value = configuration(tmp_path)
    calls = []
    monkeypatch.setenv('LINKEDIN_M2_API_TOKEN', 'local-http-test-token')
    monkeypatch.setenv('OPENAI_API_KEY', 'test-key-never-in-child')
    monkeypatch.setenv('DEBUG', 'test-diagnostics-never-in-child')
    spy(monkeypatch, calls)
    selected = BoundedBrowserCapture(value)
    response = invoke(selected)
    assert response.status == 200 and len(json.loads(response.body)['included']) == 3
    assert calls[0][0][-1] == '--runtime-capture' and '--continuation' not in calls[0][0]
    assert calls[0][1]['timeout'] == 85 and calls[0][1]['stderr'] is not None
    assert not {'LINKEDIN_M2_API_TOKEN', 'OPENAI_API_KEY', 'DEBUG'} & calls[0][1]['env'].keys()
    assert all(key in calls[0][1]['env'] for key in os.environ if key.upper() in {'PROGRAMFILES', 'PROGRAMFILES(X86)'})
    reopened = BoundedBrowserCapture(value)
    with pytest.raises(ValueError, match='already consumed'):
        invoke(reopened, 'different-source-attempt')
    assert len(calls) == 1
    with sqlite3.connect(selected.budget_path) as connection:
        row = connection.execute('SELECT * FROM capture_metadata').fetchone()
        assert row[0] == 'source-one' and row[2] == 'a' * 64 and row[3] == sha256(response.body).hexdigest()
        assert 'Own commentary' not in str(list(connection.iterdump()))


@pytest.mark.parametrize('fault', ['expired', 'wrong_class', 'two_captures', 'bool_budget', 'more_reads', 'hold', 'script_changed', 'new_declaration'])
def test_preflight_refuses_unproved_scope_without_launch(tmp_path, monkeypatch, fault):
    config, value = configuration(tmp_path)
    calls = []
    spy(monkeypatch, calls)
    if fault == 'expired': value['expires_at'] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    if fault == 'wrong_class': value['evidence_class'] = 'native_response'
    if fault == 'two_captures': value['capture_budget'] = 2
    if fault == 'bool_budget': value['capture_budget'] = True
    if fault == 'more_reads': value['native_read_budget'] = 13
    if fault == 'hold': (Path(value['project_root']) / '.local/linkedin-acquisition-hold.json').write_text('{}')
    if fault == 'script_changed': (Path(value['project_root']) / 'scripts/probe-linkedin-company.cjs').write_text('changed')
    if fault == 'new_declaration':
        BoundedBrowserCapture(value)
        value['expires_at'] = (datetime.now(UTC) + timedelta(minutes=31)).isoformat()
    with pytest.raises(ValueError): BoundedBrowserCapture(value)
    assert calls == []


@pytest.mark.parametrize('fault', ['timeout', 'unsuccessful', 'wrong_company', 'wrong_request_company', 'wrong_request_paging', 'unsigned', 'stop', 'too_many', 'stale', 'duplicate', 'wrong_origin', 'truncated'])
def test_failed_or_invalid_capture_never_refunds_allowance(tmp_path, monkeypatch, fault):
    config, value = configuration(tmp_path)
    capture = BoundedBrowserCapture(value)
    calls = []
    def change(receipt):
        if fault == 'wrong_company': receipt['responses'][0]['representation']['source_native_ids'] = ['urn:li:fsd_company:999']
        if fault == 'wrong_request_company': receipt['responses'][1]['request_parameters']['fields']['organizationalPageUrn']['value_sha256'] = 'f' * 64
        if fault == 'wrong_request_paging': receipt['responses'][1]['request_parameters']['fields']['start'] = 13
        if fault == 'unsigned': receipt['session_verified'] = False
        if fault == 'stop': receipt['stopped'] = {'reason': 'challenge_path'}
        if fault == 'too_many': receipt['native_reads_admitted'] = 13
        if fault == 'stale': receipt['responses'][1]['observed_at'] = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
        if fault == 'duplicate': receipt['responses'][1]['representation']['duplicate_entity_ids'] = ['urn:li:activity:123']
        if fault == 'wrong_origin': receipt['responses'][1]['origin'] = 'https://example.com'
        if fault == 'truncated': receipt['responses'][1]['representation']['traversal_bounded'] = True
    spy(monkeypatch, calls, change)
    if fault in {'timeout', 'unsuccessful'}:
        def failure(*args, **kwargs):
            calls.append(args)
            if fault == 'timeout': raise TimeoutError()
            return SimpleNamespace(returncode=1, stdout=b'{}')
        monkeypatch.setattr('xingestion.linkedin.live.subprocess.run', failure)
    with pytest.raises((ValueError, TimeoutError)): invoke(capture)
    with pytest.raises(ValueError, match='already consumed'): invoke(BoundedBrowserCapture(value), 'source-two')
    assert len(calls) == 1


def test_competing_capture_consumes_once(tmp_path, monkeypatch):
    config, value = configuration(tmp_path)
    first, second = BoundedBrowserCapture(value), BoundedBrowserCapture(value)
    calls, outcomes = [], []
    spy(monkeypatch, calls)
    barrier = Barrier(2)
    def execute(selected, attempt):
        barrier.wait(3)
        try: invoke(selected, attempt); outcomes.append('captured')
        except ValueError: outcomes.append('refused')
    threads = [Thread(target=execute, args=(first, 'one')), Thread(target=execute, args=(second, 'two'))]
    for thread in threads: thread.start()
    for thread in threads: thread.join(5)
    assert not any(thread.is_alive() for thread in threads)
    assert sorted(outcomes) == ['captured', 'refused'] and len(calls) == 1


@pytest.mark.parametrize('fault', ['hold', 'script', 'expiry'])
def test_physical_launch_rechecks_changes_after_startup(tmp_path, monkeypatch, fault):
    config, value = configuration(tmp_path)
    selected = BoundedBrowserCapture(value)
    calls = []; spy(monkeypatch, calls)
    if fault == 'hold': selected.hold.write_text('{}')
    if fault == 'script': selected.script.write_text('changed-after-startup')
    if fault == 'expiry': selected.expires_at = datetime.now(UTC) - timedelta(seconds=1)
    with pytest.raises(ValueError): invoke(selected)
    assert calls == []
    with sqlite3.connect(selected.budget_path) as connection:
        assert connection.execute('SELECT consumed_at FROM capture_allowance').fetchone()[0] is None


def test_security_hold_during_capture_rejects_even_successful_receipt(tmp_path, monkeypatch):
    config, value = configuration(tmp_path)
    selected = BoundedBrowserCapture(value)
    calls = []
    spy(monkeypatch, calls, lambda receipt: selected.hold.write_text('{"reason":"challenge"}'))
    with pytest.raises(ValueError, match='hold'): invoke(selected)
    assert len(calls) == 1
    with sqlite3.connect(selected.budget_path) as connection:
        assert connection.execute('SELECT consumed_at FROM capture_allowance').fetchone()[0] is not None


def die_after_consumption(value):
    import xingestion.linkedin.live as live
    live.subprocess.run = lambda *args, **kwargs: os._exit(73)
    invoke(BoundedBrowserCapture(value))


def test_abrupt_parent_death_cannot_start_another_capture(tmp_path, monkeypatch):
    config, value = configuration(tmp_path)
    process = multiprocessing.get_context('spawn').Process(target=die_after_consumption, args=(value,))
    process.start(); process.join(15)
    assert process.exitcode == 73
    calls = []
    spy(monkeypatch, calls)
    with pytest.raises(ValueError, match='already consumed'): invoke(BoundedBrowserCapture(value), 'after-crash')
    assert calls == []


def test_live_and_saved_configuration_conflict_before_optional_launch(tmp_path, monkeypatch):
    config, value = configuration(tmp_path)
    config.linkedin_replay_path = config.linkedin_live_path
    monkeypatch.setenv('LINKEDIN_M2_API_TOKEN', 'local-http-test-token')
    with pytest.raises(ValueError, match='Choose saved'): build_linkedin_runtime(config)
    assert not (tmp_path / 'linkedin-source.sqlite').exists()


def test_normal_worker_factory_loads_live_configuration_without_x_credentials(tmp_path, monkeypatch, ledger, queue):
    from xingestion.config import load_app_config
    from xingestion.workers.worker_app import build_worker
    from xingestion.releases import ReleaseStore
    config, value = configuration(tmp_path)
    for key in list(os.environ):
        if key.startswith('XINGESTION_') and key != 'XINGESTION_TEST_POSTGRES_DSN':
            monkeypatch.delenv(key)
    for key in ('X_AUTH_TOKEN', 'X_CT0', 'X_BEARER'):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv('XINGESTION_DATA_DIR', str(tmp_path / 'worker-data'))
    monkeypatch.setenv('XINGESTION_POSTGRES_DSN', os.environ['XINGESTION_TEST_POSTGRES_DSN'])
    monkeypatch.setenv('XINGESTION_REDIS_URL', 'redis://127.0.0.1:16379/15')
    monkeypatch.setenv('XINGESTION_REDIS_STREAM', queue[1])
    monkeypatch.setenv('XINGESTION_LINKEDIN_LIVE_PATH', str(config.linkedin_live_path))
    monkeypatch.setenv('XINGESTION_LINKEDIN_M2_INGEST_URL', config.linkedin_m2_ingest_url)
    monkeypatch.setenv('LINKEDIN_M2_API_TOKEN', 'local-http-test-token')
    calls = []; spy(monkeypatch, calls)
    selected_config = load_app_config(ROOT / '.local/nos-integration/M1')
    selected_config.data_dir.mkdir(parents=True, exist_ok=True)
    # Existing mixed NOS startup requires an explicit local protocol metadata
    # selection when its repository contains multiple X manifests. This
    # controlled selection does not assert current X recipe support.
    ReleaseStore(selected_config.sqlite_path).approve_release(
        'xrev-all-capabilities-merged-2026-08-22-1', reason='controlled_linkedin_startup_test')
    selected = build_worker(config=selected_config, root=ROOT / '.local/nos-integration/M1')
    try:
        assert selected.linkedin_runtime.evidence_class == 'allowlisted_source_projection'
        assert selected.auth.missing_fields() and calls == []
        assert selected.lease_seconds == 300
    finally:
        selected.ledger.pool.close(); selected.redis_client.close()


def test_live_projection_through_actual_redis_m2_http_and_reopen(tmp_path, monkeypatch, ledger, queue, m2, store):
    app, service = m2
    listener = socket.socket(); listener.bind(('127.0.0.1', 0)); listener.listen(10)
    server = uvicorn.Server(uvicorn.Config(app, log_level='critical', lifespan='off'))
    thread = Thread(target=server.run, kwargs={'sockets': [listener]}, daemon=True); thread.start()
    deadline = time.monotonic() + 5
    while not server.started and time.monotonic() < deadline: time.sleep(.01)
    assert server.started
    config, value = configuration(tmp_path, f'http://127.0.0.1:{listener.getsockname()[1]}/v1/linkedin/results')
    calls = []; spy(monkeypatch, calls)
    monkeypatch.setenv('LINKEDIN_M2_API_TOKEN', 'local-http-test-token')
    try:
        selected = build_linkedin_runtime(config)
        task = create(ledger)
        assert dispatcher(ledger, queue).dispatch_once().dispatched
        assert selected_worker(ledger, selected, queue).process_one().state.value == 'DONE'
        result = selected.result_for_task(ledger.get_task(task.task_id))
        assert result['items'][0]['source_fields']['evidence_class'] == 'allowlisted_source_projection'
        assert result['items'][0]['retention']['expires_at'] <= value['expires_at']
        assert store.admit(convert(result)[0]).duplicate_delivery
        reopened = build_linkedin_runtime(config)
        queue[0].xadd(queue[1], {'task_id': task.task_id})
        assert selected_worker(ledger, reopened, queue).process_one().state.value == 'DONE'
        assert reopened.result_for_task(ledger.get_task(task.task_id)) == result and len(calls) == 1
        another = create(ledger, key='different-job')
        assert dispatcher(ledger, queue).dispatch_once().dispatched
        assert selected_worker(ledger, reopened, queue).process_one().state.value == 'DEAD_LETTER'
        assert len(calls) == 1
    finally:
        server.should_exit = True; thread.join(5); listener.close()
    assert not thread.is_alive()
