"""Real PostgreSQL job ownership and HTTP handlers, with explicit queue bypass.

Source input is synthetic; no Redis or LinkedIn request is claimed by this test.
"""
from datetime import UTC, datetime, timedelta
from http.server import ThreadingHTTPServer
import json
import os
from pathlib import Path
import sys
from threading import Event, Thread
from types import SimpleNamespace
from urllib.parse import urlparse

import httpx
import pytest

from test_nos_integration import SOURCE, convert, store
from test_parser import COMPANY, envelope
sys.path.insert(0, str(SOURCE / 'M1/tests'))
from postgres_fixture import make_postgres_ledger
from nos_linkedin.acquisition import Journal, SourceResponse
from xingestion.capabilities import CapabilityPlanner, CapabilityRequest, LinkedInCompanyFeedInput
from xingestion.capability_ids import CapabilityId
from xingestion.capability_dispatch import queue_capability_request
from xingestion.linkedin.runtime import LinkedInJobRuntime
from xingestion.tasks import TaskState
from xingestion.web import live_server
from xingestion.workers import LocalWorker


@pytest.fixture
def ledger():
    if not os.environ.get('XINGESTION_TEST_POSTGRES_DSN'):
        pytest.skip('Dedicated real PostgreSQL task database is not configured')
    result = make_postgres_ledger()
    yield result
    result.pool.close()


def runtime(tmp_path, sink, calls, *, status=200):
    def send(payload, lease):
        assert payload.company_url == 'https://www.linkedin.com/company/linkedin/posts/'
        assert lease.start == 3 and lease.count == 10
        calls.append(lease.attempt_id)
        return SourceResponse(status, 'application/json', json.dumps(envelope()).encode(), datetime.now(UTC))
    return LinkedInJobRuntime(journal=Journal(tmp_path / 'job-source.sqlite'), send=send,
                              deliver=sink, evidence_class='synthetic_fixture')


def create(ledger, key='northbound:client-a:one'):
    return queue_capability_request(ledger=ledger, planner=CapabilityPlanner(None),
        capability_request=CapabilityRequest(CapabilityId.LINKEDIN_COMPANY_FEED, 1,
            LinkedInCompanyFeedInput('https://www.linkedin.com/company/linkedin/posts/', COMPANY)),
        idempotency_key=key, max_active_tasks_per_capability=2).task


def worker(ledger, selected):
    return LocalWorker(ledger=ledger, manifest=None, auth=None, transport=None,
                       raw_evidence_sink=None, linkedin_runtime=selected, lease_seconds=60)


class Handler(live_server.LiveAppHandler):
    # Exercise actual authentication/visibility/planning/HTTP serialization;
    # Redis rate limits and outbox dispatch are separate, explicit test seams.
    def do_GET(self):
        self._handle_northbound_get(urlparse(self.path))
    def do_POST(self):
        self._handle_northbound_post(urlparse(self.path))
    def _enforce_northbound_rate_limit(self, *args, **kwargs):
        return None
    def _enforce_auth_failure_rate_limit(self):
        return None
    def _set_api_key_log_context(self, *args):
        pass
    def _record_api_response(self, **kwargs):
        pass
    def _process_ready_outbox(self, **kwargs):
        pass
    def log_message(self, *args):
        pass


@pytest.fixture
def api(ledger, monkeypatch):
    state = SimpleNamespace(ledger=ledger, planner=CapabilityPlanner(None), api_key_store=None,
        linkedin_runtime=None, config=SimpleNamespace(api_keys={'test-key-a': 'client-a', 'test-key-b': 'client-b'},
                                                     max_active_tasks_per_capability=2))
    monkeypatch.setattr(live_server, 'STATE', state, raising=False)
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    with httpx.Client(base_url=f'http://127.0.0.1:{server.server_port}',
                      headers={'Authorization': 'Bearer test-key-a'}, timeout=5) as client:
        yield state, client
    server.shutdown()
    server.server_close()
    thread.join(3)


BODY = {'capability_id': 'LINKEDIN_COMPANY_FEED', 'idempotency_key': 'one',
        'payload': {'company_url': 'https://www.linkedin.com/company/linkedin/posts/',
                    'feed_publisher_id': COMPANY}}


def test_http_disabled_invalid_and_caller_isolation(api, tmp_path, store):
    state, client = api
    calls = []
    assert client.post('/v1/jobs', json=BODY).status_code == 503
    assert state.ledger.active_task_count(capability_id=CapabilityId.LINKEDIN_COMPANY_FEED) == 0
    state.linkedin_runtime = runtime(tmp_path, lambda wire: None, calls)
    for bad in [True, 2, '1']:
        assert client.post('/v1/jobs', json={**BODY, 'payload': {**BODY['payload'], 'max_pages': bad}}).status_code == 400
    created = client.post('/v1/jobs', json=BODY)
    assert created.status_code == 202
    job = created.json()['job_id']
    assert client.post('/v1/jobs', json=BODY).json()['job_id'] == job
    for path in [f'/v1/jobs/{job}', f'/v1/results/{job}']:
        assert client.get(path, headers={'Authorization': 'Bearer test-key-b'}).status_code == 404
        assert client.get(path, headers={'Authorization': 'Bearer invalid'}).status_code == 401
    assert calls == []


def test_http_actual_job_worker_m2_results_have_same_identity(api, tmp_path, store):
    state, client = api
    calls, deliveries = [], []
    def sink(wire):
        deliveries.append([store.admit(item).duplicate_delivery for item in convert(wire)])
    state.linkedin_runtime = runtime(tmp_path, sink, calls)
    job = client.post('/v1/jobs', json=BODY).json()['job_id']
    assert worker(state.ledger, state.linkedin_runtime)._process_delivery(job).state == TaskState.DONE
    response = client.get(f'/v1/results/{job}')
    assert response.status_code == 200
    result = response.json()
    assert 'items' not in state.ledger.get_task(job).result_json
    assert result['source_type'] == 'linkedin' and result['state'] == 'SUCCEEDED'
    assert result['coverage']['source_complete'] is None
    assert result['items'][0]['acquisition']['root_job_id'] == job
    assert result['items'][0]['acquisition']['source_attempt_id'] == calls[0]
    assert store.admit(convert(result)[0]).duplicate_delivery
    assert store.get_evidence('linkedin:urn:li:activity:123')['origin']['m1_job_id'] == job
    assert deliveries == [[False]] and len(calls) == 1
    assert worker(state.ledger, state.linkedin_runtime)._process_delivery(job).state == TaskState.DONE
    assert len(calls) == 1


def test_task_crash_after_page_ack_recovers_result_without_source(ledger, tmp_path, store, monkeypatch):
    calls = []
    selected = runtime(tmp_path, lambda wire: [store.admit(item) for item in convert(wire)], calls)
    task = create(ledger)
    original = ledger.transition_task
    def crash(task_id, **kwargs):
        if kwargs['to_state'] == TaskState.DONE:
            raise SystemExit('controlled_crash_before_task_done')
        return original(task_id, **kwargs)
    monkeypatch.setattr(ledger, 'transition_task', crash)
    with pytest.raises(SystemExit):
        worker(ledger, selected)._process_delivery(task.task_id)
    monkeypatch.setattr(ledger, 'transition_task', original)
    with ledger.pool.connection() as connection:
        connection.execute("UPDATE capability_tasks SET lease_expires_at=now()-interval '1 second' WHERE task_id=%s", (task.task_id,))
    ledger.reclaim_expired_lease(task.task_id)
    assert worker(ledger, selected)._process_delivery(task.task_id).state == TaskState.DONE
    assert len(calls) == 1 and len(store.list_observations('linkedin:urn:li:activity:123')) == 1


def test_old_postgres_owner_fenced_before_physical_source(ledger, tmp_path, monkeypatch):
    calls, deliveries = [], []
    selected = runtime(tmp_path, lambda wire: deliveries.append(wire), calls)
    task = create(ledger)
    original = selected.runs.claim
    replacements = []
    def steal(*args, **kwargs):
        lease = original(*args, **kwargs)
        with ledger.pool.connection() as connection:
            connection.execute("UPDATE capability_tasks SET lease_expires_at=now()-interval '1 second' WHERE task_id=%s", (task.task_id,))
        ledger.reclaim_expired_lease(task.task_id)
        replacements.append(ledger.acquire_execution_lease(task.task_id, owner='replacement',
                    lease_expires_at=(datetime.now(UTC) + timedelta(seconds=60)).isoformat()))
        return lease
    monkeypatch.setattr(selected.runs, 'claim', steal)
    result = worker(ledger, selected)._process_delivery(task.task_id)
    assert result.state == TaskState.RUNNING
    assert ledger.get_task(task.task_id).lease_token == replacements[0].lease_token
    assert calls == [] and deliveries == []


def test_row_guard_holds_reclaim_until_physical_call_returns(ledger):
    task = create(ledger)
    task = ledger.transition_task(task.task_id, from_state=TaskState.CREATED, to_state=TaskState.ENQUEUED)
    task = ledger.acquire_execution_lease(task.task_id, owner='first',
                    lease_expires_at=(datetime.now(UTC) + timedelta(seconds=60)).isoformat())
    started, completed = Event(), Event()
    failures = []
    def reclaim():
        started.set()
        try:
            ledger.reclaim_expired_lease(task.task_id, now=(datetime.now(UTC) + timedelta(seconds=61)).isoformat())
        except Exception as exc:
            failures.append(type(exc).__name__)
        completed.set()
    with ledger.execution_guard(task.task_id, lease_token=task.lease_token, delivery_generation=task.delivery_generation):
        thread = Thread(target=reclaim)
        thread.start()
        assert started.wait(2)
        assert not completed.wait(.2)
    thread.join(3)
    assert completed.is_set() and failures == []
    with pytest.raises(ValueError, match='task_execution_fenced'):
        with ledger.execution_guard(task.task_id, lease_token=task.lease_token, delivery_generation=task.delivery_generation):
            pytest.fail('stale physical invocation')


def test_source_restriction_parks_job_without_automatic_retry(ledger, tmp_path):
    calls, deliveries = [], []
    selected = runtime(tmp_path, lambda wire: deliveries.append(wire), calls, status=403)
    task = create(ledger)
    assert worker(ledger, selected)._process_delivery(task.task_id).state == TaskState.DEAD_LETTER
    assert worker(ledger, selected)._process_delivery(task.task_id).state == TaskState.DEAD_LETTER
    assert len(calls) == 1 and deliveries == []
    assert ledger.get_task(task.task_id).next_attempt_at is None
    with pytest.raises(ValueError, match='original source attempt'):
        ledger.replay_task(task.task_id)


def test_postgres_expired_owner_cannot_finish_job(ledger):
    task = create(ledger)
    task = ledger.transition_task(task.task_id, from_state=TaskState.CREATED, to_state=TaskState.ENQUEUED)
    task = ledger.acquire_execution_lease(task.task_id, owner='expired',
                        lease_expires_at=(datetime.now(UTC) - timedelta(seconds=1)).isoformat())
    with pytest.raises(ValueError):
        ledger.transition_task(task.task_id, from_state=TaskState.RUNNING, to_state=TaskState.DONE,
            lease_token=task.lease_token, delivery_generation=task.delivery_generation, require_live_lease=True,
            result_json={'items': []}, clear_lease=True)
    assert ledger.get_task(task.task_id).state == TaskState.RUNNING


def test_completion_rechecks_expiry_after_waiting_for_task_lock(ledger):
    task = create(ledger)
    task = ledger.transition_task(task.task_id, from_state=TaskState.CREATED, to_state=TaskState.ENQUEUED)
    task = ledger.acquire_execution_lease(task.task_id, owner='short-lease',
                        lease_expires_at=(datetime.now(UTC) + timedelta(seconds=1)).isoformat())
    started, completed = Event(), Event()
    failures = []
    def finish():
        started.set()
        try:
            ledger.transition_task(task.task_id, from_state=TaskState.RUNNING, to_state=TaskState.DONE,
                lease_token=task.lease_token, delivery_generation=task.delivery_generation,
                require_live_lease=True, clear_lease=True, result_json={'items': []})
        except ValueError:
            failures.append('expired_after_lock_wait')
        completed.set()
    with ledger.execution_guard(task.task_id, lease_token=task.lease_token, delivery_generation=task.delivery_generation):
        thread = Thread(target=finish)
        thread.start()
        assert started.wait(2)
        assert not completed.wait(1.1)
    thread.join(3)
    assert completed.is_set() and failures == ['expired_after_lock_wait']
    assert ledger.get_task(task.task_id).state == TaskState.RUNNING


def test_generic_backpressure_has_no_source_dispatch(ledger):
    assert create(ledger) is not None
    assert create(ledger, 'northbound:client-a:two') is not None
    assert create(ledger, 'northbound:client-a:three') is None


@pytest.mark.parametrize('change', ['expired', 'source_stop', 'digest_mismatch'])
def test_http_completed_result_rechecks_source_eligibility(api, tmp_path, change):
    state, client = api
    calls = []
    selected = runtime(tmp_path, lambda wire: None, calls)
    state.linkedin_runtime = selected
    job = client.post('/v1/jobs', json=BODY).json()['job_id']
    assert worker(state.ledger, selected)._process_delivery(job).state == TaskState.DONE
    assert client.get(f'/v1/results/{job}').status_code == 200
    if change == 'source_stop':
        selected.journal.stop('linkedin.company-feed', 'operator_stop')
    else:
        with selected.journal.connect() as connection:
            if change == 'expired':
                connection.execute('UPDATE responses SET expires_at=0')
            else:
                connection.execute("UPDATE responses SET body_sha256='mismatch'")
    response = client.get(f'/v1/results/{job}')
    assert response.status_code == 410 and 'items' not in response.json()
    if change == 'expired':
        with selected.journal.connect() as connection:
            assert connection.execute('SELECT body FROM responses').fetchone()[0] is None
    assert len(calls) == 1
