"""Actual paused M2 HTTP arrival after M1 timeout and durable source stop."""
import asyncio
from datetime import UTC, datetime
import socket
from threading import Event, Thread
import time

import httpx
import pytest
from pydantic import SecretStr
import uvicorn
from sqlalchemy import select

from test_nos_integration import store
from test_job_runtime import ledger, create, runtime, worker
from nos_m2.api import create_app
from nos_m2.service import M2Service
from nos_m2.settings import Settings
from nos_m2.store import StoreError
from nos_m2.store import SourceFenceTable, Store
from test_owned_retention import retained
from test_nos_integration import convert


def fenced(authority='test-authority', generation=0, job='fenced-job'):
    value = retained(job=job)
    item = value.raw_payload['m1_item']
    item['source_fence'] = {'authority_id': authority, 'generation': generation, 'stopped': False}
    return convert({'source_type': 'linkedin', 'job_id': job, 'items': [item]})[0]


def test_transmitted_page_cannot_admit_after_remote_stop_ack(ledger, store, tmp_path):
    settings = Settings(provider='unavailable', api_token=SecretStr('local-fence-test-token'),
                        api_request_log_path=str(tmp_path / 'api-requests.jsonl'))
    service = M2Service(settings, store=store)
    app = create_app(service=service, settings=settings)
    arrived, release, finished = Event(), Event(), Event()
    statuses, calls = [], []
    @app.middleware('http')
    async def pause_publication(request, call_next):
        if request.url.path != '/v1/linkedin/results':
            return await call_next(request)
        # Buffer the transmitted body before the client times out. Otherwise
        # a disconnect can prevent parsing and does not prove late admission.
        assert await request.body()
        arrived.set()
        assert await asyncio.to_thread(release.wait, 8)
        try:
            response = await call_next(request)
            statuses.append(response.status_code)
            return response
        except StoreError:
            # A disconnected client cannot receive the late response. Record
            # the actual admission rejection, then check HTTP with redelivery.
            statuses.append('admission_fenced')
            raise
        finally:
            finished.set()
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen(10)
    base = f'http://127.0.0.1:{listener.getsockname()[1]}'
    server = uvicorn.Server(uvicorn.Config(app, log_level='critical', lifespan='off'))
    thread = Thread(target=server.run, kwargs={'sockets': [listener]}, daemon=True)
    thread.start()
    deadline = time.monotonic() + 5
    while not server.started and time.monotonic() < deadline:
        time.sleep(.01)
    assert server.started
    headers = {'Authorization': 'Bearer local-fence-test-token'}
    batches = []
    def deliver(batch):
        batches.append(batch)
        response = httpx.post(base + '/v1/linkedin/results', json=batch, headers=headers, timeout=.2)
        response.raise_for_status()
        return response.json()
    def synchronize(value):
        response = httpx.post(base + '/v1/linkedin/source-fence', json=value, headers=headers, timeout=3)
        response.raise_for_status()
        return response.json()
    selected = runtime(tmp_path, deliver, calls)
    selected.synchronize_fence = synchronize
    try:
        task = create(ledger)
        assert worker(ledger, selected)._process_delivery(task.task_id).state.value == 'RETRY_SCHEDULED'
        assert arrived.is_set()
        ack = selected.stop_source('operator_stop')
        assert ack['stopped'] and ack['generation'] == 1
        release.set()
        assert finished.wait(5)
        assert len(calls) == 1
        # Before the remote fence, the timed-out request still admitted one row.
        assert store.get_evidence('linkedin:urn:li:activity:123') is None
        assert statuses == ['admission_fenced']
        response = httpx.post(base + '/v1/linkedin/results', json=batches[0], headers=headers, timeout=3)
        assert response.status_code == 409
    finally:
        release.set()
        server.should_exit = True
        thread.join(5)
        listener.close()
        service.close()


def test_unknown_generation_stop_and_reopened_sink_fence_actual_admission(store):
    value = fenced()
    with pytest.raises(StoreError, match='unknown'):
        store.admit(value)
    assert store.list_evidence() == []
    active = {'authority_id': 'test-authority', 'generation': 0, 'stopped': False}
    assert store.update_source_fence(active) == active
    first = store.admit(value)
    assert store.admit(value).duplicate_delivery
    hold = {**active, 'generation': 1, 'stopped': True}
    assert store.update_source_fence(hold) == hold
    assert store.update_source_fence(hold) == hold
    for state in (active, {**active, 'generation': 2}, {**active, 'stopped': True}):
        with pytest.raises(StoreError, match='regress|reopen'):
            store.update_source_fence(state)
    with pytest.raises(StoreError, match='stopped'):
        store.admit(value)
    # A stop fences late admission; valid records committed before it survive.
    assert store.get_evidence(first.evidence_id) is not None
    uri = store.engine.url.render_as_string(hide_password=False)
    reopened = Store(uri)
    try:
        reopened.initialize()
        with pytest.raises(StoreError, match='stopped'):
            reopened.admit(value)
        assert len(reopened.list_observations(value.evidence_id)) == 1
    finally:
        reopened.close()


def test_authority_cannot_be_stripped_or_replaced_after_hold(store):
    value = fenced()
    store.update_source_fence({'authority_id': 'test-authority', 'generation': 0, 'stopped': False})
    store.admit(value)
    store.update_source_fence({'authority_id': 'test-authority', 'generation': 1, 'stopped': True})
    plain = value.model_dump(mode='json')
    plain['source_fence'] = None
    plain['raw_payload']['m1_item'].pop('source_fence')
    plain['delivery_id'] += '-stripped'
    with pytest.raises(StoreError, match='stripped'):
        store.admit(plain)
    store.update_source_fence({'authority_id': 'replacement', 'generation': 0, 'stopped': False})
    with pytest.raises(StoreError, match='replaced'):
        store.admit(fenced('replacement', job='replacement-job'))


def test_new_source_authority_cannot_claim_nonzero_active_generation(store):
    with pytest.raises(StoreError, match='zero'):
        store.update_source_fence({'authority_id': 'never-registered', 'generation': 1, 'stopped': False})
    with store.Session() as session:
        assert session.get(SourceFenceTable, 'never-registered') is None


@pytest.mark.parametrize('failure', ['timeout', 'mismatched_ack', 'boolean_generation'])
def test_remote_ack_failure_prevents_source_dispatch(ledger, tmp_path, failure):
    calls = []
    selected = runtime(tmp_path, lambda batch: None, calls)
    def sync(state):
        if failure == 'timeout':
            raise TimeoutError('controlled remote ack timeout')
        if failure == 'boolean_generation':
            return {**state, 'generation': False}
        return {**state, 'generation': state['generation'] + 1}
    selected.synchronize_fence = sync
    result = worker(ledger, selected)._process_delivery(create(ledger).task_id)
    assert result.state.value == 'DEAD_LETTER' and calls == []


def test_failed_remote_stop_ack_keeps_local_hold_and_stable_authority(tmp_path, store):
    calls = []
    selected = runtime(tmp_path, lambda batch: None, calls)
    def unavailable(state):
        raise TimeoutError('controlled missing stop ack')
    selected.synchronize_fence = unavailable
    with pytest.raises(TimeoutError):
        selected.stop_source('operator_stop')
    with selected.journal.connect() as connection:
        state = connection.execute("SELECT generation, stopped FROM route_state WHERE scope='linkedin.company-feed'").fetchone()
        assert state['generation'] == state['stopped'] == 1
    restored = runtime(tmp_path, lambda batch: None, calls)
    assert restored.authority_id == selected.authority_id
    restored.synchronize_fence = store.update_source_fence
    ack = restored.stop_source('operator_stop')
    assert ack['generation'] == 1 and ack['stopped']
    assert calls == []


def test_source_fence_endpoint_auth_and_owned_http_policy(store, tmp_path):
    from fastapi.testclient import TestClient
    settings = Settings(provider='unavailable', api_token=SecretStr('local-fence-test-token'),
                        api_request_log_path=str(tmp_path / 'api-requests.jsonl'))
    service = M2Service(settings, store=store)
    try:
        with TestClient(create_app(service=service, settings=settings), raise_server_exceptions=False) as client:
            state = {'authority_id': 'http-fence', 'generation': 0, 'stopped': False}
            assert client.post('/v1/linkedin/source-fence', json=state).status_code == 401
            headers = {'Authorization': 'Bearer local-fence-test-token'}
            assert client.post('/v1/linkedin/source-fence', json={**state, 'generation': True}, headers=headers).status_code == 422
            assert client.post('/v1/linkedin/source-fence', json=state, headers=headers).json() == state
            value = fenced('http-fence')
            body = {'source_type': 'linkedin', 'job_id': 'fenced-job', 'items': [value.raw_payload['m1_item']]}
            assert client.post('/v1/linkedin/results', json=body, headers=headers).status_code == 200
            body['items'][0].pop('source_fence')
            assert client.post('/v1/linkedin/results', json=body, headers=headers).status_code == 400
    finally:
        service.close()


def test_remote_stop_ack_is_not_returned_if_commit_fails(store):
    from sqlalchemy import text
    active = {'authority_id': 'commit-fence', 'generation': 0, 'stopped': False}
    store.update_source_fence(active)
    if store.engine.dialect.name == 'postgresql':
        install = 'ALTER TABLE owned_source_fences ADD CONSTRAINT reject_stop CHECK (stopped = FALSE)'
        remove = 'ALTER TABLE owned_source_fences DROP CONSTRAINT reject_stop'
    else:
        install = "CREATE TRIGGER reject_stop BEFORE UPDATE ON owned_source_fences WHEN NEW.stopped = 1 BEGIN SELECT RAISE(ABORT, 'controlled stop failure'); END"
        remove = 'DROP TRIGGER reject_stop'
    with store.engine.begin() as connection:
        connection.execute(text(install))
    try:
        with pytest.raises(Exception):
            store.update_source_fence({**active, 'generation': 1, 'stopped': True})
        with store.Session() as session:
            state = session.get(SourceFenceTable, 'commit-fence')
            assert state.generation == 0 and not state.stopped
    finally:
        with store.engine.begin() as connection:
            connection.execute(text(remove))
    assert store.update_source_fence({**active, 'generation': 1, 'stopped': True})['stopped']
