"""A buffered transmitted page must respect acknowledged task cancellation."""
import asyncio
from copy import deepcopy
from datetime import UTC, datetime, timedelta
import socket
from threading import Event, Thread
from types import SimpleNamespace
import time

import httpx
import pytest
from pydantic import SecretStr
import uvicorn

from test_job_runtime import create, ledger, runtime, worker
from test_nos_integration import store
from nos_m2.api import create_app
from nos_m2.service import M2Service
from nos_m2.settings import Settings
from nos_m2.store import StoreError
from nos_m2.store import Store, TaskFenceTable
from nos_m2.models import EvidenceEnvelope
from sqlalchemy import event
from test_remote_fence import fenced
from test_nos_integration import convert
from xingestion.web import live_server


@pytest.mark.parametrize('boundary', ['cancel', 'takeover'])
def test_buffered_page_cannot_admit_after_task_cancel_ack(ledger, store, tmp_path, boundary):
    settings = Settings(provider='unavailable', api_token=SecretStr('local-task-fence-test-token'),
                        api_request_log_path=str(tmp_path / 'api-requests.jsonl'))
    service = M2Service(settings, store=store)
    app = create_app(service=service, settings=settings)
    arrived, release, finished = Event(), Event(), Event()
    statuses, calls, batches = [], [], []

    @app.middleware('http')
    async def buffer_page(request, call_next):
        if request.url.path != '/v1/linkedin/results' or arrived.is_set():
            return await call_next(request)
        assert await request.body()
        arrived.set()
        assert await asyncio.to_thread(release.wait, 8)
        try:
            response = await call_next(request)
            statuses.append(response.status_code)
            return response
        except StoreError:
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
    headers = {'Authorization': 'Bearer local-task-fence-test-token'}

    def sync(path, state):
        response = httpx.post(base + path, json=state, headers=headers, timeout=3)
        response.raise_for_status()
        return response.json()

    def deliver(batch):
        batches.append(batch)
        response = httpx.post(base + '/v1/linkedin/results', json=batch, headers=headers, timeout=.2)
        response.raise_for_status()
        return response.json()

    selected = runtime(tmp_path, deliver, calls)
    selected.synchronize_fence = lambda state: sync('/v1/linkedin/source-fence', state)
    selected.synchronize_task_fence = lambda state: sync('/v1/linkedin/task-fence', state)
    task = create(ledger)
    try:
        parked = worker(ledger, selected)._process_delivery(task.task_id)
        assert parked.state.value == 'RETRY_SCHEDULED' and arrived.is_set()
        # The fallback witnesses the committed pre-repair cancellation path.
        # The repaired coordinator must additionally wait for the remote ack.
        if boundary == 'takeover':
            ledger.enqueue_due_retries(now=(datetime.now(UTC) + timedelta(seconds=60)).isoformat())
            assert worker(ledger, selected)._process_delivery(task.task_id).state.value == 'DONE'
            completed = ledger.get_task(task.task_id)
            assert completed.state.value == 'DONE' and completed.delivery_generation == 2
            assert len(store.list_evidence()) == 1
            assert store.admit(convert(selected.result_for_task(completed))[0]).duplicate_delivery
        elif hasattr(selected, 'cancel_task'):
            assert selected.cancel_task(ledger, task.task_id).state.value == 'CANCELLED'
        else:
            assert ledger.cancel_task(task.task_id).state.value == 'CANCELLED'
        release.set()
        assert finished.wait(5)
        assert statuses in [[409], ['admission_fenced']]
        assert len(store.list_evidence()) == (1 if boundary == 'takeover' else 0)
        response = httpx.post(base + '/v1/linkedin/results', json=batches[0], headers=headers, timeout=3)
        assert response.status_code == 409
        assert len(calls) == 1
    finally:
        release.set()
        server.should_exit = True
        thread.join(5)
        listener.close()
        service.close()


def task_bound(value, generation=1):
    item = deepcopy(value.raw_payload['m1_item'])
    item['task_fence'] = {'authority_id': value.source_fence.authority_id,
                         'job_id': value.origin.m1_job_id, 'generation': generation, 'cancelled': False}
    return convert({'source_type': 'linkedin', 'job_id': value.origin.m1_job_id, 'items': [item]})[0]


def test_generation_takeover_keeps_original_observation_and_delivery_identity(store):
    original = task_bound(fenced('task-authority'))
    state = original.task_fence.model_dump()
    store.update_source_fence(original.source_fence.model_dump())
    store.update_task_fence(state)
    first = store.admit(original)
    next_state = {**state, 'generation': 2}
    assert store.update_task_fence(next_state) == next_state
    with pytest.raises(StoreError, match='stale'):
        store.admit(original)
    replacement = task_bound(original, 2)
    assert replacement.delivery_id == original.delivery_id
    duplicate = store.admit(replacement)
    assert duplicate.duplicate_delivery and duplicate.observation_id == first.observation_id
    assert duplicate.source_version_id == first.source_version_id
    assert len(store.list_observations(original.evidence_id)) == 1
    with pytest.raises(StoreError, match='regress'):
        store.update_task_fence(state)
    cancelled = {**next_state, 'cancelled': True}
    assert store.update_task_fence(cancelled) == cancelled
    with pytest.raises(StoreError, match='cancelled'):
        store.admit(replacement)
    with pytest.raises(StoreError, match='reopen'):
        store.update_task_fence({**next_state, 'generation': 3})
    reopened = Store(store.engine.url.render_as_string(hide_password=False))
    try:
        reopened.initialize()
        with pytest.raises(StoreError, match='cancelled'):
            reopened.admit(replacement)
    finally:
        reopened.close()


def test_enrolled_authority_cannot_drop_task_binding(store):
    source_only = fenced('task-authority')
    value = task_bound(source_only)
    store.update_source_fence(value.source_fence.model_dump())
    with pytest.raises(StoreError, match='unknown'):
        store.admit(value)
    store.update_task_fence(value.task_fence.model_dump())
    with pytest.raises(StoreError, match='strip'):
        store.admit(source_only)
    assert store.list_evidence() == []
    data = value.model_dump(mode='json')
    data.pop('task_fence')
    with pytest.raises(ValueError, match='stripped'):
        EvidenceEnvelope.model_validate(data)
    stripped_item = deepcopy(source_only.raw_payload['m1_item'])
    stripped_item.pop('source_fence')
    stripped = convert({'source_type': 'linkedin', 'job_id': value.origin.m1_job_id, 'items': [stripped_item]})[0]
    with pytest.raises(StoreError, match='strip'):
        store.admit(stripped)


@pytest.mark.parametrize('bad', [True, -1, '1', 1.5])
def test_task_generation_is_strict_and_invalid_cannot_enroll(store, bad):
    store.update_source_fence({'authority_id': 'task-authority', 'generation': 0, 'stopped': False})
    with pytest.raises(ValueError):
        store.update_task_fence({'authority_id': 'task-authority', 'job_id': 'task-job',
                                 'generation': bad, 'cancelled': False})
    with store.Session() as session:
        assert session.get(TaskFenceTable, ('task-authority', 'task-job')) is None


def test_task_fence_cannot_enroll_unknown_authority_or_active_zero(store):
    state = {'authority_id': 'task-authority', 'job_id': 'task-job', 'generation': 1, 'cancelled': False}
    with pytest.raises(StoreError, match='unknown'):
        store.update_task_fence(state)
    store.update_source_fence({'authority_id': 'task-authority', 'generation': 0, 'stopped': False})
    with pytest.raises(StoreError, match='positive'):
        store.update_task_fence({**state, 'generation': 0})
    cancelled = {**state, 'generation': 0, 'cancelled': True}
    assert store.update_task_fence(cancelled) == cancelled
    with pytest.raises(StoreError, match='reopen'):
        store.update_task_fence(state)


@pytest.mark.parametrize('failure', ['timeout', 'boolean_generation', 'wrong_job'])
def test_missing_task_ack_fences_before_source(ledger, tmp_path, store, failure):
    calls = []
    selected = runtime(tmp_path, lambda batch: None, calls)
    selected.synchronize_fence = store.update_source_fence
    def unavailable(state):
        if failure == 'timeout':
            raise TimeoutError('controlled task registration timeout')
        if failure == 'boolean_generation':
            return {**state, 'generation': True}
        return {**state, 'job_id': 'wrong-job'}
    selected.synchronize_task_fence = unavailable
    assert worker(ledger, selected)._process_delivery(create(ledger).task_id).state.value == 'DEAD_LETTER'
    assert calls == []


def test_failed_cancel_ack_keeps_local_cancel_and_retries_only_ack(ledger, tmp_path, store):
    calls = []
    selected = runtime(tmp_path, lambda batch: None, calls)
    selected.synchronize_fence = store.update_source_fence
    task = create(ledger)
    def unavailable(state):
        raise TimeoutError('controlled cancel ack timeout')
    selected.synchronize_task_fence = unavailable
    with pytest.raises(TimeoutError):
        selected.cancel_task(ledger, task.task_id)
    assert ledger.get_task(task.task_id).state.value == 'CANCELLED'
    restored = runtime(tmp_path, lambda batch: None, calls)
    restored.synchronize_fence = store.update_source_fence
    restored.synchronize_task_fence = store.update_task_fence
    assert restored.authority_id == selected.authority_id
    assert restored.cancel_task(ledger, task.task_id).state.value == 'CANCELLED'
    assert worker(ledger, restored)._process_delivery(task.task_id).state.value == 'CANCELLED'
    assert calls == []


def test_task_fence_ack_waits_for_commit_and_rollback_preserves_old_generation(store):
    value = task_bound(fenced('task-authority'))
    store.update_source_fence(value.source_fence.model_dump())
    state = value.task_fence.model_dump()
    store.update_task_fence(state)
    def reject_commit(connection):
        raise RuntimeError('controlled fence commit failure')
    event.listen(store.engine, 'commit', reject_commit)
    try:
        with pytest.raises(RuntimeError, match='commit failure'):
            store.update_task_fence({**state, 'generation': 2})
    finally:
        event.remove(store.engine, 'commit', reject_commit)
    with store.Session() as session:
        row = session.get(TaskFenceTable, (state['authority_id'], state['job_id']))
        assert row.generation == 1 and not row.cancelled
    assert not store.admit(value).duplicate_delivery


def test_task_fence_http_auth_strict_ack_and_cancellation(store, tmp_path):
    from fastapi.testclient import TestClient
    settings = Settings(provider='unavailable', api_token=SecretStr('local-task-fence-test-token'),
                        api_request_log_path=str(tmp_path / 'api-requests.jsonl'))
    service = M2Service(settings, store=store)
    try:
        with TestClient(create_app(service=service, settings=settings), raise_server_exceptions=False) as client:
            value = task_bound(fenced('task-authority'))
            state = value.task_fence.model_dump()
            headers = {'Authorization': 'Bearer local-task-fence-test-token'}
            assert client.post('/v1/linkedin/task-fence', json=state).status_code == 401
            assert client.post('/v1/linkedin/source-fence', json=value.source_fence.model_dump(), headers=headers).status_code == 200
            assert client.post('/v1/linkedin/task-fence', json={**state, 'generation': True}, headers=headers).status_code == 422
            assert client.post('/v1/linkedin/task-fence', json=state, headers=headers).json() == state
            body = {'source_type': 'linkedin', 'job_id': state['job_id'], 'items': [value.raw_payload['m1_item']]}
            assert client.post('/v1/linkedin/results', json=body, headers=headers).status_code == 200
            cancelled = {**state, 'cancelled': True}
            assert client.post('/v1/linkedin/task-fence', json=cancelled, headers=headers).json() == cancelled
            assert client.post('/v1/linkedin/results', json=body, headers=headers).status_code == 409
    finally:
        service.close()


def test_operator_cancel_reports_unknown_ack_and_can_reacknowledge(ledger, store, tmp_path, monkeypatch):
    calls = []
    selected = runtime(tmp_path, lambda batch: None, calls)
    selected.synchronize_fence = store.update_source_fence
    def unavailable(state):
        raise TimeoutError('controlled operator acknowledgement timeout')
    selected.synchronize_task_fence = unavailable
    task = create(ledger)
    monkeypatch.setattr(live_server, 'STATE', SimpleNamespace(ledger=ledger, linkedin_runtime=selected))
    class Operator(live_server.LiveAppHandler):
        def __init__(self):
            self.status = None
        def _json(self, value, *, status=200):
            self.status = status
            return value
    handler = Operator()
    response = handler._cancel_task(task.task_id)
    assert handler.status == 503 and response['remote_acknowledged'] is False
    assert ledger.get_task(task.task_id).state.value == 'CANCELLED'
    selected.synchronize_task_fence = store.update_task_fence
    response = handler._cancel_task(task.task_id)
    assert handler.status == 200 and response['message'] == 'Task cancelled'
    assert calls == []
