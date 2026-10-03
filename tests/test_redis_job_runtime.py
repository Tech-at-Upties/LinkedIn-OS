"""Actual owned Redis streams, SQL outbox and M1 worker; synthetic source only."""
from datetime import UTC, datetime, timedelta
import importlib.util
import json
import multiprocessing
import os
from pathlib import Path
import subprocess
import sys
from uuid import uuid4
from threading import Barrier, Thread
import socket
import time

import pytest
import redis

from test_nos_integration import convert, store
from test_job_runtime import ledger, create, runtime
from xingestion.dispatch import RedisOutboxDispatcher
from xingestion.workers import LocalWorker
from test_job_bootstrap import m2, manifest_config
from xingestion.linkedin.bootstrap import build_linkedin_runtime
import uvicorn

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('owned_redis', ROOT / 'scripts/local-verification-redis.py')
owner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(owner)


@pytest.fixture
def queue():
    url = os.environ.get('NOS_LINKEDIN_TEST_REDIS_URL')
    if not url:
        pytest.skip('Explicit project-owned Redis verification URL is required')
    assert url == 'redis://127.0.0.1:16379/15'
    checked = owner.owned_client()
    assert checked is not None
    checked.close()
    client = redis.Redis.from_url(url, decode_responses=True, socket_timeout=3)
    stream = 'linkedin-verification:' + uuid4().hex
    yield client, stream
    client.delete(stream)  # This fixture owns exactly this unique stream.
    client.close()


def selected_worker(ledger, selected, queue, consumer='current'):
    client, stream = queue
    return LocalWorker(ledger=ledger, manifest=None, auth=None, transport=None, raw_evidence_sink=None,
                       linkedin_runtime=selected, redis_client=client, redis_stream_key=stream,
                       redis_consumer_group='controlled-group', redis_consumer_name=consumer,
                       redis_claim_min_idle_ms=0, redis_read_block_ms=25, lease_seconds=60)


def dispatcher(ledger, queue):
    return RedisOutboxDispatcher(ledger=ledger, redis_client=queue[0], stream_key=queue[1])


def test_real_outbox_stream_worker_ack_and_duplicate_delivery(ledger, queue, tmp_path, store):
    calls = []
    selected = runtime(tmp_path, lambda batch: [store.admit(item) for item in convert(batch)], calls)
    task = create(ledger)
    published = dispatcher(ledger, queue).dispatch_once()
    assert published.dispatched and published.task_id == task.task_id
    assert ledger.list_unpublished_outbox_events() == ()
    result = selected_worker(ledger, selected, queue).process_one()
    assert result.state.value == 'DONE' and len(calls) == 1
    assert queue[0].xpending(queue[1], 'controlled-group')['pending'] == 0
    queue[0].xadd(queue[1], {'task_id': task.task_id, 'event_id': published.event_id, 'event_type': 'CAPABILITY_TASK_CREATED'})
    assert selected_worker(ledger, selected, queue).process_one().state.value == 'DONE'
    assert len(calls) == 1 and len(store.list_observations('linkedin:urn:li:activity:123')) == 1
    assert queue[0].xpending(queue[1], 'controlled-group')['pending'] == 0


def test_actual_xadd_before_sql_rollback_redelivers_without_source_duplicate(ledger, queue, tmp_path, store):
    calls = []
    selected = runtime(tmp_path, lambda batch: [store.admit(item) for item in convert(batch)], calls)
    create(ledger)
    with ledger.pool.connection() as connection:
        connection.execute('ALTER TABLE outbox_events ADD CONSTRAINT controlled_publish_failure CHECK (published_at IS NULL)')
    try:
        with pytest.raises(Exception):
            dispatcher(ledger, queue).dispatch_once()
        assert queue[0].xlen(queue[1]) == 1
        assert len(ledger.list_unpublished_outbox_events()) == 1
    finally:
        with ledger.pool.connection() as connection:
            connection.execute('ALTER TABLE outbox_events DROP CONSTRAINT controlled_publish_failure')
    assert dispatcher(ledger, queue).dispatch_once().dispatched
    assert queue[0].xlen(queue[1]) == 2
    selected = selected_worker(ledger, selected, queue)
    assert selected.process_one().state.value == 'DONE'
    assert selected.process_one().state.value == 'DONE'
    assert len(calls) == 1 and len(store.list_observations('linkedin:urn:li:activity:123')) == 1
    assert queue[0].xpending(queue[1], 'controlled-group')['pending'] == 0


def read_delivery_then_exit(url, stream, marker):
    client = redis.Redis.from_url(url, decode_responses=True)
    worker = LocalWorker(ledger=None, manifest=None, auth=None, transport=None, raw_evidence_sink=None,
                         redis_client=client, redis_stream_key=stream, redis_consumer_group='controlled-group',
                         redis_consumer_name='abrupt-process', redis_claim_min_idle_ms=0, redis_read_block_ms=25)
    delivery = worker._read_next_delivery()
    Path(marker).write_text(json.dumps(delivery))
    os._exit(73)


def test_actual_pending_delivery_reclaimed_after_process_exit(ledger, queue, tmp_path, store):
    task = create(ledger)
    assert dispatcher(ledger, queue).dispatch_once().dispatched
    marker = tmp_path / 'pending-delivery.json'
    process = multiprocessing.get_context('spawn').Process(target=read_delivery_then_exit,
        args=('redis://127.0.0.1:16379/15', queue[1], str(marker)))
    process.start()
    process.join(15)
    try:
        assert process.exitcode == 73
        assert json.loads(marker.read_text())[0] == task.task_id
        assert queue[0].xpending(queue[1], 'controlled-group')['pending'] == 1
        calls = []
        selected = runtime(tmp_path, lambda batch: [store.admit(item) for item in convert(batch)], calls)
        assert selected_worker(ledger, selected, queue, 'recovering-process').process_one().state.value == 'DONE'
        assert len(calls) == 1 and len(store.list_observations('linkedin:urn:li:activity:123')) == 1
        assert queue[0].xpending(queue[1], 'controlled-group')['pending'] == 0
    finally:
        if process.is_alive():
            process.terminate()
            process.join(5)


def test_real_stream_retry_preserves_source_attempt_and_m2_duplicate_identity(ledger, queue, tmp_path, store):
    calls, admissions = [], []
    def admit(batch):
        admissions.append([store.admit(item).duplicate_delivery for item in convert(batch)])
        if len(admissions) == 1:
            raise TimeoutError('controlled M2 commit before timeout')
    selected = runtime(tmp_path, admit, calls)
    task = create(ledger)
    assert dispatcher(ledger, queue).dispatch_once().dispatched
    assert selected_worker(ledger, selected, queue).process_one().state.value == 'RETRY_SCHEDULED'
    assert ledger.enqueue_due_retries(now=(datetime.now(UTC) + timedelta(minutes=2)).isoformat()) == 1
    assert dispatcher(ledger, queue).dispatch_once().dispatched
    restored = runtime(tmp_path, admit, calls)
    assert selected_worker(ledger, restored, queue, 'reopened').process_one().state.value == 'DONE'
    assert len(calls) == 1 and admissions == [[False], [True]]
    assert ledger.get_task(task.task_id).attempt_count == 2
    assert queue[0].xpending(queue[1], 'controlled-group')['pending'] == 0


def test_aof_restart_preserves_stream_and_pending_reclaim(ledger, queue, tmp_path, store):
    create(ledger)
    assert dispatcher(ledger, queue).dispatch_once().dispatched
    abandoned = selected_worker(ledger, None, queue, 'before-server-restart')
    assert abandoned._read_next_delivery() is not None
    assert queue[0].xpending(queue[1], 'controlled-group')['pending'] == 1
    before = queue[0].info('server')['run_id']
    helper = ROOT / 'scripts/local-verification-redis.py'
    for command in ('stop', 'start'):
        result = subprocess.run([sys.executable, str(helper), command], capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result.stderr
        queue[0].connection_pool.disconnect()
    assert queue[0].info('server')['run_id'] != before
    assert queue[0].xpending(queue[1], 'controlled-group')['pending'] == 1
    calls = []
    selected = runtime(tmp_path, lambda batch: [store.admit(item) for item in convert(batch)], calls)
    assert selected_worker(ledger, selected, queue, 'after-server-restart').process_one().state.value == 'DONE'
    assert len(calls) == 1 and len(store.list_observations('linkedin:urn:li:activity:123')) == 1
    assert queue[0].xpending(queue[1], 'controlled-group')['pending'] == 0


def test_lost_owned_stream_restores_same_never_leased_task(ledger, queue, tmp_path, store):
    calls = []
    selected = runtime(tmp_path, lambda batch: [store.admit(item) for item in convert(batch)], calls)
    task = create(ledger)
    assert dispatcher(ledger, queue).dispatch_once().dispatched
    # Delete exactly the unique test-owned stream, not a Redis database.
    assert queue[0].delete(queue[1]) == 1
    with ledger.pool.connection() as connection:
        connection.execute('UPDATE outbox_events SET published_at=%s WHERE task_id=%s',
            ((datetime.now(UTC) - timedelta(minutes=10)).isoformat(), task.task_id))
    recovered = selected_worker(ledger, selected, queue, 'missing-stream')
    assert not recovered.process_one().processed
    assert len(ledger.list_unpublished_outbox_events()) == 1
    assert dispatcher(ledger, queue).dispatch_once().dispatched
    assert recovered.process_one().state.value == 'DONE'
    assert len(calls) == 1 and len(store.list_observations('linkedin:urn:li:activity:123')) == 1


def make_stale(ledger, queue):
    task = create(ledger)
    assert dispatcher(ledger, queue).dispatch_once().dispatched
    with ledger.pool.connection() as connection:
        connection.execute('UPDATE outbox_events SET published_at=%s WHERE task_id=%s',
            ((datetime.now(UTC) - timedelta(minutes=10)).isoformat(), task.task_id))
    return task


def test_queue_restoration_state_and_event_roll_back_together(ledger, queue):
    task = make_stale(ledger, queue)
    with ledger.pool.connection() as connection:
        connection.execute("ALTER TABLE outbox_events ADD CONSTRAINT reject_restore CHECK (event_type <> 'CAPABILITY_TASK_QUEUE_RESTORED')")
    try:
        with pytest.raises(Exception):
            ledger.restore_stalled_linkedin_deliveries()
        assert ledger.get_task(task.task_id).state.value == 'CREATED'
        assert ledger.list_unpublished_outbox_events() == ()
    finally:
        with ledger.pool.connection() as connection:
            connection.execute('ALTER TABLE outbox_events DROP CONSTRAINT reject_restore')
    assert ledger.restore_stalled_linkedin_deliveries() == 1
    assert ledger.restore_stalled_linkedin_deliveries() == 0


def test_competing_queue_restorers_create_one_event(ledger, queue):
    make_stale(ledger, queue)
    barrier, results, failures = Barrier(3), [], []
    def restore():
        barrier.wait(3)
        try:
            results.append(ledger.restore_stalled_linkedin_deliveries())
        except Exception as error:
            failures.append(type(error).__name__)
    threads = [Thread(target=restore) for _ in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait(3)
    for thread in threads:
        thread.join(5)
    assert sorted(results) == [0, 1] and not failures
    assert len(ledger.list_unpublished_outbox_events()) == 1


@pytest.mark.parametrize('state', ['DONE', 'RUNNING', 'RETRY_SCHEDULED', 'DEAD_LETTER'])
def test_queue_restoration_does_not_override_other_task_states(ledger, queue, state):
    task = make_stale(ledger, queue)
    with ledger.pool.connection() as connection:
        connection.execute('UPDATE capability_tasks SET state=%s WHERE task_id=%s', (state, task.task_id))
    assert ledger.restore_stalled_linkedin_deliveries() == 0
    assert ledger.get_task(task.task_id).state.value == state


def test_real_queue_saved_source_and_authenticated_m2_http_compose(m2, store, ledger, queue, tmp_path, monkeypatch):
    app, service = m2
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen(10)
    endpoint = f'http://127.0.0.1:{listener.getsockname()[1]}/v1/linkedin/results'
    server = uvicorn.Server(uvicorn.Config(app, log_level='critical', lifespan='off'))
    thread = Thread(target=server.run, kwargs={'sockets': [listener]}, daemon=True)
    thread.start()
    deadline = time.monotonic() + 5
    while not server.started and time.monotonic() < deadline:
        time.sleep(.01)
    assert server.started
    monkeypatch.setenv('LINKEDIN_M2_API_TOKEN', 'local-http-test-token')
    try:
        selected = build_linkedin_runtime(manifest_config(tmp_path, endpoint))
        calls = []
        send = selected.send
        def record_source(payload, lease):
            calls.append(lease.attempt_id)
            return send(payload, lease)
        selected.send = record_source
        task = create(ledger)
        assert dispatcher(ledger, queue).dispatch_once().dispatched
        assert selected_worker(ledger, selected, queue).process_one().state.value == 'DONE'
        result = selected.result_for_task(ledger.get_task(task.task_id))
        value = convert(result)[0]
        assert value.source_fence.authority_id == selected.authority_id
        assert store.admit(value).duplicate_delivery
        assert len(calls) == len(store.list_observations(value.evidence_id)) == 1
        assert queue[0].xpending(queue[1], 'controlled-group')['pending'] == 0
    finally:
        server.should_exit = True
        thread.join(5)
        listener.close()
