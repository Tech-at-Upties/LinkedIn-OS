"""Actual task/outbox atomicity and captured-page delivery-only recovery."""
from datetime import UTC, datetime, timedelta
from threading import Barrier, Thread

import pytest

from test_job_runtime import ledger, create, runtime, worker
from test_nos_integration import convert, store
from xingestion.tasks import TaskState


def test_due_retry_state_and_outbox_roll_back_together(ledger):
    task = create(ledger)
    assert ledger.claim_next_outbox_event().task_id == task.task_id
    task = ledger.transition_task(task.task_id, from_state=TaskState.CREATED, to_state=TaskState.RETRY_SCHEDULED,
        next_attempt_at=(datetime.now(UTC) - timedelta(seconds=1)).isoformat())
    with ledger.pool.connection() as connection:
        connection.execute("ALTER TABLE outbox_events ADD CONSTRAINT li_retry_failure CHECK (event_type <> 'CAPABILITY_TASK_RETRY_DUE')")
    try:
        with pytest.raises(Exception):
            ledger.enqueue_due_retries()
        assert ledger.get_task(task.task_id).state == TaskState.RETRY_SCHEDULED
        assert ledger.list_unpublished_outbox_events() == ()
    finally:
        with ledger.pool.connection() as connection:
            connection.execute('ALTER TABLE outbox_events DROP CONSTRAINT li_retry_failure')
    assert ledger.enqueue_due_retries() == 1
    assert ledger.get_task(task.task_id).state == TaskState.ENQUEUED
    events = ledger.list_unpublished_outbox_events()
    assert len(events) == 1 and events[0].task_id == task.task_id
    assert ledger.enqueue_due_retries() == 0


def test_concurrent_due_retry_enqueue_creates_one_event(ledger):
    task = create(ledger)
    ledger.claim_next_outbox_event()
    ledger.transition_task(task.task_id, from_state=TaskState.CREATED, to_state=TaskState.RETRY_SCHEDULED,
        next_attempt_at=(datetime.now(UTC) - timedelta(seconds=1)).isoformat())
    barrier = Barrier(3)
    counts, failures = [], []
    def enqueue():
        barrier.wait(3)
        try:
            counts.append(ledger.enqueue_due_retries())
        except Exception as exc:
            failures.append(type(exc).__name__)
    threads = [Thread(target=enqueue) for _ in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait(3)
    for thread in threads:
        thread.join(3)
    assert sorted(counts) == [0, 1] and failures == []
    assert len(ledger.list_unpublished_outbox_events()) == 1


def due(ledger, task_id):
    task = ledger.get_task(task_id)
    assert task.state == TaskState.RETRY_SCHEDULED
    assert ledger.enqueue_due_retries(now=(datetime.now(UTC) + timedelta(minutes=2)).isoformat()) == 1
    return ledger.get_task(task_id)


def test_commit_then_timeout_requeues_same_source_attempt_without_reread(ledger, tmp_path, store):
    calls, batches = [], []
    def ambiguous(wire):
        batches.append([store.admit(item).duplicate_delivery for item in convert(wire)])
        raise TimeoutError('controlled M2 commit before timeout')
    selected = runtime(tmp_path, ambiguous, calls)
    task = create(ledger)
    ledger.claim_next_outbox_event()
    assert worker(ledger, selected)._process_delivery(task.task_id).state == TaskState.RETRY_SCHEDULED
    pending = ledger.get_task(task.task_id)
    assert pending.result_json['source_attempt_id'] == calls[0]
    due(ledger, task.task_id)
    # A reopened runtime must recover from durable task/source records.
    selected = runtime(tmp_path, lambda wire: batches.append(
        [store.admit(item).duplicate_delivery for item in convert(wire)]), calls)
    assert worker(ledger, selected)._process_delivery(task.task_id).state == TaskState.DONE
    assert calls == [pending.result_json['source_attempt_id']]
    assert batches == [[False], [True]]
    assert len(store.list_observations('linkedin:urn:li:activity:123')) == 1
    assert ledger.get_task(task.task_id).attempt_count == 2
    assert ledger.get_task(task.task_id).error_json == {}


@pytest.mark.parametrize('change', ['expired', 'source_stop'])
def test_delivery_retry_loses_eligibility_without_source_or_sink_dispatch(ledger, tmp_path, change):
    calls, sinks = [], []
    def unavailable(wire):
        sinks.append(wire['job_id'])
        raise ConnectionError('controlled local sink unavailable')
    selected = runtime(tmp_path, unavailable, calls)
    task = create(ledger)
    assert worker(ledger, selected)._process_delivery(task.task_id).state == TaskState.RETRY_SCHEDULED
    if change == 'expired':
        with selected.journal.connect() as connection:
            connection.execute('UPDATE responses SET expires_at=0')
    else:
        selected.journal.stop('linkedin.company-feed', 'operator_stop')
    due(ledger, task.task_id)
    assert worker(ledger, selected)._process_delivery(task.task_id).state == TaskState.DEAD_LETTER
    assert len(calls) == len(sinks) == 1


def test_delivery_retries_are_bounded_by_existing_task_attempt_budget(ledger, tmp_path):
    calls, sinks = [], []
    def unavailable(wire):
        sinks.append(wire['job_id'])
        raise ConnectionError('controlled local sink unavailable')
    selected = runtime(tmp_path, unavailable, calls)
    task = create(ledger)
    assert task.max_attempts == 3
    for number in range(3):
        result = worker(ledger, selected)._process_delivery(task.task_id)
        if number < 2:
            assert result.state == TaskState.RETRY_SCHEDULED
            due(ledger, task.task_id)
        else:
            assert result.state == TaskState.DEAD_LETTER
    assert len(calls) == 1 and len(sinks) == 3
    assert ledger.get_task(task.task_id).next_attempt_at is None


def test_unknown_source_dispatch_never_enters_delivery_retry(ledger, tmp_path):
    calls, sinks = [], []
    selected = runtime(tmp_path, lambda wire: sinks.append(wire), calls)
    def unknown(*args):
        calls.append('source_timeout')
        raise TimeoutError('controlled unknown source outcome')
    selected.send = unknown
    task = create(ledger)
    assert worker(ledger, selected)._process_delivery(task.task_id).state == TaskState.DEAD_LETTER
    assert ledger.get_task(task.task_id).next_attempt_at is None
    assert calls == ['source_timeout'] and sinks == []
