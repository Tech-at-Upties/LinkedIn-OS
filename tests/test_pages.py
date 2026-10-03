"""Durable ownership through the actual M1 executor and M2 storage boundary."""
import json
import multiprocessing
import os
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.request import urlopen

import pytest

from nos_linkedin.acquisition import Journal, RoutePermission, SourceResponse
from nos_linkedin.pages import CompanyPageRuns, PageRunFailure, SCOPE
from test_parser import COMPANY, NOW, RECIPE, envelope
from test_nos_integration import convert, store
from xingestion.linkedin.pages import execute_leased_company_page


def setup(tmp_path, *, count=1, budget=2, expiry=86400):
    journal = Journal(tmp_path / 'pages.sqlite')
    runs = CompanyPageRuns(journal)
    runs.create(run_id='run-one', feed_publisher_id=COMPANY, evidence_class='synthetic_fixture',
                first_start=3, count=count, page_budget=budget)
    permission = RoutePermission(True, True, NOW + timedelta(seconds=expiry), 'synthetic_fixture')
    return journal, runs, permission


def execute(runs, lease, permission, calls, sink, *, now=NOW, mutate=None, status=200):
    def send(selected):
        assert selected == lease
        calls.append((selected.attempt_id, selected.start))
        body = envelope()
        body['data']['data'][RECIPE]['paging'] = {'start': selected.start, 'count': selected.count, 'total': 30}
        if mutate:
            mutate(body)
        return SourceResponse(status, 'application/json', json.dumps(body).encode(), now)
    return execute_leased_company_page(runs=runs, lease=lease, permission=permission,
                                       send=send, deliver=sink, clock=lambda: now)


def admit(store, deliveries):
    def sink(wire):
        deliveries.append((wire['job_id'], [store.admit(value).duplicate_delivery for value in convert(wire)]))
    return sink


def test_run_context_is_immutable_and_bounds_reject_booleans(tmp_path):
    journal, runs, _ = setup(tmp_path)
    with pytest.raises(PageRunFailure, match='run_context_conflict'):
        runs.create(run_id='run-one', feed_publisher_id='urn:li:fsd_company:999', evidence_class='synthetic_fixture',
                    first_start=3, count=1, page_budget=2)
    with pytest.raises(PageRunFailure, match='invalid_run_bounds'):
        runs.create(run_id='bad-bounds', feed_publisher_id=COMPANY, evidence_class='synthetic_fixture',
                    first_start=3, count=True, page_budget=2)
    assert runs.summary('run-one')['completed_pages'] == 0


def test_expired_reservation_keeps_attempt_but_fences_previous_owner_before_any_admission(tmp_path):
    journal, runs, permission = setup(tmp_path)
    old = runs.claim('run-one', owner='old', now=NOW)
    assert runs.claim('run-one', owner='other', now=NOW) is None
    later = NOW + timedelta(seconds=61)
    fresh = CompanyPageRuns(Journal(journal.path)).claim('run-one', owner='new', now=later)
    assert fresh.attempt_id == old.attempt_id and fresh.token != old.token
    calls = []
    with pytest.raises(PageRunFailure, match='page_lease_fenced'):
        execute(runs, old, permission, calls, lambda wire: None)
    assert calls == []
    with journal.connect() as connection:
        assert connection.execute('SELECT count(*) FROM responses').fetchone()[0] == 0
    assert execute(runs, fresh, permission, calls, lambda wire: None, now=later).delivery_outcome == 'delivery_acknowledged'
    assert len(calls) == 1


def test_altered_ticket_cannot_change_request_or_source_context(tmp_path):
    _, runs, permission = setup(tmp_path)
    lease = runs.claim('run-one', owner='worker', now=NOW)
    calls = []
    with pytest.raises(PageRunFailure, match='page_lease_fenced'):
        execute(runs, replace(lease, start=999), permission, calls, lambda wire: None)
    assert calls == []


def test_two_pages_advance_only_after_ack_and_budget_is_not_source_exhaustion(store, tmp_path):
    journal, runs, permission = setup(tmp_path)
    calls, deliveries = [], []
    first = runs.claim('run-one', owner='first', now=NOW)
    execute(runs, first, permission, calls, admit(store, deliveries))
    later = NOW + timedelta(minutes=1)
    reopened = CompanyPageRuns(Journal(journal.path))
    second = reopened.claim('run-one', owner='second', now=later)
    assert second.ordinal == 1 and second.start == 4 and second.attempt_id != first.attempt_id
    execute(reopened, second, permission, calls, admit(store, deliveries), now=later)
    result = reopened.summary('run-one')
    assert result['state'] == 'budget_exhausted' and result['source_complete'] is None
    assert result['completed_pages'] == 2 and result['preceding_items_missing']
    assert reopened.claim('run-one', owner='third', now=later) is None
    assert [start for _, start in calls] == [3, 4]
    assert len(store.list_observations('linkedin:urn:li:activity:123')) == 2


def test_crash_after_m2_ack_before_cursor_commit_replays_same_job_without_source(store, tmp_path, monkeypatch):
    journal, runs, permission = setup(tmp_path)
    first = runs.claim('run-one', owner='first', now=NOW)
    calls, deliveries = [], []
    def crash(*args):
        raise RuntimeError('simulated_process_exit_after_ack')
    monkeypatch.setattr(runs, 'checkpoint', crash)
    with pytest.raises(RuntimeError, match='simulated_process_exit'):
        execute(runs, first, permission, calls, admit(store, deliveries))
    later = NOW + timedelta(seconds=61)
    reopened = CompanyPageRuns(Journal(journal.path))
    resumed = reopened.claim('run-one', owner='restart', now=later)
    result = execute(reopened, resumed, permission, calls, admit(store, deliveries), now=later)
    assert result.replayed and len(calls) == 1
    assert deliveries == [(first.attempt_id, [False]), (first.attempt_id, [True])]
    assert reopened.summary('run-one')['completed_pages'] == 1
    assert len(store.list_observations('linkedin:urn:li:activity:123')) == 1


def test_commit_then_timeout_keeps_current_page_until_explicit_acknowledged_replay(store, tmp_path):
    _, runs, permission = setup(tmp_path)
    first = runs.claim('run-one', owner='first', now=NOW)
    calls, deliveries = [], []
    sink = admit(store, deliveries)
    def commit_then_timeout(wire):
        sink(wire)
        raise TimeoutError('controlled_ambiguous_delivery')
    result = execute(runs, first, permission, calls, commit_then_timeout)
    assert result.delivery_outcome == 'delivery_failure' and runs.summary('run-one')['completed_pages'] == 0
    resumed = runs.claim('run-one', owner='replay', now=NOW)
    assert resumed.attempt_id == first.attempt_id and resumed.start == first.start
    result = execute(runs, resumed, permission, calls, sink)
    assert result.replayed and len(calls) == 1
    assert deliveries == [(first.attempt_id, [False]), (first.attempt_id, [True])]
    assert runs.summary('run-one')['completed_pages'] == 1


def test_wrong_response_paging_has_no_canonical_delivery(store, tmp_path):
    _, runs, permission = setup(tmp_path)
    lease = runs.claim('run-one', owner='first', now=NOW)
    calls, deliveries = [], []
    def wrong_page(body):
        body['data']['data'][RECIPE]['paging']['start'] = 100
    result = execute(runs, lease, permission, calls, admit(store, deliveries), mutate=wrong_page)
    assert result.source_outcome == 'parse_failure' and deliveries == []
    assert store.get_evidence('linkedin:urn:li:activity:123') is None
    assert runs.summary('run-one')['state'] == 'source_failure'


@pytest.mark.parametrize('case,state', [('empty', 'empty_page'), ('short', 'short_page'), ('range', 'range_unproved'), ('partial', 'partial_graph')])
def test_unproved_page_boundaries_pause_without_completeness(case, state, tmp_path):
    _, runs, permission = setup(tmp_path, count=2 if case == 'short' else 1, budget=3)
    lease = runs.claim('run-one', owner='first', now=NOW)
    def mutate(body):
        if case == 'empty':
            body['data']['data'][RECIPE]['*elements'] = []
        elif case == 'range':
            body['data']['data'][RECIPE]['paging']['total'] = 4
        elif case == 'partial':
            body['included'] = body['included'][:1]
    execute(runs, lease, permission, [], lambda wire: None, mutate=mutate)
    result = runs.summary('run-one')
    assert result['state'] == state and result['source_complete'] is None
    assert runs.claim('run-one', owner='next', now=NOW) is None


def test_checkpoint_cannot_invent_ack_or_advance_after_retention_expiry(tmp_path):
    journal, runs, permission = setup(tmp_path, expiry=5)
    lease = runs.claim('run-one', owner='first', now=NOW)
    with pytest.raises(PageRunFailure, match='page_not_acknowledged'):
        runs.checkpoint(lease, NOW)
    # Use actual execution/ack but postpone only its metadata checkpoint.
    from xingestion.linkedin.executor import execute_company_page
    body = envelope()
    body['data']['data'][RECIPE]['paging']['count'] = 1
    result = execute_company_page(journal=journal, permission=permission, feed_publisher_id=COMPANY,
                                  attempt_id=lease.attempt_id, send=lambda: SourceResponse(200, 'application/json', json.dumps(body).encode(), NOW),
                                  deliver=lambda wire: None, clock=lambda: NOW)
    assert result.delivery_outcome == 'delivery_acknowledged'
    with pytest.raises(PageRunFailure, match='page_retention_expired'):
        runs.checkpoint(lease, NOW + timedelta(seconds=6))
    assert runs.summary('run-one')['completed_pages'] == 0


def test_overfull_page_pauses_without_claiming_offset_or_completeness(tmp_path):
    _, runs, permission = setup(tmp_path, count=1, budget=3)
    lease = runs.claim('run-one', owner='first', now=NOW)
    def overfull(body):
        from copy import deepcopy
        extra = deepcopy(body['included'][0])
        extra['entityUrn'] = 'urn:li:fsd_update:(urn:li:activity:124,COMPANY_FEED_RELEVANCE)'
        extra['metadata']['backendUrn'] = 'urn:li:activity:124'
        body['included'].append(extra)
        body['data']['data'][RECIPE]['*elements'].append(extra['entityUrn'])
    result = execute(runs, lease, permission, [], lambda wire: None, mutate=overfull)
    assert result.delivery_outcome == 'delivery_acknowledged'
    summary = runs.summary('run-one')
    assert summary['state'] == 'overfull_page'
    assert summary['completed_pages'] == 1 and summary['source_complete'] is None
    assert runs.claim('run-one', owner='next', now=NOW) is None


def test_source_stop_prevents_new_claim_and_access_failure_does_not_advance(tmp_path):
    journal, runs, permission = setup(tmp_path)
    lease = runs.claim('run-one', owner='first', now=NOW)
    result = execute(runs, lease, permission, [], lambda wire: None, status=403)
    assert result.source_outcome == 'restricted' and result.delivery_outcome is None
    assert runs.summary('run-one')['state'] == 'source_failure'
    with pytest.raises(PageRunFailure, match='source_fenced'):
        runs.claim('run-one', owner='next', now=NOW)


def reclaim_in_process(path, queue):
    runs = CompanyPageRuns(Journal(path))
    queue.put(runs.claim('run-one', owner='replacement', now=NOW + timedelta(seconds=61)))


def exit_after_m2_ack(path, lease, uri, source_url):
    from nos_m2.store import Store
    runs = CompanyPageRuns(Journal(path))
    database = Store(uri)
    database.initialize()
    permission = RoutePermission(True, True, NOW + timedelta(days=1), 'synthetic_fixture')
    def send(selected):
        with urlopen(source_url, timeout=3) as response:
            return SourceResponse(response.status, 'application/json', response.read(), NOW)
    runs.checkpoint = lambda *_: os._exit(17)
    execute_leased_company_page(runs=runs, lease=lease, permission=permission, send=send,
                               deliver=admit(database, []), clock=lambda: NOW)


def test_abrupt_process_exit_after_actual_http_read_and_m2_ack_replays_without_another_request(tmp_path):
    from nos_m2.store import Store
    journal, runs, permission = setup(tmp_path)
    lease = runs.claim('run-one', owner='child', now=NOW)
    body = envelope()
    body['data']['data'][RECIPE]['paging']['count'] = 1
    calls = []
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            calls.append(self.path)
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(body).encode())
        def log_message(self, *_):
            pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    uri = f'sqlite:///{(tmp_path / "m2.sqlite").as_posix()}'
    context = multiprocessing.get_context('spawn')
    child = context.Process(target=exit_after_m2_ack,
                            args=(journal.path, lease, uri, f'http://127.0.0.1:{server.server_port}/company'))
    child.start()
    database = None
    try:
        child.join(10)
        assert child.exitcode == 17 and calls == ['/company']
        later = NOW + timedelta(seconds=61)
        reopened = CompanyPageRuns(Journal(journal.path))
        assert reopened.summary('run-one')['completed_pages'] == 0
        replacement = reopened.claim('run-one', owner='recovery', now=later)
        assert replacement.attempt_id == lease.attempt_id
        database = Store(uri)
        database.initialize()
        deliveries, callbacks = [], []
        result = execute(reopened, replacement, permission, callbacks, admit(database, deliveries), now=later)
        assert result.replayed and callbacks == [] and calls == ['/company']
        assert deliveries == [(lease.attempt_id, [True])]
        assert len(database.list_observations('linkedin:urn:li:activity:123')) == 1
        assert reopened.summary('run-one')['completed_pages'] == 1
    finally:
        if child.is_alive():
            child.terminate()
            child.join(3)
        if database is not None:
            database.close()
        server.shutdown()
        server.server_close()
        thread.join(3)


def test_owner_replaced_after_parse_is_fenced_before_actual_m2_admission(store, tmp_path, monkeypatch):
    journal, runs, permission = setup(tmp_path)
    old = runs.claim('run-one', owner='old', now=NOW)
    original = journal.finish
    replacements = []
    def finish_then_replace(*args):
        result = original(*args)
        replacements.append(runs.claim('run-one', owner='new', now=NOW + timedelta(seconds=61)))
        return result
    monkeypatch.setattr(journal, 'finish', finish_then_replace)
    calls, deliveries = [], []
    with pytest.raises(PageRunFailure, match='page_lease_fenced'):
        execute(runs, old, permission, calls, admit(store, deliveries))
    assert len(calls) == 1 and deliveries == [] and store.get_evidence('linkedin:urn:li:activity:123') is None
    monkeypatch.setattr(journal, 'finish', original)
    result = execute(runs, replacements[0], permission, calls, admit(store, deliveries), now=NOW + timedelta(seconds=61))
    assert result.replayed and len(calls) == 1 and deliveries == [(old.attempt_id, [False])]


def test_takeover_after_committed_attempt_admission_fences_physical_source(tmp_path, monkeypatch):
    journal, runs, permission = setup(tmp_path)
    old = runs.claim('run-one', owner='old', now=NOW)
    original = journal.connect
    replaced = []
    context = multiprocessing.get_context('spawn')
    queue = context.Queue()
    @contextmanager
    def connect():
        statements = []
        with original() as connection:
            connection.set_trace_callback(statements.append)
            yield connection
        if not replaced and any(sql.startswith('INSERT INTO responses') for sql in statements):
            process = context.Process(target=reclaim_in_process, args=(journal.path, queue))
            process.start()
            try:
                replaced.append(queue.get(timeout=10))
                process.join(10)
                assert process.exitcode == 0
            finally:
                if process.is_alive():
                    process.terminate()
                    process.join(3)
    monkeypatch.setattr(journal, 'connect', connect)
    calls = []
    with pytest.raises(PageRunFailure, match='page_lease_fenced'):
        execute(runs, old, permission, calls, lambda wire: None)
    assert calls == [] and replaced[0].attempt_id == old.attempt_id
    # Admission already committed: conservatively unknown, not a fabricated safe resend.
    assert Journal(journal.path).read(old.attempt_id, NOW)['outcome'] == 'dispatch_unknown'
    assert execute(runs, replaced[0], permission, calls, lambda wire: None,
                   now=NOW + timedelta(seconds=61)).source_outcome == 'dispatch_unknown'
    assert calls == [] and runs.summary('run-one')['state'] == 'source_failure'
