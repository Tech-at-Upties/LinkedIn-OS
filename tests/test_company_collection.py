"""Requested count through retained pages and actual M1 execution, without source access."""
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import timedelta
from threading import Barrier

import pytest

from nos_linkedin.acquisition import Journal, RoutePermission, SourceResponse
from nos_linkedin.pages import CompanyPageRuns, PageRunFailure
from nos_linkedin.parser import RECIPE, parse_company_feed
from test_parser import COMPANY, NOW, fixture
from test_nos_integration import convert  # Installs the dedicated M1/M2 import paths.
from xingestion.linkedin.executor import execute_company_page


def body_for(lease, ids, *, total=230, missing=False):
    graph, roots = [], []
    for number in ids:
        nodes = json.loads(json.dumps(fixture()).replace('123', str(number)).replace('456', str(number + 1000)))
        roots.append(nodes[0]['entityUrn'])
        graph.extend(nodes)
    if missing:
        roots.append('urn:li:fsd_update:(urn:li:activity:999999,COMPANY_FEED_RELEVANCE)')
    return {'data': {'data': {RECIPE: {'$type': 'com.linkedin.restli.common.CollectionResponse', '*elements': roots,
        'paging': {'start': lease.start, 'count': lease.count, 'total': total}}}}, 'included': graph}


def setup(tmp_path, *, target=15, budget=3):
    journal = Journal(tmp_path / 'collection.sqlite')
    runs = CompanyPageRuns(journal)
    runs.create(run_id='collection', feed_publisher_id=COMPANY, evidence_class='synthetic_fixture',
        first_start=0, count=3, following_count=10, requested_count=target, page_budget=budget)
    return journal, runs, RoutePermission(True, True, NOW + timedelta(hours=1), 'synthetic_fixture')


def execute(runs, lease, permission, ids, calls, deliveries, *, sink=None, missing=False, missing_actor=False, body_override=None):
    def send():
        calls.append((lease.start, lease.count))
        body = body_override if body_override is not None else body_for(lease, ids, missing=missing)
        if missing_actor:
            for node in body['included']:
                node.pop('actor', None)
        return SourceResponse(200, 'application/json', json.dumps(body).encode(), NOW)

    def validate(page):
        runs.validate_page(lease, page)
        runs.prepare_selection(lease, page, NOW)

    def deliver(wire):
        selected = runs.selected_ids(lease)
        by_id = {item['item_id']: item for item in wire['items']}
        wire['items'] = [by_id[item_id] for item_id in selected]
        # Actual M2 converter accepts precisely the selected item IDs.
        assert [item.source_item_id for item in convert(wire)] == list(selected)
        deliveries.append(wire)
        if sink is not None:
            sink(wire)

    result = execute_company_page(journal=runs.journal, permission=permission,
        feed_publisher_id=COMPANY, attempt_id=lease.attempt_id, send=send, deliver=deliver,
        clock=lambda: NOW, operation_guard=lambda: runs.require_owned(lease, NOW), page_validator=validate)
    if result.delivery_outcome == 'delivery_acknowledged':
        runs.checkpoint(lease, NOW)
    else:
        runs.failed(lease, source_outcome=result.source_outcome, delivery_outcome=result.delivery_outcome, now=NOW)
    return result


def claim(runs, owner='worker'):
    return runs.claim('collection', owner=owner, now=NOW)


def test_initial_then_api_pages_use_independent_sizes_and_unique_target(tmp_path):
    journal, runs, permission = setup(tmp_path)
    calls, deliveries = [], []
    for ids in (range(1, 4), range(3, 13), range(12, 22)):
        lease = claim(runs)
        execute(runs, lease, permission, ids, calls, deliveries)
        runs = CompanyPageRuns(Journal(journal.path))
    assert calls == [(0, 3), (3, 10), (13, 10)]
    assert [[item['item_id'] for item in wire['items']] for wire in deliveries] == [
        [f'urn:li:activity:{i}' for i in range(1, 4)],
        [f'urn:li:activity:{i}' for i in range(4, 13)],
        [f'urn:li:activity:{i}' for i in range(13, 16)]]
    summary = runs.summary('collection')
    assert summary['requested_count'] == summary['returned_count'] == 15
    assert summary['fulfilled'] and summary['state'] == 'fulfilled'
    assert summary['shortfall_reason'] is None and summary['source_complete'] is None
    assert runs.acknowledged_ids('collection') == tuple(f'urn:li:activity:{i}' for i in range(1, 16))
    assert claim(runs) is None


def test_target_smaller_than_initial_page_filters_before_m2(tmp_path):
    _, runs, permission = setup(tmp_path, target=1)
    deliveries = []
    execute(runs, claim(runs), permission, [3, 1, 2], [], deliveries)
    assert [item['item_id'] for item in deliveries[0]['items']] == ['urn:li:activity:3']
    assert runs.summary('collection')['returned_count'] == 1
    assert claim(runs) is None


def test_pending_selection_survives_timeout_reclaim_without_new_capture(tmp_path):
    journal, runs, permission = setup(tmp_path, target=2)
    calls, deliveries = [], []
    first = claim(runs)

    def timeout(_wire):
        raise TimeoutError('sink_committed_before_timeout')

    result = execute(runs, first, permission, [3, 1, 2], calls, deliveries, sink=timeout)
    assert result.delivery_outcome == 'delivery_failure'
    assert runs.summary('collection')['returned_count'] == 0
    assert runs.selected_ids(first) == ('urn:li:activity:3', 'urn:li:activity:1')
    reopened = CompanyPageRuns(Journal(journal.path))
    second = claim(reopened, 'restart')
    assert second.attempt_id == first.attempt_id and second.token != first.token
    result = execute(reopened, second, permission, [999], calls, deliveries)
    assert result.replayed and calls == [(0, 3)]
    assert [item['item_id'] for item in deliveries[0]['items']] == [item['item_id'] for item in deliveries[1]['items']]
    assert reopened.summary('collection')['returned_count'] == 2


@pytest.mark.parametrize('ids,budget,state', [([], 3, 'empty_page'), ([1], 3, 'short_page'), ([1, 2, 3], 1, 'budget_exhausted')])
def test_shortfall_reason_remains_distinct_from_source_exhaustion(tmp_path, ids, budget, state):
    _, runs, permission = setup(tmp_path, target=15, budget=budget)
    execute(runs, claim(runs), permission, ids, [], [])
    summary = runs.summary('collection')
    assert not summary['fulfilled'] and summary['shortfall_reason'] == state
    assert summary['returned_count'] == len(ids) and summary['source_complete'] is None


def test_fulfilled_partial_page_keeps_source_warnings(tmp_path):
    _, runs, permission = setup(tmp_path, target=2)
    deliveries = []
    execute(runs, claim(runs), permission, [1, 2], [], deliveries, missing=True)
    assert runs.summary('collection')['fulfilled']
    assert deliveries[0]['coverage']['issues']
    assert deliveries[0]['coverage']['source_complete'] is None


def test_missing_optional_actor_allows_count_continuation_with_warnings(tmp_path):
    _, runs, permission = setup(tmp_path, target=5)
    calls, deliveries = [], []
    execute(runs, claim(runs), permission, [1, 2, 3], calls, deliveries, missing_actor=True)
    assert runs.summary('collection')['state'] == 'active'
    assert all(item['author'] is None for item in deliveries[0]['items'])
    assert any(issue['code'] == 'unknown_actor' for issue in deliveries[0]['coverage']['issues'])
    execute(runs, claim(runs), permission, range(4, 14), calls, deliveries)
    assert calls == [(0, 3), (3, 10)]
    assert runs.summary('collection')['returned_count'] == 5
    assert runs.summary('collection')['fulfilled']


def test_missing_collection_root_pauses_unfulfilled_count(tmp_path):
    _, runs, permission = setup(tmp_path, target=15)
    execute(runs, claim(runs), permission, [1, 2], [], [], missing=True)
    summary = runs.summary('collection')
    assert summary['returned_count'] == 2 and not summary['fulfilled']
    assert summary['shortfall_reason'] == 'partial_graph' and summary['source_complete'] is None
    assert claim(runs) is None


@pytest.mark.parametrize('field,value', [('requested_count', True), ('requested_count', 0), ('following_count', True), ('following_count', 0)])
def test_collection_bounds_and_context_are_immutable(tmp_path, field, value):
    journal, runs, _ = setup(tmp_path)
    parameters = dict(run_id='collection', feed_publisher_id=COMPANY, evidence_class='synthetic_fixture',
        first_start=0, count=3, following_count=10, requested_count=15, page_budget=3)
    parameters[field] = value
    with pytest.raises(PageRunFailure, match='invalid_collection_bounds'):
        runs.create(**parameters)
    parameters[field] = 2
    with pytest.raises(PageRunFailure, match='run_context_conflict'):
        CompanyPageRuns(Journal(journal.path)).create(**parameters)


def test_selection_rejects_forged_page_stale_owner_digest_and_stopped_source(tmp_path):
    journal, runs, permission = setup(tmp_path)
    lease = claim(runs)
    body = body_for(lease, [1, 2, 3])
    journal.capture(scope='linkedin.company-feed', generation=lease.generation, attempt_id=lease.attempt_id,
        feed_publisher_id=COMPANY, permission=permission, now=NOW,
        send=lambda: SourceResponse(200, 'application/json', json.dumps(body).encode(), NOW))
    page = parse_company_feed(body, feed_publisher_id=COMPANY, observed_at=NOW, evidence_class='synthetic_fixture')
    with pytest.raises(PageRunFailure, match='selection_page_conflict'):
        runs.prepare_selection(lease, replace(page, publications=page.publications[:1]), NOW)
    with pytest.raises(PageRunFailure, match='page_lease_fenced'):
        runs.prepare_selection(replace(lease, token='stale'), page, NOW)
    with journal.connect() as connection:
        connection.execute('UPDATE responses SET body_sha256=? WHERE attempt_id=?', ('0' * 64, lease.attempt_id))
    with pytest.raises(PageRunFailure, match='selection_digest_conflict'):
        runs.prepare_selection(lease, page, NOW)
    journal.stop('linkedin.company-feed', 'restricted')
    with pytest.raises(PageRunFailure, match='source_fenced'):
        runs.prepare_selection(lease, page, NOW)
    with journal.connect() as connection:
        assert connection.execute('SELECT count(*) FROM company_page_selection').fetchone()[0] == 0


def test_legacy_run_keeps_old_summary_and_fixed_page_size(tmp_path):
    journal = Journal(tmp_path / 'legacy.sqlite')
    runs = CompanyPageRuns(journal)
    runs.create(run_id='legacy', feed_publisher_id=COMPANY, evidence_class='synthetic_fixture', first_start=3, count=10, page_budget=2)
    assert 'requested_count' not in runs.summary('legacy')
    assert runs.claim('legacy', owner='worker', now=NOW).count == 10


def test_existing_database_migration_keeps_legacy_run_and_pending_attempt(tmp_path):
    journal = Journal(tmp_path / 'existing.sqlite')
    with journal.connect() as connection:
        connection.executescript('''CREATE TABLE company_runs (
            run_id TEXT PRIMARY KEY, feed_publisher_id TEXT NOT NULL,
            evidence_class TEXT NOT NULL, generation INTEGER NOT NULL,
            first_start INTEGER NOT NULL, requested_count INTEGER NOT NULL,
            page_budget INTEGER NOT NULL, completed_pages INTEGER NOT NULL,
            next_start INTEGER NOT NULL, state TEXT NOT NULL,
            owner TEXT, lease_token TEXT, lease_until REAL);''')
        connection.execute('INSERT INTO company_runs VALUES (?, ?, ?, ?, 3, 10, 2, 0, 3, ?, NULL, NULL, NULL)',
            ('legacy', COMPANY, 'synthetic_fixture', journal.generation('linkedin.company-feed'), 'active'))
    runs = CompanyPageRuns(journal)
    runs.create(run_id='legacy', feed_publisher_id=COMPANY, evidence_class='synthetic_fixture', first_start=3, count=10, page_budget=2)
    lease = runs.claim('legacy', owner='before', now=NOW)
    restarted = CompanyPageRuns(Journal(journal.path))
    reclaimed = restarted.claim('legacy', owner='after', now=NOW + timedelta(seconds=61))
    assert reclaimed.attempt_id == lease.attempt_id and reclaimed.count == 10
    assert 'requested_count' not in restarted.summary('legacy')


def test_concurrent_initializers_migrate_one_existing_database(tmp_path):
    journal = Journal(tmp_path / 'concurrent-migration.sqlite')
    with journal.connect() as connection:
        connection.executescript('''CREATE TABLE company_runs (
            run_id TEXT PRIMARY KEY, feed_publisher_id TEXT NOT NULL,
            evidence_class TEXT NOT NULL, generation INTEGER NOT NULL,
            first_start INTEGER NOT NULL, requested_count INTEGER NOT NULL,
            page_budget INTEGER NOT NULL, completed_pages INTEGER NOT NULL,
            next_start INTEGER NOT NULL, state TEXT NOT NULL,
            owner TEXT, lease_token TEXT, lease_until REAL);''')
    simultaneous = Barrier(2)

    def initialize():
        simultaneous.wait(timeout=5)
        return CompanyPageRuns(Journal(journal.path))

    with ThreadPoolExecutor(max_workers=2) as workers:
        runs = list(workers.map(lambda _index: initialize(), range(2)))
    for initialized in runs:
        initialized.create(run_id='shared', feed_publisher_id=COMPANY, evidence_class='synthetic_fixture',
            first_start=0, count=3, requested_count=5, following_count=10, page_budget=2)
    assert runs[0].summary('shared')['requested_count'] == 5


def test_selection_rejects_conflicting_occurrence_alias_before_m2(tmp_path):
    journal, runs, permission = setup(tmp_path, target=5)
    lease = claim(runs)
    body = body_for(lease, [1, 2, 3])
    roots = body['data']['data'][RECIPE]['*elements']
    second_representation = next(node for node in body['included'] if node['entityUrn'] == roots[1])
    second_representation['metadata']['backendUrn'] = 'urn:li:activity:1'
    deliveries = []
    result = execute(runs, lease, permission, [], [], deliveries, body_override=body)
    assert result.source_outcome == 'parse_failure' and deliveries == []
    with journal.connect() as connection:
        assert connection.execute('SELECT count(*) FROM company_page_selection').fetchone()[0] == 0


def test_selection_dedupes_equivalent_occurrence_aliases_without_text_metadata(tmp_path):
    journal, runs, permission = setup(tmp_path, target=5)
    lease = claim(runs)
    body = body_for(lease, [1, 2, 3])
    roots = body['data']['data'][RECIPE]['*elements']
    equivalent = json.loads(json.dumps(body['included'][0]))
    equivalent['entityUrn'] = 'urn:li:fsd_update:(urn:li:activity:1,ALIAS)'
    body['included'].append(equivalent)
    roots[1] = equivalent['entityUrn']
    deliveries = []
    execute(runs, lease, permission, [], [], deliveries, body_override=body)
    assert [item['item_id'] for item in deliveries[0]['items']] == ['urn:li:activity:1', 'urn:li:activity:3']
    with journal.connect() as connection:
        stored = connection.execute('SELECT item_ids FROM company_page_selection').fetchone()[0]
    assert 'Own commentary' not in stored and 'fsd_update' not in stored
