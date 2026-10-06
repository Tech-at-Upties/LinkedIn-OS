"""Requested-count M1 tasks, retained recovery and SQLite M2 with synthetic pages."""
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from hashlib import sha256
import json

import pytest

from test_company_collection import body_for
from test_grant_runtime import SyntheticLedger, consumptions, installed, runtime_for
from test_initial_company_page import (PROJECT, TARGET, REQUEST_URN, COMPANY, LEGACY_PLAN,
    project_manifest, install_capture_spy, synthetic_html, actual_initial_projection)
from test_nos_integration import convert
from nos_linkedin.acquisition import Journal, SourceResponse
from nos_m2.store import Store
from xingestion.capabilities import CapabilityPlanner, CapabilityRequest, LinkedInCompanyFeedInput
from xingestion.capability_ids import CapabilityId
from xingestion.linkedin.runtime import LinkedInJobRuntime
from xingestion.tasks import TaskState


def count_payload(*, requested_count=15, max_pages=3, **extra):
    return LinkedInCompanyFeedInput(TARGET, COMPANY, page_mode='initial_document',
        requested_count=requested_count, max_pages=max_pages, **extra)


def count_ledger(payload=None):
    payload = count_payload() if payload is None else payload
    request = CapabilityRequest(CapabilityId.LINKEDIN_COMPANY_FEED, 2, payload)
    ledger = SyntheticLedger('count-runtime-synthetic')
    ledger.task.request_json = request.public_dict()
    ledger.task.plan_json = CapabilityPlanner(None).plan(request).public_dict()
    return ledger


@pytest.fixture
def database(tmp_path):
    selected = Store('sqlite:///' + (tmp_path / 'count-m2.sqlite').as_posix())
    selected.initialize()
    yield selected
    selected.close()


class SyntheticPages:
    """Real callback interface, without a browser or any LinkedIn request."""
    supports_multipage = True
    def __init__(self, *, partial=None):
        self.calls = []
        self.partial = partial
    def __call__(self, payload, lease):
        self.calls.append((lease.start, lease.count, lease.attempt_id))
        observed = datetime.now(UTC)
        ids = {0: range(1, 4), 3: range(3, 13), 13: range(12, 22)}[lease.start]
        body = body_for(lease, ids)
        if lease.ordinal == 0:
            initial = actual_initial_projection(synthetic_html(body), observed)
            body['source_provenance'] = {
                'page_mode': 'initial_document',
                'probe_sha256': sha256((PROJECT / 'scripts/probe-linkedin-company.cjs').read_bytes()).hexdigest(),
                'helper_sha256': sha256((PROJECT / 'scripts/company-bootstrap.cjs').read_bytes()).hexdigest(),
                'initial_page': {key: value for key, value in initial.items() if key != 'representation'},
            }
        if lease.ordinal == 1 and self.partial == 'optional_actor':
            for node in body['included']:
                node.pop('actor', None)
        elif lease.ordinal == 1 and self.partial == 'missing_root':
            root = body['data']['data'][next(iter(body['data']['data']))]['*elements'][-1]
            body['included'] = [node for node in body['included'] if node['entityUrn'] != root]
        return SourceResponse(200, 'application/json', json.dumps(body).encode(), observed)


def runtime_at(path, source, sink):
    return LinkedInJobRuntime(journal=Journal(path), send=source, deliver=sink,
        evidence_class='synthetic_fixture', page_mode='initial_document', feed_request_urn=REQUEST_URN,
        expires_at=datetime.now(UTC) + timedelta(minutes=9))


def test_explicit_count_is_version_two_and_legacy_public_contract_stays_identical():
    legacy = LinkedInCompanyFeedInput(TARGET, COMPANY)
    request = CapabilityRequest(CapabilityId.LINKEDIN_COMPANY_FEED, 1, legacy)
    assert CapabilityPlanner(None).plan(request).public_dict() == LEGACY_PLAN
    assert request.public_dict()['payload'] == {'company_url': TARGET, 'feed_publisher_id': COMPANY, 'max_pages': 1}
    counted = count_payload()
    request = CapabilityRequest(CapabilityId.LINKEDIN_COMPANY_FEED, 2, counted)
    plan = CapabilityPlanner(None).plan(request).public_dict()
    assert plan['contract_version'] == 2 and plan['requested_count'] == 15 and plan['max_pages'] == 3
    assert plan['first_start'] == 0 and plan['page_size'] == 3 and plan['ordering'] == 'relevance'
    for version, payload in ((1, counted), (2, legacy)):
        with pytest.raises(ValueError):
            CapabilityPlanner(None).plan(CapabilityRequest(CapabilityId.LINKEDIN_COMPANY_FEED, version, payload))


@pytest.mark.parametrize('extra', [{'requested_count': True}, {'requested_count': 0},
    {'requested_count': 10001}, {'max_pages': 101}, {'ordering': 'recent'}])
def test_count_contract_rejects_unsupported_bounds_and_order(extra):
    values = dict(requested_count=15, max_pages=3, ordering='relevance')
    values.update(extra)
    with pytest.raises(ValueError):
        count_payload(**values).validate()


@pytest.mark.parametrize('field', ['ordering', 'sort'])
def test_actual_web_payload_rejects_unproved_recent_ordering(field):
    from xingestion.web.live_server import _capability_payload_from_dict
    with pytest.raises(ValueError):
        payload = _capability_payload_from_dict(CapabilityId.LINKEDIN_COMPANY_FEED.value,
            dict(company_url=TARGET, feed_publisher_id=COMPANY, page_mode='initial_document',
                requested_count=15, max_pages=3, **{field: 'recent'}))
        payload.validate()


def test_actual_m1_count_selects_unique_items_before_sqlite_m2_and_preserves_page_provenance(tmp_path, database):
    source, delivered = SyntheticPages(), []
    def sink(wire):
        delivered.append(deepcopy(wire))
        for value in convert(wire):
            database.admit(value)
    runtime, ledger = runtime_at(tmp_path / 'source.sqlite', source, sink), count_ledger()
    result = runtime.process_task(ledger=ledger, task=ledger.task, owner='count-owner', lease_seconds=60)
    assert result.state == TaskState.DONE
    assert [(start, count) for start, count, _ in source.calls] == [(0, 3), (3, 10), (13, 10)]
    assert [[item['item_id'] for item in wire['items']] for wire in delivered] == [
        [f'urn:li:activity:{number}' for number in range(1, 4)],
        [f'urn:li:activity:{number}' for number in range(4, 13)],
        [f'urn:li:activity:{number}' for number in range(13, 16)],
    ]
    assert len(database.list_evidence()) == 15
    pulled = runtime.result_for_task(ledger.task)
    assert len(pulled['items']) == 15 and pulled['coverage']['fulfilled']
    assert pulled['coverage']['requested_count'] == pulled['coverage']['returned_count'] == 15
    assert pulled['coverage']['source_complete'] is None
    first = pulled['items'][:3]
    assert all(item['source_fields']['source_provenance']['page_mode'] == 'initial_document' for item in first)
    assert all('source_provenance' not in item['source_fields'] for item in pulled['items'][3:])
    assert [item['acquisition']['page_ordinal'] for item in pulled['items']] == [0]*3 + [1]*9 + [2]*3
    assert 'items' not in result.result_json


def test_ambiguous_second_page_delivery_reclaims_same_selection_without_second_capture(tmp_path, database):
    source, deliveries = SyntheticPages(), []
    def ambiguous(wire):
        deliveries.append(deepcopy(wire))
        for value in convert(wire):
            database.admit(value)
        if len(deliveries) == 2:
            raise TimeoutError('synthetic M2 commit before acknowledgement')
    path = tmp_path / 'recover-source.sqlite'
    runtime, ledger = runtime_at(path, source, ambiguous), count_ledger()
    assert runtime.process_task(ledger=ledger, task=ledger.task, owner='first', lease_seconds=60).state == TaskState.RETRY_SCHEDULED
    assert [(start, count) for start, count, _ in source.calls] == [(0, 3), (3, 10)]
    assert runtime.runs.summary(ledger.task.task_id)['returned_count'] == 3
    ledger.task.attempt_count = 2
    ledger.task.lease_token = 'count-recovery-token'
    reopened = runtime_at(path, source, ambiguous)
    assert reopened.process_task(ledger=ledger, task=ledger.task, owner='recovered', lease_seconds=60).state == TaskState.DONE
    assert [(start, count) for start, count, _ in source.calls] == [(0, 3), (3, 10), (13, 10)]
    assert deliveries[1] == deliveries[2]
    assert len(database.list_evidence()) == 15
    for item in reopened.result_for_task(ledger.task)['items']:
        assert len(database.list_observations('linkedin:' + item['item_id'])) == 1


@pytest.mark.parametrize('partial,expected_calls,shortfall', [
    ('optional_actor', [(0, 3), (3, 10), (13, 10)], None),
    ('missing_root', [(0, 3), (3, 10)], 'partial_graph'),
])
def test_optional_partial_fields_continue_but_missing_collection_identity_pauses(tmp_path, partial, expected_calls, shortfall):
    source, delivered = SyntheticPages(partial=partial), []
    runtime, ledger = runtime_at(tmp_path / 'partial.sqlite', source, delivered.append), count_ledger()
    result = runtime.process_task(ledger=ledger, task=ledger.task, owner='partial-owner', lease_seconds=60)
    assert result.state == TaskState.DONE
    assert [(start, count) for start, count, _ in source.calls] == expected_calls
    pulled = runtime.result_for_task(ledger.task)
    assert pulled['coverage']['shortfall_reason'] == shortfall
    assert pulled['coverage']['fulfilled'] == (partial == 'optional_actor')
    assert pulled['coverage']['source_complete'] is None
    assert delivered[1]['coverage']['issues']


def test_one_shot_installed_capture_rejects_multi_page_count_before_run_or_consumption(installed, monkeypatch):
    calls = []
    install_capture_spy(monkeypatch, calls)
    runtime, ledger = runtime_for(installed, []), count_ledger()
    result = runtime.process_task(ledger=ledger, task=ledger.task, owner='unsupported-multi', lease_seconds=60)
    assert result.state == TaskState.DEAD_LETTER
    assert calls == [] and consumptions(installed) == []
    with installed.journal.connect() as connection:
        assert connection.execute('SELECT COUNT(*) FROM responses').fetchone()[0] == 0
        assert connection.execute('SELECT COUNT(*) FROM company_runs').fetchone()[0] == 0


def test_single_page_count_uses_installed_capture_and_returns_no_surplus(installed, monkeypatch):
    calls, delivered = [], []
    install_capture_spy(monkeypatch, calls)
    runtime = runtime_for(installed, delivered)
    ledger = count_ledger(count_payload(requested_count=1, max_pages=1))
    assert runtime.process_task(ledger=ledger, task=ledger.task, owner='one-count', lease_seconds=60).state == TaskState.DONE
    assert len(calls) == len(consumptions(installed)) == 1
    assert len(delivered) == len(delivered[0]['items']) == 1
    pulled = runtime.result_for_task(ledger.task)
    assert len(pulled['items']) == 1
    assert pulled['coverage']['returned_count'] == pulled['coverage']['requested_count'] == 1
    assert pulled['coverage']['fulfilled'] and pulled['coverage']['source_complete'] is None


def test_page_budget_reports_honest_shortfall_without_scraping_more(tmp_path, database):
    source, delivered = SyntheticPages(), []
    def sink(wire):
        delivered.append(deepcopy(wire))
        for value in convert(wire):
            database.admit(value)
    runtime, ledger = runtime_at(tmp_path / 'budget.sqlite', source, sink), count_ledger(count_payload(max_pages=1))
    assert runtime.process_task(ledger=ledger, task=ledger.task, owner='budget-owner', lease_seconds=60).state == TaskState.DONE
    assert [(start, count) for start, count, _ in source.calls] == [(0, 3)]
    assert len(database.list_evidence()) == 3
    pulled = runtime.result_for_task(ledger.task)
    assert len(pulled['items']) == 3
    assert pulled['coverage']['returned_count'] == 3 and pulled['coverage']['requested_count'] == 15
    assert not pulled['coverage']['fulfilled'] and pulled['coverage']['shortfall_reason'] == 'budget_exhausted'
    assert pulled['coverage']['source_complete'] is None
