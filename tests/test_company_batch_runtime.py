"""Installed batch grants through actual M1 and SQLite M2, with a synthetic child."""
from contextlib import closing
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from hashlib import sha256
import json
import sqlite3
from types import SimpleNamespace

import pytest

from test_company_collection import body_for
from test_company_count_runtime import count_ledger, count_payload, database
from test_grant_runtime import (consumptions, runtime_for, exercise_installed_grant_factory_queue_m2_http,
    ledger, queue, m2, store)
from test_initial_company_page import (PROJECT, TARGET, REQUEST_URN, COMPANY, project_manifest,
    PURE_NODE_RUN, PURE_NODE_BINARY, synthetic_html, actual_initial_projection)
from test_nos_integration import convert
from nos_linkedin.acquisition import Journal
from nos_linkedin.pages import PageRunFailure
from xingestion.linkedin.grants import GrantIssuer, RECIPE, declaration_sha
from xingestion.tasks import TaskState


@pytest.fixture
def batch_installed(project_manifest, monkeypatch):
    root, live = project_manifest
    journal = Journal(root / '.local/linkedin-source.sqlite')
    journal.generation('linkedin.company-feed')
    authority = 'synthetic-batch-authority'
    with journal.connect() as connection:
        connection.execute('CREATE TABLE owned_sink_authority(singleton INTEGER PRIMARY KEY,authority_id TEXT)')
        connection.execute('INSERT INTO owned_sink_authority VALUES(1,?)', (authority,))
    budget = root / '.local/linkedin-live-capture.sqlite'
    with closing(sqlite3.connect(budget)):
        pass
    issuer = GrantIssuer(project_root=root, source_journal=journal.path,
        authority_id=authority, source_generation=0)
    issuer.initialize()
    manifest = dict(kind='linkedin-observation-grant/3', page_mode='initial_document', batch_page_budget=3,
        grant_id='synthetic-batch-grant', previous_grant_id=None, workload_id='synthetic-batch-workload',
        workload_position=0, declared_at=(datetime.now(UTC)-timedelta(seconds=1)).isoformat(),
        reason='explicit_observation', project_root=str(root), profile=str(issuer.profile),
        source_journal=str(journal.path), authority_id=authority, source_generation=0,
        company_url=TARGET, feed_publisher_id=COMPANY, feed_request_urn=REQUEST_URN,
        recipe=RECIPE, start=0, count=3, probe_sha256=live['probe_sha256'], helper_sha256=live['helper_sha256'],
        expires_at=(datetime.now(UTC)+timedelta(minutes=9)).isoformat(), capture_budget=1, native_read_budget=12,
        evidence_class='allowlisted_source_projection')
    issuer.declare(manifest, workload=dict(workload_id=manifest['workload_id'],
        declaration_shas=[declaration_sha(manifest)], native_read_budget=12))
    reference = dict(kind='installed-observation-grant/1', project_root=str(root), source_journal=str(journal.path),
        authority_id=authority, source_generation=0, grant_id=manifest['grant_id'],
        declaration_sha256=declaration_sha(manifest), nos_source_root=live['nos_source_root'])
    path = root / '.local/batch-installed-reference.json'
    path.write_text(json.dumps(reference))
    config = SimpleNamespace(linkedin_grant_path=path, linkedin_live_path=None, linkedin_replay_path=None,
        data_dir=root / '.local', linkedin_m2_ingest_url='http://127.0.0.1:1/v1/linkedin/results')
    monkeypatch.setenv('LINKEDIN_M2_API_TOKEN', 'synthetic-unused-http-token')
    return SimpleNamespace(root=root, journal=journal, budget=budget, issuer=issuer,
        manifest=manifest, reference=reference, path=path, config=config, live=live)


def api_record(body, observed):
    """Run the real pure Node projection export on a generated native-shaped body."""
    program = """const p=require(process.argv[1]);let input='';
process.stdin.setEncoding('utf8');process.stdin.on('data',c=>input+=c);
process.stdin.on('end',()=>console.log(JSON.stringify(p.describe(JSON.parse(input)))));"""
    result = PURE_NODE_RUN([PURE_NODE_BINARY, '-e', program, str(PROJECT / 'scripts/probe-linkedin-company.cjs')],
        input=json.dumps(body).encode(), stdout=-1, stderr=-1, check=True, timeout=5, cwd=PROJECT)
    paging = body['data']['data'][RECIPE]['paging']
    return dict(operation_id='voyagerFeedDashOrganizationalPageUpdates.synthetic', status=200,
        method='GET', origin='https://www.linkedin.com', pathname='/voyager/api/graphql',
        observed_at=observed.isoformat(), content_type='application/json',
        body_sha256=sha256(json.dumps(body).encode()).hexdigest(),
        request_parameters={'fields': {'start': paging['start'], 'count': paging['count'],
            'organizationalPageUrn': {'present': True, 'value_sha256': sha256(REQUEST_URN.encode()).hexdigest()}}},
        representation=json.loads(result.stdout))


def install_batch_child(monkeypatch, case, calls, *, pages=3, batch_budget=3, security=False):
    def child(command, **kwargs):
        assert len(consumptions(case)) == 1
        assert '--runtime-company-batch' in command
        assert '--batch-page-budget=' + str(batch_budget) in command
        observed = datetime.now(UTC)
        initial = actual_initial_projection(synthetic_html(body_for(SimpleNamespace(start=0, count=3), range(1, 4))), observed)
        records = [initial]
        if pages > 1:
            records.append(api_record(body_for(SimpleNamespace(start=3, count=10), range(3, 13)), observed))
        if pages > 2:
            records.append(api_record(body_for(SimpleNamespace(start=13, count=10), range(12, 22)), observed))
        page_specs = [(0, 3, 'initial_document'), (3, 10, 'api_following'), (13, 10, 'api_following')]
        receipt = dict(target=TARGET, page_mode='initial_document',
            evidence_class='allowlisted_source_projection', session_verified=not security,
            stopped={'reason': 'challenge_path'} if security else None, failure_kind=None,
            account_writes_blocked=True, original_bodies_retained=False, native_reads_admitted=4,
            native_read_budget=12, continuation_response_observed=None, boundary_probe=None,
            initial_page=initial, responses=[dict(operation_id='voyagerOrganizationDashCompanies.synthetic',
                origin='https://www.linkedin.com', status=200, representation={'source_native_ids': [COMPANY]})],
            batch_kind='company-source-batch/1', batch_page_budget=batch_budget,
            batch_pages=[dict(page_mode=mode, start=start, count=count, record=record)
                for (start, count, mode), record in zip(page_specs, records)],
            batch_shortfall_reason=None if pages == batch_budget else 'continuation_not_observed')
        calls.append((command, deepcopy(receipt)))
        return SimpleNamespace(returncode=0, stdout=json.dumps(receipt).encode())
    monkeypatch.setattr('xingestion.linkedin.live.subprocess.run', child)


def source_rows(case):
    with case.journal.connect() as connection:
        return [dict(row) for row in connection.execute('SELECT attempt_id,body,outcome,expires_at FROM responses ORDER BY rowid')]


def runtime_for_m2(case, database, deliveries):
    runtime = runtime_for(case, deliveries)
    runtime.synchronize_fence = database.update_source_fence
    runtime.synchronize_task_fence = database.update_task_fence
    return runtime


def batch_ledger(payload=None):
    ledger = count_ledger(payload)
    # Match real PostgreSQL lease generations when exercising actual M2 fences.
    ledger.task.delivery_generation = 1
    return ledger


def test_one_batch_child_saves_all_pages_before_m2_selects_unique_target(batch_installed, monkeypatch, database):
    case, calls, delivered = batch_installed, [], []
    install_batch_child(monkeypatch, case, calls)
    runtime = runtime_for_m2(case, database, [])
    def sink(wire):
        rows = source_rows(case)
        assert len(rows) == 3 and all(row['body'] is not None for row in rows)
        assert all(row['outcome'] in {'bounded', 'partial'} for row in rows)
        parent_body = json.loads(rows[0]['body'])
        assert parent_body['batch_capture_metadata'] == {
            'native_reads_admitted': 4, 'native_read_budget': 12, 'batch_page_budget': 3}
        assert sorted((json.loads(row['body'])['data']['data'][RECIPE]['paging']['start'],
                       json.loads(row['body'])['data']['data'][RECIPE]['paging']['count']) for row in rows) == [(0, 3), (3, 10), (13, 10)]
        delivered.append(deepcopy(wire))
        for value in convert(wire):
            database.admit(value)
    runtime.deliver = sink
    ledger = batch_ledger()
    assert runtime.process_task(ledger=ledger, task=ledger.task, owner='batch-owner', lease_seconds=60).state == TaskState.DONE
    assert len(calls) == len(consumptions(case)) == 1
    assert [len(wire['items']) for wire in delivered] == [3, 9, 3]
    assert len(database.list_evidence()) == 15
    pulled = runtime.result_for_task(ledger.task)
    assert len(pulled['items']) == 15 and pulled['coverage']['fulfilled']
    assert pulled['coverage']['source_complete'] is None
    assert all(item['source_fields']['source_provenance']['page_mode'] == 'initial_document' for item in pulled['items'][:3])
    assert all(item['source_fields']['source_provenance']['page_mode'] == 'api_following' for item in pulled['items'][3:])
    assert all('initial_page' not in item['source_fields']['source_provenance'] for item in pulled['items'][3:])
    for wire in delivered[1:]:
        provenance = wire['items'][0]['source_fields']['source_provenance']
        assert provenance['native_response']['request_parameters']['fields']['start'] == wire['coverage']['paging']['start']
    assert len({item['acquisition']['source_attempt_id'] for item in pulled['items']}) == 3


def test_batch_ambiguous_second_page_delivery_replays_retained_pages_without_child(batch_installed, monkeypatch, database):
    case, calls, delivered = batch_installed, [], []
    install_batch_child(monkeypatch, case, calls)
    runtime = runtime_for_m2(case, database, [])
    def sink(wire):
        delivered.append(deepcopy(wire))
        for value in convert(wire):
            database.admit(value)
        if len(delivered) == 2:
            raise TimeoutError('synthetic commit before acknowledgement')
    runtime.deliver = sink
    ledger = batch_ledger()
    assert runtime.process_task(ledger=ledger, task=ledger.task, owner='first', lease_seconds=60).state == TaskState.RETRY_SCHEDULED
    original = consumptions(case)
    ledger.task.attempt_count = 2
    ledger.task.lease_token = 'synthetic-batch-recovery-token'
    reopened = runtime_for_m2(case, database, [])
    reopened.deliver = sink
    assert reopened.process_task(ledger=ledger, task=ledger.task, owner='recovery', lease_seconds=60).state == TaskState.DONE
    assert delivered[1] == delivered[2]
    assert len(calls) == 1 and consumptions(case) == original
    assert len(database.list_evidence()) == 15
    for item in reopened.result_for_task(ledger.task)['items']:
        assert len(database.list_observations('linkedin:' + item['item_id'])) == 1


def test_batch_security_signal_blocks_every_page_and_cannot_recapture(batch_installed, monkeypatch, database):
    case, calls, delivered = batch_installed, [], []
    install_batch_child(monkeypatch, case, calls, security=True)
    runtime, ledger = runtime_for_m2(case, database, delivered), batch_ledger()
    assert runtime.process_task(ledger=ledger, task=ledger.task, owner='security', lease_seconds=60).state == TaskState.DEAD_LETTER
    assert len(calls) == len(consumptions(case)) == 1
    assert delivered == [] and database.list_evidence() == []
    assert all(row['body'] is None for row in source_rows(case))
    reopened = runtime_for_m2(case, database, delivered)
    assert reopened.process_task(ledger=ledger, task=ledger.task, owner='recovery', lease_seconds=60).state == TaskState.DEAD_LETTER
    assert len(calls) == 1
    with case.journal.connect() as connection:
        assert connection.execute('SELECT stopped,reason FROM route_state').fetchone()[:] == (1, 'challenge')


def test_missing_batch_continuation_returns_saved_posts_with_honest_shortfall(batch_installed, monkeypatch, database):
    case, calls, delivered = batch_installed, [], []
    install_batch_child(monkeypatch, case, calls, pages=2)
    runtime = runtime_for_m2(case, database, delivered)
    runtime.deliver = lambda wire: [database.admit(value) for value in convert(wire)]
    ledger = batch_ledger()
    assert runtime.process_task(ledger=ledger, task=ledger.task, owner='partial-batch', lease_seconds=60).state == TaskState.DONE
    pulled = runtime.result_for_task(ledger.task)
    assert len(pulled['items']) == len(database.list_evidence()) == 12
    assert not pulled['coverage']['fulfilled'] and pulled['coverage']['source_complete'] is None
    assert pulled['coverage']['shortfall_reason'] == 'continuation_not_observed'
    assert len(calls) == len(consumptions(case)) == 1


def test_batch_original_expiry_clears_every_stored_page_body(batch_installed, monkeypatch):
    case, calls = batch_installed, []
    install_batch_child(monkeypatch, case, calls)
    runtime, ledger = runtime_for(case, []), batch_ledger()
    assert runtime.process_task(ledger=ledger, task=ledger.task, owner='expiry', lease_seconds=60).state == TaskState.DONE
    assert len(source_rows(case)) == 3
    expiry = datetime.fromisoformat(case.manifest['expires_at'])
    runtime.journal.purge_expired(expiry + timedelta(seconds=1))
    assert all(row['body'] is None for row in source_rows(case))
    with pytest.raises(PageRunFailure, match='acknowledged_body_expired'):
        runtime.result_for_task(ledger.task)
    assert len(calls) == len(consumptions(case)) == 1


def test_grant_cap_three_job_budget_two_captures_only_two_page_scopes(batch_installed, monkeypatch, database):
    case, calls = batch_installed, []
    install_batch_child(monkeypatch, case, calls, pages=2, batch_budget=2)
    runtime = runtime_for_m2(case, database, [])
    runtime.deliver = lambda wire: [database.admit(value) for value in convert(wire)]
    ledger = batch_ledger(count_payload(max_pages=2))
    assert runtime.process_task(ledger=ledger, task=ledger.task, owner='two-page-budget', lease_seconds=60).state == TaskState.DONE
    assert len(calls) == len(consumptions(case)) == 1
    assert len(source_rows(case)) == 2
    pulled = runtime.result_for_task(ledger.task)
    assert len(pulled['items']) == len(database.list_evidence()) == 12
    assert pulled['coverage']['shortfall_reason'] == 'budget_exhausted'
    assert pulled['coverage']['source_complete'] is None
    batch = case.journal.batch_for_attempt(consumptions(case)[0][1])
    assert batch['declared_page_budget'] == batch['available_page_count'] == 2
    assert [(spec.start, spec.count) for spec in batch['pages']] == [(0, 3), (3, 10)]


def test_production_selection_helper_dedupes_equivalent_occurrence_aliases_and_rejects_conflict():
    from xingestion.linkedin.pages import select_company_items
    from nos_linkedin.pages import PageRunFailure
    first = dict(item_id='urn:li:activity:1', author=None,
        source_fields={'representation_id': 'urn:li:fsd_update:1', 'commentary': 'synthetic commentary'})
    alias = deepcopy(first)
    alias['source_fields']['representation_id'] = 'urn:li:fsd_update:alias'
    assert select_company_items({'items': [first, alias]}, ['urn:li:activity:1'])['items'] == [first]
    alias['source_fields']['commentary'] = 'synthetic conflicting commentary'
    with pytest.raises(PageRunFailure, match='selection_wire_conflict'):
        select_company_items({'items': [first, alias]}, ['urn:li:activity:1'])


def test_batch_actual_configured_worker_queue_authenticated_m2_http(batch_installed, monkeypatch, ledger, queue, m2, store):
    """Only the root-designated service verifier runs these SQLite/Postgres cases."""
    exercise_installed_grant_factory_queue_m2_http(batch_installed, monkeypatch, ledger, queue, m2, store,
        install_spy=lambda selected_monkeypatch, calls: install_batch_child(selected_monkeypatch, batch_installed, calls),
        requested_count=15, max_pages=3, expected_items=15, expected_attempts=3)
