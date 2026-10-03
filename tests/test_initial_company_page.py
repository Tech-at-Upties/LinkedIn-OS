"""Synthetic initial-document projection through actual M1 and SQLite M2.

No native receipt body is available for replay. All source content below is
generated fixture data; actual JavaScript exports run without their browser main.
"""
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256
import html
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
from types import SimpleNamespace

import pytest

PROJECT = Path(__file__).resolve().parents[1]
SOURCE = PROJECT / '.local/nos-integration'
sys.path[:0] = [str(SOURCE / 'M1/src'), str(SOURCE / 'M2/src')]

from nos_linkedin.acquisition import Journal, RoutePermission, SourceResponse
from nos_linkedin.pages import CompanyPageRuns, PageRunFailure
from nos_linkedin.parser import RECIPE, parse_company_feed
from nos_m2.linkedin import linkedin_envelope
from nos_m2.models import EvidenceEnvelope
from nos_m2.store import Store
from xingestion.capabilities import (CapabilityPlanner, CapabilityRequest,
                                   LinkedInCompanyFeedInput, LinkedInCompanyPlan)
from xingestion.capability_ids import CapabilityId
from xingestion.linkedin.live import BoundedBrowserCapture, declare_startup_repair_successor
from xingestion.linkedin.bootstrap import build_linkedin_runtime
from xingestion.linkedin.pages import execute_leased_company_page
from xingestion.linkedin.runtime import LinkedInJobRuntime
from test_parser import COMPANY, ROOT, fixture

TARGET = 'https://www.linkedin.com/company/linkedin/posts/'
REQUEST_URN = 'urn:li:fsd_organizationalPage:1337'
PURE_NODE_RUN = subprocess.run
PURE_NODE_BINARY = shutil.which('node')
LEGACY_PLAN = {'capability_id': 'LINKEDIN_COMPANY_FEED', 'contract_version': 1,
               'source_type': 'linkedin', 'release_id': 'linkedin-company-projection-1',
               'first_start': 3, 'page_size': 10, 'max_pages': 1, 'ordering': 'relevance'}


def synthetic_body(*, start=0, count=3):
    graph, roots = [], []
    for number in (103, 101, 102):
        nodes = json.loads(json.dumps(fixture()).replace('123', str(number)).replace('456', str(number + 1000)))
        roots.append(nodes[0]['entityUrn'])
        graph.extend(nodes)
    return {'data': {'data': {RECIPE: {'$type': 'com.linkedin.restli.common.CollectionResponse',
                                      '*elements': roots,
                                      'paging': {'start': start, 'count': count, 'total': 230}}}},
            'included': graph}


def synthetic_html(body=None, *, duplicate=False, cached_paging=False):
    body = synthetic_body() if body is None else body
    variables = '(organizationalPageUrn:urn%3Ali%3Afsd_organizationalPage%3A1337'
    if cached_paging:
        variables += ',start:3,count:10'
    variables += ')'
    envelope = {'method': 'GET', 'status': 200,
                'request': '/voyager/api/graphql?queryId=voyagerFeedDashOrganizationalPageUpdates.synthetic&variables=' + variables,
                'headers': {'csrf-token': 'synthetic-private-csrf', 'cookie': 'synthetic-private-cookie'},
                'body': 'synthetic-inert-source-id'}
    reference = '<code id="synthetic-inert-source-id"><!--' + html.escape(json.dumps(body), quote=True) + '--></code>'
    document = '<code>' + json.dumps(envelope) + '</code>' + reference
    if duplicate:
        document += reference
    return document


def actual_initial_projection(document, observed):
    assert PURE_NODE_BINARY, 'Node is required for the isolated projection boundary'
    navigation = {'observed_at': observed.isoformat(), 'method': 'GET',
                  'origin': 'https://www.linkedin.com', 'pathname': '/company/linkedin/posts/',
                  'status': 200, 'content_type': 'text/html',
                  'body_sha256': sha256(document.encode()).hexdigest()}
    javascript = """const p=require(process.argv[1]);let input='';
process.stdin.setEncoding('utf8');process.stdin.on('data',c=>input+=c);
process.stdin.on('end',()=>{const v=JSON.parse(input);console.log(JSON.stringify(
p.initialPageProjection(v.document,v.navigation,()=>{})));});"""
    process = PURE_NODE_RUN([PURE_NODE_BINARY, '-e', javascript, str(PROJECT / 'scripts/probe-linkedin-company.cjs')],
                            input=json.dumps({'document': document, 'navigation': navigation}).encode(),
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, timeout=5, cwd=PROJECT)
    return json.loads(process.stdout)


@pytest.fixture
def project_manifest(tmp_path):
    root = tmp_path / 'project'
    (root / '.local/linkedin-test-browser').mkdir(parents=True)
    (root / 'scripts').mkdir()
    for name in ('probe-linkedin-company.cjs', 'company-bootstrap.cjs'):
        (root / 'scripts' / name).write_bytes((PROJECT / 'scripts' / name).read_bytes())
    source = tmp_path / 'synthetic-installed-nos'
    (source / 'M3/node_modules/playwright').mkdir(parents=True)
    manifest = {'company_url': TARGET, 'feed_publisher_id': COMPANY,
                'feed_request_urn': REQUEST_URN, 'page_mode': 'initial_document',
                'evidence_class': 'allowlisted_source_projection', 'capture_budget': 1,
                'native_read_budget': 12, 'expires_at': (datetime.now(UTC) + timedelta(minutes=10)).isoformat(),
                'project_root': str(root), 'nos_source_root': str(source),
                'probe_sha256': sha256((root / 'scripts/probe-linkedin-company.cjs').read_bytes()).hexdigest(),
                'helper_sha256': sha256((root / 'scripts/company-bootstrap.cjs').read_bytes()).hexdigest()}
    return root, manifest


def install_capture_spy(monkeypatch, calls, *, mutate=None, duplicate=False, body=None, cached_paging=False):
    def child(command, **kwargs):
        observed = datetime.now(UTC)
        failure = None
        try:
            initial = actual_initial_projection(synthetic_html(body, duplicate=duplicate, cached_paging=cached_paging), observed)
        except subprocess.CalledProcessError:
            # The real probe catches projection refusal and emits bounded
            # failure metadata; its browser main is never run in this test.
            initial, failure = None, 'Error'
        receipt = {'target': TARGET, 'page_mode': 'initial_document',
                   'evidence_class': 'allowlisted_source_projection', 'session_verified': True,
                   'stopped': None, 'failure_kind': failure, 'account_writes_blocked': True,
                   'original_bodies_retained': False, 'native_reads_admitted': 2,
                   'native_read_budget': 12, 'continuation_response_observed': None,
                   'boundary_probe': None, 'initial_page': initial,
                   'responses': [{'operation_id': 'voyagerOrganizationDashCompanies.synthetic',
                                  'origin': 'https://www.linkedin.com', 'status': 200,
                                  'representation': {'source_native_ids': [COMPANY]}}]}
        api_body = json.loads(json.dumps(synthetic_body(start=3, count=10)).replace('103', '203').replace('101', '201').replace('102', '202'))
        receipt['responses'].append({
            'operation_id': 'voyagerFeedDashOrganizationalPageUpdates.synthetic', 'status': 200,
            'method': 'GET', 'origin': 'https://www.linkedin.com', 'observed_at': observed.isoformat(),
            'content_type': 'application/json', 'body_sha256': 'a' * 64,
            'request_parameters': {'fields': {'start': 3, 'count': 10, 'organizationalPageUrn':
                {'present': True, 'value_sha256': sha256(REQUEST_URN.encode()).hexdigest()}}},
            'representation': {'collection_projection': {'status': 'captured', 'recipe': RECIPE,
                'fields': api_body['data']['data'][RECIPE]}, 'projected_graph': api_body['included'],
                'duplicate_entity_ids': [], 'traversal_bounded': False}})
        if mutate:
            mutate(receipt)
        calls.append((command, kwargs, deepcopy(receipt)))
        return SimpleNamespace(returncode=0, stdout=json.dumps(receipt).encode())
    monkeypatch.setattr('xingestion.linkedin.live.subprocess.run', child)


def test_default_and_initial_capability_plans_keep_distinct_pages():
    legacy = LinkedInCompanyFeedInput(TARGET, COMPANY)
    request = CapabilityRequest(CapabilityId.LINKEDIN_COMPANY_FEED, 1, legacy)
    assert CapabilityPlanner(None).plan(request).public_dict() == LEGACY_PLAN
    assert request.public_dict()['payload'] == {'company_url': TARGET, 'feed_publisher_id': COMPANY, 'max_pages': 1}
    initial = LinkedInCompanyFeedInput(TARGET, COMPANY, page_mode='initial_document')
    request = CapabilityRequest(CapabilityId.LINKEDIN_COMPANY_FEED, 1, initial)
    plan = CapabilityPlanner(None).plan(request)
    assert plan.page_scope == (0, 3)
    assert plan.public_dict()['page_mode'] == 'initial_document'
    assert request.public_dict()['payload']['page_mode'] == 'initial_document'
    assert plan.public_dict()['max_pages'] == 1


@pytest.mark.parametrize('mode', ['initial_document', 'api_following', 'invalid'])
def test_actual_web_payload_parser_preserves_validated_mode(mode):
    from xingestion.web.live_server import _capability_payload_from_dict
    values = {'company_url': TARGET, 'feed_publisher_id': COMPANY}
    if mode != 'api_following':
        values['page_mode'] = mode
    payload = _capability_payload_from_dict(CapabilityId.LINKEDIN_COMPANY_FEED.value, values)
    if mode == 'invalid':
        with pytest.raises(ValueError):
            payload.validate()
    else:
        payload.validate()
        assert payload.page_mode == mode


@pytest.mark.parametrize('cached_paging', [False, True])
def test_actual_initial_projection_uses_decoded_scope_and_honest_cached_parameters(cached_paging):
    document = synthetic_html(cached_paging=cached_paging)
    initial = actual_initial_projection(document, datetime.now(UTC))
    assert initial['representation_kind'] == 'initial_document_inert_reference'
    assert initial['navigation']['body_sha256'] == sha256(document.encode()).hexdigest()
    assert initial['body_sha256'] != initial['wrapper_sha256']
    assert initial['wrapper_sha256'] != initial['navigation']['body_sha256']
    fields = initial['request_parameters']['fields']
    assert fields['organizationalPageUrn'] == {'present': True, 'value_sha256': sha256(REQUEST_URN.encode()).hexdigest()}
    assert fields.get('start') == (3 if cached_paging else None)
    assert fields.get('count') == (10 if cached_paging else None)
    collection = initial['representation']['collection_projection']['fields']
    assert collection['paging'] == {'start': 0, 'count': 3, 'total': 230}
    assert collection['*elements'] == synthetic_body()['data']['data'][RECIPE]['*elements']
    serialized = json.dumps(initial)
    assert 'synthetic-inert-source-id' not in serialized
    assert 'synthetic-private-csrf' not in serialized
    assert 'synthetic-private-cookie' not in serialized


def test_actual_capture_selects_only_initial_page_and_records_hash_classes(project_manifest, monkeypatch):
    root, manifest = project_manifest
    calls = []
    install_capture_spy(monkeypatch, calls)
    capture = BoundedBrowserCapture(manifest)
    lease = SimpleNamespace(start=0, count=3, attempt_id='synthetic-initial-capture')
    response = capture(capture.target, lease)
    page = parse_company_feed(json.loads(response.body), feed_publisher_id=COMPANY,
                              observed_at=response.captured_at, evidence_class='allowlisted_source_projection')
    assert [item.occurrence_id for item in page.publications] == ['urn:li:activity:103', 'urn:li:activity:101', 'urn:li:activity:102']
    assert page.paging == {'start': 0, 'count': 3, 'total': 230}
    assert all(item.evidence_class == 'allowlisted_source_projection' for item in page.publications)
    command, kwargs, receipt = calls[0]
    assert '--runtime-initial-page' in command and '--runtime-capture' in command
    assert '--bootstrap-deadline=' + manifest['expires_at'] in command
    assert 0 < kwargs['timeout'] <= 85
    assert receipt['responses'][1]['request_parameters']['fields']['start'] == 3
    with closing(sqlite3.connect(capture.budget_path)) as connection:
        metadata = connection.execute('SELECT native_sha,projection_sha FROM capture_metadata').fetchone()
        provenance = connection.execute('SELECT navigation_sha,wrapper_sha,decoded_sha,metadata FROM initial_capture_provenance').fetchone()
        assert metadata == (receipt['initial_page']['body_sha256'], sha256(response.body).hexdigest())
        assert provenance[:3] == (receipt['initial_page']['navigation']['body_sha256'],
                                  receipt['initial_page']['wrapper_sha256'], receipt['initial_page']['body_sha256'])
        assert len(set((*provenance[:3], metadata[1]))) == 4
        assert json.loads(provenance[3])['representation_kind'] == 'initial_document_inert_reference'
        assert 'Own commentary' not in provenance[3]
        assert 'synthetic-private-cookie' not in provenance[3]
    with pytest.raises(ValueError, match='consumed'):
        capture(capture.target, SimpleNamespace(start=0, count=3, attempt_id='second-attempt'))
    changed = dict(manifest, page_mode='api_following')
    with pytest.raises(ValueError, match='Another live experiment'):
        BoundedBrowserCapture(changed)
    assert len(calls) == 1


@pytest.mark.parametrize('fault', ['probe_pin', 'helper_pin', 'missing_helper_pin', 'wrong_lease', 'wrong_payload'])
def test_initial_scope_and_pins_refuse_before_dispatch(project_manifest, monkeypatch, fault):
    root, manifest = project_manifest
    calls = []
    install_capture_spy(monkeypatch, calls)
    if fault == 'probe_pin':
        manifest['probe_sha256'] = 'f' * 64
    elif fault == 'helper_pin':
        manifest['helper_sha256'] = 'f' * 64
    elif fault == 'missing_helper_pin':
        del manifest['helper_sha256']
    if fault in {'probe_pin', 'helper_pin', 'missing_helper_pin'}:
        with pytest.raises(ValueError):
            BoundedBrowserCapture(manifest)
    else:
        capture = BoundedBrowserCapture(manifest)
        payload = capture.target if fault == 'wrong_lease' else LinkedInCompanyFeedInput(TARGET, COMPANY)
        lease = SimpleNamespace(start=3 if fault == 'wrong_lease' else 0, count=10 if fault == 'wrong_lease' else 3,
                                attempt_id='wrong-scope')
        with pytest.raises(ValueError, match='scope'):
            capture(payload, lease)
        with closing(sqlite3.connect(capture.budget_path)) as connection:
            assert connection.execute('SELECT consumed_at FROM capture_allowance').fetchone()[0] is None
    assert calls == []


@pytest.mark.parametrize('fault', ['receipt_mode', 'request_identity', 'navigation_scope', 'wrapper_hash',
                                  'decoded_hash', 'representation_kind', 'paging', 'ambiguous_reference'])
def test_invalid_initial_receipt_consumes_once_without_fallback_to_api(project_manifest, monkeypatch, fault):
    root, manifest = project_manifest
    calls = []
    def mutate(receipt):
        initial = receipt['initial_page']
        if fault == 'receipt_mode':
            receipt['page_mode'] = 'api_following'
        elif fault == 'request_identity':
            initial['request_parameters']['fields']['organizationalPageUrn']['value_sha256'] = 'f' * 64
        elif fault == 'navigation_scope':
            initial['navigation']['pathname'] = '/company/other/posts/'
        elif fault == 'wrapper_hash':
            initial['wrapper_sha256'] = 'wrong'
        elif fault == 'decoded_hash':
            initial['body_sha256'] = 'wrong'
        elif fault == 'representation_kind':
            initial['representation_kind'] = 'native_response'
        elif fault == 'paging':
            initial['representation']['collection_projection']['fields']['paging']['count'] = 13
    install_capture_spy(monkeypatch, calls, mutate=mutate, duplicate=fault == 'ambiguous_reference')
    capture = BoundedBrowserCapture(manifest)
    lease = SimpleNamespace(start=0, count=3, attempt_id='failed-initial')
    with pytest.raises(ValueError):
        capture(capture.target, lease)
    with pytest.raises(ValueError):
        BoundedBrowserCapture(manifest)(capture.target, lease)
    assert len(calls) == 1
    with closing(sqlite3.connect(capture.budget_path)) as connection:
        assert connection.execute('SELECT consumed_at FROM capture_allowance').fetchone()[0]


def test_initial_capture_replays_to_actual_m2_sqlite_without_rereading_source(project_manifest, tmp_path, monkeypatch):
    _, manifest = project_manifest
    calls = []
    install_capture_spy(monkeypatch, calls)
    capture = BoundedBrowserCapture(manifest)
    journal = Journal(tmp_path / 'initial-source.sqlite')
    runtime = LinkedInJobRuntime(journal=journal, send=capture, deliver=None,
                                 evidence_class='allowlisted_source_projection', page_mode='initial_document',
                                 feed_request_urn=REQUEST_URN)
    now = datetime.now(UTC)
    runtime.runs.create(run_id='initial-run', feed_publisher_id=COMPANY,
                        evidence_class='allowlisted_source_projection', first_start=0, count=3, page_budget=1)
    lease = runtime.runs.claim('initial-run', owner='first', now=now)
    permission = RoutePermission(True, True, now + timedelta(minutes=5), 'allowlisted_source_projection')
    database = Store(f'sqlite:///{(tmp_path / "initial-m2.sqlite").as_posix()}')
    database.initialize()
    deliveries = []
    delivered_wires = []
    runtime.synchronize_fence = database.update_source_fence
    def admit(wire):
        runtime.sync_source_fence()
        owned = runtime._wire(wire, 'initial-run', lease.attempt_id, 0)
        delivered_wires.append(deepcopy(owned))
        records = [EvidenceEnvelope.model_validate(linkedin_envelope(item, job_id=owned['job_id'], watch_id=None))
                   for item in owned['items']]
        deliveries.append([database.admit(record).duplicate_delivery for record in records])
    def interrupted(*args):
        raise RuntimeError('synthetic loss after committed M2 acknowledgement')
    monkeypatch.setattr(runtime.runs, 'checkpoint', interrupted)
    try:
        with pytest.raises(RuntimeError, match='synthetic loss'):
            execute_leased_company_page(runs=runtime.runs, lease=lease, permission=permission,
                send=lambda selected: capture(capture.target, selected), deliver=admit)
        assert deliveries == [[False, False, False]]
        reopened = CompanyPageRuns(Journal(journal.path))
        recovered = reopened.claim('initial-run', owner='recovery', now=now + timedelta(seconds=61))
        assert recovered.attempt_id == lease.attempt_id
        def forbidden_source(_):
            raise AssertionError('Retained projection replay must not re-read source')
        result = execute_leased_company_page(runs=reopened, lease=recovered, permission=permission,
                send=forbidden_source, deliver=admit, clock=lambda: now + timedelta(seconds=61))
        assert result.replayed
        assert deliveries == [[False, False, False], [True, True, True]]
        assert len(calls) == 1
        with journal.connect() as connection:
            retained = json.loads(connection.execute('SELECT body FROM responses WHERE attempt_id=?', (lease.attempt_id,)).fetchone()[0])
        provenance = retained['source_provenance']
        assert provenance['page_mode'] == 'initial_document'
        assert provenance['probe_sha256'] == manifest['probe_sha256']
        assert provenance['helper_sha256'] == manifest['helper_sha256']
        assert provenance['initial_page']['representation_kind'] == 'initial_document_inert_reference'
        assert all(item['source_fields']['source_provenance'] == provenance
                   for owned in delivered_wires for item in owned['items'])
        task = SimpleNamespace(task_id='initial-run', delivery_generation=0,
            request_json=CapabilityRequest(CapabilityId.LINKEDIN_COMPANY_FEED, 1, capture.target).public_dict(),
            plan_json=LinkedInCompanyPlan(page_mode='initial_document').public_dict())
        pulled = runtime.result_for_task(task)
        assert all(item['source_fields']['source_provenance'] == provenance for item in pulled['items'])
        assert len(database.list_observations('linkedin:urn:li:activity:103')) == 1
        assert reopened.summary('initial-run')['completed_pages'] == 1
        assert reopened.summary('initial-run')['source_complete'] is None
    finally:
        database.close()


@pytest.mark.parametrize('field,value', [
    ('company_url', 'https://www.linkedin.com/company/other/posts/'),
    ('feed_publisher_id', 'urn:li:fsd_company:999'),
    ('feed_request_urn', 'urn:li:fsd_organizationalPage:999'),
])
def test_initial_observed_target_pair_refuses_before_allowance(project_manifest, field, value):
    root, manifest = project_manifest
    manifest[field] = value
    with pytest.raises(ValueError):
        BoundedBrowserCapture(manifest)
    assert not (root / '.local/linkedin-live-capture.sqlite').exists()


def test_acknowledged_api_page_cannot_be_relabelled_as_initial_document(tmp_path):
    now = datetime.now(UTC)
    journal = Journal(tmp_path / 'relabel-source.sqlite')
    original = LinkedInJobRuntime(journal=journal, send=None, deliver=None,
                                  evidence_class='synthetic_fixture')
    original.runs.create(run_id='acknowledged-api', feed_publisher_id=COMPANY,
                         evidence_class='synthetic_fixture', first_start=3, count=10, page_budget=1)
    lease = original.runs.claim('acknowledged-api', owner='first', now=now)
    permission = RoutePermission(True, True, now + timedelta(minutes=5), 'synthetic_fixture')
    response = SourceResponse(200, 'application/json', json.dumps(synthetic_body(start=3, count=10)).encode(), now)
    execute_leased_company_page(runs=original.runs, lease=lease, permission=permission,
                                send=lambda selected: response, deliver=lambda wire: None, clock=lambda: now)
    payload = LinkedInCompanyFeedInput(TARGET, COMPANY)
    task = SimpleNamespace(task_id='acknowledged-api', delivery_generation=0,
        request_json=CapabilityRequest(CapabilityId.LINKEDIN_COMPANY_FEED, 1, payload).public_dict(),
        plan_json=LEGACY_PLAN)
    assert original.result_for_task(task)['items']
    relabelled = LinkedInJobRuntime(journal=Journal(journal.path), send=None, deliver=None,
                                    evidence_class='synthetic_fixture', page_mode='initial_document',
                                    feed_request_urn=REQUEST_URN)
    task.request_json = CapabilityRequest(CapabilityId.LINKEDIN_COMPANY_FEED, 1,
                        LinkedInCompanyFeedInput(TARGET, COMPANY, page_mode='initial_document')).public_dict()
    task.plan_json = LinkedInCompanyPlan(page_mode='initial_document').public_dict()
    with pytest.raises((ValueError, PageRunFailure)):
        relabelled.result_for_task(task)


@pytest.mark.parametrize('scope_change', [True, False])
def test_startup_repair_cannot_change_normalized_page_scope(project_manifest, scope_change):
    root, manifest = project_manifest
    journal_path = root / '.local/failed-source.sqlite'
    journal = Journal(journal_path)
    journal.generation('linkedin.company-feed')
    manifest['source_journal'] = str(journal_path)
    old = dict(manifest)
    old.pop('page_mode')
    old.pop('helper_sha256')
    old['expires_at'] = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
    new = dict(manifest)
    if not scope_change:
        new['page_mode'] = 'api_following'
    previous, successor, witness = (root / '.local' / name for name in ('previous.json', 'successor.json', 'repair.json'))
    previous.write_text(json.dumps(old))
    successor.write_text(json.dumps(new))
    witness.write_text(json.dumps({'source_navigations': 0, 'outcomes': [
        {'mode': 'old_filtered', 'result': {'chrome_install_not_found': True}},
        {'mode': 'with_install_paths', 'result': {'launched': True, 'only_navigation': 'about:blank'}}]}))
    old_sha = sha256(json.dumps(old, sort_keys=True, allow_nan=False).encode()).hexdigest()
    budget = root / '.local/linkedin-live-capture.sqlite'
    with closing(sqlite3.connect(budget)) as connection, connection:
        connection.execute('CREATE TABLE capture_allowance(singleton INTEGER PRIMARY KEY,declaration_sha TEXT,attempt_id TEXT,consumed_at TEXT)')
        connection.execute('INSERT INTO capture_allowance VALUES(1,?,?,?)', (old_sha, 'failed-attempt', old['expires_at']))
    with journal.connect() as connection:
        connection.execute('INSERT INTO responses VALUES(?,?,?,?,?,?,?,?,?,?,?)',
            ('failed-attempt', 'linkedin.company-feed', 0, None, None, None,
             'allowlisted_source_projection', None, 0, None, 'transport_failure'))
    before = budget.read_bytes()
    if scope_change:
        with pytest.raises(ValueError, match='page mode'):
            declare_startup_repair_successor(previous_manifest=previous, next_manifest=successor,
                                             previous_journal=journal_path, witness=witness)
        assert budget.read_bytes() == before
        with closing(sqlite3.connect(budget)) as connection:
            assert not connection.execute("SELECT 1 FROM sqlite_master WHERE name='capture_allowance_history'").fetchone()
    else:
        result = declare_startup_repair_successor(previous_manifest=previous, next_manifest=successor,
                                                previous_journal=journal_path, witness=witness)
        assert result['previous_attempt_id'] == 'failed-attempt'
        with closing(sqlite3.connect(budget)) as connection:
            assert connection.execute('SELECT count(*) FROM capture_allowance_history').fetchone()[0] == 1


def synthetic_saved_manifest():
    captured = datetime.now(UTC) - timedelta(seconds=2)
    initial = actual_initial_projection(synthetic_html(), captured)
    selected = BoundedBrowserCapture.__new__(BoundedBrowserCapture)
    selected.target = LinkedInCompanyFeedInput(TARGET, COMPANY, page_mode='initial_document')
    selected.page_mode = 'initial_document'
    selected.request_urn = REQUEST_URN
    selected.expires_at = datetime.now(UTC) + timedelta(minutes=5)
    # Saved pins describe the original synthetic capture. They intentionally
    # differ from the current scripts; replay must not reinterpret their origin.
    selected.script_sha, selected.helper_sha = 'a' * 64, 'b' * 64
    receipt = {'target': TARGET, 'page_mode': 'initial_document',
        'evidence_class': 'allowlisted_source_projection', 'session_verified': True, 'stopped': None,
        'failure_kind': None, 'account_writes_blocked': True, 'original_bodies_retained': False,
        'native_reads_admitted': 2, 'native_read_budget': 12,
        'continuation_response_observed': None, 'boundary_probe': None, 'initial_page': initial,
        'responses': [{'operation_id': 'voyagerOrganizationDashCompanies.synthetic', 'status': 200,
                       'origin': 'https://www.linkedin.com', 'representation': {'source_native_ids': [COMPANY]}}]}
    body, _, _ = selected._project_receipt(receipt, started=captured - timedelta(seconds=1))
    metadata = {key: value for key, value in initial.items() if key != 'representation'}
    return {'company_url': TARGET, 'feed_publisher_id': COMPANY, 'feed_request_urn': REQUEST_URN,
        'page_mode': 'initial_document', 'evidence_class': 'allowlisted_source_projection',
        'probe_sha256': selected.script_sha, 'helper_sha256': selected.helper_sha,
        'initial_page': metadata, 'captured_at': captured.isoformat(),
        'expires_at': selected.expires_at.isoformat(), 'body': json.loads(body)}


def saved_runtime(tmp_path, monkeypatch, manifest):
    path = tmp_path / 'synthetic-initial-replay.json'
    path.write_text(json.dumps(manifest))
    monkeypatch.setenv('LINKEDIN_M2_API_TOKEN', 'synthetic-token-unused-for-network')
    config = SimpleNamespace(linkedin_replay_path=path, linkedin_live_path=None,
                             data_dir=tmp_path, linkedin_m2_ingest_url='http://127.0.0.1:1/v1/linkedin/results')
    return build_linkedin_runtime(config)


def test_saved_initial_startup_preserves_original_pins_provenance_and_scope(tmp_path, monkeypatch):
    manifest = synthetic_saved_manifest()
    assert manifest['probe_sha256'] != sha256((PROJECT / 'scripts/probe-linkedin-company.cjs').read_bytes()).hexdigest()
    runtime = saved_runtime(tmp_path, monkeypatch, manifest)
    target = LinkedInCompanyFeedInput(TARGET, COMPANY, page_mode='initial_document')
    response = runtime.send(target, SimpleNamespace(start=0, count=3))
    body = json.loads(response.body)
    assert body['source_provenance'] == manifest['body']['source_provenance']
    assert body['data']['data'][RECIPE]['paging'] == {'start': 0, 'count': 3, 'total': 230}
    with pytest.raises(ValueError):
        runtime.send(target, SimpleNamespace(start=3, count=10))
    assert not list(tmp_path.rglob('linkedin-live-capture.sqlite'))


@pytest.mark.parametrize('fault', ['foreign_target', 'foreign_publisher', 'foreign_request', 'expired',
                                  'missing_provenance', 'metadata_disagrees', 'wrong_paging'])
def test_saved_initial_startup_refuses_relabelled_or_expired_projection(tmp_path, monkeypatch, fault):
    manifest = synthetic_saved_manifest()
    if fault == 'foreign_target':
        manifest['company_url'] = 'https://www.linkedin.com/company/other/posts/'
    elif fault == 'foreign_publisher':
        manifest['feed_publisher_id'] = 'urn:li:fsd_company:999'
    elif fault == 'foreign_request':
        manifest['feed_request_urn'] = 'urn:li:fsd_organizationalPage:999'
    elif fault == 'expired':
        manifest['expires_at'] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    elif fault == 'missing_provenance':
        del manifest['initial_page']
    elif fault == 'metadata_disagrees':
        manifest['body']['source_provenance']['probe_sha256'] = 'f' * 64
    else:
        manifest['body']['data']['data'][RECIPE]['paging']['start'] = 3
    with pytest.raises((ValueError, PageRunFailure)):
        saved_runtime(tmp_path, monkeypatch, manifest)
    assert not list(tmp_path.rglob('linkedin-live-capture.sqlite'))


@pytest.mark.parametrize('boundary', ['source_stop', 'original_expiry'])
def test_initial_projection_is_fenced_after_parse_before_delivery(project_manifest, tmp_path, monkeypatch, boundary):
    _, manifest = project_manifest
    calls, deliveries = [], []
    install_capture_spy(monkeypatch, calls)
    capture = BoundedBrowserCapture(manifest)
    journal = Journal(tmp_path / 'fenced-initial.sqlite')
    runs = CompanyPageRuns(journal)
    now = datetime.now(UTC)
    runs.create(run_id='fenced-initial', feed_publisher_id=COMPANY,
                evidence_class='allowlisted_source_projection', first_start=0, count=3, page_budget=1)
    lease = runs.claim('fenced-initial', owner='first', now=now)
    permission = RoutePermission(True, True, now + timedelta(seconds=30), 'allowlisted_source_projection')
    current = [now]
    finish = journal.finish
    def classify_then_fence(*args):
        result = finish(*args)
        if boundary == 'source_stop':
            journal.stop('linkedin.company-feed', 'challenge')
        else:
            current[0] = permission.raw_expires_at + timedelta(seconds=1)
        return result
    monkeypatch.setattr(journal, 'finish', classify_then_fence)
    result = execute_leased_company_page(runs=runs, lease=lease, permission=permission,
        send=lambda selected: capture(capture.target, selected), deliver=deliveries.append,
        clock=lambda: current[0])
    expected = 'quarantined_after_stop' if boundary == 'source_stop' else 'policy_expired'
    assert result.delivery_outcome == expected
    assert deliveries == [] and len(calls) == 1
    assert runs.summary('fenced-initial')['completed_pages'] == 0
    with journal.connect() as connection:
        record = connection.execute('SELECT outcome,body,body_sha256 FROM responses WHERE attempt_id=?',
                                    (lease.attempt_id,)).fetchone()
    assert record['outcome'] == expected and len(record['body_sha256']) == 64
    if boundary == 'original_expiry':
        assert record['body'] is None
    with pytest.raises(ValueError, match='consumed'):
        capture(capture.target, SimpleNamespace(start=0, count=3, attempt_id='forbidden-retry'))
    assert len(calls) == 1


@pytest.mark.parametrize('reason', ['challenge_path', 'authentication_required'])
def test_initial_security_receipt_persists_hold_and_never_renews_allowance(project_manifest, monkeypatch, reason):
    _, manifest = project_manifest
    calls = []
    def intervention(receipt):
        receipt['session_verified'] = False
        receipt['stopped'] = {'reason': reason}
    install_capture_spy(monkeypatch, calls, mutate=intervention)
    capture = BoundedBrowserCapture(manifest)
    lease = SimpleNamespace(start=0, count=3, attempt_id='security-initial')
    with pytest.raises(ValueError):
        capture(capture.target, lease)
    before = capture.budget_path.read_bytes()
    with closing(sqlite3.connect(capture.budget_path)) as connection:
        assert connection.execute('SELECT consumed_at FROM capture_allowance').fetchone()[0]
        assert connection.execute('SELECT attempt_id FROM capture_interventions').fetchone()[0] == lease.attempt_id
    with pytest.raises(ValueError):
        BoundedBrowserCapture(manifest)(capture.target, SimpleNamespace(start=0, count=3, attempt_id='retry'))
    assert capture.budget_path.read_bytes() == before
    assert len(calls) == 1
