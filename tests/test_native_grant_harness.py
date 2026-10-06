"""Native startup selection on synthetic stores; services and browser never run."""
from contextlib import closing
from datetime import UTC, datetime, timedelta
import importlib.util
import json
import sqlite3
import sys

import pytest

from test_grant_runtime import installed
from test_initial_company_page import PROJECT, project_manifest
from test_company_batch_runtime import batch_installed


@pytest.fixture
def harness(installed, monkeypatch):
    return load_harness(installed, monkeypatch)


@pytest.fixture
def batch_harness(batch_installed, monkeypatch):
    return load_harness(batch_installed, monkeypatch)


def load_harness(installed, monkeypatch):
    spec = importlib.util.spec_from_file_location('synthetic_native_grant_harness',
        PROJECT / 'docs/experiments/execute_native_startup_job.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, 'ROOT', installed.root)
    with closing(sqlite3.connect(installed.root / '.local/m2.sqlite')):
        pass
    return module


def test_harness_accepts_current_installed_grant_without_consumption_or_new_stores(installed, harness):
    before = installed.budget.read_bytes(), installed.journal.path.read_bytes()
    names = sorted(path.name for path in (installed.root / '.local').iterdir())
    result = harness.select_declaration(['--declared-grant', str(installed.path)])
    assert result['grant'] == installed.manifest
    assert result['reference'] == installed.reference
    assert result['data'] == installed.journal.path.parent
    assert result['grant_path'] == installed.path
    assert sorted(path.name for path in (installed.root / '.local').iterdir()) == names
    assert (installed.budget.read_bytes(), installed.journal.path.read_bytes()) == before


@pytest.mark.parametrize('fault', ['wrong_context', 'source_stop', 'consumed', 'sink_absent', 'pin_changed', 'hold', 'slot_binding', 'persisted_intervention'])
def test_harness_rejects_grant_before_any_service_or_source_call(installed, harness, monkeypatch, fault):
    case = installed
    if fault == 'wrong_context':
        case.reference['source_generation'] = 1
        case.path.write_text(json.dumps(case.reference))
    elif fault == 'source_stop':
        case.journal.stop('linkedin.company-feed', 'operator_stop')
    elif fault == 'consumed':
        with closing(sqlite3.connect(case.budget)) as connection, connection:
            connection.execute('INSERT INTO capture_consumptions VALUES(?,?,?,?,?)',
                (case.manifest['grant_id'], 'synthetic-consumed-attempt', 'synthetic-task', 'synthetic-run', datetime.now(UTC).isoformat()))
    elif fault == 'sink_absent':
        (case.root / '.local/m2.sqlite').unlink()
    elif fault == 'pin_changed':
        (case.root / 'scripts/company-bootstrap.cjs').write_text('synthetic different helper')
    elif fault == 'hold':
        (case.root / '.local/linkedin-acquisition-hold.json').write_text('{}')
    elif fault == 'slot_binding':
        with closing(sqlite3.connect(case.budget)) as connection, connection:
            connection.execute("UPDATE capture_profile_slot SET authority_id='synthetic-other-authority'")
    else:
        with closing(sqlite3.connect(case.budget)) as connection, connection:
            connection.execute('CREATE TABLE capture_interventions(attempt_id TEXT PRIMARY KEY,reason TEXT,observed_at TEXT)')
            connection.execute('INSERT INTO capture_interventions VALUES(?,?,?)',
                ('synthetic-earlier-attempt', 'challenge', datetime.now(UTC).isoformat()))
    before = case.budget.read_bytes(), case.journal.path.read_bytes()
    services = []
    monkeypatch.setattr(harness.psycopg, 'connect', lambda *args, **kwargs: services.append('postgres'))
    monkeypatch.setattr(harness.importlib.util, 'spec_from_file_location',
        lambda *args, **kwargs: services.append('redis-module'))
    monkeypatch.setattr(sys, 'argv', ['execute_native_startup_job.py', '--declared-grant', str(case.path)])
    with pytest.raises(ValueError):
        harness.main()
    assert services == []
    assert (case.budget.read_bytes(), case.journal.path.read_bytes()) == before


def test_harness_rejects_expired_selected_grant_before_services(installed, harness, monkeypatch):
    from xingestion.linkedin.live import GrantedBrowserCapture
    def expired(reference):
        selected = GrantedBrowserCapture(reference)
        selected.expires_at = datetime.now(UTC) - timedelta(seconds=1)
        return selected
    monkeypatch.setattr('xingestion.linkedin.live.GrantedBrowserCapture', expired)
    services = []
    monkeypatch.setattr(harness.psycopg, 'connect', lambda *args, **kwargs: services.append('postgres'))
    monkeypatch.setattr(harness.importlib.util, 'spec_from_file_location',
        lambda *args, **kwargs: services.append('redis-module'))
    monkeypatch.setattr(sys, 'argv', ['execute_native_startup_job.py', '--declared-grant', str(installed.path)])
    with pytest.raises(ValueError, match='current_bounded'):
        harness.main()
    assert services == []


def test_harness_malformed_arguments_never_load_services(installed, harness, monkeypatch):
    services = []
    monkeypatch.setattr(harness.importlib.util, 'spec_from_file_location',
        lambda *args, **kwargs: services.append('redis-module'))
    monkeypatch.setattr(sys, 'argv', ['execute_native_startup_job.py', '--declared-grant'])
    with pytest.raises(ValueError, match='unsupported_experiment_arguments'):
        harness.main()
    assert services == []


def test_harness_receipt_selects_only_attempts_owned_by_current_grant(installed, harness):
    case = installed
    with closing(sqlite3.connect(case.budget)) as connection, connection:
        connection.execute('CREATE TABLE capture_metadata(attempt_id TEXT,captured_at TEXT,native_sha TEXT,projection_sha TEXT,native_reads INTEGER)')
        connection.execute('CREATE TABLE initial_capture_provenance(attempt_id TEXT,navigation_sha TEXT,wrapper_sha TEXT,decoded_sha TEXT,metadata TEXT)')
        connection.execute('INSERT INTO capture_consumptions VALUES(?,?,?,?,?)',
            (case.manifest['grant_id'], 'selected-attempt', 'selected-task', 'selected-run', datetime.now(UTC).isoformat()))
        for attempt in ('selected-attempt', 'historical-attempt'):
            connection.execute('INSERT INTO capture_metadata VALUES(?,?,?,?,?)',
                (attempt, datetime.now(UTC).isoformat(), 'a'*64, 'b'*64, 2))
            connection.execute('INSERT INTO initial_capture_provenance VALUES(?,?,?,?,?)',
                (attempt, 'a'*64, 'b'*64, 'c'*64, '{}'))
    before = case.budget.read_bytes()
    with closing(sqlite3.connect(case.budget)) as connection:
        result = harness.collect_capture_metadata(connection, grant_id=case.manifest['grant_id'])
    assert result['grant_consumption'][0]['attempt_id'] == 'selected-attempt'
    assert [row['attempt_id'] for row in result['capture_metadata']] == ['selected-attempt']
    assert [row['attempt_id'] for row in result['initial_capture_provenance']] == ['selected-attempt']
    assert case.budget.read_bytes() == before


@pytest.mark.parametrize('flags,valid,budget', [
    ([], False, None),
    (['--requested-count', '15', '--max-pages', '1'], False, None),
    (['--requested-count', '15', '--max-pages', '4'], False, None),
    (['--requested-count', '15', '--max-pages', '3'], True, 3),
    (['--max-pages', '2', '--requested-count', '15'], True, 2),
])
def test_batch_harness_requires_explicit_count_and_actual_budget_before_services(batch_installed, batch_harness, monkeypatch, flags, valid, budget):
    harness = batch_harness
    case = batch_installed
    monkeypatch.setattr(harness, 'ROOT', case.root)
    with closing(sqlite3.connect(case.root / '.local/m2.sqlite')):
        pass
    arguments = ['--declared-grant', str(case.path), *flags]
    before = case.budget.read_bytes(), case.journal.path.read_bytes()
    services = []
    monkeypatch.setattr(harness.psycopg, 'connect', lambda *args, **kwargs: services.append('postgres'))
    monkeypatch.setattr(harness.importlib.util, 'spec_from_file_location',
        lambda *args, **kwargs: services.append('redis-module'))
    if valid:
        selected = harness.select_declaration(arguments)
        assert selected['requested_count'] == 15 and selected['max_pages'] == budget
    else:
        monkeypatch.setattr(sys, 'argv', ['execute_native_startup_job.py', *arguments])
        with pytest.raises(ValueError):
            harness.main()
    assert services == []
    assert (case.budget.read_bytes(), case.journal.path.read_bytes()) == before


def test_batch_harness_duplicate_wires_keep_one_original_attempt_and_page_scope(batch_harness):
    items = []
    for ordinal, start, count, selected in ((0, 0, 3, 3), (1, 3, 10, 9), (2, 13, 10, 3)):
        for number in range(selected):
            items.append(dict(item_id=f'urn:li:activity:{ordinal*10+number}',
                acquisition={'root_job_id': 'synthetic-batch-job', 'source_attempt_id': f'synthetic-attempt-{ordinal}',
                    'page_ordinal': ordinal},
                retention={'source_attempt_id': f'synthetic-attempt-{ordinal}',
                    'captured_at': '2026-10-06T00:00:00+00:00', 'expires_at': '2026-10-06T00:10:00+00:00'},
                source_fence={'authority_id': 'synthetic-authority', 'generation': 0, 'stopped': False},
                task_fence={'authority_id': 'synthetic-authority', 'job_id': 'synthetic-batch-job',
                    'generation': 1, 'cancelled': False},
                source_fields={'page_paging': {'start': start, 'count': count, 'total': 230},
                    'source_provenance': {'page_mode': 'initial_document' if ordinal == 0 else 'api_following'}},
                coverage={'gaps': [], 'limitations': []}))
    merged = dict(job_id='synthetic-batch-job', items=items,
        coverage={'requested_count': 15, 'returned_count': 15, 'fulfilled': True})
    original = json.dumps(merged, sort_keys=True)
    pages = list(batch_harness.page_delivery_wires(merged))
    assert [len(page['items']) for page in pages] == [3, 9, 3]
    assert [page['coverage']['paging']['start'] for page in pages] == [0, 3, 13]
    assert all(len({item['acquisition']['source_attempt_id'] for item in page['items']}) == 1 for page in pages)
    assert all(page['job_id'] == merged['job_id'] and len(page['items']) <= 10 for page in pages)
    assert [item for page in pages for item in page['items']] == items
    assert json.dumps(merged, sort_keys=True) == original
