"""Isolated SQLite and process checks; no runtime, services or source access."""
from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from hashlib import sha256
import importlib.util
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor

import pytest
from nos_linkedin.acquisition import Journal

PROJECT = Path(__file__).resolve().parents[1]
MODULE = PROJECT / '.local/nos-integration/M1/src/xingestion/linkedin/grants.py'
spec = importlib.util.spec_from_file_location('observation_grants', MODULE)
grants = importlib.util.module_from_spec(spec)
spec.loader.exec_module(grants)


@contextmanager
def connect(*args, **kwargs):
    connection = sqlite3.connect(*args, **kwargs)
    try:
        with connection:
            yield connection
    finally:
        connection.close()


@pytest.fixture
def case():
    local = (PROJECT / '.local').resolve()
    assert local.is_relative_to(PROJECT.resolve())
    with tempfile.TemporaryDirectory(prefix='wr068-grants-', dir=local) as temporary:
        root = Path(temporary)
        (root / '.local/linkedin-test-browser').mkdir(parents=True)
        (root / 'scripts').mkdir()
        for name in ('probe-linkedin-company.cjs', 'company-bootstrap.cjs'):
            (root / 'scripts' / name).write_text('offline synthetic bytes')
        budget = root / '.local/linkedin-live-capture.sqlite'
        with connect(budget) as connection:
            connection.execute('CREATE TABLE legacy_fact(value TEXT)')
            connection.execute("INSERT INTO legacy_fact VALUES ('preserve-me')")
        journal = Journal(root / '.local/source.sqlite')
        journal.generation(grants.SCOPE)
        with journal.connect() as connection:
            connection.execute('CREATE TABLE owned_sink_authority(singleton INTEGER PRIMARY KEY, authority_id TEXT)')
            connection.execute("INSERT INTO owned_sink_authority VALUES (1,'li-authority-test')")
        moment = datetime(2026, 10, 3, 12, tzinfo=UTC)
        clock = [moment]
        context = dict(project_root=root, source_journal=journal.path, authority_id='li-authority-test', source_generation=0)
        issuer = grants.GrantIssuer(**context, clock=lambda: clock[0])
        issuer.initialize()
        consumer = grants.GrantConsumer(**context, clock=lambda: clock[0])
        def manifest(index):
            return dict(kind='linkedin-observation-grant/1', grant_id=f'grant-{index}',
                previous_grant_id=None if index == 0 else f'grant-{index-1}', workload_id='finite-plan',
                workload_position=index, declared_at=moment.isoformat(), reason='explicit_observation',
                project_root=str(root), profile=str(issuer.profile), source_journal=str(journal.path),
                authority_id='li-authority-test', source_generation=0, company_url=grants.TARGET,
                feed_publisher_id='urn:li:fsd_company:1337', feed_request_urn='urn:li:fsd_organizationalPage:1337',
                recipe=grants.RECIPE, start=3, count=10, probe_sha256=sha256(b'offline synthetic bytes').hexdigest(),
                helper_sha256=sha256(b'offline synthetic bytes').hexdigest(),
                expires_at=(moment + timedelta(minutes=10 + 10*index)).isoformat(),
                capture_budget=1, native_read_budget=12, evidence_class='allowlisted_source_projection')
        manifests = [manifest(0), manifest(1)]
        workload = dict(workload_id='finite-plan', declaration_shas=list(map(grants.declaration_sha, manifests)), native_read_budget=24)
        yield dict(root=root, budget=budget, journal=journal, moment=moment, clock=clock,
                   context=context, issuer=issuer, consumer=consumer, manifests=manifests, workload=workload)


def declare(case, index=0):
    return case['issuer'].declare(case['manifests'][index], workload=case['workload'])


def consume(case, index=0):
    return case['consumer'].consume(case['manifests'][index], attempt_id=f'attempt-{index}', task_id=f'task-{index}', run_id=f'run-{index}')


def record(case, outcome='bounded', index=0):
    with case['journal'].connect() as connection:
        connection.execute('INSERT INTO responses VALUES (?,?,?,?,?,?,?,?,?,?,?)',
            (f'attempt-{index}', grants.SCOPE, 0, 200, 'application/json', None,
             'allowlisted_source_projection', None, 0, None, outcome))


def witness(case, outcome='bounded', kind='classified', index=0):
    value = dict(kind='observation-resolution/1', grant_id=f'grant-{index}', attempt_id=f'attempt-{index}',
        task_id=f'task-{index}', run_id=f'run-{index}', source_journal=str(case['journal'].path),
        authority_id='li-authority-test', source_generation=0, source_outcome=outcome,
        resolution_kind=kind, observed_at=case['clock'][0].isoformat(),
        classification_sha256='a'*64, process_cessation_sha256='b'*64)
    path = case['root'] / '.local/resolution.json'
    path.write_text(json.dumps(value))
    return path


def slot(case):
    with connect(case['budget']) as connection:
        return connection.execute('SELECT active_grant_id,slot_generation FROM capture_profile_slot').fetchone()


def test_explicit_declare_consume_close_and_elapsed_successor(case):
    sha = declare(case)
    assert declare(case) == sha and slot(case) == ('grant-0', 1)
    assert not hasattr(case['consumer'], 'declare') and not hasattr(case['consumer'], 'resolve')
    assert consume(case)['attempt_id'] == 'attempt-0'
    with pytest.raises(ValueError, match='already_consumed'):
        consume(case)
    record(case)
    proof = witness(case)
    case['issuer'].resolve('grant-0', witness_path=proof)
    assert slot(case) == (None, 2)
    assert case['issuer'].resolve('grant-0', witness_path=proof)
    with pytest.raises(ValueError, match='unelapsed'):
        declare(case, 1)
    case['clock'][0] += timedelta(minutes=11)
    declare(case, 1)
    consume(case, 1)
    assert slot(case) == ('grant-1', 3)
    assert case['consumer'].read('grant-0') == case['manifests'][0]
    with connect(case['budget']) as connection:
        assert connection.execute('SELECT value FROM legacy_fact').fetchone()[0] == 'preserve-me'
        assert connection.execute('SELECT count(*) FROM capture_consumptions').fetchone()[0] == 2


def test_consumed_unknown_and_deadline_never_release_slot(case):
    declare(case)
    consume(case)
    case['clock'][0] += timedelta(minutes=11)
    with pytest.raises(ValueError, match='occupied'):
        declare(case, 1)
    with pytest.raises(ValueError, match='evidence_missing'):
        case['issuer'].resolve('grant-0', witness_path=witness(case))
    assert slot(case) == ('grant-0', 1)


@pytest.mark.parametrize('outcome', ['dispatch_unknown', 'captured', 'classification_expired'])
def test_unresolved_journal_blocks_every_write(case, outcome):
    declare(case)
    record(case, outcome)
    with pytest.raises(ValueError, match='unresolved_source'):
        consume(case)
    assert slot(case) == ('grant-0', 1)


@pytest.mark.parametrize('changed', ['authority_id', 'source_generation', 'profile', 'source_journal',
    'company_url', 'feed_publisher_id', 'feed_request_urn', 'recipe', 'start', 'count', 'probe_sha256', 'helper_sha256'])
def test_changed_manifest_cannot_consume_installed_grant(case, changed):
    declare(case)
    value = deepcopy(case['manifests'][0])
    value[changed] = 1 if changed in {'source_generation', 'start', 'count'} else 'changed'
    with pytest.raises((ValueError, TypeError)):
        case['consumer'].consume(value, attempt_id='different', task_id='different', run_id='different')
    assert slot(case) == ('grant-0', 1)


@pytest.mark.parametrize('consumed', [True, False])
def test_legacy_singleton_is_never_imported_replenished_or_activated(case, consumed):
    with connect(case['budget']) as connection:
        connection.execute('CREATE TABLE capture_allowance(singleton INTEGER, declaration_sha TEXT, attempt_id TEXT, consumed_at TEXT)')
        row = (1, 'old-sha', 'old-attempt' if consumed else None, 'old-time' if consumed else None)
        connection.execute('INSERT INTO capture_allowance VALUES (?,?,?,?)', row)
    with pytest.raises(ValueError, match='legacy_consumption'):
        declare(case)
    with connect(case['budget']) as connection:
        assert connection.execute('SELECT * FROM capture_allowance').fetchone() == row
        assert connection.execute('SELECT count(*) FROM capture_grants').fetchone()[0] == 0


def test_competing_consumers_commit_exactly_once(case):
    declare(case)
    def competitor(number):
        try:
            return case['consumer'].consume(case['manifests'][0], attempt_id=f'a-{number}', task_id=f't-{number}', run_id=f'r-{number}')
        except ValueError:
            return None
    with ThreadPoolExecutor(max_workers=4) as executor:
        assert sum(value is not None for value in executor.map(competitor, range(4))) == 1


def test_process_death_after_consumption_survives_reopen(case):
    case['manifests'][0]['declared_at'] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    case['manifests'][0]['expires_at'] = (datetime.now(UTC) + timedelta(minutes=10)).isoformat()
    case['workload']['declaration_shas'][0] = grants.declaration_sha(case['manifests'][0])
    case['clock'][0] = datetime.now(UTC)
    declare(case)
    payload = dict(context={key: str(value) if isinstance(value, Path) else value for key, value in case['context'].items()}, manifest=case['manifests'][0])
    script = "import importlib.util,json,os,sys; s=importlib.util.spec_from_file_location('g',sys.argv[1]); m=importlib.util.module_from_spec(s); s.loader.exec_module(m); p=json.loads(sys.stdin.read()); c=m.GrantConsumer(**p['context']); c.consume(p['manifest'],attempt_id='crash-attempt',task_id='crash-task',run_id='crash-run'); os._exit(23)"
    process = subprocess.run([sys.executable, '-c', script, str(MODULE)], input=json.dumps(payload), text=True, capture_output=True, timeout=15)
    assert process.returncode == 23, process.stderr
    assert slot(case) == ('grant-0', 1)
    with pytest.raises(ValueError, match='already_consumed'):
        consume(case)


@pytest.mark.parametrize('table', ['capture_grants', 'capture_consumptions', 'capture_resolutions', 'capture_workloads'])
def test_sql_facts_refuse_update_delete_and_replace(case, table):
    declare(case)
    consume(case)
    record(case)
    case['issuer'].resolve('grant-0', witness_path=witness(case))
    with connect(case['budget']) as connection:
        for operation in (f'DELETE FROM {table}', f'UPDATE {table} SET rowid=rowid', f'INSERT OR REPLACE INTO {table} SELECT * FROM {table}'):
            with pytest.raises(sqlite3.IntegrityError, match='immutable'):
                connection.execute(operation)


@pytest.mark.parametrize('kind', ['journal_stop', 'browser_hold', 'intervention', 'script_changed', 'authority_changed'])
def test_last_guard_changes_refuse_consumption(case, kind):
    declare(case)
    if kind == 'journal_stop':
        case['journal'].stop(grants.SCOPE, 'challenge')
    elif kind == 'browser_hold':
        (case['root'] / '.local/linkedin-acquisition-hold.json').write_text('{}')
    elif kind == 'intervention':
        with connect(case['budget']) as connection:
            connection.execute('CREATE TABLE capture_interventions(reason TEXT)')
            connection.execute("INSERT INTO capture_interventions VALUES ('challenge')")
    elif kind == 'script_changed':
        (case['root'] / 'scripts/company-bootstrap.cjs').write_text('changed')
    else:
        with case['journal'].connect() as connection:
            connection.execute("UPDATE owned_sink_authority SET authority_id='li-other'")
    with pytest.raises(ValueError):
        consume(case)


def test_policy_expiry_requires_preserved_classification_witness(case):
    declare(case)
    consume(case)
    record(case, 'policy_expired')
    with pytest.raises(ValueError, match='classification_unproved'):
        case['issuer'].resolve('grant-0', witness_path=witness(case, 'policy_expired'))
    case['issuer'].resolve('grant-0', witness_path=witness(case, 'policy_expired', 'preserved_classification'))


def test_source_lock_precedes_budget_lock_and_wrong_journal_refuses(case):
    with case['consumer']._locked():
        with connect(case['journal'].path, timeout=0) as other:
            with pytest.raises(sqlite3.OperationalError, match='locked'):
                other.execute('BEGIN IMMEDIATE')
        with connect(case['budget'], timeout=0) as other:
            with pytest.raises(sqlite3.OperationalError, match='locked'):
                other.execute('BEGIN IMMEDIATE')
    alternate = Journal(case['root'] / '.local/alternate.sqlite')
    alternate.generation(grants.SCOPE)
    with alternate.connect() as connection:
        connection.execute('CREATE TABLE owned_sink_authority(singleton INTEGER PRIMARY KEY,authority_id TEXT)')
        connection.execute("INSERT INTO owned_sink_authority VALUES (1,'li-authority-test')")
    changed = dict(case['context'], source_journal=alternate.path)
    with pytest.raises(ValueError, match='slot_binding'):
        grants.GrantIssuer(**changed).initialize()


def test_identical_grant_cannot_silently_accept_changed_other_workload_entry(case):
    declare(case)
    changed = deepcopy(case['workload'])
    changed['declaration_shas'][1] = 'a' * 64
    with pytest.raises(ValueError, match='immutable_workload_conflict'):
        case['issuer'].declare(case['manifests'][0], workload=changed)
    assert slot(case) == ('grant-0', 1)


def test_workload_cannot_skip_a_declared_position(case):
    third = deepcopy(case['manifests'][1])
    third.update(grant_id='grant-2', workload_position=2)
    case['workload']['declaration_shas'].append(grants.declaration_sha(third))
    case['workload']['native_read_budget'] = 36
    declare(case)
    consume(case)
    record(case)
    case['issuer'].resolve('grant-0', witness_path=witness(case))
    case['clock'][0] += timedelta(minutes=11)
    with pytest.raises(ValueError, match='workload_order'):
        case['issuer'].declare(third, workload=case['workload'])
    assert slot(case) == (None, 2)
