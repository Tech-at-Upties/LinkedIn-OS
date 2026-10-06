"""Explicit batch grant schema, parent consumption, and operator installation."""
from copy import deepcopy
from datetime import UTC, datetime, timedelta
import importlib.util
import json
from types import SimpleNamespace

import pytest

from nos_linkedin.acquisition import BatchPageResponse, RoutePermission, SourceBatchResponse, SourceResponse
from nos_linkedin.pages import CompanyPageRuns
from test_observation_grants import case, connect, grants, PROJECT


def manifest(case, budget=3):
    value = deepcopy(case['manifests'][0])
    value.update(kind='linkedin-observation-grant/3', page_mode='initial_document',
                 batch_page_budget=budget, start=0, count=3)
    return value


def workload(value):
    return dict(workload_id=value['workload_id'], declaration_shas=[grants.declaration_sha(value)],
                native_read_budget=12)


@pytest.mark.parametrize('budget', [2, 3])
def test_v3_explicit_batch_schema_does_not_change_historical_hash_inputs(case, budget):
    value = manifest(case, budget)
    expected = grants.declaration_sha(value)
    assert case['consumer']._manifest(value) == expected
    assert set(value) == grants.FIELDS | {'page_mode', 'batch_page_budget'}
    assert case['issuer'].declare(value, workload=workload(value)) == expected
    assert case['consumer'].read(value['grant_id']) == value
    original = case['manifests'][0]
    v2 = dict(original, kind='linkedin-observation-grant/2', page_mode='api_following')
    assert case['consumer']._manifest(original) == grants.declaration_sha(original)
    assert case['consumer']._manifest(v2) == grants.declaration_sha(v2)


@pytest.mark.parametrize('fault', ['budget_bool', 'budget_one', 'budget_four', 'api_mode',
                                  'deadline_601', 'implicit_v2', 'extra_scopes'])
def test_batch_declaration_requires_explicit_fixed_scope_and_bounds(case, fault):
    value = manifest(case)
    if fault.startswith('budget_'):
        value['batch_page_budget'] = {'budget_bool': True, 'budget_one': 1, 'budget_four': 4}[fault]
    elif fault == 'api_mode':
        value.update(page_mode='api_following', start=3, count=10)
    elif fault == 'deadline_601':
        value['expires_at'] = (case['moment'] + timedelta(seconds=601)).isoformat()
    elif fault == 'implicit_v2':
        value['kind'] = 'linkedin-observation-grant/2'
    else:
        value['page_scopes'] = [[0, 3], [3, 10]]
    with pytest.raises(ValueError):
        case['issuer'].declare(value, workload=workload(value))
    with connect(case['budget']) as connection:
        assert connection.execute('SELECT count(*) FROM capture_grants').fetchone() == (0,)


def parent(case, *, declared=3, run_budget=2, requested=21, following=10):
    value = manifest(case, declared)
    case['issuer'].declare(value, workload=workload(value))
    runs = CompanyPageRuns(case['journal'])
    runs.create(run_id='batch-task', feed_publisher_id=value['feed_publisher_id'],
        evidence_class=value['evidence_class'], first_start=0, count=3, page_budget=run_budget,
        requested_count=requested, following_count=following)
    lease = runs.claim('batch-task', owner='synthetic-owner', now=case['moment'])
    return value, runs, lease


def capture(case, value, lease, callback):
    return case['journal'].capture(scope=grants.SCOPE, generation=0, attempt_id=lease.attempt_id,
        feed_publisher_id=value['feed_publisher_id'], now=case['moment'],
        permission=RoutePermission(True, True, case['moment'] + timedelta(minutes=10), value['evidence_class']),
        send_guarded=callback)


def test_parent_consumption_commits_once_with_actual_job_budget_below_cap(case):
    value, runs, lease = parent(case, declared=3, run_budget=2, requested=21)
    members = runs.reserve_batch(lease, batch_page_budget=2, now=case['moment'])
    assert [(member.start, member.count) for member in members] == [(0, 3), (3, 10)]
    launches = []
    def dispatch(source):
        result = case['consumer'].consume_in_dispatch(value, source, attempt_id=lease.attempt_id,
                    task_id=lease.run_id, run_id=lease.run_id)
        assert result['attempt_id'] == lease.attempt_id
        assert source.in_transaction
        assert source.execute('SELECT count(*) FROM responses').fetchone()[0] == 1
        assert source.execute('SELECT outcome FROM responses').fetchone()[0] == 'dispatch_unknown'
        with connect(case['budget']) as observer:
            assert observer.execute('SELECT attempt_id FROM capture_consumptions').fetchone()[0] == lease.attempt_id
        # Reserved following identities have not been admitted as unknown source reads.
        assert source.execute('SELECT 1 FROM responses WHERE attempt_id=?', (members[1].attempt_id,)).fetchone() is None
        with pytest.raises(ValueError, match='already_consumed'):
            case['consumer'].consume_in_dispatch(value, source, attempt_id=lease.attempt_id,
                        task_id=lease.run_id, run_id=lease.run_id)
        launches.append(1)
        return SourceBatchResponse(tuple(BatchPageResponse(member,
            SourceResponse(200, 'application/json', b'{}', case['moment'])) for member in members))
    capture(case, value, lease, dispatch)
    with case['journal'].connect() as connection:
        assert connection.execute('SELECT count(*) FROM responses WHERE outcome=\'captured\'').fetchone()[0] == 2
    capture(case, value, lease, lambda _: pytest.fail('parent replay dispatched again'))
    assert launches == [1]
    with connect(case['budget']) as connection:
        assert connection.execute('SELECT count(*) FROM capture_consumptions').fetchone() == (1,)


@pytest.mark.parametrize('run_budget,declared,following', [(1, 3, 10), (3, 2, 10), (2, 3, 9)])
def test_parent_run_outside_declared_batch_refuses_without_consumption(case, run_budget, declared, following):
    value, _, lease = parent(case, declared=declared, run_budget=run_budget, following=following)
    def dispatch(source):
        with pytest.raises(ValueError, match='batch_context_mismatch'):
            case['consumer'].consume_in_dispatch(value, source, attempt_id=lease.attempt_id,
                        task_id=lease.run_id, run_id=lease.run_id)
        raise ValueError('refused_before_launch')
    assert capture(case, value, lease, dispatch) is False
    assert case['journal'].read(lease.attempt_id, case['moment'])['outcome'] == 'transport_failure'
    with connect(case['budget']) as connection:
        assert connection.execute('SELECT count(*) FROM capture_consumptions').fetchone() == (0,)


@pytest.mark.parametrize('fault', ['missing', 'wrong_following_scope'])
def test_v3_parent_requires_exact_reserved_scopes_before_consumption(case, fault):
    value, runs, lease = parent(case)
    if fault != 'missing':
        runs.reserve_batch(lease, batch_page_budget=2, now=case['moment'])
        with case['journal'].connect() as connection:
            connection.execute('UPDATE response_batch_pages SET start=4 WHERE ordinal=1')
    refused = []
    def dispatch(source):
        with pytest.raises(ValueError, match='batch_reservation_mismatch'):
            case['consumer'].consume_in_dispatch(value, source, attempt_id=lease.attempt_id,
                        task_id=lease.run_id, run_id=lease.run_id)
        refused.append(1)
        raise ValueError('refused_before_launch')
    capture(case, value, lease, dispatch)
    assert refused == [1]
    with connect(case['budget']) as connection:
        assert connection.execute('SELECT count(*) FROM capture_consumptions').fetchone() == (0,)


def operator(case):
    spec = importlib.util.spec_from_file_location('batch_grant_operator', PROJECT / 'docs/experiments/manage_observation_grants.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    context = dict(kind='grant-controller-context/1', project_root=str(case['root']),
        profile=str(case['issuer'].profile), budget=str(case['budget']), source_journal=str(case['journal'].path),
        authority_id=case['issuer'].authority, source_generation=0, nos_source_root=str(case['root']))
    path = case['root'] / '.local/controller-context.json'
    path.write_text(json.dumps(context))
    return module, path


def test_operator_explicitly_installs_v3_without_consumption(case):
    module, context = operator(case)
    value = manifest(case)
    now = datetime.now(UTC)
    value.update(declared_at=(now - timedelta(seconds=1)).isoformat(),
                 expires_at=(now + timedelta(minutes=8)).isoformat())
    declaration, plan, output = [case['root'] / '.local' / name for name in ('v3.json', 'plan.json', 'installed.json')]
    declaration.write_text(json.dumps(value))
    plan.write_text(json.dumps(workload(value)))
    args = SimpleNamespace(context=context, action='declare-install', manifest=declaration,
                           workload=plan, installed_reference=output)
    receipt = module.execute(args, api=grants)
    reference = json.loads(output.read_bytes())
    assert reference['declaration_sha256'] == grants.declaration_sha(value) == receipt['declaration_sha256']
    assert 'batch_page_budget' not in reference
    with connect(case['budget']) as connection:
        assert connection.execute('SELECT count(*) FROM capture_consumptions').fetchone() == (0,)
        assert connection.execute('SELECT count(*) FROM capture_grants').fetchone() == (1,)
