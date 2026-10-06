"""Explicit operator actions on isolated synthetic stores, without source access."""
from contextlib import closing
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from test_grant_runtime import installed
from test_initial_company_page import PROJECT, project_manifest
from xingestion.linkedin import grants


@pytest.fixture
def controller(installed):
    spec = importlib.util.spec_from_file_location('synthetic_grant_controller',
        PROJECT / 'docs/experiments/manage_observation_grants.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    case = installed
    value = dict(kind='grant-controller-context/1', project_root=str(case.root),
        profile=str(case.issuer.profile), budget=str(case.budget),
        source_journal=str(case.journal.path), authority_id=case.issuer.authority,
        source_generation=0, nos_source_root=case.live['nos_source_root'])
    path = case.root / '.local/controller-context.json'
    path.write_text(json.dumps(value))
    return SimpleNamespace(module=module, case=case, path=path, value=value)


def arguments(controller, **values):
    return SimpleNamespace(context=controller.path, **values)


def test_operator_installs_reference_idempotently_without_consumption(controller):
    case, module = controller.case, controller.module
    manifest = case.root / '.local/declaration.json'
    workload = case.root / '.local/workload.json'
    output = case.root / '.local/operator-installed.json'
    manifest.write_text(json.dumps(case.manifest))
    workload.write_text(json.dumps(dict(workload_id=case.manifest['workload_id'],
        declaration_shas=[grants.declaration_sha(case.manifest)], native_read_budget=12)))
    args = arguments(controller, action='declare-install', manifest=manifest,
        workload=workload, installed_reference=output)
    first = module.execute(args, api=grants)
    original = output.read_bytes()
    assert module.execute(args, api=grants) == first
    assert output.read_bytes() == original
    assert json.loads(original) == case.reference
    with closing(sqlite3.connect(case.budget)) as connection:
        assert connection.execute('SELECT COUNT(*) FROM capture_consumptions').fetchone() == (0,)
        assert connection.execute('SELECT COUNT(*) FROM capture_grants').fetchone() == (1,)


@pytest.mark.parametrize('fault', ['context_generation', 'manifest_schema', 'reference_conflict'])
def test_operator_rejects_before_initialization_or_store_mutation(controller, monkeypatch, fault):
    case, module = controller.case, controller.module
    manifest, workload = case.root / '.local/declaration.json', case.root / '.local/workload.json'
    output = case.root / '.local/output.json'
    declaration = dict(case.manifest)
    if fault == 'context_generation':
        controller.value['source_generation'] = True
        controller.path.write_text(json.dumps(controller.value))
    elif fault == 'manifest_schema':
        declaration['body'] = 'synthetic-private-unrequested-source'
    else:
        output.write_text('{}')
    manifest.write_text(json.dumps(declaration))
    workload.write_text(json.dumps(dict(workload_id=case.manifest['workload_id'],
        declaration_shas=[grants.declaration_sha(case.manifest)], native_read_budget=12)))
    before = case.budget.read_bytes(), case.journal.path.read_bytes()
    initialized = []
    monkeypatch.setattr(grants.GrantIssuer, 'initialize', lambda self: initialized.append(True))
    with pytest.raises(ValueError):
        module.execute(arguments(controller, action='declare-install', manifest=manifest,
            workload=workload, installed_reference=output), api=grants)
    assert initialized == []
    assert (case.budget.read_bytes(), case.journal.path.read_bytes()) == before


def test_operator_inspect_returns_selected_metadata_and_does_not_change_stores(controller):
    case, module = controller.case, controller.module
    before = case.budget.read_bytes(), case.journal.path.read_bytes()
    result = module.execute(arguments(controller, action='inspect', grant_id=case.manifest['grant_id']), api=grants)
    assert result['active_grant_id'] == case.manifest['grant_id']
    assert result['consumption'] is result['resolution'] is None
    assert set(result['grant']) == {'grant_id', 'workload_id', 'workload_position', 'expires_at', 'declaration_sha256'}
    assert (case.budget.read_bytes(), case.journal.path.read_bytes()) == before


def test_operator_main_rejects_duplicate_metadata_without_echoing_it(controller, monkeypatch, capsys):
    marker = 'synthetic-private-context-marker'
    controller.path.write_text('{"kind":"' + marker + '","kind":"duplicate"}')
    monkeypatch.setattr(controller.module, 'grant_api', lambda: grants)
    assert controller.module.main(['--context', str(controller.path), 'inspect']) == 1
    output = capsys.readouterr().out
    assert marker not in output
    assert json.loads(output)['error'] == 'grant_controller_rejected'
