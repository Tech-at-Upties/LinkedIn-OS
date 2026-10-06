"""Historical grant migration with isolated files and the actual source Journal."""
from copy import deepcopy
from datetime import timedelta
from hashlib import sha256
import json
from pathlib import Path
import sqlite3

import pytest

from test_observation_grants import case, connect, grants, slot


def artifact(case, name, value):
    path = case['root'] / '.local' / name
    raw = json.dumps(value, sort_keys=True).encode()
    path.write_bytes(raw)
    return str(path), sha256(raw).hexdigest()


def legacy(case):
    """Match original manifest shapes, including the repair-history linkage."""
    old = {key: case['manifests'][0][key] for key in (
        'company_url', 'feed_publisher_id', 'feed_request_urn', 'project_root',
        'evidence_class', 'capture_budget', 'native_read_budget', 'probe_sha256')}
    old['nos_source_root'] = str(case['root'])
    old['expires_at'] = (case['moment'] - timedelta(minutes=30)).isoformat()
    current = dict(old, source_journal=str(case['journal'].path),
                   expires_at=(case['moment'] - timedelta(minutes=10)).isoformat())
    manifests = {}
    for name, value in [('old', old), ('current', current)]:
        key = sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()
        manifests[key] = artifact(case, name + '.json', value)[0]
    old_sha, current_sha = manifests
    consumed = (case['moment'] - timedelta(hours=1)).isoformat()
    with connect(case['budget']) as connection:
        connection.execute('CREATE TABLE capture_allowance(singleton INTEGER PRIMARY KEY, declaration_sha TEXT NOT NULL, attempt_id TEXT, consumed_at TEXT)')
        connection.execute('CREATE TABLE capture_allowance_history(declaration_sha TEXT PRIMARY KEY, attempt_id TEXT NOT NULL, consumed_at TEXT NOT NULL, successor_sha TEXT NOT NULL, declared_at TEXT NOT NULL, source_journal TEXT NOT NULL, witness_sha TEXT NOT NULL, reason TEXT NOT NULL)')
        connection.execute('CREATE TABLE capture_metadata(attempt_id TEXT PRIMARY KEY, captured_at TEXT NOT NULL, native_sha TEXT NOT NULL, projection_sha TEXT NOT NULL, native_reads INTEGER NOT NULL)')
        connection.execute('INSERT INTO capture_allowance VALUES(1,?,?,?)', (current_sha, 'current-attempt', consumed))
        connection.execute('INSERT INTO capture_allowance_history VALUES(?,?,?,?,?,?,?,?)',
            (old_sha, 'old-attempt', consumed, current_sha, consumed, str(case['journal'].path), 'c'*64, 'verified_local_browser_startup_repair'))
        connection.execute('INSERT INTO capture_metadata VALUES(?,?,?,?,?)',
            ('current-attempt', consumed, 'a'*64, 'b'*64, 2))
    with case['journal'].connect() as connection:
        for attempt, outcome in [('old-attempt', 'transport_failure'), ('current-attempt', 'policy_expired')]:
            connection.execute('INSERT INTO responses VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                (attempt, grants.SCOPE, 0, None, None, None, 'allowlisted_source_projection', None, 0, None, outcome))
    return manifests, old_sha, current_sha, consumed


def resolution(case, imported, declaration, attempt, outcome, consumed):
    evidence, evidence_sha = artifact(case, attempt + '-evidence.json', {'synthetic': True})
    common = dict(attempt_id=attempt, source_journal=str(case['journal'].path),
                  authority_id=case['issuer'].authority, source_generation=0,
                  observed_at=case['moment'].isoformat())
    successful = outcome == 'policy_expired'
    classification = dict(common, kind='legacy-classification/1', source_outcome=outcome,
        classified_outcome='partial' if successful else 'transport_failure',
        captured_at=consumed if successful else None,
        native_sha256='a'*64 if successful else None,
        projection_sha256='b'*64 if successful else None,
        evidence_path=evidence, evidence_sha256=evidence_sha)
    cessation = dict(common, kind='owned-process-cessation/1', profile=str(case['issuer'].profile),
        owned_process_count=0, evidence_path=evidence, evidence_sha256=evidence_sha)
    classification_path, classification_sha = artifact(case, attempt + '-classification.json', classification)
    cessation_path, cessation_sha = artifact(case, attempt + '-cessation.json', cessation)
    witness = dict(common, kind='legacy-observation-resolution/1', declaration_sha=declaration,
        snapshot_sha=imported['snapshot_sha'], source_outcome=outcome,
        classification_path=classification_path, classification_sha256=classification_sha,
        process_cessation_path=cessation_path, process_cessation_sha256=cessation_sha)
    return artifact(case, attempt + '-resolution.json', witness)[0]


def rows(case):
    with connect(case['budget']) as connection:
        return {table: connection.execute('SELECT * FROM ' + table).fetchall()
                for table in ('capture_allowance', 'capture_allowance_history', 'capture_metadata')}


def test_historical_hashes_import_close_and_initial_grant_preserve_legacy_rows(case):
    manifests, old_sha, current_sha, consumed = legacy(case)
    original = rows(case)
    imported = case['issuer'].import_legacy(manifests=manifests)
    assert imported['legacy_id'] == 'legacy-' + current_sha
    assert case['issuer'].import_legacy(manifests=manifests) == imported
    assert case['consumer'].read(imported['legacy_id']) is None
    with connect(case['budget']) as connection:
        assert {row[0] for row in connection.execute('SELECT declaration_sha FROM capture_legacy_facts')} == {old_sha, current_sha}
    for declaration, attempt, outcome in [(current_sha, 'current-attempt', 'policy_expired'),
                                           (old_sha, 'old-attempt', 'transport_failure')]:
        proof = resolution(case, imported, declaration, attempt, outcome, consumed)
        result = case['issuer'].resolve_legacy(declaration, witness_path=proof)
        assert case['issuer'].resolve_legacy(declaration, witness_path=proof) == result
        if declaration == current_sha:
            assert slot(case)[0] == imported['legacy_id']
    assert slot(case)[0] is None
    manifest = deepcopy(case['manifests'][0])
    manifest.update(kind='linkedin-observation-grant/2', page_mode='initial_document',
                    previous_grant_id=imported['legacy_id'], start=0, count=3)
    workload = dict(workload_id=manifest['workload_id'], declaration_shas=[grants.declaration_sha(manifest)], native_read_budget=12)
    case['issuer'].declare(manifest, workload=workload)
    assert slot(case)[0] == manifest['grant_id']
    assert rows(case) == original


def test_missing_historical_manifest_stays_unresolved_and_cannot_install(case):
    manifests, old_sha, current_sha, consumed = legacy(case)
    manifests.pop(old_sha)
    imported = case['issuer'].import_legacy(manifests=manifests)
    proof = resolution(case, imported, current_sha, 'current-attempt', 'policy_expired', consumed)
    case['issuer'].resolve_legacy(current_sha, witness_path=proof)
    assert not case['consumer'].migration_state()['resolved']
    assert slot(case)[0] == imported['legacy_id']
    with pytest.raises(ValueError, match='occupied'):
        case['issuer'].declare(case['manifests'][0], workload=case['workload'])


def test_reimport_changed_baseline_refuses_without_rewriting_import(case):
    manifests, _, _, _ = legacy(case)
    imported = case['issuer'].import_legacy(manifests=manifests)
    with connect(case['budget']) as connection:
        connection.execute('UPDATE capture_allowance_history SET witness_sha=?', ('d'*64,))
    with pytest.raises(ValueError, match='immutable_legacy_import_conflict'):
        case['issuer'].import_legacy(manifests=manifests)
    assert case['consumer'].migration_state()['snapshot_sha'] == imported['snapshot_sha']


@pytest.mark.parametrize('fault', ['history_journal', 'cessation_missing', 'classification_digest'])
def test_legacy_closure_requires_original_history_and_matching_proofs(case, fault):
    manifests, old_sha, current_sha, consumed = legacy(case)
    if fault == 'history_journal':
        with connect(case['budget']) as connection:
            connection.execute('UPDATE capture_allowance_history SET source_journal=?', ('alternate.sqlite',))
        with pytest.raises(ValueError, match='historical_source_binding_unproved'):
            case['issuer'].import_legacy(manifests=manifests)
        assert slot(case)[0] is None
        return
    imported = case['issuer'].import_legacy(manifests=manifests)
    proof = resolution(case, imported, current_sha, 'current-attempt', 'policy_expired', consumed)
    witness = json.loads(Path(proof).read_text(encoding='utf-8'))
    if fault == 'cessation_missing':
        # Replace the reference, preserving the actual cessation artifact.
        witness['process_cessation_path'] = str(case['root'] / '.local/missing.json')
    else:
        witness['classification_sha256'] = 'e'*64
    artifact(case, 'bad-resolution.json', witness)
    with pytest.raises(ValueError):
        case['issuer'].resolve_legacy(current_sha, witness_path=case['root'] / '.local/bad-resolution.json')
    assert slot(case)[0] == imported['legacy_id']
    with connect(case['budget']) as connection:
        assert connection.execute('SELECT count(*) FROM capture_legacy_resolutions').fetchone()[0] == 0


def test_legacy_import_interruption_rolls_back_slot_and_all_new_facts(case):
    manifests, _, _, _ = legacy(case)
    original = rows(case)
    with connect(case['budget']) as connection:
        connection.execute("CREATE TRIGGER fail_second_legacy_fact BEFORE INSERT ON capture_legacy_facts WHEN NEW.attempt_id='old-attempt' BEGIN SELECT RAISE(ABORT,'injected_import_failure'); END")
    with pytest.raises(sqlite3.IntegrityError, match='injected_import_failure'):
        case['issuer'].import_legacy(manifests=manifests)
    assert slot(case) == (None, 0)
    with connect(case['budget']) as connection:
        assert connection.execute('SELECT count(*) FROM capture_legacy_imports').fetchone()[0] == 0
        assert connection.execute('SELECT count(*) FROM capture_legacy_facts').fetchone()[0] == 0
    assert rows(case) == original


def test_v2_initial_grant_refuses_more_than_ten_minutes(case):
    manifest = deepcopy(case['manifests'][0])
    manifest.update(kind='linkedin-observation-grant/2', page_mode='initial_document', start=0, count=3,
                    expires_at=(case['moment'] + timedelta(seconds=601)).isoformat())
    workload = dict(workload_id=manifest['workload_id'], declaration_shas=[grants.declaration_sha(manifest)], native_read_budget=12)
    with pytest.raises(ValueError, match='deadline_invalid'):
        case['issuer'].declare(manifest, workload=workload)
