"""Offline observer guards. Never invoke a real browser/profile or native source."""
from copy import deepcopy
from contextlib import closing
from datetime import UTC, datetime, timedelta
from hashlib import sha256
import importlib.util
import html
import json
from pathlib import Path
import sqlite3
import subprocess
import tempfile
from types import SimpleNamespace

import pytest

from nos_linkedin.acquisition import Journal

PROJECT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('bootstrap_observer', PROJECT / 'docs/experiments/observe_company_bootstrap.py')
observer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(observer)
METADATA_KIND = 'company-bootstrap-metadata/1'
STRUCTURE_KIND = 'company-bootstrap-structure/1'
BODY_KIND = 'company-bootstrap-body/1'
RESOLVER_KIND = 'company-bootstrap-resolver/1'
PURE_NODE_RUN = subprocess.run
PURE_NODE_BINARY = observer.shutil.which('node')


def actual_node_output(javascript, argument, *module_paths):
    """Run only pure exported helpers on synthetic input, never their entrypoint."""
    node = PURE_NODE_BINARY
    assert node, 'Node is required for the controlled producer/validator contract'
    process = PURE_NODE_RUN([node, '-e', javascript, *(str(path) for path in module_paths)],
                             input=json.dumps(argument).encode(), stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, timeout=5, check=True, cwd=PROJECT)
    return observer.decode_json(process.stdout)


def structure_capture():
    value = capture()
    value['bootstrap_observation']['candidates'] = [{
        'source': 'initial_document_json', 'body_sha256': 'a' * 64,
        'structure': {'status': 'observed', 'root_keys': ['data', 'included'],
                      'nodes': [{'path': '$.data.*elements', 'kind': 'array', 'keys': [],
                                 'type_name': None, 'reference_fields': [
                                     {'key': '*elements', 'ids': ['urn:li:activity:1'],
                                      'state': 'value', 'count': 1}]}]}}]
    return value


def normalized_embedded_body():
    return {'data': {'data': {observer.RECIPE: {
        '$type': 'com.linkedin.restli.common.CollectionResponse',
        '*elements': ['urn:li:fsd_update:3', 'urn:li:fsd_update:1', 'urn:li:fsd_update:2'],
        'paging': {'start': 0, 'count': 3, 'total': 20},
        'text': 'synthetic-authored-publication-MUST-NOT-PERSIST'}}},
        'included': [{'entityUrn': 'urn:li:fsd_update:999',
                      'text': 'synthetic-unrelated-publication-MUST-NOT-PERSIST'}]}


def body_capture():
    value = capture()
    original = value['bootstrap_observation']['candidates'][0]
    collection = {key: original[key] for key in ('status', 'recipe', 'paging', 'root_ids')}
    value['bootstrap_observation']['candidates'] = [{
        'source': 'initial_document_body_json', 'body_sha256': 'a' * 64,
        'outer_body_sha256': 'b' * 64,
        'envelope': {'method': 'GET', 'status': 200, 'request_family': 'feed',
                     'target_binding': 'matched', 'body_encoding': 'json_string'},
        'collection': collection,
        'structure': structure_capture()['bootstrap_observation']['candidates'][0]['structure']}]
    return value


def resolver_capture():
    value = body_capture()
    value['bootstrap_observation']['candidates'][0]['envelope']['body_length'] = 20
    return value


def synthetic_response_envelope(body, serialized=True):
    return {'request': '/voyager/api/graphql?queryId=voyagerFeedDashOrganizationalPageUpdates.synthetic'
                       '&variables=(organizationalPageUrn:urn%3Ali%3Afsd_organizationalPage%3A1337,start:3,count:10)',
            'method': 'GET', 'status': 200,
            'headers': {'csrf-token': 'synthetic-csrf-MUST-NOT-PERSIST',
                        'cookie': 'synthetic-cookie-MUST-NOT-PERSIST'},
            'body': json.dumps(body) if serialized else body}


def actual_embedded_body_capture(envelope):
    javascript = """const h=require(process.argv[1]),p=require(process.argv[2]);
let input='';process.stdin.setEncoding('utf8');process.stdin.on('data',chunk=>input+=chunk);
process.stdin.on('end',()=>{const envelope=JSON.parse(input);let stopped=null;
const html='<script type="application/json">'+JSON.stringify(envelope)+'</script>';
const observation=h.inspectEmbeddedBootstrap(html,body=>{const s=p.semanticSignal(body);
if(s&&(!stopped||s.security))stopped={reason:s.reason,security_hold:s.security};},'body');
console.log(JSON.stringify(p.bootstrapReceipt({session_verified:!stopped,stopped,
native_reads_admitted:2,navigation_status:200},observation)));});"""
    return actual_node_output(javascript, envelope, PROJECT / 'scripts/company-bootstrap.cjs',
                              PROJECT / 'scripts/probe-linkedin-company.cjs')


def actual_resolver_capture(document):
    javascript = """const h=require(process.argv[1]),p=require(process.argv[2]);
let input='';process.stdin.setEncoding('utf8');process.stdin.on('data',chunk=>input+=chunk);
process.stdin.on('end',()=>{const document=JSON.parse(input);let stopped=null;
const observation=h.inspectEmbeddedBootstrap(document,body=>{const s=p.semanticSignal(body);
if(s&&(!stopped||s.security))stopped={reason:s.reason,security_hold:s.security};},'resolver');
console.log(JSON.stringify(p.bootstrapReceipt({session_verified:!stopped,stopped,
native_reads_admitted:2,navigation_status:200},observation)));});"""
    return actual_node_output(javascript, document, PROJECT / 'scripts/company-bootstrap.cjs',
                              PROJECT / 'scripts/probe-linkedin-company.cjs')


def resolver_document(envelope, referenced_blocks=''):
    return '<script type="application/json">' + json.dumps(envelope) + '</script>' + referenced_blocks


def capture():
    return {'session_verified': True, 'stopped': None, 'failure_kind': None,
            'native_reads_admitted': 2, 'navigation_status': 200, 'account_writes_blocked': True,
            'bootstrap_observation': {'status': 'observed', 'candidates': [{
                'source': 'initial_document_json', 'body_sha256': 'a' * 64,
                'status': 'observed_start_zero', 'recipe': observer.RECIPE,
                'paging': {'start': 0, 'count': 3, 'total': 30},
                'root_ids': ['urn:li:activity:1', 'urn:li:activity:2', 'urn:li:activity:3']}]}}


@pytest.fixture
def setup(monkeypatch):
    local = (PROJECT / '.local').resolve()
    assert local.is_relative_to(PROJECT.resolve())
    local.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='wr047-offline-', dir=local) as temporary:
        root = Path(temporary) / 'LinkedIn-OS'
        root.mkdir()
        monkeypatch.setattr(observer, 'ROOT', root)
        instant = datetime(2026, 10, 3, 12, tzinfo=UTC)
        monkeypatch.setattr(observer, 'now', lambda: instant)
        monkeypatch.setattr(observer.shutil, 'which', lambda _: 'synthetic-node-never-executed')
        (root / '.local/linkedin-test-browser').mkdir(parents=True)
        (root / 'docs/results').mkdir(parents=True)
        (root / 'scripts').mkdir()
        script = root / 'scripts/probe-linkedin-company.cjs'
        helper = root / 'scripts/company-bootstrap.cjs'
        script.write_text('synthetic probe, not executable')
        helper.write_text('synthetic helper, not executable')
        installed = root.parent / 'NOS-V1'
        (installed / 'M3/node_modules/playwright').mkdir(parents=True)
        allowance_path = root / '.local/linkedin-live-capture.sqlite'
        with closing(sqlite3.connect(allowance_path)) as connection, connection:
            connection.execute('CREATE TABLE capture_allowance(singleton INTEGER, declaration_sha TEXT, attempt_id TEXT, consumed_at TEXT)')
            connection.execute("INSERT INTO capture_allowance VALUES (1,'old-digest','old-attempt','old-time')")
            connection.execute('CREATE TABLE capture_interventions(reason TEXT)')
        journal_path = root / '.local/source.sqlite'
        journal = Journal(journal_path)
        generation = journal.generation(observer.SCOPE)
        declaration = {'kind': 'company-bootstrap-metadata/1', 'target': observer.TARGET,
                       'native_read_budget': 12, 'continuation': False, 'boundary': False,
                       'expires_at': (instant + timedelta(minutes=10)).isoformat(),
                       'probe_sha256': sha256(script.read_bytes()).hexdigest(),
                       'helper_sha256': sha256(helper.read_bytes()).hexdigest(),
                       'consumed_runtime_allowance': ['old-digest', 'old-attempt', 'old-time'],
                       'source_journal': str(journal_path), 'source_generation': generation,
                       'nos_source_root': str(installed)}
        declaration_path = root / '.local/declaration.json'
        calls = []

        def child(command, **kwargs):
            calls.append((command, kwargs))
            kind = declaration['kind']
            marker, output = {
                METADATA_KIND: ('linkedin-company-bootstrap.consumed', capture),
                STRUCTURE_KIND: ('linkedin-company-bootstrap-structure.consumed', structure_capture),
                BODY_KIND: ('linkedin-company-bootstrap-body.consumed', body_capture),
                RESOLVER_KIND: ('linkedin-company-bootstrap-resolver.consumed', resolver_capture),
            }[kind]
            assert (root / '.local' / marker).exists()
            return SimpleNamespace(returncode=0, stdout=json.dumps(output()).encode())

        monkeypatch.setattr(observer.subprocess, 'run', child)
        state = SimpleNamespace(root=root, instant=instant, declaration=declaration,
                                path=declaration_path, journal=journal, calls=calls,
                                allowance_path=allowance_path, child=child)
        state.save = lambda: declaration_path.write_text(json.dumps(declaration), encoding='utf-8')
        state.save()
        yield state


@pytest.fixture(params=[METADATA_KIND, STRUCTURE_KIND])
def guarded_setup(setup, request):
    """Reuse only authority-critical cases across the two explicit declarations."""
    setup.declaration['kind'] = request.param
    setup.save()
    return setup


def test_installed_sibling_and_one_shot_consumption_before_dispatch(setup, monkeypatch):
    before = setup.allowance_path.read_bytes()
    syncs = []
    real_fsync = observer.os.fsync
    monkeypatch.setattr(observer.os, 'fsync', lambda fd: (syncs.append(fd), real_fsync(fd))[-1])
    path, result = observer.observe(setup.path)
    assert result['runtime_allowance_unchanged'] is True
    assert setup.allowance_path.read_bytes() == before
    assert result['bootstrap_observation']['candidates'][0]['source'] == 'initial_document_json'
    assert len(syncs) == 2
    command, kwargs = setup.calls[0]
    assert '--bootstrap' in command and '--runtime-capture' in command
    assert '--bootstrap-deadline=' + setup.declaration['expires_at'] in command
    assert kwargs['timeout'] == 85
    assert kwargs['env']['NOS_SOURCE_ROOT'] == setup.declaration['nos_source_root']
    assert path == setup.root / 'docs/results/company-bootstrap-observation.json'
    marker = json.loads((setup.root / '.local/linkedin-company-bootstrap.consumed').read_text())
    assert marker['source_scope'] == observer.SCOPE and marker['source_generation'] == 0
    assert marker['declaration_sha256'] == sha256(setup.path.read_bytes()).hexdigest()
    copied = setup.root / '.local/other.json'
    copied.write_bytes(setup.path.read_bytes())
    # Prove the profile-wide marker fences a copied declaration even if its
    # generated receipt is lost. This deletes only this fixture's owned output.
    assert path.is_relative_to(setup.root)
    path.unlink()
    with pytest.raises(ValueError, match='already_consumed'):
        observer.observe(copied)
    assert len(setup.calls) == 1


@pytest.mark.parametrize('fault', ['extra_field', 'duplicate_key', 'nonfinite'])
def test_declaration_json_refuses_ambiguous_or_extra_authority(setup, fault):
    if fault == 'extra_field':
        setup.declaration['allowance_refill'] = True
        setup.save()
    else:
        content = setup.path.read_text()
        suffix = ', "native_read_budget":12}' if fault == 'duplicate_key' else ', "injected":NaN}'
        setup.path.write_text(content[:-1] + suffix)
    with pytest.raises(ValueError):
        observer.observe(setup.path)
    assert not setup.calls
    assert not (setup.root / '.local/linkedin-company-bootstrap.consumed').exists()


def test_marker_is_fsynced_before_child_invocation(setup, monkeypatch):
    synced = []
    real_fsync = observer.os.fsync

    def fsync(fd):
        real_fsync(fd)
        synced.append(fd)

    def child(command, **kwargs):
        assert len(synced) == 1
        return setup.child(command, **kwargs)

    monkeypatch.setattr(observer.os, 'fsync', fsync)
    monkeypatch.setattr(observer.subprocess, 'run', child)
    observer.observe(setup.path)
    assert len(setup.calls) == 1
    assert len(synced) == 2


def test_lost_marker_existing_receipt_cannot_relaunch(setup):
    receipt = setup.root / 'docs/results/company-bootstrap-observation.json'
    receipt.write_text('{}')
    with pytest.raises(ValueError, match='already_consumed'):
        observer.observe(setup.path)
    assert not setup.calls
    assert not (setup.root / '.local/linkedin-company-bootstrap.consumed').exists()


@pytest.mark.parametrize('outcome', ['captured', 'classification_expired', 'dispatch_unknown'])
def test_actual_unresolved_journal_vocabulary_blocks_dispatch(guarded_setup, outcome):
    setup = guarded_setup
    with setup.journal.connect() as connection:
        connection.execute('INSERT INTO responses VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                           ('old-response', observer.SCOPE, 0, 200, 'application/json', None,
                            'allowlisted_source_projection', None, 0, None, outcome))
    with pytest.raises(ValueError, match='unresolved_source_attempt'):
        observer.observe(setup.path)
    assert not setup.calls
    assert not (setup.root / '.local/linkedin-company-bootstrap.consumed').exists()
    assert not (setup.root / '.local/linkedin-company-bootstrap-structure.consumed').exists()


@pytest.mark.parametrize('field,value', [
    ('target', 'https://www.linkedin.com/company/other/posts/'), ('native_read_budget', True),
    ('native_read_budget', 13), ('boundary', True), ('continuation', True),
    ('source_generation', True), ('probe_sha256', 'changed'), ('helper_sha256', 'changed'),
    ('consumed_runtime_allowance', ['old-digest', None, None]),
    ('expires_at', '2026-10-03T12:00:00+00:00'), ('expires_at', '2026-10-03T12:10:01+00:00'),
    ('expires_at', '2026-10-03T12:01:00')])
def test_scope_deadline_and_pin_preflights_do_not_consume(setup, field, value):
    setup.declaration[field] = value
    setup.save()
    with pytest.raises((ValueError, TypeError)):
        observer.observe(setup.path)
    assert not setup.calls
    assert not (setup.root / '.local/linkedin-company-bootstrap.consumed').exists()


@pytest.mark.parametrize('kind', ['journal_hold', 'browser_hold', 'intervention', 'missing_profile', 'changed_generation'])
def test_existing_security_and_generation_guards(guarded_setup, kind):
    setup = guarded_setup
    if kind == 'journal_hold':
        setup.journal.stop(observer.SCOPE, 'challenge')
    elif kind == 'browser_hold':
        (setup.root / '.local/linkedin-acquisition-hold.json').write_text('{}')
    elif kind == 'intervention':
        with closing(sqlite3.connect(setup.allowance_path)) as connection, connection:
            connection.execute("INSERT INTO capture_interventions VALUES ('challenge')")
    elif kind == 'missing_profile':
        (setup.root / '.local/linkedin-test-browser').rmdir()
    else:
        setup.declaration['source_generation'] = 1
        setup.save()
    with pytest.raises(ValueError):
        observer.observe(setup.path)
    assert not setup.calls


def test_paths_never_accept_arbitrary_source_or_output(setup):
    with pytest.raises(ValueError):
        observer.owned_path(setup.root / 'docs/results/foreign.json')
    arbitrary = setup.root / '.local/arbitrary'
    (arbitrary / 'M3/node_modules/playwright').mkdir(parents=True)
    with pytest.raises(ValueError, match='not_allowlisted'):
        observer.source_root(arbitrary)
    installed = setup.root / '.local/nos-integration'
    (installed / 'M3/node_modules/playwright').mkdir(parents=True)
    assert observer.source_root(installed) == installed
    with pytest.raises(ValueError, match='not_allowlisted'):
        observer.source_root(Path(setup.declaration['nos_source_root']) / 'M3')


def test_missing_journal_is_never_created(setup):
    missing = setup.root / '.local/missing.sqlite'
    setup.declaration['source_journal'] = str(missing)
    setup.save()
    with pytest.raises(ValueError, match='source_journal_or_node'):
        observer.observe(setup.path)
    assert not missing.exists() and not setup.calls


def test_remaining_deadline_caps_child_and_expired_output_is_failure(guarded_setup, monkeypatch):
    setup = guarded_setup
    setup.declaration['expires_at'] = (setup.instant + timedelta(seconds=7)).isoformat()
    setup.save()

    def child(command, **kwargs):
        assert kwargs['timeout'] == 7
        monkeypatch.setattr(observer, 'now', lambda: setup.instant + timedelta(seconds=8))
        return setup.child(command, **kwargs)

    monkeypatch.setattr(observer.subprocess, 'run', child)
    _, result = observer.observe(setup.path)
    assert result['failure_kind'] == 'DeadlineElapsed'
    assert result['post_capture_deadline_elapsed'] is True


def test_deadline_rechecked_after_preflight_before_consumption(setup, monkeypatch):
    validate = observer.validate_declaration

    def expired(path):
        result = validate(path)
        monkeypatch.setattr(observer, 'now', lambda: setup.instant + timedelta(minutes=11))
        return result

    monkeypatch.setattr(observer, 'validate_declaration', expired)
    with pytest.raises(ValueError, match='deadline_elapsed'):
        observer.observe(setup.path)
    assert not setup.calls
    assert not (setup.root / '.local/linkedin-company-bootstrap.consumed').exists()


@pytest.mark.parametrize('fault', ['timeout', 'death', 'oversize', 'duplicate', 'nonfinite', 'body_copy'])
def test_failed_child_consumes_once_and_never_copies_unsafe_bytes(setup, monkeypatch, fault):
    secret = 'authored-publication-SHOULD-NOT-BE-RETAINED'

    def child(command, **kwargs):
        setup.calls.append((command, kwargs))
        if fault == 'timeout':
            raise subprocess.TimeoutExpired(command, kwargs['timeout'], output=secret.encode())
        if fault == 'death':
            raise RuntimeError(secret)
        if fault == 'oversize':
            data = secret.encode() * 100000
        elif fault == 'duplicate':
            data = b'{"session_verified":true,"session_verified":false}'
        elif fault == 'nonfinite':
            data = b'{"native_reads_admitted":NaN}'
        else:
            value = capture()
            value['bootstrap_observation']['candidates'][0]['text'] = secret
            data = json.dumps(value).encode()
        return SimpleNamespace(returncode=1, stdout=data)

    monkeypatch.setattr(observer.subprocess, 'run', child)
    receipt, result = observer.observe(setup.path)
    assert result['failure_kind'] in {'BootstrapGuardFailure', 'TimeoutExpired'}
    assert secret not in receipt.read_text()
    assert 'bootstrap_observation' not in result
    assert result['runtime_allowance_unchanged'] is True
    with pytest.raises(ValueError, match='already_consumed'):
        observer.observe(setup.path)
    assert len(setup.calls) == 1


@pytest.mark.parametrize('reason,security', [('challenge', True), ('restricted', True),
                                          ('authentication_required', False), ('operator_stop', True)])
def test_valid_stops_commit_source_hold_without_history_or_allowance_reset(setup, monkeypatch, reason, security):
    allowance_before = setup.allowance_path.read_bytes()
    old = ('old-response', observer.SCOPE, 0, 200, 'application/json', None,
           'allowlisted_source_projection', 'b' * 64, 0, None, 'policy_expired')
    with setup.journal.connect() as connection:
        connection.execute('INSERT INTO responses VALUES (?,?,?,?,?,?,?,?,?,?,?)', old)
    value = capture()
    value['session_verified'] = False
    value['stopped'] = {'reason': reason, 'security_hold': security}
    monkeypatch.setattr(observer.subprocess, 'run', lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=json.dumps(value).encode()))
    _, result = observer.observe(setup.path)
    with setup.journal.connect() as connection:
        row = connection.execute('SELECT generation,stopped,reason FROM route_state').fetchone()
        assert tuple(row) == (1, 1, reason)
        assert tuple(connection.execute('SELECT * FROM responses').fetchone()) == old
    assert result['failure_kind'] == 'ObservationStopped'
    assert result['runtime_allowance_unchanged'] is True
    assert setup.allowance_path.read_bytes() == allowance_before


@pytest.mark.parametrize('outcome', ['policy_expired', 'transport_failure', 'bounded', 'partial'])
def test_resolved_history_is_preserved_and_not_mistaken_for_pending_capture(setup, outcome):
    old = ('old-response', observer.SCOPE, 0, 200, 'application/json', None,
           'allowlisted_source_projection', 'c' * 64, 0, None, outcome)
    with setup.journal.connect() as connection:
        connection.execute('INSERT INTO responses VALUES (?,?,?,?,?,?,?,?,?,?,?)', old)
    observer.observe(setup.path)
    assert len(setup.calls) == 1
    with setup.journal.connect() as connection:
        assert tuple(connection.execute('SELECT * FROM responses').fetchone()) == old


@pytest.mark.parametrize('mutate', [
    lambda v: v.update(native_reads_admitted=True), lambda v: v.update(native_reads_admitted=13),
    lambda v: v.update(account_writes_blocked=False), lambda v: v.update(navigation_status=True),
    lambda v: v.update(failure_kind='user-authored-text'),
    lambda v: v['bootstrap_observation'].update(text='publication'),
    lambda v: v['bootstrap_observation']['candidates'][0].update(source='dom'),
    lambda v: v['bootstrap_observation']['candidates'][0].update(root_ids=['publication text']),
    lambda v: v['bootstrap_observation']['candidates'][0].update(root_ids=['urn:li:activity:1'] * 2),
    lambda v: v['bootstrap_observation']['candidates'][0]['paging'].update(start=True),
    lambda v: v['bootstrap_observation']['candidates'][0]['paging'].update(start=3),
    lambda v: v['bootstrap_observation']['candidates'][0]['paging'].update(total=2**53),
    lambda v: v['bootstrap_observation']['candidates'][0].update(status='absent'),
    lambda v: v.update(stopped={'reason': 'challenge', 'security_hold': False}),
])
def test_strict_metadata_schema(mutate):
    value = deepcopy(capture())
    mutate(value)
    with pytest.raises((ValueError, TypeError)):
        observer.validate_capture(value)


def test_child_environment_and_physical_journal_lock(setup, monkeypatch):
    monkeypatch.setenv('WR042_SECRET', 'do-not-forward')
    monkeypatch.setenv('PROGRAMFILES', 'synthetic-install-path')

    def child(command, **kwargs):
        assert 'WR042_SECRET' not in kwargs['env']
        assert kwargs['env']['PROGRAMFILES'] == 'synthetic-install-path'
        with closing(sqlite3.connect(setup.journal.path, timeout=0)) as competing, competing:
            with pytest.raises(sqlite3.OperationalError, match='locked'):
                competing.execute('BEGIN IMMEDIATE')
        return setup.child(command, **kwargs)

    monkeypatch.setattr(observer.subprocess, 'run', child)
    observer.observe(setup.path)


def test_allowance_mutation_is_reported_without_repair(setup, monkeypatch):
    def child(command, **kwargs):
        with closing(sqlite3.connect(setup.allowance_path)) as connection, connection:
            connection.execute("UPDATE capture_allowance SET attempt_id='different' WHERE singleton=1")
        return setup.child(command, **kwargs)
    monkeypatch.setattr(observer.subprocess, 'run', child)
    _, result = observer.observe(setup.path)
    assert result['runtime_allowance_unchanged'] is False
    assert result['failure_kind'] == 'RuntimeAllowanceChanged'
    assert observer.allowance_snapshot()[1] == 'different'


def test_structure_mode_has_distinct_authority_and_preserves_consumed_original(setup):
    original_receipt, _ = observer.observe(setup.path)
    original_marker = setup.root / '.local/linkedin-company-bootstrap.consumed'
    original_marker_bytes = original_marker.read_bytes()
    original_receipt_bytes = original_receipt.read_bytes()
    allowance_bytes = setup.allowance_path.read_bytes()
    setup.declaration['kind'] = STRUCTURE_KIND
    setup.save()
    receipt, result = observer.observe(setup.path)
    assert receipt == setup.root / 'docs/results/company-bootstrap-structure-observation.json'
    assert result['kind'] == STRUCTURE_KIND
    assert result['runtime_allowance_refilled'] is False
    assert result['runtime_allowance_unchanged'] is True
    assert original_marker.read_bytes() == original_marker_bytes
    assert original_receipt.read_bytes() == original_receipt_bytes
    assert setup.allowance_path.read_bytes() == allowance_bytes
    command, _ = setup.calls[-1]
    assert '--bootstrap-structure' in command and '--bootstrap' not in command
    new_marker = setup.root / '.local/linkedin-company-bootstrap-structure.consumed'
    assert json.loads(new_marker.read_text())['declaration_sha256'] == sha256(setup.path.read_bytes()).hexdigest()
    # Losing a synthetic receipt cannot turn the new explicit experiment into
    # a renewable allowance. The original evidence remains intact too.
    receipt.unlink()
    copied = setup.root / '.local/structure-copy.json'
    copied.write_bytes(setup.path.read_bytes())
    with pytest.raises(ValueError, match='already_consumed'):
        observer.observe(copied)
    setup.declaration['kind'] = METADATA_KIND
    setup.save()
    with pytest.raises(ValueError, match='already_consumed'):
        observer.observe(setup.path)
    assert len(setup.calls) == 2
    assert original_marker.read_bytes() == original_marker_bytes
    assert original_receipt.read_bytes() == original_receipt_bytes


@pytest.mark.parametrize('kind', ['company-bootstrap-structure/2', 'company-bootstrap-structure', 'structure'])
def test_structure_mode_requires_exact_explicit_kind(setup, kind):
    setup.declaration['kind'] = kind
    setup.save()
    with pytest.raises(ValueError, match='scope_invalid'):
        observer.observe(setup.path)
    assert not setup.calls


def test_structure_schema_accepts_native_reference_names_without_upgrading_membership():
    value = structure_capture()
    node = value['bootstrap_observation']['candidates'][0]['structure']['nodes'][0]
    node['path'] = '$.included[0].references'
    node['type_name'] = 'com.linkedin.voyager.dash.feed.Update'
    node['reference_fields'][0]['ids'] = ['urn:li:fsd_pagedList:initial-1']
    assert observer.validate_capture(value, STRUCTURE_KIND) == value
    original = capture()
    original['bootstrap_observation']['candidates'][0]['root_ids'] = ['urn:li:fsd_pagedList:initial-1']
    with pytest.raises(ValueError):
        observer.validate_capture(original)


@pytest.mark.parametrize('state,count', [('missing', 0), ('null', 0), ('invalid', 2), ('value', 0)])
def test_structure_reference_states_preserve_unknown_and_missing(state, count):
    value = structure_capture()
    reference = value['bootstrap_observation']['candidates'][0]['structure']['nodes'][0]['reference_fields'][0]
    reference.update(state=state, count=count, ids=[])
    assert observer.validate_capture(value, STRUCTURE_KIND) == value


@pytest.mark.parametrize('fault', [
    'raw_text', 'unsafe_path', 'credential_path', 'unsafe_type', 'credential_root_key',
    'credential_node_key', 'credential_reference_key', 'unknown_kind', 'too_many_nodes',
    'too_many_keys', 'too_many_references', 'too_many_ids', 'duplicate_ids',
    'bad_reference', 'boolean_count', 'count_below_ids', 'null_with_count',
    'invalid_with_ids', 'unknown_structure_key', 'unknown_candidate_key', 'original_shape',
    'too_many_candidates', 'too_many_node_keys', 'oversize_path', 'oversize_type',
    'oversize_reference', 'unsafe_count', 'credential_type', 'credential_reference',
])
def test_structure_schema_rejects_unbounded_or_unsafe_metadata(fault):
    value = structure_capture()
    candidate = value['bootstrap_observation']['candidates'][0]
    structure = candidate['structure']
    node = structure['nodes'][0]
    reference = node['reference_fields'][0]
    if fault == 'raw_text':
        node['text'] = 'publication text'
    elif fault == 'unsafe_path':
        node['path'] = '$.publication text'
    elif fault == 'credential_path':
        node['path'] = '$.csrfToken'
    elif fault == 'unsafe_type':
        node['type_name'] = 'publication text'
    elif fault == 'credential_root_key':
        structure['root_keys'] = ['csrfToken']
    elif fault == 'credential_node_key':
        node['keys'] = ['password']
    elif fault == 'credential_reference_key':
        reference['key'] = 'cookie'
    elif fault == 'unknown_kind':
        node['kind'] = 'publication'
    elif fault == 'too_many_nodes':
        structure['nodes'] = [deepcopy(node) for _ in range(101)]
    elif fault == 'too_many_keys':
        structure['root_keys'] = [f'key{i}' for i in range(101)]
    elif fault == 'too_many_references':
        node['reference_fields'] = [deepcopy(reference) for _ in range(21)]
    elif fault == 'too_many_ids':
        reference['ids'] = [f'urn:li:activity:{i}' for i in range(101)]
        reference['count'] = 101
    elif fault == 'duplicate_ids':
        reference['ids'] *= 2
        reference['count'] = 2
    elif fault == 'bad_reference':
        reference['ids'] = ['authored publication']
    elif fault == 'boolean_count':
        reference['count'] = True
    elif fault == 'count_below_ids':
        reference['count'] = 0
    elif fault == 'null_with_count':
        reference.update(state='null', count=1, ids=[])
    elif fault == 'invalid_with_ids':
        reference['state'] = 'invalid'
    elif fault == 'unknown_structure_key':
        structure['html'] = '<div>authored text</div>'
    elif fault == 'unknown_candidate_key':
        candidate['raw_body'] = 'authored publication'
    elif fault == 'too_many_candidates':
        value['bootstrap_observation']['candidates'] = [deepcopy(candidate) for _ in range(101)]
    elif fault == 'too_many_node_keys':
        node['keys'] = [f'key{i}' for i in range(101)]
    elif fault == 'oversize_path':
        node['path'] = '$' + '.field' * 334
    elif fault == 'oversize_type':
        node['type_name'] = 'com.linkedin.' + 'a' * 200
    elif fault == 'oversize_reference':
        reference['ids'] = ['urn:li:activity:' + '1' * 1000]
    elif fault == 'unsafe_count':
        reference['count'] = 2**53
    elif fault == 'credential_type':
        node['type_name'] = 'com.linkedin.secretValue'
    elif fault == 'credential_reference':
        reference['ids'] = ['urn:li:activity:bearer123']
    else:
        candidate.clear()
        candidate.update(capture()['bootstrap_observation']['candidates'][0])
    with pytest.raises((ValueError, TypeError)):
        observer.validate_capture(value, STRUCTURE_KIND)


@pytest.mark.parametrize('reason,security', [('challenge', True), ('authentication_required', False)])
def test_structure_stops_persist_source_hold_without_reopening_original(setup, monkeypatch, reason, security):
    marker = setup.root / '.local/linkedin-company-bootstrap.consumed'
    marker.write_text('original-consumed')
    setup.declaration['kind'] = STRUCTURE_KIND
    setup.save()
    allowance = setup.allowance_path.read_bytes()
    value = structure_capture()
    value.update(session_verified=False, stopped={'reason': reason, 'security_hold': security})
    monkeypatch.setattr(observer.subprocess, 'run', lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=json.dumps(value).encode()))
    _, result = observer.observe(setup.path)
    assert result['failure_kind'] == 'ObservationStopped'
    with setup.journal.connect() as connection:
        assert tuple(connection.execute('SELECT generation,stopped,reason FROM route_state').fetchone()) == (1, 1, reason)
    assert marker.read_text() == 'original-consumed'
    assert setup.allowance_path.read_bytes() == allowance


@pytest.mark.parametrize('fault', ['timeout', 'unsafe_child'])
def test_structure_failure_consumes_only_its_own_marker_and_never_copies_text(setup, monkeypatch, fault):
    setup.declaration['kind'] = STRUCTURE_KIND
    setup.save()
    original_marker = setup.root / '.local/linkedin-company-bootstrap.consumed'
    original_marker.write_text('original-consumed')
    secret = 'synthetic-authored-text-MUST-NOT-PERSIST'

    def child(command, **kwargs):
        setup.calls.append((command, kwargs))
        if fault == 'timeout':
            raise subprocess.TimeoutExpired(command, kwargs['timeout'], output=secret.encode())
        value = structure_capture()
        value['bootstrap_observation']['candidates'][0]['structure']['nodes'][0]['raw'] = secret
        return SimpleNamespace(returncode=0, stdout=json.dumps(value).encode())

    monkeypatch.setattr(observer.subprocess, 'run', child)
    receipt, result = observer.observe(setup.path)
    assert result['failure_kind'] in {'BootstrapGuardFailure', 'TimeoutExpired'}
    assert secret not in receipt.read_text()
    assert 'bootstrap_observation' not in result
    assert original_marker.read_text() == 'original-consumed'
    assert (setup.root / '.local/linkedin-company-bootstrap-structure.consumed').exists()
    with pytest.raises(ValueError, match='already_consumed'):
        observer.observe(setup.path)
    assert len(setup.calls) == 1


@pytest.mark.parametrize('prefixes', [('ghp_', 'sk-'), ('GHP_', 'SK-')])
def test_actual_node_inventory_matches_python_privacy_contract(prefixes):
    """Exercise the real pure producer, with no wrapper/browser subprocess."""
    unsafe_keys = ['paginationToken', 'csrf', 'password', 'cookie',
                   prefixes[0] + 'A' * 24, prefixes[1] + 'B' * 24]
    body = {'data': {'$type': 'com.linkedin.voyager.dash.feed.Update',
                     '*elements': ['urn:li:activity:1'],
                     'text': 'synthetic-publication-MUST-NOT-APPEAR'}}
    for key in unsafe_keys:
        body[key] = {'*elements': ['urn:li:activity:9']}
        body['data'][key] = 'synthetic-private-value-MUST-NOT-APPEAR'
    javascript = "const h=require(process.argv[1]);let data='';process.stdin.setEncoding('utf8');process.stdin.on('data',c=>data+=c);process.stdin.on('end',()=>console.log(JSON.stringify(h.summarizeBootstrapStructure(JSON.parse(data)))));"
    structure = actual_node_output(javascript, body, PROJECT / 'scripts/company-bootstrap.cjs')
    value = structure_capture()
    value['bootstrap_observation']['status'] = structure['status']
    value['bootstrap_observation']['candidates'][0]['structure'] = structure
    assert observer.validate_capture(value, STRUCTURE_KIND) == value
    serialized = json.dumps(structure)
    assert all(key not in serialized for key in unsafe_keys)
    assert 'synthetic-publication-MUST-NOT-APPEAR' not in serialized
    assert 'synthetic-private-value-MUST-NOT-APPEAR' not in serialized
    assert any(ref['ids'] == ['urn:li:activity:1']
               for entry in structure['nodes'] for ref in entry['reference_fields'])


def test_body_mode_consumption_preserves_both_previous_observations(setup):
    originals = []
    for kind in (METADATA_KIND, STRUCTURE_KIND):
        setup.declaration['kind'] = kind
        setup.save()
        receipt, _ = observer.observe(setup.path)
        marker = setup.root / '.local' / ('linkedin-company-bootstrap.consumed' if kind == METADATA_KIND
                                       else 'linkedin-company-bootstrap-structure.consumed')
        originals.extend([(receipt, receipt.read_bytes()), (marker, marker.read_bytes())])
    allowance = setup.allowance_path.read_bytes()
    setup.declaration['kind'] = BODY_KIND
    setup.save()
    receipt, result = observer.observe(setup.path)
    assert receipt == setup.root / 'docs/results/company-bootstrap-body-observation.json'
    assert result['kind'] == BODY_KIND
    assert result['runtime_allowance_unchanged'] is True
    assert result['runtime_allowance_refilled'] is False
    command, _ = setup.calls[-1]
    assert '--bootstrap-body' in command
    assert '--bootstrap' not in command and '--bootstrap-structure' not in command
    marker = setup.root / '.local/linkedin-company-bootstrap-body.consumed'
    assert json.loads(marker.read_text())['declaration_sha256'] == sha256(setup.path.read_bytes()).hexdigest()
    assert all(path.read_bytes() == original for path, original in originals)
    assert setup.allowance_path.read_bytes() == allowance
    receipt.unlink()
    copied = setup.root / '.local/body-copy.json'
    copied.write_bytes(setup.path.read_bytes())
    with pytest.raises(ValueError, match='already_consumed'):
        observer.observe(copied)
    for kind in (METADATA_KIND, STRUCTURE_KIND):
        setup.declaration['kind'] = kind
        setup.save()
        with pytest.raises(ValueError, match='already_consumed'):
            observer.observe(setup.path)
    assert len(setup.calls) == 3
    assert all(path.read_bytes() == original for path, original in originals)


@pytest.mark.parametrize('serialized', [True, False])
def test_actual_embedded_normalized_body_roundtrips_without_private_values(serialized):
    envelope = synthetic_response_envelope(normalized_embedded_body(), serialized)
    value = actual_embedded_body_capture(envelope)
    assert observer.validate_capture(value, BODY_KIND) == value
    candidate = value['bootstrap_observation']['candidates'][0]
    assert candidate['source'] == 'initial_document_body_json'
    assert candidate['envelope']['request_family'] == 'feed'
    assert candidate['envelope']['target_binding'] == 'matched'
    assert candidate['envelope']['body_encoding'] == ('json_string' if serialized else 'json_object')
    assert candidate['collection']['status'] == 'observed_start_zero'
    assert candidate['collection']['paging'] == {'start': 0, 'count': 3, 'total': 20}
    assert candidate['collection']['root_ids'] == ['urn:li:fsd_update:3', 'urn:li:fsd_update:1', 'urn:li:fsd_update:2']
    assert candidate['body_sha256'] != candidate['outer_body_sha256']
    output = json.dumps(value)
    assert envelope['request'] not in output
    assert 'synthetic-csrf-MUST-NOT-PERSIST' not in output
    assert 'synthetic-cookie-MUST-NOT-PERSIST' not in output
    assert 'synthetic-authored-publication-MUST-NOT-PERSIST' not in output
    assert 'synthetic-unrelated-publication-MUST-NOT-PERSIST' not in output


def test_actual_noncanonical_body_keeps_structure_without_inventing_collection():
    body = {'payload': {'updates': {'$type': 'com.linkedin.voyager.dash.feed.Update',
                                   '*elements': ['urn:li:fsd_update:1'],
                                   'text': 'synthetic-authored-text-MUST-NOT-PERSIST'}}}
    value = actual_embedded_body_capture(synthetic_response_envelope(body))
    observer.validate_capture(value, BODY_KIND)
    candidate = value['bootstrap_observation']['candidates'][0]
    assert candidate['collection']['status'] == 'absent'
    assert candidate['collection']['root_ids'] == []
    assert any(reference['ids'] == ['urn:li:fsd_update:1']
               for entry in candidate['structure']['nodes'] for reference in entry['reference_fields'])
    assert 'synthetic-authored-text-MUST-NOT-PERSIST' not in json.dumps(value)


def test_unknown_target_binding_preserves_structure_without_target_collection():
    envelope = synthetic_response_envelope(normalized_embedded_body())
    envelope['request'] = '/voyager/api/graphql?queryId=voyagerFeedDashOrganizationalPageUpdates.synthetic'
    value = actual_embedded_body_capture(envelope)
    observer.validate_capture(value, BODY_KIND)
    candidate = value['bootstrap_observation']['candidates'][0]
    assert candidate['envelope']['target_binding'] == 'unknown'
    assert candidate['collection']['status'] not in {'observed_start_zero', 'observed_nonzero'}
    assert candidate['collection']['root_ids'] == []
    assert candidate['structure']['nodes']


@pytest.mark.parametrize('fault', ['double_encoded', 'invalid_json', 'foreign_target', 'write_method'])
def test_body_envelope_refusal_keeps_safe_bounded_metadata(fault):
    envelope = synthetic_response_envelope(normalized_embedded_body())
    if fault == 'double_encoded':
        envelope['body'] = json.dumps(envelope['body'])
    elif fault == 'invalid_json':
        envelope['body'] = 'invalid-json-synthetic-text-MUST-NOT-PERSIST'
    elif fault == 'foreign_target':
        envelope['request'] = envelope['request'].replace('1337', '9999')
    else:
        envelope['method'] = 'POST'
    value = actual_embedded_body_capture(envelope)
    observer.validate_capture(value, BODY_KIND)
    candidate = value['bootstrap_observation']['candidates'][0]
    assert candidate['collection']['status'] not in {'observed_start_zero', 'observed_nonzero'}
    assert candidate['collection']['root_ids'] == []
    assert candidate['body_sha256'] is None
    assert candidate['structure']['nodes'] == []
    if fault == 'foreign_target':
        assert candidate['envelope']['target_binding'] == 'mismatched'
    if fault == 'write_method':
        assert candidate['envelope']['method'] == 'other'
    assert 'MUST-NOT-PERSIST' not in json.dumps(value)


@pytest.mark.parametrize('error,reason,security', [
    ({'code': 'CHALLENGE'}, 'challenge', True),
    ({'status': 403}, 'restricted', True),
    ({'status': 401}, 'authentication_required', False),
])
def test_actual_decoded_errors_hold_existing_journal_without_allowance_changes(setup, monkeypatch, error, reason, security):
    error['message'] = 'synthetic-private-error-MUST-NOT-PERSIST'
    value = actual_embedded_body_capture(synthetic_response_envelope({'errors': [error]}))
    observer.validate_capture(value, BODY_KIND)
    assert value['session_verified'] is False
    assert value['stopped'] == {'reason': reason, 'security_hold': security}
    assert value['bootstrap_observation']['candidates'][0]['collection']['status'] == 'invalid'
    assert 'synthetic-private-error-MUST-NOT-PERSIST' not in json.dumps(value)
    setup.declaration['kind'] = BODY_KIND
    setup.save()
    allowance = setup.allowance_path.read_bytes()
    monkeypatch.setattr(observer.subprocess, 'run', lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=json.dumps(value).encode()))
    _, result = observer.observe(setup.path)
    assert result['failure_kind'] == 'ObservationStopped'
    with setup.journal.connect() as connection:
        assert tuple(connection.execute('SELECT generation,stopped,reason FROM route_state').fetchone()) == (1, 1, reason)
    assert setup.allowance_path.read_bytes() == allowance


@pytest.mark.parametrize('fault', ['raw_header', 'raw_request', 'missing_outer_hash', 'missing_decoded_hash',
                                  'encoded_success', 'wrong_binding', 'invalid_method',
                                  'native_outer_hash', 'native_serialized_body', 'unknown_binding_collection'])
def test_body_schema_rejects_private_or_inconsistent_provenance(fault):
    value = body_capture()
    candidate = value['bootstrap_observation']['candidates'][0]
    if fault == 'raw_header':
        candidate['envelope']['headers'] = {'cookie': 'synthetic-private'}
    elif fault == 'raw_request':
        candidate['envelope']['request'] = '/private-request'
    elif fault == 'missing_outer_hash':
        candidate['outer_body_sha256'] = None
    elif fault == 'missing_decoded_hash':
        candidate['body_sha256'] = None
    elif fault == 'encoded_success':
        candidate['envelope']['body_encoding'] = 'unsupported'
        candidate['body_sha256'] = None
    elif fault == 'wrong_binding':
        candidate['envelope']['target_binding'] = 'mismatched'
    elif fault == 'unknown_binding_collection':
        candidate['envelope']['target_binding'] = 'unknown'
    elif fault in {'native_outer_hash', 'native_serialized_body'}:
        candidate['source'] = 'native_feed_json'
        candidate['envelope']['body_encoding'] = 'json_object'
        if fault == 'native_serialized_body':
            candidate['outer_body_sha256'] = None
            candidate['envelope']['body_encoding'] = 'json_string'
    else:
        candidate['envelope']['method'] = 'POST'
    with pytest.raises((ValueError, TypeError)):
        observer.validate_capture(value, BODY_KIND)


def test_resolver_mode_preserves_three_consumed_observations_and_allowance(setup):
    originals = []
    for kind, marker_name in (
            (METADATA_KIND, 'linkedin-company-bootstrap.consumed'),
            (STRUCTURE_KIND, 'linkedin-company-bootstrap-structure.consumed'),
            (BODY_KIND, 'linkedin-company-bootstrap-body.consumed')):
        setup.declaration['kind'] = kind
        setup.save()
        receipt, _ = observer.observe(setup.path)
        marker = setup.root / '.local' / marker_name
        originals.extend([(marker, marker.read_bytes()), (receipt, receipt.read_bytes())])
    allowance = setup.allowance_path.read_bytes()
    setup.declaration['kind'] = RESOLVER_KIND
    setup.save()
    receipt, result = observer.observe(setup.path)
    assert receipt == setup.root / 'docs/results/company-bootstrap-resolver-observation.json'
    assert result['kind'] == RESOLVER_KIND
    assert result['runtime_allowance_unchanged'] is True
    assert result['runtime_allowance_refilled'] is False
    command, _ = setup.calls[-1]
    assert '--bootstrap-resolver' in command
    assert all(flag not in command for flag in ('--bootstrap', '--bootstrap-structure', '--bootstrap-body'))
    assert all(path.read_bytes() == before for path, before in originals)
    assert setup.allowance_path.read_bytes() == allowance
    marker = setup.root / '.local/linkedin-company-bootstrap-resolver.consumed'
    assert json.loads(marker.read_text())['declaration_sha256'] == sha256(setup.path.read_bytes()).hexdigest()
    receipt.unlink()
    copied = setup.root / '.local/resolver-copy.json'
    copied.write_bytes(setup.path.read_bytes())
    with pytest.raises(ValueError, match='already_consumed'):
        observer.observe(copied)
    assert len(setup.calls) == 4
    assert all(path.read_bytes() == before for path, before in originals)


@pytest.mark.parametrize('field,value', [
    ('target', 'https://www.linkedin.com/company/other/posts/'),
    ('expires_at', '2026-10-03T12:00:00+00:00'),
])
def test_resolver_target_and_deadline_refuse_before_consumption(setup, field, value):
    setup.declaration.update(kind=RESOLVER_KIND)
    setup.declaration[field] = value
    setup.save()
    with pytest.raises(ValueError):
        observer.observe(setup.path)
    assert not setup.calls
    assert not (setup.root / '.local/linkedin-company-bootstrap-resolver.consumed').exists()


@pytest.mark.parametrize('entity_encoded', [False, True])
def test_actual_unique_inert_id_resolves_direct_or_entity_json_without_leaks(entity_encoded):
    element_id = 'source-cache-unique'
    envelope = synthetic_response_envelope(normalized_embedded_body())
    envelope['body'] = element_id
    payload = json.dumps(normalized_embedded_body())
    if entity_encoded:
        payload = html.escape(payload, quote=True)
    document = resolver_document(envelope, '<code id="' + element_id + '"><!--' + payload + '--></code>')
    value = actual_resolver_capture(document)
    observer.validate_capture(value, RESOLVER_KIND)
    encoding = 'reference_entity_json' if entity_encoded else 'reference_json'
    candidate = next(candidate for candidate in value['bootstrap_observation']['candidates']
                     if candidate['envelope']['body_encoding'] == encoding)
    assert candidate['envelope']['body_length'] == len(element_id.encode())
    assert candidate['collection']['status'] == 'observed_start_zero'
    assert candidate['collection']['root_ids'] == ['urn:li:fsd_update:3', 'urn:li:fsd_update:1', 'urn:li:fsd_update:2']
    assert candidate['body_sha256'] and candidate['outer_body_sha256']
    serialized = json.dumps(value)
    assert element_id not in serialized
    assert envelope['request'] not in serialized
    assert 'synthetic-csrf-MUST-NOT-PERSIST' not in serialized
    assert 'synthetic-cookie-MUST-NOT-PERSIST' not in serialized
    assert 'synthetic-authored-publication-MUST-NOT-PERSIST' not in serialized


@pytest.mark.parametrize('fault', ['missing', 'duplicate', 'noninert', 'url', 'partial_id', 'recursive_reference', 'double_entity'])
def test_actual_resolver_refuses_ambiguous_external_or_recursive_data(fault):
    element_id = 'source-cache-refusal'
    envelope = synthetic_response_envelope(normalized_embedded_body())
    envelope['body'] = element_id
    payload = json.dumps(normalized_embedded_body())
    block = '<code id="' + element_id + '">' + payload + '</code>'
    if fault == 'missing':
        block = ''
    elif fault == 'duplicate':
        block += '<script id="' + element_id + '" type="application/json">' + payload + '</script>'
    elif fault == 'noninert':
        block = '<script id="' + element_id + '">' + payload + '</script>'
    elif fault == 'url':
        envelope['body'] = 'https://example.invalid/source-cache-refusal'
    elif fault == 'partial_id':
        envelope['body'] = 'prefix-' + element_id
    elif fault == 'recursive_reference':
        block = '<code id="' + element_id + '">' + json.dumps('another-reference') + '</code>'
        block += '<code id="another-reference">' + payload + '</code>'
    else:
        block = '<code id="' + element_id + '">' + html.escape(html.escape(payload, quote=True), quote=True) + '</code>'
    value = actual_resolver_capture(resolver_document(envelope, block))
    observer.validate_capture(value, RESOLVER_KIND)
    candidate = next(candidate for candidate in value['bootstrap_observation']['candidates']
                     if candidate['envelope']['body_length'] == len(envelope['body'].encode()))
    assert candidate['body_sha256'] is None
    assert candidate['collection']['root_ids'] == []
    assert candidate['structure']['nodes'] == []
    if fault == 'missing' or fault == 'partial_id':
        assert candidate['envelope']['body_encoding'] == 'reference_missing'
    if fault == 'duplicate':
        assert candidate['envelope']['body_encoding'] == 'reference_ambiguous'
    assert element_id not in json.dumps(value)


def test_actual_resolver_direct_json_reports_utf8_body_length():
    body = normalized_embedded_body()
    body['nonAsciiText'] = 'é'
    envelope = synthetic_response_envelope(body)
    envelope['body'] = json.dumps(body, ensure_ascii=False)
    value = actual_resolver_capture(resolver_document(envelope))
    observer.validate_capture(value, RESOLVER_KIND)
    candidate = value['bootstrap_observation']['candidates'][0]
    assert candidate['envelope']['body_encoding'] == 'json_string'
    assert candidate['envelope']['body_length'] == len(envelope['body'].encode('utf-8'))
    assert candidate['envelope']['body_length'] > len(envelope['body'])
    assert 'é' not in json.dumps(value, ensure_ascii=False)


def test_actual_referenced_security_error_flows_to_existing_hold(setup, monkeypatch):
    element_id = 'source-security-reference'
    envelope = synthetic_response_envelope({})
    envelope['body'] = element_id
    payload = json.dumps({'errors': [{'code': 'CHALLENGE', 'message': 'synthetic-error-MUST-NOT-PERSIST'}]})
    value = actual_resolver_capture(resolver_document(envelope, '<code id="' + element_id + '">' + payload + '</code>'))
    observer.validate_capture(value, RESOLVER_KIND)
    assert value['session_verified'] is False
    assert value['stopped'] == {'reason': 'challenge', 'security_hold': True}
    assert 'synthetic-error-MUST-NOT-PERSIST' not in json.dumps(value)
    setup.declaration['kind'] = RESOLVER_KIND
    setup.save()
    allowance = setup.allowance_path.read_bytes()
    monkeypatch.setattr(observer.subprocess, 'run', lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=json.dumps(value).encode()))
    _, result = observer.observe(setup.path)
    assert result['failure_kind'] == 'ObservationStopped'
    with setup.journal.connect() as connection:
        assert tuple(connection.execute('SELECT generation,stopped,reason FROM route_state').fetchone()) == (1, 1, 'challenge')
    assert setup.allowance_path.read_bytes() == allowance


@pytest.mark.parametrize('fault', ['bool', 'negative', 'oversize', 'missing', 'native_length', 'original_body_mode'])
def test_resolver_length_and_mode_schema_are_strict(fault):
    value = resolver_capture()
    candidate = value['bootstrap_observation']['candidates'][0]
    if fault in {'bool', 'negative', 'oversize'}:
        candidate['envelope']['body_length'] = {'bool': True, 'negative': -1, 'oversize': 8_000_001}[fault]
    elif fault == 'missing':
        del candidate['envelope']['body_length']
    elif fault == 'native_length':
        candidate['source'] = 'native_feed_json'
        candidate['outer_body_sha256'] = None
        candidate['envelope']['body_encoding'] = 'json_object'
    with pytest.raises((ValueError, TypeError)):
        observer.validate_capture(value, BODY_KIND if fault == 'original_body_mode' else RESOLVER_KIND)


@pytest.mark.parametrize('root', [
    'urn:li:activity:ghp_' + 'A' * 24,
    'urn:li:activity:SK-' + 'B' * 24,
    'urn:li:fsd_update:csrfValue',
])
def test_resolver_consumer_refuses_credential_shaped_collection_roots(root):
    value = resolver_capture()
    value['bootstrap_observation']['candidates'][0]['collection']['root_ids'] = [root]
    with pytest.raises(ValueError):
        observer.validate_capture(value, RESOLVER_KIND)


def test_actual_resolver_sensitive_root_preserves_only_safe_partial_metadata():
    body = normalized_embedded_body()
    sensitive = 'urn:li:activity:ghp_' + 'A' * 24
    body['data']['data'][observer.RECIPE]['*elements'].append(sensitive)
    envelope = synthetic_response_envelope(body)
    value = actual_resolver_capture(resolver_document(envelope))
    observer.validate_capture(value, RESOLVER_KIND)
    candidate = value['bootstrap_observation']['candidates'][0]
    assert candidate['collection']['status'] == 'invalid'
    assert candidate['collection']['root_ids'] == []
    assert candidate['body_sha256'] and candidate['outer_body_sha256']
    assert candidate['structure']['nodes']
    assert sensitive not in json.dumps(value)
