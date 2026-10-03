"""One root-owned metadata-only observation. Never an M1 allowance refill."""
import argparse
from contextlib import closing
from datetime import UTC, datetime
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess

ROOT = Path(__file__).resolve().parents[2]
TARGET = 'https://www.linkedin.com/company/linkedin/posts/'
SCOPE = 'linkedin.company-feed'
RECIPE = 'feedDashOrganizationalPageUpdatesByOrganizationalPageRelevanceFeed'
SAFE_INTEGER = 2**53 - 1
CHILD_KEYS = {'session_verified', 'stopped', 'failure_kind', 'native_reads_admitted',
              'navigation_status', 'account_writes_blocked', 'bootstrap_observation'}
FAILURES = {'Error', 'TimeoutError', 'ProjectionLimit', 'BootstrapLimit', 'DeadlineElapsed'}
STOPS = {'challenge', 'restricted', 'authentication_required', 'operator_stop', 'budget_exhausted'}
RUNTIME_KEYS = {'PATH', 'PATHEXT', 'SYSTEMROOT', 'WINDIR', 'TEMP', 'TMP', 'LOCALAPPDATA',
                'USERPROFILE', 'APPDATA', 'COMSPEC', 'SYSTEMDRIVE', 'PROGRAMFILES',
                'PROGRAMFILES(X86)', 'PROGRAMW6432', 'HOMEDRIVE', 'HOMEPATH'}
MODES = {
    'company-bootstrap-metadata/1': ('--bootstrap', 'linkedin-company-bootstrap.consumed',
                                     'company-bootstrap-observation.json'),
    'company-bootstrap-structure/1': ('--bootstrap-structure', 'linkedin-company-bootstrap-structure.consumed',
                                      'company-bootstrap-structure-observation.json'),
    'company-bootstrap-body/1': ('--bootstrap-body', 'linkedin-company-bootstrap-body.consumed',
                                 'company-bootstrap-body-observation.json'),
    'company-bootstrap-resolver/1': ('--bootstrap-resolver', 'linkedin-company-bootstrap-resolver.consumed',
                                     'company-bootstrap-resolver-observation.json'),
}
KEY_PATTERN = r'\*?[A-Za-z_$][A-Za-z0-9_.$-]{0,99}'
NATIVE_PATTERN = r'urn:li:(?:activity|share|ugcPost|organization|fsd_company|fsd_update):[A-Za-z0-9_(),:-]+'
STRUCTURE_NATIVE_PATTERN = r'urn:li:[A-Za-z][A-Za-z0-9_]*:[A-Za-z0-9_(),:.-]{1,1000}'
SENSITIVE_PATTERN = r'csrf|cookie|password|secret|authorization|bearer|token|api[_-]?key|gh[pousr]_[A-Za-z0-9]{20,}|sk-[A-Za-z0-9]{20,}'


def now():
    return datetime.now(UTC)


def owned_path(value):
    path = Path(value).resolve()
    local = (ROOT / '.local').resolve()
    if not local.is_relative_to(ROOT.resolve()) or path == local or not path.is_relative_to(local):
        raise ValueError('declaration_path_outside_owned_local_directory')
    return path


def source_root(value):
    """Allow only the intended local checkout or installed sibling NOS-V1."""
    path = Path(value).resolve()
    allowed = {ROOT / '.local/nos-integration', ROOT.parent / 'NOS-V1'}
    # Do not trust a symlink at an allowed path.
    if path not in allowed or not (path / 'M3/node_modules/playwright').is_dir():
        raise ValueError('playwright_source_root_not_allowlisted_or_installed')
    return path


def allowance_snapshot():
    path = owned_path(ROOT / '.local/linkedin-live-capture.sqlite')
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as connection:
        row = connection.execute('SELECT declaration_sha, attempt_id, consumed_at FROM capture_allowance WHERE singleton=1').fetchone()
        if row is None or row[1] is None or row[2] is None:
            raise ValueError('expected_existing_consumed_runtime_allowance')
        if connection.execute("SELECT 1 FROM sqlite_master WHERE name='capture_interventions'").fetchone():
            if connection.execute('SELECT 1 FROM capture_interventions LIMIT 1').fetchone():
                raise ValueError('browser_intervention_forbids_observation')
        return list(row)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate_json_key')
        result[key] = value
    return result


def decode_json(data):
    def invalid_constant(_):
        raise ValueError('nonfinite_json_number')
    return json.loads(data, object_pairs_hook=unique_object, parse_constant=invalid_constant)


def exact_keys(value, keys):
    if type(value) is not dict or set(value) != keys:
        raise ValueError('bootstrap_metadata_schema_invalid')


def integer(value, maximum=SAFE_INTEGER):
    return type(value) is int and 0 <= value <= maximum


def safe_keys(keys):
    return (type(keys) is list and len(keys) <= 100
            and all(type(key) is str and re.fullmatch(KEY_PATTERN, key)
                    and not re.search(SENSITIVE_PATTERN, key, re.IGNORECASE) for key in keys)
            and len(set(keys)) == len(keys))


def native_references(roots):
    return (type(roots) is list and len(roots) <= 100
            and all(type(root) is str and len(root) <= 1000 and re.fullmatch(STRUCTURE_NATIVE_PATTERN, root)
                    and not re.search(SENSITIVE_PATTERN, root, re.IGNORECASE) for root in roots)
            and len(set(roots)) == len(roots))


def validate_structure(structure):
    exact_keys(structure, {'status', 'root_keys', 'nodes'})
    if (structure['status'] not in {'observed', 'absent', 'bounded', 'invalid'}
            or not safe_keys(structure['root_keys']) or type(structure['nodes']) is not list
            or len(structure['nodes']) > 100):
        raise ValueError('bootstrap_metadata_schema_invalid')
    for node in structure['nodes']:
        exact_keys(node, {'path', 'kind', 'keys', 'type_name', 'reference_fields'})
        if (type(node['path']) is not str or len(node['path']) > 2000
                or not re.fullmatch(r'\$(?:\.' + KEY_PATTERN + r'|\[(?:0|[1-9][0-9]*)\])*', node['path'])
                or re.search(SENSITIVE_PATTERN, node['path'], re.IGNORECASE)
                or node['kind'] not in {'object', 'array', 'string', 'number', 'boolean', 'null'}
                or not safe_keys(node['keys'])
                or not (node['type_name'] is None or type(node['type_name']) is str
                        and len(node['type_name']) <= 200
                        and re.fullmatch(r'com\.linkedin\.[A-Za-z0-9_.$]+', node['type_name'])
                        and not re.search(SENSITIVE_PATTERN, node['type_name'], re.IGNORECASE))
                or type(node['reference_fields']) is not list or len(node['reference_fields']) > 20):
            raise ValueError('bootstrap_metadata_schema_invalid')
        for reference in node['reference_fields']:
            exact_keys(reference, {'key', 'ids', 'state', 'count'})
            if (type(reference['key']) is not str or not re.fullmatch(KEY_PATTERN, reference['key'])
                    or re.search(SENSITIVE_PATTERN, reference['key'], re.IGNORECASE)
                    or reference['state'] not in {'missing', 'null', 'invalid', 'value'}
                    or not native_references(reference['ids']) or not integer(reference['count'])
                    or reference['count'] < len(reference['ids'])
                    or reference['state'] != 'value' and reference['ids']
                    or reference['state'] in {'missing', 'null'} and reference['count'] != 0):
                raise ValueError('bootstrap_metadata_schema_invalid')


def validate_collection(collection):
    exact_keys(collection, {'status', 'recipe', 'paging', 'root_ids'})
    if (collection['recipe'] != RECIPE
            or collection['status'] not in {'absent', 'drifted', 'invalid', 'observed_start_zero', 'observed_nonzero'}):
        raise ValueError('bootstrap_metadata_schema_invalid')
    roots = collection['root_ids']
    if (type(roots) is not list or len(roots) > 100
            or any(type(root) is not str or len(root) > 1000 or not re.fullmatch(NATIVE_PATTERN, root) for root in roots)
            or len(set(roots)) != len(roots)):
        raise ValueError('bootstrap_metadata_schema_invalid')
    if collection['status'] in {'observed_start_zero', 'observed_nonzero'}:
        paging = collection['paging']
        exact_keys(paging, {'start', 'count', 'total'})
        if (not all(integer(value) for value in paging.values())
                or (paging['start'] == 0) != (collection['status'] == 'observed_start_zero')):
            raise ValueError('bootstrap_metadata_schema_invalid')
    elif collection['paging'] is not None or roots:
        raise ValueError('bootstrap_metadata_schema_invalid')


def digest(value):
    return type(value) is str and re.fullmatch(r'[0-9a-f]{64}', value) is not None


def validate_body_candidate(candidate, resolver=False):
    exact_keys(candidate, {'source', 'body_sha256', 'outer_body_sha256', 'envelope', 'collection', 'structure'})
    envelope = candidate['envelope']
    envelope_keys = {'method', 'status', 'request_family', 'target_binding', 'body_encoding'}
    encodings = {'json_string', 'json_object', 'missing', 'null', 'unsupported', 'invalid', 'bounded'}
    decoded_encodings = {'json_string', 'json_object'}
    if resolver:
        envelope_keys = envelope_keys | {'body_length'}
        encodings = encodings | {'reference_json', 'reference_entity_json', 'reference_missing',
                                 'reference_ambiguous', 'reference_unsupported'}
        decoded_encodings = decoded_encodings | {'reference_json', 'reference_entity_json'}
    exact_keys(envelope, envelope_keys)
    if resolver and not (envelope['body_length'] is None or integer(envelope['body_length'], 8_000_000)):
        raise ValueError('bootstrap_metadata_schema_invalid')
    if (envelope['method'] not in {None, 'GET', 'HEAD', 'other'}
            or not (envelope['status'] is None or type(envelope['status']) is int
                    and (100 <= envelope['status'] <= 599 or envelope['status'] == 999))
            or envelope['request_family'] not in {'company', 'feed', 'other', 'unknown'}
            or envelope['target_binding'] not in {'matched', 'mismatched', 'unknown'}
            or envelope['body_encoding'] not in encodings):
        raise ValueError('bootstrap_metadata_schema_invalid')
    decoded = envelope['body_encoding'] in decoded_encodings
    if candidate['source'] == 'initial_document_body_json':
        if not digest(candidate['outer_body_sha256']):
            raise ValueError('bootstrap_metadata_schema_invalid')
    elif candidate['source'] in {'native_company_json', 'native_feed_json'}:
        if (candidate['outer_body_sha256'] is not None or envelope['body_encoding'] != 'json_object'
                or resolver and envelope['body_length'] is not None):
            raise ValueError('bootstrap_metadata_schema_invalid')
    else:
        raise ValueError('bootstrap_metadata_schema_invalid')
    if (decoded and not digest(candidate['body_sha256'])
            or not decoded and candidate['body_sha256'] is not None):
        raise ValueError('bootstrap_metadata_schema_invalid')
    validate_collection(candidate['collection'])
    if resolver and any(re.search(SENSITIVE_PATTERN, root, re.IGNORECASE)
                        for root in candidate['collection']['root_ids']):
        raise ValueError('bootstrap_metadata_schema_invalid')
    validate_structure(candidate['structure'])
    binding = envelope['target_binding']
    if binding != 'matched' and candidate['collection']['status'] in {'observed_start_zero', 'observed_nonzero'}:
        raise ValueError('bootstrap_metadata_schema_invalid')
    if binding == 'mismatched' and (candidate['structure']['status'] == 'observed'
            or candidate['structure']['root_keys'] or candidate['structure']['nodes']):
        raise ValueError('bootstrap_metadata_schema_invalid')
    if not decoded and (candidate['collection']['status'] not in {'absent', 'drifted', 'invalid'}
            or candidate['structure']['status'] not in {'absent', 'invalid', 'bounded'}
            or candidate['structure']['root_keys'] or candidate['structure']['nodes']):
        raise ValueError('bootstrap_metadata_schema_invalid')


def validate_capture(capture, kind='company-bootstrap-metadata/1'):
    """Reject unknown fields before persisting any child-provided metadata."""
    if kind not in MODES:
        raise ValueError('declared_bootstrap_scope_invalid')
    exact_keys(capture, CHILD_KEYS)
    if (type(capture['session_verified']) is not bool or capture['account_writes_blocked'] is not True
            or not integer(capture['native_reads_admitted'], 12)
            or capture['failure_kind'] not in FAILURES | {None}
            or not (capture['navigation_status'] is None or type(capture['navigation_status']) is int
                    and (100 <= capture['navigation_status'] <= 599 or capture['navigation_status'] == 999))):
        raise ValueError('bootstrap_metadata_schema_invalid')
    stop = capture['stopped']
    if stop is not None:
        exact_keys(stop, {'reason', 'security_hold'})
        if stop['reason'] not in STOPS or type(stop['security_hold']) is not bool:
            raise ValueError('bootstrap_metadata_schema_invalid')
        if stop['security_hold'] != (stop['reason'] in {'challenge', 'restricted', 'operator_stop'}):
            raise ValueError('bootstrap_metadata_schema_invalid')
    if capture['session_verified'] and (stop is not None or capture['failure_kind'] is not None):
        raise ValueError('bootstrap_metadata_schema_invalid')
    observation = capture['bootstrap_observation']
    exact_keys(observation, {'status', 'candidates'})
    if (observation['status'] not in {'observed', 'absent', 'bounded', 'invalid'}
            or type(observation['candidates']) is not list or len(observation['candidates']) > 100):
        raise ValueError('bootstrap_metadata_schema_invalid')
    for candidate in observation['candidates']:
        if kind in {'company-bootstrap-body/1', 'company-bootstrap-resolver/1'}:
            validate_body_candidate(candidate, resolver=kind == 'company-bootstrap-resolver/1')
            continue
        if kind == 'company-bootstrap-structure/1':
            exact_keys(candidate, {'source', 'body_sha256', 'structure'})
            if (candidate['source'] not in {'initial_document_json', 'native_company_json', 'native_feed_json'}
                    or type(candidate['body_sha256']) is not str or not re.fullmatch(r'[0-9a-f]{64}', candidate['body_sha256'])):
                raise ValueError('bootstrap_metadata_schema_invalid')
            validate_structure(candidate['structure'])
            continue
        if kind != 'company-bootstrap-metadata/1':
            raise ValueError('declared_bootstrap_scope_invalid')
        exact_keys(candidate, {'source', 'body_sha256', 'status', 'recipe', 'paging', 'root_ids'})
        if (candidate['source'] not in {'initial_document_json', 'native_company_json', 'native_feed_json'}
                or type(candidate['body_sha256']) is not str or not re.fullmatch(r'[0-9a-f]{64}', candidate['body_sha256'])):
            raise ValueError('bootstrap_metadata_schema_invalid')
        validate_collection({key: candidate[key] for key in ('status', 'recipe', 'paging', 'root_ids')})
    return capture


def remaining_seconds(deadline):
    remaining = (deadline - now()).total_seconds()
    if remaining <= 0:
        raise ValueError('bootstrap_deadline_elapsed')
    return remaining


def validate_declaration(declaration_path):
    declaration_path = owned_path(declaration_path)
    data = declaration_path.read_bytes()
    declaration = decode_json(data)
    exact_keys(declaration, {'kind', 'target', 'native_read_budget', 'continuation', 'boundary',
                            'expires_at', 'probe_sha256', 'helper_sha256', 'consumed_runtime_allowance',
                            'source_journal', 'source_generation', 'nos_source_root'})
    if (declaration['kind'] not in MODES or declaration['target'] != TARGET
            or type(declaration['native_read_budget']) is not int or declaration['native_read_budget'] != 12
            or declaration['continuation'] is not False or declaration['boundary'] is not False
            or not integer(declaration['source_generation'])):
        raise ValueError('declared_bootstrap_scope_invalid')
    deadline = datetime.fromisoformat(declaration['expires_at'])
    if deadline.utcoffset() is None or not 0 < remaining_seconds(deadline) <= 600:
        raise ValueError('bootstrap_declaration_requires_current_ten_minute_deadline')
    script = ROOT / 'scripts/probe-linkedin-company.cjs'
    helper = ROOT / 'scripts/company-bootstrap.cjs'
    if (sha256(script.read_bytes()).hexdigest() != declaration['probe_sha256']
            or sha256(helper.read_bytes()).hexdigest() != declaration['helper_sha256']):
        raise ValueError('bootstrap_reviewed_script_digest_changed')
    if not owned_path(ROOT / '.local/linkedin-test-browser').is_dir() or owned_path(ROOT / '.local/linkedin-acquisition-hold.json').exists():
        raise ValueError('dedicated_session_or_security_preflight_failed')
    previous = allowance_snapshot()
    if previous != declaration['consumed_runtime_allowance']:
        raise ValueError('runtime_allowance_differs_from_declared_predecessor')
    source = owned_path(declaration['source_journal'])
    installed_source = source_root(declaration['nos_source_root'])
    node = shutil.which('node')
    if not source.is_file() or not node:
        raise ValueError('source_journal_or_node_runtime_absent')
    _, marker_name, receipt_name = MODES[declaration['kind']]
    marker = owned_path(ROOT / '.local' / marker_name)
    receipt = (ROOT / 'docs/results' / receipt_name).resolve()
    if not receipt.is_relative_to(ROOT.resolve() / 'docs/results'):
        raise ValueError('receipt_path_outside_owned_results_directory')
    if marker.exists() or receipt.exists():
        raise ValueError('bootstrap_observation_already_consumed')
    return declaration, deadline, previous, source, installed_source, node, marker, receipt, sha256(data).hexdigest()


def observe(declaration_path):
    declaration, deadline, previous, source_path, installed_source, node, marker, receipt_path, digest = validate_declaration(declaration_path)
    result = {'kind': declaration['kind'], 'target': TARGET, 'started_at': now().isoformat(),
              'declaration_sha256': digest, 'probe_sha256': declaration['probe_sha256'],
              'helper_sha256': declaration['helper_sha256'], 'native_read_budget': 12,
              'source_scope': SCOPE, 'source_generation': declaration['source_generation'],
              'runtime_allowance_refilled': False, 'publication_text_retained': False,
              'original_bodies_retained': False}
    with closing(sqlite3.connect(source_path.as_uri() + '?mode=rw', uri=True, timeout=min(5, remaining_seconds(deadline)))) as source:
        source.execute('BEGIN IMMEDIATE')
        state = source.execute('SELECT generation, stopped FROM route_state WHERE scope=?', (SCOPE,)).fetchone()
        if state is None or state[1] or state[0] != declaration['source_generation']:
            raise ValueError('existing_source_generation_or_hold_changed')
        if source.execute("SELECT 1 FROM responses WHERE scope=? AND outcome IN ('captured','classification_expired','dispatch_unknown') LIMIT 1", (SCOPE,)).fetchone():
            raise ValueError('unresolved_source_attempt_forbids_observation')
        remaining_seconds(deadline)
        if allowance_snapshot() != previous or owned_path(ROOT / '.local/linkedin-acquisition-hold.json').exists():
            raise ValueError('runtime_allowance_or_security_preflight_changed')
        with marker.open('xb') as consumed:
            consumed.write(json.dumps({'consumed_at': now().isoformat(), 'declaration_sha256': digest,
                                       'source_scope': SCOPE, 'source_generation': declaration['source_generation']}).encode())
            consumed.flush()
            os.fsync(consumed.fileno())
        try:
            env = {key: value for key, value in os.environ.items() if key.upper() in RUNTIME_KEYS}
            env['NOS_SOURCE_ROOT'] = str(installed_source)
            process = subprocess.run([node, str(ROOT / 'scripts/probe-linkedin-company.cjs'), TARGET,
                MODES[declaration['kind']][0], '--runtime-capture', '--bootstrap-deadline=' + deadline.isoformat()],
                cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                timeout=min(85, remaining_seconds(deadline)),
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
            if len(process.stdout) > 2_000_000:
                raise ValueError('bounded_bootstrap_child_failed')
            capture = validate_capture(decode_json(process.stdout), declaration['kind'])
            result.update(capture)
            stop = capture['stopped']
            if stop is not None and stop['reason'] != 'budget_exhausted':
                source.execute('UPDATE route_state SET generation=generation+1, stopped=1, reason=? WHERE scope=?', (stop['reason'], SCOPE))
                source.commit()
            if process.returncode:
                result['failure_kind'] = 'ChildExit'
            elif now() >= deadline:
                result['failure_kind'] = 'DeadlineElapsed'
            elif not capture['session_verified'] and not capture['failure_kind']:
                result['failure_kind'] = 'ObservationStopped' if stop else 'SessionUnverified'
        except subprocess.TimeoutExpired:
            result['failure_kind'] = 'TimeoutExpired'
        except Exception:
            # Never copy an exception message, URL, body or unvalidated child field.
            result['failure_kind'] = 'BootstrapGuardFailure'
        finally:
            source.rollback()
            try:
                result['runtime_allowance_unchanged'] = allowance_snapshot() == previous
            except Exception:
                result['runtime_allowance_unchanged'] = False
            if not result['runtime_allowance_unchanged']:
                result['failure_kind'] = 'RuntimeAllowanceChanged'
            result['post_capture_deadline_elapsed'] = now() >= deadline
            result['finished_at'] = now().isoformat()
            with receipt_path.open('x', encoding='utf-8') as receipt:
                json.dump(result, receipt, indent=2, allow_nan=False)
                receipt.write('\n')
                receipt.flush()
                os.fsync(receipt.fileno())
    return receipt_path, result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--declaration', required=True, type=Path)
    args = parser.parse_args()
    receipt, result = observe(args.declaration)
    print(json.dumps({'receipt': str(receipt.relative_to(ROOT)), 'failure_kind': result.get('failure_kind'),
                      'native_reads_admitted': result.get('native_reads_admitted'),
                      'runtime_allowance_unchanged': result['runtime_allowance_unchanged']}))
    return int(bool(result.get('failure_kind')))


if __name__ == '__main__':
    raise SystemExit(main())
