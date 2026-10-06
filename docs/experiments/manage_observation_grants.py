"""Explicit operator grant actions. Never dispatch source work or launch a child."""
import argparse
from hashlib import sha256
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[2]
CONTEXT_FIELDS = {'kind', 'project_root', 'profile', 'budget', 'source_journal',
                  'authority_id', 'source_generation', 'nos_source_root'}


def grant_api():
    path = ROOT / '.local/nos-integration/M1/src/xingestion/linkedin/grants.py'
    spec = importlib.util.spec_from_file_location('operator_observation_grants', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def strict_json(path):
    path = Path(path)
    if not path.is_file() or path.stat().st_size > 100_000:
        raise ValueError('bounded_metadata_file_required')
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('duplicate_metadata_key')
            result[key] = value
        return result
    def invalid_constant(_):
        raise ValueError('nonfinite_metadata')
    value = json.loads(path.read_bytes(), object_pairs_hook=unique, parse_constant=invalid_constant)
    if type(value) is not dict:
        raise ValueError('metadata_object_required')
    return value


def canonical_path(value):
    if type(value) is not str or not value or value != str(Path(value).resolve()):
        raise ValueError('explicit_canonical_path_required')
    return Path(value)


def owned_file(path, root, *, existing=True):
    path = canonical_path(str(path))
    if not path.is_relative_to(root / '.local') or not path.parent.is_dir():
        raise ValueError('original_owned_metadata_path_required')
    if existing and not path.is_file():
        raise ValueError('original_owned_metadata_file_required')
    return path


def context(path, api):
    value = strict_json(path)
    if set(value) != CONTEXT_FIELDS or value['kind'] != 'grant-controller-context/1':
        raise ValueError('grant_controller_context_invalid')
    root = canonical_path(value['project_root'])
    profile = canonical_path(value['profile'])
    budget = canonical_path(value['budget'])
    journal = canonical_path(value['source_journal'])
    source = canonical_path(value['nos_source_root'])
    if (not root.is_dir() or not profile.is_dir() or not budget.is_file() or not journal.is_file()
            or profile != root / '.local/linkedin-test-browser'
            or budget != root / '.local/linkedin-live-capture.sqlite'
            or not journal.is_relative_to(root / '.local') or not source.is_dir()
            or not api.identity(value['authority_id']) or type(value['source_generation']) is not int
            or value['source_generation'] < 0):
        raise ValueError('original_grant_context_required')
    owned_file(Path(path).resolve(), root)
    return value, dict(project_root=root, source_journal=journal,
                       authority_id=value['authority_id'], source_generation=value['source_generation'])


def reference_for(value, manifest, declaration_sha):
    return dict(kind='installed-observation-grant/1', project_root=value['project_root'],
        source_journal=value['source_journal'], authority_id=value['authority_id'],
        source_generation=value['source_generation'], grant_id=manifest['grant_id'],
        declaration_sha256=declaration_sha, nos_source_root=value['nos_source_root'])


def write_reference(path, reference, api):
    # A matching prior receipt supports recovery after the grant commit. It is
    # never replaced, even by another otherwise valid installed grant.
    if path.exists():
        if strict_json(path) != reference:
            raise ValueError('immutable_installed_reference_conflict')
        return
    with path.open('x', encoding='utf-8', newline='\n') as stream:
        stream.write(api.canonical(reference) + '\n')
        stream.flush()
        os.fsync(stream.fileno())


def execute(args, *, api=None):
    api = api or grant_api()
    value, arguments = context(args.context, api)
    root = Path(value['project_root'])
    receipt = {'kind': 'grant-controller-receipt/1', 'action': args.action, 'status': 'ok'}
    if args.action == 'inspect':
        if args.grant_id is not None and not api.identity(args.grant_id):
            raise ValueError('grant_identity_invalid')
        state = api.GrantConsumer(**arguments).inspect(args.grant_id)
        # Deliberately select metadata, rather than serialize database rows or
        # manifests wholesale. Historical evidence/witness bodies stay private.
        receipt.update(active_grant_id=state['slot']['active_grant_id'],
                       slot_generation=state['slot']['slot_generation'], migration=state['migration'])
        grant = state['grant']
        receipt['grant'] = None if grant is None else {
            key: grant['manifest'][key] for key in ('grant_id', 'workload_id', 'workload_position', 'expires_at')}
        if grant is not None:
            receipt['grant']['declaration_sha256'] = grant['declaration_sha']
        receipt['consumption'] = None if state['consumption'] is None else {
            key: state['consumption'][key] for key in ('attempt_id', 'task_id', 'run_id', 'consumed_at')}
        receipt['resolution'] = None if state['resolution'] is None else {
            key: state['resolution'][key] for key in ('attempt_id', 'resolved_at', 'witness_sha')}
        return receipt
    issuer = api.GrantIssuer(**arguments)
    if args.action == 'import-legacy':
        mapping = strict_json(owned_file(args.manifests, root))
        if not all(api.digest(key) and type(path) is str for key, path in mapping.items()):
            raise ValueError('legacy_manifest_map_invalid')
        for path in mapping.values():
            strict_json(owned_file(path, root))
        issuer.initialize()
        receipt.update(issuer.import_legacy(manifests=mapping))
    elif args.action == 'resolve-legacy':
        if not api.digest(args.declaration_sha):
            raise ValueError('legacy_declaration_digest_required')
        path = owned_file(args.witness, root)
        strict_json(path)
        # Resolution intentionally does not initialize or import anything.
        receipt.update(declaration_sha256=args.declaration_sha,
                       witness_sha256=issuer.resolve_legacy(args.declaration_sha, witness_path=path))
    elif args.action == 'declare-install':
        manifest = strict_json(owned_file(args.manifest, root))
        workload = strict_json(owned_file(args.workload, root))
        expected = api.manifest_fields(manifest.get('kind'))
        if set(manifest) != expected or set(workload) != {'workload_id', 'declaration_shas', 'native_read_budget'}:
            raise ValueError('declaration_metadata_schema_invalid')
        output = owned_file(args.installed_reference, root, existing=False)
        reference = reference_for(value, manifest, api.declaration_sha(manifest))
        if output.exists() and strict_json(output) != reference:
            raise ValueError('immutable_installed_reference_conflict')
        issuer.initialize()
        declaration = issuer.declare(manifest, workload=workload)
        write_reference(output, reference, api)
        receipt.update(grant_id=manifest['grant_id'], declaration_sha256=declaration,
                       installed_reference_sha256=sha256(output.read_bytes()).hexdigest())
    else:
        raise ValueError('explicit_grant_action_required')
    return receipt


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument('--context', required=True, type=Path)
    commands = result.add_subparsers(dest='action', required=True)
    command = commands.add_parser('import-legacy')
    command.add_argument('--manifests', required=True, type=Path)
    command = commands.add_parser('resolve-legacy')
    command.add_argument('--declaration-sha', required=True)
    command.add_argument('--witness', required=True, type=Path)
    command = commands.add_parser('declare-install')
    command.add_argument('--manifest', required=True, type=Path)
    command.add_argument('--workload', required=True, type=Path)
    command.add_argument('--installed-reference', required=True, type=Path)
    command = commands.add_parser('inspect')
    command.add_argument('--grant-id')
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        result = execute(args)
    except (ValueError, TypeError, KeyError, OSError, sqlite3.Error):
        # Exceptions can contain attacker-controlled JSON or filesystem text.
        print(json.dumps(dict(kind='grant-controller-receipt/1', action=args.action,
                              status='rejected', error='grant_controller_rejected')))
        return 1
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 0


if __name__ == '__main__':
    sys.exit(main())
