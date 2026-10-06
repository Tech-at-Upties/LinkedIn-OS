"""Bounded metadata-only semantics from current retained source pages. No source read."""
import argparse
from datetime import UTC, datetime
from hashlib import sha256
import json
from pathlib import Path
import re
import sqlite3

from nos_linkedin.parser import RECIPE, native_id, parse_company_feed

ROOT = Path(__file__).resolve().parents[2]


def _identity(value):
    return value if isinstance(value, str) and native_id(value) else None


def _field(value):
    return {'state': value.state.value, 'id': _identity(value.value)}


def summarize_batch(journal_path, parent_attempt_id, *, clock=None):
    """Read one existing batch under its original deadline, emitting no text/body."""
    clock = clock or (lambda: datetime.now(UTC))
    path = Path(journal_path).resolve()
    if (not path.is_relative_to(ROOT / '.local') or not path.is_file()
            or not isinstance(parent_attempt_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,200}', parent_attempt_id)):
        raise ValueError('bounded_owned_batch_required')
    now = clock()
    if now.utcoffset() is None:
        raise ValueError('aware_verification_clock_required')
    connection = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute('BEGIN')
        batch = connection.execute('SELECT * FROM response_batches WHERE parent_attempt_id=?', (parent_attempt_id,)).fetchone()
        if (batch is None or batch['state'] != 'captured' or batch['declared_page_budget'] not in (2, 3)
                or batch['available_page_count'] not in (2, 3) or batch['available_page_count'] > batch['declared_page_budget']):
            raise ValueError('current_captured_batch_required')
        state = connection.execute("SELECT generation,stopped FROM route_state WHERE scope='linkedin.company-feed'").fetchone()
        if state is None or state['stopped']:
            raise ValueError('batch_source_fenced')
        specs = connection.execute('SELECT * FROM response_batch_pages WHERE parent_attempt_id=? ORDER BY ordinal', (parent_attempt_id,)).fetchall()
        expected = ((0, 3, 'initial_document'), (3, 10, 'api_following'), (13, 10, 'api_following'))
        if len(specs) != batch['declared_page_budget'] or any(
                (spec['ordinal'], spec['start'], spec['count'], spec['page_mode']) != (ordinal, *expected[ordinal])
                for ordinal, spec in enumerate(specs)):
            raise ValueError('batch_reserved_scope_changed')
        reports, expiry, capture_metadata = [], None, None
        for spec in specs[:batch['available_page_count']]:
            # Check eligibility using metadata before selecting or decoding body bytes.
            now = clock()
            if now.utcoffset() is None:
                raise ValueError('aware_verification_clock_required')
            meta = connection.execute('SELECT scope,generation,expires_at,outcome FROM responses WHERE attempt_id=?', (spec['attempt_id'],)).fetchone()
            if (meta is None or meta['scope'] != 'linkedin.company-feed' or meta['generation'] != state['generation']
                    or meta['expires_at'] <= now.timestamp() or meta['outcome'] not in ('bounded', 'partial')):
                raise ValueError('batch_retention_or_classification_ineligible')
            if expiry is not None and expiry != meta['expires_at']:
                raise ValueError('batch_original_deadline_differs')
            expiry = meta['expires_at']
            if expiry <= clock().timestamp():
                raise ValueError('batch_retention_or_classification_ineligible')
            row = connection.execute('SELECT body,body_sha256,captured_at,evidence_class FROM responses WHERE attempt_id=?', (spec['attempt_id'],)).fetchone()
            publisher = connection.execute('SELECT feed_publisher_id FROM attempt_context WHERE attempt_id=?', (spec['attempt_id'],)).fetchone()
            if (row['body'] is None or len(row['body']) > 2_000_000
                    or sha256(row['body']).hexdigest() != row['body_sha256']
                    or publisher is None or publisher[0] != 'urn:li:fsd_company:1337'):
                raise ValueError('batch_source_evidence_changed')
            body = json.loads(row['body'])
            collection = body['data']['data'][RECIPE]
            roots = collection['*elements']
            if (not isinstance(roots, list) or len(roots) > spec['count'] or not all(native_id(root) for root in roots)
                    or not isinstance(body.get('included'), list) or len(body['included']) > 180):
                raise ValueError('bounded_page_graph_required')
            captured = datetime.fromtimestamp(row['captured_at'], UTC)
            page = parse_company_feed(body, feed_publisher_id=publisher[0], observed_at=captured, evidence_class=row['evidence_class'])
            if len(page.publications) > spec['count']:
                raise ValueError('bounded_page_publications_required')
            provenance = body.get('source_provenance')
            if not isinstance(provenance, dict) or provenance.get('page_mode') != spec['page_mode']:
                raise ValueError('batch_page_provenance_required')
            native = provenance.get('initial_page') if spec['ordinal'] == 0 else provenance.get('native_response')
            if not isinstance(native, dict) or not re.fullmatch(r'[a-f0-9]{64}', native.get('body_sha256', '')):
                raise ValueError('batch_native_digest_required')
            if spec['ordinal'] == 0 and any(not re.fullmatch(r'[a-f0-9]{64}', value or '') for value in
                    (native.get('wrapper_sha256'), native.get('navigation', {}).get('body_sha256'))):
                raise ValueError('batch_initial_digest_required')
            discrepancy = [{'code': issue.code, 'entity_id': _identity(issue.entity_id), 'field': issue.field} for issue in page.issues]
            if (page.paging.get('start'), page.paging.get('count')) != (spec['start'], spec['count']):
                discrepancy.append({'code': 'page_scope_discrepancy', 'entity_id': None, 'field': 'paging'})
            publications = []
            for item in page.publications:
                publications.append({'representation_id': item.representation_id, 'occurrence_id': item.occurrence_id,
                    'feed_publisher_id': item.feed_publisher_id, 'author': _field(item.actor_id),
                    'content': _field(item.content_id), 'original_occurrence': _field(item.reshared_occurrence_id),
                    'field_states': {'commentary': item.commentary.state.value, 'header': item.header_text.state.value,
                                     'root_share': item.root_share.state.value},
                    'root_share': item.root_share.value if type(item.root_share.value) is bool else None,
                    'metrics_target_id': item.metrics.target_id, 'metrics_entity_id': item.metrics.entity_id,
                    'metrics': {name: {'state': value.state.value, 'count': value.value if type(value.value) is int else None}
                                for name, value in item.metrics.values.items()},
                    'reaction_type_counts': [{'native_name': kind, 'count': count} for kind, count in item.metrics.reaction_types]})
            reports.append({'attempt_id': spec['attempt_id'], 'page_mode': spec['page_mode'], 'start': spec['start'], 'count': spec['count'],
                'root_ids': roots, 'captured_at': captured.isoformat(), 'projection_sha256': row['body_sha256'],
                'native_sha256': native['body_sha256'], 'provenance_sha256': sha256(json.dumps(provenance, sort_keys=True, separators=(',', ':')).encode()).hexdigest(),
                'navigation_sha256': native.get('navigation', {}).get('body_sha256') if spec['ordinal'] == 0 else None,
                'wrapper_sha256': native.get('wrapper_sha256') if spec['ordinal'] == 0 else None,
                'publications': publications, 'discrepancies': discrepancy})
            if spec['ordinal'] == 0:
                capture_metadata = body.get('batch_capture_metadata')
        if (type(capture_metadata) is not dict or set(capture_metadata) != {'native_reads_admitted', 'native_read_budget', 'batch_page_budget'}
                or any(type(value) is not int for value in capture_metadata.values())
                or not 0 < capture_metadata['native_reads_admitted'] <= 12 or capture_metadata['native_read_budget'] != 12
                or capture_metadata['batch_page_budget'] != batch['declared_page_budget']):
            raise ValueError('batch_capture_metadata_required')
        if expiry <= clock().timestamp():
            raise ValueError('batch_deadline_elapsed_during_verification')
        return {'kind': 'company-batch-semantic-metadata/1', 'parent_attempt_id': parent_attempt_id,
            'expires_at': datetime.fromtimestamp(expiry, UTC).isoformat(), 'capture_metadata': capture_metadata,
            'shortfall_reason': batch['shortfall_reason'], 'pages': reports}
    finally:
        connection.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--journal', required=True, type=Path)
    parser.add_argument('--parent-attempt', required=True)
    args = parser.parse_args(argv)
    print(json.dumps(summarize_batch(args.journal, args.parent_attempt), sort_keys=True))


if __name__ == '__main__':
    main()
