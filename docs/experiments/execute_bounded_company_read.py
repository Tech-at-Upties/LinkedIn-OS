"""One bounded browser capture through actual M1 execution and local M2 admission.

Retains an allowlisted projection, never a native raw body. No queue/HTTP collector
is registered. Explicit replay after process restart never launches another browser.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
NOS = ROOT / '.local/nos-integration'
sys.path[:0] = [str(NOS / 'M1/src'), str(NOS / 'M2/src')]

from nos_linkedin.acquisition import AcquisitionFailure, Journal, RoutePermission, SourceResponse
from nos_linkedin.parser import RECIPE
from nos_m2.m1 import M1Client
from nos_m2.models import EvidenceEnvelope
from nos_m2.store import Store
from xingestion.linkedin.executor import execute_company_page


def main() -> dict:
    attempt = f'bounded-company-{uuid4().hex}'
    publisher = 'urn:li:fsd_company:1337'
    source_path = ROOT / '.local/bounded-native-source.sqlite'
    sink_path = ROOT / '.local/bounded-native-m2.sqlite'
    journal = Journal(source_path)
    permission = RoutePermission(True, True, datetime.now(UTC) + timedelta(hours=1),
                                 'allowlisted_source_projection')
    store = Store(f'sqlite:///{sink_path.as_posix()}')
    store.initialize()
    source_calls = []
    delivery_results = []
    native_receipt = None
    projection_digest = None
    ids = []

    def send() -> SourceResponse:
        nonlocal native_receipt, projection_digest
        source_calls.append(attempt)
        process = subprocess.run(['node', str(ROOT / 'scripts/probe-linkedin-company.cjs')],
                                 cwd=ROOT, capture_output=True, text=True, timeout=70, check=False)
        # Never print subprocess stderr: a browser/library error can carry source URLs.
        if process.returncode != 0:
            raise AcquisitionFailure('bounded_probe_process_failure')
        report = json.loads(process.stdout.strip())
        receipt_path = (ROOT / report['result']).resolve()
        if not receipt_path.is_relative_to(ROOT / 'docs/results'):
            raise AcquisitionFailure('invalid_receipt_path')
        receipt = json.loads(receipt_path.read_text(encoding='utf-8'))
        native_receipt = {'path': str(receipt_path.relative_to(ROOT)), 'receipt': receipt}
        if receipt['stopped'] or not receipt['session_verified'] or receipt.get('failure_kind'):
            raise AcquisitionFailure('bounded_probe_stopped_or_failed')
        if not 0 < receipt['native_reads_admitted'] <= 12:
            raise AcquisitionFailure('invalid_probe_budget_receipt')
        companies = [row for row in receipt['responses']
                     if (row.get('operation_id') or '').startswith('voyagerOrganizationDashCompanies.')]
        if not any(publisher in row.get('representation', {}).get('source_native_ids', []) for row in companies):
            raise AcquisitionFailure('feed_publisher_not_observed')
        feeds = [row for row in receipt['responses'] if row['status'] == 200
                 and (row.get('operation_id') or '').startswith('voyagerFeedDashOrganizationalPageUpdates.')]
        if len(feeds) != 1:
            raise AcquisitionFailure('expected_exactly_one_feed_response')
        feed = feeds[0]
        representation = feed['representation']
        collection = representation.get('collection_projection', {})
        if collection.get('status') != 'captured' or collection.get('recipe') != RECIPE:
            raise AcquisitionFailure('exact_collection_not_captured')
        if representation.get('duplicate_entity_ids'):
            raise AcquisitionFailure('projected_graph_has_duplicate_entities')
        envelope = {'data': {'data': {RECIPE: collection['fields']}},
                    'included': representation['projected_graph'],
                    '_projection_provenance': {
                        'receipt': native_receipt['path'], 'native_body_sha256': feed['body_sha256'],
                        'limits': representation['projection_limits'],
                    }}
        body = json.dumps(envelope, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
        projection_digest = hashlib.sha256(body).hexdigest()
        return SourceResponse(feed['status'], feed['content_type'], body,
                              datetime.fromisoformat(feed['observed_at']))

    def deliver(wire: dict):
        with M1Client('http://127.0.0.1:1', 'local-unused-conversion-label', max_retries=0) as client:
            envelopes = [EvidenceEnvelope.model_validate(value) for value in client.envelopes(wire)]
        assert envelopes and wire['job_id'] == attempt
        batch = []
        for envelope in envelopes:
            assert envelope.raw_payload['m1_item']['source_fields']['evidence_class'] == 'allowlisted_source_projection'
            assert envelope.raw_payload['m1_item']['raw_evidence_ref'] is None
            admitted = store.admit(envelope)
            batch.append(admitted.duplicate_delivery)
            if envelope.evidence_id not in ids:
                ids.append(envelope.evidence_id)
        delivery_results.append(batch)

    try:
        first = execute_company_page(journal=journal, permission=permission,
                                     feed_publisher_id=publisher, attempt_id=attempt,
                                     send=send, deliver=deliver)
        if native_receipt and native_receipt['receipt']['stopped']:
            journal.stop('linkedin.company-feed', native_receipt['receipt']['stopped']['reason'])
        if first.delivery_outcome != 'delivery_acknowledged':
            return {'attempt_id': attempt, 'first': asdict(first), 'native_receipt': native_receipt['path'] if native_receipt else None,
                    'native_stop': native_receipt['receipt']['stopped'] if native_receipt else None,
                    'source_callback_calls': len(source_calls), 'delivered': sum(map(len, delivery_results)),
                    'status': 'stopped_or_failed_no_automatic_retry'}
        store.close()
        store = Store(f'sqlite:///{sink_path.as_posix()}')
        store.initialize()

        def never_send():
            raise AssertionError('Restart replay attempted another source read')

        replay = execute_company_page(journal=Journal(source_path), permission=permission,
                                      feed_publisher_id=publisher, attempt_id=attempt,
                                      send=never_send, deliver=deliver)
        assert replay.replayed and replay.delivery_outcome == 'delivery_acknowledged'
        assert len(source_calls) == 1 and len(delivery_results) == 2
        assert not any(delivery_results[0]) and all(delivery_results[1])
        assert all(len(store.list_observations(identifier)) == 1 for identifier in ids)
        receipt = native_receipt['receipt']
        return {'status': 'bounded_current_projection_integrated', 'attempt_id': attempt,
                'first': asdict(first), 'restart_replay': asdict(replay),
                'native_receipt': native_receipt['path'], 'native_reads': receipt['native_reads_admitted'],
                'source_callback_calls': len(source_calls), 'admitted': len(ids),
                'duplicate_redeliveries': sum(delivery_results[1]), 'observations_after_restart': len(ids),
                'projection_sha256': projection_digest, 'projection_expires_at': permission.raw_expires_at.isoformat(),
                'source_journal': str(source_path.relative_to(ROOT)), 'sink_database': str(sink_path.relative_to(ROOT)),
                'evidence_class': permission.evidence_class,
                'limits': ['Native original bodies are not retained; Journal digest hashes projection bytes',
                           'Browser navigation is a bounded callback, not a registered NOS queue/HTTP transport',
                           'Projection text/graph bounds and relevance order do not establish full source coverage',
                           'Journal projection expiry is enforced on access; derived M2 expiry remains open',
                           'No cadence, endurance, termination or remote canonical-fence claim']}
    finally:
        store.close()


if __name__ == '__main__':
    result = main()
    output = ROOT / 'docs/results' / f"{result['attempt_id']}-executor.json"
    with output.open('x', encoding='utf-8') as file:
        json.dump(result, file, indent=2)
        file.write('\n')
    print(json.dumps(result, indent=2))
