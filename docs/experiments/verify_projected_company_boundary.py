"""Replay one private native projection through actual M1/M2; no network reads."""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import replace
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
NOS = ROOT / '.local/nos-integration'
sys.path[:0] = [str(NOS / 'M1/src'), str(NOS / 'M2/src')]

from nos_linkedin.parser import UPDATE, parse_graph
from nos_m2.m1 import M1Client
from nos_m2.models import EvidenceEnvelope
from nos_m2.store import Store
from xingestion.linkedin.northbound import serialize_company_page


def verify(receipt_path: Path, publisher: str) -> dict:
    receipt = json.loads(receipt_path.read_text(encoding='utf-8'))
    assert receipt['evidence_class'] == 'allowlisted_source_projection'
    assert receipt['session_verified'] is True and receipt['stopped'] is None
    companies = [row for row in receipt['responses']
                 if (row.get('operation_id') or '').startswith('voyagerOrganizationDashCompanies.')]
    assert any(publisher in row.get('representation', {}).get('source_native_ids', []) for row in companies)
    feeds = [row for row in receipt['responses'] if row['status'] == 200
             and (row.get('operation_id') or '').startswith('voyagerFeedDashOrganizationalPageUpdates.')]
    assert feeds
    result_id = datetime.now().astimezone().strftime('%Y%m%dT%H%M%S%f')
    destination = ROOT / '.local' / f'projected-company-boundary-{result_id}.sqlite'
    uri = f'sqlite:///{destination.as_posix()}'
    database = Store(uri)
    envelopes = []
    page_results = []
    try:
        database.initialize()
        # envelopes() performs local conversion; it makes no HTTP request.
        with M1Client('http://127.0.0.1:1', 'local-unused-conversion-label', max_retries=0) as client:
            for index, row in enumerate(feeds):
                representation = row['representation']
                assert not representation.get('duplicate_entity_ids')
                graph = representation['projected_graph']
                roots = [node['entityUrn'] for node in graph if node.get('$type') == UPDATE]
                page = parse_graph(graph, roots, feed_publisher_id=publisher,
                                   observed_at=datetime.fromisoformat(row['observed_at']),
                                   evidence_class=receipt['evidence_class'])
                # The older probe records included Update entities, not the collection's
                # explicit root list. Do not silently upgrade them to full feed coverage.
                page = replace(page, limitations=page.limitations + (
                    'Selected included Update entities; exact collection membership/order not retained',
                    'Allowlisted projection has depth/string/array bounds; omitted fields remain unknown',
                ))
                wire = serialize_company_page(page, job_id=f'{receipt_path.stem}:projection:{index}')
                converted = [EvidenceEnvelope.model_validate(value) for value in client.envelopes(wire)]
                for publication, envelope in zip(page.publications, converted, strict=True):
                    assert envelope.evidence_id == f'linkedin:{publication.occurrence_id}'
                    assert envelope.published_at is None
                    assert envelope.coverage.state == 'partial'
                    assert envelope.raw_payload['m1_item']['raw_evidence_ref'] is None
                    assert envelope.raw_payload['m1_item']['source_fields']['evidence_class'] == receipt['evidence_class']
                    assert envelope.raw_payload['m1_item']['metrics_target_id'] == publication.metrics.target_id
                    assert envelope.author.account_id == publication.actor_id.value
                    admitted = database.admit(envelope)
                    assert not admitted.duplicate_delivery
                    assert database.admit(envelope).duplicate_delivery
                envelopes.extend(converted)
                paging = [value.get('projection') for value in representation.get('continuation_candidates', [])
                          if value['path'].endswith('.paging')]
                page_results.append({'selected_update_entities': len(roots), 'admitted': len(converted),
                                     'issues': [issue.code for issue in page.issues], 'paging': paging})
    finally:
        database.close()
    reopened = Store(uri)
    try:
        reopened.initialize()
        for envelope in envelopes:
            assert reopened.admit(envelope).duplicate_delivery
            assert len(reopened.list_observations(envelope.evidence_id)) == 1
            persisted = reopened.get_evidence(envelope.evidence_id)
            assert persisted['source_type'] == 'linkedin'
            assert persisted['origin']['m1_job_id'] == envelope.origin.m1_job_id
    finally:
        reopened.close()
    return {'receipt': str(receipt_path.relative_to(ROOT)), 'evidence_class': receipt['evidence_class'],
            'network_requests': 0, 'database': str(destination.relative_to(ROOT)),
            'pages': page_results, 'admitted': len(envelopes), 'duplicate_redeliveries': len(envelopes),
            'restart_redeliveries': len(envelopes), 'observations_after_restart': len(envelopes),
            'scope': 'Actual M1 serializer/M2 conversion/SQLite replay of bounded source projection',
            'limits': ['Not live M1 acquisition or task/HTTP registration',
                       'Collection root membership/order absent in this receipt',
                       'Projection bounds do not prove complete text/graph fidelity']}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('receipt', type=Path)
    parser.add_argument('--publisher', required=True)
    args = parser.parse_args()
    result = verify(args.receipt.resolve(), args.publisher)
    output = ROOT / 'docs/results' / f'{args.receipt.stem}-m1-m2.json'
    with output.open('x', encoding='utf-8') as file:
        json.dump(result, file, indent=2)
        file.write('\n')
    print(json.dumps(result, indent=2))
