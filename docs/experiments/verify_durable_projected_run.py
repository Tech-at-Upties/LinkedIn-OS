"""Saved exact native projection -> leased M1 page -> M2; zero network reads."""
import argparse
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
NOS = ROOT / '.local/nos-integration'
sys.path[:0] = [str(NOS / 'M1/src'), str(NOS / 'M2/src')]

from nos_linkedin.acquisition import Journal, RoutePermission, SourceResponse
from nos_linkedin.pages import CompanyPageRuns
from nos_linkedin.parser import RECIPE
from nos_m2.m1 import M1Client
from nos_m2.models import EvidenceEnvelope
from nos_m2.store import Store
from xingestion.linkedin.pages import execute_leased_company_page


def verify(path):
    receipt = json.loads(path.read_text(encoding='utf-8'))
    assert receipt['evidence_class'] == 'allowlisted_source_projection'
    feed = next(row for row in receipt['responses'] if row['status'] == 200
                and (row.get('operation_id') or '').startswith('voyagerFeedDashOrganizationalPageUpdates.'))
    projection = feed['representation']['collection_projection']
    assert projection['status'] == 'captured' and projection['recipe'] == RECIPE
    body = json.dumps({'data': {'data': {RECIPE: projection['fields']}},
                       'included': feed['representation']['projected_graph']},
                      sort_keys=True, allow_nan=False).encode()
    assert projection['fields']['paging']['start'] == 3 and projection['fields']['paging']['count'] == 10
    run_id = f'projected-run-{uuid4().hex}'
    journal_path = ROOT / '.local' / f'{run_id}-source.sqlite'
    uri = f'sqlite:///{(ROOT / ".local" / f"{run_id}-m2.sqlite").as_posix()}'
    journal = Journal(journal_path)
    runs = CompanyPageRuns(journal)
    now = datetime.now(UTC)
    permission = RoutePermission(True, True, now + timedelta(hours=1), receipt['evidence_class'])
    runs.create(run_id=run_id, feed_publisher_id='urn:li:fsd_company:1337', evidence_class=receipt['evidence_class'],
                first_start=3, count=10, page_budget=1)
    lease = runs.claim(run_id, owner='initial', now=now)
    database = Store(uri)
    database.initialize()
    source_calls, deliveries, identifiers = [], [], []
    def send(selected):
        source_calls.append(selected.attempt_id)
        return SourceResponse(200, feed['content_type'], body, datetime.fromisoformat(feed['observed_at']))
    def deliver(wire):
        with M1Client('http://127.0.0.1:1', 'local-unused-conversion-label', max_retries=0) as client:
            envelopes = [EvidenceEnvelope.model_validate(value) for value in client.envelopes(wire)]
        assert len(envelopes) == 10
        batch = []
        for envelope in envelopes:
            assert envelope.raw_payload['m1_item']['source_fields']['evidence_class'] == receipt['evidence_class']
            if envelope.evidence_id not in identifiers:
                identifiers.append(envelope.evidence_id)
            batch.append(database.admit(envelope).duplicate_delivery)
        deliveries.append(batch)
    def interrupted(*args):
        raise InterruptedError('controlled interruption after durable admission acknowledgement')
    runs.checkpoint = interrupted
    try:
        try:
            execute_leased_company_page(runs=runs, lease=lease, permission=permission,
                                        send=send, deliver=deliver, clock=lambda: now)
        except InterruptedError:
            pass
        else:
            raise AssertionError('Expected controlled interruption')
        database.close()
        database = Store(uri)
        database.initialize()
        recovery = CompanyPageRuns(Journal(journal_path))
        later = now + timedelta(seconds=61)
        replacement = recovery.claim(run_id, owner='recovery', now=later)
        assert replacement.attempt_id == lease.attempt_id
        result = execute_leased_company_page(runs=recovery, lease=replacement, permission=permission,
                                            send=send, deliver=deliver, clock=lambda: later)
        assert result.replayed and len(source_calls) == 1
        assert not any(deliveries[0]) and all(deliveries[1])
        assert all(len(database.list_observations(identifier)) == 1 for identifier in identifiers)
        summary = recovery.summary(run_id)
        assert summary['state'] == 'budget_exhausted' and summary['source_complete'] is None
        return {'run_id': run_id, 'receipt': str(path.relative_to(ROOT)), 'network_requests': 0,
                'selected_projected_publications': len(identifiers), 'source_callback_calls': len(source_calls),
                'restart_duplicates': sum(deliveries[1]), 'summary': summary,
                'native_body_sha256': feed['body_sha256'], 'projection_sha256': result.body_sha256,
                'evidence_class': receipt['evidence_class'],
                'limits': ['Controlled clock advance and interruption with saved current projection',
                           'Native raw body is not retained', 'Not generic NOS task/HTTP execution or sustained operation']}
    finally:
        database.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('receipt', type=Path)
    args = parser.parse_args()
    result = verify(args.receipt.resolve())
    output = ROOT / 'docs/results' / f"{result['run_id']}.json"
    with output.open('x', encoding='utf-8') as file:
        json.dump(result, file, indent=2)
        file.write('\n')
    print(json.dumps(result, indent=2))
