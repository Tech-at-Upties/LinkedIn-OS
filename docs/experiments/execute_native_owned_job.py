"""One bounded current browser capture -> actual Postgres job/M2Service.

Controlled queue bypass and retry-clock advance; no Redis/live startup claim.
"""
from datetime import UTC, datetime, timedelta
from hashlib import sha256
import json
from pathlib import Path
import subprocess
import sys
from time import monotonic
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
NOS = ROOT / '.local/nos-integration'
sys.path[:0] = [str(NOS / 'M1/src'), str(NOS / 'M2/src')]

import psycopg
from psycopg_pool import ConnectionPool
from nos_linkedin.acquisition import AcquisitionFailure, Journal, SourceResponse
from nos_linkedin.parser import RECIPE
from nos_m2.m1 import M1Client
from nos_m2.models import EvidenceEnvelope
from nos_m2.service import M2Service
from nos_m2.settings import Settings
from xingestion.capabilities import CapabilityPlanner, CapabilityRequest, LinkedInCompanyFeedInput
from xingestion.capability_dispatch import queue_capability_request
from xingestion.capability_ids import CapabilityId
from xingestion.linkedin.runtime import LinkedInJobRuntime
from xingestion.migrations import PostgresMigrationRunner
from xingestion.tasks import PostgresTaskLedger, TaskState
from xingestion.workers import LocalWorker

COMPANY = 'urn:li:fsd_company:1337'
TARGET = 'https://www.linkedin.com/company/linkedin/posts/'
TASK_DSN = 'postgresql://nos_test@127.0.0.1:15432/nos_linkedin_owned_jobs'


def open_ledger():
    with psycopg.connect('postgresql://nos_test@127.0.0.1:15432/postgres', autocommit=True) as connection:
        if Path(connection.execute('SHOW data_directory').fetchone()[0]).resolve() != (ROOT / '.local/verification-postgres').resolve():
            raise RuntimeError('unexpected_verification_cluster')
        if not connection.execute("SELECT 1 FROM pg_database WHERE datname='nos_linkedin_owned_jobs'").fetchone():
            connection.execute('CREATE DATABASE nos_linkedin_owned_jobs')
    pool = ConnectionPool(TASK_DSN, min_size=1, max_size=5, timeout=3)
    pool.wait(5)
    PostgresMigrationRunner(pool, NOS / 'M1/src/xingestion/migrations/postgres_sql').apply()
    return PostgresTaskLedger(pool)


def main():
    experiment = 'native-job-' + uuid4().hex
    journal_path = ROOT / '.local' / (experiment + '-source.sqlite')
    sink_path = ROOT / '.local' / (experiment + '-m2.sqlite')
    sink_settings = Settings(provider='unavailable', database_url='sqlite:///' + sink_path.as_posix())
    ledger = open_ledger()
    service = M2Service(sink_settings)
    journal = Journal(journal_path)
    calls, batches = [], []
    receipt = None
    receipt_path = None
    started = monotonic()

    def capture(payload, lease):
        nonlocal receipt, receipt_path
        if payload.company_url != TARGET or payload.feed_publisher_id != COMPANY or lease.start != 3 or lease.count != 10:
            raise AcquisitionFailure('native_experiment_scope_conflict')
        calls.append(lease.attempt_id)
        process = subprocess.run(['node', str(ROOT / 'scripts/probe-linkedin-company.cjs'), TARGET],
                                 cwd=ROOT, capture_output=True, text=True, timeout=85)
        # No stderr, credentials or native bodies leave the bounded probe.
        if process.stdout.strip():
            report = json.loads(process.stdout.strip())
            receipt_path = (ROOT / report['result']).resolve()
            if not receipt_path.is_relative_to(ROOT / 'docs/results'):
                raise AcquisitionFailure('invalid_probe_receipt_path')
            receipt = json.loads(receipt_path.read_text(encoding='utf-8'))
        if (process.returncode or receipt is None or receipt['stopped'] or not receipt['session_verified']
                or receipt.get('failure_kind') or not 0 < receipt['native_reads_admitted'] <= 12):
            raise AcquisitionFailure('native_probe_stopped_or_failed')
        companies = [row for row in receipt['responses'] if (row.get('operation_id') or '').startswith('voyagerOrganizationDashCompanies.')]
        if not any(COMPANY in row.get('representation', {}).get('source_native_ids', []) for row in companies):
            raise AcquisitionFailure('native_company_identity_unproved')
        feeds = [row for row in receipt['responses'] if row['status'] == 200
                 and (row.get('operation_id') or '').startswith('voyagerFeedDashOrganizationalPageUpdates.')]
        if len(feeds) != 1:
            raise AcquisitionFailure('expected_exactly_one_feed_response')
        feed = feeds[0]
        representation = feed['representation']
        collection = representation.get('collection_projection', {})
        if collection.get('status') != 'captured' or collection.get('recipe') != RECIPE or representation.get('duplicate_entity_ids'):
            raise AcquisitionFailure('exact_projection_unproved')
        body = json.dumps({'data': {'data': {RECIPE: collection['fields']}},
                           'included': representation['projected_graph']},
                          sort_keys=True, allow_nan=False).encode()
        return SourceResponse(200, feed['content_type'], body, datetime.fromisoformat(feed['observed_at']))

    def admit(wire):
        with M1Client('http://127.0.0.1:1', 'local-unused-conversion-label', max_retries=0) as client:
            values = [EvidenceEnvelope.model_validate(value) for value in client.envelopes(wire)]
        batch = [service.admit(value).duplicate_delivery for value in values]
        batches.append(batch)
        if len(batches) == 1:
            raise TimeoutError('controlled interruption after actual M2 admission')

    runtime = LinkedInJobRuntime(journal=journal, send=capture, deliver=admit,
                                evidence_class='allowlisted_source_projection')
    task = queue_capability_request(ledger=ledger, planner=CapabilityPlanner(None),
        capability_request=CapabilityRequest(CapabilityId.LINKEDIN_COMPANY_FEED, 1,
                                             LinkedInCompanyFeedInput(TARGET, COMPANY)),
        idempotency_key='northbound:local-native-job:' + experiment,
        max_active_tasks_per_capability=2).task
    if task is None:
        raise RuntimeError('native_job_backpressure')
    def execute(selected):
        return LocalWorker(ledger=ledger, manifest=None, auth=None, transport=None, raw_evidence_sink=None,
                           linkedin_runtime=selected, lease_seconds=300)._process_delivery(task.task_id)
    result = {'experiment_id': experiment, 'root_job_id': task.task_id, 'target': TARGET,
              'source_journal': str(journal_path.relative_to(ROOT)), 'sink_database': str(sink_path.relative_to(ROOT)),
              'evidence_class': 'allowlisted_source_projection', 'task_database': 'nos_linkedin_owned_jobs'}
    try:
        initial = execute(runtime)
        result['first_task_state'] = initial.state.value
        if receipt and receipt['stopped']:
            observed = receipt['stopped']['reason']
            reason = ('authentication_required' if observed == 'authentication_required' else
                      'challenge' if observed == 'challenge_path' else
                      'restricted' if observed.startswith('restricted_http_') else 'operator_stop')
            journal.stop('linkedin.company-feed', reason)
        if initial.state == TaskState.RETRY_SCHEDULED:
            service.close()
            ledger.pool.close()
            service = M2Service(sink_settings)
            ledger = open_ledger()
            resumed = LinkedInJobRuntime(journal=Journal(journal_path), send=capture, deliver=admit,
                                        evidence_class='allowlisted_source_projection')
            count = ledger.enqueue_due_retries(now=(datetime.now(UTC) + timedelta(seconds=31)).isoformat())
            if count < 1:
                raise AssertionError('retry task was not durably re-enqueued')
            recovered = execute(resumed)
            if recovered.state != TaskState.DONE or len(calls) != 1 or len(batches) != 2 or any(batches[0]) or not all(batches[1]):
                raise AssertionError('native job recovery contract failed')
            task_result = resumed.result_for_task(ledger.get_task(task.task_id))
            result.update(status='current_projected_job_integrated_and_recovered',
                recovered_task_state=recovered.state.value, admitted=len(task_result['items']),
                restart_duplicates=sum(batches[1]), coverage=task_result['coverage'],
                projection_expires_at=task_result['expires_at'])
        else:
            result['status'] = 'stopped_or_failed_no_native_retry'
        if receipt:
            feed = next((row for row in receipt['responses'] if (row.get('operation_id') or '').startswith('voyagerFeedDashOrganizationalPageUpdates.')), None)
            result.update(native_receipt=str(receipt_path.relative_to(ROOT)), native_reads=receipt['native_reads_admitted'],
                          session_verified=receipt['session_verified'], native_stop=receipt['stopped'],
                          native_body_sha256=feed['body_sha256'] if feed else None)
        with journal.connect() as connection:
            records = connection.execute('SELECT attempt_id,body_sha256,outcome FROM responses').fetchall()
            result['source_attempts'] = [dict(record) for record in records]
        result.update(source_callback_calls=len(calls), delivery_attempts=len(batches), elapsed_seconds=round(monotonic()-started, 3),
            limits=['Controlled direct queue delivery and retry-clock advance; no Redis claim',
                    'One current first page; no continuation, termination, cadence or endurance claim',
                    'Allowlisted projection retained; native body digest is distinct',
                    'M2Service admission is actual; this experiment uses a local callback, not remote canonical fencing',
                    'Derived M2 retention remains open'])
        return result
    finally:
        service.close()
        ledger.pool.close()


if __name__ == '__main__':
    result = main()
    output = ROOT / 'docs/results' / (result['experiment_id'] + '.json')
    with output.open('x', encoding='utf-8') as file:
        json.dump(result, file, indent=2)
        file.write('\n')
    print(json.dumps(result, indent=2))
