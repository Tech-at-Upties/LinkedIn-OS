"""Original deadline through real M1 and all four actual M2 ingress copies."""
from datetime import UTC, datetime, timedelta
import json

import pytest
from sqlalchemy import select, text

from test_nos_integration import convert, store, wire
from test_job_runtime import ledger, create, runtime, worker
from nos_m2.models import EvidenceEnvelope
from nos_m2.store import (
    DeliveryTable, EvidenceTable, EvidenceRetentionTable, ObservationTable,
    SourceVersionTable, Store, StoreError, WorkTable,
)


def retained(*, job='owned-retention', expires=None):
    capture = datetime.now(UTC) - timedelta(seconds=1)
    result = wire(observed=capture, job=job)
    item = result['items'][0]
    item['acquisition'] = {'root_job_id': job, 'source_attempt_id': 'li-page-test', 'page_ordinal': 0}
    item['retention'] = {'mode': 'ingress_only', 'source_attempt_id': 'li-page-test',
                         'captured_at': capture.isoformat(),
                         'expires_at': (expires or capture + timedelta(minutes=30)).isoformat()}
    return convert(result)[0]


def expire(monkeypatch, deadline):
    import nos_m2.retention as policy
    import nos_m2.store as persistence
    clock = lambda: deadline + timedelta(seconds=1)
    monkeypatch.setattr(policy, 'now_utc', clock)
    monkeypatch.setattr(persistence, 'now_utc', clock)


def test_expiry_erases_all_ingress_copies_and_never_becomes_source_deletion(store, monkeypatch):
    value = retained()
    ack = store.admit(value)
    assert store.admit(value).duplicate_delivery
    assert store.claim_work('worker') is None
    assert store.get_work(ack.work_item_id).status == 'retention_blocked'
    with pytest.raises(StoreError, match='derived'):
        store.put_record('analysis', 'derived', 1, {'copy': value.text}, dependencies=[f'{value.evidence_id}@{ack.source_version_id}'])
    expire(monkeypatch, value.retention.expires_at)
    assert store.get_evidence(value.evidence_id) is None
    assert store.get_evidence(value.evidence_id, ack.source_version_id) is None
    assert store.get_delivery(value.delivery_id) is None
    assert store.get_observation(ack.observation_id) is None
    assert store.list_evidence() == []
    assert store.list_source_versions(value.evidence_id) == []
    assert store.list_observations(value.evidence_id) == []
    assert store.list_observations_for_evidence([value.evidence_id]) == {value.evidence_id: []}
    assert store.get_work(ack.work_item_id).status == 'policy_expired'
    with store.Session() as session:
        for table in (EvidenceTable, SourceVersionTable, ObservationTable, DeliveryTable):
            rows = list(session.scalars(select(table)))
            assert len(rows) == 1
            tombstone = json.loads(rows[0].envelope)
            assert tombstone['retention_state'] == 'policy_expired'
            assert not set(tombstone) & {'text', 'raw_payload', 'metrics', 'author', 'availability', 'references', 'media'}
            assert value.text not in rows[0].envelope
        assert session.get(EvidenceRetentionTable, value.evidence_id).state == 'policy_expired'
        assert session.get(DeliveryTable, value.delivery_id).fingerprint
    assert store.purge_expired_evidence() == 0
    with pytest.raises(StoreError, match='expired'):
        store.admit(value)


def test_expired_first_admission_and_stripped_policy_leave_no_content(store):
    expired = retained(expires=datetime.now(UTC) - timedelta(milliseconds=100))
    with pytest.raises(StoreError, match='expired'):
        store.admit(expired)
    assert store.list_evidence() == []
    value = retained()
    store.admit(value)
    stripped = value.model_dump(mode='json')
    stripped['retention'] = None
    stripped['raw_payload'] = {}
    stripped['delivery_id'] += '-stripped'
    with pytest.raises(StoreError, match='stripped'):
        store.admit(stripped)


def test_redelivery_and_new_observation_cannot_extend_identity_deadline(store, monkeypatch):
    value = retained()
    first = store.admit(value)
    later = retained(job='later')
    store.admit(later)
    current = EvidenceEnvelope.model_validate(store.get_evidence(value.evidence_id))
    assert current.observed_at == later.observed_at
    assert current.retention == later.retention
    with store.Session() as session:
        expiry = store._aware(session.get(EvidenceRetentionTable, value.evidence_id).expires_at)
    assert expiry == value.retention.expires_at
    expire(monkeypatch, expiry)
    with pytest.raises(StoreError, match='expired'):
        store.admit(later)
    assert store.get_evidence(first.evidence_id) is None


def test_opaque_source_dependencies_and_nested_refs_cannot_create_copies(store):
    from nos_m2.intelligence import EvidenceRef
    value = retained()
    receipt = store.admit(value)
    ref = EvidenceRef(evidence_id=value.evidence_id, source_version_id=receipt.source_version_id)
    with pytest.raises(StoreError, match='derived'):
        store.put_record('analysis', 'opaque', 1, {'copy': value.text}, dependencies=[ref.key])
    with pytest.raises(StoreError, match='derived'):
        store.put_record('analysis', 'nested', 1, {'source': ref.model_dump(), 'copy': value.text})
    with pytest.raises(StoreError, match='derived'):
        store.index_content_hash('a' * 64, value.evidence_id, receipt.source_version_id, ['internal'])


def test_requeued_or_stale_work_cannot_analyze_restricted_ingress(store):
    from nos_m2.store import LeaseError
    value = retained()
    receipt = store.admit(value)
    with store.transaction() as session:
        work = session.get(WorkTable, receipt.work_item_id)
        work.status = 'pending'  # A stale reprocess command must not bypass the policy table.
    assert store.claim_work('stale-reprocessor') is None
    with store.transaction() as session:
        work = session.get(WorkTable, receipt.work_item_id)
        work.status = 'leased'
        work.lease_token = 'stale-token'
        work.lease_expires_at = datetime.now(UTC) + timedelta(minutes=1)
    with pytest.raises(LeaseError, match='retention'):
        store.complete_work(receipt.work_item_id, 'stale-token')


def test_rejected_owned_payload_quarantine_keeps_only_digest(store):
    value = retained()
    identity = store.quarantine(value.model_dump(mode='json'), 'bad input ' + value.text)
    row = store.get_record('quarantine', identity)
    assert value.text not in json.dumps(row)
    assert set(row['payload']['payload']) == {'source_type', 'rejected_input_sha256'}


def test_service_does_not_make_derivatives_or_call_provider(store):
    from nos_m2.service import M2Service
    from nos_m2.settings import Settings
    service = M2Service(Settings(provider='unavailable'), store=store)
    value = retained()
    receipt = service.admit(value)
    assert service.work_once() is None
    with store.Session() as session:
        assert session.execute(text('SELECT COUNT(*) FROM analytical_records')).scalar_one() == 0
        assert session.execute(text('SELECT COUNT(*) FROM stage_runs')).scalar_one() == 0
    assert store.get_evidence(receipt.evidence_id)['retention']['mode'] == 'ingress_only'


def test_push_pull_and_delivery_recovery_keep_exact_original_deadline(ledger, tmp_path, store):
    calls, wires = [], []
    def deliver(batch):
        wires.append(batch)
        for value in convert(batch):
            store.admit(value)
        if len(wires) == 1:
            raise TimeoutError('controlled commit timeout')
    selected = runtime(tmp_path, deliver, calls)
    task = create(ledger)
    assert worker(ledger, selected)._process_delivery(task.task_id).state.value == 'RETRY_SCHEDULED'
    ledger.enqueue_due_retries(now=(datetime.now(UTC) + timedelta(minutes=2)).isoformat())
    selected = runtime(tmp_path, deliver, calls)
    assert worker(ledger, selected)._process_delivery(task.task_id).state.value == 'DONE'
    pulled = selected.result_for_task(ledger.get_task(task.task_id))
    assert len(calls) == 1
    assert wires[0]['items'][0]['retention'] == wires[1]['items'][0]['retention'] == pulled['items'][0]['retention']
    assert store.admit(convert(pulled)[0]).duplicate_delivery


def test_reopening_preserves_expired_gate_and_purges_without_source_read(tmp_path, monkeypatch):
    uri = f"sqlite:///{tmp_path / 'retention.sqlite'}"
    first = Store(uri)
    first.initialize()
    value = retained()
    first.admit(value)
    first.close()
    expire(monkeypatch, value.retention.expires_at)
    reopened = Store(uri)
    reopened.initialize()
    try:
        assert reopened.get_evidence(value.evidence_id) is None
        with pytest.raises(StoreError, match='expired'):
            reopened.admit(value)
    finally:
        reopened.close()


@pytest.mark.parametrize('change', [
    lambda item: item.pop('retention'),
    lambda item: item['retention'].update(source_attempt_id='another-attempt'),
    lambda item: item['retention'].update(captured_at='2026-01-01T00:00:00+00:00'),
    lambda item: item['retention'].update(mode='analyze_forever'),
    lambda item: item['acquisition'].update(root_job_id='wrong-job'),
])
def test_wire_rejects_missing_or_inconsistent_policy(change):
    value = retained()
    item = value.raw_payload['m1_item']
    change(item)
    with pytest.raises(Exception):
        convert({'source_type': 'linkedin', 'job_id': value.origin.m1_job_id, 'items': [item]})


def test_audited_backfill_refuses_derivatives_and_wrong_job(store):
    from nos_m2.retention import backfill_expired_ingress
    legacy = convert(wire())[0]
    store.admit(legacy)
    expired = datetime.now(UTC) - timedelta(seconds=1)
    with pytest.raises(StoreError, match='provenance'):
        backfill_expired_ingress(store, evidence_ids=[legacy.evidence_id], expires_at=expired, expected_job_id='wrong-job')
    store.put_record('analysis', 'legacy-copy', 1, {'copy': legacy.text})
    with pytest.raises(StoreError, match='derived'):
        backfill_expired_ingress(store, evidence_ids=[legacy.evidence_id], expires_at=expired, expected_job_id=legacy.origin.m1_job_id)
    with pytest.raises(StoreError, match='clean ingress'):
        store.admit(retained())


def test_expired_rows_do_not_hide_later_eligible_pagination(store, monkeypatch):
    value = retained()
    store.admit(value)
    ordinary = convert(wire(job='legacy-other'))[0].model_dump(mode='json')
    ordinary.update(delivery_id='ordinary', evidence_id='zz-ordinary', source_item_id='ordinary',
                    source_type='x', raw_payload={}, origin={})
    store.admit(ordinary)
    expire(monkeypatch, value.retention.expires_at)
    assert [row['evidence_id'] for row in store.list_evidence(limit=1)] == ['zz-ordinary']


def test_postgres_admission_rechecks_deadline_after_waiting_for_policy_lock(store, monkeypatch):
    if store.engine.dialect.name != 'postgresql':
        return  # SQLite's BEGIN IMMEDIATE case is covered by actual expiry/reopening checks.
    from threading import Event, Thread, current_thread
    from nos_m2.retention import LOCK
    value = retained()
    waiting, errors = Event(), []
    original = store._lock_keys
    def locking(session, keys):
        if current_thread().name == 'retention-admission' and LOCK in keys:
            waiting.set()
        return original(session, keys)
    monkeypatch.setattr(store, '_lock_keys', locking)
    def admit():
        try:
            store.admit(value)
        except StoreError as error:
            errors.append(str(error))
    with store.transaction() as session:
        original(session, [LOCK])
        thread = Thread(target=admit, name='retention-admission')
        thread.start()
        assert waiting.wait(3)
        expire(monkeypatch, value.retention.expires_at)
    thread.join(5)
    assert not thread.is_alive() and len(errors) == 1 and 'expired' in errors[0]
    assert store.list_evidence() == []
