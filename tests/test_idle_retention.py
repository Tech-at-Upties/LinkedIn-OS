"""Idle application expiry on actual SQLite/PostgreSQL; no source requests."""
from datetime import UTC, datetime, timedelta
import json
import time
from threading import Barrier, Thread

import pytest
from sqlalchemy import event, inspect, select

from fastapi.testclient import TestClient
from pydantic import SecretStr

from test_nos_integration import store
from test_owned_retention import retained, expire
from test_nos_integration import wire, convert
from nos_m2.api import create_app
from nos_m2.service import M2Service
from nos_m2.settings import Settings
from nos_m2.models import EvidenceEnvelope
from nos_m2.store import EvidenceRetentionTable, EvidenceTable, DeliveryTable, ObservationTable, SourceVersionTable
from nos_m2.retention_maintenance import RetentionMaintenance


def test_idle_api_erases_expired_bytes_without_any_api_request(store, tmp_path):
    settings = Settings(provider='unavailable', api_token=SecretStr('local-http-test-token'),
        api_request_log_path=str(tmp_path / 'metadata.jsonl')).model_copy(update={'retention_maintenance_poll_seconds':1})
    service = M2Service(settings, store=store)
    value = retained(expires=datetime.now(UTC) + timedelta(seconds=2))
    store.admit(value)
    try:
        with TestClient(create_app(service=service, settings=settings)):
            deadline = time.monotonic() + 4
            while time.monotonic() < deadline:
                # Inspect SQL directly; no guarded/public Store read triggers.
                with store.Session() as session:
                    if session.get(EvidenceRetentionTable, value.evidence_id).state == 'policy_expired':
                        assert value.text not in session.get(EvidenceTable, value.evidence_id).envelope
                        break
                time.sleep(.05)
            else:
                raise AssertionError('Idle API retained expired publication bytes')
    finally:
        service.close()


def raw_policies(store):
    with store.Session() as session:
        return {row.evidence_id: row.state for row in session.scalars(select(EvidenceRetentionTable))}


def test_bounded_batches_preserve_unrestricted_data_and_restart(store, monkeypatch):
    values = []
    for index in range(3):
        raw = retained(job='policy-' + str(index)).model_dump(mode='json')
        raw['evidence_id'] += '-' + str(index)
        values.append(EvidenceEnvelope.model_validate(raw))
        store.admit(values[-1])
    unrestricted = convert(wire())[0]
    store.admit(unrestricted)
    expire(monkeypatch, max(value.retention.expires_at for value in values))
    maintenance = RetentionMaintenance(store, batch_limit=1)
    assert maintenance.sweep() == 1
    assert list(raw_policies(store).values()).count('policy_expired') == 1
    assert RetentionMaintenance(store, batch_limit=1).sweep() == 1
    assert maintenance.sweep() == 1 and maintenance.sweep() == 0
    with store.Session() as session:
        assert unrestricted.text in session.get(EvidenceTable, unrestricted.evidence_id).envelope
        for table in (EvidenceTable, SourceVersionTable, ObservationTable, DeliveryTable):
            for row in session.scalars(select(table).where(table.evidence_id != unrestricted.evidence_id)):
                assert 'retention_state' in row.envelope and 'text' not in json.loads(row.envelope)


def test_concurrent_maintenance_erases_each_identity_once(store, monkeypatch):
    value = retained(); store.admit(value); expire(monkeypatch, value.retention.expires_at)
    barrier = Barrier(2); results = []
    def sweep():
        barrier.wait(3)
        results.append(RetentionMaintenance(store).sweep())
    threads = [Thread(target=sweep), Thread(target=sweep)]
    for thread in threads: thread.start()
    for thread in threads: thread.join(5)
    assert not any(thread.is_alive() for thread in threads) and sorted(results) == [0, 1]


def test_failed_sweep_rolls_back_every_copy_and_reopened_driver_recovers(store, monkeypatch):
    value = retained(); store.admit(value); expire(monkeypatch, value.retention.expires_at)
    def reject(connection):
        raise RuntimeError('controlled_commit_failure_no_sensitive_payload')
    event.listen(store.engine, 'commit', reject)
    selected = RetentionMaintenance(store)
    try:
        with pytest.raises(RuntimeError): selected.start()
        assert selected.status()['error_kind'] == 'RuntimeError' and not selected.status()['running']
    finally:
        event.remove(store.engine, 'commit', reject)
    assert raw_policies(store)[value.evidence_id] == 'ingress_only'
    with store.Session() as session:
        for table in (EvidenceTable, SourceVersionTable, ObservationTable, DeliveryTable):
            assert value.text in session.scalars(select(table)).first().envelope
    restarted = RetentionMaintenance(store)
    restarted.start()
    try:
        assert restarted.status()['purged_identities'] == 1 and restarted.status()['error_kind'] is None
    finally:
        restarted.close()
    assert not restarted.status()['running']


@pytest.mark.parametrize('arguments', [{'poll_seconds':0}, {'poll_seconds':float('nan')}, {'poll_seconds':True},
                                     {'batch_limit':True}, {'batch_limit':101}, {'batch_limit':0}])
def test_invalid_maintenance_bounds_rejected_before_sql(store, arguments):
    with pytest.raises(ValueError): RetentionMaintenance(store, **arguments)


def test_api_exposes_failure_and_rejects_owned_ingest_until_recovery(store, tmp_path):
    settings = Settings(provider='unavailable', api_token=SecretStr('local-http-test-token'),
        api_request_log_path=str(tmp_path / 'api.jsonl'), retention_maintenance_poll_seconds=1)
    service = M2Service(settings, store=store)
    app = create_app(service=service, settings=settings)
    try:
        with TestClient(app) as client:
            selected = app.state.retention_maintenance
            original = store.purge_expired_evidence
            def fail(**kwargs): raise RuntimeError('controlled_storage_failure')
            store.purge_expired_evidence = fail
            try:
                with pytest.raises(RuntimeError): selected.sweep()
                headers = {'Authorization':'Bearer local-http-test-token'}
                assert client.get('/v1/health', headers=headers).json()['retention_maintenance']['error_kind'] == 'RuntimeError'
                assert client.post('/v1/linkedin/results', headers=headers, json=wire()).status_code == 503
            finally:
                store.purge_expired_evidence = original
            selected.sweep()
            assert selected.status()['error_kind'] is None
        assert not selected.status()['running']
    finally:
        service.close()


def test_cli_worker_erases_expired_bytes_while_semantic_work_blocks(store, tmp_path, monkeypatch):
    import nos_m2.cli as cli
    import nos_m2.service as module
    settings = Settings(provider='unavailable', retention_maintenance_poll_seconds=1)
    service = M2Service(settings, store=store)
    value = retained(expires=datetime.now(UTC) + timedelta(seconds=2)); store.admit(value)
    monkeypatch.setattr(cli, 'load_settings', lambda *args, **kwargs: settings)
    monkeypatch.setattr(module, 'M2Service', lambda settings: service)
    def blocked_work(*args, **kwargs):
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            if raw_policies(store)[value.evidence_id] == 'policy_expired':
                raise KeyboardInterrupt()
            time.sleep(.05)
        raise AssertionError('Semantic work blocked idle expiry')
    monkeypatch.setattr(service, 'work', blocked_work)
    assert cli.main(['work', '--loop', '--no-monitor-sync', '--no-rss-sync']) == 130
    from threading import enumerate as live_threads
    assert not any(thread.name == 'nos-m2-retention-maintenance' for thread in live_threads())


def test_existing_database_gets_due_index_without_changing_evidence(store):
    value = retained(); store.admit(value)
    with store.engine.begin() as connection:
        connection.exec_driver_sql('DROP INDEX ix_owned_retention_due')
    assert 'ix_owned_retention_due' not in {row['name'] for row in inspect(store.engine).get_indexes('owned_evidence_retention')}
    store.initialize()
    assert 'ix_owned_retention_due' in {row['name'] for row in inspect(store.engine).get_indexes('owned_evidence_retention')}
    with store.Session() as session:
        assert value.text in session.get(EvidenceTable, value.evidence_id).envelope
