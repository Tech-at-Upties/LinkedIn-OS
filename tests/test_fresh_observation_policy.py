"""Fresh synthetic observations preserve expired capture tombstones."""
from datetime import UTC, datetime, timedelta
import json
from threading import Barrier, Thread

import pytest
from sqlalchemy import delete, inspect, select, text

from test_nos_integration import convert, store, wire
from nos_m2.store import (
    DeliveryTable, EvidenceRetentionTable, EvidenceTable, ObservationTable,
    SourceVersionTable, StoreError,
    CaptureCopyTable, CaptureMemberTable, CapturePolicyTable, CaptureMigrationTable,
    Store,
)


AUTHORITY = "wr060-synthetic-source-authority"
POLICY_ERROR = "expired"


def clock(monkeypatch, instant):
    monkeypatch.setattr("nos_m2.retention.now_utc", lambda: instant)
    monkeypatch.setattr("nos_m2.store.now_utc", lambda: instant)


def observation(*, job, attempt, captured, expires, occurrence="urn:li:activity:123"):
    result = wire(observed=captured, job=job)
    item = result["items"][0]
    item["item_id"] = occurrence
    item["source_fields"]["occurrence_id"] = occurrence
    item["acquisition"] = {
        "root_job_id": job, "source_attempt_id": attempt, "page_ordinal": 0,
    }
    item["retention"] = {
        "mode": "ingress_only", "source_attempt_id": attempt,
        "captured_at": captured.isoformat(), "expires_at": expires.isoformat(),
    }
    item["source_fence"] = {
        "authority_id": AUTHORITY, "generation": 0, "stopped": False,
    }
    item["task_fence"] = {
        "authority_id": AUTHORITY, "job_id": job,
        "generation": 1, "cancelled": False,
    }
    return convert(result)[0]


def register(database, value):
    assert database.update_source_fence(value.source_fence.model_dump()) == value.source_fence.model_dump()
    assert database.update_task_fence(value.task_fence.model_dump()) == value.task_fence.model_dump()


def assert_old_tombstones(database, old, receipt, *, current=True):
    with database.Session() as session:
        policy = session.get(EvidenceRetentionTable, old.evidence_id)
        assert policy.state == "policy_expired"
        assert database._aware(policy.expires_at) == old.retention.expires_at
        rows = [
            session.get(SourceVersionTable, (old.evidence_id, receipt.source_version_id)),
            session.get(ObservationTable, receipt.observation_id),
            session.get(DeliveryTable, old.delivery_id),
        ]
        if current:
            rows.append(session.get(EvidenceTable, old.evidence_id))
        for row in rows:
            body = json.loads(row.envelope)
            assert body["retention_state"] == "policy_expired"
            assert body["retention"]["source_attempt_id"] == old.retention.source_attempt_id
            assert datetime.fromisoformat(body["retention"]["expires_at"]) == old.retention.expires_at
            assert not set(body) & {"text", "author", "metrics", "raw_payload"}
            assert old.text not in row.envelope


def expired_then_fresh(database, monkeypatch):
    initial_now = datetime.now(UTC)
    old_deadline = initial_now + timedelta(seconds=10)
    clock(monkeypatch, initial_now)
    old = observation(job="wr060-old-job", attempt="wr060-old-attempt",
                      captured=initial_now - timedelta(seconds=1), expires=old_deadline)
    register(database, old)
    original = database.admit(old)
    assert database.admit(old).duplicate_delivery
    assert database.get_evidence(old.evidence_id)["text"] == old.text

    fresh_now = old_deadline + timedelta(seconds=2)
    clock(monkeypatch, fresh_now)
    assert database.purge_expired_evidence() == 1
    assert database.get_evidence(old.evidence_id) is None
    assert_old_tombstones(database, old, original)
    with pytest.raises(StoreError, match=POLICY_ERROR):
        database.admit(old)

    fresh = observation(job="wr060-fresh-job", attempt="wr060-fresh-attempt",
                        captured=fresh_now - timedelta(seconds=1),
                        expires=fresh_now + timedelta(minutes=20))
    register(database, fresh)
    assert fresh.evidence_id == old.evidence_id
    assert fresh.delivery_id != old.delivery_id
    assert fresh.retention.source_attempt_id != old.retention.source_attempt_id
    assert old_deadline < fresh.retention.captured_at < fresh_now < fresh.retention.expires_at
    assert fresh.task_fence.job_id != old.task_fence.job_id
    assert fresh.source_fence == old.source_fence

    control = observation(job="wr060-control-job", attempt="wr060-control-attempt",
                          captured=fresh.retention.captured_at, expires=fresh.retention.expires_at,
                          occurrence="urn:li:activity:124")
    register(database, control)
    admitted = database.admit(control)
    assert not admitted.duplicate_delivery
    assert database.get_evidence(control.evidence_id)["text"] == control.text
    assert_old_tombstones(database, old, original)
    return old, original, fresh


def test_expired_old_attempt_rejected_and_fresh_identity_control_admitted(store, monkeypatch):
    old, receipt, fresh = expired_then_fresh(store, monkeypatch)
    store.admit(fresh)
    assert_old_tombstones(store, old, receipt, current=False)
    with store.Session() as session:
        assert session.get(DeliveryTable, fresh.delivery_id) is not None
        assert len(list(session.scalars(select(ObservationTable).where(
            ObservationTable.evidence_id == old.evidence_id)))) == 2


def test_changed_deadline_cannot_revive_the_expired_old_source_attempt(store, monkeypatch):
    old, receipt, fresh = expired_then_fresh(store, monkeypatch)
    item = json.loads(json.dumps(old.raw_payload["m1_item"]))
    item["retention"]["expires_at"] = fresh.retention.expires_at.isoformat()
    extended = convert({"source_type": "linkedin", "job_id": old.origin.m1_job_id, "items": [item]})[0]
    assert extended.retention.source_attempt_id == old.retention.source_attempt_id
    assert extended.retention.captured_at == old.retention.captured_at
    assert extended.delivery_id != old.delivery_id
    with pytest.raises(StoreError):
        store.admit(extended)
    assert_old_tombstones(store, old, receipt)


def test_fresh_same_native_observation_can_admit_without_restoring_old_capture(store, monkeypatch):
    old, original, fresh = expired_then_fresh(store, monkeypatch)
    receipt = store.admit(fresh)
    assert receipt.evidence_id == original.evidence_id
    assert receipt.observation_id != original.observation_id
    assert not receipt.duplicate_delivery
    assert store.get_evidence(fresh.evidence_id)["retention"]["source_attempt_id"] == fresh.retention.source_attempt_id
    assert store.get_observation(original.observation_id) is None
    assert store.get_delivery(old.delivery_id) is None
    assert receipt.source_version_id == original.source_version_id
    assert_old_tombstones(store, old, original, current=False)
    assert store.get_evidence(fresh.evidence_id, receipt.source_version_id)['text'] == fresh.text
    with pytest.raises(StoreError):
        store.admit(old)


@pytest.mark.parametrize("stop", ["source", "task"])
def test_fresh_identity_control_does_not_bypass_source_or_task_hold(store, monkeypatch, stop):
    instant = datetime.now(UTC)
    clock(monkeypatch, instant)
    value = observation(job="wr060-fenced-job", attempt="wr060-fenced-attempt",
                        captured=instant - timedelta(seconds=1), expires=instant + timedelta(minutes=20))
    register(store, value)
    if stop == "source":
        store.update_source_fence({"authority_id": AUTHORITY, "generation": 1, "stopped": True})
        message = "source generation is unknown, stopped or stale"
    else:
        store.update_task_fence({**value.task_fence.model_dump(), "cancelled": True})
        message = "task generation is missing, unknown, cancelled or stale"
    with pytest.raises(StoreError, match=message):
        store.admit(value)
    assert store.list_evidence() == []


@pytest.mark.parametrize('newer_shorter', [False, True])
def test_independent_capture_expiry_and_eligible_current_fallback(store, monkeypatch, newer_shorter):
    start = datetime.now(UTC)
    clock(monkeypatch, start)
    first = observation(job='wr066-first', attempt='wr066-first-attempt',
        captured=start-timedelta(seconds=1), expires=start+timedelta(seconds=60 if newer_shorter else 10))
    register(store, first)
    first_receipt = store.admit(first)
    clock(monkeypatch, start+timedelta(seconds=2))
    second = observation(job='wr066-second', attempt='wr066-second-attempt',
        captured=start+timedelta(seconds=1), expires=start+timedelta(seconds=10 if newer_shorter else 60))
    register(store, second)
    second_receipt = store.admit(second)
    assert first_receipt.source_version_id == second_receipt.source_version_id
    clock(monkeypatch, start+timedelta(seconds=11))
    assert store.purge_expired_evidence(limit=1) == 1
    live, dead = (first, second) if newer_shorter else (second, first)
    live_receipt, dead_receipt = (first_receipt, second_receipt) if newer_shorter else (second_receipt, first_receipt)
    current = store.get_evidence(first.evidence_id)
    assert current['retention']['source_attempt_id'] == live.retention.source_attempt_id
    assert store.get_delivery(dead.delivery_id) is None
    assert store.get_observation(dead_receipt.observation_id) is None
    assert store.get_delivery(live.delivery_id)['text'] == live.text
    assert store.get_evidence(live.evidence_id, live_receipt.source_version_id)['retention']['source_attempt_id'] == live.retention.source_attempt_id
    assert [value['retention']['source_attempt_id'] for value in store.list_source_versions(live.evidence_id)] == [live.retention.source_attempt_id]
    assert [value['retention']['source_attempt_id'] for value in store.list_observations(live.evidence_id)] == [live.retention.source_attempt_id]
    assert store.list_observations_for_evidence([live.evidence_id])[live.evidence_id] == store.list_observations(live.evidence_id)
    assert store.list_evidence()[0]['retention']['source_attempt_id'] == live.retention.source_attempt_id
    clock(monkeypatch, start+timedelta(seconds=61))
    assert store.purge_expired_evidence(limit=1) == 1
    assert store.get_evidence(first.evidence_id) is None
    assert store.list_evidence() == []


def test_shared_capture_has_many_publications_and_bounded_membership_sweep(store, monkeypatch):
    start = datetime.now(UTC)
    clock(monkeypatch, start)
    first = observation(job='wr066-page', attempt='wr066-page-attempt',
        captured=start-timedelta(seconds=1), expires=start+timedelta(seconds=10))
    second = observation(job='wr066-page', attempt='wr066-page-attempt',
        captured=first.retention.captured_at, expires=first.retention.expires_at, occurrence='urn:li:activity:124')
    register(store, first)
    store.admit(first)
    store.admit(second)
    with store.Session() as session:
        assert len(list(session.scalars(select(CapturePolicyTable)))) == 1
        assert len(list(session.scalars(select(CaptureMemberTable)))) == 2
    clock(monkeypatch, start+timedelta(seconds=11))
    assert store.purge_expired_evidence(limit=1) == 1
    assert store.purge_expired_evidence(limit=1) == 1
    assert store.purge_expired_evidence(limit=1) == 0


@pytest.mark.parametrize('tamper', ['deadline', 'capture_time', 'text', 'metrics', 'job'])
def test_immutable_capture_policy_and_member_facts(store, monkeypatch, tamper):
    start = datetime.now(UTC)
    clock(monkeypatch, start)
    value = observation(job='wr066-immutable', attempt='wr066-immutable-attempt',
        captured=start-timedelta(seconds=2), expires=start+timedelta(minutes=20))
    register(store, value)
    original = store.admit(value)
    item = json.loads(json.dumps(value.raw_payload['m1_item']))
    job = value.origin.m1_job_id
    if tamper == 'deadline':
        item['retention']['expires_at'] = (value.retention.expires_at+timedelta(seconds=1)).isoformat()
    elif tamper == 'capture_time':
        captured = (value.retention.captured_at+timedelta(seconds=1)).isoformat()
        item['retention']['captured_at'] = item['observed_at'] = item['source_fields']['observed_at'] = captured
    elif tamper == 'text':
        item['text'] = item['source_fields']['commentary']['value'] = 'Invented replacement'
    elif tamper == 'metrics':
        item['metrics']['numLikes'] = item['source_fields']['metrics']['values']['numLikes']['value'] = 99
    else:
        job = 'wr066-another-job'
        item['acquisition']['root_job_id'] = item['task_fence']['job_id'] = job
    changed = convert({'source_type':'linkedin', 'job_id':job, 'items':[item]})[0]
    if tamper == 'job': register(store, changed)
    with pytest.raises(StoreError, match='immutable source capture'):
        store.admit(changed)
    assert store.get_evidence(value.evidence_id)['metrics']['numLikes'] == 5
    assert store.get_observation(original.observation_id)['text'] == value.text


def test_task_takeover_does_not_change_capture_policy_or_observation(store, monkeypatch):
    start = datetime.now(UTC)
    clock(monkeypatch, start)
    value = observation(job='wr066-takeover', attempt='wr066-takeover-attempt',
        captured=start-timedelta(seconds=1), expires=start+timedelta(minutes=20))
    register(store, value)
    original = store.admit(value)
    item = json.loads(json.dumps(value.raw_payload['m1_item']))
    item['task_fence']['generation'] = 2
    current = convert({'source_type':'linkedin', 'job_id':value.origin.m1_job_id, 'items':[item]})[0]
    store.update_task_fence(current.task_fence.model_dump())
    duplicate = store.admit(current)
    assert duplicate.duplicate_delivery and duplicate.observation_id == original.observation_id
    with pytest.raises(StoreError, match='task generation'):
        store.admit(value)


def test_conservative_expired_legacy_migration_is_idempotent_without_revival(store, monkeypatch):
    old, original, fresh = expired_then_fresh(store, monkeypatch)
    with store.transaction() as session:
        for table in (CaptureCopyTable, CaptureMemberTable, CapturePolicyTable, CaptureMigrationTable):
            session.execute(delete(table))
    store.initialize()
    store.initialize()
    assert store.get_evidence(old.evidence_id) is None
    assert_old_tombstones(store, old, original)
    with store.Session() as session:
        before = [(row.capture_id, row.evidence_id, row.expires_at, row.state) for row in session.scalars(select(CaptureMemberTable))]
    store.initialize()
    with store.Session() as session:
        after = [(row.capture_id, row.evidence_id, row.expires_at, row.state) for row in session.scalars(select(CaptureMemberTable))]
    assert before == after
    admitted = store.admit(fresh)
    assert admitted.evidence_id == original.evidence_id
    assert store.get_observation(original.observation_id) is None
    assert_old_tombstones(store, old, original, current=False)


def test_legacy_identity_minimum_deadline_caps_every_historical_capture(store, monkeypatch):
    start = datetime.now(UTC)
    clock(monkeypatch, start)
    first = observation(job='wr066-legacy-first', attempt='wr066-legacy-first-attempt',
        captured=start-timedelta(seconds=1), expires=start+timedelta(seconds=10))
    register(store, first)
    first_receipt = store.admit(first)
    clock(monkeypatch, start+timedelta(seconds=2))
    second = observation(job='wr066-legacy-second', attempt='wr066-legacy-second-attempt',
        captured=start+timedelta(seconds=1), expires=start+timedelta(seconds=60))
    register(store, second)
    second_receipt = store.admit(second)
    with store.transaction() as session:
        for table in (CaptureCopyTable, CaptureMemberTable, CapturePolicyTable, CaptureMigrationTable):
            session.execute(delete(table))
    clock(monkeypatch, start+timedelta(seconds=11))
    store.initialize()
    assert store.get_evidence(first.evidence_id) is None
    assert store.get_observation(first_receipt.observation_id) is None
    assert store.get_observation(second_receipt.observation_id) is None
    with store.Session() as session:
        rows = list(session.scalars(select(CaptureMemberTable)))
        assert len(rows) == 2
        assert all(store._aware(row.expires_at) == first.retention.expires_at for row in rows)
    with pytest.raises(StoreError, match='expired'):
        store.admit(second)


def test_fresh_admission_and_expired_capture_purge_serialize_without_erasing_fresh(store, monkeypatch):
    start = datetime.now(UTC)
    clock(monkeypatch, start)
    first = observation(job='wr066-race-old', attempt='wr066-race-old-attempt',
        captured=start-timedelta(seconds=1), expires=start+timedelta(seconds=10))
    register(store, first)
    original = store.admit(first)
    instant = start+timedelta(seconds=12)
    clock(monkeypatch, instant)
    fresh = observation(job='wr066-race-fresh', attempt='wr066-race-fresh-attempt',
        captured=instant-timedelta(seconds=1), expires=instant+timedelta(minutes=20))
    register(store, fresh)
    barrier = Barrier(2)
    outcomes, errors = [], []
    def run(operation):
        try:
            barrier.wait(5)
            outcomes.append(operation())
        except Exception as error:
            errors.append(error)
    threads = [Thread(target=run, args=(lambda:store.admit(fresh),)),
               Thread(target=run, args=(lambda:store.purge_expired_evidence(limit=1),))]
    for thread in threads: thread.start()
    for thread in threads: thread.join(10)
    assert not any(thread.is_alive() for thread in threads) and errors == []
    assert len(outcomes) == 2
    assert store.get_evidence(first.evidence_id)['retention']['source_attempt_id'] == fresh.retention.source_attempt_id
    assert store.get_delivery(first.delivery_id) is None
    assert store.get_observation(original.observation_id) is None
    assert_old_tombstones(store, first, original, current=False)


def test_capture_ingress_cannot_upgrade_existing_unrestricted_identity(store, monkeypatch):
    start = datetime.now(UTC)
    clock(monkeypatch, start)
    unrestricted = convert(wire(observed=start-timedelta(seconds=1), job='wr066-unrestricted'))[0]
    store.admit(unrestricted)
    incoming = observation(job='wr066-upgrade', attempt='wr066-upgrade-attempt',
        captured=start-timedelta(milliseconds=500), expires=start+timedelta(minutes=20))
    register(store, incoming)
    with pytest.raises(StoreError, match='clean ingress identity|audited backfill'):
        store.admit(incoming)


def test_mixed_legacy_copy_bindings_never_leave_expired_raw_bytes(store, monkeypatch):
    start = datetime.now(UTC)
    clock(monkeypatch, start)
    value = observation(job='wr066-mixed', attempt='wr066-mixed-attempt',
        captured=start-timedelta(seconds=1), expires=start+timedelta(seconds=10))
    register(store, value)
    original = store.admit(value)
    with store.transaction() as session:
        for table in (CaptureCopyTable, CaptureMemberTable, CapturePolicyTable, CaptureMigrationTable):
            session.execute(delete(table))
        row = session.get(ObservationTable, original.observation_id)
        body = json.loads(row.envelope)
        body.pop('source_fence')
        body.pop('task_fence')
        body['raw_payload']['m1_item'].pop('source_fence')
        body['raw_payload']['m1_item'].pop('task_fence')
        row.envelope = json.dumps(body)
    clock(monkeypatch, start+timedelta(seconds=11))
    store.initialize()
    store.purge_expired_evidence()
    with store.Session() as session:
        row = session.get(ObservationTable, original.observation_id)
        assert value.text not in row.envelope
        assert json.loads(row.envelope)['retention_state'] == 'policy_expired'


def test_same_capture_distinct_delivery_preserves_source_observation(store, monkeypatch):
    start = datetime.now(UTC)
    clock(monkeypatch, start)
    value = observation(job='wr066-delivery', attempt='wr066-delivery-attempt',
        captured=start-timedelta(seconds=1), expires=start+timedelta(minutes=20))
    register(store, value)
    first = store.admit(value)
    second = store.admit(value.model_copy(update={'delivery_id':value.delivery_id+'-transport'}))
    assert not second.duplicate_delivery and second.observation_id == first.observation_id
    assert len(store.list_observations(value.evidence_id)) == 1


def test_capture_policy_indexes_exclude_expired_backlog(store):
    indexes = {row['name']:row for row in inspect(store.engine).get_indexes('owned_capture_members')}
    assert indexes['ix_capture_members_evidence']['column_names'] == ['evidence_id']
    assert indexes['ix_capture_members_due']['column_names'][0] == 'expires_at'
    with store.engine.connect() as connection:
        if store.engine.dialect.name == 'sqlite':
            definition = connection.execute(text("SELECT sql FROM sqlite_master WHERE name='ix_capture_members_due'")).scalar_one()
        else:
            definition = connection.execute(text("SELECT indexdef FROM pg_indexes WHERE schemaname=current_schema() AND indexname='ix_capture_members_due'")).scalar_one()
    assert 'policy_expired' in definition and 'WHERE' in definition.upper()


def test_reopened_store_preserves_old_expiry_and_current_fresh_projection(store, monkeypatch):
    old, original, fresh = expired_then_fresh(store, monkeypatch)
    receipt = store.admit(fresh)
    reopened = Store(store.engine.url.render_as_string(hide_password=False))
    try:
        reopened.initialize()
        assert reopened.get_evidence(fresh.evidence_id)['retention']['source_attempt_id'] == fresh.retention.source_attempt_id
        assert reopened.get_observation(original.observation_id) is None
        assert reopened.get_delivery(old.delivery_id) is None
        assert reopened.get_evidence(fresh.evidence_id, receipt.source_version_id)['text'] == fresh.text
        with pytest.raises(StoreError, match='expired'):
            reopened.admit(old)
    finally:
        reopened.close()


def test_missing_acquisition_cannot_authorize_a_fresh_capture(store, monkeypatch):
    old, original, fresh = expired_then_fresh(store, monkeypatch)
    item = json.loads(json.dumps(fresh.raw_payload['m1_item']))
    item.pop('acquisition')
    stripped = convert({'source_type':'linkedin', 'job_id':fresh.origin.m1_job_id, 'items':[item]})[0]
    with pytest.raises(StoreError, match='acquisition lineage'):
        store.admit(stripped)
    assert store.get_evidence(old.evidence_id) is None
    assert_old_tombstones(store, old, original)


def test_live_legacy_copy_missing_acquisition_remains_blocked(store, monkeypatch):
    start = datetime.now(UTC)
    clock(monkeypatch, start)
    value = observation(job='wr066-legacy-lineage', attempt='wr066-legacy-lineage-attempt',
        captured=start-timedelta(seconds=1), expires=start+timedelta(minutes=20))
    register(store, value)
    receipt = store.admit(value)
    with store.transaction() as session:
        for table in (CaptureCopyTable, CaptureMemberTable, CapturePolicyTable, CaptureMigrationTable):
            session.execute(delete(table))
        row = session.get(ObservationTable, receipt.observation_id)
        body = json.loads(row.envelope)
        body['raw_payload']['m1_item'].pop('acquisition')
        row.envelope = json.dumps(body)
    store.initialize()
    with store.Session() as session:
        assert list(session.scalars(select(CaptureMemberTable))) == []
        assert session.get(CaptureMigrationTable, value.evidence_id).state == 'legacy_blocked'


@pytest.mark.parametrize('source_clock', ['published_at', 'source_updated_at'])
@pytest.mark.parametrize('purge_before_fresh', [False, True])
def test_fresh_capture_head_ignores_expired_historical_source_clock(store, monkeypatch, source_clock, purge_before_fresh):
    # Typed synthetic M2 timestamps exercise current selection independently
    # of the current LinkedIn converter, whose publication time remains unknown.
    start = datetime.now(UTC)
    clock(monkeypatch, start)
    old = observation(job='wr066-clock-old', attempt='wr066-clock-old-attempt',
        captured=start-timedelta(seconds=1), expires=start+timedelta(seconds=10))
    old = old.model_copy(update={source_clock:start-timedelta(days=1)})
    register(store, old)
    original = store.admit(old)
    now = start+timedelta(seconds=12)
    clock(monkeypatch, now)
    if purge_before_fresh:
        assert store.purge_expired_evidence() == 1
    fresh = observation(job='wr066-clock-fresh', attempt='wr066-clock-fresh-attempt',
        captured=now-timedelta(seconds=1), expires=now+timedelta(minutes=20))
    fresh = fresh.model_copy(update={source_clock:start-timedelta(days=2)})
    register(store, fresh)
    admitted = store.admit(fresh)
    projection = store.evidence_projection(fresh.evidence_id)
    assert projection['content_source_version_id'] == admitted.source_version_id
    assert projection['availability_observation_id'] == admitted.observation_id
    current = store.get_evidence(fresh.evidence_id)
    assert current is not None
    assert current['retention']['source_attempt_id'] == fresh.retention.source_attempt_id
    assert store.list_evidence()[0]['retention']['source_attempt_id'] == fresh.retention.source_attempt_id
    assert store.get_evidence(fresh.evidence_id, admitted.source_version_id)['text'] == fresh.text
    projection = store.evidence_projection(fresh.evidence_id)
    assert projection['content_source_version_id'] == admitted.source_version_id
    assert projection['availability_observation_id'] == admitted.observation_id
    assert store.get_evidence(old.evidence_id, original.source_version_id) is None
    assert store.get_delivery(old.delivery_id) is None
    assert store.get_observation(original.observation_id) is None
