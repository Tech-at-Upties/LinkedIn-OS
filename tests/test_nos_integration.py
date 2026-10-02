"""Controlled actual M1 serializer -> M2 converter/model -> SQLite Store."""
import copy
import json
import multiprocessing
import os
import sys
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from uuid import uuid4

SOURCE = Path(os.environ.get("NOS_INTEGRATION_ROOT", Path(__file__).resolve().parents[1] / ".local/nos-integration"))
if not (SOURCE / "M2/src/nos_m2/linkedin.py").exists():
    pytest.skip("The dedicated NOS integration checkout is not installed", allow_module_level=True)
sys.path[:0] = [str(SOURCE / "M1/src"), str(SOURCE / "M2/src")]

from nos_m2.m1 import M1Client, M1Error
from nos_m2.models import EvidenceEnvelope
from nos_m2.store import Store
from xingestion.linkedin.northbound import serialize_company_page
from xingestion.linkedin.executor import execute_company_page

from nos_linkedin.parser import parse_graph
from nos_linkedin.acquisition import AcquisitionFailure, Journal, RoutePermission, SourceResponse, acquire_company_page
from test_parser import COMPANY, NOW, ROOT, envelope as source_envelope, fixture


def wire(nodes=None, *, observed=NOW, job="job-li-1"):
    page = parse_graph(fixture() if nodes is None else nodes, [ROOT], feed_publisher_id=COMPANY,
                       observed_at=observed, evidence_class="synthetic_fixture")
    return serialize_company_page(page, job_id=job)


def convert(result):
    with M1Client("http://127.0.0.1:8090", "synthetic-local-test-key", max_retries=0) as client:
        return [EvidenceEnvelope.model_validate(item) for item in client.envelopes(result)]


@pytest.fixture(params=["sqlite", "postgres"])
def store(tmp_path, request):
    admin = None
    if request.param == "postgres":
        url = os.environ.get("NOS_M2_TEST_DATABASE_URL")
        if not url:
            pytest.skip("Real Postgres URL not configured")
        schema = f"li_test_{uuid4().hex}"
        admin = create_engine(url)
        with admin.begin() as connection:
            connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        uri = make_url(url).update_query_dict({"options": f"-csearch_path={schema}"}).render_as_string(hide_password=False)
    else:
        uri = f"sqlite:///{tmp_path / 'm2.sqlite'}"
    database = Store(uri)
    database.initialize()
    yield database
    database.close()
    if admin is not None:
        with admin.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


def test_actual_boundary_keeps_native_identity_authorship_metrics_and_origin(store):
    envelope = convert(wire())[0]
    assert envelope.evidence_id == "linkedin:urn:li:activity:123"
    assert envelope.author.account_id == "urn:li:company:999"
    assert envelope.metrics == {"numLikes": 5, "numComments": 0, "numImpressions": None}
    assert envelope.published_at is None and envelope.coverage.state == "partial"
    assert envelope.raw_payload["m1_item"]["metrics_target_id"] == "urn:li:ugcPost:456"
    receipt = store.admit(envelope)
    assert store.get_evidence(receipt.evidence_id)["origin"]["m1_job_id"] == "job-li-1"


def test_duplicate_repeat_engagement_change_and_edit_have_separate_history(store):
    initial = convert(wire())[0]
    first = store.admit(initial)
    assert store.admit(initial).duplicate_delivery
    repeat = store.admit(convert(wire(observed=NOW + timedelta(minutes=1), job="job-2"))[0])
    assert repeat.source_version_id == first.source_version_id and repeat.observation_id != first.observation_id
    nodes = fixture()
    nodes[2]["numLikes"] = 8
    metric_change = store.admit(convert(wire(nodes, observed=NOW + timedelta(minutes=2), job="job-3"))[0])
    assert metric_change.source_version_id == first.source_version_id
    nodes[0]["commentary"]["text"]["text"] = "Edited source commentary"
    edit = store.admit(convert(wire(nodes, observed=NOW + timedelta(minutes=3), job="job-4"))[0])
    assert edit.source_version_id != first.source_version_id
    assert len(store.list_observations(first.evidence_id)) == 4


def test_partial_metric_then_recovery_never_becomes_zero(store):
    nodes = fixture()[:2]
    partial = convert(wire(nodes))[0]
    assert partial.metrics == {}
    first = store.admit(partial)
    recovered = store.admit(convert(wire(observed=NOW + timedelta(minutes=1), job="recovered"))[0])
    assert recovered.source_version_id == first.source_version_id
    assert store.list_observations(first.evidence_id)[0]["metrics"] == {}


def test_reopen_and_redelivery_preserve_history(tmp_path):
    uri = f"sqlite:///{tmp_path / 'restart.sqlite'}"
    envelope = convert(wire())[0]
    first = Store(uri)
    first.initialize()
    receipt = first.admit(envelope)
    first.close()
    reopened = Store(uri)
    try:
        reopened.initialize()
        assert reopened.admit(envelope).duplicate_delivery
        assert len(reopened.list_observations(receipt.evidence_id)) == 1
    finally:
        reopened.close()


@pytest.mark.parametrize("mutate", [
    lambda item: item.update(text="Invented text"),
    lambda item: item.update(author={"account_id": COMPANY}),
    lambda item: item.update(metrics={"numLikes": 99}),
    lambda item: item.update(metrics_target_id="urn:li:ugcPost:123"),
    lambda item: item.update(raw_evidence_ref="/v1/evidence/projected-not-raw"),
    lambda item: item["coverage"].update(state="complete"),
    lambda item: item.update(observed_at="2026-10-02T00:00:00"),
    lambda item: item["source_fields"]["commentary"].update(state="unrecognized"),
    lambda item: (item.update(author={"account_id": "not-a-native-id"}), item["source_fields"]["actor_id"].update(value="not-a-native-id")),
])
def test_boundary_rejects_inconsistent_source_claims(mutate):
    result = copy.deepcopy(wire())
    mutate(result["items"][0])
    with pytest.raises(M1Error):
        convert(result)


def acquire(journal, body, calls, *, attempt="captured", status=200):
    def send():
        calls.append(attempt)
        return SourceResponse(status, "application/json", json.dumps(body).encode(), NOW)
    return acquire_company_page(
        journal=journal, scope="linkedin.company-feed", generation=journal.generation("linkedin.company-feed"),
        permission=RoutePermission(True, True, NOW + timedelta(days=1), "synthetic_fixture"),
        feed_publisher_id=COMPANY, send=send, now=NOW, attempt_id=attempt,
    )


def test_captured_response_reshare_and_replay_through_actual_m1_m2(store, tmp_path):
    body = source_envelope()
    original = copy.deepcopy(body["included"][0])
    original["entityUrn"] = "urn:li:fsd_update:(urn:li:activity:987,ORIGINAL)"
    original["metadata"].update(backendUrn="urn:li:activity:987", shareUrn="urn:li:ugcPost:654")
    original["actor"]["backendUrn"] = "urn:li:company:888"
    original["commentary"]["text"]["text"] = "Original author's separate text"
    body["included"][0]["resharedUpdate"] = original["entityUrn"]
    body["included"].append(original)
    path = tmp_path / "capture.sqlite"
    calls = []
    captured = acquire(Journal(path), body, calls)
    assert captured.page is not None
    result = serialize_company_page(captured.page, job_id="job-captured-reshare")
    item = convert(result)[0]
    receipt = store.admit(item)
    saved = store.get_evidence(receipt.evidence_id)
    assert item.text == "Own commentary" and item.author.account_id == "urn:li:company:999"
    assert saved["references"][0]["evidence_id"] == "linkedin:urn:li:activity:987"
    assert item.raw_payload["m1_item"]["source_fields"]["feed_publisher_id"] == COMPANY
    assert len(store.list_observations(item.evidence_id)) == 1
    replay = acquire(Journal(path), {"not": "the original response"}, calls)
    replayed = convert(serialize_company_page(replay.page, job_id="job-captured-reshare"))[0]
    assert replay.replayed and replay.body_sha256 == captured.body_sha256 and calls == ["captured"]
    assert store.admit(replayed).duplicate_delivery
    assert len(store.list_observations(item.evidence_id)) == 1


def test_access_failure_after_collection_does_not_invent_deleted_evidence(store, tmp_path):
    path = tmp_path / "capture.sqlite"
    calls = []
    captured = acquire(Journal(path), source_envelope(), calls)
    item = convert(serialize_company_page(captured.page, job_id="job-before-access-stop"))[0]
    store.admit(item)
    denied = acquire(Journal(path), {}, calls, attempt="denied", status=403)
    assert denied.outcome == "restricted" and denied.page is None
    assert Journal(path).read("denied", NOW)["body"] == b"{}"
    with pytest.raises(AcquisitionFailure, match="durable_hold"):
        acquire(Journal(path), source_envelope(), calls, attempt="after-restart")
    assert calls == ["captured", "denied"]
    observations = store.list_observations(item.evidence_id)
    assert len(observations) == 1
    assert observations[0]["availability"] == "available"


def execute(journal, calls, deliver, *, clock=lambda: NOW, status=200):
    def send():
        calls.append("source")
        return SourceResponse(status, "application/json", json.dumps(source_envelope()).encode(), NOW)
    return execute_company_page(journal=journal, feed_publisher_id=COMPANY, attempt_id="m1-company-page",
                                permission=RoutePermission(True, True, NOW + timedelta(days=1), "synthetic_fixture"),
                                send=send, deliver=deliver, clock=clock)


def commit_stop(path, started, completed):
    started.set()
    Journal(path).stop("linkedin.company-feed", "operator_stop")
    completed.set()


def test_m1_executor_stop_after_parse_prevents_actual_m2_admission(store, tmp_path, monkeypatch):
    path = tmp_path / "executor.sqlite"
    journal = Journal(path)
    finish = journal.finish
    context = multiprocessing.get_context("spawn")
    started, completed = context.Event(), context.Event()
    def finish_then_stop(*args):
        outcome = finish(*args)
        process = context.Process(target=commit_stop, args=(path, started, completed))
        process.start()
        try:
            assert completed.wait(10)
            process.join(10)
            assert process.exitcode == 0
        finally:
            if process.is_alive():
                process.terminate()
                process.join(3)
        return outcome
    monkeypatch.setattr(journal, "finish", finish_then_stop)
    calls, deliveries = [], []
    def deliver(result):
        deliveries.append(result)
        store.admit(convert(result)[0])
    result = execute(journal, calls, deliver)
    assert result.delivery_outcome == "quarantined_after_stop" and deliveries == []
    assert calls == ["source"] and store.get_evidence("linkedin:urn:li:activity:123") is None
    assert Journal(path).read(result.attempt_id, NOW)["outcome"] == "quarantined_after_stop"


def test_m1_executor_stop_during_delivery_commits_after_actual_m2_admission(store, tmp_path):
    path = tmp_path / "executor.sqlite"
    context = multiprocessing.get_context("spawn")
    started, completed = context.Event(), context.Event()
    process = context.Process(target=commit_stop, args=(path, started, completed))
    calls = []
    def deliver(result):
        process.start()
        assert started.wait(10)
        assert not completed.wait(0.2)
        store.admit(convert(result)[0])
    try:
        result = execute(Journal(path), calls, deliver)
        assert result.delivery_outcome == "delivery_acknowledged"
        assert completed.wait(10)
        process.join(10)
        assert process.exitcode == 0
        assert len(store.list_observations("linkedin:urn:li:activity:123")) == 1
        with pytest.raises(AcquisitionFailure, match="durable_hold"):
            Journal(path).generation("linkedin.company-feed")
    finally:
        if process.is_alive():
            process.terminate()
            process.join(3)


def test_m1_executor_ambiguous_delivery_replay_is_idempotent_in_m2(store, tmp_path):
    path = tmp_path / "executor.sqlite"
    calls, receipts = [], []
    def commit_then_timeout(result):
        item = convert(result)[0]
        assert item.origin.m1_job_id == "m1-company-page"
        receipts.append(store.admit(item))
        raise TimeoutError("Synthetic timeout after actual commit")
    first = execute(Journal(path), calls, commit_then_timeout)
    assert first.delivery_outcome == "delivery_failure"
    def acknowledge(result):
        receipts.append(store.admit(convert(result)[0]))
    replay = execute(Journal(path), calls, acknowledge)
    assert replay.replayed and replay.delivery_outcome == "delivery_acknowledged"
    assert calls == ["source"] and receipts[1].duplicate_delivery
    assert len(store.list_observations("linkedin:urn:li:activity:123")) == 1


def test_m1_executor_expiry_before_delivery_prevents_admission(store, tmp_path):
    calls, deliveries = [], []
    times = iter([NOW, NOW + timedelta(days=2)])
    result = execute(Journal(tmp_path / "executor.sqlite"), calls, deliveries.append, clock=lambda: next(times))
    assert result.delivery_outcome == "policy_expired" and deliveries == []
    assert calls == ["source"] and store.get_evidence("linkedin:urn:li:activity:123") is None


def test_m1_executor_access_failure_does_not_deliver_an_item(store, tmp_path):
    calls, deliveries = [], []
    result = execute(Journal(tmp_path / "executor.sqlite"), calls, deliveries.append, status=403)
    assert result.source_outcome == "restricted" and result.delivery_outcome is None
    assert calls == ["source"] and deliveries == []
    assert store.get_evidence("linkedin:urn:li:activity:123") is None


def test_m1_executor_unclassified_response_between_parse_and_delivery_blocks_sink(store, tmp_path, monkeypatch):
    path = tmp_path / "executor.sqlite"
    journal = Journal(path)
    finish = journal.finish
    def finish_then_capture(*args):
        outcome = finish(*args)
        other = Journal(path)
        other.capture(scope="linkedin.company-feed", generation=other.generation("linkedin.company-feed"),
                      attempt_id="pending-security", feed_publisher_id=COMPANY,
                      permission=RoutePermission(True, True, NOW + timedelta(days=1), "synthetic_fixture"), now=NOW,
                      send=lambda: SourceResponse(403, "application/json", b"{}", NOW))
        return outcome
    monkeypatch.setattr(journal, "finish", finish_then_capture)
    calls, deliveries = [], []
    with pytest.raises(AcquisitionFailure, match="classification_pending"):
        execute(journal, calls, deliveries.append)
    assert calls == ["source"] and deliveries == []
    assert store.get_evidence("linkedin:urn:li:activity:123") is None
