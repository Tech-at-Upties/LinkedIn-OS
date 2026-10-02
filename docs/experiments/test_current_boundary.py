"""Discriminating checks against current source, using synthetic local evidence."""

from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from threading import Event, Thread
from types import SimpleNamespace
from urllib.parse import urlparse

import httpx
import pytest
from pydantic import ValidationError

from conftest import SOURCE_ROOT
from nos_m2.m1 import M1Client
from nos_m2.models import EvidenceEnvelope
from xingestion.capabilities import CapabilityPlanner, CapabilityRequest, SearchTweetsInput
from xingestion.sessions import SessionHealth, SessionStore
from xingestion.tasks import CapabilityTask, TaskState
from xingestion.web import live_server
from xingestion.workers import LocalWorker
from xingestion.xprotocol.evidence import FileRawEvidenceSink
from xingestion.xprotocol.protocol import CapabilityId, ProtocolReleaseManifest
from xingestion.xprotocol.runtime import WebSessionAuth
from xingestion.xprotocol.runtime.transaction_id import probe_transaction_id_context
from xingestion.xprotocol.runtime.urllib_transport import UrllibJsonTransport

TIME = "2026-10-02T09:00:00+00:00"


def test_m2_rejects_linkedin_source_before_admission():
    with pytest.raises(ValidationError) as error:
        EvidenceEnvelope(
            delivery_id="synthetic-delivery", evidence_id="linkedin:urn:li:activity:1",
            source_type="linkedin", source_item_id="urn:li:activity:1", observed_at=TIME,
        )
    assert any(item["loc"] == ("source_type",) for item in error.value.errors())


def test_existing_non_x_converter_is_an_rss_shape_not_linkedin():
    item = {
        "item_id": "urn:li:activity:1", "source_id": "urn:li:organization:2",
        "fetched_at": TIME, "text": "Synthetic publication text",
        "author": {"account_id": "urn:li:organization:2"}, "like_count": 0,
        "references": [{"kind": "repost", "source_item_id": "urn:li:activity:3"}],
    }
    with httpx.Client(transport=httpx.MockTransport(
        lambda request: pytest.fail("No network expected")
    )) as transport:
        client = M1Client("https://m1.example", "synthetic-key", client=transport)
        converted = client.envelopes({"source_type": "linkedin", "items": [item]})[0]
    assert converted["text"] == ""
    assert converted["metrics"] == {}
    assert converted["author"] is None
    assert converted["references"] == []
    assert converted["raw_payload"]["m1_item"]["text"] == item["text"]


class RouteHandler(live_server.LiveAppHandler):
    def __init__(self):
        self.headers = {"Authorization": "Bearer synthetic-key"}
        self.status = None

    def _json(self, payload, *, status=200):
        self.status = status
        return payload

    def _set_api_key_log_context(self, client_id):
        pass

    def _enforce_northbound_rate_limit(self, *args, **kwargs):
        return False

    def _northbound_identity(self, client_id):
        return self._json({"client_id": client_id})


@pytest.mark.parametrize("route,expected", [
    ("/v1/monitors/synthetic-monitor/runs", 404),
    ("/v1/rss/sources/synthetic-source/events", 404),
    ("/v1/auth/identity", 200),
])
def test_actual_m1_route_dispatch(route, expected, monkeypatch):
    monkeypatch.setattr(live_server, "STATE", SimpleNamespace(
        config=SimpleNamespace(api_keys={"synthetic-key": "synthetic-client"}),
        api_key_store=None,
    ))
    handler = RouteHandler()
    handler._handle_northbound_get(urlparse(route))
    assert handler.status == expected


class AdmissionLedger:
    """Only task admission is doubled; the worker, session DB and HTTP are real."""

    def __init__(self, task):
        self.task = task

    def get_task(self, task_id):
        assert task_id == self.task.task_id
        return self.task

    def acquire_execution_lease(self, task_id, *, owner, lease_expires_at):
        assert self.task.state == TaskState.ENQUEUED
        self.task = replace(self.task, state=TaskState.RUNNING, lease_owner=owner,
                            lease_token="synthetic-lease", lease_expires_at=lease_expires_at)
        return self.task

    def renew_execution_lease(self, task_id, **kwargs):
        assert kwargs["lease_token"] == self.task.lease_token
        return self.task

    def transition_task(self, task_id, *, from_state, to_state, **kwargs):
        assert self.task.state == from_state
        allowed = {key: kwargs[key] for key in ("result_json", "error_json", "next_attempt_at")
                   if key in kwargs}
        self.task = replace(self.task, state=to_state, **allowed)
        return self.task


@pytest.mark.parametrize("stop_timing,expected_requests,expected_state", [
    ("before_selection", 0, TaskState.RETRY_SCHEDULED),
    ("after_admission", 1, TaskState.DONE),
])
def test_committed_session_stop_at_actual_loopback_dispatch(
    stop_timing, expected_requests, expected_state, tmp_path, monkeypatch,
):
    calls = []

    class Receiver(BaseHTTPRequestHandler):
        def do_GET(self):
            calls.append(self.path)
            body = b'{"entries": []}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        do_POST = do_GET

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
    server_thread = Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    sessions = SessionStore(tmp_path / "sessions.sqlite")
    sessions.upsert_session(session_id="synthetic-session", account_label="fixture",
                            credential_ref="test:synthetic-reference")
    manifest = ProtocolReleaseManifest.from_file(
        SOURCE_ROOT / "M1/protocol_releases/search_tweets.candidate.json")
    binding = manifest.bindings[0]
    operation = replace(binding.recipe.operation,
                        url_template=f"http://127.0.0.1:{server.server_port}/graphql/{{operation_id}}/SearchTimeline")
    manifest = replace(manifest, bindings=(replace(
        binding, recipe=replace(binding.recipe, operation=operation)),))
    request = CapabilityRequest(CapabilityId.SEARCH_TWEETS, 1,
                                SearchTweetsInput(query="synthetic-fixture", max_pages=1))
    plan = CapabilityPlanner(manifest).plan(request)
    task = CapabilityTask(
        task_id="synthetic-task", idempotency_key="synthetic-task", capability_id=CapabilityId.SEARCH_TWEETS,
        contract_version=1, state=TaskState.ENQUEUED, request_json=request.public_dict(),
        plan_json=plan.public_dict(), result_json=None, error_json=None, attempt_count=1,
        max_attempts=3, next_attempt_at=None, lease_owner=None, lease_token=None,
        lease_expires_at=None, delivery_generation=1, replay_origin_task_id=None,
        created_at=TIME, updated_at=TIME,
    )
    ledger = AdmissionLedger(task)
    worker = LocalWorker(ledger=ledger, manifest=manifest,
                         auth=WebSessionAuth("synthetic-auth", "synthetic-csrf", "synthetic-bearer"),
                         transport=UrllibJsonTransport(timeout_seconds=5),
                         raw_evidence_sink=FileRawEvidenceSink(tmp_path / "raw"), session_store=sessions)
    admitted, released = Event(), Event()
    errors, outcomes = [], []

    def before_dispatch(session):
        admitted.set()
        if not released.wait(10):
            raise AssertionError("Dispatch barrier timeout")
        return probe_transaction_id_context()

    monkeypatch.setattr(worker, "_transaction_id_context_for_session", before_dispatch)

    def commit_stop():
        program = (
            "import sys; from xingestion.sessions import SessionStore, SessionHealth; "
            "SessionStore(sys.argv[1]).update_health('synthetic-session', "
            "health=SessionHealth.CHALLENGED, reason='injected-local-test-stop')"
        )
        subprocess.run([sys.executable, "-c", program, str(tmp_path / "sessions.sqlite")],
                       check=True, capture_output=True, timeout=10)

    def run_worker():
        try:
            outcomes.append(worker._process_delivery(task.task_id))
        except Exception as error:
            errors.append(error)

    thread = Thread(target=run_worker, daemon=True)
    try:
        if stop_timing == "before_selection":
            commit_stop()
            # A fresh store models reload after the durable stop was committed.
            worker.session_store = SessionStore(tmp_path / "sessions.sqlite")
        thread.start()
        if stop_timing == "after_admission":
            assert admitted.wait(10), "Worker did not reach the dispatch barrier"
            commit_stop()
        released.set()
        thread.join(10)
        assert not thread.is_alive()
        assert errors == []
        assert len(calls) == expected_requests
        assert outcomes[0].state == expected_state
        reloaded = SessionStore(tmp_path / "sessions.sqlite")
        assert reloaded.get_session("synthetic-session").health == SessionHealth.CHALLENGED
        assert reloaded.acquire_session(owner="restarted-worker") is None
        if stop_timing == "after_admission":
            assert outcomes[0].raw_evidence_ref is not None
        receipt = {
            "source_root": str(SOURCE_ROOT),
            "worker_sha256": hashlib.sha256((SOURCE_ROOT /
                "M1/src/xingestion/workers/local_worker.py").read_bytes()).hexdigest(),
            "verification_level": "V2",
            "task_ledger": "test double, not Postgres",
            "session_store": "actual M1 SQLite, stop committed by a separate process",
            "transport": "actual M1 UrllibJsonTransport, loopback HTTP receiver",
            "stop_timing": stop_timing,
            "physical_requests": len(calls),
            "task_outcome": outcomes[0].state.value,
            "durable_session_health": reloaded.get_session("synthetic-session").health.value,
            "restarted_admission_blocked": True,
            "raw_result_preserved": outcomes[0].raw_evidence_ref is not None,
            "required_no_dispatch_after_committed_stop": len(calls) == 0,
            "final_production_fence_gate_passed": False,
        }
        receipt_path = Path(__file__).resolve().parents[1] / "results" / f"stop-witness-{stop_timing}.json"
        receipt_path.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    finally:
        released.set()
        thread.join(10)
        server.shutdown()
        server.server_close()
        server_thread.join(5)
