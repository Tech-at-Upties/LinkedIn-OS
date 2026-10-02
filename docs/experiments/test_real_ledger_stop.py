"""Actual Postgres task ownership, SQLite session stop and HTTP invocation."""
import json
import os
import subprocess
import sys
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Event, Thread

import pytest

from test_current_boundary import (
    SOURCE_ROOT, CapabilityId, CapabilityPlanner, CapabilityRequest, FileRawEvidenceSink,
    LocalWorker, ProtocolReleaseManifest, SearchTweetsInput, SessionHealth, SessionStore,
    TaskState, UrllibJsonTransport, WebSessionAuth, probe_transaction_id_context,
)

sys.path.insert(0, str(SOURCE_ROOT / "M1/tests"))
from postgres_fixture import make_postgres_ledger


@pytest.mark.parametrize("timing", ["before_selection", "after_admission"])
def test_real_ledger_worker_committed_stop_has_no_physical_request(timing, tmp_path, monkeypatch):
    if os.environ.get("XINGESTION_TEST_POSTGRES_DSN") != "postgresql://nos_test@127.0.0.1:15432/nos_linkedin_m1_test":
        pytest.skip("Only the verified project-owned test cluster is allowed")
    ledger = make_postgres_ledger()
    calls = []
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            calls.append(self.path)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"entries": []}')
        do_POST = do_GET
        def log_message(self, *_):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server_thread = Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    sessions_path = tmp_path / "sessions.sqlite"
    sessions = SessionStore(sessions_path)
    sessions.upsert_session(session_id="synthetic-session", account_label="fixture", credential_ref="test:synthetic-reference")
    manifest = ProtocolReleaseManifest.from_file(SOURCE_ROOT / "M1/protocol_releases/search_tweets.candidate.json")
    binding = manifest.bindings[0]
    operation = replace(binding.recipe.operation, url_template=f"http://127.0.0.1:{server.server_port}/graphql/{{operation_id}}/SearchTimeline")
    manifest = replace(manifest, bindings=(replace(binding, recipe=replace(binding.recipe, operation=operation)),))
    request = CapabilityRequest(CapabilityId.SEARCH_TWEETS, 1, SearchTweetsInput(query="synthetic-fixture", max_pages=1))
    plan = CapabilityPlanner(manifest).plan(request)
    task = ledger.create_task(idempotency_key="synthetic-real-fence", capability_id=request.capability_id,
                              contract_version=1, request_json=request.public_dict(), plan_json=plan.public_dict())
    worker = LocalWorker(ledger=ledger, manifest=manifest, auth=WebSessionAuth("synthetic-auth", "synthetic-csrf", "synthetic-bearer"),
                         transport=UrllibJsonTransport(timeout_seconds=5), raw_evidence_sink=FileRawEvidenceSink(tmp_path / "raw"), session_store=sessions)
    admitted, released = Event(), Event()
    outcomes, errors = [], []
    def barrier(session):
        admitted.set()
        assert released.wait(10)
        return probe_transaction_id_context()
    monkeypatch.setattr(worker, "_transaction_id_context_for_session", barrier)
    def stop():
        command = "from xingestion.sessions import SessionStore, SessionHealth; import sys; SessionStore(sys.argv[1]).update_health('synthetic-session', health=SessionHealth.CHALLENGED, reason='synthetic-stop')"
        child_env = os.environ | {"PYTHONPATH": str(SOURCE_ROOT / "M1/src")}
        subprocess.run([sys.executable, "-c", command, str(sessions_path)], capture_output=True, check=True, timeout=10, env=child_env)
    def process():
        try:
            outcomes.append(worker._process_delivery(task.task_id))
        except Exception as error:
            errors.append(type(error).__name__)
    thread = Thread(target=process, daemon=True)
    try:
        if timing == "before_selection":
            stop()
        thread.start()
        if timing == "after_admission":
            assert admitted.wait(10)
            stop()
        released.set()
        thread.join(10)
        assert not thread.is_alive() and errors == []
        assert calls == []
        stored = ledger.get_task(task.task_id)
        assert stored.state in {TaskState.RETRY_SCHEDULED, TaskState.DEAD_LETTER}
        assert stored.result_json is None
        assert SessionStore(sessions_path).get_session("synthetic-session").health == SessionHealth.CHALLENGED
        receipt = {"timing": timing, "physical_requests": len(calls), "ledger": "actual PostgresTaskLedger",
                   "worker_state": stored.state.value, "raw_evidence_created": outcomes[0].raw_evidence_ref is not None,
                   "redis_delivery_exercised": False, "full_production_fence_gate_passed": False}
        (Path(__file__).resolve().parents[1] / "results" / f"real-ledger-stop-{timing}.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    finally:
        released.set()
        thread.join(10)
        server.shutdown()
        server.server_close()
        server_thread.join(3)
        ledger.pool.close()
