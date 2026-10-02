import json
import multiprocessing
import threading
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import urlopen

import pytest

from nos_linkedin.acquisition import AcquisitionFailure, Journal, RoutePermission, SourceResponse, acquire_company_page
from test_parser import COMPANY, NOW, envelope

SCOPE = "linkedin.company-feed"


def permission(**changes):
    values = dict(collection_allowed=True, raw_retention_allowed=True, raw_expires_at=NOW + timedelta(days=1), evidence_class="synthetic_fixture")
    return RoutePermission(**(values | changes))


def run(journal, send, **changes):
    selected = changes["generation"] if "generation" in changes else journal.generation(SCOPE)
    values = dict(journal=journal, scope=SCOPE, generation=selected, permission=permission(), feed_publisher_id=COMPANY,
                  send=send, now=NOW, attempt_id="attempt-one")
    return acquire_company_page(**(values | changes))


def response(body=None, status=200, content_type="application/vnd.linkedin.normalized+json+2.1"):
    return SourceResponse(status, content_type, json.dumps(envelope() if body is None else body).encode(), NOW)


def test_original_committed_before_parse_failure_and_no_retry(tmp_path):
    journal = Journal(tmp_path / "journal.sqlite")
    calls = []
    payload = b'{"bad":'
    result = run(journal, lambda: calls.append(1) or SourceResponse(200, "application/json", payload, NOW))
    assert result.outcome == "parse_failure" and len(calls) == 1
    assert journal.read(result.attempt_id, NOW)["body"] == payload


@pytest.mark.parametrize("status", [400, 404, 500])
def test_http_error_retains_bytes_and_cannot_be_empty_terminal(status, tmp_path):
    journal = Journal(tmp_path / "journal.sqlite")
    result = run(journal, lambda: response({"data": None, "included": []}, status))
    assert result.outcome == "http_failure" and result.page is None
    assert journal.read(result.attempt_id, NOW)["body"] is not None


@pytest.mark.parametrize("status", [401, 403, 429, 999])
def test_access_failure_is_durable_and_source_scoped(status, tmp_path):
    path = tmp_path / "journal.sqlite"
    journal = Journal(path)
    result = run(journal, lambda: response({}, status))
    assert result.outcome in {"restricted", "authentication_required"}
    with pytest.raises(AcquisitionFailure, match="durable_hold"):
        Journal(path).generation(SCOPE)


@pytest.mark.parametrize("changes", [dict(collection_allowed=False), dict(raw_retention_allowed=False), dict(raw_expires_at=NOW)])
def test_permission_failure_has_zero_dispatches(changes, tmp_path):
    calls = []
    with pytest.raises(AcquisitionFailure):
        run(Journal(tmp_path / "journal.sqlite"), lambda: calls.append(1), permission=permission(**changes))
    assert calls == []


def test_reopen_replay_has_zero_extra_transport_calls(tmp_path):
    path = tmp_path / "journal.sqlite"
    calls = []
    first = run(Journal(path), lambda: calls.append(1) or response())
    replay = run(Journal(path), lambda: calls.append(1) or response())
    assert replay.replayed and replay.body_sha256 == first.body_sha256
    assert replay.page.publications == first.page.publications and len(calls) == 1


def test_retention_expiry_is_not_source_deletion(tmp_path):
    journal = Journal(tmp_path / "journal.sqlite")
    result = run(journal, lambda: response())
    row = journal.read(result.attempt_id, NOW + timedelta(days=2))
    assert row["body"] is None and row["outcome"] == "policy_expired"
    assert row["body_sha256"] == result.body_sha256


def stop_process(path, done):
    Journal(path).stop(SCOPE, "operator_stop")
    done.set()


def test_selected_work_is_fenced_after_another_process_commits_stop(tmp_path):
    path = tmp_path / "journal.sqlite"
    journal = Journal(path)
    generation = journal.generation(SCOPE)
    context = multiprocessing.get_context("spawn")
    done = context.Event()
    process = context.Process(target=stop_process, args=(path, done))
    process.start()
    assert done.wait(10)
    process.join(10)
    assert process.exitcode == 0
    calls = []
    with pytest.raises(AcquisitionFailure, match="dispatch_fenced"):
        run(journal, lambda: calls.append(1) or response(), generation=generation)
    assert calls == []


def test_loopback_physical_dispatch_and_restart_stop(tmp_path):
    requests = []
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            payload = json.dumps(envelope()).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(payload)
        def log_message(self, *_):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    path = tmp_path / "journal.sqlite"
    journal = Journal(path)
    generation = journal.generation(SCOPE)
    def send():
        with urlopen(f"http://127.0.0.1:{server.server_port}/fixture", timeout=3) as result:
            return SourceResponse(result.status, result.headers["Content-Type"], result.read(), NOW)
    try:
        result = run(journal, send)
        assert result.page is not None and requests == ["/fixture"]
        Journal(path).stop(SCOPE, "operator_stop")
        with pytest.raises(AcquisitionFailure):
            run(Journal(path), send, generation=generation, attempt_id="attempt-two")
        assert requests == ["/fixture"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(3)


def test_html_or_semantic_error_does_not_become_success(tmp_path):
    journal = Journal(tmp_path / "journal.sqlite")
    assert run(journal, lambda: response({}, content_type="text/html")).outcome == "representation_drift"
    assert run(journal, lambda: response({"errors": [{"status": 403}]}), attempt_id="semantic").outcome == "restricted"
    with pytest.raises(AcquisitionFailure, match="durable_hold"):
        journal.generation(SCOPE)


def test_publication_text_is_not_a_security_signal(tmp_path):
    payload = envelope()
    payload["included"][0]["commentary"]["text"]["text"] = "A news story about a CAPTCHA and an account restriction"
    result = run(Journal(tmp_path / "journal.sqlite"), lambda: response(payload))
    assert result.page is not None


def test_hold_cannot_be_bypassed_with_an_identity_scope(tmp_path):
    journal = Journal(tmp_path / "journal.sqlite")
    journal.stop(SCOPE, "restricted")
    with pytest.raises(AcquisitionFailure, match="invalid_scope"):
        journal.generation("linkedin.company-feed.another-account")


def test_timeout_attempt_is_durable_and_not_resent_on_restart(tmp_path):
    path = tmp_path / "journal.sqlite"
    calls = []
    def timeout():
        calls.append(1)
        raise TimeoutError("Synthetic failure")
    first = run(Journal(path), timeout)
    repeat = run(Journal(path), timeout)
    assert first.outcome == repeat.outcome == "transport_failure"
    assert repeat.replayed and repeat.body_sha256 is None and calls == [1]


def test_unrecognized_error_code_is_not_a_secret_or_type_crash(tmp_path):
    result = run(Journal(tmp_path / "journal.sqlite"), lambda: response({"errors": [{"code": {"untrusted": "value"}}]}))
    assert result.outcome == "parse_failure"
