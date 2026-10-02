import json
import multiprocessing
import os
import threading
from datetime import timedelta
from contextlib import contextmanager
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


def crash_after_request(path, url):
    def send():
        with urlopen(url, timeout=3) as result:
            result.read()
        os._exit(7)
    run(Journal(path), send)


def test_process_crash_after_physical_request_cannot_resend_attempt(tmp_path):
    requests = []
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"{}")
        def log_message(self, *_):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    path = tmp_path / "journal.sqlite"
    process = multiprocessing.get_context("spawn").Process(
        target=crash_after_request,
        args=(path, f"http://127.0.0.1:{server.server_port}/crash"),
    )
    try:
        process.start()
        process.join(10)
        assert process.exitcode == 7 and requests == ["/crash"]
        calls = []
        replay = run(Journal(path), lambda: calls.append(1) or response())
        assert calls == []
        assert replay.replayed and replay.outcome == "dispatch_unknown"
        assert replay.body_sha256 is None and replay.page is None
    finally:
        if process.is_alive():
            process.terminate()
            process.join(3)
        server.shutdown()
        server.server_close()
        thread.join(3)


@pytest.mark.parametrize("invalid", [
    None,
    SourceResponse(200, "application/json", b"x" * 8_000_001, NOW),
    SourceResponse(True, "application/json", b"{}", NOW),
    SourceResponse(200, None, b"{}", NOW),
    SourceResponse(200, "application/json", b"{}", NOW.replace(tzinfo=None)),
])
def test_invalid_response_is_durable_and_not_resent(invalid, tmp_path):
    path = tmp_path / "journal.sqlite"
    calls = []
    first = run(Journal(path), lambda: calls.append(1) or invalid)
    replay = run(Journal(path), lambda: calls.append(1) or response())
    assert first.outcome == replay.outcome == "invalid_response"
    assert replay.replayed and calls == [1]
    assert replay.page is None and replay.body_sha256 is None


def test_stop_committed_between_admission_and_dispatch_has_zero_requests(tmp_path):
    path = tmp_path / "journal.sqlite"
    journal = Journal(path)
    generation = journal.generation(SCOPE)
    connect = journal.connect
    stopped = False
    @contextmanager
    def stop_after_commit():
        nonlocal stopped
        with connect() as connection:
            yield connection
        if not stopped:
            stopped = True
            Journal(path).stop(SCOPE, "operator_stop")
    journal.connect = stop_after_commit
    calls = []
    with pytest.raises(AcquisitionFailure, match="dispatch_fenced"):
        run(journal, lambda: calls.append(1) or response(), generation=generation)
    assert calls == []
    assert Journal(path).read("attempt-one", NOW)["outcome"] == "dispatch_unknown"


def capture_without_classifying(journal, payload, *, attempt_id="unclassified"):
    return journal.capture(scope=SCOPE, generation=journal.generation(SCOPE),
                           attempt_id=attempt_id, feed_publisher_id=COMPANY, permission=permission(), now=NOW,
                           send=lambda: payload)


def test_replay_cannot_relabel_a_saved_response_as_another_company_feed(tmp_path):
    path = tmp_path / "journal.sqlite"
    calls = []
    first = run(Journal(path), lambda: calls.append(1) or response())
    assert first.page.publications[0].feed_publisher_id == COMPANY
    with pytest.raises(AcquisitionFailure, match="attempt_identity_conflict"):
        run(Journal(path), lambda: calls.append(1) or response(), feed_publisher_id="urn:li:fsd_company:9999")
    assert calls == [1]


@pytest.mark.parametrize("changes", [
    {"collection_allowed": "false"}, {"collection_allowed": 1},
    {"raw_retention_allowed": "true"}, {"raw_retention_allowed": 1},
])
def test_non_boolean_permission_never_dispatches(changes, tmp_path):
    calls = []
    with pytest.raises(AcquisitionFailure, match="route_permission_required"):
        run(Journal(tmp_path / "journal.sqlite"), lambda: calls.append(1) or response(), permission=permission(**changes))
    assert calls == []


def test_replay_cannot_change_original_evidence_class(tmp_path):
    path = tmp_path / "journal.sqlite"
    calls = []
    run(Journal(path), lambda: calls.append(1) or response())
    with pytest.raises(AcquisitionFailure, match="attempt_identity_conflict"):
        run(Journal(path), lambda: calls.append(1) or response(), permission=permission(evidence_class="native_response"))
    assert calls == [1]


def test_legacy_receipt_stays_readable_without_guessing_feed_context(tmp_path):
    path = tmp_path / "journal.sqlite"
    calls = []
    journal = Journal(path)
    first = run(journal, lambda: calls.append(1) or response())
    with journal.connect() as connection:
        # This disposable test database represents the preceding schema.
        connection.execute("DROP TABLE attempt_context")
    reopened = Journal(path)
    assert reopened.read(first.attempt_id, NOW)["body_sha256"] == first.body_sha256
    with pytest.raises(AcquisitionFailure, match="legacy_attempt_context_missing"):
        run(reopened, lambda: calls.append(1) or response())
    assert calls == [1]


@pytest.mark.parametrize("payload", [response(), response({}, 403), response({"errors": [{"code": "CHALLENGE"}]})])
def test_saved_unclassified_response_blocks_next_attempt_after_reopen(payload, tmp_path):
    path = tmp_path / "journal.sqlite"
    capture_without_classifying(Journal(path), payload)
    calls = []
    with pytest.raises(AcquisitionFailure, match="classification_pending"):
        run(Journal(path), lambda: calls.append(1) or response(), attempt_id="next")
    assert calls == []
    assert Journal(path).read("unclassified", NOW)["body"] == payload.body


def test_benign_saved_response_replay_releases_classification_barrier(tmp_path):
    path = tmp_path / "journal.sqlite"
    capture_without_classifying(Journal(path), response())
    calls = []
    first = run(Journal(path), lambda: calls.append(1) or response(), attempt_id="unclassified")
    assert first.replayed and first.page is not None and calls == []
    next_result = run(Journal(path), lambda: calls.append(1) or response(), attempt_id="next")
    assert next_result.page is not None and calls == [1]


def test_saved_semantic_challenge_replay_commits_hold_without_resending(tmp_path):
    path = tmp_path / "journal.sqlite"
    capture_without_classifying(Journal(path), response({"errors": [{"code": "CHALLENGE"}]}))
    calls = []
    result = run(Journal(path), lambda: calls.append(1) or response(), attempt_id="unclassified")
    assert result.replayed and result.outcome == "challenge" and calls == []
    with pytest.raises(AcquisitionFailure, match="durable_hold"):
        Journal(path).generation(SCOPE)


def test_expired_unclassified_body_preserves_uncertainty_and_dispatch_barrier(tmp_path):
    path = tmp_path / "journal.sqlite"
    capture_without_classifying(Journal(path), response({"errors": [{"code": "CHALLENGE"}]}))
    now = NOW + timedelta(days=2)
    journal = Journal(path)
    row = journal.read("unclassified", now)
    assert row["body"] is None and row["outcome"] == "classification_expired"
    calls = []
    permitted = permission(raw_expires_at=now + timedelta(days=1))
    replay = run(journal, lambda: calls.append(1) or response(), now=now,
                 permission=permitted, attempt_id="unclassified")
    assert replay.replayed and replay.outcome == "classification_expired" and replay.page is None
    with pytest.raises(AcquisitionFailure, match="classification_pending"):
        run(Journal(path), lambda: calls.append(1) or response(), now=now,
            permission=permitted, attempt_id="next")
    assert calls == []


def test_status_access_failure_remains_classifiable_after_raw_expiry(tmp_path):
    path = tmp_path / "journal.sqlite"
    capture_without_classifying(Journal(path), response({}, 403))
    now = NOW + timedelta(days=2)
    calls = []
    result = run(Journal(path), lambda: calls.append(1) or response(), now=now,
                 permission=permission(raw_expires_at=now + timedelta(days=1)), attempt_id="unclassified")
    assert result.replayed and result.outcome == "restricted" and calls == []
    assert Journal(path).read("unclassified", now)["body"] is None
    with pytest.raises(AcquisitionFailure, match="durable_hold"):
        Journal(path).generation(SCOPE)


def test_response_saved_after_admission_is_checked_at_physical_dispatch(tmp_path):
    path = tmp_path / "journal.sqlite"
    journal = Journal(path)
    generation = journal.generation(SCOPE)
    connect = journal.connect
    inserted = False
    @contextmanager
    def save_after_admission():
        nonlocal inserted
        with connect() as connection:
            yield connection
        if not inserted:
            inserted = True
            capture_without_classifying(Journal(path), response({}, 403))
    journal.connect = save_after_admission
    calls = []
    with pytest.raises(AcquisitionFailure, match="classification_pending"):
        run(journal, lambda: calls.append(1) or response(), generation=generation)
    assert calls == []


def exit_after_committing_response(path, url):
    journal = Journal(path)
    def send():
        with urlopen(url, timeout=3) as result:
            return SourceResponse(result.status, result.headers["Content-Type"], result.read(), NOW)
    journal.capture(scope=SCOPE, generation=journal.generation(SCOPE), attempt_id="unclassified",
                    feed_publisher_id=COMPANY, permission=permission(), now=NOW, send=send)
    os._exit(9)


def test_committed_security_response_after_process_exit_blocks_physical_request(tmp_path):
    requests = []
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"errors":[{"code":"CHALLENGE"}]}')
        def log_message(self, *_):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    path = tmp_path / "journal.sqlite"
    url = f"http://127.0.0.1:{server.server_port}/pending"
    process = multiprocessing.get_context("spawn").Process(target=exit_after_committing_response, args=(path, url))
    def send():
        with urlopen(url, timeout=3) as result:
            return SourceResponse(result.status, result.headers["Content-Type"], result.read(), NOW)
    try:
        process.start()
        process.join(10)
        assert process.exitcode == 9 and requests == ["/pending"]
        with pytest.raises(AcquisitionFailure, match="classification_pending"):
            run(Journal(path), send, attempt_id="next")
        replay = run(Journal(path), send, attempt_id="unclassified")
        assert replay.replayed and replay.outcome == "challenge"
        assert requests == ["/pending"]
        with pytest.raises(AcquisitionFailure, match="durable_hold"):
            Journal(path).generation(SCOPE)
    finally:
        if process.is_alive():
            process.terminate()
            process.join(3)
        server.shutdown()
        server.server_close()
        thread.join(3)
