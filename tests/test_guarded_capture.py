"""Physical Journal callback borrowing; synthetic temporary SQLite only."""
import json
import multiprocessing
import os
import sqlite3
from contextlib import contextmanager
from datetime import timedelta

import pytest

from nos_linkedin.acquisition import AcquisitionFailure, Journal, RoutePermission, SourceResponse, acquire_company_page
from test_parser import COMPANY, NOW, envelope

SCOPE = "linkedin.company-feed"


def permission():
    return RoutePermission(True, True, NOW + timedelta(days=1), "synthetic_fixture")


def response():
    return SourceResponse(200, "application/json", json.dumps(envelope()).encode(), NOW)


def capture_args(journal, **changes):
    return dict(scope=SCOPE, generation=journal.generation(SCOPE), attempt_id="guarded-one",
                feed_publisher_id=COMPANY, permission=permission(), now=NOW) | changes


def test_borrowed_connection_is_physical_writer_after_both_operation_checks(tmp_path):
    journal = Journal(tmp_path / "guarded.sqlite")
    with journal.connect() as connection:
        connection.execute("CREATE TABLE borrowed_writes (attempt_id TEXT)")
    checks, calls = [], []

    def send_guarded(connection):
        calls.append(connection)
        assert checks == [1, 1]
        assert connection.in_transaction
        assert connection.execute("SELECT outcome FROM responses").fetchone()[0] == "dispatch_unknown"
        # A distinct connection sees committed admission but cannot become a writer.
        with sqlite3.connect(journal.path, timeout=0) as observer:
            assert observer.execute("SELECT outcome FROM responses").fetchone()[0] == "dispatch_unknown"
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                observer.execute("BEGIN IMMEDIATE")
        with pytest.raises(sqlite3.OperationalError, match="within a transaction"):
            connection.execute("BEGIN IMMEDIATE")
        connection.execute("INSERT INTO borrowed_writes VALUES (?)", ("guarded-one",))
        return response()

    result = acquire_company_page(journal=journal, **capture_args(journal),
                                  send_guarded=send_guarded, operation_guard=lambda: checks.append(1))
    assert result.page is not None and len(calls) == 1
    with journal.connect() as connection:
        assert connection.execute("SELECT attempt_id FROM borrowed_writes").fetchone()[0] == "guarded-one"


@pytest.mark.parametrize("callbacks", [{}, {"send": 1}, {"send_guarded": False},
                                      {"send": response, "send_guarded": lambda _: response()}])
def test_invalid_transport_selection_refuses_before_admission(tmp_path, callbacks):
    journal = Journal(tmp_path / "invalid.sqlite")
    args = capture_args(journal)
    with pytest.raises(AcquisitionFailure, match="invalid_transport_callback"):
        journal.capture(**args, **callbacks)
    assert journal.read(args["attempt_id"], NOW) is None


def test_replay_calls_neither_transport_and_preserves_default_compatibility(tmp_path):
    journal = Journal(tmp_path / "replay.sqlite")
    args = capture_args(journal)
    first = acquire_company_page(journal=journal, **args, send=response)
    calls = []
    replay = acquire_company_page(journal=journal, **args,
                                  send_guarded=lambda connection: calls.append(connection) or response())
    assert replay.replayed and replay.body_sha256 == first.body_sha256 and calls == []
    again = acquire_company_page(journal=journal, **args, send=lambda: calls.append(1) or response())
    assert again.replayed and calls == []


def test_independent_stop_between_admission_and_physical_guard_calls_nothing(tmp_path):
    journal = Journal(tmp_path / "stop.sqlite")
    args = capture_args(journal)
    connect = journal.connect
    stopped = False

    @contextmanager
    def stop_after_commit():
        nonlocal stopped
        with connect() as connection:
            yield connection
        if not stopped:
            stopped = True
            Journal(journal.path).stop(SCOPE, "operator_stop")

    journal.connect = stop_after_commit
    calls = []
    with pytest.raises(AcquisitionFailure, match="dispatch_fenced"):
        journal.capture(**args, send_guarded=lambda connection: calls.append(connection) or response())
    assert calls == []
    assert Journal(journal.path).read(args["attempt_id"], NOW)["outcome"] == "dispatch_unknown"


def test_operation_and_classification_guards_precede_borrow(tmp_path):
    journal = Journal(tmp_path / "guards.sqlite")
    calls, checks = [], []

    def operation_guard():
        checks.append(1)
        if len(checks) == 2:
            raise AcquisitionFailure("owner_expired")

    with pytest.raises(AcquisitionFailure, match="owner_expired"):
        journal.capture(**capture_args(journal), operation_guard=operation_guard,
                        send_guarded=lambda connection: calls.append(connection) or response())
    assert calls == []
    # A separate captured/unclassified response keeps the normal dispatch barrier.
    journal.capture(**capture_args(journal, attempt_id="pending"), send=response)
    with pytest.raises(AcquisitionFailure, match="classification_pending"):
        journal.capture(**capture_args(journal, attempt_id="new"),
                        send_guarded=lambda connection: calls.append(connection) or response())
    assert calls == []


def test_guarded_callback_failure_is_durable_and_does_not_resend(tmp_path):
    journal = Journal(tmp_path / "failure.sqlite")
    args = capture_args(journal)
    calls = []

    def fail(connection):
        assert connection.in_transaction
        calls.append(1)
        raise TimeoutError("synthetic transport failure")

    first = acquire_company_page(journal=journal, **args, send_guarded=fail)
    replay = acquire_company_page(journal=Journal(journal.path), **args, send_guarded=fail)
    assert first.outcome == replay.outcome == "transport_failure"
    assert replay.replayed and calls == [1]


def _exit_guarded_owner(path, witness):
    journal = Journal(path)

    def die(connection):
        assert connection.in_transaction
        with open(witness, "w", encoding="ascii") as output:
            output.write("physical callback entered")
        os._exit(17)

    journal.capture(**capture_args(journal), send_guarded=die)


def test_abrupt_guarded_owner_death_keeps_unknown_attempt_without_resend(tmp_path):
    path, witness = tmp_path / "death.sqlite", tmp_path / "entered.txt"
    process = multiprocessing.get_context("spawn").Process(target=_exit_guarded_owner, args=(path, witness))
    try:
        process.start()
        process.join(10)
        assert process.exitcode == 17 and witness.read_text(encoding="ascii") == "physical callback entered"
        journal = Journal(path)
        calls = []
        replay = acquire_company_page(journal=journal, **capture_args(journal),
                                      send_guarded=lambda connection: calls.append(connection) or response())
        assert replay.replayed and replay.outcome == "dispatch_unknown" and calls == []
        assert replay.page is None and replay.body_sha256 is None
    finally:
        if process.is_alive():
            process.terminate()
            process.join(3)
