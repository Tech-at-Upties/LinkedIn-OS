"""One-attempt, explicitly permitted response capture with durable stop state.

No browser/API session recipe or automatic live collector is implemented here.
SQLite serialization is an isolated primitive, not the NOS Postgres fence gate.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Callable
from uuid import uuid4

from .models import ParsedPage
from .parser import native_id, parse_company_feed


class AcquisitionFailure(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class RoutePermission:
    collection_allowed: bool
    raw_retention_allowed: bool
    raw_expires_at: datetime
    evidence_class: str


@dataclass(frozen=True)
class SourceResponse:
    status: int
    content_type: str
    body: bytes = field(repr=False)
    captured_at: datetime


@dataclass(frozen=True)
class AcquisitionResult:
    attempt_id: str
    outcome: str
    body_sha256: str | None
    page: ParsedPage | None
    replayed: bool = False


def _time(value: datetime) -> float:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise AcquisitionFailure("aware_time_required")
    return value.timestamp()


def _scope(scope: str) -> None:
    # One source-wide scope for the implemented slice. It never contains an
    # account, session or company ID, so substitution cannot bypass a hold.
    if scope != "linkedin.company-feed":
        raise AcquisitionFailure("invalid_scope")


class Journal:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS route_state (
                  scope TEXT PRIMARY KEY, generation INTEGER NOT NULL,
                  stopped INTEGER NOT NULL, reason TEXT);
                CREATE TABLE IF NOT EXISTS responses (
                  attempt_id TEXT PRIMARY KEY, scope TEXT NOT NULL, generation INTEGER NOT NULL,
                  status INTEGER, content_type TEXT, captured_at REAL,
                  evidence_class TEXT NOT NULL, body_sha256 TEXT,
                  expires_at REAL NOT NULL, body BLOB, outcome TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS response_classification
                  ON responses(scope, outcome);
                CREATE TABLE IF NOT EXISTS attempt_context (
                  attempt_id TEXT PRIMARY KEY, feed_publisher_id TEXT NOT NULL);
            """)

    @contextmanager
    def connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def generation(self, scope: str) -> int:
        _scope(scope)
        with self.connect() as connection:
            connection.execute("INSERT OR IGNORE INTO route_state VALUES (?, 0, 0, NULL)", (scope,))
            row = connection.execute("SELECT * FROM route_state WHERE scope=?", (scope,)).fetchone()
            if row["stopped"]:
                raise AcquisitionFailure("durable_hold")
            return row["generation"]

    def stop(self, scope: str, reason: str) -> None:
        _scope(scope)
        if reason not in {"challenge", "restricted", "authentication_required", "operator_stop"}:
            raise AcquisitionFailure("invalid_stop_reason")
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("INSERT OR IGNORE INTO route_state VALUES (?, 0, 0, NULL)", (scope,))
            connection.execute("UPDATE route_state SET generation=generation+1, stopped=1, reason=? WHERE scope=?", (reason, scope))

    def read(self, attempt_id: str, now: datetime):
        with self.connect() as connection:
            connection.execute("""UPDATE responses SET body=NULL,
                outcome=CASE WHEN outcome='captured' THEN 'classification_expired'
                             ELSE 'policy_expired' END
                WHERE expires_at<=? AND body IS NOT NULL""", (_time(now),))
            row = connection.execute("SELECT * FROM responses WHERE attempt_id=?", (attempt_id,)).fetchone()
            return dict(row) if row else None

    @staticmethod
    def _require_classified(connection, scope: str) -> None:
        # A committed response might contain an access/security signal. Its
        # owner may have exited before classifying it. Replay that attempt
        # before dispatching new work; absence after expiry is not clearance.
        pending = connection.execute("""SELECT 1 FROM responses WHERE scope=?
            AND outcome IN ('captured', 'classification_expired') LIMIT 1""", (scope,)).fetchone()
        if pending:
            raise AcquisitionFailure("classification_pending")

    def capture(self, *, scope: str, generation: int, attempt_id: str,
                feed_publisher_id: str, permission: RoutePermission,
                now: datetime, send: Callable[[], SourceResponse]):
        _scope(scope)
        if type(generation) is not int or generation < 0:
            raise AcquisitionFailure("invalid_generation")
        if not isinstance(attempt_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,200}", attempt_id):
            raise AcquisitionFailure("invalid_attempt_id")
        if not native_id(feed_publisher_id):
            raise AcquisitionFailure("invalid_feed_publisher")
        if permission.collection_allowed is not True or permission.raw_retention_allowed is not True:
            raise AcquisitionFailure("route_permission_required")
        expires = _time(permission.raw_expires_at)
        if expires <= _time(now):
            raise AcquisitionFailure("retention_policy_expired")
        if permission.evidence_class not in {"native_response", "synthetic_fixture"}:
            raise AcquisitionFailure("original_bytes_evidence_class_required")
        with self.connect() as connection:
            # Commit admission before dispatch. A process death cannot erase
            # the attempt and cause replay to silently send it again.
            connection.execute("BEGIN IMMEDIATE")
            state = connection.execute("SELECT * FROM route_state WHERE scope=?", (scope,)).fetchone()
            if not state or state["stopped"] or state["generation"] != generation:
                raise AcquisitionFailure("dispatch_fenced")
            existing = connection.execute("SELECT scope, generation, evidence_class FROM responses WHERE attempt_id=?", (attempt_id,)).fetchone()
            if existing:
                context = connection.execute("SELECT feed_publisher_id FROM attempt_context WHERE attempt_id=?", (attempt_id,)).fetchone()
                if not context:
                    raise AcquisitionFailure("legacy_attempt_context_missing")
                if (existing["scope"] != scope or existing["generation"] != generation
                        or context["feed_publisher_id"] != feed_publisher_id
                        or existing["evidence_class"] != permission.evidence_class):
                    raise AcquisitionFailure("attempt_identity_conflict")
                return True
            self._require_classified(connection, scope)
            connection.execute("INSERT INTO responses VALUES (?, ?, ?, NULL, NULL, NULL, ?, NULL, ?, NULL, 'dispatch_unknown')", (
                attempt_id, scope, generation, permission.evidence_class, expires,
            ))
            connection.execute("INSERT INTO attempt_context VALUES (?, ?)", (attempt_id, feed_publisher_id))
        with self.connect() as connection:
            # Recheck after admission, at physical dispatch. An in-flight send
            # finishes before a concurrent stop can commit. Only the caller
            # that inserted the admission may invoke the transport.
            connection.execute("BEGIN IMMEDIATE")
            state = connection.execute("SELECT * FROM route_state WHERE scope=?", (scope,)).fetchone()
            if not state or state["stopped"] or state["generation"] != generation:
                raise AcquisitionFailure("dispatch_fenced")
            self._require_classified(connection, scope)
            try:
                response = send()
            except Exception:
                # Persist that this attempt actually dispatched. Reusing the
                # same attempt must never silently send again after a timeout.
                connection.execute("UPDATE responses SET outcome='transport_failure' WHERE attempt_id=?", (attempt_id,))
                return False
            try:
                if not isinstance(response, SourceResponse) or not isinstance(response.body, bytes) or len(response.body) > 8_000_000:
                    raise AcquisitionFailure("invalid_or_oversize_response")
                captured = _time(response.captured_at)
                if type(response.status) is not int or not (100 <= response.status <= 599 or response.status == 999):
                    raise AcquisitionFailure("invalid_response_status")
                if not isinstance(response.content_type, str) or len(response.content_type) > 200:
                    raise AcquisitionFailure("invalid_content_type")
            except AcquisitionFailure:
                connection.execute("UPDATE responses SET outcome='invalid_response' WHERE attempt_id=?", (attempt_id,))
                return False
            connection.execute("UPDATE responses SET status=?, content_type=?, captured_at=?, body_sha256=?, body=?, outcome='captured' WHERE attempt_id=?", (
                response.status, response.content_type, captured,
                hashlib.sha256(response.body).hexdigest(), response.body, attempt_id,
            ))
        # The original bytes are durably committed before JSON decoding or parsing.
        return False

    def finish(self, scope: str, generation: int, attempt_id: str, outcome: str) -> str:
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            state = connection.execute("SELECT * FROM route_state WHERE scope=?", (scope,)).fetchone()
            if not state or state["stopped"] or state["generation"] != generation:
                outcome = "quarantined_after_stop"
            connection.execute("UPDATE responses SET outcome=? WHERE attempt_id=? AND scope=?", (outcome, attempt_id, scope))
        return outcome

    def deliver(self, *, scope: str, generation: int, attempt_id: str,
                now: datetime, send: Callable[[], object]) -> str:
        """Fence one explicit delivery callback; it must acknowledge or raise.

        A callback failure may follow a remote commit. Explicit replay must
        reuse the same downstream delivery identity. No automatic retry or
        durable delivery outbox is provided by this primitive.
        """
        _scope(scope)
        timestamp = _time(now)
        with self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            record = connection.execute("SELECT * FROM responses WHERE attempt_id=?", (attempt_id,)).fetchone()
            if (not record or record["scope"] != scope or record["generation"] != generation
                    or type(generation) is not int):
                raise AcquisitionFailure("attempt_identity_conflict")
            state = connection.execute("SELECT * FROM route_state WHERE scope=?", (scope,)).fetchone()
            if not state or state["stopped"] or state["generation"] != generation:
                connection.execute("UPDATE responses SET outcome='quarantined_after_stop' WHERE attempt_id=?", (attempt_id,))
                return "quarantined_after_stop"
            self._require_classified(connection, scope)
            if record["expires_at"] <= timestamp:
                connection.execute("UPDATE responses SET body=NULL, outcome='policy_expired' WHERE attempt_id=?", (attempt_id,))
                return "policy_expired"
            if record["outcome"] not in {"bounded", "partial"} or record["body"] is None:
                raise AcquisitionFailure("delivery_not_eligible")
            try:
                send()
            except Exception:
                return "delivery_failure"
        return "delivery_acknowledged"


def acquire_company_page(*, journal: Journal, scope: str, generation: int,
                         permission: RoutePermission, feed_publisher_id: str,
                         send: Callable[[], SourceResponse], now: datetime,
                         attempt_id: str | None = None) -> AcquisitionResult:
    attempt_id = attempt_id or uuid4().hex
    replayed = journal.capture(scope=scope, generation=generation, attempt_id=attempt_id,
                               feed_publisher_id=feed_publisher_id, permission=permission, now=now, send=send)
    record = journal.read(attempt_id, now)
    if record["outcome"] in {"dispatch_unknown", "transport_failure", "invalid_response"}:
        return AcquisitionResult(attempt_id, record["outcome"], None, None, replayed)
    status = record["status"]
    if status in {401, 403, 429, 999}:
        reason = "authentication_required" if status == 401 else "restricted"
        journal.stop(scope, reason)
        outcome = reason
    elif record["body"] is None:
        return AcquisitionResult(attempt_id, record["outcome"], record["body_sha256"], None, replayed)
    elif not 200 <= status < 300:
        outcome = "http_failure"
    elif "json" not in record["content_type"].lower():
        outcome = "representation_drift"
    else:
        try:
            body = json.loads(record["body"])
            stop_reason = _semantic_stop(body)
            if stop_reason:
                journal.stop(scope, stop_reason)
                with journal.connect() as connection:
                    connection.execute("UPDATE responses SET outcome=? WHERE attempt_id=?", (stop_reason, attempt_id))
                return AcquisitionResult(attempt_id, stop_reason, record["body_sha256"], None, replayed)
            page = parse_company_feed(body, feed_publisher_id=feed_publisher_id,
                                      observed_at=datetime.fromtimestamp(record["captured_at"], UTC),
                                      evidence_class=record["evidence_class"])
        except (ValueError, UnicodeDecodeError, RecursionError):
            outcome = "parse_failure"
        else:
            outcome = journal.finish(scope, generation, attempt_id, "partial" if page.issues else "bounded")
            return AcquisitionResult(attempt_id, outcome, record["body_sha256"],
                                     None if outcome == "quarantined_after_stop" else page, replayed)
    # Stop classifications retain their actual reason, while accepted data is
    # separately subject to a generation check immediately before emission.
    if outcome in {"restricted", "authentication_required"}:
        with journal.connect() as connection:
            connection.execute("UPDATE responses SET outcome=? WHERE attempt_id=?", (outcome, attempt_id))
    else:
        outcome = journal.finish(scope, generation, attempt_id, outcome)
    return AcquisitionResult(attempt_id, outcome, record["body_sha256"], None, replayed)


def _semantic_stop(body) -> str | None:
    """Conservative error-envelope checks, never publication-text matching.

    Synthetic contract tests only. Live error/challenge precision is unproved.
    """
    if not isinstance(body, dict):
        return None
    containers = [body]
    if isinstance(body.get("data"), dict):
        containers.append(body["data"])
        if isinstance(body["data"].get("data"), dict):
            containers.append(body["data"]["data"])
    for container in containers:
        for key in ("errors", "error"):
            errors = container.get(key)
            errors = errors if isinstance(errors, list) else [errors]
            for error in errors[:20]:
                if not isinstance(error, dict):
                    continue
                status = error.get("status")
                code = error.get("code") if isinstance(error.get("code"), str) else None
                if code == "CHALLENGE":
                    return "challenge"
                if (type(status) is int and status == 401) or code == "AUTH_REQUIRED":
                    return "authentication_required"
                if (type(status) is int and status in {403, 429, 999}) or code in {"ACCESS_DENIED", "RATE_LIMITED"}:
                    return "restricted"
    return None
