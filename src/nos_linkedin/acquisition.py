"""One-attempt response capture with explicit retention and durable stop state.

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
from .parser import EVIDENCE_CLASSES, native_id, parse_company_feed


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
class BatchPageSpec:
    attempt_id: str
    ordinal: int
    start: int
    count: int
    page_mode: str


@dataclass(frozen=True)
class BatchPageResponse:
    spec: BatchPageSpec
    response: SourceResponse


@dataclass(frozen=True)
class SourceBatchResponse:
    pages: tuple[BatchPageResponse, ...]
    shortfall_reason: str | None = None


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
                CREATE TABLE IF NOT EXISTS delivery_receipts (
                  attempt_id TEXT PRIMARY KEY, generation INTEGER NOT NULL,
                  body_sha256 TEXT NOT NULL, eligibility_checked_at REAL NOT NULL,
                  outcome TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS response_batches (
                  parent_attempt_id TEXT PRIMARY KEY, run_id TEXT NOT NULL UNIQUE,
                  declared_page_budget INTEGER NOT NULL, available_page_count INTEGER,
                  shortfall_reason TEXT, state TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS response_batch_pages (
                  parent_attempt_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
                  attempt_id TEXT NOT NULL UNIQUE, start INTEGER NOT NULL,
                  count INTEGER NOT NULL, page_mode TEXT NOT NULL,
                  PRIMARY KEY (parent_attempt_id, ordinal));
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

    @staticmethod
    def _purge_expired(connection, now: datetime):
        return connection.execute("""UPDATE responses SET body=NULL,
            outcome=CASE WHEN outcome='captured' THEN 'classification_expired'
                         ELSE 'policy_expired' END
            WHERE expires_at<=? AND body IS NOT NULL""", (_time(now),)).rowcount

    def purge_expired(self, now: datetime) -> int:
        """Remove expired retained bytes without requiring a result read.

        Unclassified expiry retains its dispatch barrier; policy expiry is
        not a source deletion or permission to resend an unknown attempt.
        """
        with self.connect() as connection:
            return self._purge_expired(connection, now)

    def read(self, attempt_id: str, now: datetime):
        with self.connect() as connection:
            self._purge_expired(connection, now)
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

    def batch_for_attempt(self, attempt_id: str) -> dict | None:
        """Read-only reservation/member metadata, never a second body copy."""
        with self.connect() as connection:
            return self._batch_for_attempt(connection, attempt_id)

    @staticmethod
    def _batch_for_attempt(connection, attempt_id):
        row = connection.execute('''SELECT b.* FROM response_batches b JOIN response_batch_pages p
            ON p.parent_attempt_id=b.parent_attempt_id WHERE p.attempt_id=?''', (attempt_id,)).fetchone()
        if row is None:
            return None
        pages = connection.execute('SELECT * FROM response_batch_pages WHERE parent_attempt_id=? ORDER BY ordinal',
                                   (row['parent_attempt_id'],)).fetchall()
        result = dict(row)
        if row['available_page_count'] is not None:
            pages = pages[:row['available_page_count']]
        result['pages'] = tuple(BatchPageSpec(p['attempt_id'], p['ordinal'], p['start'], p['count'], p['page_mode']) for p in pages)
        return result

    @staticmethod
    def _validated_response(response):
        if not isinstance(response, SourceResponse) or not isinstance(response.body, bytes) or len(response.body) > 8_000_000:
            raise AcquisitionFailure('invalid_or_oversize_response')
        captured = _time(response.captured_at)
        if type(response.status) is not int or not (100 <= response.status <= 599 or response.status == 999):
            raise AcquisitionFailure('invalid_response_status')
        if not isinstance(response.content_type, str) or len(response.content_type) > 200:
            raise AcquisitionFailure('invalid_content_type')
        return captured

    def _store_batch(self, connection, response, *, attempt_id, scope, generation, feed_publisher_id, permission):
        batch = self._batch_for_attempt(connection, attempt_id)
        if (batch is None or batch['parent_attempt_id'] != attempt_id or batch['state'] != 'reserved'
                or not isinstance(response.pages, tuple) or len(response.pages) not in {2, 3}
                or len(response.pages) > batch['declared_page_budget']):
            raise AcquisitionFailure('invalid_batch_response')
        expected_reason = 'continuation_not_observed' if len(response.pages) < batch['declared_page_budget'] else None
        if response.shortfall_reason != expected_reason:
            raise AcquisitionFailure('invalid_batch_shortfall')
        captured_times = []
        for expected, page in zip(batch['pages'], response.pages):
            if not isinstance(page, BatchPageResponse) or page.spec != expected:
                raise AcquisitionFailure('batch_member_context_conflict')
            captured = self._validated_response(page.response)
            if captured >= _time(permission.raw_expires_at):
                raise AcquisitionFailure('batch_capture_after_expiry')
            if expected.ordinal and connection.execute('SELECT 1 FROM responses WHERE attempt_id=?', (expected.attempt_id,)).fetchone():
                raise AcquisitionFailure('batch_member_already_captured')
            captured_times.append(captured)
        if captured_times != sorted(captured_times):
            raise AcquisitionFailure('batch_capture_time_order')
        # All member validation precedes the first write. The caller owns the
        # physical transaction, so no partial batch can survive a write error.
        for page, captured in zip(response.pages, captured_times):
            value = page.response
            if page.spec.ordinal:
                connection.execute("INSERT INTO responses VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'captured')",
                    (page.spec.attempt_id, scope, generation, value.status, value.content_type, captured,
                     permission.evidence_class, hashlib.sha256(value.body).hexdigest(), _time(permission.raw_expires_at), value.body))
                connection.execute('INSERT INTO attempt_context VALUES (?, ?)', (page.spec.attempt_id, feed_publisher_id))
            else:
                connection.execute("UPDATE responses SET status=?, content_type=?, captured_at=?, body_sha256=?, body=?, outcome='captured' WHERE attempt_id=?",
                    (value.status, value.content_type, captured, hashlib.sha256(value.body).hexdigest(), value.body, attempt_id))
        connection.execute("UPDATE response_batches SET available_page_count=?, shortfall_reason=?, state='captured' WHERE parent_attempt_id=?",
                           (len(response.pages), response.shortfall_reason, attempt_id))

    def capture(self, *, scope: str, generation: int, attempt_id: str,
                feed_publisher_id: str, permission: RoutePermission,
                now: datetime, send: Callable[[], SourceResponse] | None = None,
                send_guarded: Callable[[sqlite3.Connection], SourceResponse] | None = None,
                operation_guard: Callable[[], object] | None = None):
        """Dispatch once, optionally lending the physical source transaction.

        A guarded transport borrows the live writer connection after all
        dispatch guards. It must not begin, commit, roll back or close it.
        This is a trusted callback contract, not an OS security boundary.
        """
        if ((send is None) == (send_guarded is None)
                or send is not None and not callable(send)
                or send_guarded is not None and not callable(send_guarded)):
            raise AcquisitionFailure("invalid_transport_callback")
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
        if permission.evidence_class not in EVIDENCE_CLASSES:
            raise AcquisitionFailure("invalid_evidence_class")
        with self.connect() as connection:
            # Commit admission before dispatch. A process death cannot erase
            # the attempt and cause replay to silently send it again.
            connection.execute("BEGIN IMMEDIATE")
            state = connection.execute("SELECT * FROM route_state WHERE scope=?", (scope,)).fetchone()
            if not state or state["stopped"] or state["generation"] != generation:
                raise AcquisitionFailure("dispatch_fenced")
            if operation_guard is not None:
                operation_guard()
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
            batch = self._batch_for_attempt(connection, attempt_id)
            if batch is not None and batch['parent_attempt_id'] != attempt_id:
                raise AcquisitionFailure('batch_member_unavailable')
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
            if operation_guard is not None:
                operation_guard()
            try:
                response = send_guarded(connection) if send_guarded is not None else send()
            except Exception:
                # Persist that this attempt actually dispatched. Reusing the
                # same attempt must never silently send again after a timeout.
                connection.execute("UPDATE responses SET outcome='transport_failure' WHERE attempt_id=?", (attempt_id,))
                return False
            try:
                if isinstance(response, SourceBatchResponse):
                    self._store_batch(connection, response, attempt_id=attempt_id, scope=scope,
                        generation=generation, feed_publisher_id=feed_publisher_id, permission=permission)
                    return False
                if self._batch_for_attempt(connection, attempt_id) is not None:
                    raise AcquisitionFailure('batch_response_required')
                captured = self._validated_response(response)
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
                now: datetime, send: Callable[[], object],
                operation_guard: Callable[[], object] | None = None) -> str:
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
            if operation_guard is not None:
                operation_guard()
            try:
                send()
            except Exception:
                connection.execute("INSERT OR REPLACE INTO delivery_receipts VALUES (?, ?, ?, ?, ?)",
                                   (attempt_id, generation, record["body_sha256"], timestamp, "delivery_failure"))
                return "delivery_failure"
            connection.execute("INSERT OR REPLACE INTO delivery_receipts VALUES (?, ?, ?, ?, ?)",
                               (attempt_id, generation, record["body_sha256"], timestamp, "delivery_acknowledged"))
        return "delivery_acknowledged"


def acquire_company_page(*, journal: Journal, scope: str, generation: int,
                         permission: RoutePermission, feed_publisher_id: str,
                         now: datetime, send: Callable[[], SourceResponse] | None = None,
                         send_guarded: Callable[[sqlite3.Connection], SourceResponse] | None = None,
                         attempt_id: str | None = None,
                         operation_guard: Callable[[], object] | None = None,
                         page_validator: Callable[[ParsedPage], object] | None = None) -> AcquisitionResult:
    attempt_id = attempt_id or uuid4().hex
    replayed = journal.capture(scope=scope, generation=generation, attempt_id=attempt_id,
                               feed_publisher_id=feed_publisher_id, permission=permission, now=now, send=send,
                               send_guarded=send_guarded, operation_guard=operation_guard)
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
            if page_validator is not None:
                page_validator(page)
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


def classify_company_batch(*, journal: Journal, parent_attempt_id: str, permission: RoutePermission,
                           now: datetime, operation_guard: Callable[[], object] | None = None) -> tuple[AcquisitionResult, ...]:
    """Classify every saved member before first delivery, with zero dispatch.

    Existing capture replay bypasses the classification barrier without
    weakening it. Any member's security signal holds the whole source.
    """
    batch = journal.batch_for_attempt(parent_attempt_id)
    if batch is None or batch['parent_attempt_id'] != parent_attempt_id or batch['state'] != 'captured':
        raise AcquisitionFailure('batch_not_captured')
    with journal.connect() as connection:
        parent = connection.execute('SELECT generation FROM responses WHERE attempt_id=?', (parent_attempt_id,)).fetchone()
        context = connection.execute('SELECT feed_publisher_id FROM attempt_context WHERE attempt_id=?', (parent_attempt_id,)).fetchone()
    if parent is None or context is None:
        raise AcquisitionFailure('batch_parent_context_missing')

    def no_dispatch():
        raise AcquisitionFailure('batch_replay_must_not_dispatch')

    results, failed = [], False
    for spec in batch['pages']:
        record = journal.read(spec.attempt_id, now)
        if (record is None or record['body'] is None or record['generation'] != parent['generation']
                or record['evidence_class'] != permission.evidence_class
                or record['outcome'] not in {'captured', 'bounded', 'partial'}
                or hashlib.sha256(record['body']).hexdigest() != record['body_sha256']):
            failed = True
            continue
        try:
            result = acquire_company_page(journal=journal, scope='linkedin.company-feed', generation=parent['generation'],
                permission=permission, feed_publisher_id=context['feed_publisher_id'], attempt_id=spec.attempt_id,
                now=now, send=no_dispatch, operation_guard=operation_guard,
                page_validator=lambda page, selected=spec: _validate_batch_page(selected, page))
        except AcquisitionFailure:
            failed = True
            continue
        results.append(result)
        if result.outcome not in {'bounded', 'partial'}:
            failed = True
            if result.outcome in {'restricted', 'authentication_required', 'challenge', 'quarantined_after_stop'}:
                break
    if failed:
        raise AcquisitionFailure('batch_member_not_eligible')
    return tuple(results)


def _validate_batch_page(spec: BatchPageSpec, page: ParsedPage):
    if page.paging.get('start') != spec.start or page.paging.get('count') != spec.count:
        raise ParseFailure('batch_page_paging_conflict')


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
