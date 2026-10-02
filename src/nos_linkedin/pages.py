"""Bounded relevance-page ownership; never infers source completeness."""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from .acquisition import Journal
from .models import ParsedPage
from .parser import EVIDENCE_CLASSES, ParseFailure, native_id, parse_company_feed

SCOPE = 'linkedin.company-feed'
_LABEL = re.compile(r'[A-Za-z0-9_-]{1,120}\Z')


class PageRunFailure(RuntimeError):
    """A bounded local-state reason, never source content or credentials."""


def _time(value: datetime) -> float:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise PageRunFailure('aware_time_required')
    return value.timestamp()


def _label(value: str):
    if not isinstance(value, str) or not _LABEL.fullmatch(value):
        raise PageRunFailure('invalid_label')


@dataclass(frozen=True)
class PageLease:
    run_id: str
    attempt_id: str
    ordinal: int
    start: int
    count: int
    feed_publisher_id: str
    evidence_class: str
    generation: int
    owner: str
    token: str
    expires_at: datetime


class CompanyPageRuns:
    """Use the source Journal DB so ownership and dispatch share its writer lock.

    require_owned is read-only and may run inside Journal's operation_guard.
    State-changing methods must run outside source/delivery callbacks.
    """

    def __init__(self, journal: Journal):
        self.journal = journal
        with journal.connect() as connection:
            connection.executescript('''
                CREATE TABLE IF NOT EXISTS company_runs (
                  run_id TEXT PRIMARY KEY, feed_publisher_id TEXT NOT NULL,
                  evidence_class TEXT NOT NULL, generation INTEGER NOT NULL,
                  first_start INTEGER NOT NULL, requested_count INTEGER NOT NULL,
                  page_budget INTEGER NOT NULL, completed_pages INTEGER NOT NULL,
                  next_start INTEGER NOT NULL, state TEXT NOT NULL,
                  owner TEXT, lease_token TEXT, lease_until REAL);
                CREATE TABLE IF NOT EXISTS company_pages (
                  run_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
                  attempt_id TEXT NOT NULL UNIQUE, start INTEGER NOT NULL,
                  completed INTEGER NOT NULL DEFAULT 0, observed_total INTEGER,
                  publication_count INTEGER, body_sha256 TEXT,
                  PRIMARY KEY (run_id, ordinal));
            ''')

    @staticmethod
    def _source(connection, generation):
        row = connection.execute('SELECT * FROM route_state WHERE scope=?', (SCOPE,)).fetchone()
        if row is None or row['stopped'] or row['generation'] != generation:
            raise PageRunFailure('source_fenced')

    @staticmethod
    def _selected(connection, run_id):
        return connection.execute('''SELECT r.*, p.attempt_id, p.ordinal, p.start
            FROM company_runs r JOIN company_pages p ON p.run_id=r.run_id
            AND p.ordinal=r.completed_pages WHERE r.run_id=? AND p.completed=0''', (run_id,)).fetchone()

    @staticmethod
    def _ticket(row):
        return PageLease(row['run_id'], row['attempt_id'], row['ordinal'], row['start'],
                         row['requested_count'], row['feed_publisher_id'], row['evidence_class'],
                         row['generation'], row['owner'], row['lease_token'],
                         datetime.fromtimestamp(row['lease_until'], UTC))

    def _owned(self, connection, lease, timestamp, *, require_source=True):
        row = self._selected(connection, lease.run_id)
        if (row is None or row['state'] != 'active' or row['lease_until'] is None
                or row['lease_until'] <= timestamp or lease != self._ticket(row)):
            raise PageRunFailure('page_lease_fenced')
        if require_source:
            self._source(connection, row['generation'])
        return row

    def create(self, *, run_id: str, feed_publisher_id: str, evidence_class: str,
               first_start: int, count: int, page_budget: int):
        _label(run_id)
        if not native_id(feed_publisher_id) or evidence_class not in EVIDENCE_CLASSES:
            raise PageRunFailure('invalid_run_context')
        if (type(first_start) is not int or not 0 <= first_start <= 10_000_000
                or type(count) is not int or not 1 <= count <= 100
                or type(page_budget) is not int or not 1 <= page_budget <= 100):
            raise PageRunFailure('invalid_run_bounds')
        generation = self.journal.generation(SCOPE)
        context = (feed_publisher_id, evidence_class, generation, first_start, count, page_budget)
        with self.journal.connect() as connection:
            connection.execute('BEGIN IMMEDIATE')
            self._source(connection, generation)
            previous = connection.execute('SELECT * FROM company_runs WHERE run_id=?', (run_id,)).fetchone()
            if previous:
                if tuple(previous[key] for key in ('feed_publisher_id', 'evidence_class', 'generation',
                                                  'first_start', 'requested_count', 'page_budget')) != context:
                    raise PageRunFailure('run_context_conflict')
                return
            connection.execute('INSERT INTO company_runs VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, NULL, NULL, NULL)',
                               (run_id, *context, first_start, 'active'))

    def claim(self, run_id: str, *, owner: str, now: datetime, lease_seconds: int = 60) -> PageLease | None:
        _label(run_id)
        _label(owner)
        timestamp = _time(now)
        if type(lease_seconds) is not int or not 1 <= lease_seconds <= 3600:
            raise PageRunFailure('invalid_lease_seconds')
        with self.journal.connect() as connection:
            connection.execute('BEGIN IMMEDIATE')
            run = connection.execute('SELECT * FROM company_runs WHERE run_id=?', (run_id,)).fetchone()
            if run is None:
                raise PageRunFailure('unknown_run')
            self._source(connection, run['generation'])
            if run['state'] != 'active' or (run['lease_until'] is not None and run['lease_until'] > timestamp):
                return None
            # Preserve page/attempt identity when an owner exits or its lease expires.
            connection.execute('INSERT OR IGNORE INTO company_pages (run_id, ordinal, attempt_id, start) VALUES (?, ?, ?, ?)',
                               (run_id, run['completed_pages'], f'li-page-{uuid4().hex}', run['next_start']))
            connection.execute('UPDATE company_runs SET owner=?, lease_token=?, lease_until=? WHERE run_id=?',
                               (owner, uuid4().hex, timestamp + lease_seconds, run_id))
            return self._ticket(self._selected(connection, run_id))

    def require_owned(self, lease: PageLease, now: datetime):
        with self.journal.connect() as connection:
            self._owned(connection, lease, _time(now))

    @staticmethod
    def validate_page(lease: PageLease, page: ParsedPage):
        if (page.paging.get('start') != lease.start or page.paging.get('count') != lease.count
                or any(item.feed_publisher_id != lease.feed_publisher_id
                       or item.evidence_class != lease.evidence_class for item in page.publications)):
            raise ParseFailure('page_request_mismatch')

    def checkpoint(self, lease: PageLease, now: datetime) -> dict:
        timestamp = _time(now)
        with self.journal.connect() as connection:
            connection.execute('BEGIN IMMEDIATE')
            run = self._owned(connection, lease, timestamp)
            record = connection.execute('SELECT * FROM responses WHERE attempt_id=?', (lease.attempt_id,)).fetchone()
            ack = connection.execute('SELECT * FROM delivery_receipts WHERE attempt_id=?', (lease.attempt_id,)).fetchone()
            if (record is None or ack is None or ack['outcome'] != 'delivery_acknowledged'
                    or record['outcome'] not in {'bounded', 'partial'} or record['scope'] != SCOPE
                    or record['generation'] != lease.generation or ack['generation'] != lease.generation
                    or record['body_sha256'] != ack['body_sha256'] or record['evidence_class'] != lease.evidence_class):
                raise PageRunFailure('page_not_acknowledged')
            if record['body'] is None or record['expires_at'] <= timestamp:
                raise PageRunFailure('page_retention_expired')
            context = connection.execute('SELECT feed_publisher_id FROM attempt_context WHERE attempt_id=?',
                                         (lease.attempt_id,)).fetchone()
            if context is None or context[0] != lease.feed_publisher_id:
                raise PageRunFailure('page_context_conflict')
            page = parse_company_feed(json.loads(record['body']), feed_publisher_id=lease.feed_publisher_id,
                                      observed_at=datetime.fromtimestamp(record['captured_at'], UTC),
                                      evidence_class=lease.evidence_class)
            self.validate_page(lease, page)
            next_start = lease.start + lease.count
            if any(issue.code != 'preceding_items_not_in_this_page' for issue in page.issues):
                state = 'partial_graph'
            elif not page.publications:
                state = 'empty_page'
            elif len(page.publications) < lease.count:
                state = 'short_page'
            elif run['completed_pages'] + 1 >= run['page_budget']:
                state = 'budget_exhausted'
            elif next_start >= page.paging['total']:
                state = 'range_unproved'
            else:
                state = 'active'
            connection.execute('''UPDATE company_pages SET completed=1, observed_total=?,
                publication_count=?, body_sha256=? WHERE run_id=? AND ordinal=?''',
                               (page.paging['total'], len(page.publications), record['body_sha256'], lease.run_id, lease.ordinal))
            connection.execute('''UPDATE company_runs SET completed_pages=completed_pages+1,
                next_start=?, state=?, owner=NULL, lease_token=NULL, lease_until=NULL WHERE run_id=?''',
                               (next_start, state, lease.run_id))
        return self.summary(lease.run_id)

    def failed(self, lease: PageLease, *, source_outcome: str, delivery_outcome: str | None, now: datetime):
        """Only an ambiguous delivery can be explicitly reclaimed for same-page replay."""
        state = 'active' if source_outcome in {'bounded', 'partial'} and delivery_outcome == 'delivery_failure' else 'source_failure'
        with self.journal.connect() as connection:
            connection.execute('BEGIN IMMEDIATE')
            self._owned(connection, lease, _time(now), require_source=False)
            connection.execute('UPDATE company_runs SET state=?, owner=NULL, lease_token=NULL, lease_until=NULL WHERE run_id=?',
                               (state, lease.run_id))

    def summary(self, run_id: str) -> dict:
        with self.journal.connect() as connection:
            row = connection.execute('SELECT * FROM company_runs WHERE run_id=?', (run_id,)).fetchone()
            if row is None:
                raise PageRunFailure('unknown_run')
            return {'run_id': run_id, 'state': row['state'], 'completed_pages': row['completed_pages'],
                    'page_budget': row['page_budget'], 'next_start_candidate': row['next_start'],
                    'source_complete': None, 'ordering': 'relevance',
                    'preceding_items_missing': row['first_start'] > 0}
