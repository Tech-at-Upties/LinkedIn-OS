"""One physical capture saves every page; delivery only replays saved members."""
import json
import sqlite3
from dataclasses import replace
from datetime import timedelta

import pytest

from nos_linkedin.acquisition import (AcquisitionFailure, BatchPageResponse, Journal,
    SourceBatchResponse, SourceResponse, acquire_company_page, classify_company_batch)
from nos_linkedin.pages import CompanyPageRuns, PageRunFailure
from nos_linkedin.parser import ParseFailure
from test_company_collection import body_for, execute, setup
from test_parser import COMPANY, NOW


SCOPE = 'linkedin.company-feed'


def reserved(tmp_path, *, budget=3, target=15):
    journal, runs, permission = setup(tmp_path, budget=budget, target=target)
    lease = runs.claim('collection', owner='first', now=NOW)
    specs = runs.reserve_batch(lease, batch_page_budget=budget, now=NOW)
    return journal, runs, permission, lease, specs


def batch_for(specs, *, available=None, security=False):
    available = len(specs) if available is None else available
    pages = []
    for spec in specs[:available]:
        ids = range(1 + spec.start, 1 + spec.start + spec.count)
        body = body_for(spec, ids)
        if security and spec.ordinal == 1:
            body = {'errors': [{'code': 'ACCESS_DENIED'}]}
        pages.append(BatchPageResponse(spec, SourceResponse(200, 'application/json', json.dumps(body).encode(), NOW + timedelta(seconds=spec.ordinal))))
    return SourceBatchResponse(tuple(pages), 'continuation_not_observed' if available < len(specs) else None)


def capture(journal, runs, permission, lease, batch, calls):
    def guarded(connection):
        assert connection.in_transaction
        calls.append(lease.attempt_id)
        return batch
    return journal.capture(scope=SCOPE, generation=lease.generation, attempt_id=lease.attempt_id,
        feed_publisher_id=COMPANY, permission=permission, now=NOW, send_guarded=guarded,
        operation_guard=lambda: runs.require_owned(lease, NOW))


def classify(journal, permission, lease):
    return classify_company_batch(journal=journal, parent_attempt_id=lease.attempt_id,
        permission=permission, now=NOW)


def test_batch_all_pages_saved_before_delivery_then_replay_no_more_capture(tmp_path):
    journal, runs, permission, lease, specs = reserved(tmp_path)
    calls, deliveries = [], []
    capture(journal, runs, permission, lease, batch_for(specs), calls)
    with journal.connect() as connection:
        rows = connection.execute('SELECT * FROM responses ORDER BY captured_at').fetchall()
        assert len(rows) == 3 and all(row['body'] is not None and row['outcome'] == 'captured' for row in rows)
        assert [row['captured_at'] for row in rows] == [(NOW + timedelta(seconds=i)).timestamp() for i in range(3)]
        assert {row['expires_at'] for row in rows} == {permission.raw_expires_at.timestamp()}
    with pytest.raises(AcquisitionFailure, match='classification_pending'):
        journal.deliver(scope=SCOPE, generation=lease.generation, attempt_id=lease.attempt_id, now=NOW,
                        send=lambda: deliveries.append('forbidden'))
    assert len(classify(journal, permission, lease)) == 3
    for index in range(3):
        selected = lease if index == 0 else runs.claim('collection', owner='next', now=NOW)
        execute(runs, selected, permission, [], calls, deliveries)
    assert calls == [lease.attempt_id]
    assert [len(wire['items']) for wire in deliveries] == [3, 10, 2]
    assert runs.summary('collection')['fulfilled']
    assert [spec.start for spec in Journal(journal.path).batch_for_attempt(lease.attempt_id)['pages']] == [0, 3, 13]


def test_missing_continuation_has_no_dispatchable_phantom_page(tmp_path):
    journal, runs, permission, lease, specs = reserved(tmp_path, target=20)
    calls = []
    capture(journal, runs, permission, lease, batch_for(specs, available=2), calls)
    classify(journal, permission, lease)
    for index in range(2):
        selected = lease if index == 0 else runs.claim('collection', owner='next', now=NOW)
        execute(runs, selected, permission, [], calls, [])
    summary = runs.summary('collection')
    assert summary['returned_count'] == 13 and not summary['fulfilled']
    assert summary['shortfall_reason'] == 'continuation_not_observed' and summary['source_complete'] is None
    assert runs.claim('collection', owner='third', now=NOW) is None
    with pytest.raises(AcquisitionFailure, match='batch_member_unavailable'):
        journal.capture(scope=SCOPE, generation=lease.generation, attempt_id=specs[2].attempt_id,
            feed_publisher_id=COMPANY, permission=permission, now=NOW,
            send=lambda: calls.append('forbidden'))
    assert calls == [lease.attempt_id] and journal.read(specs[2].attempt_id, NOW) is None


def test_batch_reservation_reopen_and_reclaim_preserve_all_attempt_ids(tmp_path):
    journal, runs, _, lease, specs = reserved(tmp_path)
    fresh = CompanyPageRuns(Journal(journal.path))
    resumed = fresh.claim('collection', owner='restart', now=NOW + timedelta(seconds=61))
    assert resumed.attempt_id == lease.attempt_id and resumed.token != lease.token
    assert fresh.reserve_batch(resumed, batch_page_budget=3, now=NOW + timedelta(seconds=61)) == specs
    with pytest.raises(PageRunFailure, match='page_lease_fenced'):
        runs.reserve_batch(lease, batch_page_budget=3, now=NOW + timedelta(seconds=61))


def test_invalid_later_member_cannot_commit_any_body_or_relaunch_parent(tmp_path):
    journal, runs, permission, lease, specs = reserved(tmp_path)
    good = batch_for(specs)
    bad = replace(good, pages=(*good.pages[:2], replace(good.pages[2], spec=replace(specs[2], start=14))))
    calls = []
    capture(journal, runs, permission, lease, bad, calls)
    assert journal.read(lease.attempt_id, NOW)['outcome'] == 'invalid_response'
    with journal.connect() as connection:
        assert connection.execute('SELECT count(*) FROM responses WHERE body IS NOT NULL').fetchone()[0] == 0
        assert connection.execute('SELECT count(*) FROM responses').fetchone()[0] == 1
    assert capture(journal, runs, permission, lease, good, calls)
    assert calls == [lease.attempt_id]


def test_write_failure_rolls_back_whole_batch_keeps_parent_unknown(tmp_path):
    journal, runs, permission, lease, specs = reserved(tmp_path)
    with journal.connect() as connection:
        connection.execute('''CREATE TRIGGER reject_third BEFORE INSERT ON responses
            WHEN NEW.attempt_id=? BEGIN SELECT RAISE(ABORT, 'controlled_batch_failure'); END'''.replace('?', "'" + specs[2].attempt_id + "'"))
    calls = []
    with pytest.raises(sqlite3.IntegrityError, match='controlled_batch_failure'):
        capture(journal, runs, permission, lease, batch_for(specs), calls)
    assert journal.read(lease.attempt_id, NOW)['outcome'] == 'dispatch_unknown'
    with journal.connect() as connection:
        assert connection.execute('SELECT count(*) FROM responses').fetchone()[0] == 1
        assert connection.execute('SELECT count(*) FROM responses WHERE body IS NOT NULL').fetchone()[0] == 0
    assert capture(journal, runs, permission, lease, batch_for(specs), calls)
    assert calls == [lease.attempt_id]


def test_security_on_later_page_stops_entire_batch_before_any_delivery(tmp_path):
    journal, runs, permission, lease, specs = reserved(tmp_path)
    capture(journal, runs, permission, lease, batch_for(specs, security=True), [])
    with pytest.raises(AcquisitionFailure, match='batch_member_not_eligible'):
        classify(journal, permission, lease)
    with journal.connect() as connection:
        assert connection.execute('SELECT stopped FROM route_state WHERE scope=?', (SCOPE,)).fetchone()[0] == 1
    delivered = []
    assert journal.deliver(scope=SCOPE, generation=lease.generation, attempt_id=lease.attempt_id,
        now=NOW, send=lambda: delivered.append('forbidden')) == 'quarantined_after_stop'
    assert delivered == []


def test_invalid_parent_still_classifies_later_security(tmp_path):
    journal, runs, permission, lease, specs = reserved(tmp_path)
    value = batch_for(specs, security=True)
    malformed_parent = replace(value.pages[0], response=replace(value.pages[0].response, body=b'{"unrecognized": true}'))
    capture(journal, runs, permission, lease, replace(value, pages=(malformed_parent, *value.pages[1:])), [])
    with pytest.raises(AcquisitionFailure, match='batch_member_not_eligible'):
        classify(journal, permission, lease)
    with journal.connect() as connection:
        assert connection.execute('SELECT stopped FROM route_state WHERE scope=?', (SCOPE,)).fetchone()[0] == 1
        assert connection.execute('SELECT outcome FROM responses WHERE attempt_id=?', (specs[1].attempt_id,)).fetchone()[0] == 'restricted'


def test_parent_selection_failure_is_preserved_while_saved_children_classify(tmp_path):
    journal, runs, permission, lease, specs = reserved(tmp_path)
    capture(journal, runs, permission, lease, batch_for(specs), [])

    def reject_selection(_page):
        raise ParseFailure('ambiguous_occurrence_identity')

    result = acquire_company_page(journal=journal, scope=SCOPE, generation=lease.generation,
        attempt_id=lease.attempt_id, feed_publisher_id=COMPANY, permission=permission, now=NOW,
        send=lambda: pytest.fail('saved_parent_must_not_dispatch'), page_validator=reject_selection)
    assert result.outcome == 'parse_failure'
    with pytest.raises(AcquisitionFailure, match='batch_member_not_eligible'):
        classify(journal, permission, lease)
    assert journal.read(lease.attempt_id, NOW)['outcome'] == 'parse_failure'
    assert all(journal.read(spec.attempt_id, NOW)['outcome'] in {'bounded', 'partial'} for spec in specs[1:])


def test_purge_all_distinct_bodies_keeps_batch_metadata_and_no_resend(tmp_path):
    journal, runs, permission, lease, specs = reserved(tmp_path)
    capture(journal, runs, permission, lease, batch_for(specs), [])
    classify(journal, permission, lease)
    assert journal.purge_expired(permission.raw_expires_at) == 3
    with pytest.raises(AcquisitionFailure, match='batch_member_not_eligible'):
        classify_company_batch(journal=journal, parent_attempt_id=lease.attempt_id,
            permission=permission, now=permission.raw_expires_at)
    assert journal.batch_for_attempt(lease.attempt_id)['available_page_count'] == 3


def test_two_page_declaration_reserves_no_continuation(tmp_path):
    journal, runs, permission, lease, specs = reserved(tmp_path, budget=2)
    assert [(s.start, s.count, s.page_mode) for s in specs] == [(0, 3, 'initial_document'), (3, 10, 'api_following')]
    capture(journal, runs, permission, lease, batch_for(specs), [])
    assert len(classify(journal, permission, lease)) == 2
    assert journal.batch_for_attempt(lease.attempt_id)['shortfall_reason'] is None
