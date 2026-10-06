"""Distinct batch projection tests; no services, browser launch or real grants."""
from copy import deepcopy
from datetime import UTC, datetime, timedelta
import json
from types import SimpleNamespace

import pytest

from test_company_batch_runtime import api_record, batch_installed, install_batch_child
from test_company_collection import body_for
from test_initial_company_page import (COMPANY, TARGET, actual_initial_projection, project_manifest, synthetic_html)
from nos_linkedin.acquisition import BatchPageSpec, Journal, RoutePermission
from nos_linkedin.pages import CompanyPageRuns
from xingestion.linkedin.batch import GrantedCompanyBatchCapture


def projected_receipt(budget=3, available=None):
    observed = datetime.now(UTC)
    available = budget if available is None else available
    specs = tuple(BatchPageSpec('synthetic-page-' + str(index), index, start, count, mode)
        for index, (start, count, mode) in enumerate(((0, 3, 'initial_document'), (3, 10, 'api_following'),
                                                    (13, 10, 'api_following'))[:budget]))
    records = [actual_initial_projection(synthetic_html(body_for(SimpleNamespace(start=0, count=3), range(1, 4))), observed)]
    for start in (3, 13)[:available - 1]:
        records.append(api_record(body_for(SimpleNamespace(start=start, count=10), range(start + 1, start + 11)), observed))
    return specs, dict(target=TARGET, evidence_class='allowlisted_source_projection', session_verified=True,
        stopped=None, failure_kind=None, account_writes_blocked=True, original_bodies_retained=False,
        native_reads_admitted=available, native_read_budget=12, boundary_probe=None,
        batch_kind='company-source-batch/1', batch_page_budget=budget,
        batch_shortfall_reason=None if available == budget else 'continuation_not_observed',
        batch_pages=[dict(page_mode=spec.page_mode, start=spec.start, count=spec.count, record=record)
                     for spec, record in zip(specs, records)],
        responses=[dict(operation_id='voyagerOrganizationDashCompanies.synthetic', origin='https://www.linkedin.com',
            status=200, representation={'source_native_ids': [COMPANY]})])


@pytest.mark.parametrize('budget,available', [(2, 2), (3, 3), (3, 2)])
def test_projection_keeps_distinct_original_scope_time_digest_and_shortfall(batch_installed, budget, available):
    capture = GrantedCompanyBatchCapture(batch_installed.reference)
    specs, receipt = projected_receipt(budget, available)
    result = capture._project_batch(receipt, specs=specs, started=datetime.now(UTC) - timedelta(seconds=5))
    assert capture.max_pages == capture.batch_page_budget == 3 and capture.supports_multipage
    assert len(result.pages) == available
    assert result.shortfall_reason == (None if available == budget else 'continuation_not_observed')
    for entry, record in zip(result.pages, receipt['batch_pages']):
        body = json.loads(entry.response.body)
        paging = body['data']['data']['feedDashOrganizationalPageUpdatesByOrganizationalPageRelevanceFeed']['paging']
        assert (paging['start'], paging['count']) == (entry.spec.start, entry.spec.count)
        assert entry.response.captured_at.isoformat() == record['record']['observed_at']
        if entry.spec.ordinal:
            assert body['source_provenance']['native_response']['body_sha256'] == record['record']['body_sha256']
        else:
            assert body['source_provenance']['initial_page']['body_sha256'] == record['record']['body_sha256']


@pytest.mark.parametrize('fault', ['security', 'over_budget', 'wrong_identity', 'wrong_page', 'wrong_paging',
                                  'extra_page', 'missing_reason', 'expired_capture', 'company_absent'])
def test_invalid_batch_cannot_release_source_responses(batch_installed, fault):
    capture = GrantedCompanyBatchCapture(batch_installed.reference)
    specs, receipt = projected_receipt()
    if fault == 'security': receipt['stopped'] = {'reason': 'challenge_path'}
    elif fault == 'over_budget': receipt['native_reads_admitted'] = 13
    elif fault == 'wrong_identity': receipt['batch_pages'][1]['record']['request_parameters']['fields']['organizationalPageUrn']['value_sha256'] = 'a' * 64
    elif fault == 'wrong_page': receipt['batch_pages'][1]['start'] = 13
    elif fault == 'wrong_paging': receipt['batch_pages'][1]['record']['representation']['collection_projection']['fields']['paging']['start'] = 0
    elif fault == 'extra_page': receipt['batch_pages'].append(deepcopy(receipt['batch_pages'][-1]))
    elif fault == 'missing_reason': receipt['batch_pages'].pop()
    elif fault == 'expired_capture': receipt['batch_pages'][1]['record']['observed_at'] = capture.expires_at.isoformat()
    else: receipt['responses'] = []
    with pytest.raises(ValueError):
        capture._project_batch(receipt, specs=specs, started=datetime.now(UTC) - timedelta(seconds=5))


def test_guarded_batch_uses_borrowed_connection_without_journal_construction(batch_installed, monkeypatch):
    case, calls = batch_installed, []
    capture = GrantedCompanyBatchCapture(case.reference)
    now = datetime.now(UTC)
    runs = CompanyPageRuns(case.journal)
    runs.create(run_id='synthetic-batch-run', feed_publisher_id=COMPANY,
        evidence_class='allowlisted_source_projection', first_start=0, count=3, following_count=10,
        requested_count=15, page_budget=3)
    lease = runs.claim('synthetic-batch-run', owner='synthetic-owner', now=now)
    runs.reserve_batch(lease, batch_page_budget=3, now=now)
    from xingestion.capabilities import LinkedInCompanyFeedInput
    from test_grant_runtime import consumptions
    payload = LinkedInCompanyFeedInput(TARGET, COMPANY, page_mode='initial_document', requested_count=15, max_pages=3)
    install_batch_child(monkeypatch, case, calls)
    def no_constructor(*args, **kwargs):
        raise AssertionError('A guarded callback must borrow the original source connection')
    monkeypatch.setattr(Journal, '__init__', no_constructor)
    case.journal.capture(scope='linkedin.company-feed', generation=0, attempt_id=lease.attempt_id,
        feed_publisher_id=COMPANY, permission=RoutePermission(True, True, capture.expires_at, 'allowlisted_source_projection'),
        now=now, send_guarded=lambda connection: capture.send_guarded(payload, lease, connection,
            task_id=lease.run_id, run_id=lease.run_id))
    assert len(calls) == len(consumptions(case)) == 1
    batch = case.journal.batch_for_attempt(lease.attempt_id)
    assert batch['state'] == 'captured' and batch['available_page_count'] == 3
