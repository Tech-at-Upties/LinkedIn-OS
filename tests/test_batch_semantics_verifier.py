"""Current bounded batch metadata and expiry/privacy checks, synthetic source only."""
from datetime import UTC, datetime, timedelta
import importlib.util
import json

import pytest

from test_company_batch_transport import (batch_installed, project_manifest,
    test_guarded_batch_uses_borrowed_connection_without_journal_construction as capture_batch)
from test_initial_company_page import PROJECT
from test_grant_runtime import consumptions


@pytest.fixture
def semantic_case(batch_installed, monkeypatch):
    case = batch_installed
    capture_batch(case, monkeypatch)
    parent = consumptions(case)[0][1]
    for spec in case.journal.batch_for_attempt(parent)['pages']:
        case.journal.finish('linkedin.company-feed', 0, spec.attempt_id, 'partial')
    spec = importlib.util.spec_from_file_location('synthetic_batch_verifier', PROJECT / 'docs/experiments/verify_batch_semantics.py')
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    monkeypatch.setattr(module, 'ROOT', case.root)
    return case, parent, module


def test_metadata_verifier_exports_distinct_ids_counts_and_digests_without_body_text(semantic_case):
    case, parent, module = semantic_case
    before = case.journal.path.read_bytes()
    result = module.summarize_batch(case.journal.path, parent)
    assert [(page['start'], page['count']) for page in result['pages']] == [(0, 3), (3, 10), (13, 10)]
    assert result['capture_metadata']['native_reads_admitted'] == 4
    assert sum(len(page['publications']) for page in result['pages']) == 23
    for page in result['pages']:
        assert len(page['native_sha256']) == len(page['projection_sha256']) == len(page['provenance_sha256']) == 64
        for item in page['publications']:
            assert set(item['field_states']) == {'commentary', 'header', 'root_share'}
            assert 'numLikes' in item['metrics']
            assert item['metrics']['numComments'] == {'state': 'value', 'count': 0}
    encoded = json.dumps(result)
    assert not any(key in encoded for key in ('"text"', '"body"', '"headers"', '"cookie"', '"request_parameters"'))
    assert case.journal.path.read_bytes() == before


def test_expired_batch_refuses_before_publication_decode(semantic_case, monkeypatch):
    case, parent, module = semantic_case
    expiry = datetime.fromisoformat(case.manifest['expires_at'])
    monkeypatch.setattr(module.json, 'loads', lambda value: pytest.fail('Expired body must never decode'))
    with pytest.raises(ValueError, match='retention_or_classification'):
        module.summarize_batch(case.journal.path, parent, clock=lambda: expiry + timedelta(seconds=1))


def test_deadline_crossing_during_verification_releases_no_metadata(semantic_case):
    case, parent, module = semantic_case
    ticks = iter([datetime.now(UTC)] * 7 + [datetime.fromisoformat(case.manifest['expires_at']) + timedelta(seconds=1)])
    with pytest.raises(ValueError, match='elapsed_during'):
        module.summarize_batch(case.journal.path, parent, clock=lambda: next(ticks))


def test_clock_crossing_between_pages_never_decodes_later_expired_body(semantic_case, monkeypatch):
    case, parent, module = semantic_case
    ticks = iter([datetime.now(UTC)] * 3 + [datetime.fromisoformat(case.manifest['expires_at']) + timedelta(seconds=1)])
    loads, original = [], module.json.loads
    def decode(value):
        loads.append(1)
        return original(value)
    monkeypatch.setattr(module.json, 'loads', decode)
    with pytest.raises(ValueError, match='retention_or_classification'):
        module.summarize_batch(case.journal.path, parent, clock=lambda: next(ticks))
    assert len(loads) == 1
