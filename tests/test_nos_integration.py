"""Controlled actual M1 serializer -> M2 converter/model -> SQLite Store."""
import copy
import os
import sys
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from uuid import uuid4

SOURCE = Path(os.environ.get("NOS_INTEGRATION_ROOT", Path(__file__).resolve().parents[1] / ".local/nos-integration"))
if not (SOURCE / "M2/src/nos_m2/linkedin.py").exists():
    pytest.skip("The dedicated NOS integration checkout is not installed", allow_module_level=True)
sys.path[:0] = [str(SOURCE / "M1/src"), str(SOURCE / "M2/src")]

from nos_m2.m1 import M1Client, M1Error
from nos_m2.models import EvidenceEnvelope
from nos_m2.store import Store
from xingestion.linkedin.northbound import serialize_company_page

from nos_linkedin.parser import parse_graph
from test_parser import COMPANY, NOW, ROOT, fixture


def wire(nodes=None, *, observed=NOW, job="job-li-1"):
    page = parse_graph(fixture() if nodes is None else nodes, [ROOT], feed_publisher_id=COMPANY,
                       observed_at=observed, evidence_class="synthetic_fixture")
    return serialize_company_page(page, job_id=job)


def convert(result):
    with M1Client("http://127.0.0.1:8090", "synthetic-local-test-key", max_retries=0) as client:
        return [EvidenceEnvelope.model_validate(item) for item in client.envelopes(result)]


@pytest.fixture(params=["sqlite", "postgres"])
def store(tmp_path, request):
    admin = None
    if request.param == "postgres":
        url = os.environ.get("NOS_M2_TEST_DATABASE_URL")
        if not url:
            pytest.skip("Real Postgres URL not configured")
        schema = f"li_test_{uuid4().hex}"
        admin = create_engine(url)
        with admin.begin() as connection:
            connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        uri = make_url(url).update_query_dict({"options": f"-csearch_path={schema}"}).render_as_string(hide_password=False)
    else:
        uri = f"sqlite:///{tmp_path / 'm2.sqlite'}"
    database = Store(uri)
    database.initialize()
    yield database
    database.close()
    if admin is not None:
        with admin.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()


def test_actual_boundary_keeps_native_identity_authorship_metrics_and_origin(store):
    envelope = convert(wire())[0]
    assert envelope.evidence_id == "linkedin:urn:li:activity:123"
    assert envelope.author.account_id == "urn:li:company:999"
    assert envelope.metrics == {"numLikes": 5, "numComments": 0, "numImpressions": None}
    assert envelope.published_at is None and envelope.coverage.state == "partial"
    assert envelope.raw_payload["m1_item"]["metrics_target_id"] == "urn:li:ugcPost:456"
    receipt = store.admit(envelope)
    assert store.get_evidence(receipt.evidence_id)["origin"]["m1_job_id"] == "job-li-1"


def test_duplicate_repeat_engagement_change_and_edit_have_separate_history(store):
    initial = convert(wire())[0]
    first = store.admit(initial)
    assert store.admit(initial).duplicate_delivery
    repeat = store.admit(convert(wire(observed=NOW + timedelta(minutes=1), job="job-2"))[0])
    assert repeat.source_version_id == first.source_version_id and repeat.observation_id != first.observation_id
    nodes = fixture()
    nodes[2]["numLikes"] = 8
    metric_change = store.admit(convert(wire(nodes, observed=NOW + timedelta(minutes=2), job="job-3"))[0])
    assert metric_change.source_version_id == first.source_version_id
    nodes[0]["commentary"]["text"]["text"] = "Edited source commentary"
    edit = store.admit(convert(wire(nodes, observed=NOW + timedelta(minutes=3), job="job-4"))[0])
    assert edit.source_version_id != first.source_version_id
    assert len(store.list_observations(first.evidence_id)) == 4


def test_partial_metric_then_recovery_never_becomes_zero(store):
    nodes = fixture()[:2]
    partial = convert(wire(nodes))[0]
    assert partial.metrics == {}
    first = store.admit(partial)
    recovered = store.admit(convert(wire(observed=NOW + timedelta(minutes=1), job="recovered"))[0])
    assert recovered.source_version_id == first.source_version_id
    assert store.list_observations(first.evidence_id)[0]["metrics"] == {}


def test_reopen_and_redelivery_preserve_history(tmp_path):
    uri = f"sqlite:///{tmp_path / 'restart.sqlite'}"
    envelope = convert(wire())[0]
    first = Store(uri)
    first.initialize()
    receipt = first.admit(envelope)
    first.close()
    reopened = Store(uri)
    try:
        reopened.initialize()
        assert reopened.admit(envelope).duplicate_delivery
        assert len(reopened.list_observations(receipt.evidence_id)) == 1
    finally:
        reopened.close()


@pytest.mark.parametrize("mutate", [
    lambda item: item.update(text="Invented text"),
    lambda item: item.update(author={"account_id": COMPANY}),
    lambda item: item.update(metrics={"numLikes": 99}),
    lambda item: item.update(metrics_target_id="urn:li:ugcPost:123"),
    lambda item: item.update(raw_evidence_ref="/v1/evidence/projected-not-raw"),
    lambda item: item["coverage"].update(state="complete"),
    lambda item: item.update(observed_at="2026-10-02T00:00:00"),
    lambda item: item["source_fields"]["commentary"].update(state="unrecognized"),
    lambda item: (item.update(author={"account_id": "not-a-native-id"}), item["source_fields"]["actor_id"].update(value="not-a-native-id")),
])
def test_boundary_rejects_inconsistent_source_claims(mutate):
    result = copy.deepcopy(wire())
    mutate(result["items"][0])
    with pytest.raises(M1Error):
        convert(result)
