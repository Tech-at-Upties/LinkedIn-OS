import copy
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from nos_linkedin.models import FieldState
from nos_linkedin.parser import COUNTS, RECIPE, SOCIAL, UPDATE, ParseFailure, parse_company_feed, parse_graph

NOW = datetime(2026, 10, 2, tzinfo=UTC)
COMPANY = "urn:li:fsd_company:1337"
ROOT = "urn:li:fsd_update:(urn:li:activity:123,COMPANY_FEED_RELEVANCE)"
SOCIAL_ID = "urn:li:fsd_socialDetail:(urn:li:ugcPost:456,urn:li:ugcPost:456)"
COUNT_ID = "urn:li:fsd_socialActivityCounts:urn:li:ugcPost:456"


def fixture():
    return [
        {"entityUrn": ROOT, "$type": UPDATE, "metadata": {"backendUrn": "urn:li:activity:123", "shareUrn": "urn:li:ugcPost:456", "rootShare": True},
         "actor": {"backendUrn": "urn:li:company:999"}, "commentary": {"text": {"text": "Own commentary"}},
         "header": {"text": {"text": "LinkedIn collaborated on this"}}, "*socialDetail": SOCIAL_ID, "resharedUpdate": None},
        {"entityUrn": SOCIAL_ID, "$type": SOCIAL, "*totalSocialActivityCounts": COUNT_ID},
        {"entityUrn": COUNT_ID, "$type": COUNTS, "urn": "urn:li:ugcPost:456", "numLikes": 5, "numComments": 0, "numImpressions": None,
         "reactionTypeCounts": [{"reactionType": "LIKE", "count": 3}, {"reactionType": "PRAISE", "count": 2}]},
    ]


def parse(entities=None, roots=None):
    return parse_graph(fixture() if entities is None else entities, [ROOT] if roots is None else roots,
                       feed_publisher_id=COMPANY, observed_at=NOW, evidence_class="synthetic_fixture")


def test_occurrence_author_content_metric_target_stay_distinct():
    item = parse().publications[0]
    assert item.occurrence_id == "urn:li:activity:123"
    assert item.content_id.value == item.metrics.target_id == "urn:li:ugcPost:456"
    assert item.actor_id.value == "urn:li:company:999"
    assert item.feed_publisher_id == COMPANY
    assert item.commentary.value == "Own commentary"
    assert item.header_text.value == "LinkedIn collaborated on this"
    assert item.reshared_occurrence_id.state == FieldState.null
    assert item.published_at is None
    assert item.metrics.values["numLikes"].value == 5
    assert dict(item.metrics.reaction_types)["LIKE"] == 3


def test_missing_null_zero_remain_distinct():
    values = parse().publications[0].metrics.values
    assert values["numComments"].state == FieldState.value and values["numComments"].value == 0
    assert values["numImpressions"].state == FieldState.null
    assert values["numShares"].state == FieldState.missing


@pytest.mark.parametrize("bad", [True, -1, 1.5, "0"])
def test_invalid_counts_are_unknown_with_issue(bad):
    nodes = fixture()
    nodes[2]["numLikes"] = bad
    result = parse(nodes)
    assert result.publications[0].metrics.values["numLikes"].state == FieldState.invalid
    assert "invalid_metric" in {issue.code for issue in result.issues}


def test_missing_metric_reference_does_not_guess_from_urn_suffix():
    result = parse(fixture()[:2])
    assert result.publications[0].metrics.target_id is None
    assert "missing_reference" in {issue.code for issue in result.issues}


def test_conflicting_root_quarantined_instead_of_last_map_value_winning():
    nodes = fixture()
    conflict = copy.deepcopy(nodes[0])
    conflict["actor"]["backendUrn"] = "urn:li:company:1337"
    for variants in (nodes + [conflict], [conflict] + nodes):
        assert parse(variants).publications == ()


def test_conflicting_counts_rejects_only_affected_metrics():
    nodes = fixture()
    conflict = copy.deepcopy(nodes[2])
    conflict["numLikes"] = 77
    result = parse(nodes + [conflict])
    assert len(result.publications) == 1
    assert result.publications[0].metrics.target_id is None


def test_identical_duplicate_entity_is_consistent():
    nodes = fixture()
    assert len(parse(nodes + [copy.deepcopy(nodes[0])]).publications) == 1


def test_metric_target_mismatch_never_attaches_other_counts():
    nodes = fixture()
    nodes[2]["urn"] = "urn:li:ugcPost:123"
    assert parse(nodes).publications[0].metrics.target_id is None


def test_collection_references_define_membership_and_order():
    nodes = fixture()
    other = copy.deepcopy(nodes[0])
    other["entityUrn"] = "urn:li:fsd_update:(urn:li:activity:321,OTHER)"
    other["metadata"]["backendUrn"] = "urn:li:activity:321"
    result = parse(nodes + [other], [other["entityUrn"], ROOT, ROOT])
    assert [item.occurrence_id for item in result.publications] == ["urn:li:activity:321", "urn:li:activity:123"]
    assert "duplicate_collection_reference" in {issue.code for issue in result.issues}
    assert len(parse(nodes + [other]).publications) == 1


def test_explicit_reshare_preserves_own_commentary_without_merging_original_text():
    nodes = fixture()
    original = copy.deepcopy(nodes[0])
    original["entityUrn"] = "urn:li:fsd_update:(urn:li:activity:987,ORIGINAL)"
    original["metadata"]["backendUrn"] = "urn:li:activity:987"
    original["commentary"]["text"]["text"] = "Original text"
    nodes[0]["resharedUpdate"] = original["entityUrn"]
    item = parse(nodes + [original]).publications[0]
    assert item.reshared_occurrence_id.value == "urn:li:activity:987"
    assert item.commentary.value == "Own commentary"


def test_naive_observation_time_is_rejected():
    with pytest.raises(ParseFailure, match="observation_time_requires_timezone"):
        parse_graph(fixture(), [ROOT], feed_publisher_id=COMPANY, observed_at=datetime(2026, 10, 2), evidence_class="synthetic_fixture")


def envelope(roots=None):
    return {"data": {"data": {RECIPE: {"$type": "com.linkedin.restli.common.CollectionResponse", "*elements": [ROOT] if roots is None else roots,
                                      "paging": {"start": 3, "count": 10, "total": 231}}}}, "included": fixture()}


def test_page_start_and_total_do_not_prove_source_complete():
    result = parse_company_feed(envelope([]), feed_publisher_id=COMPANY, observed_at=NOW, evidence_class="synthetic_fixture")
    assert result.publications == () and result.source_complete is None
    assert result.paging == {"start": 3, "count": 10, "total": 231}
    assert "preceding_items_not_in_this_page" in {issue.code for issue in result.issues}


@pytest.mark.parametrize("body", [{}, {"data": None, "included": []}, {"data": {"data": None}, "included": []}, {"errors": [{"status": 403}]}])
def test_error_and_missing_collection_are_not_empty_success(body):
    with pytest.raises(ParseFailure):
        parse_company_feed(body, feed_publisher_id=COMPANY, observed_at=NOW)


def test_type_confusion_in_social_reference_is_partial():
    nodes = fixture()
    nodes[1]["$type"] = UPDATE
    result = parse(nodes)
    assert result.publications[0].metrics.target_id is None
    assert "reference_type_mismatch" in {issue.code for issue in result.issues}


def test_current_projected_receipt_explicitly_is_not_raw_body_conformance():
    receipt = Path(__file__).resolve().parents[1] / "docs/results/authorized-company-semantic-probe.json"
    if not receipt.exists():
        pytest.skip("Private local projected receipt is not shipped")
    record = json.loads(receipt.read_text(encoding="utf-8"))
    assert record["evidence_class"] == "allowlisted_source_projection"
    response = next(row for row in record["responses"] if row["status"] == 200 and (row["operation_id"] or "").startswith("voyagerFeed"))
    graph = response["representation"]["projected_graph"]
    roots = [node["entityUrn"] for node in graph if node.get("$type") == UPDATE]
    result = parse_graph(graph, roots, feed_publisher_id=COMPANY, observed_at=NOW, evidence_class=record["evidence_class"])
    assert len(result.publications) == 10
    first = result.publications[0]
    assert first.occurrence_id == "urn:li:activity:7509250359064707072"
    assert first.metrics.target_id == "urn:li:ugcPost:7509250358259404800"
    assert first.actor_id.value == "urn:li:company:1337"
    assert first.metrics.values["numImpressions"].state == FieldState.null
