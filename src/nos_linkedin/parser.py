from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from .models import FieldState, Issue, Metrics, ParsedPage, Publication, SourceField

UPDATE = "com.linkedin.voyager.dash.feed.Update"
SOCIAL = "com.linkedin.voyager.dash.social.SocialDetail"
COUNTS = "com.linkedin.voyager.dash.feed.SocialActivityCounts"
RECIPE = "feedDashOrganizationalPageUpdatesByOrganizationalPageRelevanceFeed"
EVIDENCE_CLASSES = {"native_response", "allowlisted_source_projection", "synthetic_fixture"}
_URN = re.compile(r"urn:li:[A-Za-z][A-Za-z0-9_]*:[A-Za-z0-9_(),:.-]{1,1000}\Z")
_PUBLICATION = re.compile(r"urn:li:(?:activity|share|ugcPost):[0-9]+\Z")


class ParseFailure(ValueError):
    """A bounded code, never a source body, credential or full request URL."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def native_id(value: Any) -> bool:
    return isinstance(value, str) and bool(_URN.fullmatch(value))


def _field(node: Mapping[str, Any], key: str, valid) -> SourceField:
    if key not in node:
        return SourceField(FieldState.missing)
    value = node[key]
    if value is None:
        return SourceField(FieldState.null)
    if not valid(value):
        return SourceField(FieldState.invalid)
    return SourceField(FieldState.value, value)


def _text(node: Any) -> SourceField:
    if node is None:
        return SourceField(FieldState.null)
    if not isinstance(node, Mapping):
        return SourceField(FieldState.invalid)
    return _field(node, "text", lambda value: isinstance(value, str))


def _component_text(root: Mapping[str, Any], key: str) -> SourceField:
    if key not in root:
        return SourceField(FieldState.missing)
    component = root[key]
    if component is None:
        return SourceField(FieldState.null)
    if not isinstance(component, Mapping):
        return SourceField(FieldState.invalid)
    if "text" not in component:
        return SourceField(FieldState.missing)
    return _text(component["text"])


class _Graph:
    def __init__(self, entities: Sequence[Any]):
        if len(entities) > 20_000:
            raise ParseFailure("graph_size_limit")
        self.index: dict[str, Mapping[str, Any]] = {}
        self.conflicts: set[str] = set()
        self.issues: list[Issue] = []
        for node in entities:
            if not isinstance(node, Mapping):
                raise ParseFailure("invalid_included_entity")
            identifier = node.get("entityUrn")
            if not native_id(identifier):
                continue
            if identifier in self.index and self.index[identifier] != node:
                self.conflicts.add(identifier)
            else:
                self.index[identifier] = node
        self.issues.extend(Issue("conflicting_entity", identifier) for identifier in sorted(self.conflicts))

    def resolve(self, identifier: Any, expected_type: str) -> Mapping[str, Any] | None:
        if not native_id(identifier):
            self.issues.append(Issue("invalid_reference", field=expected_type))
            return None
        if identifier in self.conflicts:
            return None
        node = self.index.get(identifier)
        if node is None:
            self.issues.append(Issue("missing_reference", identifier, expected_type))
            return None
        if node.get("$type") != expected_type:
            self.issues.append(Issue("reference_type_mismatch", identifier, expected_type))
            return None
        return node


def _counts(graph: _Graph, update: Mapping[str, Any], occurrence_id: str, content: SourceField) -> Metrics:
    metric_fields = ("numLikes", "numComments", "numShares", "numImpressions")
    unknown = Metrics(None, None, {key: SourceField(FieldState.missing) for key in metric_fields}, ())
    if "*socialDetail" not in update:
        graph.issues.append(Issue("missing_social_detail", update.get("entityUrn")))
        return unknown
    social = graph.resolve(update["*socialDetail"], SOCIAL)
    if social is None:
        return unknown
    counts = graph.resolve(social.get("*totalSocialActivityCounts"), COUNTS)
    if counts is None:
        return unknown
    target = counts.get("urn")
    targets = {occurrence_id}
    if content.state == FieldState.value:
        targets.add(content.value)
    if target not in targets:
        graph.issues.append(Issue("metric_target_mismatch", counts.get("entityUrn")))
        return unknown
    values = {key: _field(counts, key, lambda value: type(value) is int and value >= 0) for key in metric_fields}
    for key, value in values.items():
        if value.state == FieldState.invalid:
            graph.issues.append(Issue("invalid_metric", counts["entityUrn"], key))
    reactions: list[tuple[str, int]] = []
    seen: set[str] = set()
    reaction_list = counts.get("reactionTypeCounts", [])
    if not isinstance(reaction_list, list):
        graph.issues.append(Issue("invalid_reaction_types", counts["entityUrn"]))
    else:
        for item in reaction_list:
            if not isinstance(item, Mapping):
                graph.issues.append(Issue("invalid_reaction_type", counts["entityUrn"]))
                continue
            kind, count = item.get("reactionType"), item.get("count")
            if not isinstance(kind, str) or not re.fullmatch(r"[A-Z_]{1,50}", kind) or type(count) is not int or count < 0 or kind in seen:
                graph.issues.append(Issue("invalid_reaction_type", counts["entityUrn"]))
                continue
            seen.add(kind)
            reactions.append((kind, count))
    return Metrics(target, counts["entityUrn"], values, tuple(reactions))


def parse_graph(
    entities: Sequence[Any], root_ids: Sequence[str], *, feed_publisher_id: str,
    observed_at: datetime, evidence_class: str,
) -> ParsedPage:
    if not native_id(feed_publisher_id):
        raise ParseFailure("invalid_feed_publisher")
    if not isinstance(observed_at, datetime) or observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ParseFailure("observation_time_requires_timezone")
    if evidence_class not in EVIDENCE_CLASSES:
        raise ParseFailure("invalid_evidence_class")
    if not isinstance(entities, (list, tuple)) or not isinstance(root_ids, (list, tuple)) or len(root_ids) > 1000:
        raise ParseFailure("invalid_graph_input")
    graph = _Graph(entities)
    publications: list[Publication] = []
    seen: set[str] = set()
    for identifier in root_ids:
        if not native_id(identifier):
            graph.issues.append(Issue("invalid_root_reference"))
            continue
        if identifier in seen:
            graph.issues.append(Issue("duplicate_collection_reference", identifier))
            continue
        seen.add(identifier)
        root = graph.resolve(identifier, UPDATE)
        if root is None:
            continue
        metadata = root.get("metadata")
        if not isinstance(metadata, Mapping) or not isinstance(metadata.get("backendUrn"), str) or not _PUBLICATION.fullmatch(metadata["backendUrn"]):
            graph.issues.append(Issue("missing_occurrence_identity", identifier))
            continue
        occurrence_id = metadata["backendUrn"]
        content = _field(metadata, "shareUrn", lambda value: isinstance(value, str) and bool(_PUBLICATION.fullmatch(value)))
        actor = root.get("actor")
        actor_id = _field(actor, "backendUrn", native_id) if isinstance(actor, Mapping) else SourceField(FieldState.missing)
        if actor_id.state != FieldState.value:
            graph.issues.append(Issue("unknown_actor", identifier))
        reshared = SourceField(FieldState.missing)
        if "resharedUpdate" in root:
            value = root["resharedUpdate"]
            if value is None:
                reshared = SourceField(FieldState.null)
            else:
                # Only an explicit typed referenced/inline Update can establish this relation.
                original = graph.resolve(value, UPDATE) if isinstance(value, str) else value
                original_metadata = original.get("metadata") if isinstance(original, Mapping) and original.get("$type") == UPDATE else None
                original_id = original_metadata.get("backendUrn") if isinstance(original_metadata, Mapping) else None
                if isinstance(original_id, str) and _PUBLICATION.fullmatch(original_id) and original_id != occurrence_id:
                    reshared = SourceField(FieldState.value, original_id)
                else:
                    reshared = SourceField(FieldState.invalid)
                    graph.issues.append(Issue("unresolved_reshare", identifier))
        publications.append(Publication(
            representation_id=identifier, occurrence_id=occurrence_id, content_id=content,
            feed_publisher_id=feed_publisher_id, actor_id=actor_id,
            commentary=_component_text(root, "commentary"), header_text=_component_text(root, "header"),
            root_share=_field(metadata, "rootShare", lambda value: type(value) is bool),
            reshared_occurrence_id=reshared, metrics=_counts(graph, root, occurrence_id, content),
            observed_at=observed_at, evidence_class=evidence_class,
        ))
    return ParsedPage(tuple(publications), tuple(graph.issues), {})


def parse_company_feed(body: Any, *, feed_publisher_id: str, observed_at: datetime, evidence_class: str = "native_response") -> ParsedPage:
    if not isinstance(body, Mapping):
        raise ParseFailure("invalid_envelope")
    if body.get("errors") or body.get("error"):
        raise ParseFailure("semantic_error")
    outer = body.get("data")
    data = outer.get("data") if isinstance(outer, Mapping) else None
    collection = data.get(RECIPE) if isinstance(data, Mapping) else None
    if not isinstance(collection, Mapping) or collection.get("$type") != "com.linkedin.restli.common.CollectionResponse":
        raise ParseFailure("recipe_schema_drift")
    if collection.get("errors") or collection.get("error"):
        raise ParseFailure("semantic_error")
    roots = collection.get("*elements")
    included = body.get("included")
    paging = collection.get("paging")
    if not isinstance(roots, list) or not isinstance(included, list) or not isinstance(paging, Mapping):
        raise ParseFailure("incomplete_collection")
    if any(type(paging.get(key)) is not int or paging[key] < 0 for key in ("start", "count", "total")):
        raise ParseFailure("invalid_paging")
    page = parse_graph(included, roots, feed_publisher_id=feed_publisher_id, observed_at=observed_at, evidence_class=evidence_class)
    issues = list(page.issues)
    if paging["start"] > 0:
        issues.append(Issue("preceding_items_not_in_this_page"))
    return ParsedPage(page.publications, tuple(issues), {key: paging[key] for key in ("start", "count", "total")})
