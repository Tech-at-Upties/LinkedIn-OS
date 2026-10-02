from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any


class FieldState(StrEnum):
    missing = "missing"
    null = "null"
    value = "value"
    invalid = "invalid"


@dataclass(frozen=True)
class SourceField:
    state: FieldState
    value: Any = None


@dataclass(frozen=True)
class Issue:
    code: str
    entity_id: str | None = None
    field: str | None = None


@dataclass(frozen=True)
class Metrics:
    target_id: str | None
    entity_id: str | None
    values: dict[str, SourceField]
    reaction_types: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class Publication:
    representation_id: str
    occurrence_id: str
    content_id: SourceField
    feed_publisher_id: str
    actor_id: SourceField
    commentary: SourceField
    header_text: SourceField
    root_share: SourceField
    reshared_occurrence_id: SourceField
    metrics: Metrics
    observed_at: datetime
    evidence_class: str
    published_at: datetime | None = None


@dataclass(frozen=True)
class ParsedPage:
    publications: tuple[Publication, ...]
    issues: tuple[Issue, ...]
    paging: dict[str, int]
    source_complete: bool | None = None
    ordering: str = "relevance"
    limitations: tuple[str, ...] = (
        "Source exhaustion is unproved for this recipe",
        "Relevance order does not establish chronological history",
        "Absolute publication time is not established",
    )
