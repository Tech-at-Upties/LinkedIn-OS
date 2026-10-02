from __future__ import annotations

from dataclasses import asdict

from .models import FieldState, ParsedPage, Publication


def publication_item(publication: Publication, *, raw_evidence_ref: str | None = None) -> dict:
    """An explicit LinkedIn wire item; no X/RSS field or identity translation."""
    states = asdict(publication)
    states["observed_at"] = publication.observed_at.isoformat()
    states["published_at"] = None
    metrics = {
        key: field.value for key, field in publication.metrics.values.items()
        if field.state in {FieldState.value, FieldState.null}
    }
    references = []
    if publication.reshared_occurrence_id.state == FieldState.value:
        references.append({"kind": "repost", "source_item_id": publication.reshared_occurrence_id.value})
    limitations = [
        "Source exhaustion and chronological completeness are unproved",
        "Company-feed membership does not establish company authorship",
        "Metrics retain native names and their separate native target",
        "Absolute publication time is unknown",
    ]
    if publication.evidence_class != "native_response":
        limitations.append(f"Evidence class is {publication.evidence_class}, not retained native raw response")
    return {
        "schema_version": "linkedin-publication/1", "item_id": publication.occurrence_id,
        "observed_at": publication.observed_at.isoformat(), "published_at": None,
        "text": publication.commentary.value if publication.commentary.state == FieldState.value else "",
        "author": {"account_id": publication.actor_id.value} if publication.actor_id.state == FieldState.value else None,
        "references": references, "metrics": metrics,
        "metrics_target_id": publication.metrics.target_id,
        "raw_evidence_ref": raw_evidence_ref, "source_fields": states,
        "coverage": {"state": "partial", "as_of": publication.observed_at.isoformat(), "limitations": limitations},
    }


def serialize_page(page: ParsedPage, *, job_id: str, raw_evidence_ref: str | None = None) -> dict:
    if not isinstance(job_id, str) or not job_id.strip():
        raise ValueError("job_id is required")
    items = [publication_item(item, raw_evidence_ref=raw_evidence_ref) for item in page.publications]
    for item in items:
        item["coverage"]["gaps"] = [asdict(issue) for issue in page.issues]
        item["coverage"]["limitations"].extend(page.limitations)
        item["source_fields"]["page_paging"] = page.paging
    return {
        "source_type": "linkedin", "job_id": job_id,
        "items": items,
        "coverage": {
            "state": "partial", "source_complete": page.source_complete,
            "ordering": page.ordering, "paging": page.paging,
            "limitations": list(page.limitations), "issues": [asdict(issue) for issue in page.issues],
        },
    }
