"""
What-if projector: splice counterfactual facts, causal filtration, in-memory Phase 3 replay.

Safety: never INSERT/UPDATE the physical ``events`` table (DB load-only; synthesis in RAM).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Mapping, Optional, Sequence
from uuid import UUID

import asyncpg

from src.event_store import _jsonb_to_dict
from src.models.events import StoredEvent
from src.what_if.memory_projections import InMemoryPhase3Projections


def _meta(ev: StoredEvent) -> dict[str, Any]:
    return ev.metadata if isinstance(ev.metadata, dict) else {}


def _str_id(eid: Any) -> str:
    if eid is None:
        return ""
    if isinstance(eid, UUID):
        return str(eid)
    return str(eid)


async def load_application_events_ordered(
    pool: asyncpg.Pool,
    application_id: str,
    *,
    up_to_event_type: Optional[str] = None,
) -> List[StoredEvent]:
    """Loan, compliance, and agent streams for ``application_id``, ordered by ``global_position``."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT event_id, stream_id, stream_position, global_position,
                   event_type, event_version, payload, metadata, recorded_at
            FROM events
            WHERE stream_id = $2 OR stream_id = $3
               OR (stream_id LIKE 'agent-%' AND COALESCE(payload->>'application_id', '') = $1)
            ORDER BY global_position ASC
            """,
            application_id,
            f"loan-{application_id}",
            f"compliance-{application_id}",
        )

    def _row_to_ev(r: asyncpg.Record) -> StoredEvent:
        return StoredEvent(
            event_id=r["event_id"],
            stream_id=r["stream_id"],
            stream_position=int(r["stream_position"]),
            global_position=int(r["global_position"]),
            event_type=r["event_type"],
            event_version=int(r["event_version"]),
            payload=_jsonb_to_dict(r["payload"]),
            metadata=_jsonb_to_dict(r["metadata"]),
            recorded_at=r["recorded_at"],
        )

    out = [_row_to_ev(r) for r in rows]
    if up_to_event_type:
        for i, ev in enumerate(out):
            if ev.event_type == up_to_event_type:
                return out[: i + 1]
        return out
    return out


def _anchor_event_ids(original: Sequence[StoredEvent], branch_index: int) -> set[str]:
    if branch_index < 0 or branch_index >= len(original):
        return set()
    branch_gp = original[branch_index].global_position
    return {_str_id(e.event_id) for e in original if e.global_position >= branch_gp}


def _original_by_id(original: Sequence[StoredEvent]) -> dict[str, StoredEvent]:
    return {_str_id(e.event_id): e for e in original}


def causally_depends_on_anchor(
    event: StoredEvent,
    anchor_ids: set[str],
    by_id: Mapping[str, StoredEvent],
) -> bool:
    """True if ``metadata.causation_id`` chain reaches any ``anchor_ids`` event."""
    visited: set[str] = set()
    cur = _str_id(_meta(event).get("causation_id"))
    while cur and cur not in visited:
        visited.add(cur)
        if cur in anchor_ids:
            return True
        parent = by_id.get(cur)
        if not parent:
            break
        cur = _str_id(_meta(parent).get("causation_id"))
    return False


def _remap_global_positions(events: List[StoredEvent]) -> List[StoredEvent]:
    return [ev.model_copy(update={"global_position": i}) for i, ev in enumerate(events, start=1)]


def last_loan_decision_recommendation(events: Sequence[StoredEvent]) -> Optional[str]:
    last: Optional[str] = None
    for ev in events:
        if ev.stream_id.startswith("loan-") and ev.event_type == "DecisionGenerated":
            p = ev.payload if isinstance(ev.payload, dict) else {}
            rec = p.get("recommendation")
            if rec:
                last = str(rec).upper()
    return last


def score5_infer_decision_if_orphaned(events: Sequence[StoredEvent]) -> Optional[str]:
    """
    If ``DecisionGenerated`` was pruned, infer demo outcome from the latest loan
    ``CreditAnalysisCompleted.risk_tier``: HIGH → DECLINE, else APPROVE.
    """
    direct = last_loan_decision_recommendation(events)
    if direct:
        return direct
    risk: Optional[str] = None
    for ev in events:
        if ev.stream_id.startswith("loan-") and ev.event_type == "CreditAnalysisCompleted":
            p = ev.payload if isinstance(ev.payload, dict) else {}
            risk = str(p.get("risk_tier") or "").upper() or risk
    if risk == "HIGH":
        return "DECLINE"
    if risk:
        return "APPROVE"
    return None


@dataclass
class WhatIfResult:
    baseline_events: List[StoredEvent]
    synthetic_events: List[StoredEvent]
    skipped_original_event_ids: List[str]
    projections: InMemoryPhase3Projections
    branch_index: int
    baseline_final_recommendation: Optional[str]
    counterfactual_final_recommendation: Optional[str]

    def to_summary_dict(self) -> dict[str, Any]:
        return {
            "branch_index": self.branch_index,
            "skipped_original_event_ids": self.skipped_original_event_ids,
            "baseline_recommendation": self.baseline_final_recommendation,
            "counterfactual_recommendation": self.counterfactual_final_recommendation,
            "synthetic_event_count": len(self.synthetic_events),
            "projection_snapshot": self.projections.snapshot_dict(),
        }


def run_what_if(
    original_timeline: Sequence[StoredEvent],
    *,
    branch_at_event_type: str,
    counterfactual_events: Sequence[StoredEvent],
    branch_stream_prefix: Optional[str] = None,
) -> WhatIfResult:
    """
    Splice counterfactuals at the first ``branch_at_event_type``, drop causally dependent tails,
    remap ``global_position``, replay Phase 3 in-memory. No writes to ``events``.
    """
    original = list(original_timeline)
    branch_idx = -1
    for i, ev in enumerate(original):
        if ev.event_type != branch_at_event_type:
            continue
        if branch_stream_prefix and not ev.stream_id.startswith(branch_stream_prefix):
            continue
        branch_idx = i
        break
    if branch_idx < 0:
        raise ValueError(
            f"No branch event with type {branch_at_event_type!r}"
            + (f" on stream prefix {branch_stream_prefix!r}" if branch_stream_prefix else "")
        )

    anchor_ids = _anchor_event_ids(original, branch_idx)
    by_id = _original_by_id(original)

    prefix = original[:branch_idx]
    tail_original_original = original[branch_idx + 1 :]
    skipped: list[str] = []
    tail_kept: list[StoredEvent] = []
    for e in tail_original_original:
        if causally_depends_on_anchor(e, anchor_ids, by_id):
            skipped.append(_str_id(e.event_id))
        else:
            tail_kept.append(e)

    synthetic = list(prefix) + list(counterfactual_events) + tail_kept
    synthetic_reindexed = _remap_global_positions(synthetic)

    mem = InMemoryPhase3Projections()
    mem.apply_all(synthetic_reindexed)

    base_rec = last_loan_decision_recommendation(original)
    cf_rec = score5_infer_decision_if_orphaned(synthetic_reindexed)

    return WhatIfResult(
        baseline_events=original,
        synthetic_events=synthetic_reindexed,
        skipped_original_event_ids=skipped,
        projections=mem,
        branch_index=branch_idx,
        baseline_final_recommendation=base_rec,
        counterfactual_final_recommendation=cf_rec,
    )
