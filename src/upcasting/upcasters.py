"""
Concrete upcasters (deterministic; no writes; DecisionGenerated uses read-only store lookups).

See DESIGN.md for inference strategy and why confidence_score uses null for v1.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict

from src.models.events import StoredEvent

if TYPE_CHECKING:
    from src.event_store import EventStore


def _canonical_payload_hash_bytes(payload: Dict[str, Any]) -> bytes:
    """Stable JSON for hashing / audit (not used in upcast output)."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def infer_model_version_from_recorded_at(recorded_at: datetime) -> str:
    """
    Deterministic model-era label from wall time (no external API).
    Policy: quarterly buckets per calendar year — see DESIGN.md.
    """
    if recorded_at.tzinfo is None:
        recorded_at = recorded_at.replace(tzinfo=timezone.utc)
    y = recorded_at.year
    q = (recorded_at.month - 1) // 3 + 1
    return f"credit-analytics-{y}-Q{q}"


def infer_regulatory_basis_from_recorded_at(recorded_at: datetime) -> str:
    """
    Deterministic regulatory snapshot id from time (maps to active rule-set era in DESIGN.md).
    """
    if recorded_at.tzinfo is None:
        recorded_at = recorded_at.replace(tzinfo=timezone.utc)
    return f"EU-CRR-CREDIT-SNAPSHOT-{recorded_at.year}"


def upcast_credit_analysis_completed_v1_to_v2(ev: StoredEvent) -> StoredEvent:
    """
    v1 -> v2: add model_version, confidence_score=null, regulatory_basis.

    confidence_score is explicitly null (JSON) for v1-era facts: fabricating a numeric score
    would be a regulatory risk if downstream systems treat it as measured model output.
    """
    p = dict(ev.payload)
    recorded_at = ev.recorded_at
    if recorded_at.tzinfo is None:
        recorded_at = recorded_at.replace(tzinfo=timezone.utc)

    p.setdefault("application_id", p.get("application_id", ""))
    p.setdefault("agent_id", p.get("agent_id", ""))
    p["model_version"] = infer_model_version_from_recorded_at(recorded_at)
    p["confidence_score"] = None  # unknown at source — never fabricate
    p["regulatory_basis"] = infer_regulatory_basis_from_recorded_at(recorded_at)
    p.setdefault("session_id", p.get("session_id", ""))
    p.setdefault("risk_tier", p.get("risk_tier", ""))
    p.setdefault("recommended_limit_usd", p.get("recommended_limit_usd", 0.0))
    p.setdefault("analysis_duration_ms", p.get("analysis_duration_ms", 0))
    p.setdefault("input_data_hash", p.get("input_data_hash", ""))
    p.setdefault("data_quality_caveats", p.get("data_quality_caveats") or [])

    return ev.model_copy(update={"event_version": 2, "payload": p})


async def upcast_decision_generated_v1_to_v2(ev: StoredEvent, store: "EventStore") -> StoredEvent:
    """
    v1 -> v2: populate model_versions{} by read-only replay of contributing agent streams.

    Uses load_stream(apply_upcast=False) to avoid recursive upcast while reading AgentContextLoaded.
    """
    p = dict(ev.payload)
    sessions = list(p.get("contributing_agent_sessions") or [])
    model_versions: Dict[str, str] = dict(p.get("model_versions") or {})

    for session_key in sessions:
        stream_id = f"agent-{session_key}"
        events = await store.load_stream(stream_id, apply_upcast=False)
        for e in events:
            if e.event_type == "AgentContextLoaded":
                mv = e.payload.get("model_version") if isinstance(e.payload, dict) else None
                model_versions[str(session_key)] = str(mv or "unknown")
                break
        else:
            model_versions[str(session_key)] = "unknown"

    p["model_versions"] = model_versions
    p.setdefault("decision_basis_summary", p.get("decision_basis_summary", ""))

    return ev.model_copy(update={"event_version": 2, "payload": p})
