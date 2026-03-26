"""
Self-contained regulatory examination JSON for offline verification.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

import asyncpg

from src.integrity.audit_chain import fold_integrity_chain
from src.models.events import StoredEvent
from src.what_if.memory_projections import InMemoryPhase3Projections
from src.what_if.projector import load_application_events_ordered

PACKAGE_VERSION = "1.0"

SIGNIFICANT_EVENT_TYPES = frozenset(
    {
        "ApplicationSubmitted",
        "CreditAnalysisRequested",
        "FraudScreeningCompleted",
        "CreditAnalysisCompleted",
        "ComplianceReviewStarted",
        "ComplianceCheckRequested",
        "ComplianceRulePassed",
        "ComplianceRuleFailed",
        "DecisionGenerated",
        "HumanReviewRequested",
        "HumanReviewCompleted",
        "ApplicationApproved",
        "ApplicationDeclined",
        "AgentContextLoaded",
    }
)


def _json_safe(v: Any) -> Any:
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, UUID):
        return str(v)
    if isinstance(v, dict):
        return {str(k): _json_safe(val) for k, val in v.items()}
    if isinstance(v, list):
        return [_json_safe(x) for x in v]
    return v


def event_to_dict(ev: StoredEvent) -> dict[str, Any]:
    return {
        "event_id": str(ev.event_id),
        "stream_id": ev.stream_id,
        "stream_position": ev.stream_position,
        "global_position": ev.global_position,
        "event_type": ev.event_type,
        "event_version": ev.event_version,
        "payload": _json_safe(ev.payload),
        "metadata": _json_safe(ev.metadata),
        "recorded_at": ev.recorded_at.isoformat() if ev.recorded_at else None,
    }


def _sentence_for(ev: StoredEvent) -> Optional[str]:
    et = ev.event_type
    p: dict[str, Any] = ev.payload if isinstance(ev.payload, dict) else {}
    app = p.get("application_id", "")
    if et == "ApplicationSubmitted":
        return (
            f"Application {app} submitted for {p.get('requested_amount_usd')} USD "
            f"via {p.get('submission_channel')}."
        )
    if et == "CreditAnalysisRequested":
        return f"Credit analysis requested for application {app} (agent {p.get('assigned_agent_id')})."
    if et == "FraudScreeningCompleted":
        return f"Fraud screening completed for {app} (score {p.get('fraud_score')})."
    if et == "CreditAnalysisCompleted":
        return (
            f"Credit analysis completed for {app}: tier {p.get('risk_tier')}, "
            f"confidence {p.get('confidence_score')}."
        )
    if et == "ComplianceReviewStarted":
        return f"Compliance review started for application {app}."
    if et == "ComplianceCheckRequested":
        return f"Compliance check requested for {app} ({len(p.get('checks_required') or [])} checks)."
    if et == "ComplianceRulePassed":
        return f"Compliance rule {p.get('rule_id')} passed for {app}."
    if et == "ComplianceRuleFailed":
        return f"Compliance rule {p.get('rule_id')} failed for {app}."
    if et == "DecisionGenerated":
        return (
            f"Decision generated for {app}: {p.get('recommendation')} "
            f"(confidence {p.get('confidence_score')})."
        )
    if et == "HumanReviewRequested":
        return (
            f"Human review requested for {app}"
            + (f" by {p.get('requested_by')}" if p.get("requested_by") else "")
            + "."
        )
    if et == "HumanReviewCompleted":
        return (
            f"Human review by {p.get('reviewer_id')} for {app}: final {p.get('final_decision')}, "
            f"override={p.get('override')}."
        )
    if et == "ApplicationApproved":
        return f"Application {app} approved for {p.get('approved_amount_usd')} USD."
    if et == "ApplicationDeclined":
        return f"Application {app} declined."
    if et == "AgentContextLoaded":
        return (
            f"Agent context loaded for {p.get('agent_id')} session {p.get('session_id')} "
            f"(model {p.get('model_version')})."
        )
    return None


def _agent_metadata_rows(events: List[StoredEvent]) -> List[dict[str, Any]]:
    rows: List[dict[str, Any]] = []
    for ev in events:
        if ev.event_type not in ("CreditAnalysisCompleted", "AgentContextLoaded"):
            continue
        p = ev.payload if isinstance(ev.payload, dict) else {}
        rows.append(
            {
                "stream_id": ev.stream_id,
                "event_type": ev.event_type,
                "event_id": str(ev.event_id),
                "recorded_at": ev.recorded_at.isoformat() if ev.recorded_at else None,
                "agent_id": p.get("agent_id"),
                "session_id": p.get("session_id"),
                "model_version": p.get("model_version"),
                "confidence_score": p.get("confidence_score"),
                "input_data_hash": p.get("input_data_hash"),
            }
        )
    return rows


def _events_at_or_before(events: List[StoredEvent], cutoff: datetime) -> List[StoredEvent]:
    if cutoff.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=timezone.utc)
    out: List[StoredEvent] = []
    for ev in events:
        rt = ev.recorded_at
        if rt.tzinfo is None:
            rt = rt.replace(tzinfo=timezone.utc)
        if rt <= cutoff:
            out.append(ev)
    return out


def _projections_at_cutoff(events_slice: List[StoredEvent]) -> dict[str, Any]:
    dense: List[StoredEvent] = []
    for i, ev in enumerate(sorted(events_slice, key=lambda e: (e.global_position, e.stream_id)), start=1):
        dense.append(ev.model_copy(update={"global_position": i}))
    mem = InMemoryPhase3Projections()
    mem.apply_all(dense)
    snap = mem.snapshot_dict()
    last_state: Optional[dict[str, Any]] = None
    for row in reversed(mem.compliance_timeline):
        last_state = row.get("compliance_state")
        break
    snap["ComplianceAuditView"]["compliance_state_at_cutoff"] = last_state
    return snap


async def generate_regulatory_package(
    pool: asyncpg.Pool,
    application_id: str,
    examination_date: datetime,
) -> dict[str, Any]:
    if examination_date.tzinfo is None:
        examination_date = examination_date.replace(tzinfo=timezone.utc)

    events = await load_application_events_ordered(pool, application_id)
    events_by_stream: dict[str, List[StoredEvent]] = {}
    for ev in events:
        events_by_stream.setdefault(ev.stream_id, []).append(ev)
    for sid in events_by_stream:
        events_by_stream[sid].sort(key=lambda e: e.stream_position)

    event_streams = {
        sid: [event_to_dict(e) for e in lst] for sid, lst in sorted(events_by_stream.items())
    }

    can_blob = json.dumps(event_streams, sort_keys=True, separators=(",", ":"), default=str)
    events_canonical_sha256 = hashlib.sha256(can_blob.encode("utf-8")).hexdigest()

    loan_stream = f"loan-{application_id}"
    loan_events = events_by_stream.get(loan_stream, [])
    business_loan = [e for e in loan_events if e.event_type != "AuditIntegrityCheckRun"]
    integrity_hash = fold_integrity_chain(business_loan)

    slice_events = _events_at_or_before(events, examination_date)
    projections_snapshot = _projections_at_cutoff(slice_events)

    narrative: List[str] = []
    for ev in sorted(events, key=lambda e: (e.global_position, e.stream_id)):
        if ev.event_type in SIGNIFICANT_EVENT_TYPES:
            s = _sentence_for(ev)
            if s:
                narrative.append(s)

    return {
        "package_version": PACKAGE_VERSION,
        "application_id": application_id,
        "examination_date": examination_date.isoformat(),
        "event_streams": event_streams,
        "projections_at_examination": projections_snapshot,
        "audit_integrity": {
            "stream_id": loan_stream,
            "integrity_hash": integrity_hash,
            "events_verified_count": len(business_loan),
            "algorithm": "fold_integrity_chain",
        },
        "narrative": narrative,
        "agent_metadata": _agent_metadata_rows(events),
        "verification": {
            "events_canonical_sha256": events_canonical_sha256,
            "notes": "SHA-256 over event_streams JSON, sort_keys=true, separators=(',', ':').",
        },
    }


def verify_regulatory_package(package: dict[str, Any]) -> Tuple[bool, List[str]]:
    errors: List[str] = []
    for k in (
        "package_version",
        "application_id",
        "examination_date",
        "event_streams",
        "audit_integrity",
        "verification",
    ):
        if k not in package:
            errors.append(f"missing_field:{k}")

    ev_streams = package.get("event_streams")
    ver = package.get("verification") or {}
    expected_sha = ver.get("events_canonical_sha256") if isinstance(ver, dict) else None

    if isinstance(ev_streams, dict) and expected_sha:
        can_blob = json.dumps(ev_streams, sort_keys=True, separators=(",", ":"), default=str)
        got = hashlib.sha256(can_blob.encode("utf-8")).hexdigest()
        if got != expected_sha:
            errors.append(f"events_canonical_sha256_mismatch expected={expected_sha} got={got}")

    aid = package.get("application_id")
    audit = package.get("audit_integrity") or {}
    if isinstance(audit, dict) and isinstance(ev_streams, dict) and isinstance(aid, str):
        loan_sid = audit.get("stream_id") or f"loan-{aid}"
        loan_dicts = ev_streams.get(loan_sid) or []
        try:
            loan_events: List[StoredEvent] = []
            for d in loan_dicts:
                loan_events.append(
                    StoredEvent(
                        event_id=UUID(str(d["event_id"])),
                        stream_id=str(d["stream_id"]),
                        stream_position=int(d["stream_position"]),
                        global_position=int(d["global_position"]),
                        event_type=str(d["event_type"]),
                        event_version=int(d["event_version"]),
                        payload=dict(d.get("payload") or {}),
                        metadata=dict(d.get("metadata") or {}),
                        recorded_at=datetime.fromisoformat(str(d["recorded_at"]).replace("Z", "+00:00")),
                    )
                )
            business = [e for e in loan_events if e.event_type != "AuditIntegrityCheckRun"]
            chain = fold_integrity_chain(business)
            if chain != audit.get("integrity_hash"):
                errors.append(
                    f"integrity_hash_mismatch expected={audit.get('integrity_hash')} got={chain}"
                )
            if len(business) != int(audit.get("events_verified_count", -1)):
                errors.append(
                    "integrity_event_count_mismatch "
                    f"package={audit.get('events_verified_count')} recomputed={len(business)}"
                )
        except (KeyError, ValueError, TypeError) as e:
            errors.append(f"audit_recompute_error:{e}")

    return len(errors) == 0, errors
