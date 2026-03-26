"""
MCP Resources — query side (read path).
All resources read from projections except two justified direct stream loads:
  - ledger://applications/{id}/audit-trail  → AuditLedgerAggregate.load_cross_stream_timeline()
  - ledger://agents/{id}/sessions/{id}      → EventStore.load_stream() (session stream IS the record)
Register by calling register_resources(mcp, rt).
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

import asyncpg
from fastmcp import FastMCP

from src.aggregates.audit_ledger import AuditLedgerAggregate
from src.mcp.runtime import LedgerRuntime
from src.models.events import StoredEvent


def _resource_json(data: Any) -> str:
    """FastMCP resources must return str/bytes (JSON text for clients)."""
    return json.dumps(data, default=str, indent=2)


def _stored_event_to_json(ev: StoredEvent) -> dict[str, Any]:
    d = ev.model_dump(mode="json")
    return d


def _record_to_json(row: asyncpg.Record | None) -> dict[str, Any] | None:
    if row is None:
        return None
    out: dict[str, Any] = {}
    for k in row.keys():
        v = row[k]
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


def _overall_verdict_top_level(compliance_state: dict[str, Any]) -> str:
    """Expose a single verdict for MCP clients (maps projection fold to CLEAR/BLOCKED/CONDITIONAL)."""
    v = str(compliance_state.get("check_verdict") or "").strip().upper()
    if v in ("BLOCKED", "CONDITIONAL", "CLEAR"):
        return v
    overall = str(compliance_state.get("overall_status") or "")
    if overall == "PASSED":
        return "CLEAR"
    if overall == "FAILED":
        return "BLOCKED"
    return "CONDITIONAL" if overall == "IN_PROGRESS" else "CLEAR"


def _parse_as_of_timestamp(raw: str) -> datetime:
    s = raw.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def register_resources(mcp: FastMCP, rt: LedgerRuntime) -> None:
    """Register all MCP resources onto the given FastMCP instance."""

    @mcp.resource(
        "ledger://applications/{application_id}",
        description=(
            "ApplicationSummary projection (single-row read, indexed by PK). "
            "Target p99 <50ms on warm pool."
        ),
    )
    async def res_application_summary(application_id: str) -> str:
        async with rt.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM projection_application_summary WHERE application_id = $1",
                application_id,
            )
        data = _record_to_json(row)
        if data is None:
            return _resource_json(
                {
                    "error_type": "ResourceNotFound",
                    "application_id": application_id,
                    "suggested_action": "verify_application_id_or_complete_projections",
                }
            )
        return _resource_json(data)

    @mcp.resource(
        "ledger://applications/{application_id}/compliance",
        description=(
            "ComplianceAuditView — current snapshot from projection table compliance_audit_timeline. "
            "Temporal: use ``ledger://applications/{id}/compliance/as_of/{as_of}`` with ISO-8601 "
            "(equivalent semantics to HTTP ``?as_of=timestamp``). Target p99 <200ms."
        ),
    )
    async def res_compliance_current(application_id: str) -> str:
        snap = await rt.compliance_view.get_current_compliance(application_id)
        if snap is None:
            return _resource_json(
                {
                    "error_type": "ResourceNotFound",
                    "application_id": application_id,
                    "suggested_action": "record_compliance_check_or_wait_for_projection",
                }
            )
        cs = snap.get("compliance_state") or {}
        ov = _overall_verdict_top_level(cs if isinstance(cs, dict) else {})
        return _resource_json({"application_id": application_id, "overall_verdict": ov, **snap})

    @mcp.resource(
        "ledger://applications/{application_id}/compliance/as_of/{as_of}",
        description=(
            "ComplianceAuditView point-in-time (latest snapshot with recorded_at <= as_of). "
            "``as_of`` is ISO-8601; equivalent to ``ledger://.../compliance?as_of=...``."
        ),
    )
    async def res_compliance_as_of(application_id: str, as_of: str) -> str:
        at = _parse_as_of_timestamp(as_of)
        snap = await rt.compliance_view.get_compliance_at(application_id, at)
        if snap is None:
            return _resource_json(
                {
                    "error_type": "ResourceNotFound",
                    "application_id": application_id,
                    "as_of": as_of,
                    "suggested_action": "pick_later_timestamp_or_record_compliance_events",
                }
            )
        cs = snap.get("compliance_state") or {}
        ov = _overall_verdict_top_level(cs if isinstance(cs, dict) else {})
        return _resource_json(
            {"application_id": application_id, "query_as_of": as_of, "overall_verdict": ov, **snap}
        )

    @mcp.resource(
        "ledger://applications/{application_id}/audit-trail",
        description=(
            "Cross-stream audit trail for a loan application: events from loan, agent, compliance, "
            "and fraud streams interleaved in causal order (recorded_at ASC, global_position ASC). "
            "Justified direct stream load — no single projection captures cross-stream causal ordering. "
            "Low-frequency compliance/forensic operation. Target p99 <500ms."
        ),
    )
    async def res_audit_trail(application_id: str) -> str:
        # Justified exception to the projection-only rule:
        # The audit trail is inherently cross-stream. No single projection captures events from
        # loan, agent, compliance, and fraud streams in causal order for a single application.
        # This is a low-frequency compliance/debug operation with a 500ms SLO (vs 50ms for projections).
        agg = await AuditLedgerAggregate.load(rt.store, entity_type="loan", entity_id=application_id)
        events = await agg.load_cross_stream_timeline(rt.store)
        return _resource_json(
            {
                "application_id": application_id,
                "streams_audited": agg.cross_stream_refs,
                "total_events": len(events),
                "events": [_stored_event_to_json(e) for e in events],
            }
        )

    @mcp.resource(
        "ledger://agents/{agent_id}/performance",
        description=(
            "AgentPerformanceLedger projection rows. "
            "NOTE: The projection is keyed by model_version, not agent_id. "
            "All projection rows are returned; filter client-side by known model versions for this agent. "
            "agent_id is recorded for context. Target p99 <50ms."
        ),
    )
    async def res_agent_performance(agent_id: str) -> str:
        async with rt.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM projection_agent_performance ORDER BY updated_at DESC"
            )
        perf_rows = [_record_to_json(r) for r in rows]
        return _resource_json(
            {
                "agent_id": agent_id,
                "note": (
                    "projection_agent_performance is keyed by model_version. "
                    "Filter by the model_version(s) known for this agent."
                ),
                "projection_rows": perf_rows,
            }
        )

    @mcp.resource(
        "ledger://agents/{agent_id}/sessions/{session_id}",
        description=(
            "Direct load of agent-{agent_id}-{session_id} for session reconstruction (not a projection read)."
        ),
    )
    async def res_agent_session(agent_id: str, session_id: str) -> str:
        stream_id = f"agent-{agent_id}-{session_id}"
        events = await rt.store.load_stream(stream_id)
        return _resource_json(
            {
                "stream_id": stream_id,
                "event_count": len(events),
                "events": [_stored_event_to_json(e) for e in events],
            }
        )

    @mcp.resource(
        "ledger://ledger/health",
        description=(
            "Watchdog: ProjectionDaemon lag for ApplicationSummary, AgentPerformanceLedger, "
            "ComplianceAuditView. Per-projection lag classification: OK (<500ms all), "
            "DEGRADED (any 500ms–2000ms), CRITICAL (any >2000ms). Target p99 <10ms."
        ),
    )
    async def res_health() -> str:
        lags = await rt.daemon.get_all_lags()

        def _classify(lag_ms: float) -> str:
            if lag_ms > 2000:
                return "CRITICAL"
            if lag_ms >= 500:
                return "DEGRADED"
            return "OK"

        projection_statuses = [
            {**asdict(x), "status": _classify(x.lag_ms)}
            for x in lags
        ]
        all_statuses = [p["status"] for p in projection_statuses]
        overall = "CRITICAL" if "CRITICAL" in all_statuses else "DEGRADED" if "DEGRADED" in all_statuses else "OK"

        return _resource_json(
            {
                "overall_status": overall,
                "checked_at": datetime.now(timezone.utc).isoformat(),
                "projections": projection_statuses,
                "slo_thresholds_ms": {
                    "OK": "<500",
                    "DEGRADED": "500–2000",
                    "CRITICAL": ">2000",
                },
            }
        )
