"""
Projection 3: ComplianceAuditView — temporal compliance state + regulatory read API.

Temporal strategy (snapshot choice):
-------------------------------------
We persist **one snapshot row per compliance-relevant event** on the compliance-* stream,
keyed by (application_id, global_position). This is an **event-triggered** snapshot chain
(not periodic time-bucket snapshots). Ordering for point-in-time queries uses **recorded_at**
from the canonical events table (wall time when the fact was recorded), with **global_position**
as a deterministic tie-breaker.

**Why not pure event-count indexing alone?** Regulators and loan officers ask "what did we know
at time T?" — **recorded_at** answers that. **global_position** ensures total ordering when two
events share the same timestamp.

**get_compliance_at(application_id, timestamp)** returns the latest snapshot with
`recorded_at <= timestamp`.

rebuild_from_scratch:
----------------------
Truncates the timeline and resets the projection checkpoint to 0, then replays all events
in global order. For **near-zero read downtime**, use **rebuild_blue_green** (shadow table +
transactional rename) instead of TRUNCATE on the live timeline.

Never store PII in logs without encryption.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import asyncpg

from src.models.events import StoredEvent
from src.projections.sql_ident import validate_pg_identifier

if TYPE_CHECKING:
    from src.projections.daemon import ProjectionDaemon


def _payload(ev: StoredEvent) -> Dict[str, Any]:
    return ev.payload if isinstance(ev.payload, dict) else json.loads(ev.payload)  # type: ignore[arg-type]


def _jsonb_to_dict(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)


def _app_id_compliance(stream_id: str, payload: Dict[str, Any]) -> Optional[str]:
    aid = payload.get("application_id")
    if aid is not None:
        return str(aid)
    if stream_id.startswith("compliance-"):
        return stream_id[11:]
    return None


def _fold_compliance(prev: Optional[Dict[str, Any]], ev: StoredEvent, p: Dict[str, Any]) -> Dict[str, Any]:
    base = dict(prev) if prev else {
        "required_checks": [],
        "passed_rules": [],
        "failed_rules": [],
        "overall_status": "UNKNOWN",
    }
    req: List[str] = list(base.get("required_checks") or [])
    passed: List[str] = list(base.get("passed_rules") or [])
    failed: List[Dict[str, Any]] = list(base.get("failed_rules") or [])

    et = ev.event_type
    if et == "ComplianceCheckRequested":
        req = [str(x) for x in (p.get("checks_required") or [])]
        base["required_checks"] = req
    elif et == "ComplianceRulePassed":
        rid = str(p.get("rule_id", ""))
        if rid and rid not in passed:
            passed.append(rid)
        base["passed_rules"] = passed
    elif et == "ComplianceRuleFailed":
        failed.append(
            {
                "rule_id": str(p.get("rule_id", "")),
                "failure_reason": str(p.get("failure_reason", "")),
                "is_hard_block": bool(p.get("is_hard_block", False)),
            }
        )
        base["failed_rules"] = failed
    elif et == "ComplianceRuleNoted":
        noted: List[Dict[str, Any]] = list(base.get("noted_rules") or [])
        noted.append(
            {
                "rule_id": str(p.get("rule_id", "")),
                "note_type": str(p.get("note_type", "")),
            }
        )
        base["noted_rules"] = noted
    elif et == "ComplianceCheckCompleted":
        base["check_verdict"] = str(p.get("overall_verdict", ""))

    req_set = set(base["required_checks"])
    passed_set = set(base["passed_rules"])
    verdict = str(base.get("check_verdict", ""))
    if verdict == "BLOCKED":
        overall = "FAILED"
    elif failed:
        overall = "FAILED"
    elif req_set and req_set.issubset(passed_set):
        overall = "PASSED"
    elif req_set:
        overall = "IN_PROGRESS"
    else:
        overall = "IN_PROGRESS"

    base["overall_status"] = overall
    base["last_event_type"] = et
    return base


def _compliance_status_for_summary(state: Dict[str, Any]) -> str:
    return str(state.get("overall_status", "UNKNOWN"))


class ComplianceAuditProjection:
    projection_name = "ComplianceAuditView"

    def __init__(
        self,
        timeline_table: str = "compliance_audit_timeline",
        summary_table: str = "projection_application_summary",
    ) -> None:
        self._timeline = validate_pg_identifier(timeline_table)
        self._summary = validate_pg_identifier(summary_table)

    async def apply(self, conn: asyncpg.Connection, ev: StoredEvent) -> None:
        if not ev.stream_id.startswith("compliance-"):
            return
        p = _payload(ev)
        app_id = _app_id_compliance(ev.stream_id, p)
        if not app_id:
            return
        if ev.event_type not in (
            "ComplianceCheckRequested",
            "ComplianceRulePassed",
            "ComplianceRuleFailed",
            "ComplianceRuleNoted",
            "ComplianceCheckCompleted",
        ):
            return

        tl = self._timeline
        sm = self._summary
        prev_row = await conn.fetchrow(
            f"""
            SELECT compliance_state
            FROM {tl}
            WHERE application_id = $1
            ORDER BY global_position DESC
            LIMIT 1
            """,
            app_id,
        )
        prev = None
        if prev_row and prev_row["compliance_state"]:
            prev = _jsonb_to_dict(prev_row["compliance_state"])

        state = _fold_compliance(prev, ev, p)

        await conn.execute(
            f"""
            INSERT INTO {tl} (
              application_id, global_position, recorded_at, compliance_state
            )
            VALUES ($1, $2, $3, $4::jsonb)
            ON CONFLICT (application_id, global_position) DO UPDATE SET
              recorded_at = EXCLUDED.recorded_at,
              compliance_state = EXCLUDED.compliance_state
            """,
            app_id,
            ev.global_position,
            ev.recorded_at,
            json.dumps(state),
        )

        status = _compliance_status_for_summary(state)
        await conn.execute(
            f"""
            INSERT INTO {sm} (
              application_id, state, applicant_id, requested_amount_usd,
              risk_tier, fraud_score, compliance_status, last_event_at
            )
            VALUES ($1, NULL, NULL, NULL, NULL, NULL, $2, $3)
            ON CONFLICT (application_id) DO UPDATE SET
              compliance_status = EXCLUDED.compliance_status,
              last_event_at = CASE
                WHEN {sm}.last_event_at IS NULL THEN EXCLUDED.last_event_at
                ELSE GREATEST({sm}.last_event_at, EXCLUDED.last_event_at)
              END
            """,
            app_id,
            status,
            ev.recorded_at,
        )


class ComplianceAuditView:
    """Query API + rebuild orchestration."""

    def __init__(self, pool: asyncpg.Pool, *, timeline_table: str = "compliance_audit_timeline") -> None:
        self._pool = pool
        self._timeline = validate_pg_identifier(timeline_table)

    async def get_compliance_at(
        self,
        application_id: str,
        at: datetime,
    ) -> Optional[Dict[str, Any]]:
        """Point-in-time compliance snapshot (latest row with recorded_at <= at)."""
        if at.tzinfo is None:
            at = at.replace(tzinfo=timezone.utc)
        tl = self._timeline
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT compliance_state, global_position, recorded_at
                FROM {tl}
                WHERE application_id = $1 AND recorded_at <= $2
                ORDER BY recorded_at DESC, global_position DESC
                LIMIT 1
                """,
                application_id,
                at,
            )
        if not row:
            return None
        return {
            "compliance_state": _jsonb_to_dict(row["compliance_state"]),
            "global_position": int(row["global_position"]),
            "recorded_at": row["recorded_at"],
        }

    async def get_current_compliance(self, application_id: str) -> Optional[Dict[str, Any]]:
        tl = self._timeline
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT compliance_state, global_position, recorded_at
                FROM {tl}
                WHERE application_id = $1
                ORDER BY global_position DESC
                LIMIT 1
                """,
                application_id,
            )
        if not row:
            return None
        return {
            "compliance_state": _jsonb_to_dict(row["compliance_state"]),
            "global_position": int(row["global_position"]),
            "recorded_at": row["recorded_at"],
        }

    async def rebuild_from_scratch(
        self,
        compliance_projection: ComplianceAuditProjection,
        daemon: ProjectionDaemon,
    ) -> None:
        """
        Truncate compliance timeline, reset checkpoint, replay full event log via run_catch_up.
        """
        from src.projections.daemon import run_catch_up

        tl = compliance_projection._timeline
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(f"TRUNCATE {tl}")
                await conn.execute(
                    """
                    INSERT INTO projection_checkpoints (projection_name, last_position, updated_at)
                    VALUES ($1, 0, NOW())
                    ON CONFLICT (projection_name) DO UPDATE SET last_position = 0, updated_at = NOW()
                    """,
                    compliance_projection.projection_name,
                )

        batch_size = getattr(daemon, "_batch_size", 500)
        await run_catch_up(self._pool, compliance_projection, batch_size=batch_size)

    async def rebuild_from_scratch_blue_green(
        self,
        daemon: ProjectionDaemon,
        *,
        lag_slo_ms: float = 2000.0,
        batch_sleep_s: float = 0.02,
        batch_size: int = 500,
    ) -> None:
        """
        Blue/green rebuild of the compliance timeline (and shadow summary mirror), then transactional
        swap + merge of compliance columns into projection_application_summary. Preserves
        get_compliance_at / get_current_compliance semantics on the live timeline name.
        """
        from src.projections.blue_green import rebuild_compliance_audit_blue_green

        await rebuild_compliance_audit_blue_green(
            self,
            daemon,
            lag_slo_ms=lag_slo_ms,
            batch_sleep_s=batch_sleep_s,
            batch_size=batch_size,
        )
