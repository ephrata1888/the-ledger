"""
Projection 1: ApplicationSummary — flat read model for loan + compliance status mirror.

Compliance_status is written by ComplianceAuditProjection; loan events preserve it on upsert.

Never store PII in logs without encryption.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

import asyncpg

from src.models.events import StoredEvent
from src.projections.sql_ident import validate_pg_identifier


def _payload(ev: StoredEvent) -> Dict[str, Any]:
    return ev.payload if isinstance(ev.payload, dict) else json.loads(ev.payload)  # type: ignore[arg-type]


def _loan_application_id(stream_id: str, payload: Dict[str, Any]) -> Optional[str]:
    aid = payload.get("application_id")
    if aid is not None:
        return str(aid)
    if stream_id.startswith("loan-"):
        return stream_id[5:]
    return None


class ApplicationSummaryProjection:
    """Processes loan-* streams for lifecycle fields."""

    projection_name = "ApplicationSummary"

    def __init__(self, table_name: str = "projection_application_summary") -> None:
        self._table = validate_pg_identifier(table_name)

    async def apply(self, conn: asyncpg.Connection, ev: StoredEvent) -> None:
        if not ev.stream_id.startswith("loan-"):
            return
        p = _payload(ev)
        app_id = _loan_application_id(ev.stream_id, p)
        if not app_id:
            return
        await self._apply_loan(conn, app_id, ev, p)

    async def _apply_loan(
        self,
        conn: asyncpg.Connection,
        application_id: str,
        ev: StoredEvent,
        p: Dict[str, Any],
    ) -> None:
        t = self._table
        row = await conn.fetchrow(
            f"SELECT * FROM {t} WHERE application_id = $1",
            application_id,
        )
        cur: Dict[str, Any] = dict(row) if row else {}

        state: Optional[str] = cur.get("state")
        applicant_id = cur.get("applicant_id")
        requested = cur.get("requested_amount_usd")
        risk_tier = cur.get("risk_tier")
        fraud_score = cur.get("fraud_score")
        compliance_status = str(cur.get("compliance_status") or "UNKNOWN")
        last_at = ev.recorded_at

        et = ev.event_type
        if et == "ApplicationSubmitted":
            state = "SUBMITTED"
            applicant_id = p.get("applicant_id")
            requested = float(p.get("requested_amount_usd", 0))
        elif et == "CreditAnalysisRequested":
            state = "AWAITING_ANALYSIS"
        elif et == "FraudScreeningCompleted":
            fraud_score = float(p.get("fraud_score", 0))
        elif et == "CreditAnalysisCompleted":
            risk_tier = str(p.get("risk_tier", "") or "")
        elif et == "ComplianceReviewStarted":
            state = "COMPLIANCE_REVIEW"
        elif et == "DecisionGenerated":
            state = "PENDING_DECISION"
        elif et == "HumanReviewCompleted":
            fd = str(p.get("final_decision", "")).upper()
            if fd in ("APPROVED", "APPROVE", "DECLINED", "DECLINE"):
                state = "DECIDED_PENDING_HUMAN"
        elif et == "ApplicationApproved":
            state = "FINAL_APPROVED"
        elif et == "ApplicationDeclined":
            state = "FINAL_DECLINED"

        if et in ("FraudScreeningCompleted", "CreditAnalysisCompleted"):
            fs = fraud_score
            rt = risk_tier
            if fs is not None and rt is not None and str(rt) != "":
                state = "ANALYSIS_COMPLETE"

        await conn.execute(
            f"""
            INSERT INTO {t} (
              application_id, state, applicant_id, requested_amount_usd,
              risk_tier, fraud_score, compliance_status, last_event_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (application_id) DO UPDATE SET
              state = EXCLUDED.state,
              applicant_id = COALESCE(EXCLUDED.applicant_id, {t}.applicant_id),
              requested_amount_usd = COALESCE(EXCLUDED.requested_amount_usd, {t}.requested_amount_usd),
              risk_tier = COALESCE(EXCLUDED.risk_tier, {t}.risk_tier),
              fraud_score = COALESCE(EXCLUDED.fraud_score, {t}.fraud_score),
              compliance_status = {t}.compliance_status,
              last_event_at = EXCLUDED.last_event_at
            """,
            application_id,
            state,
            applicant_id,
            requested,
            risk_tier,
            fraud_score,
            compliance_status,
            last_at,
        )
