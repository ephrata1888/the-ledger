"""
Projection 2: AgentPerformanceLedger — metrics by model_version (credit analyses on agent streams).

Never store PII in logs without encryption.
"""

from __future__ import annotations

import json
from typing import Any, Dict

import asyncpg

from src.models.events import StoredEvent
from src.projections.sql_ident import validate_pg_identifier


def _payload(ev: StoredEvent) -> Dict[str, Any]:
    return ev.payload if isinstance(ev.payload, dict) else json.loads(ev.payload)  # type: ignore[arg-type]


class AgentPerformanceProjection:
    projection_name = "AgentPerformanceLedger"

    def __init__(self, table_name: str = "projection_agent_performance") -> None:
        self._table = validate_pg_identifier(table_name)

    async def apply(self, conn: asyncpg.Connection, ev: StoredEvent) -> None:
        p = _payload(ev)
        sid = ev.stream_id

        if ev.event_type == "CreditAnalysisCompleted" and sid.startswith("agent-"):
            await self._credit_completed(conn, p)
        elif ev.event_type == "DecisionGenerated" and sid.startswith("loan-"):
            await self._decision(conn, p)
        elif ev.event_type == "HumanReviewCompleted" and sid.startswith("loan-"):
            await self._human_review(conn, p)

    async def _credit_completed(self, conn: asyncpg.Connection, p: Dict[str, Any]) -> None:
        t = self._table
        mv = str(p.get("model_version") or "unknown")
        raw_c = p.get("confidence_score")
        conf = float(raw_c) if raw_c is not None else 0.0
        dur = float(p.get("analysis_duration_ms", 0.0))
        await conn.execute(
            f"""
            INSERT INTO {t} (
              model_version, analyses_completed, sum_confidence, sum_duration_ms,
              decision_approve_count, decision_decline_count, decision_refer_count,
              human_review_total, human_override_true_count, updated_at
            )
            VALUES ($1, 1, $2, $3, 0, 0, 0, 0, 0, NOW())
            ON CONFLICT (model_version) DO UPDATE SET
              analyses_completed = {t}.analyses_completed + 1,
              sum_confidence = {t}.sum_confidence + EXCLUDED.sum_confidence,
              sum_duration_ms = {t}.sum_duration_ms + EXCLUDED.sum_duration_ms,
              updated_at = NOW()
            """,
            mv,
            conf,
            dur,
        )

    async def _decision(self, conn: asyncpg.Connection, p: Dict[str, Any]) -> None:
        """Bucket DecisionGenerated metrics by first model_versions entry or orchestrator default."""
        t = self._table
        mvs = p.get("model_versions") or {}
        if isinstance(mvs, dict) and mvs:
            mv = str(next(iter(mvs.values())))
        else:
            mv = "orchestrator-default"
        rec = str(p.get("recommendation", "REFER")).upper()
        approve = 1 if rec == "APPROVE" else 0
        decline = 1 if rec == "DECLINE" else 0
        refer = 1 if rec == "REFER" else 0
        await conn.execute(
            f"""
            INSERT INTO {t} (
              model_version, analyses_completed, sum_confidence, sum_duration_ms,
              decision_approve_count, decision_decline_count, decision_refer_count,
              human_review_total, human_override_true_count, updated_at
            )
            VALUES ($1, 0, 0, 0, $2, $3, $4, 0, 0, NOW())
            ON CONFLICT (model_version) DO UPDATE SET
              decision_approve_count = {t}.decision_approve_count + EXCLUDED.decision_approve_count,
              decision_decline_count = {t}.decision_decline_count + EXCLUDED.decision_decline_count,
              decision_refer_count = {t}.decision_refer_count + EXCLUDED.decision_refer_count,
              updated_at = NOW()
            """,
            mv,
            approve,
            decline,
            refer,
        )

    async def _human_review(self, conn: asyncpg.Connection, p: Dict[str, Any]) -> None:
        t = self._table
        mv = "human-review"
        override = bool(p.get("override"))
        await conn.execute(
            f"""
            INSERT INTO {t} (
              model_version, analyses_completed, sum_confidence, sum_duration_ms,
              decision_approve_count, decision_decline_count, decision_refer_count,
              human_review_total, human_override_true_count, updated_at
            )
            VALUES ($1, 0, 0, 0, 0, 0, 0, 1, $2, NOW())
            ON CONFLICT (model_version) DO UPDATE SET
              human_review_total = {t}.human_review_total + 1,
              human_override_true_count = {t}.human_override_true_count + EXCLUDED.human_override_true_count,
              updated_at = NOW()
            """,
            mv,
            1 if override else 0,
        )


async def fetch_performance_row(
    conn: asyncpg.Connection,
    model_version: str,
    *,
    table_name: str = "projection_agent_performance",
) -> asyncpg.Record | None:
    t = validate_pg_identifier(table_name)
    return await conn.fetchrow(
        f"SELECT * FROM {t} WHERE model_version = $1",
        model_version,
    )


def rates_from_row(row: asyncpg.Record | None) -> Dict[str, float]:
    """Derive approve_rate and refer_rate from decision counts."""
    if row is None:
        return {"approve_rate": 0.0, "refer_rate": 0.0, "avg_confidence": 0.0, "avg_duration_ms": 0.0}
    ap = int(row["decision_approve_count"])
    de = int(row["decision_decline_count"])
    rf = int(row["decision_refer_count"])
    tot = ap + de + rf
    n = int(row["analyses_completed"])
    avg_conf = float(row["sum_confidence"]) / n if n else 0.0
    avg_dur = float(row["sum_duration_ms"]) / n if n else 0.0
    return {
        "approve_rate": (ap / tot) if tot else 0.0,
        "refer_rate": (rf / tot) if tot else 0.0,
        "avg_confidence": avg_conf,
        "avg_duration_ms": avg_dur,
    }
