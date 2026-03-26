"""
Blue/green projection rebuild: live reads must not hit missing relations during ApplicationSummary swap.

Requires PostgreSQL (DATABASE_URL).
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone

import pytest
from fastmcp import Client

from src.event_store import EventStore
from src.mcp.server import build_ledger_mcp
from src.models.events import (
    ApplicationApprovedEvent,
    ApplicationApprovedPayload,
    ApplicationSubmittedEvent,
    ApplicationSubmittedPayload,
    ComplianceReviewStartedEvent,
    ComplianceReviewStartedPayload,
    CreditAnalysisCompletedEvent,
    CreditAnalysisCompletedPayload,
    CreditAnalysisRequestedEvent,
    CreditAnalysisRequestedPayload,
    DecisionGeneratedEvent,
    DecisionGeneratedPayload,
    FraudScreeningCompletedEvent,
    FraudScreeningCompletedPayload,
    HumanReviewCompletedEvent,
    HumanReviewCompletedPayload,
)
from src.projections import ProjectionDaemon, rebuild_application_summary_blue_green, run_catch_up
from src.projections.application_summary import ApplicationSummaryProjection


def _resource_body(contents: list) -> dict:
    assert contents, "empty resource contents"
    return json.loads(contents[0].text)


def _pgish_error(text: str) -> bool:
    t = text.lower()
    return (
        "does not exist" in t
        or "undefinedtable" in t.replace(" ", "")
        or "table not found" in t
        or "relation " in t and "does not exist" in t
    )


@pytest.mark.asyncio
async def test_application_summary_blue_green_rebuild_mcp_reads_no_downtime(pool):
    store = EventStore(pool)
    apex = "APEX-0021"
    apex_stream = f"loan-{apex}"
    now = datetime.now(timezone.utc).isoformat()

    def cid() -> str:
        return str(uuid.uuid4())

    # 992 single-event streams + 8 lifecycle events on APEX-0021 = 1000 appends
    for i in range(992):
        aid = f"noise-{i}-{uuid.uuid4().hex[:8]}"
        ev = ApplicationSubmittedEvent(
            payload=ApplicationSubmittedPayload(
                application_id=aid,
                applicant_id="p",
                requested_amount_usd=float(i),
                loan_purpose="x",
                submission_channel="web",
                submitted_at=now,
            )
        )
        await store.append(stream_id=f"loan-{aid}", events=[ev], expected_version=-1, correlation_id=cid())

    apex_events = [
        ApplicationSubmittedEvent(
            payload=ApplicationSubmittedPayload(
                application_id=apex,
                applicant_id="applicant-apex",
                requested_amount_usd=125_000.0,
                loan_purpose="equipment",
                submission_channel="branch",
                submitted_at=now,
            )
        ),
        CreditAnalysisRequestedEvent(
            payload=CreditAnalysisRequestedPayload(
                application_id=apex,
                assigned_agent_id="agent-1",
                requested_at=now,
                priority="normal",
            )
        ),
        FraudScreeningCompletedEvent(
            payload=FraudScreeningCompletedPayload(application_id=apex, fraud_score=0.05)
        ),
        CreditAnalysisCompletedEvent(
            payload=CreditAnalysisCompletedPayload(
                application_id=apex,
                agent_id="agent-1",
                session_id="s1",
                model_version="credit-v1",
                confidence_score=0.88,
                risk_tier="LOW",
                recommended_limit_usd=150_000.0,
                analysis_duration_ms=200,
                input_data_hash="h1",
            )
        ),
        ComplianceReviewStartedEvent(
            payload=ComplianceReviewStartedPayload(application_id=apex, started_at=now)
        ),
        DecisionGeneratedEvent(
            payload=DecisionGeneratedPayload(
                application_id=apex,
                orchestrator_agent_id="agent-1",
                recommendation="APPROVE",
                confidence_score=0.9,
                contributing_agent_sessions=["agent-1-s1"],
                decision_basis_summary="ok",
                model_versions={"credit": "credit-v1"},
            )
        ),
        HumanReviewCompletedEvent(
            payload=HumanReviewCompletedPayload(
                application_id=apex,
                reviewer_id="h1",
                override=False,
                final_decision="APPROVED",
            )
        ),
        ApplicationApprovedEvent(
            payload=ApplicationApprovedPayload(
                application_id=apex,
                approved_amount_usd=125_000.0,
                interest_rate=5.5,
                approved_by="committee",
                effective_date=now,
            )
        ),
    ]
    v = -1
    for ev in apex_events:
        v = await store.append(
            stream_id=apex_stream,
            events=[ev],
            expected_version=v,
            correlation_id=cid(),
        )

    # 8 appends on apex_stream — total appends = 992 + 8 = 1000
    assert 992 + len(apex_events) == 1000

    app_h = ApplicationSummaryProjection()
    d0 = ProjectionDaemon(pool, batch_size=500)
    await run_catch_up(pool, app_h, batch_size=500)

    mcp = build_ledger_mcp(pool)
    uri = f"ledger://applications/{apex}"

    poll_errors: list[str] = []
    tableish_failures: list[str] = []

    async def poll_while_rebuild(task: asyncio.Task) -> None:
        async with Client(mcp) as client:
            while not task.done():
                try:
                    contents = await client.read_resource(uri)
                    raw = contents[0].text if contents else ""
                    if _pgish_error(raw):
                        tableish_failures.append(raw[:500])
                    _resource_body(contents)  # valid JSON
                except Exception as exc:  # noqa: BLE001
                    poll_errors.append(f"{type(exc).__name__}: {exc}")
                await asyncio.sleep(0.005)

    rebuild_task = asyncio.create_task(
        rebuild_application_summary_blue_green(
            pool,
            d0,
            lag_slo_ms=500.0,
            batch_sleep_s=0.04,
            batch_size=50,
        )
    )
    await poll_while_rebuild(rebuild_task)
    await rebuild_task

    assert not poll_errors, poll_errors
    assert not tableish_failures, tableish_failures

    async with Client(mcp) as client:
        final_contents = await client.read_resource(uri)
    body = _resource_body(final_contents)
    assert body.get("error_type") != "ResourceNotFound", body
    assert body.get("state") == "FINAL_APPROVED"
    assert float(body.get("requested_amount_usd") or 0) == 125_000.0

    async with pool.acquire() as conn:
        cp = await conn.fetchval(
            "SELECT last_position FROM projection_checkpoints WHERE projection_name = $1",
            ApplicationSummaryProjection.projection_name,
        )
        head = await conn.fetchval("SELECT MAX(global_position) FROM events")
    assert int(cp or 0) == int(head or 0)
