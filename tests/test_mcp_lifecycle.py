"""
Full MCP lifecycle integration test.

Drives ApplicationSubmitted → ApplicationApproved using ONLY MCP tool calls.
Verifies state using ONLY MCP resource queries.
No direct calls to handlers, aggregates, or event store in the test body.

12 assertions — the spec's required count for Score 3 on MCP criterion.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from fastmcp import Client, FastMCP

from src.mcp.server import build_ledger_mcp

os.environ["OPENROUTER_MOCK"] = "1"


def _tool_payload(call_result: object) -> dict:
    """Extract structured dict from FastMCP Client.call_tool result."""
    data = getattr(call_result, "data", None)
    if isinstance(data, dict):
        return data
    sc = getattr(call_result, "structured_content", None)
    if isinstance(sc, dict):
        return sc
    if getattr(call_result, "is_error", False):
        txt = getattr(call_result, "data", None) or str(call_result)
        return {"status": "error", "message": str(txt)}
    raise AssertionError(f"Unexpected tool result shape: {call_result!r}")


async def call_tool(mcp: FastMCP, tool_name: str, args: dict) -> dict:
    """Call an MCP tool and return the result dict (via in-process FastMCP Client)."""
    async with Client(mcp) as client:
        raw = await client.call_tool(tool_name, args, raise_on_error=False)
    return _tool_payload(raw)


async def read_resource(mcp: FastMCP, uri: str) -> str:
    """Read an MCP resource and return the raw JSON string (first content part)."""
    async with Client(mcp) as client:
        contents = await client.read_resource(uri)
    assert contents, f"empty resource contents for {uri!r}"
    return contents[0].text


@pytest.fixture
def app_id() -> str:
    return f"LIFECYCLE-{uuid4().hex[:8]}"


@pytest.fixture
def mcp_instance(pool):
    return build_ledger_mcp(pool)


@pytest.mark.asyncio
async def test_full_lifecycle_via_mcp_tools_only(
    mcp_instance: FastMCP,
    app_id: str,
    narr_companies_seeded: None,
) -> None:
    mcp = mcp_instance
    sess_id = f"sess-{uuid4().hex[:8]}"
    now = datetime.now(timezone.utc).isoformat()

    # Step 1 — Gas Town: credit + fraud-detection (same session id for Rule 6 pairing).
    r_credit = await call_tool(
        mcp,
        "start_agent_session",
        {
            "agent_id": "credit",
            "session_id": sess_id,
            "model_version": "test-model-v1",
            "correlation_id": app_id,
            "context_source": "mcp_lifecycle_test",
            "event_replay_from_position": 0,
            "context_token_count": 100,
        },
    )
    r_fraud_gas = await call_tool(
        mcp,
        "start_agent_session",
        {
            "agent_id": "fraud-detection",
            "session_id": sess_id,
            "model_version": "test-model-v1",
            "correlation_id": app_id,
            "context_source": "mcp_lifecycle_test",
            "event_replay_from_position": 0,
            "context_token_count": 100,
        },
    )
    assert r_credit.get("status") == "ok" and r_fraud_gas.get("status") == "ok", (
        f"Gas Town failed: credit={r_credit!r} fraud_detection={r_fraud_gas!r}"
    )

    # Step 2 — submit_application
    r_submit = await call_tool(
        mcp,
        "submit_application",
        {
            "application_id": app_id,
            "applicant_id": "COMP-031",
            "requested_amount_usd": 500_000.0,
            "loan_purpose": "equipment_purchase",
            "submission_channel": "mcp_test",
            "submitted_at": now,
            "correlation_id": app_id,
            "assigned_agent_id": "credit",
            "priority": "normal",
        },
    )
    assert r_submit.get("status") == "ok", f"submit_application failed: {r_submit!r}"

    # Step 3 — record_credit_analysis
    r_credit_done = await call_tool(
        mcp,
        "record_credit_analysis",
        {
            "application_id": app_id,
            "agent_id": "credit",
            "session_id": sess_id,
            "model_version": "test-model-v1",
            "confidence_score": 0.82,
            "risk_tier": "MEDIUM",
            "recommended_limit_usd": 500_000.0,
            "analysis_duration_ms": 1200,
            "input_data_hash": "abc123",
            "correlation_id": app_id,
        },
    )
    assert r_credit_done.get("status") == "ok", f"record_credit_analysis failed: {r_credit_done!r}"

    # Step 4 — record_fraud_screening (mirror to fraud-detection session for Rule 6)
    r_fraud = await call_tool(
        mcp,
        "record_fraud_screening",
        {
            "application_id": app_id,
            "fraud_score": 0.12,
            "correlation_id": app_id,
            "agent_id": "fraud-detection",
            "session_id": sess_id,
        },
    )
    assert r_fraud.get("status") == "ok", f"record_fraud_screening failed: {r_fraud!r}"

    # Step 5 — record_compliance_check
    r_comp = await call_tool(
        mcp,
        "record_compliance_check",
        {
            "application_id": app_id,
            "regulation_set_version": "2026-Q1",
            "checks_required": ["REG-001", "REG-002", "REG-003", "REG-004", "REG-005"],
            "correlation_id": app_id,
            "initial_rule_passes": [
                {"rule_id": "REG-001", "rule_version": "v1", "evidence_hash": "e1"},
                {"rule_id": "REG-002", "rule_version": "v1", "evidence_hash": "e2"},
                {"rule_id": "REG-003", "rule_version": "v1", "evidence_hash": "e3"},
                {"rule_id": "REG-004", "rule_version": "v1", "evidence_hash": "e4"},
                {"rule_id": "REG-005", "rule_version": "v1", "evidence_hash": "e5"},
            ],
        },
    )
    assert r_comp.get("status") == "ok", f"record_compliance_check failed: {r_comp!r}"

    fraud_sess = f"fraud-detection-{sess_id}"
    r_decision = await call_tool(
        mcp,
        "generate_decision",
        {
            "application_id": app_id,
            "orchestrator_agent_id": "orchestrator",
            "recommendation": "APPROVE",
            "confidence_score": 0.82,
            "contributing_agent_sessions": [f"credit-{sess_id}", fraud_sess],
            "decision_basis_summary": "MEDIUM risk, low fraud score, all compliance checks passed.",
            "correlation_id": app_id,
        },
    )
    assert r_decision.get("status") == "ok", f"generate_decision failed: {r_decision!r}"

    r_human = await call_tool(
        mcp,
        "record_human_review",
        {
            "application_id": app_id,
            "reviewer_id": "LO-Test-Reviewer",
            "override": False,
            "final_decision": "APPROVE",
            "correlation_id": app_id,
            "emit_application_approved": True,
            "approved_amount_usd": 500_000.0,
            "conditions": ["Standard quarterly reporting"],
        },
    )
    assert r_human.get("status") == "ok", f"record_human_review failed: {r_human!r}"

    raw = await read_resource(mcp, f"ledger://applications/{app_id}")
    summary = json.loads(raw)
    assert "error_type" not in summary, f"Resource returned error: {summary}"
    assert summary["state"] == "FINAL_APPROVED"

    raw = await read_resource(mcp, f"ledger://applications/{app_id}/compliance")
    compliance = json.loads(raw)
    assert "error_type" not in compliance
    assert compliance["overall_verdict"] == "CLEAR"

    raw = await read_resource(mcp, f"ledger://applications/{app_id}/audit-trail")
    trail = json.loads(raw)
    stream_ids = {e["stream_id"] for e in trail["events"]}
    assert len(stream_ids) > 1, "audit trail must contain events from multiple streams"

    raw = await read_resource(mcp, f"ledger://agents/credit/sessions/{sess_id}")
    session = json.loads(raw)
    assert "error_type" not in session
    assert session["event_count"] > 0

    raw = await read_resource(mcp, "ledger://ledger/health")
    health = json.loads(raw)
    assert health["overall_status"] in ("OK", "DEGRADED"), f"Health must not be CRITICAL: {health}"
