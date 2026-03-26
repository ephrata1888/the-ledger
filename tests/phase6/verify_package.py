"""
Regulatory package verification.

CLI: ``python tests/phase6/verify_package.py artifacts/regulatory_package_NARR05.json``
"""

from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.commands.handlers import handle_submit_application
from src.event_store import EventStore
from src.models.commands import SubmitApplicationCommand
from src.regulatory.package import generate_regulatory_package, verify_regulatory_package
from src.upcasting.registry import default_registry


@pytest.mark.asyncio
async def test_generated_package_verifies_offline(pool):
    store = EventStore(pool, upcaster_registry=default_registry())
    app_id = f"pkg-{uuid.uuid4().hex[:12]}"
    now = datetime.now(timezone.utc).isoformat()

    await handle_submit_application(
        store,
        SubmitApplicationCommand(
            application_id=app_id,
            applicant_id="auditor-test",
            requested_amount_usd=10_000.0,
            loan_purpose="verify",
            submission_channel="test",
            submitted_at=now,
            correlation_id=str(uuid.uuid4()),
        ),
    )

    pkg = await generate_regulatory_package(pool, app_id, datetime.now(timezone.utc))
    ok, errs = verify_regulatory_package(pkg)
    assert ok, errs


@pytest.mark.asyncio
async def test_what_if_medium_to_high_flips_recommendation(pool):
    from src.commands import handlers
    from src.event_store import EventStore
    from src.models.commands import CreditAnalysisCompletedCommand, SubmitApplicationCommand
    from src.upcasting.registry import default_registry
    from src.what_if.projector import load_application_events_ordered, run_what_if

    store = EventStore(pool, upcaster_registry=default_registry())
    app_id = f"wi-{uuid.uuid4().hex[:10]}"
    agent_id = f"ag-{uuid.uuid4().hex[:6]}"
    session_id = f"sess-{uuid.uuid4().hex[:6]}"
    now = datetime.now(timezone.utc).isoformat()
    cid = lambda: str(uuid.uuid4())

    await handlers.handle_start_agent_session(
        store, agent_id=agent_id, session_id=session_id, model_version="credit-v1", correlation_id=cid()
    )
    await handlers.handle_submit_application(
        store,
        SubmitApplicationCommand(
            application_id=app_id,
            applicant_id="c",
            requested_amount_usd=100_000.0,
            loan_purpose="whatif",
            submission_channel="t",
            submitted_at=now,
            correlation_id=cid(),
            assigned_agent_id=agent_id,
        ),
    )
    await handlers.handle_record_fraud_screening(
        store,
        application_id=app_id,
        fraud_score=0.1,
        correlation_id=cid(),
        agent_id=agent_id,
        session_id=session_id,
    )
    await handlers.handle_credit_analysis_completed(
        store,
        CreditAnalysisCompletedCommand(
            application_id=app_id,
            agent_id=agent_id,
            session_id=session_id,
            model_version="credit-v1",
            confidence_score=0.9,
            risk_tier="MEDIUM",
            recommended_limit_usd=120_000.0,
            analysis_duration_ms=50,
            input_data_hash="h1",
            correlation_id=cid(),
        ),
    )
    await handlers.handle_record_compliance_check(
        store,
        application_id=app_id,
        regulation_set_version="v1",
        checks_required=["R1"],
        correlation_id=cid(),
        initial_rule_passes=[{"rule_id": "R1", "rule_version": "1"}],
    )
    await handlers.handle_generate_decision(
        store,
        application_id=app_id,
        orchestrator_agent_id=agent_id,
        recommendation="APPROVE",
        confidence_score=0.9,
        contributing_agent_sessions=[f"{agent_id}-{session_id}"],
        decision_basis_summary="x",
        correlation_id=cid(),
    )

    events = await load_application_events_ordered(pool, app_id)
    loan = f"loan-{app_id}"
    branch = next(e for e in events if e.event_type == "CreditAnalysisCompleted" and e.stream_id == loan)
    pl = dict(branch.payload)
    pl["risk_tier"] = "HIGH"
    cf = branch.model_copy(update={"event_id": uuid.uuid4(), "payload": pl})

    wf = run_what_if(
        events,
        branch_at_event_type="CreditAnalysisCompleted",
        counterfactual_events=[cf],
        branch_stream_prefix="loan-",
    )
    assert wf.baseline_final_recommendation == "APPROVE"
    assert wf.counterfactual_final_recommendation == "DECLINE"


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python tests/phase6/verify_package.py <package.json>", file=sys.stderr)
        raise SystemExit(2)
    pkg = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    ok, errs = verify_regulatory_package(pkg)
    if ok:
        print("verify_regulatory_package: OK")
        raise SystemExit(0)
    print("verify_regulatory_package: FAILED", errs)
    raise SystemExit(1)


if __name__ == "__main__":
    main()
