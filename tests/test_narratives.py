import asyncio
import json
import os
from datetime import datetime, timezone
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio

os.environ["OPENROUTER_MOCK"] = "1"

from ledger.agents.compliance_agent import ComplianceAgent  # noqa: E402
from ledger.agents.credit_analysis_agent import CreditAnalysisAgent  # noqa: E402
from ledger.agents.stub_agents import (  # noqa: E402
    DecisionOrchestratorAgent,
    DocumentProcessingAgent,
    FraudDetectionAgent,
)
from src.commands import handlers  # noqa: E402
from src.commands.handlers import _last_event_id  # noqa: E402
from src.event_store import EventStore  # noqa: E402
from src.models.commands import SubmitApplicationCommand  # noqa: E402
from src.models.events import (  # noqa: E402
    AgentSessionFailedEvent,
    AgentSessionFailedPayload,
)

pytestmark = pytest.mark.asyncio


async def _submit(
    store: EventStore,
    *,
    application_id: str,
    applicant_id: str,
    requested_amount_usd: float = 500_000.0,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    cmd = SubmitApplicationCommand(
        application_id=application_id,
        applicant_id=applicant_id,
        requested_amount_usd=requested_amount_usd,
        loan_purpose="narrative_test",
        submission_channel="pytest",
        submitted_at=now,
        correlation_id=application_id,
        assigned_agent_id="narr-test",
        analysis_requested_at=now,
    )
    await handlers.handle_submit_application(store, cmd)


async def _gas(
    store: EventStore,
    *,
    agent_id: str,
    session_id: str,
    correlation_id: str,
) -> None:
    await handlers.handle_start_agent_session(
        store,
        agent_id=agent_id,
        session_id=session_id,
        model_version="narr-mv1",
        correlation_id=correlation_id,
        context_source="narratives_test",
    )


@pytest_asyncio.fixture
async def store(pool: asyncpg.Pool) -> EventStore:
    return EventStore(pool)


async def test_narr01_occ_collision(store: EventStore, pool: asyncpg.Pool, narr_companies_seeded: None):
    uid = uuid4().hex[:8]
    application_id = f"NARR-01-{uid}"
    await _submit(store, application_id=application_id, applicant_id="COMP-031")

    doc_sess = f"doc-{uid}"
    await _gas(store, agent_id="document", session_id=doc_sess, correlation_id=application_id)
    doc = DocumentProcessingAgent(
        store,
        pool,
        agent_id="document",
        session_id=doc_sess,
        correlation_id=application_id,
        application_correlation_id=application_id,
    )
    await doc.run_pipeline(
        application_id,
        "COMP-031",
        "Revenue and net income narrative for Meridian Industrial; no material gaps noted.",
    )

    sess_a = f"sess-a-{uid}"
    sess_b = f"sess-b-{uid}"
    await _gas(store, agent_id="credit", session_id=sess_a, correlation_id=application_id)
    await _gas(store, agent_id="credit", session_id=sess_b, correlation_id=application_id)

    agent_a = CreditAnalysisAgent(
        store,
        pool,
        agent_id="credit",
        session_id=sess_a,
        correlation_id=application_id,
        application_correlation_id=application_id,
    )
    agent_b = CreditAnalysisAgent(
        store,
        pool,
        agent_id="credit",
        session_id=sess_b,
        correlation_id=application_id,
        application_correlation_id=application_id,
    )

    results = await asyncio.gather(
        agent_a.run(application_id),
        agent_b.run(application_id),
        return_exceptions=True,
    )
    assert all(not isinstance(r, Exception) for r in results)

    credit_events = await store.load_stream(f"credit-{application_id}")
    completed = [e for e in credit_events if e.event_type == "CreditAnalysisCompleted"]
    assert len(completed) == 2, "credit stream must have exactly 2 CreditAnalysisCompleted events"
    assert completed[0].stream_position < completed[1].stream_position
    assert not any(isinstance(r, Exception) for r in results)


async def test_narr02_missing_ebitda(store: EventStore, pool: asyncpg.Pool, narr_companies_seeded: None):
    uid = uuid4().hex[:8]
    application_id = f"NARR-02-{uid}"
    await _submit(store, application_id=application_id, applicant_id="COMP-044")

    doc_sess = f"doc-{uid}"
    await _gas(store, agent_id="document", session_id=doc_sess, correlation_id=application_id)
    doc = DocumentProcessingAgent(
        store,
        pool,
        agent_id="document",
        session_id=doc_sess,
        correlation_id=application_id,
        application_correlation_id=application_id,
    )
    document_text_no_ebitda = """
    Cascade Health Partners — management discussion.
    Revenue for the period was $5,200,000.
    Net income was $420,000.
    Operating margin improved year over year.
    """
    await doc.run_pipeline(application_id, "COMP-044", document_text_no_ebitda)

    docpkg_events = await store.load_stream(f"docpkg-{application_id}")
    extraction = next(e for e in docpkg_events if e.event_type == "ExtractionCompleted")
    assert extraction.payload["facts"].get("ebitda") is None, "ebitda must be None for missing_ebitda variant"

    quality = next(e for e in docpkg_events if e.event_type == "QualityAssessmentCompleted")
    assert "ebitda" in quality.payload.get("critical_missing_fields", []), (
        "ebitda must appear in critical_missing_fields"
    )

    credit_sess = f"sess-{uid}"
    await _gas(store, agent_id="credit", session_id=credit_sess, correlation_id=application_id)
    credit_agent = CreditAnalysisAgent(
        store,
        pool,
        agent_id="credit",
        session_id=credit_sess,
        correlation_id=application_id,
        application_correlation_id=application_id,
    )
    await credit_agent.run(application_id)

    credit_events = await store.load_stream(f"credit-{application_id}")
    completed = next(e for e in credit_events if e.event_type == "CreditAnalysisCompleted")
    assert completed.payload["confidence_score"] <= 0.75, "confidence must be capped at 0.75"
    assert len(completed.payload.get("data_quality_caveats", [])) > 0, (
        "data_quality_caveats must be non-empty"
    )


async def test_narr03_agent_crash_recovery(store: EventStore, pool: asyncpg.Pool, narr_companies_seeded: None):
    uid = uuid4().hex[:8]
    application_id = f"NARR-03-{uid}"
    await _submit(store, application_id=application_id, applicant_id="COMP-057")

    doc_sess = f"doc-{uid}"
    await _gas(store, agent_id="document", session_id=doc_sess, correlation_id=application_id)
    doc = DocumentProcessingAgent(
        store,
        pool,
        agent_id="document",
        session_id=doc_sess,
        correlation_id=application_id,
        application_correlation_id=application_id,
    )
    await doc.run_pipeline(application_id, "COMP-057", "Technology sector revenue and income summary.")

    crashed_session_id = f"fraud-sess-crash-{uid}"
    await _gas(store, agent_id="fraud-detection", session_id=crashed_session_id, correlation_id=application_id)
    agent1 = FraudDetectionAgent(
        store,
        pool,
        agent_id="fraud-detection",
        session_id=crashed_session_id,
        correlation_id=application_id,
        application_correlation_id=application_id,
    )

    original_method = FraudDetectionAgent.run_pipeline

    async def crash_after_load_signals(self, app_id, company_id, extracted_facts):
        row = await self._fetch_company(company_id)
        await self._record_tool_call(
            "applicant_registry.companies.fetch_for_fraud",
            json.dumps({"company_id": company_id, "found": row is not None}),
        )
        await self._record_node_execution(
            node_name="fraud.load_signals",
            llm_tokens_input=0,
            llm_tokens_output=0,
            llm_cost_usd=0.0,
            detail="simulated_crash_after_load_signals",
        )
        await self._append_single(
            AgentSessionFailedEvent(
                payload=AgentSessionFailedPayload(
                    agent_id=self.agent_id,
                    session_id=self.session_id,
                    failed_at_node="fraud.load_signals",
                    recoverable=True,
                    error_message="Simulated crash for NARR-03",
                )
            )
        )
        raise RuntimeError("Simulated crash after load_signals")

    FraudDetectionAgent.run_pipeline = crash_after_load_signals  # type: ignore[method-assign]
    try:
        with pytest.raises(RuntimeError, match="Simulated crash"):
            await agent1.run_pipeline(application_id, "COMP-057", {})
    finally:
        FraudDetectionAgent.run_pipeline = original_method  # type: ignore[method-assign]

    recovered_session_id = f"fraud-sess-recovered-{uid}"
    await _gas(store, agent_id="fraud-detection", session_id=recovered_session_id, correlation_id=application_id)
    agent2 = FraudDetectionAgent(
        store,
        pool,
        agent_id="fraud-detection",
        session_id=recovered_session_id,
        correlation_id=application_id,
        application_correlation_id=application_id,
        replay_context_label=f"prior_session_replay:{crashed_session_id}",
    )
    await agent2.run_pipeline(application_id, "COMP-057", {})

    fraud_events = await store.load_stream(f"fraud-{application_id}")
    completed = [e for e in fraud_events if e.event_type == "FraudScreeningCompleted"]
    assert len(completed) == 1, "exactly one FraudScreeningCompleted on fraud stream (no duplicate from crash)"

    crashed_stream = await store.load_stream(f"agent-fraud-detection-{crashed_session_id}")
    failed_events = [e for e in crashed_stream if e.event_type == "AgentSessionFailed"]
    assert len(failed_events) == 1
    assert failed_events[0].payload["recoverable"] is True

    recovered_stream = await store.load_stream(f"agent-fraud-detection-{recovered_session_id}")
    started = next(e for e in recovered_stream if e.event_type == "AgentSessionStarted")
    assert started.payload["context_source"].startswith(f"prior_session_replay:{crashed_session_id}")

    load_signals_executions = [
        e
        for e in crashed_stream + recovered_stream
        if e.event_type == "AgentNodeExecuted" and e.payload.get("node_id") == "fraud.load_signals"
    ]
    assert len(load_signals_executions) == 1, "load_signals must not be duplicated in recovery"


async def test_narr04_compliance_hard_block_montana(
    store: EventStore, pool: asyncpg.Pool, narr_companies_seeded: None
):
    uid = uuid4().hex[:8]
    application_id = f"NARR-04-{uid}"
    await _submit(store, application_id=application_id, applicant_id="COMP-MT1")

    doc_sess = f"doc-{uid}"
    await _gas(store, agent_id="document", session_id=doc_sess, correlation_id=application_id)
    doc = DocumentProcessingAgent(
        store,
        pool,
        agent_id="document",
        session_id=doc_sess,
        correlation_id=application_id,
        application_correlation_id=application_id,
    )
    await doc.run_pipeline(application_id, "COMP-MT1", "Agricultural revenue summary; Montana operations.")

    fraud_sess = f"fraud-{uid}"
    await _gas(store, agent_id="fraud-detection", session_id=fraud_sess, correlation_id=application_id)
    fraud = FraudDetectionAgent(
        store,
        pool,
        agent_id="fraud-detection",
        session_id=fraud_sess,
        correlation_id=application_id,
        application_correlation_id=application_id,
    )
    await fraud.run_pipeline(application_id, "COMP-MT1", {})

    credit_sess = f"credit-{uid}"
    await _gas(store, agent_id="credit", session_id=credit_sess, correlation_id=application_id)
    credit = CreditAnalysisAgent(
        store,
        pool,
        agent_id="credit",
        session_id=credit_sess,
        correlation_id=application_id,
        application_correlation_id=application_id,
    )
    await credit.run(application_id)

    comp_sess = f"comp-{uid}"
    await _gas(store, agent_id="compliance", session_id=comp_sess, correlation_id=application_id)
    compliance_agent = ComplianceAgent(
        store,
        pool,
        agent_id="compliance",
        session_id=comp_sess,
        correlation_id=application_id,
        application_correlation_id=application_id,
    )
    await compliance_agent.run(application_id, requested_amount_usd=500_000.0)

    compliance_events = await store.load_stream(f"compliance-{application_id}")
    rule_events = [
        e
        for e in compliance_events
        if e.event_type in ("ComplianceRulePassed", "ComplianceRuleFailed", "ComplianceRuleNoted")
    ]
    assert len(rule_events) == 3, (
        f"Expected 3 rule events (REG-001, REG-002, REG-003); got {len(rule_events)}"
    )
    assert rule_events[0].event_type == "ComplianceRulePassed"
    assert rule_events[0].payload["rule_id"] == "REG-001"
    assert rule_events[1].event_type == "ComplianceRulePassed"
    assert rule_events[1].payload["rule_id"] == "REG-002"
    assert rule_events[2].event_type == "ComplianceRuleFailed"
    assert rule_events[2].payload["rule_id"] == "REG-003"
    assert rule_events[2].payload.get("is_hard_block") is True

    completed = next(e for e in compliance_events if e.event_type == "ComplianceCheckCompleted")
    assert completed.payload["overall_verdict"] == "BLOCKED"

    loan_events = await store.load_stream(f"loan-{application_id}")
    decision_events = [e for e in loan_events if e.event_type == "DecisionGenerated"]
    assert len(decision_events) == 0, "DecisionGenerated must never appear after compliance BLOCK"

    declined = next(e for e in loan_events if e.event_type == "ApplicationDeclined")
    assert declined.payload["adverse_action_notice_required"] is True
    assert any("REG-003" in str(r) for r in declined.payload["decline_reasons"])


async def test_narr05_human_override(store: EventStore, pool: asyncpg.Pool, narr_companies_seeded: None):
    uid = uuid4().hex[:8]
    application_id = f"NARR-05-{uid}"
    await _submit(store, application_id=application_id, applicant_id="COMP-068", requested_amount_usd=800_000.0)

    doc_sess = f"doc-{uid}"
    await _gas(store, agent_id="document", session_id=doc_sess, correlation_id=application_id)
    doc = DocumentProcessingAgent(
        store,
        pool,
        agent_id="document",
        session_id=doc_sess,
        correlation_id=application_id,
        application_correlation_id=application_id,
    )
    await doc.run_pipeline(
        application_id,
        "COMP-068",
        "Retail segment revenue trend; declining top line year over year.",
    )

    fraud_sess = f"fraud-{uid}"
    await _gas(store, agent_id="fraud-detection", session_id=fraud_sess, correlation_id=application_id)
    fraud = FraudDetectionAgent(
        store,
        pool,
        agent_id="fraud-detection",
        session_id=fraud_sess,
        correlation_id=application_id,
        application_correlation_id=application_id,
    )
    fraud_result = await fraud.run_pipeline(application_id, "COMP-068", {})

    credit_sess = f"credit-{uid}"
    await _gas(store, agent_id="credit", session_id=credit_sess, correlation_id=application_id)
    credit = CreditAnalysisAgent(
        store,
        pool,
        agent_id="credit",
        session_id=credit_sess,
        correlation_id=application_id,
        application_correlation_id=application_id,
    )
    credit_result = await credit.run(application_id)

    comp_sess = f"comp-{uid}"
    await _gas(store, agent_id="compliance", session_id=comp_sess, correlation_id=application_id)
    compliance_agent = ComplianceAgent(
        store,
        pool,
        agent_id="compliance",
        session_id=comp_sess,
        correlation_id=application_id,
        application_correlation_id=application_id,
    )
    await compliance_agent.run(application_id, requested_amount_usd=800_000.0)

    orch_sess = f"orch-{uid}"
    await _gas(store, agent_id="orchestrator", session_id=orch_sess, correlation_id=application_id)
    orchestrator = DecisionOrchestratorAgent(
        store,
        pool,
        agent_id="orchestrator",
        session_id=orch_sess,
        correlation_id=application_id,
        application_correlation_id=application_id,
    )
    await orchestrator.run_pipeline(
        application_id,
        credit_memo=credit_result["memo"],
        fraud_analysis=fraud_result["fraud_analysis"],
    )

    last_loan = await _last_event_id(store, f"loan-{application_id}")
    await handlers.handle_generate_decision(
        store,
        application_id=application_id,
        orchestrator_agent_id="orchestrator",
        recommendation="DECLINE",
        confidence_score=0.78,
        contributing_agent_sessions=[f"credit-{credit_sess}", f"fraud-detection-{fraud_sess}"],
        decision_basis_summary="Automated decline for narrative test (declining retail profile).",
        correlation_id=application_id,
        causation_id=last_loan,
        model_versions={"credit": "narr-mv1", "fraud": "narr-mv1"},
    )

    last_loan = await _last_event_id(store, f"loan-{application_id}")
    await handlers.handle_record_human_review(
        store,
        application_id=application_id,
        reviewer_id="LO-Sarah-Chen",
        override=True,
        final_decision="APPROVE",
        correlation_id=application_id,
        override_reason="15-year customer, prior repayment history, collateral offered",
        emit_application_approved=True,
        approved_amount_usd=750_000.0,
        conditions=["Monthly revenue reporting for 12 months", "Personal guarantee from CEO"],
        causation_id=last_loan,
    )

    loan_events = await store.load_stream(f"loan-{application_id}")
    decision = next(e for e in loan_events if e.event_type == "DecisionGenerated")
    assert decision.payload["recommendation"] == "DECLINE"

    review = next(e for e in loan_events if e.event_type == "HumanReviewCompleted")
    assert review.payload["override"] is True
    assert review.payload["reviewer_id"] == "LO-Sarah-Chen"

    approved = next(e for e in loan_events if e.event_type == "ApplicationApproved")
    assert approved.payload["approved_amount_usd"] == 750_000
    assert len(approved.payload["conditions"]) == 2
