"""
Command handlers: Load → Validate → Determine → Append.
Never store PII in the payload without encryption.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from src.aggregates.agent_session import AgentSessionAggregate
from src.aggregates.loan_application import ApplicationState, LoanApplicationAggregate
from src.event_store import EventStore
from src.models.commands import CreditAnalysisCompletedCommand, SubmitApplicationCommand
from src.aggregates.compliance_record import ComplianceRecordAggregate
from src.models.events import (
    AgentContextLoadedEvent,
    AgentContextLoadedPayload,
    ApplicationApprovedEvent,
    ApplicationApprovedPayload,
    ApplicationSubmittedEvent,
    ApplicationSubmittedPayload,
    DomainError,
    ComplianceCheckRequestedEvent,
    ComplianceCheckRequestedPayload,
    ComplianceReviewStartedEvent,
    ComplianceReviewStartedPayload,
    ComplianceRulePassedEvent,
    ComplianceRulePassedPayload,
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
    HumanReviewRequestedEvent,
    HumanReviewRequestedPayload,
)


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _last_event_id(store: EventStore, stream_id: str) -> Optional[str]:
    events = await store.load_stream(stream_id)
    if not events:
        return None
    return str(events[-1].event_id)


async def handle_submit_application(store: EventStore, cmd: SubmitApplicationCommand) -> int:
    loan = await LoanApplicationAggregate.load(store, cmd.application_id)
    loan.assert_may_submit_new_application()

    stream = f"loan-{cmd.application_id}"
    submitted = ApplicationSubmittedEvent(
        payload=ApplicationSubmittedPayload(
            application_id=cmd.application_id,
            applicant_id=cmd.applicant_id,
            requested_amount_usd=cmd.requested_amount_usd,
            loan_purpose=cmd.loan_purpose,
            submission_channel=cmd.submission_channel,
            submitted_at=cmd.submitted_at,
        )
    )
    requested_at = cmd.analysis_requested_at or cmd.submitted_at
    analysis_requested = CreditAnalysisRequestedEvent(
        payload=CreditAnalysisRequestedPayload(
            application_id=cmd.application_id,
            assigned_agent_id=cmd.assigned_agent_id,
            requested_at=requested_at,
            priority=cmd.priority,
        )
    )

    expected_first = -1 if loan.version == 0 else loan.version
    await store.append(
        stream_id=stream,
        events=[submitted],
        expected_version=expected_first,
        correlation_id=cmd.correlation_id,
        causation_id=None,
    )
    root_id = await _last_event_id(store, stream)

    loan = await LoanApplicationAggregate.load(store, cmd.application_id)
    return await store.append(
        stream_id=stream,
        events=[analysis_requested],
        expected_version=loan.version,
        correlation_id=cmd.correlation_id,
        causation_id=root_id,
    )


async def handle_credit_analysis_completed(
    store: EventStore,
    cmd: CreditAnalysisCompletedCommand,
) -> tuple[int, int]:
    """
    Appends CreditAnalysisCompleted to the AgentSession stream and mirrors it on the Loan stream.
    When the loan reaches ANALYSIS_COMPLETE, appends ComplianceReviewStarted (bridge to COMPLIANCE_REVIEW).
    Returns (new_agent_stream_version, new_loan_stream_version).
    """
    loan_stream = f"loan-{cmd.application_id}"
    agent_stream = f"agent-{cmd.agent_id}-{cmd.session_id}"

    loan = await LoanApplicationAggregate.load(store, cmd.application_id)
    session_events_before = await store.load_stream(agent_stream)
    loan_events = await store.load_stream(loan_stream)

    agent = await AgentSessionAggregate.load(store, cmd.agent_id, cmd.session_id)

    loan.assert_ready_for_credit_completion_on_loan()

    if agent.version != 0:
        agent.assert_context_loaded_for_decision()

    agent.validate_credit_analysis_not_locked(
        cmd.application_id,
        loan_events=loan_events,
        session_events=session_events_before,
    )

    correlation = cmd.correlation_id
    causation: Optional[str] = cmd.causation_id

    if agent.version == 0:
        agent.assert_first_event_is_context("AgentContextLoaded")
        ctx = AgentContextLoadedEvent(
            payload=AgentContextLoadedPayload(
                agent_id=cmd.agent_id,
                session_id=cmd.session_id,
                context_source=cmd.context_source or "command_handler",
                event_replay_from_position=cmd.event_replay_from_position,
                context_token_count=cmd.context_token_count or 0,
                model_version=cmd.model_version,
            )
        )
        await store.append(
            stream_id=agent_stream,
            events=[ctx],
            expected_version=-1 if agent.version == 0 else agent.version,
            correlation_id=correlation,
            causation_id=causation,
        )
        causation = await _last_event_id(store, agent_stream)
        agent = await AgentSessionAggregate.load(store, cmd.agent_id, cmd.session_id)

    agent.assert_model_version_match(cmd.model_version)

    if causation is None:
        causation = await _last_event_id(store, agent_stream)

    credit_payload = CreditAnalysisCompletedPayload(
        application_id=cmd.application_id,
        agent_id=cmd.agent_id,
        session_id=cmd.session_id,
        model_version=cmd.model_version,
        confidence_score=cmd.confidence_score,
        risk_tier=cmd.risk_tier,
        recommended_limit_usd=cmd.recommended_limit_usd,
        analysis_duration_ms=cmd.analysis_duration_ms,
        input_data_hash=cmd.input_data_hash,
        data_quality_caveats=list(cmd.data_quality_caveats or []),
    )
    credit_agent = CreditAnalysisCompletedEvent(payload=credit_payload)

    await store.append(
        stream_id=agent_stream,
        events=[credit_agent],
        expected_version=agent.version,
        correlation_id=correlation,
        causation_id=causation,
    )
    agent_credit_event_id = await _last_event_id(store, agent_stream)

    loan = await LoanApplicationAggregate.load(store, cmd.application_id)
    credit_loan = CreditAnalysisCompletedEvent(payload=credit_payload)

    await store.append(
        stream_id=loan_stream,
        events=[credit_loan],
        expected_version=loan.version,
        correlation_id=correlation,
        causation_id=agent_credit_event_id,
    )

    loan = await LoanApplicationAggregate.load(store, cmd.application_id)
    loan_v = loan.version

    if loan.state == ApplicationState.ANALYSIS_COMPLETE:
        crs_id = await _last_event_id(store, loan_stream)
        bridge = ComplianceReviewStartedEvent(
            payload=ComplianceReviewStartedPayload(
                application_id=cmd.application_id,
                started_at=_utc_iso(),
                regulation_set_hint="",
            )
        )
        loan_v = await store.append(
            stream_id=loan_stream,
            events=[bridge],
            expected_version=loan.version,
            correlation_id=correlation,
            causation_id=crs_id,
        )

    agent = await AgentSessionAggregate.load(store, cmd.agent_id, cmd.session_id)
    return agent.version, loan_v


async def handle_start_agent_session(
    store: EventStore,
    *,
    agent_id: str,
    session_id: str,
    model_version: str,
    correlation_id: str,
    context_source: str = "mcp",
    event_replay_from_position: int = 0,
    context_token_count: int = 0,
    causation_id: str | None = None,
) -> int:
    """
    Gas Town anchor: persist AgentContextLoaded as the first event on agent-{agent_id}-{session_id}.
    """
    agent_stream = f"agent-{agent_id}-{session_id}"
    agent = await AgentSessionAggregate.load(store, agent_id, session_id)
    agent.assert_first_event_is_context("AgentContextLoaded")
    if agent.version != 0:
        raise DomainError(
            "AGENT_SESSION_ALREADY_STARTED",
            "Agent session stream is not empty; Gas Town anchor already exists.",
            {"stream_id": agent_stream, "version": agent.version},
        )
    ctx = AgentContextLoadedEvent(
        payload=AgentContextLoadedPayload(
            agent_id=agent_id,
            session_id=session_id,
            context_source=context_source,
            event_replay_from_position=event_replay_from_position,
            context_token_count=context_token_count,
            model_version=model_version,
        )
    )
    return await store.append(
        stream_id=agent_stream,
        events=[ctx],
        expected_version=-1,
        correlation_id=correlation_id,
        causation_id=causation_id,
    )


async def handle_record_fraud_screening(
    store: EventStore,
    *,
    application_id: str,
    fraud_score: float,
    correlation_id: str,
    causation_id: str | None = None,
    agent_id: str | None = None,
    session_id: str | None = None,
    agent_stream_fraud_score_mirror: bool = True,
) -> dict[str, int]:
    """
    Append FraudScreeningCompleted to loan-{application_id}.
    When agent_id+session_id are provided, mirror the same fact to agent-{agent_id}-{session_id}
    (required for Rule 6 / contributing sessions on decisions).
    """
    loan_stream = f"loan-{application_id}"
    loan = await LoanApplicationAggregate.load(store, application_id)
    loan._require_states({ApplicationState.AWAITING_ANALYSIS}, "FraudScreeningCompleted")  # type: ignore[attr-defined]

    payload = FraudScreeningCompletedPayload(
        application_id=application_id,
        agent_id=agent_id or "",
        fraud_score=fraud_score,
    )
    ev = FraudScreeningCompletedEvent(payload=payload)
    await store.append(
        stream_id=loan_stream,
        events=[ev],
        expected_version=loan.version,
        correlation_id=correlation_id,
        causation_id=causation_id,
    )
    loan_event_id = await _last_event_id(store, loan_stream)
    fraud_summary_stream = f"fraud-{application_id}"
    fv = await store.stream_version(fraud_summary_stream)
    await store.append(
        stream_id=fraud_summary_stream,
        events=[ev],
        expected_version=-1 if fv == 0 else fv,
        correlation_id=correlation_id,
        causation_id=loan_event_id,
    )
    versions: dict[str, int] = {
        "loan_stream_version": await store.stream_version(loan_stream),
        "fraud_summary_stream_version": await store.stream_version(fraud_summary_stream),
    }

    if agent_id and session_id and agent_stream_fraud_score_mirror:
        agent_stream = f"agent-{agent_id}-{session_id}"
        agent = await AgentSessionAggregate.load(store, agent_id, session_id)
        agent.assert_context_loaded_for_decision()
        await store.append(
            stream_id=agent_stream,
            events=[FraudScreeningCompletedEvent(payload=payload)],
            expected_version=agent.version,
            correlation_id=correlation_id,
            causation_id=causation_id or await _last_event_id(store, agent_stream),
        )
        versions["agent_stream_version"] = await store.stream_version(agent_stream)

    loan = await LoanApplicationAggregate.load(store, application_id)
    if loan.state == ApplicationState.ANALYSIS_COMPLETE:
        loan_events = await store.load_stream(loan_stream)
        if not any(e.event_type == "ComplianceReviewStarted" for e in loan_events):
            crs_id = await _last_event_id(store, loan_stream)
            bridge = ComplianceReviewStartedEvent(
                payload=ComplianceReviewStartedPayload(
                    application_id=application_id,
                    started_at=_utc_iso(),
                    regulation_set_hint="",
                )
            )
            await store.append(
                stream_id=loan_stream,
                events=[bridge],
                expected_version=loan.version,
                correlation_id=correlation_id,
                causation_id=crs_id,
            )
            versions["loan_stream_version"] = await store.stream_version(loan_stream)

    return versions


async def handle_record_compliance_check(
    store: EventStore,
    *,
    application_id: str,
    regulation_set_version: str,
    checks_required: list[str],
    correlation_id: str,
    causation_id: str | None = None,
    initial_rule_passes: list[dict] | None = None,
) -> int:
    """
    Append ComplianceCheckRequested to compliance-{application_id}, optional ComplianceRulePassed batch.
    """
    cstream = f"compliance-{application_id}"
    comp = await ComplianceRecordAggregate.load(store, application_id)
    expected = -1 if comp.version == 0 else comp.version
    req = ComplianceCheckRequestedEvent(
        payload=ComplianceCheckRequestedPayload(
            application_id=application_id,
            regulation_set_version=regulation_set_version,
            checks_required=checks_required,
        )
    )
    await store.append(
        stream_id=cstream,
        events=[req],
        expected_version=expected,
        correlation_id=correlation_id,
        causation_id=causation_id,
    )
    comp = await ComplianceRecordAggregate.load(store, application_id)
    last = comp.version
    if initial_rule_passes:
        for rp in initial_rule_passes:
            rid = str(rp.get("rule_id", ""))
            rv = str(rp.get("rule_version", "1"))
            passed = ComplianceRulePassedEvent(
                payload=ComplianceRulePassedPayload(
                    application_id=application_id,
                    rule_id=rid,
                    rule_version=rv,
                )
            )
            comp = await ComplianceRecordAggregate.load(store, application_id)
            await store.append(
                stream_id=cstream,
                events=[passed],
                expected_version=comp.version,
                correlation_id=correlation_id,
                causation_id=causation_id,
            )
        last = await store.stream_version(cstream)
    return last


def _normalize_decision_recommendation(raw: str) -> str:
    u = raw.upper().strip()
    if u in ("APPROVED", "APPROVE"):
        return "APPROVE"
    if u in ("DECLINED", "DECLINE"):
        return "DECLINE"
    return "REFER"


async def handle_generate_decision(
    store: EventStore,
    *,
    application_id: str,
    orchestrator_agent_id: str,
    recommendation: str,
    confidence_score: float,
    contributing_agent_sessions: list[str],
    decision_basis_summary: str,
    correlation_id: str,
    causation_id: str | None = None,
    model_versions: dict[str, str] | None = None,
) -> int:
    """Append DecisionGenerated to loan stream with Rule 4 floor + Rule 6 causal validation."""
    loan_stream = f"loan-{application_id}"
    loan = await LoanApplicationAggregate.load(store, application_id)
    await LoanApplicationAggregate.validate_decision_causal_chain(
        store, application_id, contributing_agent_sessions
    )
    base = {
        "application_id": application_id,
        "orchestrator_agent_id": orchestrator_agent_id,
        "recommendation": _normalize_decision_recommendation(recommendation),
        "confidence_score": confidence_score,
        "contributing_agent_sessions": contributing_agent_sessions,
        "decision_basis_summary": decision_basis_summary,
        "model_versions": dict(model_versions or {}),
    }
    payload_dict = LoanApplicationAggregate.build_decision_generated_payload(base)
    ev = DecisionGeneratedEvent(
        payload=DecisionGeneratedPayload.model_validate(payload_dict),
    )
    loan = await LoanApplicationAggregate.load(store, application_id)
    if causation_id is None:
        causation_id = await _last_event_id(store, loan_stream)
    return await store.append(
        stream_id=loan_stream,
        events=[ev],
        expected_version=loan.version,
        correlation_id=correlation_id,
        causation_id=causation_id,
    )


async def handle_request_human_review(
    store: EventStore,
    *,
    application_id: str,
    correlation_id: str,
    requested_by: str = "",
    reason: str = "",
    causation_id: str | None = None,
) -> int:
    """Append HumanReviewRequested while loan is PENDING_DECISION (after DecisionGenerated)."""
    loan_stream = f"loan-{application_id}"
    loan = await LoanApplicationAggregate.load(store, application_id)
    ev = HumanReviewRequestedEvent(
        payload=HumanReviewRequestedPayload(
            application_id=application_id,
            requested_by=requested_by,
            reason=reason,
        )
    )
    if causation_id is None:
        causation_id = await _last_event_id(store, loan_stream)
    return await store.append(
        stream_id=loan_stream,
        events=[ev],
        expected_version=loan.version,
        correlation_id=correlation_id,
        causation_id=causation_id,
    )


async def handle_record_human_review(
    store: EventStore,
    *,
    application_id: str,
    reviewer_id: str,
    override: bool,
    final_decision: str,
    correlation_id: str,
    causation_id: str | None = None,
    override_reason: str = "",
    emit_application_approved: bool = False,
    approved_amount_usd: float = 0.0,
    conditions: list[str] | None = None,
) -> int:
    """
    Append HumanReviewCompleted; optionally append ApplicationApproved when emit_application_approved=True
    (same command surface — two appends) after Rule 5 compliance gate.
    """
    loan_stream = f"loan-{application_id}"
    loan = await LoanApplicationAggregate.load(store, application_id)
    human = HumanReviewCompletedEvent(
        payload=HumanReviewCompletedPayload(
            application_id=application_id,
            reviewer_id=reviewer_id,
            override=override,
            final_decision=final_decision,
            override_reason=override_reason,
        )
    )
    await store.append(
        stream_id=loan_stream,
        events=[human],
        expected_version=loan.version,
        correlation_id=correlation_id,
        causation_id=causation_id,
    )
    if not emit_application_approved:
        return await store.stream_version(loan_stream)

    loan = await LoanApplicationAggregate.load(store, application_id)
    compliance = await ComplianceRecordAggregate.load(store, application_id)
    loan.validate_application_approved(compliance)
    approved = ApplicationApprovedEvent(
        payload=ApplicationApprovedPayload(
            application_id=application_id,
            approved_amount_usd=approved_amount_usd,
            approved_by=reviewer_id,
            conditions=list(conditions or []),
        )
    )
    await store.append(
        stream_id=loan_stream,
        events=[approved],
        expected_version=loan.version,
        correlation_id=correlation_id,
        causation_id=await _last_event_id(store, loan_stream),
    )
    return await store.stream_version(loan_stream)
