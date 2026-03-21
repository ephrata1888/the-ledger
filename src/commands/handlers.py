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
from src.models.events import (
    AgentContextLoadedEvent,
    AgentContextLoadedPayload,
    ApplicationSubmittedEvent,
    ApplicationSubmittedPayload,
    ComplianceReviewStartedEvent,
    ComplianceReviewStartedPayload,
    CreditAnalysisCompletedEvent,
    CreditAnalysisCompletedPayload,
    CreditAnalysisRequestedEvent,
    CreditAnalysisRequestedPayload,
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
