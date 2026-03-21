"""
Command handlers: Load → Validate → Determine → Append.
Never store PII in the payload without encryption.
"""

from __future__ import annotations

from typing import Optional

from src.aggregates.agent_session import AgentSessionAggregate
from src.aggregates.loan_application import LoanApplicationAggregate
from src.event_store import EventStore
from src.models.commands import CreditAnalysisCompletedCommand, SubmitApplicationCommand
from src.models.events import BaseEvent, DomainError


async def _last_event_id(store: EventStore, stream_id: str) -> Optional[str]:
    events = await store.load_stream(stream_id)
    if not events:
        return None
    return str(events[-1].event_id)


async def handle_submit_application(store: EventStore, cmd: SubmitApplicationCommand) -> int:
    # --- Load ---
    loan = await LoanApplicationAggregate.load(store, cmd.application_id)
    # --- Validate ---
    if loan.version != 0 or loan.state is not None:
        raise DomainError("Application already exists for this application_id.")
    # --- Determine ---
    submitted = BaseEvent(
        event_type="ApplicationSubmitted",
        event_version=1,
        payload={
            "application_id": cmd.application_id,
            "applicant_id": cmd.applicant_id,
            "requested_amount_usd": cmd.requested_amount_usd,
            "loan_purpose": cmd.loan_purpose,
            "submission_channel": cmd.submission_channel,
            "submitted_at": cmd.submitted_at,
        },
    )
    requested_at = cmd.analysis_requested_at or cmd.submitted_at
    analysis_requested = BaseEvent(
        event_type="CreditAnalysisRequested",
        event_version=1,
        payload={
            "application_id": cmd.application_id,
            "assigned_agent_id": cmd.assigned_agent_id,
            "requested_at": requested_at,
            "priority": cmd.priority,
        },
    )
    # --- Append (two steps so causation_id chains ApplicationSubmitted → CreditAnalysisRequested) ---
    stream = f"loan-{cmd.application_id}"
    v1 = await store.append(
        stream_id=stream,
        events=[submitted],
        expected_version=-1,
        correlation_id=cmd.correlation_id,
        causation_id=None,
    )
    root_id = await _last_event_id(store, stream)
    return await store.append(
        stream_id=stream,
        events=[analysis_requested],
        expected_version=v1,
        correlation_id=cmd.correlation_id,
        causation_id=root_id,
    )


async def handle_credit_analysis_completed(
    store: EventStore,
    cmd: CreditAnalysisCompletedCommand,
) -> tuple[int, int]:
    """
    Appends CreditAnalysisCompleted to the AgentSession stream and mirrors it on the Loan stream.
    Returns (new_agent_stream_version, new_loan_stream_version).
    """
    loan_stream = f"loan-{cmd.application_id}"
    agent_stream = f"agent-{cmd.agent_id}-{cmd.session_id}"

    # --- Load ---
    loan = await LoanApplicationAggregate.load(store, cmd.application_id)
    session_events_before = await store.load_stream(agent_stream)
    loan_events = await store.load_stream(loan_stream)

    agent = await AgentSessionAggregate.load(store, cmd.agent_id, cmd.session_id)

    # --- Validate (loan) ---
    loan.assert_ready_for_credit_completion_on_loan()

    # --- Validate (agent / Rules 2 & 3) ---
    if agent.version != 0:
        agent.assert_context_loaded_for_decision()

    agent.validate_credit_analysis_not_locked(
        cmd.application_id,
        loan_events=loan_events,
        session_events=session_events_before,
    )

    # --- Determine + Append (agent stream) ---
    correlation = cmd.correlation_id
    causation: Optional[str] = cmd.causation_id

    if agent.version == 0:
        ctx = BaseEvent(
            event_type="AgentContextLoaded",
            event_version=1,
            payload={
                "agent_id": cmd.agent_id,
                "session_id": cmd.session_id,
                "context_source": cmd.context_source or "command_handler",
                "event_replay_from_position": cmd.event_replay_from_position,
                "context_token_count": cmd.context_token_count or 0,
                "model_version": cmd.model_version,
            },
        )
        v_ctx = await store.append(
            stream_id=agent_stream,
            events=[ctx],
            expected_version=-1,
            correlation_id=correlation,
            causation_id=causation,
        )
        causation = await _last_event_id(store, agent_stream)
    else:
        if causation is None:
            causation = await _last_event_id(store, agent_stream)

    credit_agent = BaseEvent(
        event_type="CreditAnalysisCompleted",
        event_version=2,
        payload={
            "application_id": cmd.application_id,
            "agent_id": cmd.agent_id,
            "session_id": cmd.session_id,
            "model_version": cmd.model_version,
            "confidence_score": cmd.confidence_score,
            "risk_tier": cmd.risk_tier,
            "recommended_limit_usd": cmd.recommended_limit_usd,
            "analysis_duration_ms": cmd.analysis_duration_ms,
            "input_data_hash": cmd.input_data_hash,
        },
    )

    agent_v = await store.append(
        stream_id=agent_stream,
        events=[credit_agent],
        expected_version=await store.stream_version(agent_stream),
        correlation_id=correlation,
        causation_id=causation,
    )
    agent_credit_event_id = await _last_event_id(store, agent_stream)

    # --- Determine + Append (loan stream mirror; matches Phase 1 concurrency test shape) ---
    loan = await LoanApplicationAggregate.load(store, cmd.application_id)
    credit_loan = BaseEvent(
        event_type="CreditAnalysisCompleted",
        event_version=2,
        payload={
            "application_id": cmd.application_id,
            "agent_id": cmd.agent_id,
            "session_id": cmd.session_id,
            "model_version": cmd.model_version,
            "confidence_score": cmd.confidence_score,
            "risk_tier": cmd.risk_tier,
            "recommended_limit_usd": cmd.recommended_limit_usd,
            "analysis_duration_ms": cmd.analysis_duration_ms,
            "input_data_hash": cmd.input_data_hash,
        },
    )
    loan_v = await store.append(
        stream_id=loan_stream,
        events=[credit_loan],
        expected_version=loan.version,
        correlation_id=correlation,
        causation_id=agent_credit_event_id,
    )

    return agent_v, loan_v
