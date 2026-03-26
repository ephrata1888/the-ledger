"""
MCP Tools — command side (write path).
All tools call Phase 2 command handlers and return structured error dicts on failure.
Register by calling register_tools(mcp, rt).
"""

from __future__ import annotations

from typing import Any

from fastmcp import FastMCP

from src.commands import handlers
from src.integrity.audit_chain import run_integrity_check
from src.mcp.errors import serialize_exception
from src.mcp.runtime import LedgerRuntime
from src.models.commands import CreditAnalysisCompletedCommand, SubmitApplicationCommand
from src.models.events import DomainError, OptimisticConcurrencyError


def _ok(**parts: Any) -> dict[str, Any]:
    return {"status": "ok", **parts}


def _err(exc: BaseException) -> dict[str, Any]:
    return {"status": "error", **serialize_exception(exc)}


def register_tools(mcp: FastMCP, rt: LedgerRuntime) -> None:
    """Register all 8 MCP tools onto the given FastMCP instance."""

    @mcp.tool(
        name="submit_application",
        description=(
            "Submit a new loan application and enqueue CreditAnalysisRequested on the loan stream.\n\n"
            "**Preconditions:** ``application_id`` must be new (empty loan stream). No agent session required."
        ),
    )
    async def submit_application(
        application_id: str,
        applicant_id: str,
        requested_amount_usd: float,
        loan_purpose: str,
        submission_channel: str,
        submitted_at: str,
        correlation_id: str,
        assigned_agent_id: str = "unassigned",
        analysis_requested_at: str = "",
        priority: str = "normal",
    ) -> dict[str, Any]:
        try:
            cmd = SubmitApplicationCommand(
                application_id=application_id,
                applicant_id=applicant_id,
                requested_amount_usd=requested_amount_usd,
                loan_purpose=loan_purpose,
                submission_channel=submission_channel,
                submitted_at=submitted_at,
                correlation_id=correlation_id,
                assigned_agent_id=assigned_agent_id,
                analysis_requested_at=analysis_requested_at or submitted_at,
                priority=priority,
            )
            v = await handlers.handle_submit_application(rt.store, cmd)
            await rt.catch_up_projections()
            return _ok(loan_stream_version=v)
        except (DomainError, OptimisticConcurrencyError) as e:
            return _err(e)

    @mcp.tool(
        name="record_credit_analysis",
        description=(
            "Append CreditAnalysisCompleted to agent and loan streams; may emit ComplianceReviewStarted.\n\n"
            "**Preconditions:** Application must be AWAITING_ANALYSIS on the loan stream. "
            "If the agent session stream is non-empty, the first event must be AgentContextLoaded (Gas Town) — "
            "otherwise call start_agent_session first. Model version must match AgentContextLoaded when context exists."
        ),
    )
    async def record_credit_analysis(
        application_id: str,
        agent_id: str,
        session_id: str,
        model_version: str,
        confidence_score: float,
        risk_tier: str,
        recommended_limit_usd: float,
        analysis_duration_ms: int,
        input_data_hash: str,
        correlation_id: str,
        causation_id: str | None = None,
        context_source: str | None = None,
        context_token_count: int | None = None,
        event_replay_from_position: int = 0,
    ) -> dict[str, Any]:
        try:
            cmd = CreditAnalysisCompletedCommand(
                application_id=application_id,
                agent_id=agent_id,
                session_id=session_id,
                model_version=model_version,
                confidence_score=confidence_score,
                risk_tier=risk_tier,
                recommended_limit_usd=recommended_limit_usd,
                analysis_duration_ms=analysis_duration_ms,
                input_data_hash=input_data_hash,
                correlation_id=correlation_id,
                causation_id=causation_id,
                context_source=context_source,
                context_token_count=context_token_count,
                event_replay_from_position=event_replay_from_position,
            )
            av, lv = await handlers.handle_credit_analysis_completed(rt.store, cmd)
            await rt.catch_up_projections()
            return _ok(agent_stream_version=av, loan_stream_version=lv)
        except (DomainError, OptimisticConcurrencyError) as e:
            return _err(e)

    @mcp.tool(
        name="record_fraud_screening",
        description=(
            "Append FraudScreeningCompleted to the loan stream; optionally mirror to an agent session.\n\n"
            "**Preconditions:** Loan must be AWAITING_ANALYSIS. "
            "If ``agent_id`` and ``session_id`` are provided for mirroring, "
            "start_agent_session must have created that agent stream first (Gas Town)."
        ),
    )
    async def record_fraud_screening(
        application_id: str,
        fraud_score: float,
        correlation_id: str,
        causation_id: str | None = None,
        agent_id: str | None = None,
        session_id: str | None = None,
        agent_stream_fraud_score_mirror: bool = True,
    ) -> dict[str, Any]:
        try:
            versions = await handlers.handle_record_fraud_screening(
                rt.store,
                application_id=application_id,
                fraud_score=fraud_score,
                correlation_id=correlation_id,
                causation_id=causation_id,
                agent_id=agent_id,
                session_id=session_id,
                agent_stream_fraud_score_mirror=agent_stream_fraud_score_mirror,
            )
            await rt.catch_up_projections()
            return _ok(**versions)
        except (DomainError, OptimisticConcurrencyError) as e:
            return _err(e)

    @mcp.tool(
        name="record_compliance_check",
        description=(
            "Append ComplianceCheckRequested (and optional ComplianceRulePassed batch) to compliance-{application_id}.\n\n"
            "**Preconditions:** None on the compliance stream beyond normal OCC; "
            "for downstream ApplicationApproved (Rule 5), ensure ``checks_required`` rule ids "
            "each receive a matching ComplianceRulePassed (use ``initial_rule_passes``)."
        ),
    )
    async def record_compliance_check(
        application_id: str,
        regulation_set_version: str,
        checks_required: list[str],
        correlation_id: str,
        causation_id: str | None = None,
        initial_rule_passes: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        try:
            v = await handlers.handle_record_compliance_check(
                rt.store,
                application_id=application_id,
                regulation_set_version=regulation_set_version,
                checks_required=checks_required,
                correlation_id=correlation_id,
                causation_id=causation_id,
                initial_rule_passes=initial_rule_passes,
            )
            await rt.catch_up_projections()
            return _ok(compliance_stream_version=v)
        except (DomainError, OptimisticConcurrencyError) as e:
            return _err(e)

    @mcp.tool(
        name="generate_decision",
        description=(
            "Append DecisionGenerated to the loan stream (Rule 4 confidence floor + Rule 6 causal validation).\n\n"
            "**Preconditions:** Loan must be in COMPLIANCE_REVIEW. "
            "Each entry in ``contributing_agent_sessions`` must be the session key ``{agent_id}-{session_id}`` "
            "for a stream that already contains CreditAnalysisCompleted or FraudScreeningCompleted for this "
            "application (mirror fraud to the agent stream when using fraud-only causality). "
            "Recommendations normalize to APPROVE / DECLINE / REFER."
        ),
    )
    async def generate_decision(
        application_id: str,
        orchestrator_agent_id: str,
        recommendation: str,
        confidence_score: float,
        contributing_agent_sessions: list[str],
        decision_basis_summary: str,
        correlation_id: str,
        causation_id: str | None = None,
        model_versions: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        try:
            v = await handlers.handle_generate_decision(
                rt.store,
                application_id=application_id,
                orchestrator_agent_id=orchestrator_agent_id,
                recommendation=recommendation,
                confidence_score=confidence_score,
                contributing_agent_sessions=contributing_agent_sessions,
                decision_basis_summary=decision_basis_summary,
                correlation_id=correlation_id,
                causation_id=causation_id,
                model_versions=model_versions,
            )
            await rt.catch_up_projections()
            return _ok(loan_stream_version=v)
        except (DomainError, OptimisticConcurrencyError) as e:
            return _err(e)

    @mcp.tool(
        name="record_human_review",
        description=(
            "Append HumanReviewCompleted; optionally append ApplicationApproved in the same command surface.\n\n"
            "**Preconditions:** Loan must be PENDING_DECISION after generate_decision. "
            "If ``emit_application_approved`` is true, prior HumanReviewCompleted path must be APPROVE/APPROVED "
            "and Rule 5 requires all ``checks_required`` from compliance to have passed."
        ),
    )
    async def record_human_review(
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
    ) -> dict[str, Any]:
        try:
            v = await handlers.handle_record_human_review(
                rt.store,
                application_id=application_id,
                reviewer_id=reviewer_id,
                override=override,
                final_decision=final_decision,
                correlation_id=correlation_id,
                causation_id=causation_id,
                override_reason=override_reason,
                emit_application_approved=emit_application_approved,
                approved_amount_usd=approved_amount_usd,
                conditions=conditions,
            )
            await rt.catch_up_projections()
            return _ok(loan_stream_version=v)
        except (DomainError, OptimisticConcurrencyError) as e:
            return _err(e)

    @mcp.tool(
        name="start_agent_session",
        description=(
            "Gas Town anchor: persist AgentContextLoaded as the first event on agent-{agent_id}-{session_id}.\n\n"
            "**Preconditions:** The agent session stream must be empty (no prior events)."
        ),
    )
    async def start_agent_session(
        agent_id: str,
        session_id: str,
        model_version: str,
        correlation_id: str,
        context_source: str = "mcp",
        event_replay_from_position: int = 0,
        context_token_count: int = 0,
        causation_id: str | None = None,
    ) -> dict[str, Any]:
        try:
            v = await handlers.handle_start_agent_session(
                rt.store,
                agent_id=agent_id,
                session_id=session_id,
                model_version=model_version,
                correlation_id=correlation_id,
                context_source=context_source,
                event_replay_from_position=event_replay_from_position,
                context_token_count=context_token_count,
                causation_id=causation_id,
            )
            await rt.catch_up_projections()
            return _ok(agent_stream_version=v)
        except (DomainError, OptimisticConcurrencyError) as e:
            return _err(e)

    @mcp.tool(
        name="run_integrity_check",
        description=(
            "Run cryptographic hash-chain integrity over a stream and append AuditIntegrityCheckRun.\n\n"
            "PRECONDITIONS: ``entity_type`` is the stream prefix, e.g. ``loan`` or ``compliance``. "
            "``entity_id`` is the application or entity ID, e.g. ``APEX-001``. "
            "Together they identify the audit stream ``audit-{entity_type}-{entity_id}``."
        ),
    )
    async def run_integrity_check_tool(
        entity_type: str,
        entity_id: str,
        correlation_id: str,
        causation_id: str | None = None,
    ) -> dict[str, Any]:
        try:
            res = await run_integrity_check(
                rt.store,
                entity_type,
                entity_id,
                correlation_id=correlation_id,
                causation_id=causation_id,
            )
            await rt.catch_up_projections()
            return _ok(
                tamper_detected=res.tamper_detected,
                integrity_hash=res.integrity_hash,
                previous_hash=res.previous_hash,
                events_verified_count=res.events_verified_count,
            )
        except (DomainError, OptimisticConcurrencyError) as e:
            return _err(e)
