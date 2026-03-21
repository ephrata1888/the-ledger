"""
LoanApplication aggregate (loan-{application_id}).
Never store PII in the payload without encryption.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Set, Tuple

from src.aggregates.compliance_record import ComplianceRecordAggregate
from src.event_store import EventStore
from src.models.events import (
    ApplicationSubmittedPayload,
    DecisionGeneratedPayload,
    DomainError,
    HumanReviewCompletedPayload,
    StoredEvent,
)


class ApplicationState(str, Enum):
    """Seven-state loan lifecycle (strict)."""

    SUBMITTED = "SUBMITTED"
    AWAITING_ANALYSIS = "AWAITING_ANALYSIS"
    ANALYSIS_COMPLETE = "ANALYSIS_COMPLETE"
    COMPLIANCE_REVIEW = "COMPLIANCE_REVIEW"
    PENDING_DECISION = "PENDING_DECISION"
    DECIDED_PENDING_HUMAN = "DECIDED_PENDING_HUMAN"
    FINAL_VERDICT = "FINAL_VERDICT"


# Directed edges (from_state, to_state) allowed by the state machine.
_VALID_TRANSITIONS: Set[Tuple[ApplicationState | None, ApplicationState]] = {
    (None, ApplicationState.SUBMITTED),
    (ApplicationState.SUBMITTED, ApplicationState.AWAITING_ANALYSIS),
    (ApplicationState.AWAITING_ANALYSIS, ApplicationState.ANALYSIS_COMPLETE),
    (ApplicationState.ANALYSIS_COMPLETE, ApplicationState.COMPLIANCE_REVIEW),
    (ApplicationState.COMPLIANCE_REVIEW, ApplicationState.PENDING_DECISION),
    (ApplicationState.PENDING_DECISION, ApplicationState.DECIDED_PENDING_HUMAN),
    (ApplicationState.DECIDED_PENDING_HUMAN, ApplicationState.FINAL_VERDICT),
}


class LoanApplicationAggregate:
    def __init__(self, application_id: str) -> None:
        self.application_id = application_id
        self.version: int = 0
        self.state: ApplicationState | None = None
        self.applicant_id: str | None = None
        self.requested_amount_usd: float | None = None
        self._fraud_screening_done: bool = False
        self._credit_analysis_done: bool = False
        self._last_decision_recommendation: str | None = None
        self._human_line_decision: str | None = None  # "APPROVE" | "DECLINE" after human review

    def _apply(self, event: StoredEvent) -> None:
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
        self.version = event.stream_position

    # --- State machine guards ---

    def assert_can_transition_to(self, new_state: ApplicationState) -> None:
        """Raise DomainError if current -> new_state is not in the allowed graph."""
        if self.state == new_state:
            return
        key = (self.state, new_state)
        if key not in _VALID_TRANSITIONS:
            raise DomainError(
                "INVALID_STATE_TRANSITION",
                f"Cannot transition {self.state!r} -> {new_state!r}",
                {
                    "application_id": self.application_id,
                    "from_state": self.state.value if self.state else None,
                    "to_state": new_state.value,
                },
            )

    def _require_states(self, allowed: Set[ApplicationState], event_type: str) -> None:
        if self.state not in allowed:
            raise DomainError(
                "INVALID_STATE_FOR_EVENT",
                f"Invalid state for {event_type}: current={self.state}, allowed={sorted(s.value for s in allowed)}",
                {
                    "application_id": self.application_id,
                    "event_type": event_type,
                    "current_state": self.state.value if self.state else None,
                    "allowed_states": [s.value for s in allowed],
                },
            )

    def _maybe_enter_analysis_complete(self) -> None:
        if (
            self.state == ApplicationState.AWAITING_ANALYSIS
            and self._fraud_screening_done
            and self._credit_analysis_done
        ):
            self.assert_can_transition_to(ApplicationState.ANALYSIS_COMPLETE)
            self.state = ApplicationState.ANALYSIS_COMPLETE

    # --- event handlers ---

    def _on_ApplicationSubmitted(self, event: StoredEvent) -> None:
        if self.state is not None:
            raise DomainError(
                "LOAN_STREAM_NOT_EMPTY",
                "ApplicationSubmitted: stream must start empty",
                {"application_id": self.application_id},
            )
        p = ApplicationSubmittedPayload.model_validate(event.payload)
        self.assert_can_transition_to(ApplicationState.SUBMITTED)
        self.state = ApplicationState.SUBMITTED
        self.applicant_id = p.applicant_id
        self.requested_amount_usd = p.requested_amount_usd

    def _on_CreditAnalysisRequested(self, event: StoredEvent) -> None:
        self._require_states({ApplicationState.SUBMITTED}, "CreditAnalysisRequested")
        self.assert_can_transition_to(ApplicationState.AWAITING_ANALYSIS)
        self.state = ApplicationState.AWAITING_ANALYSIS

    def _on_FraudScreeningCompleted(self, event: StoredEvent) -> None:
        self._require_states({ApplicationState.AWAITING_ANALYSIS}, "FraudScreeningCompleted")
        self._fraud_screening_done = True
        self._maybe_enter_analysis_complete()

    def _on_CreditAnalysisCompleted(self, event: StoredEvent) -> None:
        self._require_states({ApplicationState.AWAITING_ANALYSIS}, "CreditAnalysisCompleted")
        self._credit_analysis_done = True
        self._maybe_enter_analysis_complete()

    def _on_ComplianceReviewStarted(self, event: StoredEvent) -> None:
        self._require_states({ApplicationState.ANALYSIS_COMPLETE}, "ComplianceReviewStarted")
        self.assert_can_transition_to(ApplicationState.COMPLIANCE_REVIEW)
        self.state = ApplicationState.COMPLIANCE_REVIEW

    def _on_DecisionGenerated(self, event: StoredEvent) -> None:
        self._require_states({ApplicationState.COMPLIANCE_REVIEW}, "DecisionGenerated")
        p = DecisionGeneratedPayload.model_validate(event.payload)
        rec = self._apply_confidence_floor(p.model_dump())
        self._last_decision_recommendation = rec
        self.assert_can_transition_to(ApplicationState.PENDING_DECISION)
        self.state = ApplicationState.PENDING_DECISION

    def _on_HumanReviewCompleted(self, event: StoredEvent) -> None:
        self._require_states(
            {ApplicationState.PENDING_DECISION},
            "HumanReviewCompleted",
        )
        p = HumanReviewCompletedPayload.model_validate(event.payload)
        final = p.final_decision.upper()
        if final in ("APPROVED", "APPROVE"):
            self.assert_can_transition_to(ApplicationState.DECIDED_PENDING_HUMAN)
            self.state = ApplicationState.DECIDED_PENDING_HUMAN
            self._human_line_decision = "APPROVE"
        elif final in ("DECLINED", "DECLINE"):
            self.assert_can_transition_to(ApplicationState.DECIDED_PENDING_HUMAN)
            self.state = ApplicationState.DECIDED_PENDING_HUMAN
            self._human_line_decision = "DECLINE"
        else:
            self._human_line_decision = None

    def _on_ApplicationApproved(self, event: StoredEvent) -> None:
        self._require_states(
            {ApplicationState.DECIDED_PENDING_HUMAN},
            "ApplicationApproved",
        )
        if self._human_line_decision != "APPROVE":
            raise DomainError(
                "INVALID_FINAL_EVENT",
                "ApplicationApproved requires prior HumanReviewCompleted with final_decision APPROVE/APPROVED",
                {"application_id": self.application_id, "human_line_decision": self._human_line_decision},
            )
        self.assert_can_transition_to(ApplicationState.FINAL_VERDICT)
        self.state = ApplicationState.FINAL_VERDICT

    def _on_ApplicationDeclined(self, event: StoredEvent) -> None:
        self._require_states(
            {ApplicationState.DECIDED_PENDING_HUMAN},
            "ApplicationDeclined",
        )
        if self._human_line_decision != "DECLINE":
            raise DomainError(
                "INVALID_FINAL_EVENT",
                "ApplicationDeclined requires prior HumanReviewCompleted with final_decision DECLINE/DECLINED",
                {"application_id": self.application_id, "human_line_decision": self._human_line_decision},
            )
        self.assert_can_transition_to(ApplicationState.FINAL_VERDICT)
        self.state = ApplicationState.FINAL_VERDICT

    # --- Rule 4 (used before append + kept consistent on replay) ---

    @staticmethod
    def _apply_confidence_floor(payload: Dict[str, Any]) -> str:
        confidence = float(payload.get("confidence_score", 0.0))
        recommendation = str(payload.get("recommendation", "REFER")).upper()
        if confidence < 0.6:
            return "REFER"
        return recommendation

    @classmethod
    def build_decision_generated_payload(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Determine step: enforce confidence floor before persisting."""
        out = dict(payload)
        out["recommendation"] = cls._apply_confidence_floor(out)
        return out

    # --- Rule 5 ---

    def validate_application_approved(self, compliance: ComplianceRecordAggregate) -> None:
        if not compliance.all_mandatory_checks_passed():
            raise DomainError(
                "RULE5_COMPLIANCE_GATE",
                "ApplicationApproved blocked: not all mandatory ComplianceRulePassed events are present.",
                {"application_id": self.application_id},
            )

    # --- Rule 6 ---

    @staticmethod
    async def validate_decision_causal_chain(
        store: EventStore,
        application_id: str,
        contributing_session_ids: List[str],
    ) -> None:
        """
        contributing_agent_sessions[] must reference AgentSession streams that contain
        a decision-class event (credit or fraud) for this application_id.
        """
        for session_key in contributing_session_ids:
            stream_id = f"agent-{session_key}"
            events = await store.load_stream(stream_id)
            found = False
            for ev in events:
                if ev.event_type not in (
                    "CreditAnalysisCompleted",
                    "FraudScreeningCompleted",
                ):
                    continue
                if str(ev.payload.get("application_id")) != str(application_id):
                    continue
                found = True
                break
            if not found:
                raise DomainError(
                    "RULE6_CAUSAL_CHAIN",
                    f"DecisionGenerated causal chain invalid: session {session_key!r} "
                    f"has no credit/fraud decision for application_id={application_id!r}.",
                    {"application_id": application_id, "session_key": session_key},
                )

    def assert_may_submit_new_application(self) -> None:
        """Handler guard: empty loan stream only."""
        if self.version != 0 or self.state is not None:
            raise DomainError(
                "APPLICATION_ALREADY_EXISTS",
                "Application already exists for this application_id.",
                {
                    "application_id": self.application_id,
                    "version": self.version,
                    "state": self.state.value if self.state else None,
                },
            )

    def assert_ready_for_credit_completion_on_loan(self) -> None:
        """Validate before appending CreditAnalysisCompleted to the loan stream."""
        self._require_states({ApplicationState.AWAITING_ANALYSIS}, "CreditAnalysisCompleted")
        if self._credit_analysis_done:
            raise DomainError(
                "CREDIT_ANALYSIS_ALREADY_COMPLETE",
                "Loan stream already has credit analysis completion for this application.",
                {"application_id": self.application_id},
            )

    def validate_replay_invariants(self, events: List[StoredEvent]) -> None:
        """Rule 2 companion for loan stream ordering (loan-specific events only)."""
        if not events:
            return

    @classmethod
    async def load(cls, store: EventStore, application_id: str) -> "LoanApplicationAggregate":
        stream_id = f"loan-{application_id}"
        events = await store.load_stream(stream_id)
        agg = cls(application_id=application_id)
        for ev in events:
            agg._apply(ev)
        agg.validate_replay_invariants(events)
        return agg
