"""
LoanApplication aggregate (loan-{application_id}).
Never store PII in the payload without encryption.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Set

from src.aggregates.compliance_record import ComplianceRecordAggregate
from src.event_store import EventStore
from src.models.events import DomainError, StoredEvent


class ApplicationState(str, Enum):
    SUBMITTED = "SUBMITTED"
    AWAITING_ANALYSIS = "AWAITING_ANALYSIS"
    ANALYSIS_COMPLETE = "ANALYSIS_COMPLETE"
    COMPLIANCE_REVIEW = "COMPLIANCE_REVIEW"
    PENDING_DECISION = "PENDING_DECISION"
    APPROVED_PENDING_HUMAN = "APPROVED_PENDING_HUMAN"
    DECLINED_PENDING_HUMAN = "DECLINED_PENDING_HUMAN"
    FINAL_APPROVED = "FINAL_APPROVED"
    FINAL_DECLINED = "FINAL_DECLINED"


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
        self._human_review_override: bool = False

    def _apply(self, event: StoredEvent) -> None:
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
        self.version = event.stream_position

    # --- transition helpers (Rule 1) ---

    def _require_states(self, allowed: Set[ApplicationState], event_type: str) -> None:
        if self.state not in allowed:
            raise DomainError(
                f"Invalid state for {event_type}: current={self.state}, allowed={allowed}"
            )

    def _enter_compliance_review_if_ready(self) -> None:
        if (
            self.state == ApplicationState.AWAITING_ANALYSIS
            and self._fraud_screening_done
            and self._credit_analysis_done
        ):
            # ANALYSIS_COMPLETE (logical) and COMPLIANCE_REVIEW are represented by this state:
            # catalogue has no separate loan event between the two phases.
            self.state = ApplicationState.COMPLIANCE_REVIEW

    # --- event handlers ---

    def _on_ApplicationSubmitted(self, event: StoredEvent) -> None:
        if self.state is not None:
            raise DomainError("ApplicationSubmitted: stream must start empty")
        self.state = ApplicationState.SUBMITTED
        self.applicant_id = event.payload.get("applicant_id")
        self.requested_amount_usd = event.payload.get("requested_amount_usd")

    def _on_CreditAnalysisRequested(self, event: StoredEvent) -> None:
        self._require_states({ApplicationState.SUBMITTED}, "CreditAnalysisRequested")
        self.state = ApplicationState.AWAITING_ANALYSIS

    def _on_FraudScreeningCompleted(self, event: StoredEvent) -> None:
        self._require_states({ApplicationState.AWAITING_ANALYSIS}, "FraudScreeningCompleted")
        self._fraud_screening_done = True
        self._enter_compliance_review_if_ready()

    def _on_CreditAnalysisCompleted(self, event: StoredEvent) -> None:
        self._require_states({ApplicationState.AWAITING_ANALYSIS}, "CreditAnalysisCompleted")
        self._credit_analysis_done = True
        self._enter_compliance_review_if_ready()

    def _on_DecisionGenerated(self, event: StoredEvent) -> None:
        self._require_states({ApplicationState.COMPLIANCE_REVIEW}, "DecisionGenerated")
        # Rule 4 — recompute on replay so read-side matches persisted facts
        rec = self._apply_confidence_floor(event.payload)
        self._last_decision_recommendation = rec
        self.state = ApplicationState.PENDING_DECISION

    def _on_HumanReviewCompleted(self, event: StoredEvent) -> None:
        self._require_states(
            {ApplicationState.PENDING_DECISION},
            "HumanReviewCompleted",
        )
        self._human_review_override = bool(event.payload.get("override"))
        final = (event.payload.get("final_decision") or "").upper()
        if final in ("APPROVED", "APPROVE"):
            self.state = ApplicationState.APPROVED_PENDING_HUMAN
        elif final in ("DECLINED", "DECLINE"):
            self.state = ApplicationState.DECLINED_PENDING_HUMAN
        else:
            self.state = ApplicationState.PENDING_DECISION

    def _on_ApplicationApproved(self, event: StoredEvent) -> None:
        self._require_states(
            {ApplicationState.APPROVED_PENDING_HUMAN},
            "ApplicationApproved",
        )
        self.state = ApplicationState.FINAL_APPROVED

    def _on_ApplicationDeclined(self, event: StoredEvent) -> None:
        self._require_states(
            {ApplicationState.DECLINED_PENDING_HUMAN},
            "ApplicationDeclined",
        )
        self.state = ApplicationState.FINAL_DECLINED

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
                "ApplicationApproved blocked (Rule 5): not all mandatory ComplianceRulePassed "
                "events are present for required checks."
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
                    f"DecisionGenerated causal chain invalid (Rule 6): session {session_key!r} "
                    f"has no credit/fraud decision for application_id={application_id!r}."
                )

    def assert_ready_for_credit_completion_on_loan(self) -> None:
        """Validate before appending CreditAnalysisCompleted to the loan stream."""
        self._require_states({ApplicationState.AWAITING_ANALYSIS}, "CreditAnalysisCompleted")
        if self._credit_analysis_done:
            raise DomainError("Loan stream already has credit analysis completion for this application.")

    def validate_replay_invariants(self, events: List[StoredEvent]) -> None:
        """Rule 2 companion for loan stream ordering (loan-specific events only)."""
        if not events:
            return
        # Intentionally minimal: loan stream does not carry AgentContextLoaded.

    @classmethod
    async def load(cls, store: EventStore, application_id: str) -> "LoanApplicationAggregate":
        stream_id = f"loan-{application_id}"
        events = await store.load_stream(stream_id)
        agg = cls(application_id=application_id)
        for ev in events:
            agg._apply(ev)
        agg.validate_replay_invariants(events)
        return agg
