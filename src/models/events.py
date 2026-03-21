"""
Domain events, payloads, and structured errors.

Never store PII in the event payload without encryption.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Dict, List, Literal, Optional, Union
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

# ---------------------------------------------------------------------------
# Structured errors
# ---------------------------------------------------------------------------


class OptimisticConcurrencyError(Exception):
    """Append rejected: stream version did not match expected_version."""

    def __init__(
        self,
        stream_id: str,
        expected_version: int,
        actual_version: int,
        message: str | None = None,
    ) -> None:
        self.stream_id = stream_id
        self.expected_version = expected_version
        self.actual_version = actual_version
        msg = message or (
            f"OCC conflict on stream {stream_id!r}: "
            f"expected_version={expected_version}, actual_version={actual_version}"
        )
        super().__init__(msg)


class DomainError(Exception):
    """Domain / invariant violation with machine-readable metadata."""

    def __init__(
        self,
        error_code: str,
        message: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.error_code = error_code
        self.metadata: Dict[str, Any] = dict(metadata or {})
        super().__init__(message)


# ---------------------------------------------------------------------------
# Typed payloads (Event Catalogue + operational extensions)
# ---------------------------------------------------------------------------


class ApplicationSubmittedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    application_id: str
    applicant_id: str
    requested_amount_usd: float
    loan_purpose: str
    submission_channel: str
    submitted_at: str


class CreditAnalysisRequestedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    application_id: str
    assigned_agent_id: str
    requested_at: str
    priority: str


class CreditAnalysisCompletedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    application_id: str
    agent_id: str
    session_id: str = ""
    model_version: str = ""
    confidence_score: float = 0.0
    risk_tier: str = ""
    recommended_limit_usd: float = 0.0
    analysis_duration_ms: int = 0
    input_data_hash: str = ""


class FraudScreeningCompletedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    application_id: str
    agent_id: str = ""
    fraud_score: float
    anomaly_flags: List[str] = Field(default_factory=list)
    screening_model_version: str = ""
    input_data_hash: str = ""


class ComplianceCheckRequestedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    application_id: str
    regulation_set_version: str
    checks_required: List[str]


class ComplianceRulePassedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    application_id: str
    rule_id: str
    rule_version: str
    evaluation_timestamp: str = ""
    evidence_hash: str = ""


class ComplianceRuleFailedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    application_id: str
    rule_id: str
    rule_version: str
    failure_reason: str = ""
    remediation_required: bool = False


class DecisionGeneratedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    application_id: str
    orchestrator_agent_id: str
    recommendation: Literal["APPROVE", "DECLINE", "REFER"]
    confidence_score: float
    contributing_agent_sessions: List[str]
    decision_basis_summary: str = ""
    model_versions: Dict[str, str] = Field(default_factory=dict)


class HumanReviewCompletedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    application_id: str
    reviewer_id: str
    override: bool
    final_decision: str
    override_reason: str = ""


class ApplicationApprovedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    application_id: str
    approved_amount_usd: float
    interest_rate: float = 0.0
    conditions: List[str] = Field(default_factory=list)
    approved_by: str = ""
    effective_date: str = ""


class ApplicationDeclinedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    application_id: str
    decline_reasons: List[str]
    declined_by: str = ""
    adverse_action_notice_required: bool = False


class AgentContextLoadedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    agent_id: str
    session_id: str
    context_source: str
    event_replay_from_position: int = 0
    context_token_count: int = 0
    model_version: str


class AuditIntegrityCheckRunPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    entity_id: str
    check_timestamp: str
    events_verified_count: int
    integrity_hash: str
    previous_hash: str = ""


class ComplianceReviewStartedPayload(BaseModel):
    """Loan-stream bridge: ANALYSIS_COMPLETE → COMPLIANCE_REVIEW (catalogue extension)."""

    model_config = ConfigDict(extra="forbid")
    application_id: str
    started_at: str = ""
    regulation_set_hint: str = ""


class SyntheticSeedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    seq: int
    stream_shard: int = 0
    batch: str = ""


# ---------------------------------------------------------------------------
# Discriminated domain events (append / replay)
# ---------------------------------------------------------------------------


class ApplicationSubmittedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_type: Literal["ApplicationSubmitted"] = "ApplicationSubmitted"
    event_version: int = 1
    payload: ApplicationSubmittedPayload


class CreditAnalysisRequestedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_type: Literal["CreditAnalysisRequested"] = "CreditAnalysisRequested"
    event_version: int = 1
    payload: CreditAnalysisRequestedPayload


class CreditAnalysisCompletedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_type: Literal["CreditAnalysisCompleted"] = "CreditAnalysisCompleted"
    event_version: int = 2
    payload: CreditAnalysisCompletedPayload


class FraudScreeningCompletedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_type: Literal["FraudScreeningCompleted"] = "FraudScreeningCompleted"
    event_version: int = 1
    payload: FraudScreeningCompletedPayload


class ComplianceCheckRequestedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_type: Literal["ComplianceCheckRequested"] = "ComplianceCheckRequested"
    event_version: int = 1
    payload: ComplianceCheckRequestedPayload


class ComplianceRulePassedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_type: Literal["ComplianceRulePassed"] = "ComplianceRulePassed"
    event_version: int = 1
    payload: ComplianceRulePassedPayload


class ComplianceRuleFailedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_type: Literal["ComplianceRuleFailed"] = "ComplianceRuleFailed"
    event_version: int = 1
    payload: ComplianceRuleFailedPayload


class DecisionGeneratedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_type: Literal["DecisionGenerated"] = "DecisionGenerated"
    event_version: int = 2
    payload: DecisionGeneratedPayload


class HumanReviewCompletedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_type: Literal["HumanReviewCompleted"] = "HumanReviewCompleted"
    event_version: int = 1
    payload: HumanReviewCompletedPayload


class ApplicationApprovedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_type: Literal["ApplicationApproved"] = "ApplicationApproved"
    event_version: int = 1
    payload: ApplicationApprovedPayload


class ApplicationDeclinedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_type: Literal["ApplicationDeclined"] = "ApplicationDeclined"
    event_version: int = 1
    payload: ApplicationDeclinedPayload


class AgentContextLoadedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_type: Literal["AgentContextLoaded"] = "AgentContextLoaded"
    event_version: int = 1
    payload: AgentContextLoadedPayload


class AuditIntegrityCheckRunEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_type: Literal["AuditIntegrityCheckRun"] = "AuditIntegrityCheckRun"
    event_version: int = 1
    payload: AuditIntegrityCheckRunPayload


class ComplianceReviewStartedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_type: Literal["ComplianceReviewStarted"] = "ComplianceReviewStarted"
    event_version: int = 1
    payload: ComplianceReviewStartedPayload


class SyntheticSeedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_type: Literal["SyntheticSeedEvent"] = "SyntheticSeedEvent"
    event_version: int = 1
    payload: SyntheticSeedPayload


DomainEventUnion = Union[
    ApplicationSubmittedEvent,
    CreditAnalysisRequestedEvent,
    CreditAnalysisCompletedEvent,
    FraudScreeningCompletedEvent,
    ComplianceCheckRequestedEvent,
    ComplianceRulePassedEvent,
    ComplianceRuleFailedEvent,
    DecisionGeneratedEvent,
    HumanReviewCompletedEvent,
    ApplicationApprovedEvent,
    ApplicationDeclinedEvent,
    AgentContextLoadedEvent,
    AuditIntegrityCheckRunEvent,
    ComplianceReviewStartedEvent,
    SyntheticSeedEvent,
]

DomainEventDiscriminated = Annotated[
    DomainEventUnion,
    Field(discriminator="event_type"),
]

DomainEventAdapter = TypeAdapter(DomainEventDiscriminated)

# Type alias for `EventStore.append(..., events=[...])` (concrete event models).
BaseEvent = DomainEventUnion


def domain_event_to_storage_dict(ev: DomainEventUnion) -> Dict[str, Any]:
    """Serialize envelope for JSONB (payload as plain JSON-compatible dict)."""
    dumped = ev.model_dump(mode="json")
    return {
        "event_type": dumped["event_type"],
        "event_version": dumped["event_version"],
        "payload": dumped["payload"],
    }


def parse_domain_event_from_storage(data: Dict[str, Any]) -> DomainEventUnion:
    """Validate a dict envelope (e.g. from tests) into a concrete domain event."""
    return DomainEventAdapter.validate_python(data)


# ---------------------------------------------------------------------------
# Stored event (read model from DB)
# ---------------------------------------------------------------------------


class StoredEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: UUID
    stream_id: str
    stream_position: int
    global_position: int
    event_type: str
    event_version: int
    payload: Dict[str, Any]
    metadata: Dict[str, Any]
    recorded_at: datetime

    def payload_as_model(self) -> BaseModel:
        """Parse payload dict into the catalogue payload type."""
        model = PAYLOAD_TYPES.get(self.event_type)
        if model is None:
            raise DomainError(
                "UNKNOWN_EVENT_TYPE",
                f"No payload model registered for event_type={self.event_type!r}",
                {"event_type": self.event_type},
            )
        return model.model_validate(self.payload)


# event_type -> payload model (for aggregates)
PAYLOAD_TYPES: Dict[str, type[BaseModel]] = {
    "ApplicationSubmitted": ApplicationSubmittedPayload,
    "CreditAnalysisRequested": CreditAnalysisRequestedPayload,
    "CreditAnalysisCompleted": CreditAnalysisCompletedPayload,
    "FraudScreeningCompleted": FraudScreeningCompletedPayload,
    "ComplianceCheckRequested": ComplianceCheckRequestedPayload,
    "ComplianceRulePassed": ComplianceRulePassedPayload,
    "ComplianceRuleFailed": ComplianceRuleFailedPayload,
    "DecisionGenerated": DecisionGeneratedPayload,
    "HumanReviewCompleted": HumanReviewCompletedPayload,
    "ApplicationApproved": ApplicationApprovedPayload,
    "ApplicationDeclined": ApplicationDeclinedPayload,
    "AgentContextLoaded": AgentContextLoadedPayload,
    "AuditIntegrityCheckRun": AuditIntegrityCheckRunPayload,
    "ComplianceReviewStarted": ComplianceReviewStartedPayload,
    "SyntheticSeedEvent": SyntheticSeedPayload,
}


class StreamMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid")
    stream_id: str
    aggregate_type: str
    current_version: int
    created_at: datetime
    archived_at: Optional[datetime] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
