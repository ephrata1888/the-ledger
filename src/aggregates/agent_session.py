"""
AgentSession aggregate (agent-{agent_id}-{session_id}).
Never store PII in the payload without encryption.
"""

from __future__ import annotations

from typing import List, Set

from src.event_store import EventStore
from src.models.events import AgentContextLoadedPayload, DomainError, StoredEvent


class AgentSessionAggregate:
    def __init__(self, agent_id: str, session_id: str) -> None:
        self.agent_id = agent_id
        self.session_id = session_id
        self.version: int = 0
        self._context_loaded: bool = False
        self._model_version: str | None = None
        self._credit_completed_for_app: Set[str] = set()

    @property
    def stream_id(self) -> str:
        return f"agent-{self.agent_id}-{self.session_id}"

    @classmethod
    async def load(cls, store: EventStore, agent_id: str, session_id: str) -> "AgentSessionAggregate":
        agg = cls(agent_id=agent_id, session_id=session_id)
        events = await store.load_stream(agg.stream_id)
        for ev in events:
            agg._apply(ev)
        if events and events[0].event_type != "AgentContextLoaded":
            raise DomainError(
                "AGENT_GAS_TOWN_ORDER",
                "AgentSession: first persisted event must be AgentContextLoaded (Gas Town).",
                {"stream_id": agg.stream_id, "first_event_type": events[0].event_type},
            )
        return agg

    def _apply(self, event: StoredEvent) -> None:
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
        self.version = event.stream_position

    def _on_AgentContextLoaded(self, event: StoredEvent) -> None:
        p = AgentContextLoadedPayload.model_validate(event.payload)
        self._context_loaded = True
        self._model_version = p.model_version

    def _on_CreditAnalysisCompleted(self, event: StoredEvent) -> None:
        app_id = event.payload.get("application_id")
        if app_id is not None:
            self._credit_completed_for_app.add(str(app_id))

    def _on_FraudScreeningCompleted(self, event: StoredEvent) -> None:
        pass

    # --- Rule 2: Gas Town ---

    def assert_context_loaded_for_decision(self) -> None:
        if not self._context_loaded:
            raise DomainError(
                "AGENT_CONTEXT_REQUIRED",
                "AgentSession (Gas Town): AgentContextLoaded must precede decision events.",
                {"stream_id": self.stream_id},
            )

    def assert_first_event_is_context(self, proposed_event_type: str) -> None:
        """When stream is empty, only AgentContextLoaded may be appended first."""
        if self.version == 0 and proposed_event_type != "AgentContextLoaded":
            raise DomainError(
                "AGENT_FIRST_EVENT_CONTEXT",
                f"AgentSession: first event must be AgentContextLoaded, not {proposed_event_type}.",
                {"stream_id": self.stream_id, "proposed_event_type": proposed_event_type},
            )

    def assert_model_version_match(self, declared_version: str) -> None:
        """Command model_version must match the anchored context (AgentContextLoaded)."""
        if not self._context_loaded or self._model_version is None:
            raise DomainError(
                "AGENT_MODEL_VERSION_UNKNOWN",
                "Cannot verify model version: AgentContextLoaded not replayed on this session.",
                {"stream_id": self.stream_id},
            )
        if declared_version != self._model_version:
            raise DomainError(
                "MODEL_VERSION_MISMATCH",
                "Declared model_version does not match AgentContextLoaded.model_version.",
                {
                    "stream_id": self.stream_id,
                    "declared_version": declared_version,
                    "stored_model_version": self._model_version,
                },
            )

    # --- Rule 3: model / analysis locking ---

    def validate_credit_analysis_not_locked(
        self,
        application_id: str,
        loan_events: List[StoredEvent],
        session_events: List[StoredEvent],
    ) -> None:
        """
        After one CreditAnalysisCompleted for application_id in this session, reject another
        unless a HumanReviewCompleted with override=True exists on the loan stream *after*
        that credit event (global ordering).
        """
        credit_gps = [
            ev.global_position
            for ev in session_events
            if ev.event_type == "CreditAnalysisCompleted"
            and str(ev.payload.get("application_id")) == str(application_id)
        ]
        if not credit_gps:
            return
        last_credit_gp = max(credit_gps)
        override_ok = any(
            ev.event_type == "HumanReviewCompleted"
            and bool(ev.payload.get("override"))
            and str(ev.payload.get("application_id")) == str(application_id)
            and ev.global_position > last_credit_gp
            for ev in loan_events
        )
        if not override_ok:
            raise DomainError(
                "CREDIT_ANALYSIS_LOCKED",
                "CreditAnalysisCompleted already exists for this application in this session; "
                "HumanReviewCompleted(override=true) on the loan stream after that analysis is required.",
                {"stream_id": self.stream_id, "application_id": application_id},
            )
