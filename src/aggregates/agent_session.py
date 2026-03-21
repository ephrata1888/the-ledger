"""
AgentSession aggregate (agent-{agent_id}-{session_id}).
Never store PII in the payload without encryption.
"""

from __future__ import annotations

from typing import Dict, List, Set

from src.event_store import EventStore
from src.models.events import DomainError, StoredEvent


class AgentSessionAggregate:
    def __init__(self, agent_id: str, session_id: str) -> None:
        self.agent_id = agent_id
        self.session_id = session_id
        self.version: int = 0
        self._context_loaded: bool = False
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
                "AgentSession (Rule 2): first persisted event must be AgentContextLoaded (Gas Town)."
            )
        return agg

    def _apply(self, event: StoredEvent) -> None:
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
        self.version = event.stream_position

    def _on_AgentContextLoaded(self, event: StoredEvent) -> None:
        self._context_loaded = True

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
                "AgentSession (Rule 2 / Gas Town): AgentContextLoaded must be the first event "
                "before any decision event."
            )

    def assert_first_event_is_context(self, proposed_event_type: str) -> None:
        """When stream is empty, only AgentContextLoaded may be appended first."""
        if self.version == 0 and proposed_event_type != "AgentContextLoaded":
            raise DomainError(
                f"AgentSession (Rule 2): first event must be AgentContextLoaded, not {proposed_event_type}."
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
                "AgentSession (Rule 3): CreditAnalysisCompleted already exists for this "
                "application in this session; HumanReviewCompleted(override=true) on the loan "
                "stream after that analysis is required before another credit analysis."
            )
