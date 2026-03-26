"""ComplianceRecord aggregate (compliance-{application_id}) — minimal for Rule 5."""

from __future__ import annotations

from typing import List, Set

from src.event_store import EventStore
from src.models.events import StoredEvent


class ComplianceRecordAggregate:
    """Tracks mandatory checks and passes from the compliance stream."""

    def __init__(self, application_id: str) -> None:
        self.application_id = application_id
        self.version: int = 0
        self._required: Set[str] = set()
        self._passed: Set[str] = set()

    @classmethod
    async def load(cls, store: EventStore, application_id: str) -> "ComplianceRecordAggregate":
        stream_id = f"compliance-{application_id}"
        events = await store.load_stream(stream_id)
        agg = cls(application_id=application_id)
        for ev in events:
            agg._apply(ev)
        return agg

    def _apply(self, event: StoredEvent) -> None:
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
        self.version = event.stream_position

    def _on_ComplianceCheckRequested(self, event: StoredEvent) -> None:
        checks = event.payload.get("checks_required") or []
        self._required = {str(c) for c in checks}

    def _on_ComplianceRulePassed(self, event: StoredEvent) -> None:
        rid = event.payload.get("rule_id")
        if rid is not None:
            self._passed.add(str(rid))

    def _on_ComplianceRuleFailed(self, event: StoredEvent) -> None:
        pass

    def _on_ComplianceRuleNoted(self, event: StoredEvent) -> None:
        pass

    def _on_ComplianceCheckCompleted(self, event: StoredEvent) -> None:
        pass

    def all_mandatory_checks_passed(self) -> bool:
        if not self._required:
            # No compliance gate defined yet — cannot approve under Rule 5 interpretation
            return False
        return self._required.issubset(self._passed)
