"""
Read-time schema evolution: UpcasterRegistry applies version chains without mutating DB rows.

See DESIGN.md for inference policies and regulatory rationale.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Awaitable, Callable, Dict, Optional, Tuple

from src.models.events import DomainError, StoredEvent

# Canonical catalogue head versions (event_type -> max event_version).
CATALOGUE_MAX_VERSION: Dict[str, int] = {
    "CreditAnalysisCompleted": 2,
    "DecisionGenerated": 2,
}

SyncUpcaster = Callable[[StoredEvent], StoredEvent]
AsyncUpcaster = Callable[[StoredEvent, "EventStore"], Awaitable[StoredEvent]]


@dataclass
class UpcasterRegistry:
    """
    Keyed by (event_type, from_version) -> single step to from_version+1.
    Chains are applied repeatedly until CATALOGUE_MAX_VERSION is reached.
    """

    _sync: Dict[Tuple[str, int], SyncUpcaster] = field(default_factory=dict)
    _async_step: Dict[Tuple[str, int], AsyncUpcaster] = field(default_factory=dict)

    def register_sync(self, event_type: str, from_version: int, fn: SyncUpcaster) -> None:
        self._sync[(event_type, from_version)] = fn

    def register_async(self, event_type: str, from_version: int, fn: AsyncUpcaster) -> None:
        self._async_step[(event_type, from_version)] = fn

    def max_version(self, event_type: str) -> int:
        return CATALOGUE_MAX_VERSION.get(event_type, 0)

    async def upcast(
        self,
        ev: StoredEvent,
        store: Optional["EventStore"] = None,
    ) -> StoredEvent:
        """
        Apply upcast chain until current catalogue head or no registered step.
        Async steps (e.g. DecisionGenerated) require `store` for read-only agent stream lookups.
        """
        while True:
            target = CATALOGUE_MAX_VERSION.get(ev.event_type)
            if target is None or ev.event_version >= target:
                break

            key = (ev.event_type, ev.event_version)
            if key in self._async_step:
                if store is None:
                    raise DomainError(
                        "UPCAST_STORE_REQUIRED",
                        f"Async upcast for {ev.event_type} v{ev.event_version} requires EventStore context",
                        {"event_type": ev.event_type, "event_version": ev.event_version},
                    )
                ev = await self._async_step[key](ev, store)
                continue

            fn = self._sync.get(key)
            if fn is None:
                break
            ev = fn(ev)

        return ev


def default_registry() -> UpcasterRegistry:
    """Registry with built-in CreditAnalysisCompleted and DecisionGenerated upcasters."""
    from src.upcasting.upcasters import (
        upcast_credit_analysis_completed_v1_to_v2,
        upcast_decision_generated_v1_to_v2,
    )

    r = UpcasterRegistry()
    r.register_sync("CreditAnalysisCompleted", 1, upcast_credit_analysis_completed_v1_to_v2)
    r.register_async("DecisionGenerated", 1, upcast_decision_generated_v1_to_v2)
    return r
