"""
AuditLedger is a governance aggregate: it ties cross-stream business activity
for one entity to a cryptographic integrity chain stored on `audit-{entity_type}-{entity_id}`.

It is **not** a duplicate of loan/compliance/agent streams. Those streams remain
canonical; the audit stream only records `AuditIntegrityCheckRun` checkpoints that
reference which streams were hashed and the resulting chain head.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from uuid import UUID

from src.event_store import EventStore
from src.integrity.audit_chain import IntegrityCheckResult
from src.models.events import (
    AuditIntegrityCheckRunEvent,
    AuditIntegrityCheckRunPayload,
    StoredEvent,
)


class AuditViolationError(Exception):
    """Raised when the audit ledger refuses a write because the chain is already broken."""

    def __init__(self, entity_type: str, entity_id: str, reason: str) -> None:
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.reason = reason
        super().__init__(f"Audit violation for {entity_type}-{entity_id}: {reason}")


def _payload_digest_hex(ev: StoredEvent) -> str:
    payload_str = json.dumps(ev.payload, sort_keys=True, default=str)
    return hashlib.sha256(payload_str.encode("utf-8")).hexdigest()


def _chain_head(anchor: str, events: list[StoredEvent]) -> str:
    parts = "".join(_payload_digest_hex(e) for e in events)
    return hashlib.sha256((anchor + parts).encode("utf-8")).hexdigest()


class AuditLedgerAggregate:
    """Replay-only state + one write path: `record_integrity_check`."""

    def __init__(self, entity_type: str, entity_id: str) -> None:
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.version: int = -1
        self.event_count: int = 0
        self.last_integrity_hash: str | None = None
        # Anchor that prefixed the join(...) for the last verified checkpoint (None → GENESIS).
        self._last_anchor: str | None = None
        self.last_check_at: datetime | None = None
        self.chain_valid: bool = True
        self.cross_stream_refs: list[str] = []

    @classmethod
    async def load(cls, store: EventStore, entity_type: str, entity_id: str) -> "AuditLedgerAggregate":
        stream_id = f"audit-{entity_type}-{entity_id}"
        events = await store.load_stream(stream_id)
        agg = cls(entity_type=entity_type, entity_id=entity_id)
        for event in events:
            agg._apply(event)
        return agg

    def _apply(self, event: StoredEvent) -> None:
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
        self.version = event.stream_position

    def _merge_audit_payload(self, p: AuditIntegrityCheckRunPayload) -> None:
        self.event_count = int(p.events_verified_count)
        self.last_integrity_hash = str(p.integrity_hash) if p.integrity_hash else None
        ph = (p.previous_hash or "").strip()
        self._last_anchor = ph if ph else None
        raw_ts = p.check_timestamp.replace("Z", "+00:00")
        self.last_check_at = datetime.fromisoformat(raw_ts)
        if self.last_check_at.tzinfo is None:
            self.last_check_at = self.last_check_at.replace(tzinfo=timezone.utc)
        self.chain_valid = self.chain_valid and bool(p.chain_valid)
        for sid in p.streams_audited:
            if sid not in self.cross_stream_refs:
                self.cross_stream_refs.append(sid)

    def _on_AuditIntegrityCheckRun(self, event: StoredEvent) -> None:
        p = AuditIntegrityCheckRunPayload.model_validate(event.payload)
        self._merge_audit_payload(p)

    def assert_append_only(self) -> None:
        """
        Raises AuditViolationError if any condition indicates the stream
        has been tampered with (chain_valid == False).
        This must be called at the start of every command that writes to this stream.
        """
        if not self.chain_valid:
            raise AuditViolationError(
                self.entity_type,
                self.entity_id,
                "Integrity chain marked invalid; refuse further audit writes.",
            )

    def stream_id(self) -> str:
        return f"audit-{self.entity_type}-{self.entity_id}"

    def _primary_business_stream_id(self) -> str:
        return f"{self.entity_type}-{self.entity_id}"

    async def load_cross_stream_timeline(
        self,
        store: EventStore,
    ) -> list[StoredEvent]:
        """
        Loads all events related to this entity across ALL streams.
        Uses two strategies to find related events:
        1. Direct stream membership: streams in self.cross_stream_refs
        2. Payload correlation: any event where payload['application_id'] == self.entity_id
           OR metadata['correlation_id'] is shared with the primary stream's events

        Returns events sorted by (recorded_at ASC, global_position ASC).
        Deduplicates by event_id.
        Never reads from the audit-{entity_type}-{entity_id} stream itself —
        that stream only contains integrity check events, not business events.
        """
        audit_sid = self.stream_id()
        primary = self._primary_business_stream_id()
        streams_to_load: set[str] = set(self.cross_stream_refs)
        if primary != audit_sid:
            streams_to_load.add(primary)

        correlation_ids: set[str] = set()
        by_id: dict[UUID, StoredEvent] = {}

        for sid in streams_to_load:
            if sid == audit_sid:
                continue
            for ev in await store.load_stream(sid):
                by_id[ev.event_id] = ev
                cid = ev.metadata.get("correlation_id")
                if cid:
                    correlation_ids.add(str(cid))

        async for ev in store.load_all():
            if ev.stream_id == audit_sid:
                continue
            payload_app = ev.payload.get("application_id") if isinstance(ev.payload, dict) else None
            if payload_app is not None and str(payload_app) == str(self.entity_id):
                by_id[ev.event_id] = ev
                continue
            mc = ev.metadata.get("correlation_id")
            if mc is not None and str(mc) in correlation_ids:
                by_id[ev.event_id] = ev

        timeline = list(by_id.values())
        timeline.sort(key=lambda e: (e.recorded_at, e.global_position))
        return timeline

    async def record_integrity_check(
        self,
        store: EventStore,
        triggered_by: str,
    ) -> IntegrityCheckResult:
        self.assert_append_only()

        timeline = await self.load_cross_stream_timeline(store)
        streams_audited = sorted({e.stream_id for e in timeline})

        tamper = False

        if self.last_integrity_hash is not None and self.event_count > 0:
            prev_n = self.event_count
            if len(timeline) < prev_n:
                tamper = True
            else:
                prior_slice = timeline[:prev_n]
                event_payload_digests = [
                    hashlib.sha256(
                        json.dumps(e.payload, sort_keys=True, default=str).encode("utf-8"),
                    ).hexdigest()
                    for e in prior_slice
                ]
                anchor_for_prior = self._last_anchor or "GENESIS"
                recomputed_prior_head = hashlib.sha256(
                    (anchor_for_prior + "".join(event_payload_digests)).encode("utf-8"),
                ).hexdigest()
                if recomputed_prior_head != self.last_integrity_hash:
                    tamper = True

        outer_anchor = self.last_integrity_hash if self.last_integrity_hash is not None else "GENESIS"
        new_hash = _chain_head(outer_anchor, timeline)
        chain_ok = not tamper

        checked_at = datetime.now(timezone.utc)
        prev_result: str | None = self.last_integrity_hash

        payload = AuditIntegrityCheckRunPayload(
            entity_id=self.entity_id,
            entity_type=self.entity_type,
            check_timestamp=checked_at.isoformat(),
            events_verified_count=len(timeline),
            integrity_hash=new_hash,
            previous_hash=self.last_integrity_hash or "",
            chain_valid=chain_ok,
            tamper_detected=tamper,
            streams_audited=streams_audited,
            triggered_by=triggered_by,
        )
        event = AuditIntegrityCheckRunEvent(payload=payload)

        new_version = await store.append(
            stream_id=self.stream_id(),
            events=[event],
            expected_version=self.version,
        )
        self._merge_audit_payload(payload)
        self.version = new_version

        result = IntegrityCheckResult(
            tamper_detected=tamper,
            integrity_hash=new_hash,
            previous_hash=prev_result,
            events_verified_count=len(timeline),
            entity_type=self.entity_type,
            entity_id=self.entity_id,
            chain_valid=not tamper,
            new_hash=new_hash,
            streams_audited=tuple(streams_audited),
            triggered_by=triggered_by,
            checked_at=checked_at,
        )
        return result

    async def get_causal_chain(
        self,
        store: EventStore,
        root_event_id: str,
        max_depth: int = 20,
    ) -> list[StoredEvent]:
        """
        Starting from root_event_id, follow causation_id references forward
        (events whose causation_id == root_event_id, then their children, etc.)
        across ALL streams in cross_stream_refs.

        Returns events in causal order (depth-first), deduplicated.
        Stops at max_depth to guard against cycles.

        Use this to answer: "What did this event cause to happen downstream?"
        """
        audit_sid = self.stream_id()
        primary = self._primary_business_stream_id()
        streams: set[str] = set(self.cross_stream_refs)
        if primary != audit_sid:
            streams.add(primary)

        all_events: list[StoredEvent] = []
        for sid in streams:
            if sid == audit_sid:
                continue
            all_events.extend(await store.load_stream(sid))
        all_events.sort(key=lambda e: (e.recorded_at, e.global_position))

        by_causation: dict[str, list[StoredEvent]] = {}
        for ev in all_events:
            cid = ev.metadata.get("causation_id")
            if cid is None:
                continue
            key = str(cid)
            by_causation.setdefault(key, []).append(ev)

        for key in by_causation:
            by_causation[key].sort(key=lambda e: (e.recorded_at, e.global_position))

        out: list[StoredEvent] = []
        visited: set[UUID] = set()

        def walk(parent: str, depth: int) -> None:
            if depth >= max_depth:
                return
            for ev in by_causation.get(parent, []):
                if ev.event_id in visited:
                    continue
                visited.add(ev.event_id)
                out.append(ev)
                walk(str(ev.event_id), depth + 1)

        walk(str(root_event_id), 0)
        return out
