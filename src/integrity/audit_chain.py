"""
Cryptographic hash chain over audit stream events (tamper evidence).

Uses causal metadata from Phase 2 where present (correlation_id / causation_id in metadata).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from src.event_store import EventStore
from src.models.events import (
    AuditIntegrityCheckRunEvent,
    AuditIntegrityCheckRunPayload,
    StoredEvent,
)


def _payload_digest(ev: StoredEvent) -> bytes:
    raw = json.dumps(ev.payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).digest()


def fold_integrity_chain(business_events: List[StoredEvent]) -> str:
    """
    new_hash = sha256(previous_hash_bytes + sha256(payload_json))
    repeated for each business event in stream order.
    """
    prev = b""
    for ev in business_events:
        prev = hashlib.sha256(prev + _payload_digest(ev)).digest()
    return prev.hex()


@dataclass(frozen=True)
class IntegrityCheckResult:
    """Unified result shape for `run_integrity_check` and `record_integrity_check`."""

    tamper_detected: bool
    integrity_hash: str
    previous_hash: str | None
    events_verified_count: int
    entity_type: Optional[str] = None
    entity_id: Optional[str] = None
    chain_valid: Optional[bool] = None
    new_hash: Optional[str] = None
    streams_audited: tuple[str, ...] = ()
    triggered_by: str = ""
    checked_at: Optional[datetime] = None


async def run_integrity_check(
    store: EventStore,
    entity_type: str,
    entity_id: str,
    *,
    correlation_id: Optional[str] = None,
    causation_id: Optional[str] = None,
) -> IntegrityCheckResult:
    """
    Recompute cumulative hash over all non-integrity events on the business stream
    ``{entity_type}-{entity_id}`` and append ``AuditIntegrityCheckRun`` to
    ``audit-{entity_type}-{entity_id}``.

    If the latest prior AuditIntegrityCheckRun claimed the same event count but a different
    cumulative hash than recomputed from current DB payloads, tamper_detected=True.
    Appends a new AuditIntegrityCheckRun with the current chain head.
    """
    business_stream = f"{entity_type}-{entity_id}"
    audit_stream = f"audit-{entity_type}-{entity_id}"

    events_business = await store.load_stream(business_stream, apply_upcast=False)
    business = [e for e in events_business if e.event_type != "AuditIntegrityCheckRun"]
    chain_hex = fold_integrity_chain(business)

    audit_events = await store.load_stream(audit_stream, apply_upcast=False)
    last_integrity: StoredEvent | None = None
    for e in reversed(audit_events):
        if e.event_type == "AuditIntegrityCheckRun":
            last_integrity = e
            break

    prev_claimed = ""
    tamper = False
    if last_integrity is not None:
        lp = last_integrity.payload if isinstance(last_integrity.payload, dict) else {}
        prev_claimed = str(lp.get("integrity_hash", "") or "")
        n_prev = int(lp.get("events_verified_count", 0) or 0)
        if n_prev == len(business) and prev_claimed and prev_claimed != chain_hex:
            tamper = True

    ts = datetime.now(timezone.utc).isoformat()
    out = AuditIntegrityCheckRunEvent(
        payload=AuditIntegrityCheckRunPayload(
            entity_id=entity_id,
            check_timestamp=ts,
            events_verified_count=len(business),
            integrity_hash=chain_hex,
            previous_hash=prev_claimed,
        )
    )

    v = await store.stream_version(audit_stream)
    expected_version = -1 if v == 0 else v
    await store.append(
        stream_id=audit_stream,
        events=[out],
        expected_version=expected_version,
        correlation_id=correlation_id,
        causation_id=causation_id,
    )

    return IntegrityCheckResult(
        tamper_detected=tamper,
        integrity_hash=chain_hex,
        previous_hash=prev_claimed or None,
        events_verified_count=len(business),
        chain_valid=not tamper,
        new_hash=chain_hex,
    )
