"""Tamper detection on audit hash chain."""

import json
from uuid import uuid4

import asyncpg
import pytest

from src.event_store import EventStore
from src.integrity.audit_chain import run_integrity_check
from src.models.events import ApplicationSubmittedEvent, ApplicationSubmittedPayload


@pytest.mark.asyncio
async def test_tamper_detected_when_payload_mutated(pool: asyncpg.Pool):
    store = EventStore(pool)
    entity = str(uuid4())
    loan_stream = f"loan-{entity}"

    ev = ApplicationSubmittedEvent(
        payload=ApplicationSubmittedPayload(
            application_id="X",
            applicant_id="p1",
            requested_amount_usd=1.0,
            loan_purpose="t",
            submission_channel="web",
            submitted_at="2025-01-01T00:00:00Z",
        )
    )
    await store.append(stream_id=loan_stream, events=[ev, ev], expected_version=-1, correlation_id="c1")

    r1 = await run_integrity_check(
        store, "loan", entity, correlation_id="c2"
    )
    assert r1.tamper_detected is False

    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE events
            SET payload = jsonb_set(payload::jsonb, '{requested_amount_usd}', '9999')
            WHERE stream_id = $1 AND event_type = 'ApplicationSubmitted' AND stream_position = 1
            """,
            loan_stream,
        )

    r2 = await run_integrity_check(
        store, "loan", entity, correlation_id="c3"
    )
    assert r2.tamper_detected is True
