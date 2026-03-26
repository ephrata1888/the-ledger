"""AuditLedgerAggregate: cross-stream timeline, hash chain, tamper detection."""

from uuid import uuid4

import asyncpg
import pytest

from src.aggregates.audit_ledger import AuditLedgerAggregate, AuditViolationError
from src.event_store import EventStore
from src.models.events import ApplicationSubmittedEvent, ApplicationSubmittedPayload


@pytest.mark.asyncio
async def test_load_empty_audit_ledger_has_version_minus_one(pool: asyncpg.Pool) -> None:
    store = EventStore(pool)
    aid = str(uuid4())
    agg = await AuditLedgerAggregate.load(store, "loan", aid)
    assert agg.version == -1
    assert agg.chain_valid is True
    assert agg.last_integrity_hash is None


@pytest.mark.asyncio
async def test_record_integrity_check_twice_chains_previous_hash(pool: asyncpg.Pool) -> None:
    store = EventStore(pool)
    aid = str(uuid4())
    loan_stream = f"loan-{aid}"

    ev = ApplicationSubmittedEvent(
        payload=ApplicationSubmittedPayload(
            application_id=aid,
            applicant_id="p1",
            requested_amount_usd=100_000.0,
            loan_purpose="working_capital",
            submission_channel="web",
            submitted_at="2025-01-01T00:00:00Z",
        )
    )
    await store.append(stream_id=loan_stream, events=[ev], expected_version=-1, correlation_id="c1")

    agg = await AuditLedgerAggregate.load(store, "loan", aid)
    r1 = await agg.record_integrity_check(store, triggered_by="test")
    assert r1.tamper_detected is False
    assert r1.chain_valid is True
    assert r1.previous_hash is None
    assert agg.version == 1

    r2 = await agg.record_integrity_check(store, triggered_by="test")
    assert r2.tamper_detected is False
    assert r2.previous_hash == r1.new_hash

    stored = await store.load_stream(agg.stream_id())
    assert len(stored) == 2
    p2 = stored[1].payload
    assert p2.get("previous_hash") == r1.new_hash


@pytest.mark.asyncio
async def test_assert_append_only_blocks_after_detected_tamper(pool: asyncpg.Pool) -> None:
    store = EventStore(pool)
    aid = str(uuid4())
    loan_stream = f"loan-{aid}"

    ev = ApplicationSubmittedEvent(
        payload=ApplicationSubmittedPayload(
            application_id=aid,
            applicant_id="p1",
            requested_amount_usd=25_000.0,
            loan_purpose="equipment",
            submission_channel="web",
            submitted_at="2025-03-01T00:00:00Z",
        )
    )
    await store.append(stream_id=loan_stream, events=[ev], expected_version=-1, correlation_id="c-block")

    agg = await AuditLedgerAggregate.load(store, "loan", aid)
    await agg.record_integrity_check(store, triggered_by="first")
    audit_sid = agg.stream_id()
    assert len(await store.load_stream(audit_sid)) == 1

    agg.chain_valid = False
    with pytest.raises(AuditViolationError):
        await agg.record_integrity_check(store, triggered_by="second")

    assert len(await store.load_stream(audit_sid)) == 1


@pytest.mark.asyncio
async def test_tamper_detected_and_append_only_raises(pool: asyncpg.Pool) -> None:
    store = EventStore(pool)
    aid = str(uuid4())
    loan_stream = f"loan-{aid}"

    ev = ApplicationSubmittedEvent(
        payload=ApplicationSubmittedPayload(
            application_id=aid,
            applicant_id="p1",
            requested_amount_usd=50_000.0,
            loan_purpose="inventory",
            submission_channel="branch",
            submitted_at="2025-02-01T00:00:00Z",
        )
    )
    await store.append(stream_id=loan_stream, events=[ev], expected_version=-1, correlation_id="cx")

    agg = await AuditLedgerAggregate.load(store, "loan", aid)
    await agg.record_integrity_check(store, triggered_by="sched")

    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE events
            SET payload = jsonb_set(payload::jsonb, '{requested_amount_usd}', '999999')
            WHERE stream_id = $1 AND event_type = 'ApplicationSubmitted' AND stream_position = 1
            """,
            loan_stream,
        )

    agg2 = await AuditLedgerAggregate.load(store, "loan", aid)
    r2 = await agg2.record_integrity_check(store, triggered_by="sched")
    assert r2.tamper_detected is True
    assert r2.chain_valid is False

    agg3 = await AuditLedgerAggregate.load(store, "loan", aid)
    with pytest.raises(AuditViolationError):
        await agg3.record_integrity_check(store, triggered_by="blocked")


@pytest.mark.asyncio
async def test_load_cross_stream_timeline_includes_correlated_stream(pool: asyncpg.Pool) -> None:
    store = EventStore(pool)
    aid = str(uuid4())
    loan_stream = f"loan-{aid}"
    agent_stream = f"agent-{uuid4()}"

    ev1 = ApplicationSubmittedEvent(
        payload=ApplicationSubmittedPayload(
            application_id=aid,
            applicant_id="p1",
            requested_amount_usd=10_000.0,
            loan_purpose="t",
            submission_channel="web",
            submitted_at="2025-01-01T00:00:00Z",
        )
    )
    await store.append(stream_id=loan_stream, events=[ev1], expected_version=-1, correlation_id="corr-x")

    ev2 = ApplicationSubmittedEvent(
        payload=ApplicationSubmittedPayload(
            application_id=aid,
            applicant_id="p1",
            requested_amount_usd=10_000.0,
            loan_purpose="t",
            submission_channel="web",
            submitted_at="2025-01-01T00:00:01Z",
        )
    )
    await store.append(
        stream_id=agent_stream,
        events=[ev2],
        expected_version=-1,
        correlation_id="corr-x",
    )

    agg = await AuditLedgerAggregate.load(store, "loan", aid)
    timeline = await agg.load_cross_stream_timeline(store)
    streams = {e.stream_id for e in timeline}
    assert loan_stream in streams
    assert agent_stream in streams
    assert all(not e.stream_id.startswith("audit-") for e in timeline)


@pytest.mark.asyncio
async def test_get_causal_chain_follows_metadata(pool: asyncpg.Pool) -> None:
    store = EventStore(pool)
    aid = str(uuid4())
    loan_stream = f"loan-{aid}"

    ev1 = ApplicationSubmittedEvent(
        payload=ApplicationSubmittedPayload(
            application_id=aid,
            applicant_id="p1",
            requested_amount_usd=1.0,
            loan_purpose="t",
            submission_channel="web",
            submitted_at="2025-01-01T00:00:00Z",
        )
    )
    await store.append(stream_id=loan_stream, events=[ev1], expected_version=-1, correlation_id="c0")

    loan_evs = await store.load_stream(loan_stream)
    root_id = str(loan_evs[0].event_id)

    ev2 = ApplicationSubmittedEvent(
        payload=ApplicationSubmittedPayload(
            application_id=aid,
            applicant_id="p1",
            requested_amount_usd=2.0,
            loan_purpose="t",
            submission_channel="web",
            submitted_at="2025-01-01T00:00:05Z",
        )
    )
    await store.append(
        stream_id=loan_stream,
        events=[ev2],
        expected_version=1,
        causation_id=root_id,
        correlation_id="c0",
    )

    agg = await AuditLedgerAggregate.load(store, "loan", aid)
    chain = await agg.get_causal_chain(store, root_event_id=root_id, max_depth=5)
    assert len(chain) == 1
    assert chain[0].metadata.get("causation_id") == root_id
