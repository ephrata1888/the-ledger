from uuid import uuid4

import asyncpg
import pytest

from src.event_store import EventStore
from src.models.events import (
    ApplicationSubmittedEvent,
    ApplicationSubmittedPayload,
    CreditAnalysisCompletedEvent,
    CreditAnalysisCompletedPayload,
    DomainError,
)


@pytest.mark.asyncio
async def test_append_new_stream_and_load_stream(pool: asyncpg.Pool):
    store = EventStore(pool)
    stream_id = f"loan-{uuid4()}"

    new_version = await store.append(
        stream_id=stream_id,
        events=[
            ApplicationSubmittedEvent(
                payload=ApplicationSubmittedPayload(
                    application_id="A1",
                    applicant_id="p1",
                    requested_amount_usd=5000.0,
                    loan_purpose="x",
                    submission_channel="web",
                    submitted_at="2025-01-01T00:00:00Z",
                )
            ),
        ],
        expected_version=-1,
        correlation_id="C1",
        causation_id=None,
    )
    assert new_version == 1

    loaded = await store.load_stream(stream_id)
    assert len(loaded) == 1
    assert loaded[0].event_type == "ApplicationSubmitted"
    assert loaded[0].stream_position == 1
    assert loaded[0].global_position >= 1
    assert loaded[0].metadata["correlation_id"] == "C1"

    version = await store.stream_version(stream_id)
    assert version == 1


@pytest.mark.asyncio
async def test_outbox_written_in_same_transaction(pool: asyncpg.Pool):
    store = EventStore(pool)
    stream_id = f"loan-{uuid4()}"

    await store.append(
        stream_id=stream_id,
        events=[
            ApplicationSubmittedEvent(
                payload=ApplicationSubmittedPayload(
                    application_id="A1",
                    applicant_id="p1",
                    requested_amount_usd=1.0,
                    loan_purpose="x",
                    submission_channel="web",
                    submitted_at="2025-01-01T00:00:00Z",
                )
            )
        ],
        expected_version=-1,
    )

    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM outbox")
        assert count == 1


@pytest.mark.asyncio
async def test_archive_prevents_append(pool: asyncpg.Pool):
    store = EventStore(pool)
    stream_id = f"loan-{uuid4()}"

    await store.append(
        stream_id=stream_id,
        events=[
            ApplicationSubmittedEvent(
                payload=ApplicationSubmittedPayload(
                    application_id="A1",
                    applicant_id="p1",
                    requested_amount_usd=1.0,
                    loan_purpose="x",
                    submission_channel="web",
                    submitted_at="2025-01-01T00:00:00Z",
                )
            )
        ],
        expected_version=-1,
    )
    await store.archive_stream(stream_id)

    with pytest.raises(DomainError) as exc:
        await store.append(
            stream_id=stream_id,
            events=[
                CreditAnalysisCompletedEvent(
                    payload=CreditAnalysisCompletedPayload(
                        application_id="A1",
                        agent_id="a1",
                        risk_tier="LOW",
                    )
                )
            ],
            expected_version=1,
        )
    assert exc.value.error_code == "STREAM_ARCHIVED"
