import asyncio
from uuid import uuid4

import asyncpg
import pytest

from src.event_store import EventStore
from src.models.events import BaseEvent, OptimisticConcurrencyError


@pytest.mark.asyncio
async def test_double_decision_occ_collision(pool: asyncpg.Pool):
    """
    Mandatory Phase 1 test:
    - Two concurrent appends at expected_version=3
    - Exactly one succeeds, one raises OptimisticConcurrencyError
    - Final stream length is 4
    """
    store = EventStore(pool)
    stream_id = f"loan-{uuid4()}"

    # Seed the stream to version 3
    await store.append(
        stream_id=stream_id,
        expected_version=-1,
        events=[BaseEvent(event_type="ApplicationSubmitted", payload={"application_id": "A1"})],
    )
    await store.append(
        stream_id=stream_id,
        expected_version=1,
        events=[BaseEvent(event_type="CreditAnalysisRequested", payload={"application_id": "A1"})],
    )
    await store.append(
        stream_id=stream_id,
        expected_version=2,
        events=[BaseEvent(event_type="FraudScreeningCompleted", payload={"application_id": "A1", "fraud_score": 0.1})],
    )
    assert await store.stream_version(stream_id) == 3

    async def try_append(agent_id: str):
        # Each append() calls pool.acquire() internally; concurrent tasks use
        # distinct connections when the pool has min_size>=2.
        return await store.append(
            stream_id=stream_id,
            expected_version=3,
            events=[
                BaseEvent(
                    event_type="CreditAnalysisCompleted",
                    payload={"application_id": "A1", "agent_id": agent_id, "risk_tier": "MEDIUM"},
                )
            ],
        )

    results = await asyncio.gather(
        try_append("agent-A"),
        try_append("agent-B"),
        return_exceptions=True,
    )

    successes = [r for r in results if not isinstance(r, Exception)]
    failures = [r for r in results if isinstance(r, Exception)]

    assert len(successes) == 1
    assert len(failures) == 1
    assert isinstance(failures[0], OptimisticConcurrencyError)

    final_events = await store.load_stream(stream_id)
    assert len(final_events) == 4

