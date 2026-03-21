import asyncio
from uuid import uuid4

import asyncpg
import pytest

from src.event_store import EventStore
from src.models.events import (
    ApplicationSubmittedEvent,
    ApplicationSubmittedPayload,
    CreditAnalysisCompletedEvent,
    CreditAnalysisCompletedPayload,
    CreditAnalysisRequestedEvent,
    CreditAnalysisRequestedPayload,
    FraudScreeningCompletedEvent,
    FraudScreeningCompletedPayload,
    OptimisticConcurrencyError,
)


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

    await store.append(
        stream_id=stream_id,
        expected_version=-1,
        events=[
            ApplicationSubmittedEvent(
                payload=ApplicationSubmittedPayload(
                    application_id="A1",
                    applicant_id="p1",
                    requested_amount_usd=10_000.0,
                    loan_purpose="test",
                    submission_channel="api",
                    submitted_at="2025-01-01T00:00:00Z",
                )
            )
        ],
    )
    await store.append(
        stream_id=stream_id,
        expected_version=1,
        events=[
            CreditAnalysisRequestedEvent(
                payload=CreditAnalysisRequestedPayload(
                    application_id="A1",
                    assigned_agent_id="ag-1",
                    requested_at="2025-01-01T00:00:01Z",
                    priority="normal",
                )
            )
        ],
    )
    await store.append(
        stream_id=stream_id,
        expected_version=2,
        events=[
            FraudScreeningCompletedEvent(
                payload=FraudScreeningCompletedPayload(
                    application_id="A1",
                    fraud_score=0.1,
                )
            )
        ],
    )
    assert await store.stream_version(stream_id) == 3

    async def try_append(agent_id: str):
        return await store.append(
            stream_id=stream_id,
            expected_version=3,
            events=[
                CreditAnalysisCompletedEvent(
                    payload=CreditAnalysisCompletedPayload(
                        application_id="A1",
                        agent_id=agent_id,
                        risk_tier="MEDIUM",
                    )
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
    occ = failures[0]
    assert occ.stream_id == stream_id
    assert occ.expected_version == 3

    final_events = await store.load_stream(stream_id)
    assert len(final_events) == 4
