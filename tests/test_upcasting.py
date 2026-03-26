"""
Upcasting: read-time v1 -> v2 without mutating persisted rows.
"""

import json
from uuid import uuid4

import asyncpg
import pytest

from src.event_store import EventStore
from src.models.events import (
    AgentContextLoadedEvent,
    AgentContextLoadedPayload,
    CreditAnalysisCompletedEvent,
    CreditAnalysisCompletedPayload,
    DecisionGeneratedEvent,
    DecisionGeneratedPayload,
)
from src.upcasting.registry import default_registry


@pytest.mark.asyncio
async def test_credit_analysis_v1_row_unchanged_upcast_returns_v2(pool: asyncpg.Pool):
    reg = default_registry()
    store = EventStore(pool, upcaster_registry=reg)

    payload_v1 = {
        "application_id": "A1",
        "agent_id": "ag1",
        "risk_tier": "LOW",
    }
    stream = f"loan-{uuid4()}"

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO event_streams (stream_id, aggregate_type, current_version, metadata)
            VALUES ($1, 'loan', 1, '{}'::jsonb)
            """,
            stream,
        )
        await conn.execute(
            """
            INSERT INTO events (stream_id, stream_position, event_type, event_version, payload, metadata)
            VALUES ($1, 1, 'CreditAnalysisCompleted', 1, $2::jsonb, '{}'::jsonb)
            """,
            stream,
            json.dumps(payload_v1),
        )

    loaded = await store.load_stream(stream)
    assert len(loaded) == 1
    ev = loaded[0]
    assert ev.event_version == 2
    assert ev.payload.get("model_version") is not None
    assert ev.payload.get("confidence_score") is None
    assert ev.payload.get("regulatory_basis") is not None

    async with pool.acquire() as conn:
        raw = await conn.fetchrow(
            "SELECT event_version, payload FROM events WHERE stream_id = $1", stream
        )
    assert int(raw["event_version"]) == 1
    pl = raw["payload"]
    if isinstance(pl, str):
        pl = json.loads(pl)
    assert pl["risk_tier"] == "LOW"
    assert "model_version" not in pl


@pytest.mark.asyncio
async def test_decision_generated_v2_model_versions_from_agent_streams(pool: asyncpg.Pool):
    reg = default_registry()
    store = EventStore(pool, upcaster_registry=reg)

    session_key = "ag99-sess1"
    agent_stream = f"agent-{session_key}"
    loan_stream = f"loan-{uuid4()}"

    await store.append(
        stream_id=agent_stream,
        events=[
            AgentContextLoadedEvent(
                payload=AgentContextLoadedPayload(
                    agent_id="ag99",
                    session_id="sess1",
                    context_source="test",
                    model_version="v2.3",
                )
            )
        ],
        expected_version=-1,
        correlation_id="c1",
    )

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO event_streams (stream_id, aggregate_type, current_version, metadata)
            VALUES ($1, 'loan', 1, '{}'::jsonb)
            """,
            loan_stream,
        )
        await conn.execute(
            """
            INSERT INTO events (stream_id, stream_position, event_type, event_version, payload, metadata)
            VALUES ($1, 1, 'DecisionGenerated', 1, $2::jsonb, '{}'::jsonb)
            """,
            loan_stream,
            json.dumps(
                {
                    "application_id": "A1",
                    "orchestrator_agent_id": "orch",
                    "recommendation": "REFER",
                    "confidence_score": 0.7,
                    "contributing_agent_sessions": [session_key],
                }
            ),
        )

    out = await store.load_stream(loan_stream)
    assert out[0].event_version == 2
    assert out[0].payload["model_versions"][session_key] == "v2.3"
