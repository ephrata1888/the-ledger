"""Gas Town agent context reconstruction."""

from uuid import uuid4

import asyncpg
import pytest

from src.event_store import EventStore
from src.integrity.gas_town import reconstruct_agent_context
from src.models.events import (
    AgentContextLoadedEvent,
    AgentContextLoadedPayload,
    AgentNodeExecutedEvent,
    AgentNodeExecutedPayload,
    AgentOutputWrittenEvent,
    AgentOutputWrittenPayload,
)


@pytest.mark.asyncio
async def test_reconstruct_after_five_events_continues_without_lost_work(pool: asyncpg.Pool):
    store = EventStore(pool)
    agent_id = "agent-x"
    session_id = f"s-{uuid4()}"
    sid = f"agent-{agent_id}-{session_id}"

    events = [
        AgentContextLoadedEvent(
            payload=AgentContextLoadedPayload(
                agent_id=agent_id,
                session_id=session_id,
                context_source="boot",
                model_version="v1",
            )
        ),
        AgentNodeExecutedEvent(
            payload=AgentNodeExecutedPayload(node_id="n1", status="ok", detail="")
        ),
        AgentOutputWrittenEvent(
            payload=AgentOutputWrittenPayload(node_id="n1", output_id="o1", content_ref="ref1")
        ),
        AgentNodeExecutedEvent(
            payload=AgentNodeExecutedPayload(node_id="n2", status="partial", detail="running")
        ),
        AgentNodeExecutedEvent(
            payload=AgentNodeExecutedPayload(node_id="n2", status="partial", detail="still")
        ),
    ]

    v = -1
    for e in events:
        v = await store.append(
            stream_id=sid,
            events=[e],
            expected_version=v,
            correlation_id="gt1",
        )

    ctx = await reconstruct_agent_context(store, agent_id, session_id, token_budget=8000)
    assert len(ctx.verbatim_recent) + len(ctx.verbatim_priority) >= 3
    assert ctx.needs_reconciliation is True
    assert any("n2" in p for p in ctx.pending_work)
    assert ctx.summary_prose
