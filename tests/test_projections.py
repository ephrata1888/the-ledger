"""
Projection daemon + read models (Phase 3).

Requires PostgreSQL (DATABASE_URL).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import asyncpg
import pytest

from src.event_store import EventStore
from src.models.events import ApplicationSubmittedEvent, ApplicationSubmittedPayload
from src.projections import (
    ApplicationSummaryProjection,
    ComplianceAuditProjection,
    ComplianceAuditView,
    ProjectionDaemon,
    run_catch_up,
)


@pytest.mark.asyncio
async def test_application_summary_upsert_from_loan_stream(pool: asyncpg.Pool):
    store = EventStore(pool)
    app_id = f"test-{uuid4()}"
    stream = f"loan-{app_id}"
    ev = ApplicationSubmittedEvent(
        payload=ApplicationSubmittedPayload(
            application_id=app_id,
            applicant_id="p1",
            requested_amount_usd=1000.0,
            loan_purpose="x",
            submission_channel="web",
            submitted_at="2025-01-01T00:00:00Z",
        )
    )
    await store.append(stream_id=stream, events=[ev], expected_version=-1)

    h = ApplicationSummaryProjection()
    d = ProjectionDaemon(pool, batch_size=50)
    n = await d.run_once(h)
    assert n == 1

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM projection_application_summary WHERE application_id = $1", app_id
        )
    assert row is not None
    assert row["state"] == "SUBMITTED"
    assert float(row["requested_amount_usd"]) == 1000.0


@pytest.mark.asyncio
async def test_compliance_audit_rebuild_matches_incremental_for_1847_seed_events(pool: asyncpg.Pool):
    """
    After bulk-inserting 1,847 synthetic events (same count as datagen), incremental catch-up
    and rebuild_from_scratch must both yield the same checkpoint == MAX(global_position).
    """
    n = 1847
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO events (stream_id, stream_position, event_type, event_version, payload, metadata)
            SELECT
              'seed-rebuild-test',
              gs,
              'SyntheticSeedEvent',
              1,
              jsonb_build_object('seq', gs, 'stream_shard', 0, 'batch', 'test'),
              '{}'::jsonb
            FROM generate_series(1, $1) AS gs
            """,
            n,
        )
        max_gp = await conn.fetchval("SELECT MAX(global_position) FROM events")
        assert max_gp == n

    comp = ComplianceAuditProjection()
    daemon = ProjectionDaemon(pool, batch_size=500)
    await run_catch_up(pool, comp, batch_size=500)

    async with pool.acquire() as conn:
        cp_after = await conn.fetchval(
            "SELECT last_position FROM projection_checkpoints WHERE projection_name = $1",
            comp.projection_name,
        )
        assert int(cp_after or 0) == n

    view = ComplianceAuditView(pool)
    await view.rebuild_from_scratch(comp, daemon)

    async with pool.acquire() as conn:
        cp_re = await conn.fetchval(
            "SELECT last_position FROM projection_checkpoints WHERE projection_name = $1",
            comp.projection_name,
        )
        assert int(cp_re or 0) == n


@pytest.mark.asyncio
async def test_compliance_audit_temporal_query(pool: asyncpg.Pool):
    """Insert a compliance stream event and verify get_current_compliance returns state."""
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO events (stream_id, stream_position, event_type, event_version, payload, metadata)
            VALUES (
              'compliance-app1', 1, 'ComplianceCheckRequested', 1,
              '{"application_id": "app1", "regulation_set_version": "v1", "checks_required": ["R1"]}'::jsonb,
              '{}'::jsonb
            )
            """
        )

    comp = ComplianceAuditProjection()
    daemon = ProjectionDaemon(pool, batch_size=10)
    await daemon.run_once(comp)

    view = ComplianceAuditView(pool)
    cur = await view.get_current_compliance("app1")
    assert cur is not None
    assert cur["compliance_state"]["overall_status"] == "IN_PROGRESS"

    at = datetime(2025, 1, 1, tzinfo=timezone.utc)
    past = await view.get_compliance_at("app1", at)
    assert past is None or "compliance_state" in past


@pytest.mark.asyncio
async def test_slo_lag_under_concurrent_load(pool: asyncpg.Pool):
    """
    Smoke: 50 concurrent appends to distinct streams; catch up projections.
    Asserts caught-up lag (event-time) within SLO bands after full processing.
    """
    store = EventStore(pool)

    async def one_append(i: int):
        aid = f"slo-{i}-{uuid4()}"
        ev = ApplicationSubmittedEvent(
            payload=ApplicationSubmittedPayload(
                application_id=aid,
                applicant_id="p1",
                requested_amount_usd=1.0,
                loan_purpose="x",
                submission_channel="web",
                submitted_at="2025-01-01T00:00:00Z",
            )
        )
        await store.append(
            stream_id=f"loan-{aid}",
            events=[ev],
            expected_version=-1,
            correlation_id=f"corr-{i}",
        )

    await asyncio.gather(*[one_append(i) for i in range(50)])

    app = ApplicationSummaryProjection()
    comp = ComplianceAuditProjection()
    d = ProjectionDaemon(pool, batch_size=100)
    await run_catch_up(pool, app, batch_size=200)
    await run_catch_up(pool, comp, batch_size=200)

    lag_app = await d.get_lag(ApplicationSummaryProjection.projection_name)
    lag_c = await d.get_lag(ComplianceAuditProjection.projection_name)

    assert lag_app.events_behind == 0
    assert lag_c.events_behind == 0
    assert lag_app.lag_ms < 500.0
    assert lag_c.lag_ms < 2000.0
