"""
Async ProjectionDaemon: poll events by global_position, checkpoint atomically with projection writes.

Advisory lock: pg_try_advisory_lock(87214, hashtext(projection_name)::int).

Never store PII in logs without encryption.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Protocol, runtime_checkable

import asyncpg

from src.models.events import StoredEvent

logger = logging.getLogger(__name__)

ADVISORY_NS = 87214


@dataclass(frozen=True)
class LagMetrics:
    projection_name: str
    events_behind: int
    lag_ms: float
    head_global_position: int
    checkpoint_position: int


@runtime_checkable
class ProjectionHandler(Protocol):
    projection_name: str

    async def apply(self, conn: asyncpg.Connection, ev: StoredEvent) -> None:
        ...


def row_to_stored(r: asyncpg.Record) -> StoredEvent:
    pl = r["payload"]
    md = r["metadata"]
    if not isinstance(pl, dict):
        pl = json.loads(pl) if isinstance(pl, str) else dict(pl)
    if not isinstance(md, dict):
        md = json.loads(md) if isinstance(md, str) else dict(md)
    return StoredEvent(
        event_id=r["event_id"],
        stream_id=r["stream_id"],
        stream_position=int(r["stream_position"]),
        global_position=int(r["global_position"]),
        event_type=r["event_type"],
        event_version=int(r["event_version"]),
        payload=pl,
        metadata=md,
        recorded_at=r["recorded_at"],
    )


class ProjectionDaemon:
    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        batch_size: int = 200,
        max_retries: int = 3,
        poll_interval_s: float = 0.05,
    ) -> None:
        self._pool = pool
        self._batch_size = batch_size
        self._max_retries = max_retries
        self._poll_interval_s = poll_interval_s

    async def ensure_checkpoint(self, conn: asyncpg.Connection, projection_name: str) -> None:
        await conn.execute(
            """
            INSERT INTO projection_checkpoints (projection_name, last_position, updated_at)
            VALUES ($1, 0, NOW())
            ON CONFLICT (projection_name) DO NOTHING
            """,
            projection_name,
        )

    async def run_once(self, handler: ProjectionHandler) -> int:
        """
        Process up to batch_size events in one atomic transaction (checkpoint last GP).
        Per-event failures use SAVEPOINT + retry, then dead-letter without aborting the batch.
        """
        name = handler.projection_name
        processed = 0
        async with self._pool.acquire() as conn:
            locked = await conn.fetchval(
                "SELECT pg_try_advisory_lock($1::int, hashtext($2)::int)",
                ADVISORY_NS,
                name,
            )
            if not locked:
                logger.debug("projection %s: advisory lock not acquired; skip tick", name)
                return 0
            try:
                await self.ensure_checkpoint(conn, name)
                row = await conn.fetchrow(
                    "SELECT last_position FROM projection_checkpoints WHERE projection_name = $1",
                    name,
                )
                last = int(row["last_position"]) if row else 0

                rows = await conn.fetch(
                    """
                    SELECT event_id, stream_id, stream_position, global_position,
                           event_type, event_version, payload, metadata, recorded_at
                    FROM events
                    WHERE global_position > $1
                    ORDER BY global_position ASC
                    LIMIT $2
                    """,
                    last,
                    self._batch_size,
                )
                if not rows:
                    return 0

                last_gp = last
                async with conn.transaction():
                    for r in rows:
                        ev = row_to_stored(r)
                        done = False
                        last_err: Optional[BaseException] = None
                        for attempt in range(self._max_retries + 1):
                            try:
                                await conn.execute("SAVEPOINT proj_sp")
                                await handler.apply(conn, ev)
                                await conn.execute("RELEASE SAVEPOINT proj_sp")
                                done = True
                                break
                            except Exception as exc:  # noqa: BLE001
                                last_err = exc
                                await conn.execute("ROLLBACK TO SAVEPOINT proj_sp")
                                logger.warning(
                                    "projection %s attempt %s gp=%s: %s",
                                    name,
                                    attempt,
                                    ev.global_position,
                                    exc,
                                )
                        if not done:
                            assert last_err is not None
                            await conn.execute(
                                """
                                INSERT INTO projection_dead_letter (
                                  projection_name, global_position, event_type, stream_id, error_message
                                )
                                VALUES ($1, $2, $3, $4, $5)
                                """,
                                name,
                                ev.global_position,
                                ev.event_type,
                                ev.stream_id,
                                str(last_err)[:8000],
                            )
                            logger.error(
                                "projection %s dead-letter gp=%s: %s",
                                name,
                                ev.global_position,
                                last_err,
                            )

                        last_gp = ev.global_position
                        processed += 1

                    await conn.execute(
                        """
                        UPDATE projection_checkpoints
                        SET last_position = $2, updated_at = NOW()
                        WHERE projection_name = $1
                        """,
                        name,
                        last_gp,
                    )
            finally:
                await conn.execute(
                    "SELECT pg_advisory_unlock($1::int, hashtext($2)::int)",
                    ADVISORY_NS,
                    name,
                )

        return processed

    async def run_forever(
        self,
        handlers: list[ProjectionHandler],
        *,
        stop_event: Optional[asyncio.Event] = None,
    ) -> None:
        while stop_event is None or not stop_event.is_set():
            for h in handlers:
                await self.run_once(h)
            await asyncio.sleep(self._poll_interval_s)

    def start_background(
        self,
        handlers: list[ProjectionHandler],
        *,
        stop_event: Optional[asyncio.Event] = None,
    ) -> asyncio.Task[None]:
        return asyncio.create_task(self.run_forever(handlers, stop_event=stop_event))

    async def get_lag(self, projection_name: str) -> LagMetrics:
        """
        events_behind: head global_position minus checkpoint last_position.
        lag_ms: time between head event recorded_at and checkpoint event recorded_at;
                if checkpoint is 0 but events exist, uses now - head recorded_at (catch-up lag).
        """
        async with self._pool.acquire() as conn:
            await self.ensure_checkpoint(conn, projection_name)
            head = await conn.fetchrow(
                "SELECT MAX(global_position) AS gp, MAX(recorded_at) AS rt FROM events"
            )
            cp_row = await conn.fetchrow(
                "SELECT last_position FROM projection_checkpoints WHERE projection_name = $1",
                projection_name,
            )
            checkpoint = int(cp_row["last_position"]) if cp_row else 0
            head_gp = int(head["gp"] or 0) if head else 0
            events_behind = max(0, head_gp - checkpoint)

            head_rt: Optional[datetime] = head["rt"] if head else None
            lag_ms = 0.0
            if head_gp == 0:
                lag_ms = 0.0
            elif checkpoint == 0 and head_rt:
                lag_ms = max(
                    0.0,
                    (datetime.now(timezone.utc) - head_rt.replace(tzinfo=timezone.utc)).total_seconds()
                    * 1000
                    if head_rt.tzinfo is None
                    else (datetime.now(timezone.utc) - head_rt).total_seconds() * 1000,
                )
            elif checkpoint > 0 and head_rt:
                tail = await conn.fetchrow(
                    "SELECT recorded_at FROM events WHERE global_position = $1",
                    checkpoint,
                )
                tail_rt = tail["recorded_at"] if tail else None
                if tail_rt and head_rt:
                    h = head_rt.replace(tzinfo=timezone.utc) if head_rt.tzinfo is None else head_rt
                    t = tail_rt.replace(tzinfo=timezone.utc) if tail_rt.tzinfo is None else tail_rt
                    lag_ms = max(0.0, (h - t).total_seconds() * 1000)

        return LagMetrics(
            projection_name=projection_name,
            events_behind=events_behind,
            lag_ms=lag_ms,
            head_global_position=head_gp,
            checkpoint_position=checkpoint,
        )

    async def get_all_lags(self) -> list[LagMetrics]:
        """Watchdog: lag metrics for all registered Phase 3 projection names."""
        names = [
            "ApplicationSummary",
            "AgentPerformanceLedger",
            "ComplianceAuditView",
        ]
        return [await self.get_lag(n) for n in names]

    async def get_rebuild_lag(self, green_checkpoint_name: str) -> LagMetrics:
        """Lag for a green rebuild checkpoint row (e.g. ``ApplicationSummary__green`` vs events head)."""
        return await self.get_lag(green_checkpoint_name)

    async def rebuild_application_summary_from_scratch(
        self,
        *,
        lag_slo_ms: float = 500.0,
        batch_sleep_s: float = 0.02,
        batch_size: int = 500,
    ) -> None:
        from src.projections.blue_green import rebuild_application_summary_blue_green

        await rebuild_application_summary_blue_green(
            self._pool,
            self,
            lag_slo_ms=lag_slo_ms,
            batch_sleep_s=batch_sleep_s,
            batch_size=batch_size,
        )

    async def rebuild_agent_performance_from_scratch(
        self,
        *,
        lag_slo_ms: float = 500.0,
        batch_sleep_s: float = 0.02,
        batch_size: int = 500,
    ) -> None:
        from src.projections.blue_green import rebuild_agent_performance_blue_green

        await rebuild_agent_performance_blue_green(
            self._pool,
            self,
            lag_slo_ms=lag_slo_ms,
            batch_sleep_s=batch_sleep_s,
            batch_size=batch_size,
        )


async def run_catch_up(
    pool: asyncpg.Pool,
    handler: ProjectionHandler,
    *,
    batch_size: int = 500,
    max_rounds: int = 100_000,
) -> None:
    d = ProjectionDaemon(pool, batch_size=batch_size)
    for _ in range(max_rounds):
        n = await d.run_once(handler)
        if n == 0:
            break
