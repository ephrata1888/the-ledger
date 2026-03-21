from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, AsyncIterator, Callable, Dict, Iterable, List, Optional

import asyncpg

from src.models.events import (
    BaseEvent,
    DomainError,
    OptimisticConcurrencyError,
    StoredEvent,
    StreamMetadata,
)


def _aggregate_type_from_stream_id(stream_id: str) -> str:
    # Convention: "{aggregate_type}-{rest...}", e.g. "loan-<uuid>"
    return stream_id.split("-", 1)[0] if "-" in stream_id else stream_id


def _jsonb_to_dict(value: Any) -> Dict[str, Any]:
    """Normalize JSONB values from asyncpg (dict or JSON string) to plain dict."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)


@dataclass(frozen=True)
class EventStoreConfig:
    outbox_destination: str = "event_bus"


class EventStore:
    """
    Infrastructure only.

    Never store PII in the payload without encryption.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        config: EventStoreConfig | None = None,
        upcast: Optional[Callable[[StoredEvent], StoredEvent]] = None,
    ) -> None:
        self._pool = pool
        self._config = config or EventStoreConfig()
        # Phase 4 placeholder: UpcasterRegistry would be applied in load paths
        self._upcast = upcast

    async def append(
        self,
        stream_id: str,
        events: List[BaseEvent],
        expected_version: int,  # -1=new stream; N=exact match required
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> int:
        if not events:
            raise DomainError("append() requires at least one event")

        aggregate_type = _aggregate_type_from_stream_id(stream_id)

        # Build one metadata envelope for this append; callers can add more into payload
        base_metadata: Dict[str, Any] = {}
        if correlation_id is not None:
            base_metadata["correlation_id"] = correlation_id
        if causation_id is not None:
            base_metadata["causation_id"] = causation_id

        # Single atomic transaction: event_streams version gate + events insert + outbox rows + version update
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT current_version, archived_at
                    FROM event_streams
                    WHERE stream_id = $1
                    FOR UPDATE
                    """,
                    stream_id,
                )

                if row is None:
                    if expected_version != -1:
                        raise OptimisticConcurrencyError(
                            f"Stream {stream_id} does not exist; expected_version={expected_version}"
                        )
                    current_version = 0
                    await conn.execute(
                        """
                        INSERT INTO event_streams (stream_id, aggregate_type, current_version, metadata)
                        VALUES ($1, $2, 0, '{}'::jsonb)
                        """,
                        stream_id,
                        aggregate_type,
                    )
                else:
                    if row["archived_at"] is not None:
                        raise DomainError(f"Stream {stream_id} is archived; cannot append")
                    current_version = int(row["current_version"])
                    if expected_version == -1:
                        raise OptimisticConcurrencyError(
                            f"Stream {stream_id} already exists; expected_version=-1 rejected"
                        )

                if current_version != (0 if expected_version == -1 else expected_version):
                    raise OptimisticConcurrencyError(
                        f"Expected version {expected_version}, but current_version is {current_version}"
                    )

                # Prepare JSONB array for single INSERT..SELECT to assign stream_position deterministically.
                event_rows = []
                for ev in events:
                    event_rows.append(
                        {
                            "event_type": ev.event_type,
                            "event_version": ev.event_version,
                            "payload": ev.payload,
                            "metadata": base_metadata,
                        }
                    )

                inserted = await conn.fetch(
                    """
                    WITH input AS (
                      SELECT $2::text[] AS evs
                    ),
                    unpacked AS (
                      SELECT
                        (e->>'event_type')::text AS event_type,
                        (e->>'event_version')::int AS event_version,
                        (e->'payload') AS payload,
                        (e->'metadata') AS metadata,
                        ordinality
                      FROM input,
                           unnest(input.evs) WITH ORDINALITY AS t(e_text, ordinality),
                           LATERAL (SELECT (e_text::jsonb) AS e) AS j
                    ),
                    ins AS (
                      INSERT INTO events (
                        stream_id,
                        stream_position,
                        event_type,
                        event_version,
                        payload,
                        metadata
                      )
                      SELECT
                        $1,
                        $3 + ROW_NUMBER() OVER (ORDER BY ordinality),
                        event_type,
                        event_version,
                        payload,
                        metadata
                      FROM unpacked
                      RETURNING event_id, stream_id, stream_position, global_position, event_type,
                                event_version, payload, metadata, recorded_at
                    )
                    SELECT * FROM ins ORDER BY stream_position ASC
                    """,
                    stream_id,
                    [json.dumps(r) for r in event_rows],
                    current_version,
                )

                # Outbox: one row per stored event, same transaction.
                for r in inserted:
                    outbox_payload = {
                        "event_id": str(r["event_id"]),
                        "stream_id": r["stream_id"],
                        "stream_position": int(r["stream_position"]),
                        "global_position": int(r["global_position"]),
                        "event_type": r["event_type"],
                        "event_version": int(r["event_version"]),
                        "payload": r["payload"],
                        "metadata": r["metadata"],
                        "recorded_at": r["recorded_at"].isoformat(),
                    }
                    await conn.execute(
                        """
                        INSERT INTO outbox (event_id, destination, payload)
                        VALUES ($1, $2, $3::jsonb)
                        """,
                        r["event_id"],
                        self._config.outbox_destination,
                        json.dumps(outbox_payload),
                    )

                new_version = current_version + len(events)
                await conn.execute(
                    """
                    UPDATE event_streams
                    SET current_version = $2
                    WHERE stream_id = $1
                    """,
                    stream_id,
                    new_version,
                )

                return new_version

    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: int | None = None,
    ) -> List[StoredEvent]:
        sql = """
            SELECT event_id, stream_id, stream_position, global_position,
                   event_type, event_version, payload, metadata, recorded_at
            FROM events
            WHERE stream_id = $1 AND stream_position > $2
        """
        params: list[Any] = [stream_id, from_position]
        if to_position is not None:
            sql += " AND stream_position <= $3"
            params.append(to_position)
        sql += " ORDER BY stream_position ASC"

        async with self._pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        out: List[StoredEvent] = []
        for r in rows:
            ev = StoredEvent(
                event_id=r["event_id"],
                stream_id=r["stream_id"],
                stream_position=int(r["stream_position"]),
                global_position=int(r["global_position"]),
                event_type=r["event_type"],
                event_version=int(r["event_version"]),
                payload=_jsonb_to_dict(r["payload"]),
                metadata=_jsonb_to_dict(r["metadata"]),
                recorded_at=r["recorded_at"],
            )
            if self._upcast is not None:
                ev = self._upcast(ev)
            out.append(ev)
        return out

    async def load_all(
        self,
        from_global_position: int = 0,
        event_types: List[str] | None = None,
        batch_size: int = 500,
    ) -> AsyncIterator[StoredEvent]:
        if batch_size <= 0:
            raise ValueError("batch_size must be > 0")

        async with self._pool.acquire() as conn:
            last = from_global_position
            while True:
                if event_types:
                    rows = await conn.fetch(
                        """
                        SELECT event_id, stream_id, stream_position, global_position,
                               event_type, event_version, payload, metadata, recorded_at
                        FROM events
                        WHERE global_position > $1
                          AND event_type = ANY($2::text[])
                        ORDER BY global_position ASC
                        LIMIT $3
                        """,
                        last,
                        event_types,
                        batch_size,
                    )
                else:
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
                        batch_size,
                    )
                if not rows:
                    return
                for r in rows:
                    ev = StoredEvent(
                        event_id=r["event_id"],
                        stream_id=r["stream_id"],
                        stream_position=int(r["stream_position"]),
                        global_position=int(r["global_position"]),
                        event_type=r["event_type"],
                        event_version=int(r["event_version"]),
                        payload=_jsonb_to_dict(r["payload"]),
                        metadata=_jsonb_to_dict(r["metadata"]),
                        recorded_at=r["recorded_at"],
                    )
                    if self._upcast is not None:
                        ev = self._upcast(ev)
                    last = ev.global_position
                    yield ev

                # next batch continues after the highest global_position observed
                # (last is updated inside the loop)

    async def stream_version(self, stream_id: str) -> int:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT current_version FROM event_streams WHERE stream_id = $1",
                stream_id,
            )
        return int(row["current_version"]) if row else 0

    async def archive_stream(self, stream_id: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE event_streams SET archived_at = NOW() WHERE stream_id = $1",
                stream_id,
            )

    async def get_stream_metadata(self, stream_id: str) -> StreamMetadata:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT stream_id, aggregate_type, current_version, created_at, archived_at, metadata
                FROM event_streams
                WHERE stream_id = $1
                """,
                stream_id,
            )
        if row is None:
            raise DomainError(f"Stream {stream_id} not found")
        return StreamMetadata(
            stream_id=row["stream_id"],
            aggregate_type=row["aggregate_type"],
            current_version=int(row["current_version"]),
            created_at=row["created_at"],
            archived_at=row["archived_at"],
            metadata=_jsonb_to_dict(row["metadata"]),
        )

