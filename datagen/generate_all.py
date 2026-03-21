"""
Synthetic event seed for load / integration scenarios.

Appends exactly 1,847 events across 40 streams (seed-0 … seed-39) using the
canonical EventStore (OCC + outbox), so global ordering and constraints match production.

Never store PII in the event payload without encryption.

Prerequisites: DATABASE_URL set, schema applied (src/schema.sql), database initialized.
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import asyncpg
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

load_dotenv(REPO_ROOT / ".env")

from src.event_store import EventStore  # noqa: E402
from src.models.events import SyntheticSeedEvent, SyntheticSeedPayload  # noqa: E402

TOTAL_EVENTS = 1847
NUM_STREAMS = 40
EVENT_TYPE = "SyntheticSeedEvent"


async def main() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is not set. Configure .env in the project root.")

    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5)
    try:
        store = EventStore(pool)
        versions: dict[str, int] = {}

        for seq in range(TOTAL_EVENTS):
            stream_id = f"seed-{seq % NUM_STREAMS}"
            expected = versions.get(stream_id)
            if expected is None:
                exp = -1
            else:
                exp = expected

            new_v = await store.append(
                stream_id=stream_id,
                events=[
                    SyntheticSeedEvent(
                        payload=SyntheticSeedPayload(
                            seq=seq,
                            stream_shard=seq % NUM_STREAMS,
                            batch="datagen_generate_all",
                        )
                    )
                ],
                expected_version=exp,
                correlation_id=f"datagen-{seq // NUM_STREAMS}",
                causation_id=None,
            )
            versions[stream_id] = new_v

            if (seq + 1) % 200 == 0:
                print(f"Appended {seq + 1} / {TOTAL_EVENTS} events…")

        print(f"Done. Appended {TOTAL_EVENTS} events across {NUM_STREAMS} seed-* streams.")
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
