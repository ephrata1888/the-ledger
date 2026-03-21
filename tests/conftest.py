import os
import sys
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
from dotenv import load_dotenv

# Ensure repo root is importable even under pytest importlib mode
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

load_dotenv(REPO_ROOT / ".env")

from src.db.schema_apply import iter_schema_statements  # noqa: E402


@pytest.fixture(scope="session")
def database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("DATABASE_URL not set (needs a PostgreSQL connection string)")
    return url


@pytest_asyncio.fixture(scope="function")
async def pool(database_url: str):
    """
    Function-scoped pool so every test runs on the same asyncio loop as the pool.
    A session-scoped pool + per-test event loops causes:
    'Task got Future attached to a different loop'.
    min_size>=2 so concurrent asyncio.gather appends can each acquire a connection.
    """
    try:
        pool = await asyncpg.create_pool(
            dsn=database_url,
            min_size=2,
            max_size=10,
        )
    except asyncpg.InvalidPasswordError:
        pytest.fail(
            "PostgreSQL rejected DATABASE_URL credentials (InvalidPasswordError).\n"
            "Update .env, for example:\n"
            "  DATABASE_URL=postgresql://postgres:YOUR_PASSWORD@localhost/apex_ledger\n"
            "Then run: python scripts/init_apex_ledger.py\n"
            "See README.md.",
            pytrace=False,
        )
    except asyncpg.InvalidCatalogNameError:
        pytest.fail(
            "The database in DATABASE_URL does not exist (InvalidCatalogNameError).\n"
            "Run: python scripts/init_apex_ledger.py\n"
            "See README.md.",
            pytrace=False,
        )
    try:
        yield pool
    finally:
        await pool.close()


@pytest_asyncio.fixture(autouse=True)
async def _apply_schema_and_truncate(pool: asyncpg.Pool):
    """
    One connection for the whole migration + truncate sequence.
    asyncpg requires one SQL statement per execute(); the full schema file is split
    by iter_schema_statements() — a single multi-statement string is not reliable.
    """
    schema_path = Path(__file__).resolve().parents[1] / "src" / "schema.sql"
    sql = schema_path.read_text(encoding="utf-8")

    async with pool.acquire() as conn:
        # DDL / extension: one statement per execute (asyncpg); avoid wrapping CREATE EXTENSION
        # in an explicit transaction — some PostgreSQL builds reject it.
        for statement in iter_schema_statements(sql):
            await conn.execute(statement)
        await conn.execute("TRUNCATE TABLE outbox RESTART IDENTITY CASCADE;")
        await conn.execute("TRUNCATE TABLE events RESTART IDENTITY CASCADE;")
        await conn.execute("TRUNCATE TABLE event_streams RESTART IDENTITY CASCADE;")
        await conn.execute("TRUNCATE TABLE projection_checkpoints RESTART IDENTITY CASCADE;")

    yield

