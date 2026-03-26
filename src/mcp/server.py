"""
MCP server entry point for The Ledger.

Usage:
    pool = await asyncpg.create_pool(dsn)
    mcp = build_ledger_mcp(pool)
    mcp.run()   # or await mcp.run_async()
"""

from __future__ import annotations

import asyncpg
from fastmcp import FastMCP

from src.mcp.resources import register_resources
from src.mcp.runtime import LedgerRuntime
from src.mcp.tools import register_tools


def build_ledger_mcp(pool: asyncpg.Pool) -> FastMCP:
    rt = LedgerRuntime.from_pool(pool)
    mcp = FastMCP(
        "The Ledger",
        instructions=(
            "Writes use MCP tools (commands). Reads use MCP resources backed by projections. "
            "On domain failure, tools return JSON with status=error and a typed error_type. "
            "Gas Town: call start_agent_session before any decision-class events on an agent session."
        ),
    )
    register_tools(mcp, rt)
    register_resources(mcp, rt)
    return mcp
