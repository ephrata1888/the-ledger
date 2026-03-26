"""
Gas Town agent memory: reconstruct working context from the agent session stream after crash.

Uses token_budget as a soft cap (chars ≈ 4 * tokens heuristic).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from src.event_store import EventStore
from src.models.events import StoredEvent


def _approx_tokens(text: str) -> int:
    return max(1, len(text) // 4)


def _summarize_events(events: List[StoredEvent]) -> str:
    """Deterministic compact prose for early history (not an LLM call)."""
    parts: List[str] = []
    for ev in events:
        parts.append(f"{ev.event_type}@{ev.stream_position}")
    return "Earlier session history (" + ", ".join(parts) + ")."


@dataclass
class AgentContextReconstruction:
    summary_prose: str
    verbatim_recent: List[Dict[str, Any]]
    verbatim_priority: List[Dict[str, Any]]
    last_completed_action: Optional[str]
    pending_work: List[str]
    needs_reconciliation: bool
    approx_tokens_used: int


async def reconstruct_agent_context(
    store: EventStore,
    agent_id: str,
    session_id: str,
    *,
    token_budget: int = 8000,
) -> AgentContextReconstruction:
    """
    Rebuild agent memory from stream agent-{agent_id}-{session_id}.

    Strategy:
    - Summarize all but the last 3 events into compact prose (deterministic).
    - Preserve the last 3 events verbatim (payload + type).
    - Always include events that look PENDING/ERROR (AgentNodeExecuted with those statuses).
    - Partial decision: last event is AgentNodeExecuted with no later AgentOutputWritten for same node_id
      => NEEDS_RECONCILIATION.
    """
    stream_id = f"agent-{agent_id}-{session_id}"
    events = await store.load_stream(stream_id, apply_upcast=True)

    pending_like: Set[int] = set()
    for i, ev in enumerate(events):
        if ev.event_type != "AgentNodeExecuted":
            continue
        st = (ev.payload or {}).get("status", "")
        if st in ("pending", "error", "partial"):
            pending_like.add(i)

    verbatim_idx: Set[int] = set()
    if len(events) >= 3:
        verbatim_idx.update(range(len(events) - 3, len(events)))
    else:
        verbatim_idx.update(range(len(events)))
    verbatim_idx.update(pending_like)

    early = [i for i in range(len(events)) if i not in verbatim_idx]
    early_events = [events[i] for i in early]
    summary = _summarize_events(early_events) if early_events else "(no prior events)"

    verbatim_recent: List[Dict[str, Any]] = []
    verbatim_priority: List[Dict[str, Any]] = []
    for i in sorted(verbatim_idx):
        ev = events[i]
        block = {"event_type": ev.event_type, "payload": dict(ev.payload or {})}
        if i in pending_like:
            verbatim_priority.append(block)
        else:
            verbatim_recent.append(block)

    last_node_out: Dict[str, int] = {}
    last_node_exec: Dict[str, int] = {}
    for i, ev in enumerate(events):
        if ev.event_type == "AgentNodeExecuted":
            nid = str((ev.payload or {}).get("node_id", ""))
            if nid:
                last_node_exec[nid] = i
        if ev.event_type == "AgentOutputWritten":
            nid = str((ev.payload or {}).get("node_id", ""))
            if nid:
                last_node_out[nid] = i

    last_completed_action: Optional[str] = None
    pending_work: List[str] = []
    needs_reconciliation = False

    if events:
        last_ev = events[-1]
        if last_ev.event_type == "AgentSessionCompleted":
            last_completed_action = "agent_session_completed"
            needs_reconciliation = False
        elif last_ev.event_type == "AgentOutputWritten":
            last_completed_action = f"output_written:{(last_ev.payload or {}).get('node_id')}"
        elif last_ev.event_type == "AgentNodeExecuted":
            nid = str((last_ev.payload or {}).get("node_id", ""))
            # Tool/infra rows do not require AgentOutputWritten pairing.
            if nid.startswith("tool:"):
                needs_reconciliation = False
                last_completed_action = f"tool_invocation:{nid}"
            else:
                out_i = last_node_out.get(nid, -1)
                ex_i = last_node_exec.get(nid, -1)
                if ex_i > out_i:
                    needs_reconciliation = True
                    pending_work.append(
                        f"NEEDS_RECONCILIATION: AgentNodeExecuted for node {nid} without "
                        f"matching AgentOutputWritten / downstream completion on stream"
                    )

    text_parts = [summary]
    for b in verbatim_priority + verbatim_recent:
        text_parts.append(str(b))
    total_text = "\n".join(text_parts)
    approx = _approx_tokens(total_text)
    if approx > token_budget:
        summary = summary[: max(0, len(summary) - (approx - token_budget) * 4)]

    return AgentContextReconstruction(
        summary_prose=summary,
        verbatim_recent=verbatim_recent,
        verbatim_priority=verbatim_priority,
        last_completed_action=last_completed_action,
        pending_work=pending_work,
        needs_reconciliation=needs_reconciliation,
        approx_tokens_used=min(approx, token_budget),
    )
