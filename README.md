# The Ledger

## Project overview

**The Ledger** is the **immutable memory and governance backbone** for the **Apex Financial Services** multi-agent platform. It provides a PostgreSQL-backed, ACID-compliant **event store** so every agent action, compliance signal, and human decision is recorded as an append-only **fact** (past-tense domain events), not as silent CRUD state. Together with **optimistic concurrency control (OCC)** and **CQRS-style** projections (later phases), it underpins regulatory auditability, causal tracing, and safe multi-agent coordination.

---

## Repository layout (Ten-Day Plan alignment)

```text
the-ledger/
├── src/                    # Application & domain code
│   ├── schema.sql          # Canonical PostgreSQL event-store schema
│   ├── event_store.py      # EventStore (asyncpg, OCC, outbox)
│   ├── models/             # Pydantic models (events, commands)
│   ├── aggregates/         # Domain aggregates (loan, agent session, compliance, audit ledger)
│   ├── commands/           # Command handlers (load → validate → determine → append)
│   ├── projections/        # Phase 3: CQRS read side (daemon + ApplicationSummary, AgentPerformance, ComplianceAudit)
│   ├── upcasting/          # Phase 4A: UpcasterRegistry + versioned read transforms
│   ├── integrity/          # Phase 4B–C: audit hash chain + Gas Town recovery
│   ├── mcp/                # FastMCP integration (tools=commands, resources=projections)
│   │   ├── server.py       # Entry point: build_ledger_mcp()
│   │   ├── tools.py        # 8 MCP tools (command side)
│   │   ├── resources.py    # 6 MCP resources (query side)
│   │   ├── errors.py       # Structured error serialisation
│   │   └── runtime.py      # LedgerRuntime (pool, store, projections, daemon)
│   ├── what_if/            # Phase 6: counterfactual projector
│   │   ├── projector.py    # run_what_if(), load_application_events_ordered()
│   │   └── memory_projections.py  # In-memory Phase 3 replay (no DB writes)
│   ├── regulatory/         # Phase 6: regulatory examination package
│   │   └── package.py      # generate_regulatory_package(), verify_regulatory_package()
│   └── db/                 # Schema application helpers
├── tests/                  # pytest suite
│   ├── test_concurrency.py         # Double-Decision OCC gate
│   ├── test_projections.py         # Projection SLO + rebuild tests
│   ├── test_upcasting.py           # Immutability + upcaster chain tests
│   ├── test_audit_chain.py         # SHA-256 hash chain + tamper detection
│   ├── test_audit_ledger_aggregate.py  # AuditLedgerAggregate unit tests
│   ├── test_gas_town.py            # Crash recovery reconstruction tests
│   ├── test_narratives.py          # NARR-01 through NARR-05 (all 5 pass)
│   └── test_mcp_lifecycle.py       # Full lifecycle via MCP tools only (12 assertions)
├── artifacts/              # Generated artifacts (committed)
│   ├── api_cost_report.txt
│   ├── regulatory_package_NARR05.json
│   └── counterfactual_narr05.json
├── ledger/                 # LangGraph agents
│   └── agents/
│       ├── base_agent.py           # BaseApexAgent (Gas Town, OCC retry, LLM cost tracking)
│       ├── credit_analysis_agent.py  # CreditAnalysisAgent (reference implementation)
│       ├── stub_agents.py          # DocumentProcessingAgent, FraudDetectionAgent, DecisionOrchestratorAgent
│       └── compliance_agent.py     # ComplianceAgent (6 deterministic rules, hard-block short-circuit)
├── datagen/                # Synthetic data generators
│   └── generate_all.py     # Seeds 1,847 canonical events via EventStore
├── scripts/                # Operational scripts
│   ├── init_apex_ledger.py # Create DB + apply schema
│   ├── run_mcp_server.py   # FastMCP HTTP server (default port 8765)
│   ├── demo_narr05.py      # Week Standard demo: history + integrity + what-if + package (<60s)
│   └── run_whatif.py       # Phase 6 gate: counterfactual projection CLI
├── DOMAIN_NOTES.md         # Graded domain reasoning (TRP1)
├── DESIGN.md               # Phase 4 inference policies (upcasting, audit chain, Gas Town)
├── requirements.txt        # Locked dependency list for uv
├── pyproject.toml          # Project metadata (also lists dependencies)
├── pytest.ini              # pytest-asyncio configuration
├── .env                    # Local secrets (not committed)
└── README.md               # This document
```

---

## Prerequisites

| Requirement   | Notes |
|---------------|--------|
| **Python**    | **3.11+** |
| **PostgreSQL**| Running instance reachable from your workstation (local or container). |
| **uv**        | Required by the TRP1 challenge for dependency management ([astral-sh/uv](https://github.com/astral-sh/uv)). |

---

## Installation (uv + `requirements.txt`)

From the repository root:

```bash
uv pip install -r requirements.txt
```

This installs runtime libraries (**asyncpg**, **pydantic**, **python-dotenv**, **fastmcp**) and test tooling (**pytest**, **pytest-asyncio**) in the active environment.

> Optional (editable install for development): `uv pip install -e ".[dev]"` using `pyproject.toml`.

---

## Environment configuration

Create a **`.env`** file in the project root (see `.env.example`). Tests and scripts load it via **python-dotenv**.

### `.env` template

```env
# Canonical format (include user, password, host, port, database)
DATABASE_URL=postgresql://user:password@localhost:5432/apex_ledger
```

- Replace `user`, `password`, host, port, and database name to match your PostgreSQL role and instance.
- **URL-encode** reserved characters in the password (e.g. `@` → `%40`).

### Mandatory data-classification warning

> **Never store PII in the event payload without encryption.**

The event log is **permanent** and **auditable**; payloads must follow your firm’s encryption and tokenization standards.

---

## Database migrations & initialization

### 1. Apply the canonical schema (`src/schema.sql`)

The **single source of truth** for the relational contract is:

- **`src/schema.sql`** — defines `events`, `event_streams`, `projection_checkpoints`, `outbox`, **read-model tables** (`projection_application_summary`, `projection_agent_performance`, `compliance_audit_timeline`, `projection_dead_letter`), indexes (including **BRIN** on `recorded_at`), and the **`uq_stream_position`** unique constraint on **`(stream_id, stream_position)`**.

**Option A — automated (recommended)**  
Run the initialization script (creates the database if missing, then applies each DDL statement):

```bash
python scripts/init_apex_ledger.py
```

**Option B — manual**  
Execute `src/schema.sql` with your SQL client against the `apex_ledger` database, e.g.:

```bash
psql "postgresql://user:password@localhost:5432/apex_ledger" -f src/schema.sql
```

### 2. Seed synthetic events (1,847 rows)

After the schema exists and `DATABASE_URL` is set:

```bash
python datagen/generate_all.py
```

This appends **exactly 1,847** **`SyntheticSeedEvent`** records across **40** streams (`seed-0` … `seed-39`) through **`EventStore.append`**, preserving **OCC**, **outbox** writes, and **`global_position`** semantics. Use a non-production database when re-running (truncate or recreate DB if you need a clean slate).

---

## Executing the test suite

### Double-Decision test (Phase 1 gate)

```bash
pytest tests/test_concurrency.py
```

**Expected outcome**

- **Exactly one** concurrent `append` **succeeds**.
- The other task raises **`OptimisticConcurrencyError`** (not swallowed).
- The loan stream ends with **exactly four** events (three seeded positions plus **one** winning append).

Add `-q` for quiet mode:

```bash
pytest tests/test_concurrency.py -q
```

### Full test suite

```bash
# Full suite (requires DATABASE_URL and OPENROUTER_MOCK=1 for agent tests)
OPENROUTER_MOCK=1 pytest tests/ -q
```

```
# Expected: 27 tests pass
```

---

## Implementation summary

| Area | Status |
|------|--------|
| **EventStore** | Implemented: **PostgreSQL + asyncio + asyncpg**, atomic append with **`SELECT … FOR UPDATE`** on **`event_streams`**, **outbox** in the same transaction, **`load_stream` / `load_all`**, stream metadata helpers. |
| **OCC at the database** | **Fully enforced** by the **`uq_stream_position` UNIQUE (`stream_id`, `stream_position`)** constraint on **`events`**, in addition to application-level **`expected_version`** checks (double protection under contention). |
| **LoanApplicationAggregate** | Implemented: **state machine** (submitted → analysis → compliance gate → decision → human → final), **Rule 1 / 4 / 5 / 6** hooks, **`load` / `_apply`**. |
| **AgentSessionAggregate** | Implemented: **Gas Town** (first event **`AgentContextLoaded`**), **model / analysis locking (Rule 3)** with cross-stream override ordering via **`global_position`**. |
| **Command handlers** | Implemented: **`handle_submit_application`**, **`handle_credit_analysis_completed`** (`src/commands/handlers.py`) following **load → validate → determine → append**, with **correlation_id** / **causation_id** chaining where applicable. |

### Phase 3 — Projections & async daemon

| Area | Status |
|------|--------|
| **`ProjectionDaemon`** | **`src/projections/daemon.py`**: polls **`events`** by **`global_position`**, **advisory lock** `pg_try_advisory_lock(87214, hashtext(projection_name))`, **SAVEPOINT** + retry + dead-letter, **checkpoint** updated in the **same transaction** as projection writes. |
| **`get_lag()`** | **`LagMetrics`**: **`events_behind`** and **`lag_ms`** (head vs checkpoint **`recorded_at`**; catch-up uses wall-clock vs head when checkpoint is 0). |
| **`ApplicationSummary`** | Flat **`projection_application_summary`** with **`INSERT … ON CONFLICT DO UPDATE`**; loan stream events + **`compliance_status`** mirror from compliance projection. |
| **`AgentPerformanceLedger`** | **`projection_agent_performance`** by **`model_version`**: analysis counts, rolling sums for avg confidence / duration, decision + human-override counters. |
| **`ComplianceAuditView`** | **`compliance_audit_timeline`**: **event-triggered** snapshots + **`get_compliance_at` / `get_current_compliance`**, **`rebuild_from_scratch`**. |
| **Tests** | **`tests/test_projections.py`**: rebuild vs incremental on **1,847** synthetic rows, temporal smoke, SLO smoke after **50** concurrent appends. |

```bash
pytest tests/test_projections.py -q
```

### Phase 4 — Upcasting, audit chain, Gas Town

| Area | Status |
|------|--------|
| **UpcasterRegistry** | `src/upcasting/registry.py` — keyed by `(event_type, from_version)`; chains until catalogue head; `EventStore.load_stream` / `load_all` apply upcasts (use `apply_upcast=False` for raw rows / nested reads). |
| **Built-in upcasters** | `src/upcasting/upcasters.py` — **CreditAnalysisCompleted** v1→v2 (deterministic `model_version` / `regulatory_basis`, `confidence_score: null`); **DecisionGenerated** v1→v2 (read-only agent stream lookup for `model_versions`, `apply_upcast=False` on nested loads). |
| **Audit chain** | `src/integrity/audit_chain.py` — `run_integrity_check` folds SHA-256 chain over business events, detects tamper vs last `AuditIntegrityCheckRun`, appends new check event. |
| **Gas Town** | `src/integrity/gas_town.py` — `reconstruct_agent_context` rebuilds agent memory with summary + verbatim tail + reconciliation flags. |
| **Tests** | `tests/test_upcasting.py`, `tests/test_audit_chain.py`, `tests/test_gas_town.py` |

**Inference rationale** (exam / regulator): see **`DESIGN.md`**.

### Phase 5 — FastMCP integration (CQRS surface)

| Surface | Implementation |
|---------|----------------|
| **Tools (commands)** | `src/mcp/tools.py` — `register_tools()` calls Phase 2 handlers in `src/commands/handlers.py`; each tool description documents **preconditions**; failures return typed JSON (`error_type`: `DomainError` \| `OptimisticConcurrencyError` \| …), not bare strings. |
| **Resources (queries)** | `src/mcp/resources.py` — **ApplicationSummary**, **ComplianceAuditView** (current + `…/compliance/as_of/{ISO-8601}` for temporal / `?as_of=` semantics), **AgentPerformanceLedger** (projection rows; keyed by `model_version`), **health** (`ProjectionDaemon.get_all_lags` + SLO status). **Exceptions:** `ledger://…/audit-trail` and agent session URIs **direct-load** streams for audit/session reconstruction. |
| **Projections** | After each successful tool, `LedgerRuntime.catch_up_projections()` advances all Phase 3 handlers so reads stay fresh without replay-on-read. |
| **HTTP server** | `python scripts/run_mcp_server.py` — binds **0.0.0.0:8765** (requires `DATABASE_URL`). |
| **Lifecycle test** | `tests/test_mcp_lifecycle.py` — full loan flow using **only** `Client.call_tool` / `read_resource` (in-process FastMCP). |

**Read-model SLO targets (design):** application summary **p99 &lt;50ms**, compliance **&lt;200ms**, health **&lt;10ms** (single-row / indexed queries on a warm pool).

### Narrative scenarios (NARR-01 through NARR-05)

All five production failure scenarios pass. Run with `OPENROUTER_MOCK=1` for fast, cost-free execution:

```bash
pytest tests/test_narratives.py -v
```

| Scenario | What it tests |
|----------|---------------|
| **NARR-01** | Concurrent OCC collision — two `CreditAnalysisAgent` instances on the same application; both complete without raising to the caller |
| **NARR-02** | Missing EBITDA in extraction — `confidence_score` capped at 0.75; `data_quality_caveats` non-empty |
| **NARR-03** | Agent crash recovery — `FraudDetectionAgent` crashes after `load_signals`; recovery agent resumes without duplicate work |
| **NARR-04** | Compliance hard block — Montana company (REG-003); exactly 3 rule events; no `DecisionGenerated` ever written |
| **NARR-05** | Human override — orchestrator recommends DECLINE; loan officer LO-Sarah-Chen approves at $750k with conditions |

### Phase 6 — What-If Projector & Regulatory Examination Package

| Area | Status |
|------|--------|
| **WhatIfProjector** | `src/what_if/projector.py` — `run_what_if()` splices counterfactual events at a branch point, filters causally dependent downstream events via `causation_id` chain traversal, remaps `global_position`, and replays Phase 3 projections in-memory. Never writes to the event store. |
| **In-memory projections** | `src/what_if/memory_projections.py` — `InMemoryPhase3Projections` mirrors `ApplicationSummary`, `ComplianceAuditView`, and `AgentPerformanceLedger` apply-paths for counterfactual replay without DB dependency. |
| **Regulatory package** | `src/regulatory/package.py` — `generate_regulatory_package()` produces a self-contained JSON with: all event streams in order, projection state at examination date, SHA-256 audit chain integrity proof, per-agent model metadata, and a plain-English narrative. `verify_regulatory_package()` validates the SHA-256 independently — no live DB access required. |
| **CLI gate script** | `scripts/run_whatif.py` — `--application` + `--substitute-credit-tier`; writes `artifacts/counterfactual_narr05.json`; exits 1 if baseline and counterfactual recommendations are identical. |
| **Demo script** | `scripts/demo_narr05.py` — seeds a NARR-05 application, runs history + integrity + what-if + regulatory package end-to-end in under 60 seconds. |
| **Artifacts** | `artifacts/regulatory_package_NARR05.json` and `artifacts/counterfactual_narr05.json` committed. |

**Gate tests:**
```bash
# Run the Week Standard demo (must complete < 60s)
python scripts/demo_narr05.py

# Run the what-if gate (copy the application ID printed by the demo)
python scripts/run_whatif.py --application <NARR05-id> --substitute-credit-tier MEDIUM
# Expected: baseline=DECLINE, counterfactual=APPROVE, materially_different=True
```

---

## Additional references

- **`DOMAIN_NOTES.md`** — Domain reconnaissance: event catalogue decisions, aggregate boundaries, Gas Town rationale, data boundary analysis.
- **`DESIGN.md`** — Six required sections: aggregate boundary justification, projection strategy, Week 3 integration, prompt design, agent failure modes, what I would do differently.
- **`artifacts/regulatory_package_NARR05.json`** — Self-contained regulatory examination package for NARR-05; independently verifiable without live DB access.
- **`artifacts/counterfactual_narr05.json`** — What-if counterfactual result for NARR-05 (MEDIUM risk substitution → APPROVE vs baseline DECLINE).

---

## Support

For connection failures, verify **`DATABASE_URL`**, PostgreSQL **`pg_hba.conf`** / SSL settings, and that database **`apex_ledger`** exists. The init script prints actionable guidance when authentication fails.
