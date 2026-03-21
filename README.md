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
│   ├── aggregates/         # Domain aggregates (loan, agent session, compliance)
│   ├── commands/           # Command handlers (load → validate → determine → append)
│   └── db/                 # Schema application helpers
├── tests/                  # pytest suite (incl. Double-Decision OCC gate)
├── datagen/                # Synthetic data generators
│   └── generate_all.py     # Seeds 1,847 canonical events via EventStore
├── scripts/                # Operational scripts
│   └── init_apex_ledger.py # Create DB + apply schema
├── DOMAIN_NOTES.md         # Graded domain reasoning (TRP1)
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

This installs runtime libraries (**asyncpg**, **pydantic**, **python-dotenv**) and test tooling (**pytest**, **pytest-asyncio**) in the active environment.

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

- **`src/schema.sql`** — defines `events`, `event_streams`, `projection_checkpoints`, `outbox`, indexes (including **BRIN** on `recorded_at`), and the **`uq_stream_position`** unique constraint on **`(stream_id, stream_position)`**.

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
pytest
```

---

## Phase 1 & 2 progress summary

| Area | Status |
|------|--------|
| **EventStore** | Implemented: **PostgreSQL + asyncio + asyncpg**, atomic append with **`SELECT … FOR UPDATE`** on **`event_streams`**, **outbox** in the same transaction, **`load_stream` / `load_all`**, stream metadata helpers. |
| **OCC at the database** | **Fully enforced** by the **`uq_stream_position` UNIQUE (`stream_id`, `stream_position`)** constraint on **`events`**, in addition to application-level **`expected_version`** checks (double protection under contention). |
| **LoanApplicationAggregate** | Implemented: **state machine** (submitted → analysis → compliance gate → decision → human → final), **Rule 1 / 4 / 5 / 6** hooks, **`load` / `_apply`**. |
| **AgentSessionAggregate** | Implemented: **Gas Town** (first event **`AgentContextLoaded`**), **model / analysis locking (Rule 3)** with cross-stream override ordering via **`global_position`**. |
| **Command handlers** | Implemented: **`handle_submit_application`**, **`handle_credit_analysis_completed`** (`src/commands/handlers.py`) following **load → validate → determine → append**, with **correlation_id** / **causation_id** chaining where applicable. |

---

## Additional references

- **`DOMAIN_NOTES.md`** — TRP1 graded deliverable: EDA vs ES, aggregates, OCC, projections, upcasting, Marten-style daemon notes.
- **Practitioner materials** (local copies): `sources/*.docx` and `_extracted/*.md`.

---

## Support

For connection failures, verify **`DATABASE_URL`**, PostgreSQL **`pg_hba.conf`** / SSL settings, and that database **`apex_ledger`** exists. The init script prints actionable guidance when authentication fails.
