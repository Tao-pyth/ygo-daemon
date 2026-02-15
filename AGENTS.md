
# AGENTS.md
## Project Instructions for Codex

---

# 1. Project Overview

This project implements a scheduled data synchronization daemon for the YGOPRODeck API v7.

Primary goals:

- Fetch Yu-Gi-Oh! card data using `cardinfo.php`
- Always request `misc=yes`
- Store API responses **losslessly** (full JSON preserved)
- Use SQLite for persistence
- Prioritize external queue (KONAMI_ID-based) over full sync
- Operate via periodic execution (Windows Task Scheduler style)
- Neo4j integration is explicitly out of scope

---

# 2. Architecture Principles

## 2.1 Data Preservation Policy (Critical)

- API JSON must be stored **exactly as received**
- No field deletion or structural mutation
- Store original JSON in `cards_raw.json` column (TEXT)
- Hash comparison allowed for change detection
- Secondary index table (`cards_index`) may extract commonly queried fields

Never replace raw JSON with partial representations.

---

## 3. Execution Model

Each run performs:

1. Acquire lock
2. Call `checkDBVer`
3. If changed → reset full sync state
4. Process queue (KONAMI_ID)
5. If queue empty → process one full sync page
6. Persist JSONL → SQLite batch ingest
7. Release lock

One execution must only advance progress incrementally.

---

# 4. Queue Policy

- Initial implementation uses `KONAMI_ID` only
- No fuzzy search
- No name-based search
- State values: PENDING / DONE / ERROR
- Errors must not crash full execution cycle

---

# 5. Full Sync Policy

- Use offset-based pagination
- Parameters: `num`, `offset`
- Maintain state in `kv_store`
- Stop when `meta.next_page_offset` is null

---

# 6. Code Style Guidelines

- Python 3.10+
- Use type hints
- No global mutable state except configuration
- Separate concerns (API / DB / Runner / Ingest)
- Avoid over-engineering
- Keep functions deterministic where possible

---

# 7. Database Rules

- SQLite in WAL mode
- Use UPSERT
- Always commit in controlled transaction blocks
- No schema mutation without explicit version update

---

# 8. Error Handling

- Use exponential backoff for API failures
- Never lose data already fetched
- JSONL staging must remain intact if DB ingest fails
- Failed ingest files must be moved to `failed/`

---

# 9. Forbidden Actions

Codex must NOT:

- Remove raw JSON storage
- Convert storage to partial schema only
- Introduce Neo4j references
- Replace offset pagination with ID guessing
- Remove queue priority

---

# 10. Directory Expectations

Project root layout:

