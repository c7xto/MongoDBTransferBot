# C7 Data Engine V11 — Claude Code Instructions

This file governs all AI-assisted development on this codebase.
Read it in full before making any change, however small.

---

## 1. System Purpose — Non-Negotiable

**This system is a direct file delivery engine. Nothing else.**

It reads documents from a user's MongoDB collection, each containing a Telegram
`file_id`, and forwards those files to a target Telegram channel using
`send_cached_media` / `send_media_group`. That is the entire product scope.

**Strictly prohibited scope creep — never implement:**
- General media downloading, uploading, or re-encoding
- Caption editing, watermarking, or content transformation
- Media scraping from external URLs or third-party sources
- Any feature that fetches, processes, or re-uploads actual file bytes
- Conversion between media types (video → document, image → PDF, etc.)
- Content generation or AI-assisted media manipulation

If a request would require downloading a file's bytes, processing media content,
or acting as a general-purpose media bot, refuse and explain the system boundary.
The delivery engine touches `file_id` strings only — never raw file bytes.

---

## 2. Motor Connection Pooling — Enforced Architecture

**Rule: One `AsyncIOMotorClient` per unique `mongo_uri`. Never more.**

The `_user_motor_clients` dict in `db.py` is the single source of truth for all
user Motor clients. Every database access for user data MUST go through
`get_user_motor_db(mongo_uri, db_name)`. Never instantiate `AsyncIOMotorClient`
directly outside of `db.py`.

### What you must never do

```python
# BANNED — creates a new connection pool on every call
client = motor.motor_asyncio.AsyncIOMotorClient(user_cfg["mongo_uri"], ...)
col = client[db_name][col_name]
```

### What you must always do

```python
# CORRECT — reuses the cached client for this URI
from db import get_user_motor_db
user_db = get_user_motor_db(user_cfg["mongo_uri"], user_cfg["db_name"])
col = user_db[col_name]
```

### Connection pool sizing

All user Motor clients must be created with:
```python
AsyncIOMotorClient(uri, maxPoolSize=10, minPoolSize=1, tlsCAFile=certifi.where())
```
The default `maxPoolSize=100` will exhaust Atlas connection limits at low user
counts. This is not negotiable and must not be changed without a documented
capacity analysis.

### TTL eviction

The `_user_motor_clients` cache has no eviction policy today. Any refactoring
that touches the Motor client cache must preserve the one-client-per-URI
invariant and must not remove TTL tracking when it is added.

### At shutdown

`close_all_user_motor_clients()` must be called in the `finally` block of
`__main__` before `app.stop()`. Do not remove or reorder this call.

---

## 3. MongoDB URI Separation — Hard Boundary

Two categories of MongoDB connection exist. They must never be mixed.

### Master DB — host infrastructure only

- URI source: `MASTER_MONGO_URI` environment variable
- Database: `c7_settings`
- Collection: `c7_users`
- Purpose: stores user configuration documents (credentials, settings)
- Accessible via: `db.py` functions only (`get_user`, `update_user_field`, etc.)
- **Never** used to store per-user transfer state or dedup data

### Per-user source DB — read-only catalogue

- URI source: `user_cfg["mongo_uri"]` (user-supplied, stored in Master DB)
- Database: `user_cfg["db_name"]`
- Collections: the user's source data collection (`user_cfg["col_name"]`)
- Purpose: read source file documents only
- Accessible via: `get_user_motor_db(...)` and `source_db[col_name]`
- **Never** used for host-level configuration or cross-tenant queries

### Host-owned runtime state DB — mutable transfer data

- URI source: optional `STATE_MONGO_URI`, defaulting to the host-owned
  `MASTER_MONGO_URI`
- Database: isolated `STATE_DB_PREFIX_<telegram_user_id>` database per tenant
- Collections: `c7_sent_ids`, `c7_scan_index`, and `c7_state`
- Purpose: deduplication, pre-scan index, transfer cursor, and monitor resume token
- Accessible through `get_user_state_db(user_id)` and `user_db.py` helpers
- This separation is required so a friend's source account can remain read-only

**There must be zero code paths that write runtime state into a tenant's source
database or read host configuration from a user's URI.**

---

## 4. User-Supplied MongoDB URIs — SSRF Validation Required

Every user-supplied `mongo_uri` must be validated before any Motor client is
created for it. Validation must enforce:

1. **Scheme whitelist** — only `mongodb://` and `mongodb+srv://` are permitted
2. **Private IP block** — reject URIs resolving to RFC-1918 ranges
   (`10.x`, `172.16-31.x`, `192.168.x`), loopback (`127.x`, `::1`),
   link-local (`169.254.x`), and `localhost`
3. **Option stripping** — dangerous URI options that override pool/auth behavior
   must be stripped before the URI is stored or used

Until a formal `validate_mongo_uri_format()` function implementing these rules
is added to `db.py`, treat any new feature that stores or connects to a user URI
as incomplete. Do not ship URI-handling code without SSRF protection.

---

## 5. Asyncio Task Management — No Fire-and-Forget

**Every `asyncio.create_task()` call must store its return value.**

```python
# BANNED — task reference discarded, exceptions invisible, no cancellation
asyncio.create_task(_launch_transfer(user))

# REQUIRED — task tracked in registry
task = asyncio.create_task(_launch_transfer(user), name=f"transfer-{user_id}")
cfg.active_tasks[user_id] = task
task.add_done_callback(lambda _: cfg.active_tasks.pop(user_id, None))
```

`cfg.active_tasks: dict[int, asyncio.Task]` must be populated before any new
long-running task type is introduced. The shutdown sequence must cancel all
tracked tasks and await their completion before stopping clients.

---

## 6. Global State Dicts — Use Correctly, Do Not Expand

The five runtime state dicts in `config.py` are a known architectural debt item.
Rules for working with them:

- **Do not add new global dicts** for new features. If a new feature needs
  runtime state, it must use one of the existing dicts or propose a structured
  replacement reviewed against the Section 15 refactoring blueprint.
- **Always pop, never just set to False.** When a transfer or monitor ends
  (success, stop, or crash), use `.pop(user_id, None)` on `paused_transfers`
  and clean the user's entry from `transfer_progress`. Boolean `False` entries
  accumulate and constitute a memory leak.
- **No TOCTOU reads without a lock.** Any check of `active_transfers.get(user_id)`
  followed by `active_transfers[user_id] = True` must be wrapped in a per-user
  `asyncio.Lock` to prevent double-launch races from rapid button taps.

---

## 7. Credential Safety in Logging

**Never log raw credential values.** This applies to all logging calls across
all modules.

Sensitive fields that must never appear in log output:
- `bot_token`
- `api_hash`
- `mongo_uri` (full URI with credentials)
- Phone numbers (from prescan auth flow)
- OTP codes (from prescan auth flow)

Motor exception messages frequently embed the full `mongo_uri` including the
password component. Before any `logger.error` or `logger.exception` call that
includes an exception object touching MongoDB, redact the URI:

```python
import re
def _redact_uri(text: str) -> str:
    return re.sub(r"mongodb(\+srv)?://[^@\s]+@", r"mongodb\1://***:***@", str(text))

# Usage:
L.error(f"[DB] Connection failed  err={_redact_uri(e)}")
```

The `_mask()` utility in `mdb.py` is for user-facing display only. It does not
substitute for log-level redaction.

---

## 8. Transfer Engine — Separation from UI Layer

The transfer engine in `transfer.py` must remain decoupled from Pyrogram message
types and Telegram-specific UI constructs except through:

1. The `admin_app` client parameter (for sending DMs and editing the progress card)
2. The `_edit_card` internal helper (the sole UI touchpoint)

**Never import `ui.py` components into `transfer.py` beyond what already exists.**
Never call `query.answer()`, `query.message.edit_text()`, or any callback-query
method from inside the transfer engine. UI interactions belong in `mdb.py` handlers.

---

## 9. Handler Architecture — No New elif Branches

`_cb_handler_inner` in `mdb.py` is a known God Function. Do not extend it with
additional `elif data == "..."` branches. New callback handlers must be grouped
logically and documented with a section comment. The long-term goal is a dispatch
table; every new handler added to the elif chain makes that migration harder.

When adding a new inline button:
1. Define the `callback_data` string as a named constant, not an inline literal
2. Keep `callback_data` under 64 bytes (Telegram hard limit)
3. For values that could exceed 64 bytes (e.g., long collection names), use the
   server-side payload reference pattern, not direct embedding

---

## 10. File Structure & Module Responsibilities

| File | Responsibility | Must Not |
|------|----------------|----------|
| `config.py` | Env vars + runtime state dicts | Contain business logic |
| `db.py` | Master DB access + Motor client factory | Touch per-user collections |
| `user_db.py` | Per-user DB helpers | Import from `db.py` or `mdb.py` |
| `transfer.py` | Transfer loop engine | Import `mdb.py` or call handlers |
| `monitor.py` | Change stream monitor | Create its own Motor client |
| `prescan.py` | Channel pre-scan | Create its own Motor client |
| `ui.py` | Keyboard builders + card renderer | Import Motor or Pyrogram clients |
| `mdb.py` | Handler routing + orchestration | Contain transfer business logic |

Any proposed change that would add an import crossing these boundaries requires
justification against this table.

---

## 11. Dependency Rules

- **Do not add `bson` as a standalone package.** Use `from pymongo` (already
  provided by Motor) to access `ObjectId`. Standalone `bson` conflicts with
  pymongo's bundled copy and will cause version resolution errors.
- **Do not upgrade `pyrogram` without testing the event loop jumpstart block.**
  The `asyncio.get_event_loop()` guard at the top of `mdb.py` exists specifically
  for Pyrogram 2.x's import-time loop capture. Any Pyrogram version change must
  be verified against this interaction.
- **All new packages must be added to `requirements.txt`** with a pinned minor
  version range (`>=X.Y, <X.Z`). No unpinned dependencies.
- **`certifi` must be kept current.** Outdated CA bundles cause silent TLS
  failures. Do not pin `certifi` to a specific version; allow patch updates.

---

## 12. What This Audit Found — Do Not Regress

The V11 production audit (July 2026) identified the following items as fixed.
Do not reintroduce them:

| Item | Status | File |
|------|--------|------|
| Motor client created per handler call | Fixed | `db.py` |
| `_col_sent_ids` imported across module boundary | Fixed | `user_db.py` |
| `certifi`/`motor` imported in `transfer.py`, `prescan.py`, `monitor.py` | Fixed | All three |
| Dead `auto_resume` function | Removed | `transfer.py` |
| `auto_resume` import in `mdb.py` | Removed | `mdb.py` |
| Separate `db_client` in `monitor.py` ignoring `user_db` param | Fixed | `monitor.py` |
| Speed menu label whitespace (`"  3.5s"`) | Fixed | `ui.py` |
| Help page URLs not clickable (code-block wrapping) | Fixed | `ui.py` |
| Admin panel button with no handler | Fixed | `mdb.py` |
| `/setup` wiping config without confirmation | Fixed | `mdb.py` |
| `pick_col:` making 3 DB writes instead of 1 | Fixed | `mdb.py` |
| Wipe confirmation lacking record counts | Fixed | `mdb.py` |
| Home card showing "Running" when transfer is paused | Fixed | `mdb.py` |
| `from_user` accessed without None guard | Fixed | `mdb.py` |
| `validate_mongo_uri` with mismatched inner/outer timeouts | Fixed | `db.py` |
| Redundant `create_index("_id")` on master collection | Open | `db.py` |
| Prescan step-2 N+1 query pattern | Open | `prescan.py` |
| No SIGTERM handler | Open | `mdb.py` |
| No asyncio Task registry | Open | `mdb.py` / `config.py` |
| SSRF validation on user-supplied URIs | Open | `db.py` |
| Motor `maxPoolSize` not explicitly set | Open | `db.py` |
| Motor client cache has no TTL eviction | Open | `db.py` |
