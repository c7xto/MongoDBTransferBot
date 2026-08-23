"""
config.py — host-level env vars + shared runtime state
Multi-user mode: only host credentials live here.
Per-user credentials are stored in Master MongoDB.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import sys

import pyrogram.utils

# Overwrite Pyrogram's legacy limits to support newer 2024-2026 Telegram IDs
pyrogram.utils.MIN_CHAT_ID = -999999999999
pyrogram.utils.MIN_CHANNEL_ID = -100999999999999


# ── .env loader ───────────────────────────────────────────────────────────────
def _load_dotenv(path: str = ".env") -> None:
    if not os.path.isfile(path):
        return
    try:
        from dotenv import load_dotenv
        load_dotenv(path, override=False)
        return
    except ImportError:
        pass
    with open(path) as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw or raw.startswith("#") or "=" not in raw:
                continue
            k, _, v = raw.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

_load_dotenv()


# ── credential redaction ──────────────────────────────────────────────────────
# Motor/pymongo exceptions frequently embed the full mongo_uri (password
# included) in their str(). Applied centrally inside both formatters below so
# every record reaching either handler is scrubbed regardless of whether the
# raising code remembered to redact — relying on per-call-site discipline is
# exactly the kind of thing that gets forgotten.
_MONGO_CRED_RE = re.compile(r"mongodb(\+srv)?://[^@\s]+@")
_BOT_TOKEN_RE  = re.compile(r"\b\d{6,10}:[A-Za-z0-9_-]{30,40}\b")


def _redact_secrets(text: str) -> str:
    text = _MONGO_CRED_RE.sub(r"mongodb\1://***:***@", text)
    text = _BOT_TOKEN_RE.sub("[REDACTED_BOT_TOKEN]", text)
    return text


# ── logging ───────────────────────────────────────────────────────────────────
class _Fmt(logging.Formatter):
    """
    Console formatter.
    Output:  HH:MM:SS │ TAG     │ message
    Tags are extracted from the [TAG] prefix in log messages.
    Tracebacks are suppressed on console — full detail goes to c7_errors.log.
    """
    DIM  = "\033[90m"
    R    = "\033[0m"
    _SEP = "\033[90m│\033[0m"
    _TAG = {
        "XFER":    "\033[1;97m",    # bold white
        "DB":      "\033[92m",      # green
        "WORKER":  "\033[93m",      # yellow
        "CONN":    "\033[91m",      # red
        "BOOT":    "\033[95m",      # magenta
        "SCAN":    "\033[94m",      # blue
        "STEALTH": "\033[90m",      # dim gray
        "MAIN":    "\033[96m",      # cyan
        "ADMIN":   "\033[93m",      # yellow
        "FLOOD":   "\033[93m",      # yellow
        "SYS":     "\033[96m",      # cyan
        "SYSTEM":  "\033[96m",      # cyan — container/host lifecycle events
    }
    _LVL = {
        10: "\033[90m",
        20: "\033[97m",
        30: "\033[93m",
        40: "\033[91m",
        50: "\033[91;1m",
    }

    def format(self, r: logging.LogRecord) -> str:
        ts  = self.formatTime(r, "%H:%M:%S")
        msg = _redact_secrets(r.getMessage())

        tag = "SYS"
        if msg.startswith("[") and "]" in msg:
            end = msg.index("]")
            tag = msg[1:end].strip()[:7].upper()
            msg = msg[end + 1:].lstrip()

        tag_c = self._TAG.get(tag, "\033[97m")
        lvl_c = self._LVL.get(r.levelno, "\033[97m")
        hint  = (f"  {self.DIM}→ c7_errors.log{self.R}"
                 if r.levelno >= logging.ERROR and r.exc_info else "")

        return (
            f"{self.DIM}{ts}{self.R} {self._SEP} "
            f"{tag_c}{tag:<7}{self.R} {self._SEP} "
            f"{lvl_c}{msg}{self.R}{hint}"
        )


class _FileFmt(logging.Formatter):
    """File formatter — plain text, full tracebacks, for post-mortem analysis."""
    def format(self, r: logging.LogRecord) -> str:
        ts   = self.formatTime(r, "%Y-%m-%d %H:%M:%S")
        base = f"{ts}  {r.levelname:<8}  {r.getMessage()}"
        if r.exc_info:
            base += "\n" + self.formatException(r.exc_info)
        return _redact_secrets(base)


_h = logging.StreamHandler()
_h.setFormatter(_Fmt())

_fh = logging.FileHandler("c7_errors.log", encoding="utf-8")
_fh.setLevel(logging.ERROR)
_fh.setFormatter(_FileFmt())

logging.basicConfig(level=logging.INFO, handlers=[_h, _fh])
for _n in ("pyrogram.connection", "pyrogram.session.auth",
           "pyrogram.session.session", "pyrogram.dispatcher"):
    logging.getLogger(_n).setLevel(logging.WARNING)
logger = logging.getLogger("C7")


# ── env var helpers ───────────────────────────────────────────────────────────
def _req(key: str) -> str:
    val = os.environ.get(key, "").strip()
    if not val:
        logger.critical(f"Missing required env var: {key}")
        sys.exit(1)
    return val

def _opt(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


# ── container-safe session storage ────────────────────────────────────────────
# Every Pyrogram Client in this codebase runs with in_memory=True, so no
# .session file is ever actually written to disk — auth happens fresh (bot
# token) or via a session_string persisted in Mongo (userbot pre-scan flow).
# SESSIONS_DIR is still set and passed as `workdir` defensively: if in_memory
# is ever disabled for any client, session files land in a known, writable
# folder under the working directory instead of crashing on a read-only
# container root filesystem (common on Pterodactyl/OptikLink volumes).
SESSIONS_DIR = os.path.join(os.getcwd(), "sessions")
try:
    os.makedirs(SESSIONS_DIR, exist_ok=True)
except OSError as _e:
    logger.warning(f"[SYSTEM] Could not create ./sessions ({_e}) — falling back to cwd for session workdir")
    SESSIONS_DIR = os.getcwd()


# ── host-level credentials (from .env only) ───────────────────────────────────
# The parent bot that all users interact with
PARENT_BOT_TOKEN  = _req("BOT_TOKEN")           # also aliased as BOT_TOKEN for compat
HOST_API_ID       = int(_req("TELEGRAM_API_ID"))
HOST_API_HASH     = _req("TELEGRAM_API_HASH")

# Master MongoDB — stores all user configs (c7_users collection)
MASTER_MONGO_URI  = _req("MONGO_URI")           # host's Atlas cluster

# Transfer checkpoints and deduplication must never require write access to a
# friend's source catalogue. By default they live beside the master config but
# in isolated per-user databases. Hosts may point STATE_MONGO_URI at a separate
# owned cluster without changing tenant configuration.
STATE_MONGO_URI    = _opt("STATE_MONGO_URI", MASTER_MONGO_URI)
STATE_DB_PREFIX    = _opt("STATE_DB_PREFIX", "c7_runtime")

# Optional: host admin Telegram ID (can bootstrap first user from .env)
HOST_ADMIN_ID     = int(_opt("ADMIN_ID", "0")) or None

# ── legacy single-user fields (read from .env for auto-import on first boot) ──
# These are ONLY used once in db.py to seed the first user doc, then ignored.
_LEGACY_DB_NAME   = _opt("DB_NAME")
_LEGACY_COL_NAME  = _opt("COLLECTION_NAME")
_LEGACY_TARGET    = _opt("TARGET_CHANNEL")
_LEGACY_SPEED     = float(_opt("SPEED_DELAY", "3.5"))


# ── runtime state (multi-user, keyed by Telegram user_id) ────────────────────
# True while a transfer is running for that user_id
active_transfers:   dict[int, bool]   = {}

# True while a transfer loop is suspended (paused) for that user_id
paused_transfers:   dict[int, bool]   = {}

# True while a monitor is running for that user_id
active_monitors:    dict[int, bool]   = {}

# Future objects used during /prescan interactive auth, keyed by user_id
scan_auth_futures:  dict[int, object] = {}

# Cached live Pyrogram worker clients, keyed by user_id
# Each value is a started pyrogram.Client instance
active_workers:     dict[int, object] = {}

# Telegram message ID of the live progress card, keyed by user_id.
# Cleared (popped) once the card has been given its terminal edit so the
# transfer loop never double-edits a card the callback handler already closed.
progress_msg_ids:   dict[int, int]    = {}

# Live snapshot of transfer progress, keyed by user_id.
# Written by run_transfer every iteration so callback handlers can build an
# accurate card immediately when the user taps Pause / Stop.
transfer_progress:  dict[int, dict]   = {}
# Schema: {"count": int, "total": int, "elapsed": float, "failed": int}

# Tracked asyncio.Task objects for every background launch (transfer/monitor/
# prescan), keyed by user_id. Populated by mdb.py's _track_task() so crashes
# are logged and shutdown can cancel + await every in-flight task.
active_tasks:        dict[str, asyncio.Task]  = {}

# Reserved negative sentinel key for system-level (non-per-user) background
# tasks tracked in active_tasks. Telegram user_ids are always positive, so a
# negative key can never collide with a real user's task slot.
SYSTEM_TASK_MOTOR_EVICTION = -1

# Per-user asyncio.Lock guarding the check-then-claim of active_transfers at
# transfer launch, so rapid double-taps on Start can't both pass the "already
# running?" check before either claims the slot.
# Deliberately never popped/evicted: recreating a fresh Lock for a user_id
# who might already be awaiting the existing one would let two callers each
# hold a *different* Lock instance and both believe they hold "the" per-user
# lock — reopening exactly the double-launch race this dict exists to close.
# The memory cost is one small Lock per distinct user ever seen (bounded by
# total registered users, not by transfer count) — not an unbounded leak.
launch_locks:        dict[int, asyncio.Lock]  = {}


# ── UI constants ──────────────────────────────────────────────────────────────
SEP  = "━" * 28
SEP2 = "─" * 28

# Required setup fields in wizard order
SETUP_FIELDS = ["api_id", "api_hash", "bot_token", "mongo_uri",
                "db_name", "col_name", "target"]
