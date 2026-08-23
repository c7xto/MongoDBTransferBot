"""
db.py — Master MongoDB layer
Stores and retrieves per-user configuration in the host's Atlas cluster.
All per-user dedup/state helpers live in user_db.py instead.
"""
from __future__ import annotations

import asyncio
import datetime
import ipaddress
import socket
import time
from dataclasses import dataclass, field
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import certifi
import dns.asyncresolver
import dns.exception
import motor.motor_asyncio

from config import (
    _LEGACY_COL_NAME,
    _LEGACY_DB_NAME,
    _LEGACY_SPEED,
    _LEGACY_TARGET,
    HOST_ADMIN_ID,
    HOST_API_HASH,
    HOST_API_ID,
    MASTER_MONGO_URI,
    PARENT_BOT_TOKEN,
    STATE_DB_PREFIX,
    STATE_MONGO_URI,
    active_tasks,
    logger,
)

# ── Master client (singleton) ─────────────────────────────────────────────────
_master_client: motor.motor_asyncio.AsyncIOMotorClient | None = None

def _master_db():
    global _master_client
    if _master_client is None:
        _master_client = motor.motor_asyncio.AsyncIOMotorClient(
            MASTER_MONGO_URI,
            maxPoolSize=10, minPoolSize=1,
            serverSelectionTimeoutMS=8000,
            tlsCAFile=certifi.where())
    return _master_client["c7_settings"]

def col_users():
    return _master_db()["c7_users"]


async def get_all_configured_users() -> list[dict]:
    """Return every user whose setup wizard is complete."""
    return await col_users().find({"is_configured": True}).to_list(length=10_000)


# ── Per-user DB factory ───────────────────────────────────────────────────────
# One Motor client per unique URI — reused across all calls so we never open a
# new connection pool on every handler invocation.
@dataclass
class _CachedMotorClient:
    client: motor.motor_asyncio.AsyncIOMotorClient
    last_used: float   # time.monotonic(), refreshed on every access
    # user_ids that have ever requested this URI — checked against
    # cfg.active_tasks at eviction time so a client backing a long-running
    # transfer/monitor/prescan is never closed out from under it just because
    # its own Motor round-trips have been quiet for a while. Only grows: if a
    # user later points their config at a different URI, their id can linger
    # here and make eviction of the *old* URI slightly more conservative than
    # strictly necessary — harmless (worst case: a client sits idle a bit
    # longer than it needs to), never incorrect in the other direction.
    user_ids: set[int] = field(default_factory=set)


_user_motor_clients: dict[str, _CachedMotorClient] = {}

# Idle-eviction: a cached client unused for this long gets closed and dropped
# from the cache, freeing its connection pool. Checked on a periodic sweep,
# not on every access. 30 minutes is generous relative to this app's actual
# usage pattern (short, speed-paced per-file sends — see transfer.py), so no
# legitimate in-flight operation should hold last_used stale that long.
# Residual risk (accepted, not mitigated): a single operation that itself
# runs longer than the TTL could theoretically have its client closed out
# from under it — same category of accepted point-in-time risk as the SSRF
# DNS-rebinding note above.
_USER_CLIENT_IDLE_TTL_SECONDS = 30 * 60
_USER_CLIENT_EVICTION_INTERVAL_SECONDS = 5 * 60


async def get_user_motor_db(
        mongo_uri: str,
        db_name: str,
        user_id: int | None = None) -> motor.motor_asyncio.AsyncIOMotorDatabase:
    """
    Return a Motor database handle, reusing the client for the same URI.
    `user_id`, when passed, is recorded against the cache entry so idle
    eviction can check cfg.active_tasks before closing a client that's
    still backing someone's in-flight transfer/monitor/prescan.
    """
    entry = _user_motor_clients.get(mongo_uri)
    if entry is None:
        ok, err, sanitized = await validate_mongo_uri_format(mongo_uri)
        if not ok:
            raise ValueError(f"[DB] SSRF validation failed: {err}")
        # Cache key stays the original URI (preserves the one-client-per-URI
        # invariant); the client itself connects with the sanitized string.
        client = motor.motor_asyncio.AsyncIOMotorClient(
            sanitized,
            maxPoolSize=10, minPoolSize=1,
            serverSelectionTimeoutMS=15000,
            tlsCAFile=certifi.where())
        entry = _CachedMotorClient(client=client, last_used=time.monotonic())
        _user_motor_clients[mongo_uri] = entry
    else:
        entry.last_used = time.monotonic()
    if user_id is not None:
        entry.user_ids.add(user_id)
    return entry.client[db_name]


async def get_user_state_db(user_id: int):
    """Return the host-owned, per-user runtime-state database.

    Keeping this separate from the source catalogue allows its MongoDB account
    to be strictly read-only while checkpoints and dedup ledgers remain writable.
    """
    return await get_user_motor_db(
        STATE_MONGO_URI,
        f"{STATE_DB_PREFIX}_{int(user_id)}",
        user_id=user_id,
    )


def evict_idle_user_motor_clients(now: float | None = None) -> int:
    """
    Close and drop every cached client idle longer than the TTL — unless one
    of the user_ids ever associated with that URI currently has a tracked
    background task running (cfg.active_tasks), in which case eviction is
    skipped even though the *client's own* last_used looks stale (a transfer
    can go tens of minutes between Motor round-trips at the speed-paced
    delays this engine uses, while still very much being "in use").
    `now` is injectable (defaults to time.monotonic()) so tests can drive
    stale/fresh boundaries deterministically. Returns the number evicted.
    """
    if now is None:
        now = time.monotonic()
    stale_uris = []
    for uri, entry in _user_motor_clients.items():
        if now - entry.last_used <= _USER_CLIENT_IDLE_TTL_SECONDS:
            continue
        if any(
            any(key.endswith(f":{uid}") for key in active_tasks)
            for uid in entry.user_ids
        ):
            continue
        stale_uris.append(uri)
    for uri in stale_uris:
        _user_motor_clients.pop(uri).client.close()
    if stale_uris:
        # Never log the raw mongo_uri here — it's the dict key and contains
        # credentials (CLAUDE.md §7).
        logger.info(
            f"[DB] Evicted {len(stale_uris)} idle Motor client(s)  "
            f"idle>{_USER_CLIENT_IDLE_TTL_SECONDS}s")
    return len(stale_uris)


async def run_user_motor_client_eviction_loop() -> None:
    """Background loop: periodically sweep and evict idle cached Motor
    clients. Runs forever — intended to be scheduled as a tracked task so
    shutdown cancels it cleanly alongside every other background task."""
    while True:
        await asyncio.sleep(_USER_CLIENT_EVICTION_INTERVAL_SECONDS)
        try:
            evict_idle_user_motor_clients()
        except Exception as e:
            logger.error(f"[DB] Motor client eviction pass failed  err={e}")


def close_all_user_motor_clients() -> None:
    """Close every cached user Motor client (call at shutdown)."""
    for entry in _user_motor_clients.values():
        entry.client.close()
    _user_motor_clients.clear()


# ── Master DB initialisation ──────────────────────────────────────────────────
async def init_master_db() -> None:
    """Master DB has no custom indexes to create — `_id` is unique by default."""
    logger.info("[DB] Master DB ready")
    await _maybe_seed_first_user()


async def _maybe_seed_first_user() -> None:
    """
    Auto-import the single-user .env credentials as the first user doc.
    Only runs if HOST_ADMIN_ID is set and that user_id is not in the DB yet.
    """
    if not HOST_ADMIN_ID:
        return
    existing = await col_users().find_one({"_id": HOST_ADMIN_ID})
    if existing:
        return

    all_present = all([
        PARENT_BOT_TOKEN, HOST_API_ID, HOST_API_HASH,
        MASTER_MONGO_URI, _LEGACY_DB_NAME, _LEGACY_COL_NAME, _LEGACY_TARGET,
    ])
    if not all_present:
        logger.warning("[DB] AUTO-IMPORT skipped — missing legacy .env fields")
        return

    doc = _blank_user(HOST_ADMIN_ID)
    doc.update({
        "bot_token":     PARENT_BOT_TOKEN,
        "api_id":        HOST_API_ID,
        "api_hash":      HOST_API_HASH,
        "mongo_uri":     MASTER_MONGO_URI,
        "db_name":       _LEGACY_DB_NAME,
        "col_name":      _LEGACY_COL_NAME,
        "target":        _LEGACY_TARGET,
        "speed_delay":   _LEGACY_SPEED,
        "setup_step":    None,
        "is_configured": True,
    })
    await col_users().insert_one(doc)
    logger.info(f"[DB] AUTO-IMPORT: seeded first user  user_id={HOST_ADMIN_ID}")


# ── User CRUD ─────────────────────────────────────────────────────────────────
def _blank_user(user_id: int) -> dict:
    now = datetime.datetime.utcnow()
    return {
        "_id":             user_id,
        "username":        None,
        "bot_token":       None,
        "api_id":          None,
        "api_hash":        None,
        "mongo_uri":       None,
        "db_name":         None,
        "col_name":        None,
        "target":          None,
        "speed_delay":     3.5,
        "userbot_session": None,
        "setup_step":      "api_id",   # start wizard at first field
        "is_configured":   False,
        "registered_at":   now,
        "updated_at":      now,
    }


async def get_user(user_id: int) -> dict | None:
    return await col_users().find_one({"_id": user_id})


async def get_or_create_user(user_id: int, username: str | None = None) -> dict:
    doc = await get_user(user_id)
    if doc:
        return doc
    doc = _blank_user(user_id)
    if username:
        doc["username"] = username
    await col_users().insert_one(doc)
    logger.info(f"[DB] New user registered  user_id={user_id}")
    return doc


async def update_user_field(user_id: int, field: str, value) -> None:
    await col_users().update_one(
        {"_id": user_id},
        {"$set": {field: value, "updated_at": datetime.datetime.utcnow()}})


async def update_user_fields(user_id: int, fields: dict) -> None:
    fields["updated_at"] = datetime.datetime.utcnow()
    await col_users().update_one({"_id": user_id}, {"$set": fields})


async def mark_user_configured(user_id: int) -> None:
    await update_user_fields(user_id, {"is_configured": True, "setup_step": None})


async def reset_user_config(user_id: int) -> None:
    """Re-run the setup wizard from scratch (keeps user_id, clears credentials)."""
    blank = _blank_user(user_id)
    blank["registered_at"] = (await get_user(user_id) or blank)["registered_at"]
    await col_users().replace_one({"_id": user_id}, blank)


# ── SSRF validation for user-supplied MongoDB URIs ─────────────────────────────
# Every user-supplied mongo_uri must pass this before any Motor client is
# constructed from it — at setup time (validate_mongo_uri) AND again at every
# later connection point (get_user_motor_db / list_user_databases /
# list_user_collections), since DNS can rebind a host from a public IP to a
# private one after initial validation.
_DANGEROUS_URI_OPTIONS = {
    # Weaken/bypass TLS verification
    "tlsinsecure", "tlsallowinvalidcertificates", "tlsallowinvalidhostnames",
    # Point the driver at an arbitrary local file path on the host server
    "tlscafile", "tlscertificatekeyfile",
    # Bypass SRV-based seed-list resolution / load-balancer topology checks
    "directconnection", "loadbalanced",
    # SOCKS5 proxy — lets the actual TCP connection go anywhere, regardless
    # of what host/IP validation below concluded. The sharpest SSRF vector.
    "proxyhost", "proxyport", "proxyusername", "proxypassword",
    # Override the pool-sizing invariant mandated by CLAUDE.md §2
    "maxpoolsize", "minpoolsize",
}


def _strip_dangerous_uri_options(query: str) -> str:
    """Remove dangerous query options from a URI query string. Pure/network-free."""
    pairs = parse_qsl(query, keep_blank_values=True)
    kept = [(k, v) for k, v in pairs if k.lower() not in _DANGEROUS_URI_OPTIONS]
    return urlencode(kept, doseq=True)


def _split_host_port(entry: str) -> tuple[str, str | None]:
    """Split a single 'host[:port]' seed-list entry, handling bracketed IPv6."""
    entry = entry.strip()
    if entry.startswith("["):
        end = entry.find("]")
        if end == -1:
            return entry, None
        host = entry[1:end]
        rest = entry[end + 1:]
        port = rest[1:] if rest.startswith(":") else None
        return host, port
    if ":" in entry:
        host, _, port = entry.rpartition(":")
        if port.isdigit():
            return host, port
    return entry, None


def _is_disallowed_ip(ip_str: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
    except ValueError:
        return True  # unparsable — fail safe
    return (ip.is_private or ip.is_loopback or ip.is_link_local
            or ip.is_reserved or ip.is_unspecified or ip.is_multicast)


async def _resolve_and_check_host(host: str) -> tuple[bool, str]:
    try:
        infos = await asyncio.get_running_loop().getaddrinfo(host, None)
    except (socket.gaierror, OSError, asyncio.TimeoutError) as e:
        return False, f"Could not resolve host '{host}' — rejected for safety ({e})"
    if not infos:
        return False, f"Could not resolve host '{host}' — rejected for safety"
    for info in infos:
        ip_str = info[4][0]
        if _is_disallowed_ip(ip_str):
            return False, f"Host '{host}' resolves to a disallowed address ({ip_str})"
    return True, ""


async def _check_srv_targets(host: str) -> tuple[bool, str]:
    """
    Resolve the real _mongodb._tcp.<host> SRV target list and IP-check each
    target — matches what the driver will actually connect to. Many Atlas
    +srv clusters have no A/AAAA record on the bare hostname at all, so
    checking only the bare host would either false-reject valid clusters or
    false-pass an unrelated/absent record.
    """
    try:
        answer = await dns.asyncresolver.resolve(
            f"_mongodb._tcp.{host}", "SRV", lifetime=5)
    except dns.exception.DNSException as e:
        return False, f"Could not resolve SRV record for '{host}' — rejected for safety ({e})"
    targets = [str(rdata.target).rstrip(".") for rdata in answer]
    if not targets:
        return False, f"No SRV targets found for '{host}'"
    for target in targets:
        ok, err = await _resolve_and_check_host(target)
        if not ok:
            return False, err
    return True, ""


async def validate_mongo_uri_format(uri: str) -> tuple[bool, str, str | None]:
    """
    SSRF/format validation for a user-supplied mongo_uri, run before any
    Motor client is constructed from it. Returns (ok, error, sanitized_uri) —
    sanitized_uri is None on failure. This is a point-in-time check; DNS can
    still rebind after a connection pool is already open (accepted residual
    risk), which is why this is re-run at every client-creation site, not
    just once at setup time.
    """
    parsed = urlsplit(uri)
    scheme = parsed.scheme.lower()
    if scheme not in ("mongodb", "mongodb+srv"):
        return False, "Only mongodb:// and mongodb+srv:// URIs are allowed", None

    hosts_part = parsed.netloc.rpartition("@")[2]
    host_entries = [h.strip() for h in hosts_part.split(",") if h.strip()]
    if not host_entries:
        return False, "No host found in URI", None
    if scheme == "mongodb+srv" and len(host_entries) > 1:
        return False, "mongodb+srv:// URIs must specify exactly one host", None

    for entry in host_entries:
        host, _port = _split_host_port(entry)
        if not host:
            return False, "Malformed host in URI", None
        if host.lower() == "localhost":
            return False, "localhost is not allowed", None

        try:
            ip_literal = ipaddress.ip_address(host)
        except ValueError:
            ip_literal = None

        if ip_literal is not None:
            if _is_disallowed_ip(str(ip_literal)):
                return False, f"Host '{host}' is a disallowed address", None
            continue

        if scheme == "mongodb+srv":
            ok, err = await _check_srv_targets(host)
        else:
            ok, err = await _resolve_and_check_host(host)
        if not ok:
            return False, err, None

    sanitized_query = _strip_dangerous_uri_options(parsed.query)
    sanitized = urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, sanitized_query, parsed.fragment))
    return True, "", sanitized


# ── Validation helpers ────────────────────────────────────────────────────────
async def validate_bot_token(token: str) -> tuple[bool, str]:
    """
    Call Telegram's getMe endpoint to verify the token.
    Returns (True, bot_username) or (False, error_message).
    """
    import aiohttp
    url = f"https://api.telegram.org/bot{token}/getMe"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                data = await resp.json()
                if data.get("ok"):
                    uname = data["result"].get("username", "unknown")
                    return True, uname
                desc = data.get("description", "Token rejected by Telegram")
                return False, desc
    except Exception as e:
        return False, f"Network error: {e}"


async def validate_mongo_uri(uri: str) -> tuple[bool, str, str]:
    """
    Validate the URI's format/SSRF-safety, then ping the cluster to verify
    it's reachable.
    Returns (True, "", sanitized_uri) or (False, error_message, "").
    """
    ok, err, sanitized = await validate_mongo_uri_format(uri)
    if not ok:
        return False, err, ""

    client = motor.motor_asyncio.AsyncIOMotorClient(
        sanitized, maxPoolSize=10, minPoolSize=1,
        serverSelectionTimeoutMS=5000, tlsCAFile=certifi.where())
    try:
        await client.admin.command("ping")
        return True, "", sanitized
    except Exception as e:
        return False, str(e), ""
    finally:
        client.close()


async def list_user_databases(uri: str) -> list[str]:
    """
    Return all non-system database names from the user's Atlas cluster.
    """
    ok, err, sanitized = await validate_mongo_uri_format(uri)
    if not ok:
        raise ValueError(f"[DB] SSRF validation failed: {err}")

    SYSTEM_DBS = {"admin", "local", "config"}
    client = motor.motor_asyncio.AsyncIOMotorClient(
        sanitized, maxPoolSize=10, minPoolSize=1,
        serverSelectionTimeoutMS=5000, tlsCAFile=certifi.where())
    try:
        names = await client.list_database_names()
        return [n for n in names if n not in SYSTEM_DBS]
    finally:
        client.close()


async def list_user_collections(uri: str, db_name: str) -> list[str]:
    """
    Return all collection names from the specified database.
    """
    ok, err, sanitized = await validate_mongo_uri_format(uri)
    if not ok:
        raise ValueError(f"[DB] SSRF validation failed: {err}")

    client = motor.motor_asyncio.AsyncIOMotorClient(
        sanitized, maxPoolSize=10, minPoolSize=1,
        serverSelectionTimeoutMS=5000, tlsCAFile=certifi.where())
    try:
        return await client[db_name].list_collection_names()
    finally:
        client.close()
