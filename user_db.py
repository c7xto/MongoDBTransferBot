"""
user_db.py — per-user MongoDB helpers
All functions accept a Motor `db` handle (AsyncIOMotorDatabase) instead of
reading global config. This lets every user's transfer/monitor/prescan operate
against their own Atlas cluster without interfering with each other.

Usage:
    from db import get_user_motor_db
    from user_db import init_user_db, filter_already_sent, mark_as_sent, ...

    db = get_user_motor_db(user_cfg["mongo_uri"], user_cfg["db_name"])
    await init_user_db(db)
"""
from __future__ import annotations
from config import logger


# ── Collection accessors ──────────────────────────────────────────────────────
def _col_sent_ids(db):      return db["c7_sent_ids"]
def _col_scan_index(db):    return db["c7_scan_index"]
def _col_state(db):         return db["c7_state"]


# ── Indexes ───────────────────────────────────────────────────────────────────
async def init_user_db(db) -> None:
    """Create required indexes in the user's database."""
    await _col_sent_ids(db).create_index("file_id", unique=True)
    await _col_scan_index(db).create_index("match_key", unique=True)


# ── State persistence ─────────────────────────────────────────────────────────
async def load_state(db) -> dict:
    doc = await _col_state(db).find_one({"_id": "transfer_state"})
    return doc if doc else {"last_id": None}


async def save_state(db, last_id) -> None:
    await _col_state(db).update_one(
        {"_id": "transfer_state"},
        {"$set": {"last_id": str(last_id) if last_id else None}},
        upsert=True)


async def clear_state(db) -> None:
    await _col_state(db).update_one(
        {"_id": "transfer_state"},
        {"$set": {"last_id": None}},
        upsert=True)


# ── Dedup helpers ─────────────────────────────────────────────────────────────
async def filter_already_sent(db, file_ids: list) -> set:
    """
    Given a list of file_ids, return the subset (as str) already present in
    c7_sent_ids — one $in query instead of one find_one per file_id, so
    callers can dedup a whole fetched batch (up to thousands of docs) in a
    single round-trip instead of one per document.
    """
    if not file_ids:
        return set()
    cursor = _col_sent_ids(db).find(
        {"file_id": {"$in": [str(fid) for fid in file_ids]}}, {"_id": 0, "file_id": 1})
    return {doc["file_id"] async for doc in cursor}


async def mark_as_sent(db, file_ids: list) -> None:
    if not file_ids:
        return
    try:
        await _col_sent_ids(db).insert_many(
            [{"file_id": str(fid)} for fid in file_ids],
            ordered=False)
    except Exception as e:
        bulk_ok = False
        try:
            from pymongo.errors import BulkWriteError
            if isinstance(e, BulkWriteError):
                codes = {err.get("code") for err in e.details.get("writeErrors", [])}
                bulk_ok = codes <= {11000}
        except ImportError:
            pass
        if not bulk_ok:
            logger.error(f"[DB] mark_as_sent write failed  err={e}")


async def clear_sent_ids(db) -> None:
    await _col_sent_ids(db).delete_many({})


# ── Scan index helpers ────────────────────────────────────────────────────────
async def index_channel_key(db, keys: list) -> None:
    if not keys:
        return
    try:
        await _col_scan_index(db).insert_many(
            [{"match_key": str(k).lower().strip()} for k in keys if k],
            ordered=False)
    except Exception as e:
        bulk_ok = False
        try:
            from pymongo.errors import BulkWriteError
            if isinstance(e, BulkWriteError):
                codes = {err.get("code") for err in e.details.get("writeErrors", [])}
                bulk_ok = codes <= {11000}
        except ImportError:
            pass
        if not bulk_ok:
            logger.error(f"[DB] index_channel_key write failed  err={e}")


async def filter_in_channel_index(db, keys_by_file: dict) -> set:
    """
    Given {file_id_str: [candidate_key, ...]}, return the subset of file_ids
    whose *any* candidate key (file_unique_id / file_name / caption, already
    lowercased+stripped by the caller) matches c7_scan_index — one $in query
    across every key in the batch instead of one find_one per file.
    """
    if not keys_by_file:
        return set()
    all_keys = {k for keys in keys_by_file.values() for k in keys if k}
    if not all_keys:
        return set()
    cursor = _col_scan_index(db).find(
        {"match_key": {"$in": list(all_keys)}}, {"_id": 0, "match_key": 1})
    matched_keys = {doc["match_key"] async for doc in cursor}
    return {
        fid for fid, keys in keys_by_file.items()
        if matched_keys & {k for k in keys if k}
    }


async def clear_channel_index(db) -> None:
    await _col_scan_index(db).delete_many({})


async def get_channel_index_count(db) -> int:
    return await _col_scan_index(db).count_documents({})


async def count_sent_ids(db) -> int:
    return await _col_sent_ids(db).count_documents({})


# ── Stats ─────────────────────────────────────────────────────────────────────
async def get_stats(db, col_name: str) -> dict:
    """
    Return transfer progress stats.
    `col_name` is the user's source data collection name.
    """
    data_col = db[col_name]
    total = await data_col.count_documents({})
    sent  = await _col_sent_ids(db).count_documents({})
    state = await load_state(db)
    return {
        "total_files": total,
        "sent_files":  sent,
        "remaining":   max(0, total - sent),
        "last_id":     state.get("last_id"),
    }
