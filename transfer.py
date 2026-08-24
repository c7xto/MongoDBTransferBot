"""
transfer.py — file transfer engine
Multi-user mode: accepts user_cfg dict and a Motor db handle.
Includes live progress card editing (throttled to every 15 s) and
a pause gate that sleeps the loop without blocking the event loop.
"""
from __future__ import annotations

import asyncio
import io
import random
import time
from collections import deque

from bson import ObjectId
from pyrogram import Client
from pyrogram.errors import AuthKeyDuplicated, FloodWait
from pyrogram.types import (
    InputMediaAudio,
    InputMediaDocument,
    InputMediaPhoto,
    InputMediaVideo,
)

import config as cfg
import target_resolve
import ui
from source_docs import SOURCE_PROJECTION, get_file_id, get_match_keys
from user_db import (
    clear_state,
    count_sent_ids,
    filter_already_sent,
    filter_in_channel_index,
    load_state,
    mark_as_sent,
    save_state,
)

L = cfg.logger

_VIDEO_EXTS = {
    ".mp4", ".mkv", ".avi", ".mov", ".webm",
    ".flv", ".wmv", ".m4v", ".3gp", ".ts", ".m2ts",
}

# How often (seconds) the live Telegram card is edited while a transfer runs.
# Keep ≥ 10 s to stay well clear of Telegram's edit rate limit.
_UI_EDIT_INTERVAL = 10.0

# Network resilience: how long to pause after a connection drop, and how many
# times to retry a single batch before giving up the entire transfer.
_CONN_RETRY_WAIT  = 15
_CONN_MAX_RETRIES = 8


def _is_video(doc: dict) -> bool:
    mime = (doc.get("mime_type") or "").lower()
    if mime.startswith("video/"):
        return True
    fname = (doc.get("file_name") or "").lower()
    if "." in fname:
        return ("." + fname.rsplit(".", 1)[-1]) in _VIDEO_EXTS
    return False


def _media_kind(doc: dict) -> str:
    """Resolve the Telegram media class without relying on obsolete ID prefixes."""
    declared = str(doc.get("file_type") or "").lower().strip()
    if declared in {"document", "video", "audio", "photo"}:
        return declared

    mime = str(doc.get("mime_type") or "").lower().strip()
    if mime.startswith("video/"):
        return "video"
    if mime.startswith("audio/"):
        return "audio"
    if mime.startswith("image/"):
        return "photo"
    if mime:
        return "document"

    # Retain compatibility with older Bot API file IDs. Modern file IDs no
    # longer expose a stable media-class prefix, so unknown values fall back to
    # document unless their filename clearly identifies a video.
    fid = get_file_id(doc) or ""
    if fid.startswith("AgAD"):
        return "photo"
    if fid.startswith("CQAD"):
        return "audio"
    if fid.startswith("BQAD"):
        return "document"
    return "video" if _is_video(doc) else "document"


def _make_media_item(doc: dict):
    cap = f"<code>{doc.get('caption') or doc.get('file_name', 'No Title')}</code>"
    fid = get_file_id(doc)
    if not fid:
        raise ValueError("Source document has no Telegram file_id")
    kind = _media_kind(doc)
    if kind == "document":
        return InputMediaDocument(media=fid, caption=cap)
    if kind == "photo":
        return InputMediaPhoto(media=fid, caption=cap)
    if kind == "audio":
        return InputMediaAudio(media=fid, caption=cap)
    return InputMediaVideo(media=fid, caption=cap)


async def _send_single(worker: Client, target_id: int, doc: dict) -> None:
    """
    Send one file, auto-detecting document vs video type.
    Three-attempt chain:
      1. send_cached_media (type-agnostic)
      2. Type-appropriate send (send_video or send_document)
      3. Swap type on mismatch error and retry once
    FloodWait is caught at every attempt and paused on via
    asyncio.sleep(e.value) before the retry, instead of falling through
    to the next attempt and spamming Telegram while still rate-limited.
    """
    cap  = f"<code>{doc.get('caption') or doc.get('file_name', 'No Title')}</code>"
    fid  = get_file_id(doc)
    if not fid:
        raise ValueError("Source document has no Telegram file_id")
    is_v = _media_kind(doc) == "video"

    try:
        await worker.send_cached_media(chat_id=target_id, file_id=fid, caption=cap)
        return
    except FloodWait as e:
        await asyncio.sleep(e.value)
        await worker.send_cached_media(chat_id=target_id, file_id=fid, caption=cap)
        return
    except Exception:
        pass

    try:
        if is_v:
            await worker.send_video(chat_id=target_id, video=fid, caption=cap)
        else:
            await worker.send_document(chat_id=target_id, document=fid, caption=cap)
        return
    except FloodWait as e:
        await asyncio.sleep(e.value)
        if is_v:
            await worker.send_video(chat_id=target_id, video=fid, caption=cap)
        else:
            await worker.send_document(chat_id=target_id, document=fid, caption=cap)
        return
    except Exception as e:
        err = str(e)

    if "Expected DOCUMENT, got VIDEO" in err:
        try:
            await worker.send_video(chat_id=target_id, video=fid, caption=cap)
        except FloodWait as e:
            await asyncio.sleep(e.value)
            await worker.send_video(chat_id=target_id, video=fid, caption=cap)
        return
    if "Expected VIDEO, got DOCUMENT" in err:
        try:
            await worker.send_document(chat_id=target_id, document=fid, caption=cap)
        except FloodWait as e:
            await asyncio.sleep(e.value)
            await worker.send_document(chat_id=target_id, document=fid, caption=cap)
        return

    raise RuntimeError(err)


def _cast_last_id(last_id, sample_id):
    if last_id is None:
        return None
    if isinstance(sample_id, ObjectId):
        try:
            return ObjectId(str(last_id))
        except Exception:
            return last_id
    if isinstance(sample_id, int):
        try:
            return int(last_id)
        except Exception:
            return last_id
    return str(last_id)


async def _edit_card(
        admin_app: Client,
        admin_id:  int,
        user_id:   int,
        count:     int,
        total:     int,
        elapsed:   float,
        failed:    int,
        state:     str,
        progress:  dict | None = None,
) -> None:
    """
    Edit the live progress card for this user.
    Silently swallows all errors — UI failures must never kill the transfer.
    Pops the msg_id from cfg for terminal states (stopped / done) so the
    transfer loop never attempts a second edit on a closed card.
    """
    msg_id = cfg.progress_msg_ids.get(user_id)
    if not msg_id:
        return
    try:
        await admin_app.edit_message_text(
            admin_id,
            msg_id,
            (ui.build_progress_snapshot_card(progress, state)
             if progress else
             ui.build_progress_card(count, total, elapsed, failed, state)),
            reply_markup=ui.live_controls(state))
    except Exception:
        pass
    if state in ("stopped", "done"):
        cfg.progress_msg_ids.pop(user_id, None)


async def _ensure_worker_connected(worker: Client, user_id: int) -> None:
    """Re-connect a Pyrogram worker that has dropped, ignoring all errors."""
    if worker.is_connected:
        return
    L.warning(f"[CONN] Worker disconnected — reconnecting  user={user_id}")
    for attempt in range(1, 4):
        try:
            await worker.start()
            L.info(f"[CONN] Worker reconnected  user={user_id}")
            return
        except FloodWait as fw:
            L.warning(f"[CONN] FloodWait {fw.value}s on worker reconnect  user={user_id}")
            await asyncio.sleep(fw.value + 5)
        except Exception as e:
            L.warning(f"[CONN] Worker reconnect attempt {attempt}/3 failed  err={e}  user={user_id}")
            if attempt < 3:
                await asyncio.sleep(10)
    L.error(f"[CONN] Worker reconnect failed after 3 attempts  user={user_id}")


async def _refresh_worker_session(worker: Client, user_id: int) -> None:
    """
    Force-cycle a Pyrogram worker session (used after AuthKeyDuplicated or
    silent disconnection).  Stops the client, waits 5 s, then restarts with
    FloodWait awareness.  Errors are logged but never re-raised so the
    transfer loop can decide whether to continue.
    """
    L.warning(f"[CONN] Refreshing worker session  user={user_id}")
    try:
        await worker.stop()
    except Exception:
        pass
    await asyncio.sleep(5)
    for attempt in range(1, 4):
        try:
            await worker.start()
            L.info(f"[CONN] Worker session refreshed successfully  user={user_id}")
            return
        except FloodWait as fw:
            L.warning(f"[CONN] FloodWait {fw.value}s during session refresh  user={user_id}")
            await asyncio.sleep(fw.value + 5)
        except Exception as e:
            L.warning(f"[CONN] Session refresh attempt {attempt}/3 failed  err={e}  user={user_id}")
            if attempt < 3:
                await asyncio.sleep(10)
    L.error(f"[CONN] Session refresh failed after 3 attempts  user={user_id}")


async def run_transfer(
        worker:     Client,
        admin_app:  Client,
        user_cfg:   dict,
        source_db,
        state_db=None,
) -> None:
    """
    Main transfer engine.
    user_cfg keys used: mongo_uri, db_name, col_name, target, speed_delay, _id.
    ``source_db`` is read-only catalogue access. ``state_db`` stores mutable
    checkpoints and duplicate records on the host-owned database.
    """
    user_id    = user_cfg["_id"]
    col_name   = user_cfg["col_name"]
    admin_id   = user_id
    speed_cfg  = float(user_cfg.get("speed_delay", 3.5))

    # The caller (mdb.py) already claimed active_transfers[user_id] = True
    # under its per-user launch lock before scheduling this coroutine — see
    # CLAUDE.md Phase 1 TOCTOU fix. Keep this pop as a harmless, idempotent
    # safety net.
    cfg.paused_transfers.pop(user_id, None)

    failed_files: list = []
    transfer_start = time.time()
    current_speed  = speed_cfg
    last_ui_edit   = 0.0       # epoch time of the last card edit

    # Initialise to safe defaults so finally-block can always reference them
    count = 0
    total = 0
    sent_files = 0
    skipped_files = 0
    send_total = 0
    delivery_rate = 0.0
    eta_seconds = 0.0
    rate_samples: deque[tuple[float, int]] = deque()
    if state_db is None:
        raise RuntimeError("Host-owned state database is required")

    def _snapshot(now: float | None = None) -> dict:
        """Build a live snapshot using recent successful Telegram sends."""
        nonlocal delivery_rate, eta_seconds
        now = now or time.time()
        rate_samples.append((now, sent_files))
        cutoff = now - 300.0
        while len(rate_samples) > 2 and rate_samples[1][0] < cutoff:
            rate_samples.popleft()

        first_t, first_sent = rate_samples[0]
        span = now - first_t
        delivered = sent_files - first_sent
        if delivered > 0 and span > 0:
            delivery_rate = delivered / span
        elif sent_files > 0 and now > transfer_start:
            delivery_rate = sent_files / (now - transfer_start)

        remaining = max(0, send_total - sent_files - len(failed_files))
        eta_seconds = remaining / delivery_rate if delivery_rate > 0 else 0.0
        snapshot = {
            "count": count,
            "total": total,
            "elapsed": now - transfer_start,
            "failed": len(failed_files),
            "sent": sent_files,
            "skipped": skipped_files,
            "send_total": send_total,
            "delivery_rate": delivery_rate,
            "eta_seconds": eta_seconds,
        }
        cfg.transfer_progress[user_id] = snapshot
        return snapshot

    L.info(f"[XFER] Transfer started  user={user_id}  speed={current_speed}s")

    try:
        # ── Pre-flight: resolve target channel ────────────────────────────────
        L.info(f"[XFER] Running pre-flight checks  user={user_id}")
        target = str(user_cfg["target"])
        target_id = await target_resolve.resolve_target_chat_id(worker, target)

        test = await worker.send_message(target_id, "🔄 C7 › Connection Test")
        await test.delete()
        L.info(f"[XFER] Pre-flight passed  target={target_id}  user={user_id}")

        # ── Connect to user's source data collection ──────────────────────────
        # source_db is already connected to db_name — reuse its client rather than
        # opening a second connection pool just for the source collection.
        col = source_db[col_name]

        state = await load_state(state_db)
        total = await col.count_documents({})
        sent_before = await count_sent_ids(state_db)
        send_total = max(0, total - sent_before)
        rate_samples.append((transfer_start, 0))

        # Keyset pagination is safe only for naturally ordered identifiers.
        # Many auto-filter databases put a Telegram file_id string in `_id`;
        # a newly inserted string can sort before the saved cursor and would
        # then be missed forever. Such schemas use restart-safe offset scans,
        # with the persistent sent ledger providing idempotency.
        sample = await col.find_one({}, {"_id": 1})
        sample_id = sample.get("_id") if sample else None
        cursor_mode = "keyset" if isinstance(sample_id, (ObjectId, int)) else "offset"
        if state.get("mode") not in (None, cursor_mode):
            state = {"last_id": None, "offset": 0}

        raw_last_id = state.get("last_id")
        last_id = (_cast_last_id(raw_last_id, sample_id)
                   if cursor_mode == "keyset" and raw_last_id else None)
        scan_offset = max(0, int(state.get("offset") or 0)) if cursor_mode == "offset" else 0
        if cursor_mode == "keyset" and last_id is not None:
            count = await col.count_documents({"_id": {"$lte": last_id}})
        else:
            count = min(scan_offset, total)

        L.info(f"[XFER] Collection loaded  total={total:,}  "
               f"already_sent={sent_before:,}  mode={cursor_mode}  "
               f"resumed_from={last_id if cursor_mode == 'keyset' else scan_offset}  user={user_id}")

        # Push initial snapshot so the card shows real totals immediately
        elapsed_now = time.time() - transfer_start
        initial_progress = _snapshot()
        await _edit_card(admin_app, admin_id, user_id,
                         count, total, elapsed_now, 0, ui.TransferState.RUNNING,
                         initial_progress)
        last_ui_edit = time.time()

        BATCH = 1000
        ALBUM = 10
        keep   = True
        chunks = 0
        # Telegram pacing protects actual delivery calls. Duplicate-only
        # chunks never contact Telegram and must not inherit send delays.
        delivery_chunks = 0
        speed  = current_speed

        while keep and count < total:
            qf = ({"_id": {"$gt": last_id}}
                  if cursor_mode == "keyset" and last_id is not None else {})

            # ── Resilient batch fetch ────────────────────────────────────────
            fetch_attempt = 0
            batch = None
            while True:
                try:
                    cursor = col.find(qf, SOURCE_PROJECTION).sort("_id", 1)
                    if cursor_mode == "offset":
                        cursor = cursor.skip(scan_offset)
                    batch = await cursor.limit(BATCH).to_list(length=BATCH)
                    break  # success
                except Exception as conn_err:
                    fetch_attempt += 1
                    if fetch_attempt > _CONN_MAX_RETRIES:
                        L.error(
                            f"[CONN] Batch fetch failed after {_CONN_MAX_RETRIES} retries "
                            f"— aborting transfer  err={conn_err}  user={user_id}")
                        keep = False
                        break
                    L.warning(
                        f"[CONN] Connection lost (attempt {fetch_attempt}/{_CONN_MAX_RETRIES}) — "
                        f"pausing {_CONN_RETRY_WAIT}s to reconnect…  err={conn_err}  user={user_id}")
                    await asyncio.sleep(_CONN_RETRY_WAIT)
                    col = source_db[col_name]  # Motor reconnects automatically on next use
                    await _ensure_worker_connected(worker, user_id)

            if not keep or not batch:
                break

            # ── Batch dedup: two $in queries for the whole fetched batch ─────
            # (up to 1000 docs) instead of up to two find_one round-trips per
            # document — this is the difference between ~2 DB calls and ~2000
            # DB calls per batch at scale.
            valid_batch = [doc for doc in batch if get_file_id(doc)]
            invalid_count = len(batch) - len(valid_batch)
            if invalid_count:
                L.warning(f"[XFER] Skipping {invalid_count} source document(s) without file_id  user={user_id}")
                count += invalid_count
                skipped_files += invalid_count
                send_total = max(sent_files + len(failed_files), send_total - invalid_count)
            if not valid_batch:
                if cursor_mode == "offset":
                    scan_offset += len(batch)
                else:
                    last_id = batch[-1]["_id"]
                await save_state(
                    state_db,
                    last_id if cursor_mode == "keyset" else None,
                    offset=scan_offset,
                    mode=cursor_mode,
                )
                continue
            batch_file_ids = [get_file_id(doc) for doc in valid_batch]
            already_sent_set = await filter_already_sent(state_db, batch_file_ids)

            keys_by_file = {}
            for doc in valid_batch:
                fid = get_file_id(doc)
                if fid in already_sent_set:
                    continue
                keys_by_file[fid] = get_match_keys(doc)

            in_index_set = await filter_in_channel_index(state_db, keys_by_file)
            if in_index_set:
                await mark_as_sent(state_db, list(in_index_set))
                newly_indexed = in_index_set - already_sent_set
                send_total = max(
                    sent_files + len(failed_files),
                    send_total - len(newly_indexed),
                )

            for i in range(0, len(valid_batch), ALBUM):
                chunk = valid_batch[i:i + ALBUM]

                # ── Pause gate ────────────────────────────────────────────────
                while cfg.paused_transfers.get(user_id):
                    await asyncio.sleep(1)
                    if not cfg.active_transfers.get(user_id):
                        keep = False
                        break
                if not keep:
                    break

                # ── Stop check ────────────────────────────────────────────────
                if not cfg.active_transfers.get(user_id):
                    L.info(f"[XFER] Transfer stopped by user  count={count:,}  user={user_id}")
                    keep = False
                    break

                speed = current_speed

                # ── Duplicate filter (membership check against the batch-level
                # dedup sets computed once above — no per-document DB calls) ──
                fresh       = []
                skipped_ids = []
                for doc in chunk:
                    fid = get_file_id(doc)
                    if fid in already_sent_set or fid in in_index_set:
                        skipped_ids.append(fid)
                        continue
                    fresh.append(doc)

                skipped = len(skipped_ids)
                if skipped:
                    L.info(f"[XFER] Skipped {skipped} duplicate(s)  user={user_id}")
                    skipped_files += skipped

                has_fresh = bool(fresh)
                if not fresh:
                    count   += skipped
                    last_id = chunk[-1]["_id"]
                    chunks  += 1
                else:
                    chunk      = fresh
                    media_group = [_make_media_item(doc) for doc in chunk]

                    try:
                        await worker.send_media_group(chat_id=target_id, media=media_group)
                        await mark_as_sent(state_db, [get_file_id(d) for d in chunk])
                        sent_files += len(chunk)
                        count  += len(chunk) + skipped
                        last_id = chunk[-1]["_id"]

                    except FloodWait as fw:
                        current_speed = min(current_speed + 0.5, 6.0)
                        speed = current_speed
                        L.warning(f"[XFER] FloodWait {fw.value}s — speed bumped to "
                                   f"{current_speed}s  user={user_id}")
                        await asyncio.sleep(fw.value + 5)

                        first_fid = get_file_id(chunk[0])
                        already_sent = bool(await filter_already_sent(state_db, [first_fid]))
                        if already_sent:
                            L.info(f"[XFER] FloodWait retry skipped — original send succeeded  user={user_id}")
                            sent_files += len(chunk)
                            count  += len(chunk) + skipped
                            last_id = chunk[-1]["_id"]
                        else:
                            try:
                                await worker.send_media_group(chat_id=target_id, media=media_group)
                                await mark_as_sent(state_db, [get_file_id(d) for d in chunk])
                                sent_files += len(chunk)
                                count  += len(chunk) + skipped
                                last_id = chunk[-1]["_id"]
                                L.info(f"[XFER] FloodWait retry succeeded  user={user_id}")
                            except Exception as re:
                                for doc in chunk:
                                    failed_files.append(f"ID: {get_file_id(doc)} | {re}")
                                count  += len(chunk) + skipped
                                last_id = chunk[-1]["_id"]
                                L.error(f"[XFER] FloodWait retry failed  err={re}  user={user_id}")

                    except Exception as batch_err:
                        # ── Session error: refresh then retry the whole album ──
                        _is_session_err = (
                            isinstance(batch_err, AuthKeyDuplicated)
                            or "AuthKeyDuplicated" in str(batch_err)
                            or "SESSION_REVOKED"   in str(batch_err)
                            or not worker.is_connected
                        )
                        _retry_ok = False
                        if _is_session_err:
                            L.warning(
                                f"[CONN] Session error detected — refreshing worker "
                                f"before retry  err={batch_err}  user={user_id}")
                            await _refresh_worker_session(worker, user_id)
                            try:
                                await worker.send_media_group(chat_id=target_id, media=media_group)
                                await mark_as_sent(state_db, [get_file_id(d) for d in chunk])
                                sent_files += len(chunk)
                                count  += len(chunk) + skipped
                                last_id = chunk[-1]["_id"]
                                L.info(f"[CONN] Post-refresh retry succeeded  user={user_id}")
                                _retry_ok = True
                            except Exception as retry_err:
                                L.warning(
                                    f"[CONN] Post-refresh retry failed — falling back to "
                                    f"individual sends  err={retry_err}  user={user_id}")

                        if not _retry_ok:
                            L.warning(f"[XFER] Batch send failed — falling back to individual  "
                                       f"err={batch_err}  user={user_id}")
                            for doc in chunk:
                                try:
                                    await _send_single(worker, target_id, doc)
                                    await mark_as_sent(state_db, [get_file_id(doc)])
                                    sent_files += 1
                                except Exception as sub_e:
                                    failed_files.append(f"ID: {get_file_id(doc)} | {sub_e}")
                                    L.error(f"[XFER] Individual send failed  id={get_file_id(doc)}  "
                                            f"err={sub_e}  user={user_id}")
                                count  += 1
                                last_id = doc["_id"]
                            count += skipped

                    chunks += 1

                if has_fresh:
                    delivery_chunks += 1

                # ── Console progress log ──────────────────────────────────────
                if chunks % 5 == 0:
                    pct      = (count / total * 100) if total else 0
                    _filled  = int(round(pct / 100 * 10))
                    _bar     = "■" * _filled + "□" * (10 - _filled)
                    progress_now = _snapshot()
                    _rate = progress_now["delivery_rate"]
                    if _rate > 0:
                        _s = progress_now["eta_seconds"]
                        _eta = (f"{_s/86400:.1f}d" if _s >= 86400
                                else f"{_s/3600:.1f}h" if _s >= 3600
                                else f"{_s/60:.0f}m")
                    else:
                        _eta = "—"
                    _k = lambda n: f"{n/1000:.0f}k" if n >= 1000 else str(n)
                    L.info(
                        f"[XFER] [{_bar}] {pct:.0f}% │ {_k(count)}/{_k(total)} │ "
                        f"sent={_k(sent_files)}/{_k(send_total)} │ "
                        f"ETA: {_eta} │ Rate: {_rate * 60:.1f}/min  user={user_id}")

                if cursor_mode == "offset":
                    # Offset advances by source documents, including malformed
                    # rows, so a bad row cannot trap the scan in a loop.
                    scan_offset += len(batch) if i + ALBUM >= len(valid_batch) else 0

                # ── Persist resume cursor every 10 chunks ─────────────────────
                if chunks % 10 == 0:
                    await save_state(
                        state_db,
                        last_id if cursor_mode == "keyset" else None,
                        offset=scan_offset,
                        mode=cursor_mode,
                    )

                # ── Live progress snapshot (for callback handlers) ─────────────
                now_t       = time.time()
                elapsed_now = now_t - transfer_start
                progress_now = _snapshot(now_t)

                # ── Throttled Telegram card edit (every 15 s) ─────────────────
                if now_t - last_ui_edit >= _UI_EDIT_INTERVAL:
                    await _edit_card(admin_app, admin_id, user_id,
                                     count, total, elapsed_now,
                                     len(failed_files), ui.TransferState.RUNNING,
                                     progress_now)
                    last_ui_edit = now_t   # advance timer regardless of edit result

                # ── Telegram pacing (delivery chunks only) ────────────────────
                # Duplicate-only chunks perform MongoDB membership checks but
                # make no Telegram API calls, so delaying them cannot protect
                # the account and makes resume scans unnecessarily slow.
                if has_fresh:
                    jitter = random.uniform(-0.5, 1.2)
                    await asyncio.sleep(max(0.5, speed + jitter))

                    # Cool only after 500 actual delivery attempts. Counting
                    # duplicate scans here previously added minute-long pauses
                    # while the bot was not sending anything.
                    if delivery_chunks % 500 == 0:
                        break_secs = random.randint(30, 60)
                        L.info(
                            f"💤 [STEALTH] Taking a {break_secs}s micro-cooling break "
                            f"to protect account health…  delivery_chunks={delivery_chunks}  "
                            f"user={user_id}")
                        await asyncio.sleep(break_secs)

        # ── Save final cursor ────────────────────────────────────────────────
        await save_state(
            state_db,
            last_id if cursor_mode == "keyset" else None,
            offset=scan_offset,
            mode=cursor_mode,
        )

        elapsed_final = time.time() - transfer_start

        # ── Transfer completed naturally ──────────────────────────────────────
        if cfg.active_transfers.get(user_id) and count >= total:
            await clear_state(state_db)
            L.info(f"[XFER] Transfer complete  sent={sent_files:,}  skipped={skipped_files:,}  "
                   f"failed={len(failed_files)}  elapsed={elapsed_final:.0f}s  user={user_id}")
            await _edit_card(admin_app, admin_id, user_id,
                             count, total, elapsed_final, len(failed_files), ui.TransferState.DONE,
                             _snapshot())
            try:
                await admin_app.send_message(
                    admin_id,
                    f"✅ **Transfer Complete**\n"
                    f"📤 Sent now › `{sent_files:,}`\n"
                    f"⏭ Skipped › `{skipped_files:,}`\n"
                    f"❌ Failed › `{len(failed_files)}`\n"
                    f"⏱ Elapsed › `{ui._fmt_elapsed(elapsed_final)}`")
            except Exception:
                pass

        # ── Stopped by /stop command (no button, so msg_id still present) ────
        elif cfg.progress_msg_ids.get(user_id):
            await _edit_card(admin_app, admin_id, user_id,
                             count, total, elapsed_final, len(failed_files), ui.TransferState.STOPPED,
                             _snapshot())

    except Exception as e:
        L.exception(f"[XFER] Worker error  err={e}  user={user_id}")

    finally:
        cfg.active_transfers.pop(user_id, None)
        cfg.paused_transfers.pop(user_id, None)

        # If a card is still open (e.g., unhandled exception mid-transfer),
        # give it a terminal edit so the user isn't left with a stale card.
        elapsed_final = time.time() - transfer_start
        leftover_msg  = cfg.progress_msg_ids.get(user_id)
        if leftover_msg:
            await _edit_card(admin_app, admin_id, user_id,
                             count, total, elapsed_final, len(failed_files), ui.TransferState.STOPPED,
                             _snapshot())

        # Clean up progress snapshot
        cfg.transfer_progress.pop(user_id, None)

        if failed_files:
            L.warning(f"[XFER] Sending diagnostics  failed_count={len(failed_files)}  user={user_id}")
            f_obj = io.BytesIO("\n".join(failed_files).encode())
            f_obj.name = "C7_Failed_Transfers.txt"
            try:
                await admin_app.send_document(
                    admin_id,
                    document=f_obj,
                    caption="⚠️ **C7 Diagnostics** — Failed transfers")
            except Exception:
                pass
