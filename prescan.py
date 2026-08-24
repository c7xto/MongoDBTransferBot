"""
prescan.py — channel pre-scan (userbot session from user config or prompted)
Multi-user mode: accepts user_cfg dict and a Motor db handle.
"""
from __future__ import annotations

import asyncio
import time

from pyrogram import Client
from pyrogram.errors import SessionPasswordNeeded

import config as cfg
import crypto
import target_resolve
import ui
from source_docs import SOURCE_PROJECTION, get_file_id, get_match_keys
from user_db import (
    count_sent_ids,
    filter_in_channel_index,
    get_channel_index_count,
    index_channel_key,
    mark_as_sent,
)

L = cfg.logger
_UI_EDIT_INTERVAL = 10.0


async def _edit_status(admin_app: Client, user_id: int, progress: dict,
                       state: str = "running") -> None:
    """Best-effort edit of the persistent Telegram pre-scan card."""
    cfg.prescan_progress[user_id] = dict(progress)
    msg_id = cfg.prescan_msg_ids.get(user_id)
    if not msg_id:
        try:
            msg = await admin_app.send_message(
                user_id,
                ui.build_prescan_card(progress, state),
                reply_markup=ui.prescan_controls(state),
            )
            cfg.prescan_msg_ids[user_id] = msg.id
        except Exception:
            pass
        return
    try:
        await admin_app.edit_message_text(
            user_id,
            msg_id,
            ui.build_prescan_card(progress, state),
            reply_markup=ui.prescan_controls(state),
        )
    except Exception:
        pass


async def run_prescan(
        admin_app: Client,
        user_cfg:  dict,
        source_db,
        state_db=None,
) -> None:
    """
    Pre-scan the user's target channel to build a duplicate index.
    Session can be saved in user_cfg["userbot_session"] (base64 string),
    or the user will be prompted for phone/OTP/2FA via Telegram.
    """
    user_id  = user_cfg["_id"]
    admin_id = user_id
    if state_db is None:
        raise RuntimeError("Host-owned state database is required")

    L.info(f"[SCAN] Pre-scan started  user={user_id}")
    started_at = time.monotonic()
    progress = {
        "phase": "Preparing database and Telegram session",
        "step": 0,
        "current": 0,
        "total": 0,
        "elapsed": 0.0,
    }

    def _progress(**updates) -> dict:
        progress.update(updates)
        progress["elapsed"] = time.monotonic() - started_at
        return dict(progress)

    # mdb.py normally creates this card before scheduling the task so even
    # database connection setup is visible. This fallback also covers direct
    # calls and keeps the operation observable if that initial send failed.
    if not cfg.prescan_msg_ids.get(user_id):
        try:
            msg = await admin_app.send_message(
                admin_id,
                ui.build_prescan_card(_progress()),
                reply_markup=ui.prescan_controls("running"),
            )
            cfg.prescan_msg_ids[user_id] = msg.id
        except Exception:
            pass
    else:
        await _edit_status(admin_app, user_id, _progress())

    stored_session = (user_cfg.get("userbot_session") or "").strip()
    api_id   = user_cfg["api_id"]
    api_hash = user_cfg["api_hash"]

    session_string = ""
    if stored_session:
        try:
            session_string = crypto.decrypt_str(stored_session)
        except ValueError:
            # Encrypted under a since-rotated ENCRYPTION_KEY, or corrupted —
            # fall back to a fresh login rather than crashing the pre-scan.
            L.warning(
                f"[SCAN] Stored session could not be decrypted (key rotated or "
                f"corrupt) — clearing and re-prompting login  user={user_id}")
            from db import update_user_field
            await update_user_field(user_id, "userbot_session", None)

    async def _ask(prompt: str, retries: int = 3) -> str:
        """Ask the user for sensitive input via Telegram."""
        for attempt in range(1, retries + 1):
            fut = asyncio.get_running_loop().create_future()
            cfg.scan_auth_futures[user_id] = fut
            note = f" _(attempt {attempt}/{retries})_" if attempt > 1 else ""
            await admin_app.send_message(admin_id, prompt + note)
            try:
                result = await asyncio.wait_for(fut, timeout=120)
                # Delete the sensitive reply to keep the chat clean
                try:
                    async for msg in admin_app.get_chat_history(admin_id, limit=1):
                        if msg.from_user and msg.from_user.id == admin_id:
                            await msg.delete()
                except Exception:
                    pass
                return result
            except asyncio.TimeoutError:
                if attempt == retries:
                    raise RuntimeError("⏰ Timed out. Run pre-scan again.")
                L.warning(f"[SCAN] Auth input timeout attempt {attempt}/{retries}  user={user_id}")
                await admin_app.send_message(admin_id, "⏰ No response — trying again…")
            finally:
                cfg.scan_auth_futures.pop(user_id, None)
        raise RuntimeError("Auth abandoned.")

    try:
        if not session_string:
            L.warning(f"[SCAN] No userbot_session — prompting login  user={user_id}")
            await admin_app.send_message(
                admin_id,
                f"🔐 **Pre-Scan Login Required**\n`{cfg.SEP2}`\n"
                f"A **userbot session** is needed to read channel history.\n"
                f"After first login, the session is saved so you won't be asked again.")

            phone = await _ask("📱 **Send your phone number:**\n`+1234567890`")
            L.info(f"[SCAN] Phone received — sending OTP  user={user_id}")

            tmp = Client(
                f"tmp_prescan_{user_id}",
                api_id=api_id,
                api_hash=api_hash,
                in_memory=True,
                workdir=cfg.SESSIONS_DIR)
            await tmp.connect()
            auth_ok = False

            try:
                sent = await tmp.send_code(phone)
                otp  = await _ask("🔑 **Enter the OTP** Telegram sent you:\n_(digits only)_")
                L.info(f"[SCAN] OTP received — signing in  user={user_id}")

                try:
                    await tmp.sign_in(phone, sent.phone_code_hash, otp)
                    auth_ok = True
                except SessionPasswordNeeded:
                    L.info(f"[SCAN] 2FA required  user={user_id}")
                    pwd = await _ask("🔐 **2FA enabled** — send your cloud password:")
                    await tmp.check_password(pwd)
                    auth_ok = True

                if auth_ok:
                    session_string = await tmp.export_session_string()
                    L.info(f"[SCAN] Userbot authenticated — session obtained  user={user_id}")

                    # Persist session (encrypted — it's a live Telegram account
                    # login, not just a bot-scoped token) so user won't be
                    # prompted again
                    from db import update_user_field
                    await update_user_field(
                        user_id, "userbot_session", crypto.encrypt_str(session_string))

                    await admin_app.send_message(
                        admin_id,
                        "✅ **Logged in!** Session saved to your profile.\n\n"
                        "Starting scan…")
            finally:
                await tmp.disconnect()
                if not auth_ok:
                    L.warning(f"[SCAN] Auth failed  user={user_id}")
                    raise RuntimeError("Authentication failed.")
        else:
            L.info(f"[SCAN] Session loaded from user profile  user={user_id}")

        # ── Step 1: Scan channel ───────────────────────────────────────────────
        async with Client(
            f"prescan_worker_{user_id}",
            session_string=session_string,
            api_id=api_id,
            api_hash=api_hash,
            in_memory=True,
            workdir=cfg.SESSIONS_DIR,
        ) as worker:

            target = str(user_cfg["target"])
            await _edit_status(admin_app, user_id, _progress(
                phase="Resolving target channel",
                step=1,
            ))
            target_id = await target_resolve.resolve_target_chat_id(
                worker, target, user_id=user_id, raise_friendly_error=True)

            L.info(f"[SCAN] Step 1/3 — scanning channel  target={target_id}  user={user_id}")

            channel_total = 0
            await _edit_status(admin_app, user_id, _progress(
                phase="Scanning target channel for existing files",
                step=1,
                current=0,
                total=0,
                messages=0,
                keys=0,
            ))
            try:
                counter = getattr(worker, "get_chat_history_count", None)
                if counter:
                    channel_total = int(await counter(target_id))
            except Exception:
                # A total improves the bar but is not required for a safe scan.
                channel_total = 0

            channel_keys: set = set()
            msg_count = 0
            last_ui_edit = 0.0
            await _edit_status(admin_app, user_id, _progress(
                phase="Scanning target channel for existing files",
                step=1,
                current=0,
                total=channel_total,
                messages=0,
                keys=0,
            ))

            async for message in worker.get_chat_history(target_id):
                media = (message.document or message.video
                         or message.audio or message.photo)
                if media:
                    if getattr(media, "file_unique_id", None):
                        channel_keys.add(media.file_unique_id.lower())
                    if getattr(media, "file_name", None):
                        channel_keys.add(media.file_name.lower().strip())
                if message.caption:
                    channel_keys.add(message.caption.lower().strip())
                msg_count += 1

                if msg_count % 500 == 0:
                    L.info(f"[SCAN] Step 1/3 progress  messages={msg_count:,}  "
                           f"keys={len(channel_keys):,}  user={user_id}")
                    now = time.monotonic()
                    if now - last_ui_edit >= _UI_EDIT_INTERVAL:
                        await _edit_status(admin_app, user_id, _progress(
                            current=msg_count,
                            messages=msg_count,
                            keys=len(channel_keys),
                        ))
                        last_ui_edit = now

            await index_channel_key(state_db, list(channel_keys))
            total_indexed = await get_channel_index_count(state_db)
            L.info(f"[SCAN] Step 1/3 done  messages={msg_count:,}  "
                   f"keys_indexed={total_indexed:,}  user={user_id}")

            # ── Step 2: Cross-reference with MongoDB ───────────────────────────
            col = source_db[user_cfg["col_name"]]
            total_docs = await col.count_documents({})

            L.info(f"[SCAN] Step 2/3 — cross-referencing MongoDB  "
                   f"total_docs={total_docs:,}  user={user_id}")

            await _edit_status(admin_app, user_id, _progress(
                phase="Cross-referencing MongoDB with the channel index",
                step=2,
                current=0,
                total=total_docs,
                messages=msg_count,
                keys=total_indexed,
                checked=0,
            ))
            last_ui_edit = 0.0

            matched: list = []
            checked = 0

            # Cross-reference in batches of 1000: one $in query per batch
            # against c7_scan_index instead of one is_in_channel_index()
            # round-trip per document — the doc count here is exactly the
            # kind of "millions of files" scale this needs to survive.
            CHECK_BATCH = 1000
            doc_buffer: list = []

            async def _flush_buffer(buf: list) -> None:
                if not buf:
                    return
                keys_by_file = {
                    get_file_id(d): get_match_keys(d)
                    for d in buf if get_file_id(d)
                }
                hits = await filter_in_channel_index(state_db, keys_by_file)
                matched.extend(hits)

            async for doc in col.find({}, SOURCE_PROJECTION).sort("_id", 1):
                doc_buffer.append(doc)
                checked += 1

                if len(doc_buffer) >= CHECK_BATCH:
                    await _flush_buffer(doc_buffer)
                    doc_buffer = []

                if len(matched) >= 500:
                    await mark_as_sent(state_db, matched)
                    matched = []

                if checked % 2000 == 0:
                    L.info(f"[SCAN] Step 2/3 progress  checked={checked:,}/{total_docs:,}  "
                           f"user={user_id}")
                    now = time.monotonic()
                    if now - last_ui_edit >= _UI_EDIT_INTERVAL:
                        await _edit_status(admin_app, user_id, _progress(
                            current=checked,
                            checked=checked,
                        ))
                        last_ui_edit = now

            await _flush_buffer(doc_buffer)
            if matched:
                await mark_as_sent(state_db, matched)

            # ── Step 3: Summary ───────────────────────────────────────────────
            skippable = await count_sent_ids(state_db)
            fresh     = max(0, total_docs - skippable)

            L.info(f"[SCAN] Pre-scan complete  skip={skippable:,}  "
                   f"fresh={fresh:,}  user={user_id}")

            await _edit_status(admin_app, user_id, _progress(
                phase="Duplicates indexed — ready to launch",
                step=3,
                current=checked,
                total=total_docs,
                checked=checked,
                skippable=skippable,
                fresh=fresh,
            ), state="done")

    except asyncio.CancelledError:
        L.info(f"[SCAN] Pre-scan stopped by user  user={user_id}")
        await _edit_status(admin_app, user_id, _progress(
            phase="Stopped by user — no transfer was started",
        ), state="stopped")
        raise

    except Exception as e:
        err = str(e)
        if "PASSWORD_HASH_INVALID" in err:
            L.warning(f"[SCAN] Failed — wrong 2FA password  user={user_id}")
            err = "Wrong 2FA password. Run pre-scan again."
        elif "PHONE_CODE_INVALID" in err:
            L.warning(f"[SCAN] Failed — wrong OTP  user={user_id}")
            err = "Wrong OTP. Run pre-scan again."
        elif "PHONE_NUMBER_INVALID" in err:
            L.warning(f"[SCAN] Failed — invalid phone format  user={user_id}")
            err = "Invalid phone format. Use: `+1234567890`"
        elif "SESSION_REVOKED" in err or "AUTH_KEY" in err:
            L.warning(f"[SCAN] Session expired  user={user_id}")
            # Clear the stale session
            from db import update_user_field
            await update_user_field(user_id, "userbot_session", None)
            err = "Session expired and cleared. Run pre-scan again to re-login."
        else:
            L.exception(f"[SCAN] Pre-scan error  user={user_id}")

        await _edit_status(admin_app, user_id, _progress(
            phase="Pre-scan could not continue",
            error=err,
        ), state="failed")
    finally:
        cfg.scan_auth_futures.pop(user_id, None)
        cfg.prescan_msg_ids.pop(user_id, None)
        cfg.prescan_progress.pop(user_id, None)
