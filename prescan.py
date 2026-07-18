"""
prescan.py — channel pre-scan (userbot session from user config or prompted)
Multi-user mode: accepts user_cfg dict and a Motor db handle.
"""
from __future__ import annotations
import asyncio
from pyrogram import Client
from pyrogram.errors import SessionPasswordNeeded
import config as cfg
import crypto
import target_resolve
from user_db import (
    index_channel_key, get_channel_index_count,
    filter_in_channel_index, mark_as_sent, count_sent_ids,
)

L = cfg.logger


async def run_prescan(
        admin_app: Client,
        user_cfg:  dict,
        user_db,              # AsyncIOMotorDatabase for the user's cluster
) -> None:
    """
    Pre-scan the user's target channel to build a duplicate index.
    Session can be saved in user_cfg["userbot_session"] (base64 string),
    or the user will be prompted for phone/OTP/2FA via Telegram.
    """
    user_id  = user_cfg["_id"]
    admin_id = user_id

    L.info(f"[SCAN] Pre-scan started  user={user_id}")

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
                        f"✅ **Logged in!** Session saved to your profile.\n\n"
                        f"Starting scan…")
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
            target_id = await target_resolve.resolve_target_chat_id(
                worker, target, user_id=user_id, raise_friendly_error=True)

            L.info(f"[SCAN] Step 1/3 — scanning channel  target={target_id}  user={user_id}")

            channel_keys: set = set()
            msg_count = 0

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

            await index_channel_key(user_db, list(channel_keys))
            total_indexed = await get_channel_index_count(user_db)
            L.info(f"[SCAN] Step 1/3 done  messages={msg_count:,}  "
                   f"keys_indexed={total_indexed:,}  user={user_id}")

            # ── Step 2: Cross-reference with MongoDB ───────────────────────────
            # user_db already points at the same database — reuse it.
            col = user_db[user_cfg["col_name"]]
            total_docs = await col.count_documents({})

            L.info(f"[SCAN] Step 2/3 — cross-referencing MongoDB  "
                   f"total_docs={total_docs:,}  user={user_id}")

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
                    str(d["_id"]): [
                        str(d["_id"]),
                        (d.get("file_name") or "").lower().strip(),
                        (d.get("caption")   or "").lower().strip(),
                    ]
                    for d in buf
                }
                hits = await filter_in_channel_index(user_db, keys_by_file)
                matched.extend(hits)

            async for doc in col.find(
                    {}, {"_id": 1, "file_name": 1, "caption": 1}).sort("_id", 1):
                doc_buffer.append(doc)
                checked += 1

                if len(doc_buffer) >= CHECK_BATCH:
                    await _flush_buffer(doc_buffer)
                    doc_buffer = []

                if len(matched) >= 500:
                    await mark_as_sent(user_db, matched)
                    matched = []

                if checked % 2000 == 0:
                    L.info(f"[SCAN] Step 2/3 progress  checked={checked:,}/{total_docs:,}  "
                           f"user={user_id}")

            await _flush_buffer(doc_buffer)
            if matched:
                await mark_as_sent(user_db, matched)

            # ── Step 3: Summary ───────────────────────────────────────────────
            skippable = await count_sent_ids(user_db)
            fresh     = max(0, total_docs - skippable)

            L.info(f"[SCAN] Pre-scan complete  skip={skippable:,}  "
                   f"fresh={fresh:,}  user={user_id}")

            await admin_app.send_message(
                admin_id,
                f"✅ **Pre-Scan Complete!**\n`{cfg.SEP}`\n"
                f"📡 Channel messages › `{msg_count:,}`\n"
                f"📦 MongoDB docs › `{checked:,}`\n"
                f"⏭  Will skip › `{skippable:,}`\n"
                f"🆕 Will transfer › `{fresh:,}`\n`{cfg.SEP}`\n"
                f"Duplicates locked out — ready to launch.")

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

        try:
            await admin_app.send_message(
                admin_id,
                f"❌ **Pre-Scan Failed**\n`{cfg.SEP2}`\n{err}")
        except Exception:
            pass
    finally:
        cfg.scan_auth_futures.pop(user_id, None)
