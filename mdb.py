"""
╔══════════════════════════════════════════════════════════════════╗
║      ⚡  C7  M O N G O D B  T R A N S F E R  B O T  —  V2.0       ║
║          Multi-User · SaaS-style · MongoDB                       ║
╠══════════════════════════════════════════════════════════════════╣
║  HOST ENV VARS (.env):                                           ║
║    BOT_TOKEN  TELEGRAM_API_ID  TELEGRAM_API_HASH                 ║
║    MONGO_URI  (Master Atlas cluster for user settings)           ║
║    ADMIN_ID   (optional — seeds first user from .env on boot)    ║
║                                                                  ║
║  PER-USER CONFIG (stored in Master DB via /setup):               ║
║    api_id  api_hash  bot_token  mongo_uri                        ║
║    db_name  col_name  target  speed_delay                        ║
╚══════════════════════════════════════════════════════════════════╝
"""
from __future__ import annotations

import asyncio
import os
import sys

# ── Windows event-loop policy (must run before creating any loop) ──
# Selects the loop *implementation* needed on Windows for Proactor/selector
# compatibility. Must be set before the jumpstart block below creates a
# loop, so that loop is actually built with this policy.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ── Event loop jumpstart (must run before importing pyrogram) ──
# Pyrogram's own pyrogram/sync.py calls asyncio.get_event_loop() at import
# time (building sync wrappers for every async method) — and Python 3.14
# removed get_event_loop()'s old fallback of silently creating a loop when
# none exists for the thread; it now raises RuntimeError instead. That
# import-time call happens before any of our code runs, so it has to be
# pre-empted right here, before `from pyrogram import ...` below.
try:
    asyncio.get_event_loop()
except RuntimeError:
    _jumpstart_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_jumpstart_loop)

# ── Third-Party Imports ──
import aiohttp
from aiohttp import web
from pyrogram import Client, enums, idle
from pyrogram.errors import FloodWait
from pyrogram.types import CallbackQuery, Message

import config as cfg
import ui
from db import (
    close_all_user_motor_clients,
    get_all_configured_users,
    get_or_create_user,
    get_user,
    get_user_motor_db,
    get_user_state_db,
    init_master_db,
    list_user_collections,
    list_user_databases,
    mark_user_configured,
    reset_user_config,
    run_user_motor_client_eviction_loop,
    update_user_field,
    update_user_fields,
    validate_bot_token,
    validate_mongo_uri,
)
from monitor import run_monitor
from prescan import run_prescan
from transfer import run_transfer
from user_db import (
    clear_channel_index,
    clear_monitor_resume_token,
    clear_sent_ids,
    clear_state,
    count_sent_ids,
    get_channel_index_count,
    get_stats,
    init_user_db,
    load_state,
)

# ── Parent bot client ─────────────────────────────────────────────────────────
# This single Pyrogram client is the public-facing C7 MDTransfer Bot.
# Per-user worker clients are managed in cfg.active_workers.
app = Client(
    "C7_ParentBot",
    api_id=cfg.HOST_API_ID,
    api_hash=cfg.HOST_API_HASH,
    bot_token=cfg.PARENT_BOT_TOKEN,
    in_memory=True,
    workdir=cfg.SESSIONS_DIR)


# ── Per-user worker client factory ────────────────────────────────────────────
async def get_or_start_worker(user_cfg: dict) -> Client:
    """
    Return a started Pyrogram worker client for the given user.
    Creates and starts a new one if not already cached.
    Uses the USER's own bot_token + api_id/api_hash.
    """
    user_id = user_cfg["_id"]
    if user_id in cfg.active_workers:
        worker = cfg.active_workers[user_id]
        if worker.is_connected:
            return worker
        # Reconnect if disconnected
        await safe_start_client(worker, f"C7 Worker [{user_id}] (reconnect)")
        return worker

    worker = Client(
        f"C7_Worker_{user_id}",
        api_id=user_cfg["api_id"],
        api_hash=user_cfg["api_hash"],
        bot_token=user_cfg["bot_token"],
        in_memory=True,
        workdir=cfg.SESSIONS_DIR)
    await safe_start_client(worker, f"C7 Worker [{user_id}]")
    cfg.active_workers[user_id] = worker
    cfg.logger.info(f"[WORKER] Client started  user={user_id}")
    return worker


async def stop_worker(user_id: int) -> None:
    """Stop and remove a user's cached worker client."""
    worker = cfg.active_workers.pop(user_id, None)
    if worker:
        try:
            await worker.stop()
        except Exception:
            pass
        cfg.logger.info(f"[WORKER] Client stopped  user={user_id}")


async def stop_all_workers() -> None:
    for uid in list(cfg.active_workers.keys()):
        await stop_worker(uid)


async def safe_start_client(client: Client, name: str = "Client") -> None:
    """
    Start a Pyrogram client, handling FloodWait with a visible ticking countdown.
    Retries automatically after the required wait — never crashes on rate-limit.
    """
    while True:
        try:
            await client.start()
            return
        except FloodWait as e:
            wait = e.value
            cfg.logger.warning(f"[FLOOD] {name} hit rate limit — pausing {wait}s before retry")
            for remaining in range(wait, 0, -1):
                if remaining % 10 == 0 or remaining <= 5:
                    cfg.logger.warning(f"[FLOOD] {name} resuming in {remaining}s…")
                await asyncio.sleep(1)
            cfg.logger.info(f"[FLOOD] Reconnecting {name} now…")


# ── Setup wizard state machine ────────────────────────────────────────────────
# Maps setup_step value → human prompt + field name to write
WIZARD_STEPS = {
    "api_id": {
        "prompt": (
            "🔧 **Setup — Step 1/7: API ID**\n`{sep}`\n\n"
            "Visit https://my.telegram.org → **App configuration**\n"
            "and copy your **API ID** (a numeric value).\n\n"
            "Send it now:"),
        "field": "api_id",
        "next":  "api_hash",
    },
    "api_hash": {
        "prompt": (
            "🔧 **Setup — Step 2/7: API Hash**\n`{sep}`\n\n"
            "From the same page, copy your **API Hash** (32-char hex string).\n\n"
            "Send it now:"),
        "field": "api_hash",
        "next":  "bot_token",
    },
    "bot_token": {
        "prompt": (
            "🔧 **Setup — Step 3/7: Bot Token**\n`{sep}`\n\n"
            "This is the token for the **client bot** (e.g. Kuttu Bot) that owns\n"
            "your file_ids and will post to the target channel.\n\n"
            "Send the bot token now:"),
        "field": "bot_token",
        "next":  "mongo_uri",
    },
    "mongo_uri": {
        "prompt": (
            "🔧 **Setup — Step 4/7: MongoDB URI**\n`{sep}`\n\n"
            "Get this from **MongoDB Atlas → Connect → Drivers**.\n"
            "Make sure `0.0.0.0/0` is whitelisted in Network Access.\n\n"
            "Send your connection string now:"),
        "field": "mongo_uri",
        "next":  "db_name",
    },
    "db_name": {
        "prompt": (
            "🔧 **Setup — Step 5/7: Database Name**\n`{sep}`\n\n"
            "Use the **🔍 Browse DBs** button below to pick your database,\n"
            "or type the database name manually."),
        "field": "db_name",
        "next":  "col_name",
    },
    "col_name": {
        "prompt": (
            "🔧 **Setup — Step 6/7: Collection Name**\n`{sep}`\n\n"
            "Use the **🔍 Browse Collections** button below to pick,\n"
            "or type the collection name manually."),
        "field": "col_name",
        "next":  "target",
    },
    "target": {
        "prompt": (
            "🔧 **Setup — Step 7/7: Target Channel**\n`{sep}`\n\n"
            "Forward any message from your channel to @userinfobot\n"
            "to get its numeric ID (e.g. `-1001234567890`).\n"
            "Make your **client bot** an admin in that channel first!\n\n"
            "Send the channel ID now:"),
        "field": "target",
        "next":  None,   # wizard complete
    },
}


async def _send_wizard_step(client: Client, chat_id: int, step: str, user: dict) -> None:
    """Send the prompt for the given setup wizard step."""
    step_cfg = WIZARD_STEPS[step]
    text     = step_cfg["prompt"].format(sep=cfg.SEP2)

    # For db_name step, show browse button if mongo_uri is already set
    if step == "db_name" and user.get("mongo_uri"):
        kb = ui.InlineKeyboardMarkup([[
            ui.InlineKeyboardButton("🔍 Browse DBs", callback_data="explore_dbs"),
        ]])
        await client.send_message(chat_id, text, reply_markup=kb)
    elif step == "col_name" and user.get("db_name"):
        kb = ui.InlineKeyboardMarkup([[
            ui.InlineKeyboardButton("🔍 Browse Collections", callback_data="explore_cols"),
        ]])
        await client.send_message(chat_id, text, reply_markup=kb)
    else:
        await client.send_message(chat_id, text)


async def _process_wizard_input(
        client: Client,
        message: Message,
        user: dict,
        step: str,
) -> None:
    """
    Validate and save user input for the current setup wizard step.
    Advances to the next step on success, or re-prompts on failure.
    """
    user_id = user["_id"]
    text    = message.text.strip() if message.text else ""

    if not text:
        await message.reply("⚠️ Please send a text value.")
        return

    # ── Per-field validation ──────────────────────────────────────────────────
    if step == "api_id":
        if not text.isdigit():
            await message.reply("❌ API ID must be a number. Try again:")
            return
        value = int(text)

    elif step == "api_hash":
        if len(text) != 32 or not all(c in "0123456789abcdef" for c in text.lower()):
            await message.reply("❌ API Hash must be a 32-character hex string. Try again:")
            return
        value = text.lower()

    elif step == "bot_token":
        wait_msg = await message.reply("⏳ Verifying token with Telegram…")
        ok, result = await validate_bot_token(text)
        if not ok:
            await wait_msg.delete()
            await message.reply(f"❌ Invalid token: `{result}`\nTry again:")
            return
        await wait_msg.edit(f"✅ Token verified — bot: **@{result}**")
        value = text

    elif step == "mongo_uri":
        wait_msg = await message.reply("⏳ Pinging your MongoDB cluster…")
        ok, err, sanitized_uri = await validate_mongo_uri(text)
        if not ok:
            await wait_msg.delete()
            await message.reply(
                f"❌ Cannot connect to MongoDB:\n`{err}`\n\n"
                f"Check your URI and whitelist `0.0.0.0/0` in Atlas Network Access. Try again:")
            return
        await wait_msg.edit("✅ MongoDB connection successful!")
        value = sanitized_uri

    elif step == "target":
        cleaned = text.strip()
        if not cleaned.lstrip("-").isdigit() and not cleaned.startswith("@") and "t.me" not in cleaned:
            await message.reply(
                "❌ Send a numeric channel ID (e.g. `-1001234567890`), "
                "@username, or invite link. Try again:")
            return
        value = cleaned

    else:
        # db_name, col_name — accept as-is
        value = text

    # ── Save the field ────────────────────────────────────────────────────────
    field = WIZARD_STEPS[step]["field"]
    await update_user_field(user_id, field, value)

    next_step = WIZARD_STEPS[step]["next"]

    if next_step is None:
        # Wizard complete
        await mark_user_configured(user_id)
        await message.reply(
            f"🎉 **Setup Complete!**\n`{cfg.SEP}`\n\n"
            f"Your configuration has been saved.\n"
            f"Use `/transfer` to start, or `/start` to see the menu.",
            reply_markup=ui.home_menu(user_id))
    else:
        # Advance wizard
        await update_user_field(user_id, "setup_step", next_step)
        updated_user = await get_user(user_id)
        await _send_wizard_step(client, user_id, next_step, updated_user)


# ══════════════════════════════════════════════════════════════════════════════
#  MESSAGE HANDLER
# ══════════════════════════════════════════════════════════════════════════════
@app.on_message()
async def msg_handler(client: Client, message: Message) -> None:
    # Only handle private chats (manual check — more reliable than filters.private
    # across Pyrogram 2.x patch versions on Linux containers)
    if not message.chat or message.chat.type != enums.ChatType.PRIVATE:
        return
    try:
        await _msg_handler_inner(client, message)
    except Exception as exc:
        cfg.logger.exception(f"[SYSTEM] Unhandled error in msg_handler  err={exc}")
        try:
            await message.reply("❌ An internal error occurred. Please try again.")
        except Exception:
            pass


async def _msg_handler_inner(client: Client, message: Message) -> None:
    if not message.text or not message.from_user:
        return

    user_id  = message.from_user.id
    username = message.from_user.username
    text     = message.text.strip()

    # ── Route scan auth input (pre-scan interactive login) ────────────────────
    if user_id in cfg.scan_auth_futures:
        fut = cfg.scan_auth_futures.get(user_id)
        if fut and not fut.done():
            fut.set_result(text)
            return

    # ── Ensure user record exists in master DB ────────────────────────────────
    user = await get_or_create_user(user_id, username)

    # ── Route setup wizard input ──────────────────────────────────────────────
    step = user.get("setup_step")
    if step and not text.startswith("/"):
        await _process_wizard_input(client, message, user, step)
        return

    # ── Commands ──────────────────────────────────────────────────────────────

    # /start — match bare command or with arguments/bot-username suffix
    cmd = text.split()[0].split("@")[0].lower() if text else ""
    if cmd in ("/start", "/help"):
        if not user.get("is_configured"):
            return await message.reply(
                f"⚡ **Welcome to C7 MongoDB Transfer Bot V2.0**\n`{cfg.SEP}`\n\n"
                f"Run `/setup` to configure your personal bot instance.\n\n"
                f"**What you'll need:**\n"
                f"› Telegram API ID & Hash\n"
                f"› Your client bot token\n"
                f"› MongoDB URI, DB & Collection name\n"
                f"› Target channel ID")
        stats = await _get_user_stats(user)
        return await message.reply(
            ui.home_card_text(stats, user_id),
            reply_markup=ui.home_menu(user_id))

    # /setup
    elif text == "/setup":
        if user.get("is_configured"):
            return await message.reply(
                f"⚠️ **Reset Configuration?**\n`{cfg.SEP2}`\n\n"
                "This will **wipe all your current credentials** and restart the setup wizard.\n\n"
                "Are you sure?",
                reply_markup=ui.InlineKeyboardMarkup([[
                    ui.InlineKeyboardButton("⚠️ Yes, Reset Everything", callback_data="setup_confirm_reset"),
                    ui.InlineKeyboardButton("❌ Cancel", callback_data="go_home"),
                ]]))
        await reset_user_config(user_id)
        fresh_user = await get_user(user_id)
        await message.reply(
            f"🔧 **Setup Wizard**\n`{cfg.SEP2}`\n\n"
            f"This will walk you through configuring your personal bot instance.\n"
            f"You can re-run `/setup` at any time to change your settings.")
        await _send_wizard_step(client, user_id, "api_id", fresh_user)
        return

    # /stats
    elif text == "/stats":
        if not user.get("is_configured"):
            return await message.reply("⚠️ Run `/setup` first.")
        stats = await _get_user_stats(user)
        return await message.reply(ui.stats_card_text(stats, user_id))

    # /transfer
    elif text == "/transfer":
        if not user.get("is_configured"):
            return await message.reply("⚠️ Run `/setup` first.")
        async with _get_launch_lock(user_id):
            if cfg.active_transfers.get(user_id):
                return await message.reply("⚠️ Transfer already running.")
            cfg.active_transfers[user_id] = True
            cfg.paused_transfers.pop(user_id, None)
        card_msg = await message.reply(
            ui.build_progress_card(0, 0, 0.0, 0, ui.TransferState.RUNNING),
            reply_markup=ui.live_controls(ui.TransferState.RUNNING))
        cfg.progress_msg_ids[user_id] = card_msg.id
        _track_task(_launch_transfer(user), user_id, "transfer")

    # /stop
    elif text == "/stop":
        if not cfg.active_transfers.get(user_id):
            return await message.reply("⚠️ No transfer running.")
        cfg.active_transfers.pop(user_id, None)
        cfg.paused_transfers.pop(user_id, None)
        # Card will be updated to "stopped" by the transfer loop's finally block
        return await message.reply("⏹ **Transfer stopping.** Progress saved.")

    # /prescan
    elif text == "/prescan":
        if not user.get("is_configured"):
            return await message.reply("⚠️ Run `/setup` first.")
        if cfg.active_transfers.get(user_id):
            return await message.reply("⚠️ Stop transfer first.")
        await message.reply("🔎 **Starting pre-scan…**")
        _track_task(_launch_prescan(user), user_id, "prescan")

    # /monitor
    elif text == "/monitor":
        if not user.get("is_configured"):
            return await message.reply("⚠️ Run `/setup` first.")
        if cfg.active_monitors.get(user_id):
            return await message.reply("⚠️ Monitor already running.")
        if cfg.active_transfers.get(user_id):
            return await message.reply("⚠️ Stop transfer first.")
        await message.reply("👁 **Starting live monitor…**")
        _track_task(_launch_monitor(user), user_id, "monitor")

    # /stopmonitor
    elif text == "/stopmonitor":
        if not cfg.active_monitors.get(user_id):
            return await message.reply("⚠️ No monitor running.")
        cfg.active_monitors.pop(user_id, None)
        return await message.reply("🔴 **Monitor stopping…**")

    # /wipe
    elif text == "/wipe":
        if not user.get("is_configured"):
            return await message.reply("⚠️ Run `/setup` first.")
        user_db = await _get_state_db(user)
        await clear_sent_ids(user_db)
        await clear_channel_index(user_db)
        await clear_state(user_db)
        await clear_monitor_resume_token(user_db)
        cfg.logger.info(f"[ADMIN] All transfer data wiped  user={user_id}")
        return await message.reply(
            f"🗑 **Data Wiped**\n`{cfg.SEP2}`\n"
            f"Cleared: sent_ids, scan_index, transfer_state")

    # /config — shortcut to see current configuration
    elif text == "/config":
        if not user.get("is_configured"):
            return await message.reply("⚠️ Run `/setup` first.")
        return await message.reply(
            f"⚙️ **Your Configuration**\n`{cfg.SEP2}`\n"
            f"🤖 Bot Token › `{_mask(user.get('bot_token'))}`\n"
            f"🔑 API ID › `{user.get('api_id')}`\n"
            f"🔑 API Hash › `{_mask(user.get('api_hash'))}`\n"
            f"🗄 Mongo URI › `{_mask(user.get('mongo_uri'))}`\n"
            f"📂 DB › `{user.get('db_name')}`\n"
            f"📜 Collection › `{user.get('col_name')}`\n"
            f"📡 Target › `{user.get('target')}`\n"
            f"⚡ Speed Delay › `{user.get('speed_delay', 3.5)}s`\n"
            f"`{cfg.SEP2}`\n"
            f"Run `/setup` to reconfigure.",
            reply_markup=ui.settings_menu(user))


# ── Callback query handler ────────────────────────────────────────────────────
@app.on_callback_query()
async def cb_handler(client: Client, query: CallbackQuery) -> None:
    try:
        await _cb_handler_inner(client, query)
    except Exception as exc:
        cfg.logger.exception(f"[SYSTEM] Unhandled error in cb_handler  data={query.data!r}  err={exc}")
        try:
            await query.answer("❌ An internal error occurred. Please try again.", show_alert=True)
        except Exception:
            pass


# ── Callback query handlers ───────────────────────────────────────────────────
# Uniform signature (client, query, user, user_id, data, answer). Each body is
# a verbatim copy of the elif-branch it replaces — see _resolve_cb_handler for
# the dispatch table this feeds, and _cb_handler_inner for the thin dispatcher.

async def _cb_go_home(client, query, user, user_id, data, answer):
    await answer()
    stats = await _get_user_stats(user)
    await query.message.edit_text(
        ui.home_card_text(stats, user_id),
        reply_markup=ui.home_menu(user_id))


async def _cb_show_stats(client, query, user, user_id, data, answer):
    await answer()
    stats = await _get_user_stats(user)
    await query.message.edit_text(
        ui.stats_card_text(stats, user_id),
        reply_markup=ui.back_button("go_home"))


async def _cb_settings_menu(client, query, user, user_id, data, answer):
    await answer()
    await query.message.edit_text(
        f"⚙️ **Configuration**\n`{cfg.SEP2}`\n"
        f"Tap a field to update it:",
        reply_markup=ui.settings_menu(user))


async def _cb_set_field(client, query, user, user_id, data, answer):
    field_to_step = {
        "set_api_id":    "api_id",
        "set_api_hash":  "api_hash",
        "set_bot_token": "bot_token",
        "set_mongo_uri": "mongo_uri",
        "set_target":    "target",
    }
    step = field_to_step[data]
    await update_user_field(user_id, "setup_step", step)
    await answer()
    await _send_wizard_step(client, user_id, step, user)


async def _cb_explore_dbs(client, query, user, user_id, data, answer):
    await answer("Loading your databases…")
    mongo_uri = user.get("mongo_uri")
    if not mongo_uri:
        await client.send_message(
            user_id, "⚠️ Set your MongoDB URI first (`/setup` or tap **DB URI** in settings).")
        return
    try:
        dbs = await list_user_databases(mongo_uri)
    except Exception as e:
        await client.send_message(user_id, f"❌ Could not fetch databases:\n`{e}`")
        return
    if not dbs:
        await client.send_message(user_id, "⚠️ No databases found (or all are system DBs).")
        return
    await client.send_message(
        user_id,
        f"📂 **Select your Database** ({len(dbs)} found):",
        reply_markup=ui.db_list_menu(dbs))


async def _cb_pick_db(client, query, user, user_id, data, answer):
    db_name = data.split(":", 1)[1]
    await update_user_fields(user_id, {"db_name": db_name, "col_name": None, "setup_step": "col_name"})
    await answer(f"✅ DB: {db_name}")
    updated = await get_user(user_id)
    await _send_wizard_step(client, user_id, "col_name", updated)


async def _cb_explore_cols(client, query, user, user_id, data, answer):
    await answer("Loading collections…")
    mongo_uri = user.get("mongo_uri")
    db_name   = user.get("db_name")
    if not mongo_uri or not db_name:
        await client.send_message(
            user_id, "⚠️ Set your MongoDB URI and Database first.")
        return
    try:
        cols = await list_user_collections(mongo_uri, db_name)
    except Exception as e:
        await client.send_message(user_id, f"❌ Could not fetch collections:\n`{e}`")
        return
    if not cols:
        await client.send_message(user_id, "⚠️ No collections found in this database.")
        return
    await client.send_message(
        user_id,
        f"📜 **Select your Collection** ({len(cols)} found in `{db_name}`):",
        reply_markup=ui.col_list_menu(cols))


async def _cb_pick_col(client, query, user, user_id, data, answer):
    col_name = data.split(":", 1)[1]
    await answer(f"✅ Collection: {col_name}")
    if user.get("target"):
        await update_user_fields(user_id, {
            "col_name": col_name, "setup_step": None, "is_configured": True
        })
        await client.send_message(
            user_id,
            f"✅ Collection updated to `{col_name}`.",
            reply_markup=ui.home_menu(user_id))
    else:
        await update_user_fields(user_id, {"col_name": col_name, "setup_step": "target"})
        updated = await get_user(user_id)
        await _send_wizard_step(client, user_id, "target", updated)


async def _cb_speed_settings(client, query, user, user_id, data, answer):
    await answer()
    current = float(user.get("speed_delay", 3.5))
    await query.message.edit_text(
        f"⚡ **Speed Delay**\n`{cfg.SEP2}`\n"
        f"Controls the pause between each batch send.\n"
        f"Lower = faster but higher FloodWait risk.",
        reply_markup=ui.speed_menu(current))


async def _cb_set_speed(client, query, user, user_id, data, answer):
    val = float(data.split(":", 1)[1])
    await update_user_field(user_id, "speed_delay", val)
    await answer(f"Speed set to {val}s")
    await query.message.edit_reply_markup(reply_markup=ui.speed_menu(val))


async def _cb_wipe_data_confirm(client, query, user, user_id, data, answer):
    await answer()
    try:
        _wdb = await _get_state_db(user)
        _sent_cnt  = await count_sent_ids(_wdb)
        _idx_cnt   = await get_channel_index_count(_wdb)
    except Exception:
        _sent_cnt = _idx_cnt = 0
    await query.message.edit_text(
        f"⚠️ **Confirm Data Wipe**\n`{cfg.SEP2}`\n\n"
        f"This will permanently delete:\n"
        f"• `{_sent_cnt:,}` sent file IDs\n"
        f"• `{_idx_cnt:,}` scan index entries\n"
        f"• Transfer resume cursor\n\n"
        f"**This cannot be undone.** Are you sure?",
        reply_markup=ui.wipe_confirm_menu())


async def _cb_wipe_confirmed(client, query, user, user_id, data, answer):
    user_db = await _get_state_db(user)
    await clear_sent_ids(user_db)
    await clear_channel_index(user_db)
    await clear_state(user_db)
    await clear_monitor_resume_token(user_db)
    await answer("✅ All data wiped")
    cfg.logger.info(f"[ADMIN] All transfer data wiped  user={user_id}")
    await query.message.edit_text(
        f"🗑 **Data Wiped**\n`{cfg.SEP2}`\n"
        f"Cleared: sent_ids, scan_index, transfer_state",
        reply_markup=ui.back_button("settings_menu"))


async def _cb_start_transfer(client, query, user, user_id, data, answer):
    if not user.get("is_configured"):
        return await answer("⚠️ Run /setup first.", alert=True)
    async with _get_launch_lock(user_id):
        if cfg.active_transfers.get(user_id):
            return await answer("⚠️ Transfer already running.", alert=True)
        cfg.active_transfers[user_id] = True
        cfg.paused_transfers.pop(user_id, None)
    await answer("🚀 Starting transfer…")
    card_msg = await app.send_message(
        user_id,
        ui.build_progress_card(0, 0, 0.0, 0, ui.TransferState.RUNNING),
        reply_markup=ui.live_controls(ui.TransferState.RUNNING))
    cfg.progress_msg_ids[user_id] = card_msg.id
    _track_task(_launch_transfer(user), user_id, "transfer")


async def _cb_ctrl_pause(client, query, user, user_id, data, answer):
    if not cfg.active_transfers.get(user_id):
        return await answer("No transfer is running.", alert=True)
    if cfg.paused_transfers.get(user_id):
        return await answer("Transfer is already paused.", alert=True)
    cfg.paused_transfers[user_id] = True
    prog = cfg.transfer_progress.get(user_id, {})
    _cnt = prog.get("count", 0); _tot = prog.get("total", 1)
    _pct = _cnt / _tot * 100 if _tot else 0
    await answer(f"⏸️  Paused at {_pct:.1f}%  ·  cursor is safe", alert=False)
    try:
        await query.message.edit_text(
            ui.build_progress_snapshot_card(prog, ui.TransferState.PAUSED),
            reply_markup=ui.live_controls(ui.TransferState.PAUSED))
    except Exception:
        pass


async def _cb_ctrl_resume(client, query, user, user_id, data, answer):
    cfg.paused_transfers.pop(user_id, None)
    prog = cfg.transfer_progress.get(user_id, {})
    _cnt = prog.get("count", 0); _tot = prog.get("total", 1)
    _pct = _cnt / _tot * 100 if _tot else 0
    await answer(f"▶️  Resuming from {_pct:.1f}%", alert=False)
    try:
        await query.message.edit_text(
            ui.build_progress_snapshot_card(prog, ui.TransferState.RUNNING),
            reply_markup=ui.live_controls(ui.TransferState.RUNNING))
    except Exception:
        pass


async def _cb_ctrl_stop(client, query, user, user_id, data, answer):
    if not cfg.active_transfers.get(user_id):
        return await answer("⚠️  No transfer is currently running.", alert=True)
    # Signal the loop to exit
    cfg.active_transfers.pop(user_id, None)
    cfg.paused_transfers.pop(user_id, None)
    prog = cfg.transfer_progress.get(user_id, {})
    _sent = prog.get("sent", 0)
    await answer(f"⏹️  Stopped  ·  {_sent:,} files sent  ·  cursor saved", alert=False)
    # Build terminal card immediately; pop msg_id so the loop's finally block
    # doesn't attempt a second edit on the same message.
    msg_id = cfg.progress_msg_ids.pop(user_id, None)
    if msg_id:
        try:
            await app.edit_message_text(
                user_id,
                msg_id,
                ui.build_progress_snapshot_card(prog, ui.TransferState.STOPPED),
                reply_markup=ui.live_controls(ui.TransferState.STOPPED))
        except Exception:
            pass


async def _cb_reset_offset(client, query, user, user_id, data, answer):
    if cfg.active_transfers.get(user_id):
        return await answer("⚠️  Stop the transfer first.", alert=True)
    user_db = await _get_state_db(user)
    await clear_state(user_db)
    await answer("🔄  Transfer cursor cleared — next run starts from the beginning",
                 alert=False)
    await query.message.edit_text(
        f"⚙️ **Configuration**\n`{cfg.SEP2}`\n"
        f"✅ Transfer offset cleared.\nTap a field to update it:",
        reply_markup=ui.settings_menu(user))


async def _cb_monitor_menu(client, query, user, user_id, data, answer):
    await answer()
    await query.message.edit_text(
        f"👁 **Live Monitor**\n`{cfg.SEP2}`\n"
        f"Watches MongoDB for new inserts and forwards them to your channel.",
        reply_markup=ui.monitor_menu(user_id))


async def _cb_monitor_start(client, query, user, user_id, data, answer):
    if not user.get("is_configured"):
        return await answer("⚠️ Run /setup first.", alert=True)
    if cfg.active_monitors.get(user_id):
        return await answer("⚠️ Monitor already running.", alert=True)
    if cfg.active_transfers.get(user_id):
        return await answer("⚠️ Stop transfer first.", alert=True)
    await answer("👁 Starting monitor…")
    _track_task(_launch_monitor(user), user_id, "monitor")


async def _cb_monitor_stop(client, query, user, user_id, data, answer):
    cfg.active_monitors.pop(user_id, None)
    await answer("🔴 Monitor stopping…")


async def _cb_prescan_channel(client, query, user, user_id, data, answer):
    if not user.get("is_configured"):
        return await answer("⚠️ Run /setup first.", alert=True)
    if cfg.active_transfers.get(user_id):
        return await answer("⚠️ Stop transfer first.", alert=True)
    await answer("🔎 Starting pre-scan…")
    _track_task(_launch_prescan(user), user_id, "prescan")


async def _cb_xfer_resume(client, query, user, user_id, data, answer):
    try:
        _, action, owner_id_str = data.split(":", 2)
        owner_id = int(owner_id_str)
    except (ValueError, IndexError):
        return await answer("❌ Malformed request.", alert=True)

    # Security check — only the transfer's actual owner may act on it.
    if user_id != owner_id:
        return await answer("❌ This is not your transfer pipeline.", alert=True)

    # Guarded lookup — a Mongo hiccup here must never leave the callback
    # un-answered, or the button spins forever on the client with no
    # feedback at all. Always resolve to an answer(), success or failure.
    try:
        target_user = await get_user(owner_id)
    except Exception as e:
        cfg.logger.error(f"[SYSTEM] Resume-decision lookup failed  user={owner_id}  err={e}")
        return await answer("❌ Something went wrong — try again.", alert=True)

    if not target_user or not target_user.get("is_configured"):
        return await answer("⚠️ Configuration no longer found.", alert=True)

    async with _get_launch_lock(owner_id):
        if cfg.active_transfers.get(owner_id):
            return await answer("⚠️ Transfer already running.", alert=True)
        cfg.active_transfers[owner_id] = True
        cfg.paused_transfers.pop(owner_id, None)

    try:
        if action == "continue":
            await answer("🚀 Resuming transfer from saved progress...", alert=True)
            cfg.logger.info(
                f"[SYSTEM] ▶️ User chose Continue — resuming from saved cursor  user={owner_id}")
            _track_task(_launch_transfer(target_user), owner_id, "transfer")
            try:
                await query.message.edit_text(
                    "🚀 **Resuming transfer from saved progress...**",
                    reply_markup=None)
            except Exception:
                pass

        elif action == "fresh":
            await answer("🔄 Starting fresh scan (skipping existing duplicates)...", alert=True)
            try:
                # Reset only the resume cursor — c7_sent_ids / c7_scan_index
                # (the actual duplicate-tracking collections) are untouched,
                # so the fresh scan still skips anything already delivered.
                user_db = await _get_state_db(target_user)
                await clear_state(user_db)
                cfg.logger.info(
                    f"[SYSTEM] 🔄 User chose Start Fresh — cursor reset  user={owner_id}")
            except Exception as e:
                cfg.logger.error(f"[SYSTEM] Failed to reset cursor  user={owner_id}  err={e}")
            _track_task(_launch_transfer(target_user), owner_id, "transfer")
            try:
                await query.message.edit_text(
                    "🔄 **Starting fresh scan (skipping existing duplicates)...**",
                    reply_markup=None)
            except Exception:
                pass

        else:
            await answer("❌ Unknown action.", alert=True)
    except Exception as e:
        # Last-resort safety net: whatever else goes wrong past this point,
        # the callback still gets answered so Telegram clears the spinner.
        # (answer() is itself exception-safe, so a harmless double-answer
        # after an already-successful one above is a silent no-op.)
        cfg.logger.exception(f"[SYSTEM] xfer_resume handling crashed  user={owner_id}  err={e}")
        await answer("❌ Something went wrong — try again.", alert=True)


async def _cb_admin_panel(client, query, user, user_id, data, answer):
    if user_id != cfg.HOST_ADMIN_ID:
        return await answer("❌ Not authorized.", alert=True)
    await answer()
    all_users = await get_all_configured_users()
    active_t  = sum(1 for v in cfg.active_transfers.values() if v)
    active_m  = sum(1 for v in cfg.active_monitors.values()  if v)
    await query.message.edit_text(
        f"🛠 **Admin Panel**\n`{cfg.SEP2}`\n"
        f"👥 Configured users › `{len(all_users)}`\n"
        f"🔄 Active transfers › `{active_t}`\n"
        f"👁 Active monitors  › `{active_m}`\n"
        f"🤖 Cached workers   › `{len(cfg.active_workers)}`",
        reply_markup=ui.back_button("go_home"))


async def _cb_setup_confirm_reset(client, query, user, user_id, data, answer):
    await answer()
    await reset_user_config(user_id)
    fresh_user = await get_user(user_id)
    await query.message.edit_text(
        f"🔧 **Setup Wizard**\n`{cfg.SEP2}`\n\nConfiguration cleared. Let's set up from scratch.")
    await _send_wizard_step(client, user_id, "api_id", fresh_user)


async def _cb_help_page(client, query, user, user_id, data, answer):
    await answer()
    await query.message.edit_text(
        ui.HELP_PAGES[data],
        reply_markup=ui.help_nav(data))


async def _cb_unknown(client, query, user, user_id, data, answer):
    await answer()


# ── Callback dispatch table ───────────────────────────────────────────────────
_CB_EXACT_HANDLERS = {
    "go_home":             _cb_go_home,
    "show_stats":          _cb_show_stats,
    "settings_menu":       _cb_settings_menu,
    "set_api_id":          _cb_set_field,
    "set_api_hash":        _cb_set_field,
    "set_bot_token":       _cb_set_field,
    "set_mongo_uri":       _cb_set_field,
    "set_target":          _cb_set_field,
    "explore_dbs":         _cb_explore_dbs,
    "explore_cols":        _cb_explore_cols,
    "speed_settings":      _cb_speed_settings,
    "wipe_data_confirm":   _cb_wipe_data_confirm,
    "wipe_confirmed":      _cb_wipe_confirmed,
    "start_transfer":      _cb_start_transfer,
    "ctrl_pause":          _cb_ctrl_pause,
    "ctrl_resume":         _cb_ctrl_resume,
    "ctrl_stop":           _cb_ctrl_stop,
    "reset_offset":        _cb_reset_offset,
    "monitor_menu":        _cb_monitor_menu,
    "monitor_start":       _cb_monitor_start,
    "monitor_stop":        _cb_monitor_stop,
    "prescan_channel":     _cb_prescan_channel,
    "admin_panel":         _cb_admin_panel,
    "setup_confirm_reset": _cb_setup_confirm_reset,
}

_CB_PREFIX_HANDLERS = [
    ("pick_db:", _cb_pick_db),
    ("pick_col:", _cb_pick_col),
    ("set_speed:", _cb_set_speed),
    ("xfer_resume:", _cb_xfer_resume),
]


def _resolve_cb_handler(data: str):
    handler = _CB_EXACT_HANDLERS.get(data)
    if handler is not None:
        return handler
    for prefix, prefix_handler in _CB_PREFIX_HANDLERS:
        if data.startswith(prefix):
            return prefix_handler
    if data in ui.HELP_PAGES:
        return _cb_help_page
    return _cb_unknown


async def _cb_handler_inner(client: Client, query: CallbackQuery) -> None:
    user_id  = query.from_user.id
    username = query.from_user.username
    data     = query.data
    user     = await get_or_create_user(user_id, username)

    async def answer(text: str = "", alert: bool = False):
        try: await query.answer(text, show_alert=alert)
        except Exception: pass

    handler = _resolve_cb_handler(data)
    await handler(client, query, user, user_id, data, answer)


# ── Background task tracking / launch locking ─────────────────────────────────
def _track_task(coro, user_id: int, kind: str) -> asyncio.Task:
    """Schedule coro as a tracked task so crashes are logged and shutdown can
    cancel + await every in-flight task instead of discarding it silently."""
    task_key = f"{kind}:{user_id}"
    existing = cfg.active_tasks.get(task_key)
    if existing is not None and not existing.done():
        if hasattr(coro, "close"):
            coro.close()
        raise RuntimeError(f"Task already running: {task_key}")
    task = asyncio.create_task(coro, name=f"{kind}-{user_id}")
    cfg.active_tasks[task_key] = task

    def _on_done(t: asyncio.Task, _key: str = task_key) -> None:
        if cfg.active_tasks.get(_key) is t:
            cfg.active_tasks.pop(_key, None)
        if t.cancelled():
            return
        exc = t.exception()
        if exc is not None:
            cfg.logger.error(
                f"[MAIN] Background task crashed  user={user_id}  "
                f"name={t.get_name()}  err={exc}")

    task.add_done_callback(_on_done)
    return task


def _get_launch_lock(user_id: int) -> asyncio.Lock:
    """Per-user lock guarding the check-then-claim of active_transfers at
    transfer launch, so rapid double-taps on Start can't both pass the
    'already running?' check before either claims the slot."""
    lock = cfg.launch_locks.get(user_id)
    if lock is None:
        lock = cfg.launch_locks[user_id] = asyncio.Lock()
    return lock


# ── Internal launchers ────────────────────────────────────────────────────────
async def _launch_transfer(user: dict) -> None:
    user_id = user["_id"]
    try:
        worker  = await get_or_start_worker(user)
        source_db = await _get_user_db(user)
        state_db = await _get_state_db(user)
        await init_user_db(state_db)
        await run_transfer(worker, app, user, source_db, state_db)
    except Exception as e:
        cfg.logger.exception(f"[MAIN] Transfer launch error  user={user_id}  err={e}")
        cfg.active_transfers.pop(user_id, None)
        cfg.transfer_progress.pop(user_id, None)


async def _launch_monitor(user: dict) -> None:
    user_id = user["_id"]
    try:
        worker  = await get_or_start_worker(user)
        source_db = await _get_user_db(user)
        state_db = await _get_state_db(user)
        await init_user_db(state_db)
        await run_monitor(worker, app, user, source_db, state_db)
    except Exception as e:
        cfg.logger.exception(f"[MAIN] Monitor launch error  user={user_id}  err={e}")
        cfg.active_monitors.pop(user_id, None)


async def _launch_prescan(user: dict) -> None:
    user_id = user["_id"]
    try:
        source_db = await _get_user_db(user)
        state_db = await _get_state_db(user)
        await init_user_db(state_db)
        await run_prescan(app, user, source_db, state_db)
    except Exception as e:
        cfg.logger.exception(f"[MAIN] Prescan launch error  user={user_id}  err={e}")


# ── Helpers ───────────────────────────────────────────────────────────────────
async def _get_user_db(user: dict):
    """Return a Motor database handle for this user."""
    return await get_user_motor_db(user["mongo_uri"], user["db_name"], user_id=user["_id"])


async def _get_state_db(user: dict):
    """Return the host-owned runtime DB for this transfer tenant."""
    return await get_user_state_db(user["_id"])


async def _get_user_stats(user: dict) -> dict:
    try:
        source_db = await _get_user_db(user)
        state_db = await _get_state_db(user)
        return await get_stats(state_db, user["col_name"], source_db=source_db)
    except Exception:
        return {"total_files": 0, "sent_files": 0, "remaining": 0, "last_id": None}


def _mask(value: str | None, keep: int = 6) -> str:
    """Mask a sensitive string, showing only the first and last few chars."""
    if not value:
        return "❌ Not set"
    s = str(value)
    if len(s) <= keep * 2:
        return "•" * len(s)
    return s[:keep] + "•" * (len(s) - keep * 2) + s[-keep:]


# ══════════════════════════════════════════════════════════════════════════════
#  HEALTH SERVER
# ══════════════════════════════════════════════════════════════════════════════
async def start_health_server() -> None:
    async def health(request):
        active_t = sum(1 for v in cfg.active_transfers.values() if v)
        active_m = sum(1 for v in cfg.active_monitors.values()  if v)
        return web.Response(
            text=f"C7 MongoDB Transfer Bot V2.0 | "
                 f"active_transfers={active_t} | "
                 f"active_monitors={active_m} | "
                 f"cached_workers={len(cfg.active_workers)}")

    # Pterodactyl/OptikLink inject the assigned port via PORT or SERVER_PORT —
    # never assume 8080 is free on a shared-host container.
    port = int(os.environ.get("PORT") or os.environ.get("SERVER_PORT") or 8080)

    wa = web.Application()
    wa.router.add_get("/", health)
    runner = web.AppRunner(wa)

    try:
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", port).start()
        cfg.logger.info(f"[SYSTEM] Health server listening on :{port}")
    except OSError as e:
        # Address already in use / permission denied on this container — do
        # NOT crash the bot over a health-check endpoint. Log and move on.
        cfg.logger.warning(
            f"[SYSTEM] ⚠️ Health server failed to bind. Running bot without health port.  err={e}")
        try:
            await runner.cleanup()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
#  BOOT AUTO-RESUME (interactive)
# ══════════════════════════════════════════════════════════════════════════════
async def _boot_auto_resume() -> None:
    """
    On every startup, scan every configured user's MongoDB for an unfinished
    transfer cursor (last_id ≠ null). Rather than silently relaunching the
    transfer, DM the owner an interactive Continue / Start Fresh prompt —
    the actual launch happens later from the `xfer_resume:*` callback below,
    once the user picks an option.

    The whole scan is wrapped defensively: a Mongo hiccup here must never
    raise out of this function, since main() awaits it *before* the parent
    bot settles into its listening state — an unhandled exception at this
    point would kill the process before it ever starts receiving /start.
    """
    try:
        users = await get_all_configured_users()
    except Exception as e:
        cfg.logger.error(f"[BOOT] Could not load configured users — skipping resume scan  err={e}")
        return

    cfg.logger.info(f"[BOOT] Scanning {len(users)} configured user(s) for unfinished transfers")

    prompted = 0
    for user in users:
        user_id = user["_id"]
        try:
            state_db = await _get_state_db(user)
            state = await load_state(state_db)
            if not state.get("last_id") and not state.get("offset"):
                continue

            cfg.logger.info(f"[SYSTEM] ⏳ Waiting for user decision on auto-resume user={user_id}")
            await app.send_message(
                user_id,
                "⚠️ **Interrupted Transfer Detected**\n\n"
                "I detected an unfinished transfer that was interrupted during my "
                "server restart. How would you like to proceed?",
                reply_markup=ui.resume_prompt_menu(user_id))
            prompted += 1
            await asyncio.sleep(1)   # stagger DMs to avoid Telegram flood limits
        except Exception as e:
            cfg.logger.error(f"[BOOT] Auto-resume prompt failed  user={user_id}  err={e}")

    if prompted:
        cfg.logger.info(f"[BOOT] {prompted} user(s) prompted for resume decision ⚡")
    else:
        cfg.logger.info("[BOOT] No unfinished transfers detected — clean boot")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════
async def _startup() -> None:
    """All initialisation that runs after the client is started."""
    await init_master_db()
    _track_task(run_user_motor_client_eviction_loop(),
                cfg.SYSTEM_TASK_MOTOR_EVICTION, "motor-eviction")
    cfg.logger.info("[SYSTEM] Started Motor client idle-eviction loop")
    await start_health_server()

    try:
        async with aiohttp.ClientSession() as _sess:
            _url = f"https://api.telegram.org/bot{cfg.PARENT_BOT_TOKEN}/deleteWebhook?drop_pending_updates=false"
            async with _sess.get(_url) as _resp:
                _body = await _resp.json()
                if _body.get("result"):
                    cfg.logger.info("[SYSTEM] Webhook cleared — updates will arrive via long-polling")
                else:
                    cfg.logger.warning(f"[SYSTEM] deleteWebhook returned unexpected body: {_body}")
    except Exception as _e:
        cfg.logger.warning(f"[SYSTEM] Could not clear webhook (non-fatal): {_e}")

    await _boot_auto_resume()
    cfg.logger.info("[SYSTEM] Listening for Telegram updates — idle()")


async def _run_main() -> None:
    """Run startup and idle while the event loop is already active.

    Pyrogram exposes sync-friendly wrappers around its async methods. Calling
    those wrappers before ``run_until_complete`` makes them consume their own
    coroutine and return ``None`` on newer Python versions. Keeping the whole
    lifecycle inside one running loop avoids that double execution.
    """
    cfg.logger.info("Starting parent bot client")
    await safe_start_client(app, "C7 ParentBot")
    cfg.logger.info("C7 MongoDB Transfer Bot V2.0 online ⚡")
    await _startup()
    await idle()


async def _shutdown() -> None:
    cfg.logger.info(
        f"Shutting down — cancelling {len(cfg.active_tasks)} tracked background task(s)")
    pending = list(cfg.active_tasks.values())
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)

    cfg.logger.info("Shutting down — stopping all worker clients")
    await stop_all_workers()
    close_all_user_motor_clients()
    if app.is_connected:
        cfg.logger.info("Stopping parent bot")
        await app.stop()


if __name__ == "__main__":
    # Use the event loop that already exists at import time — the same one
    # that Client.__init__ captured for app.loop and app.dispatcher.loop.
    # asyncio.run() always creates a *new* loop, which causes a loop mismatch:
    # Dispatcher.handler_worker tasks are created on the old (import-time) loop
    # and never run, so updates arrive but handlers are never invoked.
    _loop = asyncio.get_event_loop()
    try:
        _loop.run_until_complete(_run_main())
    except KeyboardInterrupt:
        pass
    finally:
        _loop.run_until_complete(_shutdown())
        _loop.close()
