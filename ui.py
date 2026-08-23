"""
ui.py — inline keyboard builders + live progress card renderer
"""
from __future__ import annotations

import math
from enum import Enum

from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

import config as cfg


class TransferState(str, Enum):
    """Transfer/card status vocabulary. Inherits str so every existing
    ==/in/dict-key comparison against a plain string literal keeps working
    unchanged whether callers pass a member or a raw string."""
    RUNNING = "running"
    PAUSED  = "paused"
    STOPPED = "stopped"
    DONE    = "done"


# ══════════════════════════════════════════════════════════════════════════════
#  PROGRESS CARD HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _progress_bar(pct: float, width: int = 22) -> str:
    filled = max(0, min(width, int(round(pct / 100 * width))))
    return "█" * filled + "░" * (width - filled)


def _fmt_eta(seconds: float) -> str:
    if seconds <= 0 or not math.isfinite(seconds):
        return "Calculating…"
    d = int(seconds // 86400)
    h = int((seconds % 86400) // 3600)
    m = int((seconds % 3600) // 60)
    parts: list[str] = []
    if d: parts.append(f"{d} day{'s' if d != 1 else ''}")
    if h: parts.append(f"{h} hr{'s' if h != 1 else ''}")
    if m or not parts: parts.append(f"{m} min")
    return ", ".join(parts)


def _fmt_elapsed(seconds: float) -> str:
    d = int(seconds // 86400)
    h = int((seconds % 86400) // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    if d: return f"{d}d {h}h {m}m"
    if h: return f"{h}h {m}m {s}s"
    if m: return f"{m}m {s}s"
    return f"{s}s"


def build_progress_card(
        count:   int,
        total:   int,
        elapsed: float,
        failed:  int = 0,
        state:   TransferState | str = TransferState.RUNNING,
) -> str:
    """
    Build the live transfer progress card text.

    state values:
      "running"  — transfer is active
      "paused"   — transfer loop is suspended
      "stopped"  — aborted by user
      "done"     — all files transferred
    """
    # Edge case: initial card before the DB has been queried
    if total == 0:
        bar = "░" * 22
        return (
            "⏳ **TRANSFER PIPELINE STATUS**\n"
            f"`{'─' * 28}`\n"
            "📦 Progress › `Initializing…`\n"
            f"`{bar}`\n"
            "⚡ Speed › `—`\n"
            "📅 ETA › `Calculating…`"
        )

    pct      = count / total * 100
    rate     = count / elapsed if elapsed > 0 else 0.0
    eta_secs = (total - count) / rate if rate > 0 else 0.0
    bar      = _progress_bar(pct)

    headers = {
        "running": "⏳ **TRANSFER PIPELINE STATUS**",
        "paused":  "⏸️ **TRANSFER PAUSED**",
        "stopped": "⏹️ **TRANSFER ABORTED BY USER**",
        "done":    "✅ **TRANSFER COMPLETE**",
    }
    header = headers.get(state, "⏳ **TRANSFER PIPELINE STATUS**")

    lines = [
        header,
        f"`{'─' * 28}`",
        f"📦 Progress › `{count:,}` / `{total:,}` · `{pct:.1f}%`",
        f"`{bar}`",
        f"⚡ Speed › `{rate:.1f}` files/sec",
    ]

    if state in ("running", "paused"):
        lines.append(f"📅 ETA › `{_fmt_eta(eta_secs)}`")
    else:
        lines.append(f"⏱ Elapsed › `{_fmt_elapsed(elapsed)}`")

    if failed:
        lines.append(f"⚠️ Failed › `{failed}`")

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
#  CARD TEXT RENDERERS
# ══════════════════════════════════════════════════════════════════════════════

def home_card_text(stats: dict, user_id: int) -> str:
    return (
        f"⚡ **C7 MongoDB Transfer Bot V2.0**\n`{cfg.SEP}`\n"
        f"📦 Total files › `{stats['total_files']:,}`\n"
        f"✅ Sent › `{stats['sent_files']:,}`\n"
        f"⏳ Remaining › `{stats['remaining']:,}`\n"
        f"🔄 Transfer › `{'Paused' if cfg.paused_transfers.get(user_id) else 'Running' if cfg.active_transfers.get(user_id) else 'Idle'}`\n"
        f"👁 Monitor › `{'Running' if cfg.active_monitors.get(user_id) else 'Idle'}`\n"
        f"`{cfg.SEP}`"
    )


def stats_card_text(stats: dict, user_id: int) -> str:
    return (
        f"📊 **Transfer Statistics**\n`{cfg.SEP}`\n"
        f"📦 Total files › `{stats['total_files']:,}`\n"
        f"✅ Sent › `{stats['sent_files']:,}`\n"
        f"⏳ Remaining › `{stats['remaining']:,}`\n"
        f"🔄 Transfer › `{'Paused' if cfg.paused_transfers.get(user_id) else 'Running' if cfg.active_transfers.get(user_id) else 'Idle'}`\n"
        f"👁 Monitor › `{'Running' if cfg.active_monitors.get(user_id) else 'Idle'}`\n"
        f"📍 Last ID › `{stats['last_id'] or 'None'}`"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  KEYBOARD BUILDERS
# ══════════════════════════════════════════════════════════════════════════════

def home_menu(user_id: int) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton("🚀  Launch Transfer",      callback_data="start_transfer")],
        [InlineKeyboardButton("👁  Live Monitor",          callback_data="monitor_menu"),
         InlineKeyboardButton("🔎  Pre-Scan",              callback_data="prescan_channel")],
        [InlineKeyboardButton("📊  Active Stats",          callback_data="show_stats"),
         InlineKeyboardButton("⚙️  Pipeline Settings",    callback_data="settings_menu")],
        [InlineKeyboardButton("📖  Admin Guide",           callback_data="help_1"),
         InlineKeyboardButton("🛑  Emergency Stop",        callback_data="ctrl_stop")],
    ]
    if user_id == cfg.HOST_ADMIN_ID:
        buttons.append([InlineKeyboardButton("🛠  Admin Panel", callback_data="admin_panel")])
    return InlineKeyboardMarkup(buttons)


def settings_menu(user: dict) -> InlineKeyboardMarkup:
    def ck(v): return "✅" if v else "❌"
    spd  = user.get("speed_delay", 3.5)
    db   = user.get("db_name")
    col  = user.get("col_name")
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🔑 API ID {ck(user.get('api_id'))}",        callback_data="set_api_id"),
         InlineKeyboardButton(f"🔑 API Hash {ck(user.get('api_hash'))}",    callback_data="set_api_hash")],
        [InlineKeyboardButton(f"🤖 Bot Token {ck(user.get('bot_token'))}",  callback_data="set_bot_token")],
        [InlineKeyboardButton(f"🗄 MongoDB URI {ck(user.get('mongo_uri'))}", callback_data="set_mongo_uri")],
        [InlineKeyboardButton(f"📂 {db}"  if db  else "📂 Browse DBs",      callback_data="explore_dbs"),
         InlineKeyboardButton(f"📜 {col}" if col else "📜 Browse Cols",     callback_data="explore_cols")],
        [InlineKeyboardButton(f"📡 Target Channel {ck(user.get('target'))}", callback_data="set_target")],
        [InlineKeyboardButton(f"⏱️ Speed: {spd}s",                           callback_data="speed_settings"),
         InlineKeyboardButton("🔄 Reset Offset",                              callback_data="reset_offset")],
        [InlineKeyboardButton("🧹 Wipe Data",                                 callback_data="wipe_data_confirm"),
         InlineKeyboardButton("🏠 Return to Home",                            callback_data="go_home")],
    ])


def live_controls(state: TransferState | str) -> InlineKeyboardMarkup:
    """
    Inline keyboard attached to the live progress card.
    state: "running" | "paused" | "stopped" | "done"
    """
    if state == "paused":
        return InlineKeyboardMarkup([[
            InlineKeyboardButton("▶️ Resume",    callback_data="ctrl_resume"),
            InlineKeyboardButton("⏹️ Stop",      callback_data="ctrl_stop"),
            InlineKeyboardButton("⚙️ Settings",  callback_data="settings_menu"),
        ]])
    if state in ("stopped", "done"):
        return InlineKeyboardMarkup([[
            InlineKeyboardButton("🏠 Home",       callback_data="go_home"),
            InlineKeyboardButton("⚙️ Settings",   callback_data="settings_menu"),
        ]])
    # running (default)
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("⏸️ Pause",         callback_data="ctrl_pause"),
        InlineKeyboardButton("⏹️ Stop",          callback_data="ctrl_stop"),
        InlineKeyboardButton("⚙️ Settings",      callback_data="settings_menu"),
    ]])


def back_button(target: str = "settings_menu") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=target)]])


def monitor_menu(user_id: int) -> InlineKeyboardMarkup:
    active = cfg.active_monitors.get(user_id, False)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "🔴 Stop Monitor" if active else "🟢 Start Monitor",
            callback_data="monitor_stop" if active else "monitor_start")],
        [InlineKeyboardButton("🏠 Home", callback_data="go_home")],
    ])


def db_list_menu(db_names: list[str]) -> InlineKeyboardMarkup:
    rows = []
    row  = []
    for name in db_names:
        row.append(InlineKeyboardButton(f"📂 {name}", callback_data=f"pick_db:{name}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("🔙 Back", callback_data="settings_menu")])
    return InlineKeyboardMarkup(rows)


def col_list_menu(col_names: list[str]) -> InlineKeyboardMarkup:
    rows = []
    row  = []
    for name in col_names:
        row.append(InlineKeyboardButton(f"📜 {name}", callback_data=f"pick_col:{name}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("🔙 Back", callback_data="explore_dbs")])
    return InlineKeyboardMarkup(rows)


def speed_menu(current: float) -> InlineKeyboardMarkup:
    options = [1.5, 2.5, 3.5, 4.5, 5.5, 6.0]
    rows = []
    row  = []
    for v in options:
        label = f"{'✅ ' if v == current else ''}{v}s"
        row.append(InlineKeyboardButton(label, callback_data=f"set_speed:{v}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("🔙 Back", callback_data="settings_menu")])
    return InlineKeyboardMarkup(rows)


def wipe_confirm_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⚠️ Yes, Wipe Everything", callback_data="wipe_confirmed"),
         InlineKeyboardButton("❌ Cancel",               callback_data="settings_menu")],
    ])


def resume_prompt_menu(user_id: int) -> InlineKeyboardMarkup:
    """
    Boot-time prompt shown when an interrupted transfer is detected for
    `user_id`. callback_data embeds the owning user_id so cb_handler can
    verify the tap came from the transfer's actual owner before acting.
    """
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Continue Pending", callback_data=f"xfer_resume:continue:{user_id}")],
        [InlineKeyboardButton("🔄 Start Fresh (Skip Duplicates)", callback_data=f"xfer_resume:fresh:{user_id}")],
    ])


HELP_PAGES = {
    "help_1": "📖 **Setup Guide (1/6)**\n\nYou'll need: API ID, API Hash, Bot Token, "
              "MongoDB URI, DB Name, Collection, and Target Channel ID.",
    "help_2": "📖 **Step 1 — API Credentials**\n\nVisit [my.telegram.org](https://my.telegram.org) → "
              "App configuration.\nCopy your **API ID** and **API Hash**.",
    "help_3": "📖 **Step 2 — Bot Token**\n\nCreate a bot via @BotFather → `/newbot`.\n"
              "Copy the token and make it **admin** in your target channel.",
    "help_4": "📖 **Step 3 — MongoDB URI**\n\nGet from [MongoDB Atlas](https://cloud.mongodb.com) → Connect → Drivers.\n"
              "Whitelist `0.0.0.0/0` in **Network Access**.",
    "help_5": "📖 **Steps 4 & 5 — DB & Collection**\n\nUse 🔍 Browse buttons in "
              "⚙️ Configure to pick your database and collection.",
    "help_6": "📖 **Step 6 — Target Channel**\n\nForward a message from your channel "
              "to @userinfobot.\nSet the numeric ID (starts with `-100`) as your target.",
}


def help_nav(page_key: str) -> InlineKeyboardMarkup:
    order = ["help_1", "help_2", "help_3", "help_4", "help_5", "help_6"]
    idx   = order.index(page_key)
    nav   = []
    if idx > 0:
        nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=order[idx - 1]))
    if idx < len(order) - 1:
        nav.append(InlineKeyboardButton("Next ➡️", callback_data=order[idx + 1]))
    buttons = [nav] if nav else []
    if page_key == "help_6":
        buttons.append([InlineKeyboardButton("⚙️ Start Configuring", callback_data="settings_menu")])
    buttons.append([InlineKeyboardButton("🏠 Home", callback_data="go_home")])
    return InlineKeyboardMarkup(buttons)
