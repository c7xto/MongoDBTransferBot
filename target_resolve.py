"""
target_resolve.py — shared Telegram target-string -> numeric chat_id resolution.
Used by transfer.py, monitor.py, and prescan.py, each of which independently
resolves user_cfg["target"] (invite link / @username / numeric ID) via a live
Pyrogram worker client.
"""
from __future__ import annotations

from pyrogram import Client

import config as cfg

L = cfg.logger


async def resolve_target_chat_id(
        worker: Client,
        target: str,
        *,
        user_id: int | None = None,
        raise_friendly_error: bool = False,
) -> int:
    """
    Resolve user_cfg["target"] (invite link / @username / numeric ID) into a
    numeric chat_id. The three input shapes are mutually exclusive for every
    value that can actually reach here (mdb.py's wizard validation already
    only ever persists one of these three shapes), so a single branch order
    is safe for all callers.

    raise_friendly_error=True reproduces prescan.py's behavior: a failed
    join_chat is logged and retried via get_chat, and total failure raises a
    user-facing RuntimeError. =False (default) reproduces transfer.py's and
    monitor.py's current behavior: a bare except silently falls through to
    get_chat, and total failure propagates the raw exception unwrapped.
    """
    if "t.me/+" in target or "t.me/joinchat" in target:
        invite = target if target.startswith("https://") else "https://" + target
        if raise_friendly_error:
            try:
                return (await worker.join_chat(invite)).id
            except Exception as join_err:
                L.warning(f"[SCAN] join_chat failed  err={join_err}  user={user_id}")
                try:
                    return (await worker.get_chat(invite)).id
                except Exception:
                    raise RuntimeError(
                        f"Cannot access channel. Banned or invalid link.\n`{join_err}`")
        try:
            return (await worker.join_chat(invite)).id
        except Exception:
            return (await worker.get_chat(invite)).id
    elif target.startswith("@"):
        return (await worker.get_chat(target)).id
    else:
        return int(target)
