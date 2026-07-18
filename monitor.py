"""
monitor.py — live MongoDB change-stream monitor
Multi-user mode: accepts user_cfg dict and a Motor db handle.
"""
from __future__ import annotations
import asyncio
from pyrogram import Client
from pyrogram.errors import FloodWait
import config as cfg
import target_resolve

L = cfg.logger

_CS_NOT_SUPPORTED_MARKERS = (
    "The $changeStream stage is only supported on replica sets",
    "not supported",
    "ChangeStreamHistoryLost",
    "CommandNotFound",
    "Unrecognized pipeline stage",
    "does not support change streams",
)


def _is_cs_not_supported(err: Exception) -> bool:
    msg = str(err).lower()
    return any(m.lower() in msg for m in _CS_NOT_SUPPORTED_MARKERS)


async def run_monitor(
        worker:    Client,
        admin_app: Client,
        user_cfg:  dict,
        user_db,              # AsyncIOMotorDatabase for the user's cluster
) -> None:
    """
    Live monitor — watches MongoDB for new inserts and forwards to target.
    `user_cfg` keys used: db_name, col_name, target, _id.
    """
    user_id  = user_cfg["_id"]
    admin_id = user_id

    if cfg.active_monitors.get(user_id):
        L.warning(f"[MON] Monitor already running  user={user_id}")
        return

    cfg.active_monitors[user_id] = True
    counters = {"sent": 0, "failed": 0}

    try:
        # ── Resolve target channel ────────────────────────────────────────────
        target = str(user_cfg["target"])
        target_id = await target_resolve.resolve_target_chat_id(worker, target)

        col_name = user_cfg["col_name"]

        # ── Check Change Streams support ──────────────────────────────────────
        # user_db is already connected to the right database — reuse its client.
        L.info(f"[MON] Checking Change Streams support  user={user_id}")
        cs_supported      = False
        cs_check_attempts = 2
        last_cs_err       = None

        for attempt in range(cs_check_attempts):
            try:
                async with user_db[col_name].watch(
                        [{"$match": {"operationType": "insert"}}]):
                    pass
                cs_supported = True
                break
            except Exception as cs_err:
                last_cs_err = cs_err
                if _is_cs_not_supported(cs_err):
                    L.error(f"[MON] Change Streams not supported  err={cs_err}  user={user_id}")
                    break
                L.warning(f"[MON] CS check attempt {attempt+1} transient fail — retrying  "
                           f"err={cs_err}  user={user_id}")
                if attempt < cs_check_attempts - 1:
                    await asyncio.sleep(3)

        if not cs_supported:
            L.error(f"[MON] Change Streams unavailable — monitor blocked  user={user_id}")
            cfg.active_monitors.pop(user_id, None)
            try:
                await admin_app.send_message(
                    admin_id,
                    "❌ **Monitor Failed**\n"
                    "Change Streams are not supported by your MongoDB tier.\n"
                    "Upgrade to an M10+ replica set cluster to use this feature.")
            except Exception:
                pass
            return

        col = user_db[col_name]
        L.info(f"[MON] Monitor started  col={user_cfg['db_name']}.{col_name}  "
               f"target={target_id}  user={user_id}")

        pipeline     = [{"$match": {"operationType": "insert"}}]
        resume_token = None

        while cfg.active_monitors.get(user_id):
            try:
                kw = {"full_document": "whenAvailable"}
                if resume_token:
                    kw["resume_after"] = resume_token

                async with col.watch(pipeline, **kw) as stream:
                    async for change in stream:
                        if not cfg.active_monitors.get(user_id):
                            break

                        resume_token = stream.resume_token
                        doc     = change.get("fullDocument") or {}
                        file_id = doc.get("_id")
                        caption = doc.get("caption") or doc.get("file_name", "New File")

                        if not file_id:
                            continue

                        async def _send(fid=file_id, cap=caption):
                            try:
                                await worker.send_cached_media(
                                    chat_id=target_id, file_id=str(fid),
                                    caption=f"<code>{cap}</code>")
                            except Exception:
                                await worker.send_document(
                                    chat_id=target_id, document=str(fid),
                                    caption=f"<code>{cap}</code>")

                        try:
                            await _send()
                            counters["sent"] += 1
                            L.info(f"[MON] Doc forwarded  file_id={file_id}  "
                                   f"total_sent={counters['sent']}  user={user_id}")
                        except FloodWait as fw:
                            L.warning(f"[MON] FloodWait {fw.value}s  user={user_id}")
                            await asyncio.sleep(fw.value + 3)
                            try:
                                await _send()
                                counters["sent"] += 1
                                L.info(f"[MON] FloodWait retry succeeded  "
                                       f"total_sent={counters['sent']}  user={user_id}")
                            except Exception as fe:
                                counters["failed"] += 1
                                L.warning(f"[MON] FloodWait retry failed  err={fe}  user={user_id}")
                        except Exception as se:
                            counters["failed"] += 1
                            L.warning(f"[MON] Send failed  err={se}  user={user_id}")

            except asyncio.CancelledError:
                raise
            except Exception as stream_err:
                if not cfg.active_monitors.get(user_id):
                    break
                L.warning(f"[MON] Stream dropped — reconnecting in 5s  "
                           f"err={stream_err}  user={user_id}")
                await asyncio.sleep(5)
                L.info(f"[MON] Stream reconnected  user={user_id}")

    except asyncio.CancelledError:
        L.info(f"[MON] Monitor cancelled  user={user_id}")
    except Exception as e:
        L.exception(f"[MON] Monitor error  err={e}  user={user_id}")
    finally:
        cfg.active_monitors.pop(user_id, None)
        L.info(f"[MON] Monitor stopped  sent={counters['sent']}  "
               f"failed={counters['failed']}  user={user_id}")
        try:
            await admin_app.send_message(
                admin_id,
                f"⏹ **Monitor Stopped**\n"
                f"✅ Forwarded › `{counters['sent']}`\n"
                f"❌ Failed › `{counters['failed']}`")
        except Exception:
            pass
