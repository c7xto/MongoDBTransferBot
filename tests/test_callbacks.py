from types import SimpleNamespace

import pytest

import config as cfg
import mdb


@pytest.mark.asyncio
async def test_stop_callback_uses_answer_wrapper_contract():
    user_id = 987656
    answers = []

    async def answer(text="", alert=False):
        answers.append((text, alert))

    query = SimpleNamespace(message=SimpleNamespace())
    cfg.active_transfers[user_id] = True
    cfg.paused_transfers[user_id] = True
    cfg.transfer_progress[user_id] = {"sent": 42}
    cfg.progress_msg_ids.pop(user_id, None)

    try:
        await mdb._cb_ctrl_stop(
            None, query, {}, user_id, "ctrl_stop", answer
        )
    finally:
        cfg.active_transfers.pop(user_id, None)
        cfg.paused_transfers.pop(user_id, None)
        cfg.transfer_progress.pop(user_id, None)
        cfg.progress_msg_ids.pop(user_id, None)

    assert answers == [
        ("⏹️  Stopped  ·  42 files sent  ·  cursor saved", False)
    ]
