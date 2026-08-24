from types import SimpleNamespace

import pytest

import config as cfg
import mdb


class _FakeTask:
    def __init__(self):
        self.cancelled = False

    def done(self):
        return False

    def cancel(self):
        self.cancelled = True

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


@pytest.mark.asyncio
async def test_emergency_stop_cancels_prescan_and_monitor_together():
    user_id = 987657
    prescan_task = _FakeTask()
    monitor_task = _FakeTask()
    answers = []

    async def answer(text="", alert=False):
        answers.append((text, alert))

    cfg.active_tasks[f"prescan:{user_id}"] = prescan_task
    cfg.active_tasks[f"monitor:{user_id}"] = monitor_task
    cfg.active_monitors[user_id] = True
    try:
        await mdb._cb_ctrl_stop(
            None, SimpleNamespace(message=SimpleNamespace()), {}, user_id,
            "ctrl_stop", answer)
    finally:
        cfg.active_tasks.pop(f"prescan:{user_id}", None)
        cfg.active_tasks.pop(f"monitor:{user_id}", None)
        cfg.active_monitors.pop(user_id, None)

    assert prescan_task.cancelled is True
    assert monitor_task.cancelled is True
    assert answers == [
        ("⏹️  Stopping monitor and pre-scan… status will update in chat", False)
    ]


@pytest.mark.asyncio
async def test_start_transfer_is_blocked_while_prescan_runs():
    user_id = 987658
    prescan_task = _FakeTask()
    answers = []

    async def answer(text="", alert=False):
        answers.append((text, alert))

    cfg.active_tasks[f"prescan:{user_id}"] = prescan_task
    try:
        await mdb._cb_start_transfer(
            None, SimpleNamespace(message=SimpleNamespace()),
            {"is_configured": True}, user_id, "start_transfer", answer)
    finally:
        cfg.active_tasks.pop(f"prescan:{user_id}", None)
        cfg.launch_locks.pop(user_id, None)

    assert answers == [
        ("⚠️ Pre-scan is already running. Stop it first.", True)
    ]


@pytest.mark.asyncio
async def test_wipe_is_blocked_during_any_data_operation():
    user_id = 987659
    monitor_task = _FakeTask()
    answers = []

    async def answer(text="", alert=False):
        answers.append((text, alert))

    cfg.active_tasks[f"monitor:{user_id}"] = monitor_task
    try:
        await mdb._cb_wipe_confirmed(
            None, SimpleNamespace(message=SimpleNamespace()), {}, user_id,
            "wipe_confirmed", answer)
    finally:
        cfg.active_tasks.pop(f"monitor:{user_id}", None)

    assert answers == [
        ("⚠️ Stop Live monitor before wiping data.", True)
    ]


@pytest.mark.asyncio
async def test_configuration_reset_is_blocked_during_transfer():
    user_id = 987660
    answers = []

    async def answer(text="", alert=False):
        answers.append((text, alert))

    cfg.active_transfers[user_id] = True
    try:
        await mdb._cb_setup_confirm_reset(
            None, SimpleNamespace(message=SimpleNamespace()), {}, user_id,
            "setup_confirm_reset", answer)
    finally:
        cfg.active_transfers.pop(user_id, None)

    assert answers == [
        ("⚠️ Stop Transfer before resetting configuration.", True)
    ]
