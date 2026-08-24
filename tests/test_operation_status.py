from types import SimpleNamespace

import pytest

import config as cfg
import prescan
import ui


def test_prescan_card_exposes_stage_counts_and_rate_limit_expectation():
    card = ui.build_prescan_card({
        "phase": "Scanning target channel for existing files",
        "step": 1,
        "current": 54_000,
        "total": 200_000,
        "messages": 54_000,
        "keys": 158_340,
        "elapsed": 600,
    })

    assert "PRE-SCAN IN PROGRESS" in card
    assert "54,000" in card
    assert "158,340" in card
    assert "Telegram may briefly pause" in card


def test_monitor_and_failed_transfer_cards_explain_current_state():
    monitor_card = ui.build_monitor_card({
        "status": "MongoDB stream disconnected — reconnecting in 5s",
        "sent": 12,
        "failed": 1,
        "elapsed": 65,
    }, "waiting")
    transfer_card = ui.build_progress_card(
        100, 1000, 30, state=ui.TransferState.FAILED,
        status="Stopped by error: database unavailable")

    assert "LIVE MONITOR WAITING" in monitor_card
    assert "reconnecting in 5s" in monitor_card
    assert "TRANSFER FAILED" in transfer_card
    assert "database unavailable" in transfer_card


def test_transfer_startup_failure_is_terminal_even_before_total_is_loaded():
    card = ui.build_progress_card(
        0, 0, 0, state=ui.TransferState.FAILED,
        status="Could not start: Telegram login failed")

    assert "TRANSFER FAILED" in card
    assert "Could not start" in card


def test_home_reports_tasks_during_their_startup_phase():
    class RunningTask:
        @staticmethod
        def done():
            return False

    user_id = 82002
    cfg.active_tasks[f"prescan:{user_id}"] = RunningTask()
    cfg.active_tasks[f"monitor:{user_id}"] = RunningTask()
    try:
        card = ui.home_card_text({
            "total_files": 10,
            "sent_files": 2,
            "remaining": 8,
        }, user_id)
    finally:
        cfg.active_tasks.pop(f"prescan:{user_id}", None)
        cfg.active_tasks.pop(f"monitor:{user_id}", None)

    assert "Monitor › `Running`" in card
    assert "Pre-Scan › `Running`" in card


class _FakeAdmin:
    def __init__(self):
        self.edits = []

    async def edit_message_text(self, chat_id, message_id, text, reply_markup=None):
        self.edits.append((chat_id, message_id, text, reply_markup))

    async def send_message(self, *args, **kwargs):
        return SimpleNamespace(id=99)


class _FakeHistoryClient:
    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def get_chat_history_count(self, target_id):
        return 2

    def get_chat_history(self, target_id):
        async def iterate():
            yield SimpleNamespace(
                document=SimpleNamespace(file_unique_id="unique-1", file_name="one.mkv"),
                video=None, audio=None, photo=None, caption=None)
            yield SimpleNamespace(
                document=SimpleNamespace(file_unique_id="unique-2", file_name="two.mkv"),
                video=None, audio=None, photo=None, caption=None)
        return iterate()


class _EmptyCursor:
    def sort(self, *args):
        return self

    def __aiter__(self):
        async def iterate():
            if False:
                yield None
        return iterate()


class _EmptyCollection:
    async def count_documents(self, query):
        return 0

    def find(self, *args, **kwargs):
        return _EmptyCursor()


class _SourceDB:
    def __getitem__(self, name):
        return _EmptyCollection()


@pytest.mark.asyncio
async def test_prescan_edits_a_live_card_through_start_and_completion(monkeypatch):
    user_id = 82001
    admin = _FakeAdmin()
    cfg.prescan_msg_ids[user_id] = 99

    monkeypatch.setattr(prescan, "Client", _FakeHistoryClient)
    monkeypatch.setattr(prescan.crypto, "decrypt_str", lambda value: "session")
    monkeypatch.setattr(
        prescan.target_resolve, "resolve_target_chat_id",
        _async_value(-1001))
    monkeypatch.setattr(prescan, "index_channel_key", _async_value(None))
    monkeypatch.setattr(prescan, "get_channel_index_count", _async_value(4))
    monkeypatch.setattr(prescan, "filter_in_channel_index", _async_value(set()))
    monkeypatch.setattr(prescan, "mark_as_sent", _async_value(True))
    monkeypatch.setattr(prescan, "count_sent_ids", _async_value(0))

    try:
        await prescan.run_prescan(
            admin,
            {
                "_id": user_id,
                "userbot_session": "encrypted",
                "api_id": 12345,
                "api_hash": "hash",
                "target": "-1001",
                "col_name": "files",
            },
            _SourceDB(),
            object(),
        )
    finally:
        cfg.prescan_msg_ids.pop(user_id, None)
        cfg.prescan_progress.pop(user_id, None)

    rendered = [edit[2] for edit in admin.edits]
    assert any("PRE-SCAN IN PROGRESS" in text for text in rendered)
    assert "PRE-SCAN COMPLETE" in rendered[-1]
    assert "2" in rendered[-1]


def _async_value(value):
    async def inner(*args, **kwargs):
        return value
    return inner
