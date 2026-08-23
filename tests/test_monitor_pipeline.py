from types import SimpleNamespace

import pytest

import config as cfg
import monitor


class FakeStream:
    def __init__(self, changes):
        self.changes = changes
        self.resume_token = {"_data": "checkpoint"}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        for change in self.changes:
            yield change


class FakeCollection:
    def __init__(self, change):
        self.change = change
        self.calls = 0

    def watch(self, *args, **kwargs):
        self.calls += 1
        return FakeStream([] if self.calls == 1 else [self.change])


class FakeDB:
    def __init__(self, change):
        self.collection = FakeCollection(change)

    def __getitem__(self, name):
        return self.collection


class FakeWorker:
    def __init__(self, user_id):
        self.user_id = user_id
        self.sent = []

    async def send_cached_media(self, chat_id, file_id, caption):
        self.sent.append(file_id)
        cfg.active_monitors.pop(self.user_id, None)


class FakeAdmin:
    async def send_message(self, *args, **kwargs):
        return SimpleNamespace()


@pytest.mark.asyncio
async def test_monitor_delivers_modern_schema_and_checkpoints(monkeypatch):
    user_id = 24680
    worker = FakeWorker(user_id)
    checkpoints = []
    ledger = set()

    async def mark_sent(db, ids):
        ledger.update(ids)
        return True

    async def save_token(db, token):
        # The ledger must be durable before its stream position advances.
        assert "BQADnew" in ledger
        checkpoints.append(token)

    monkeypatch.setattr(monitor.target_resolve, "resolve_target_chat_id", _async_value(-1001))
    monkeypatch.setattr(monitor, "load_monitor_resume_token", _async_value(None))
    monkeypatch.setattr(monitor, "filter_already_sent", _async_value(set()))
    monkeypatch.setattr(monitor, "mark_as_sent", mark_sent)
    monkeypatch.setattr(monitor, "save_monitor_resume_token", save_token)

    await monitor.run_monitor(
        worker,
        FakeAdmin(),
        {"_id": user_id, "target": "-1001", "col_name": "files", "db_name": "source"},
        FakeDB({"fullDocument": {"_id": "mongo-id", "file_id": "BQADnew"}}),
        object(),
    )

    assert worker.sent == ["BQADnew"]
    assert checkpoints == [{"_data": "checkpoint"}]


def _async_value(value):
    async def inner(*args, **kwargs):
        return value
    return inner
