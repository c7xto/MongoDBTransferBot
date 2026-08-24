from types import SimpleNamespace

import pytest
from bson import ObjectId

import config as cfg
import transfer


class FakeCursor:
    def __init__(self, documents):
        self.documents = list(documents)
        self.offset = 0
        self.maximum = None

    def sort(self, field, direction):
        self.documents.sort(key=lambda item: item[field])
        return self

    def skip(self, count):
        self.offset = count
        return self

    def limit(self, count):
        self.maximum = count
        return self

    async def to_list(self, length):
        end = self.offset + (self.maximum or length)
        return self.documents[self.offset:end]


class FakeSourceCollection:
    def __init__(self, documents):
        self.documents = list(documents)

    async def count_documents(self, query):
        if not query:
            return len(self.documents)
        boundary = query["_id"].get("$lte")
        return sum(doc["_id"] <= boundary for doc in self.documents)

    async def find_one(self, query, projection):
        return self.documents[0] if self.documents else None

    def find(self, query, projection):
        documents = self.documents
        if query:
            boundary = query["_id"]["$gt"]
            documents = [doc for doc in documents if doc["_id"] > boundary]
        return FakeCursor(documents)


class FakeSourceDB:
    def __init__(self, documents):
        self.collection = FakeSourceCollection(documents)

    def __getitem__(self, name):
        return self.collection


class FakeWorker:
    def __init__(self):
        self.groups = []
        self.is_connected = True

    async def send_message(self, chat_id, text):
        return SimpleNamespace(delete=self._delete)

    async def _delete(self):
        return None

    async def send_media_group(self, chat_id, media):
        self.groups.append([item.media for item in media])


class FakeAdmin:
    async def send_message(self, *args, **kwargs):
        return None

    async def edit_message_text(self, *args, **kwargs):
        return None


async def _no_sleep(*args, **kwargs):
    return None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "documents",
    [
        [
            {"_id": ObjectId(), "file_id": "BQADmodern1", "file_name": "One.mkv"},
            {"_id": ObjectId(), "file_id": "BQADmodern2", "file_name": "Two.mkv"},
        ],
        [
            {"_id": "BQADlegacy1", "file_name": "One.mkv"},
            {"_id": "BQADlegacy2", "file_name": "Two.mkv"},
        ],
    ],
)
async def test_pipeline_delivers_both_supported_schemas(monkeypatch, documents):
    worker = FakeWorker()
    sent = set()
    saved_states = []

    async def filter_sent(db, ids):
        return set(ids) & sent

    async def mark_sent(db, ids):
        sent.update(ids)
        return True

    async def save_state(db, last_id=None, **values):
        saved_states.append((last_id, values))

    monkeypatch.setattr(transfer.target_resolve, "resolve_target_chat_id", lambda *a, **k: _value(-1001))
    monkeypatch.setattr(transfer, "load_state", lambda db: _value({"last_id": None}))
    monkeypatch.setattr(transfer, "count_sent_ids", lambda db: _value(0))
    monkeypatch.setattr(transfer, "filter_already_sent", filter_sent)
    monkeypatch.setattr(transfer, "filter_in_channel_index", lambda db, keys: _value(set()))
    monkeypatch.setattr(transfer, "mark_as_sent", mark_sent)
    monkeypatch.setattr(transfer, "save_state", save_state)
    monkeypatch.setattr(transfer, "clear_state", lambda db: _value(None))
    monkeypatch.setattr(transfer.asyncio, "sleep", _no_sleep)

    user_id = 987654
    cfg.active_transfers[user_id] = True
    await transfer.run_transfer(
        worker,
        FakeAdmin(),
        {"_id": user_id, "col_name": "files", "target": "-1001", "speed_delay": 0.5},
        FakeSourceDB(documents),
        object(),
    )

    assert worker.groups == [["BQADmodern1", "BQADmodern2"]] or worker.groups == [["BQADlegacy1", "BQADlegacy2"]]
    assert len(sent) == 2
    assert saved_states


async def _value(value):
    return value


@pytest.mark.asyncio
async def test_duplicate_only_chunks_do_not_inherit_telegram_send_delay(monkeypatch):
    documents = [
        {"_id": f"file-{index:04d}", "file_id": f"BQADduplicate{index}", "file_name": f"{index}.mkv"}
        for index in range(25)
    ]
    worker = FakeWorker()
    sleeps = []
    saved_states = []

    async def record_sleep(seconds):
        sleeps.append(seconds)

    async def save_state(db, last_id=None, **values):
        saved_states.append((last_id, values))

    monkeypatch.setattr(transfer.target_resolve, "resolve_target_chat_id", lambda *a, **k: _value(-1001))
    monkeypatch.setattr(transfer, "load_state", lambda db: _value({"last_id": None, "offset": 0}))
    monkeypatch.setattr(transfer, "count_sent_ids", lambda db: _value(len(documents)))
    monkeypatch.setattr(transfer, "filter_already_sent", lambda db, ids: _value(set(ids)))
    monkeypatch.setattr(transfer, "filter_in_channel_index", lambda db, keys: _value(set()))
    monkeypatch.setattr(transfer, "mark_as_sent", lambda db, ids: _value(True))
    monkeypatch.setattr(transfer, "save_state", save_state)
    monkeypatch.setattr(transfer, "clear_state", lambda db: _value(None))
    monkeypatch.setattr(transfer.asyncio, "sleep", record_sleep)

    user_id = 987655
    cfg.active_transfers[user_id] = True
    await transfer.run_transfer(
        worker,
        FakeAdmin(),
        {"_id": user_id, "col_name": "files", "target": "-1001", "speed_delay": 3.5},
        FakeSourceDB(documents),
        object(),
    )

    assert worker.groups == []
    assert sleeps == []
    assert saved_states
