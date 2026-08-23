import pytest
from pymongo.errors import BulkWriteError

from user_db import (
    clear_monitor_resume_token,
    load_monitor_resume_token,
    mark_as_sent,
    save_monitor_resume_token,
    save_state,
)


class FakeCollection:
    def __init__(self):
        self.docs = {}
        self.insert_error = None

    async def update_one(self, query, update, upsert=False):
        self.docs.setdefault(query["_id"], {"_id": query["_id"]}).update(update["$set"])

    async def find_one(self, query, projection=None):
        return self.docs.get(query["_id"])

    async def delete_one(self, query):
        self.docs.pop(query["_id"], None)

    async def insert_many(self, docs, ordered=False):
        if self.insert_error:
            raise self.insert_error
        for doc in docs:
            self.docs[doc["file_id"]] = dict(doc)


class FakeDB:
    def __init__(self):
        self.collections = {}

    def __getitem__(self, name):
        return self.collections.setdefault(name, FakeCollection())


@pytest.mark.asyncio
async def test_transfer_state_saves_offset_and_mode():
    db = FakeDB()
    await save_state(db, offset=1500, mode="offset")
    assert db["c7_state"].docs["transfer_state"]["offset"] == 1500
    assert db["c7_state"].docs["transfer_state"]["mode"] == "offset"


@pytest.mark.asyncio
async def test_monitor_resume_token_round_trip():
    db = FakeDB()
    token = {"_data": "resume-token"}
    await save_monitor_resume_token(db, token)
    assert await load_monitor_resume_token(db) == token
    await clear_monitor_resume_token(db)
    assert await load_monitor_resume_token(db) is None


@pytest.mark.asyncio
async def test_mark_as_sent_success_is_reported():
    assert await mark_as_sent(FakeDB(), ["one", "two"]) is True


@pytest.mark.asyncio
async def test_duplicate_only_bulk_error_is_idempotent():
    db = FakeDB()
    db["c7_sent_ids"].insert_error = BulkWriteError({
        "writeErrors": [{"code": 11000}],
        "writeConcernErrors": [],
    })
    assert await mark_as_sent(db, ["one"]) is True


@pytest.mark.asyncio
async def test_non_duplicate_bulk_error_is_failure():
    db = FakeDB()
    db["c7_sent_ids"].insert_error = RuntimeError("database unavailable")
    assert await mark_as_sent(db, ["one"], attempts=1) is False
