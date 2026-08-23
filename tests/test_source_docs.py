from bson import ObjectId

from source_docs import (
    SOURCE_PROJECTION,
    get_dedup_key,
    get_file_id,
    get_match_keys,
    with_resolved_file_id,
)


def test_legacy_schema_uses_string_id():
    assert get_file_id({"_id": "BQADlegacy"}) == "BQADlegacy"


def test_modern_schema_prefers_file_id():
    doc = {"_id": ObjectId(), "file_id": "BQADmodern"}
    assert get_file_id(doc) == "BQADmodern"


def test_explicit_file_id_wins_over_legacy_id():
    assert get_file_id({"_id": "old", "file_id": "new"}) == "new"


def test_object_id_without_file_id_is_invalid():
    assert get_file_id({"_id": ObjectId()}) is None


def test_blank_file_id_is_invalid():
    assert get_file_id({"file_id": "   "}) is None


def test_unique_id_is_preferred_dedup_identity():
    doc = {"file_id": "abc", "file_unique_id": " UNIQUE "}
    assert get_dedup_key(doc) == "unique:unique"


def test_file_id_is_dedup_fallback():
    assert get_dedup_key({"file_id": "abc"}) == "file:abc"


def test_match_keys_include_filename_caption_and_ids():
    keys = get_match_keys({
        "file_id": "FILE",
        "file_unique_id": "UNIQUE",
        "file_name": " Movie.MKV ",
        "caption": " Caption ",
    })
    assert {"file", "unique", "movie.mkv", "caption", "unique:unique"} <= set(keys)


def test_resolved_copy_does_not_mutate_source():
    original = {"file_id": "abc"}
    resolved = with_resolved_file_id(original)
    assert resolved["resolved_file_id"] == "abc"
    assert "resolved_file_id" not in original


def test_projection_contains_both_schema_fields():
    assert SOURCE_PROJECTION["_id"] == 1
    assert SOURCE_PROJECTION["file_id"] == 1

