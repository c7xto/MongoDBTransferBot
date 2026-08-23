import pytest
from pyrogram.types import InputMediaDocument, InputMediaVideo

from transfer import _cast_last_id, _is_video, _make_media_item, _media_kind


@pytest.mark.parametrize("name", ["movie.mkv", "movie.MP4", "clip.webm"])
def test_video_extension_detection(name):
    assert _is_video({"file_name": name})


def test_video_mime_detection():
    assert _is_video({"file_name": "unknown.bin", "mime_type": "video/x-matroska"})


def test_non_video_detection():
    assert not _is_video({"file_name": "archive.zip", "mime_type": "application/zip"})


def test_media_item_accepts_modern_schema():
    item = _make_media_item({"file_id": "BQADdocument", "file_name": "Movie.mkv"})
    assert item.media == "BQADdocument"


def test_modern_document_uses_declared_file_type_not_file_id_prefix():
    item = _make_media_item({
        "file_id": "BQACmodern",
        "file_name": "Movie.mkv",
        "file_type": "document",
        "mime_type": "video/x-matroska",
    })
    assert isinstance(item, InputMediaDocument)


def test_modern_video_uses_declared_file_type():
    item = _make_media_item({
        "file_id": "BAACmodern",
        "file_name": "Movie.bin",
        "file_type": "video",
    })
    assert isinstance(item, InputMediaVideo)


def test_unknown_modern_id_defaults_to_document():
    assert _media_kind({"file_id": "BQACmodern", "file_name": "archive.bin"}) == "document"


def test_media_item_rejects_missing_id():
    with pytest.raises(ValueError, match="file_id"):
        _make_media_item({"file_name": "Movie.mkv"})


def test_integer_cursor_cast():
    assert _cast_last_id("42", 1) == 42


def test_string_cursor_cast():
    assert _cast_last_id(42, "sample") == "42"
