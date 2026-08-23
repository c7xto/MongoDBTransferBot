import pytest

from transfer import _cast_last_id, _is_video, _make_media_item


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


def test_media_item_rejects_missing_id():
    with pytest.raises(ValueError, match="file_id"):
        _make_media_item({"file_name": "Movie.mkv"})


def test_integer_cursor_cast():
    assert _cast_last_id("42", 1) == 42


def test_string_cursor_cast():
    assert _cast_last_id(42, "sample") == "42"
