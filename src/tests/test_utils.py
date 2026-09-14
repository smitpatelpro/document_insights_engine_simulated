from utils import build_summary, hash_content, mock_summary_content_only


def test_hash_and_summary_core():
    """Verify content hashing is stable and summary builders merge correctly."""
    assert hash_content("hello") == hash_content("hello")
    assert hash_content("hello") != hash_content("Hello")
    assert len(hash_content("")) == 64
    s = mock_summary_content_only("")
    assert s == {
        "word_count": 0,
        "character_count": 0,
        "top_line": "",
        "abstract": "",
    }
    s = mock_summary_content_only("hello world\nsecond line")
    assert s["word_count"] == 4
    assert s["top_line"] == "hello world"
    merged = build_summary("My Title", "hi there", s)
    assert merged["title"] == "My Title"
    assert merged["word_count"] == 4
