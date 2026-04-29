import json
import shutil
from pathlib import Path

import jsonlines

from src.ingestion.scraping.normalizers.content_cleaner import ContentCleaner
from src.ingestion.scraping.state_manager import StateManager


def test_content_cleaner_removes_artifacts_and_keeps_structure():
    cleaner = ContentCleaner(
        {
            "min_length_chars": 10,
            "required_keywords": ["Dieu 1"],
            "blacklist_keywords": ["Dang nhap"],
        }
    )
    raw_text = """
    Trang chu
    Dieu 1. Pham vi dieu chinh

    Khoan 1. Noi dung.
    Dang nhap
    """
    cleaned = cleaner.clean(raw_text)
    assert "Trang chu" not in cleaned
    assert "Dang nhap" not in cleaned
    assert "Dieu 1. Pham vi dieu chinh" in cleaned
    assert cleaner.has_structure(cleaned)


def test_state_manager_marks_processed():
    workspace = Path("tests/runtime/state_manager")
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    jsonl_path = workspace / "discovered.jsonl"
    records = [
        {"url": "https://example.com/a", "title": "A", "source_domain": "example.com", "search_query": "", "is_processed": False},
        {"url": "https://example.com/b", "title": "B", "source_domain": "example.com", "search_query": "", "is_processed": False},
    ]
    with jsonlines.open(jsonl_path, mode="w") as writer:
        writer.write_all(records)

    manager = StateManager(str(jsonl_path))
    updated = manager.mark_processed(["https://example.com/a"], backup=True)
    assert updated == 1

    with jsonlines.open(jsonl_path) as reader:
        saved = list(reader)
    assert saved[0]["is_processed"] is True
    assert saved[1]["is_processed"] is False
    backup_dir = jsonl_path.parent / "backups"
    assert backup_dir.exists()
    assert any(backup_dir.iterdir())


def test_output_json_shape_round_trip():
    workspace = Path("tests/runtime/output_shape")
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    payload = {
        "law_name": "Luat Doanh nghiep",
        "law_id": "67/VBHN-VPQH",
        "source_url": "https://example.com",
        "source_domain": "example.com",
        "title": "Van ban hop nhat",
        "clean_text": "Dieu 1\nKhoan 1",
        "char_count": 15,
        "word_count": 4,
        "has_structure": True,
        "validation_passed": True,
    }
    path = workspace / "content.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert loaded["law_id"] == "67/VBHN-VPQH"
