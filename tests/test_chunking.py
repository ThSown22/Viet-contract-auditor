"""
Integration test for Phase 4 - Semantic Chunking.
Tests the chunking pipeline end-to-end with sample legal text.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ingestion.chunking.chunker import VietnameseLegalChunker
from src.ingestion.schemas.models import ChunkingConfig, LegalChunk


@pytest.fixture
def sample_legal_text() -> str:
    return """Điều 1. Phạm vi điều chỉnh
Bộ luật này quy định địa vị pháp lý của cá nhân, pháp nhân và các quan hệ dân sự.

Điều 2. Nguyên tắc cơ bản của pháp luật dân sự
1. Mọi cá nhân, pháp nhân đều bình đẳng, không được lấy bất kỳ lý do nào để phân biệt đối xử.
2. Cá nhân, pháp nhân xác lập, thực hiện quyền, nghĩa vụ dân sự của mình trên cơ sở tự do, tự nguyện.

Điều 3. Giải thích từ ngữ
Trong Bộ luật này, các từ ngữ dưới đây được hiểu như sau:
1. Cá nhân là con người với tư cách là chủ thể của quan hệ pháp luật dân sự.
2. Pháp nhân là tổ chức có tổ chức chặt chẽ, có tài sản độc lập với cá nhân."""


def test_phase4_chunking_pipeline(sample_legal_text: str) -> None:
    config = ChunkingConfig(
        target_min_tokens=50,
        target_max_tokens=200,
        overlap_tokens=20,
    )
    chunker = VietnameseLegalChunker(config)

    chunks = chunker.chunk_document(
        law_name="Bộ luật Dân sự (Test)",
        full_text=sample_legal_text,
        law_id="TEST/2024",
    )

    assert len(chunks) > 0, "No chunks generated"
    assert all(isinstance(chunk, LegalChunk) for chunk in chunks), "Invalid chunk type"

    for chunk in chunks:
        assert chunk.law_name == "Bộ luật Dân sự (Test)"
        assert chunk.law_id == "TEST/2024"
        assert chunk.token_count > 0
        assert chunk.char_count > 0
        assert len(chunk.article_ids) > 0
        assert chunk.chunk_id.startswith("blds")

    token_counts = [chunk.token_count for chunk in chunks]
    in_range = sum(1 for token_count in token_counts if 50 <= token_count <= 200)
    compliance = in_range / len(chunks)
    assert compliance >= 0.8, f"Only {compliance:.0%} chunks in range"

    if len(chunks) >= 2:
        assert chunks[0].prev_chunk_id is None
        assert chunks[0].next_chunk_id == chunks[1].chunk_id
        assert chunks[-1].next_chunk_id is None
        assert chunks[-1].prev_chunk_id == chunks[-2].chunk_id
        assert any(chunk.has_overlap for chunk in chunks[1:])

    for chunk in chunks:
        json_str = chunk.model_dump_json(ensure_ascii=False)
        parsed = json.loads(json_str)
        assert "chunk_id" in parsed
        assert "text" in parsed
        assert "token_count" in parsed

    all_text = " ".join(chunk.text for chunk in chunks)
    assert "Điều 1" in all_text
    assert "Điều 2" in all_text
    assert "Điều 3" in all_text
