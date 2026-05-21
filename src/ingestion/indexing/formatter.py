from __future__ import annotations

from typing import Any

from src.ingestion.schemas.models import LegalChunk


def format_chunk_text(chunk: LegalChunk) -> str:
    header_lines = [
        f"Nguồn luật: {chunk.law_name}",
        f"Mã luật: {chunk.law_id}",
    ]
    if chunk.article_titles:
        header_lines.append(f"Tiêu đề điều: {' | '.join(chunk.article_titles)}")

    return "\n".join(header_lines) + f"\n\n{chunk.text.strip()}"


def format_chunk_document(chunk: LegalChunk) -> dict[str, Any]:
    return {
        "chunk_id": chunk.chunk_id,
        "text": format_chunk_text(chunk),
        "metadata": {
            "law_id": chunk.law_id,
            "law_name": chunk.law_name,
            "article_ids": chunk.article_ids,
            "article_titles": chunk.article_titles,
            "prev_chunk_id": chunk.prev_chunk_id,
            "next_chunk_id": chunk.next_chunk_id,
            "token_count": chunk.token_count,
            "char_count": chunk.char_count,
            "has_overlap": chunk.has_overlap,
        },
    }


def format_chunk_documents(chunks: list[LegalChunk]) -> list[dict[str, Any]]:
    return [format_chunk_document(chunk) for chunk in chunks]
