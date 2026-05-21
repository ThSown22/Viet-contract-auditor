from __future__ import annotations

import logging
from pathlib import Path
from collections import defaultdict

from src.ingestion.schemas.models import LegalChunk


logger = logging.getLogger(__name__)


def list_chunk_files(input_dir: Path, pattern: str) -> list[Path]:
    return sorted(path for path in input_dir.glob(pattern) if path.is_file())


def load_chunks_from_file(file_path: Path) -> list[LegalChunk]:
    chunks: list[LegalChunk] = []

    with file_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            raw_line = line.strip()
            if not raw_line:
                continue

            try:
                chunks.append(LegalChunk.model_validate_json(raw_line))
            except Exception as exc:
                raise ValueError(
                    f"Không thể parse chunk ở {file_path.name}:{line_number}: {exc}"
                ) from exc

    return chunks


def load_chunks(input_dir: Path, pattern: str) -> list[LegalChunk]:
    all_chunks: list[LegalChunk] = []
    chunk_files = list_chunk_files(input_dir, pattern)
    sources_by_chunk_id: dict[str, list[str]] = defaultdict(list)

    for chunk_file in chunk_files:
        file_chunks = load_chunks_from_file(chunk_file)
        logger.info("Đã load %s chunk từ %s", len(file_chunks), chunk_file.name)
        for line_number, chunk in enumerate(file_chunks, start=1):
            sources_by_chunk_id[chunk.chunk_id].append(f"{chunk_file.name}:{line_number}")
        all_chunks.extend(file_chunks)

    duplicate_sources = {
        chunk_id: sources
        for chunk_id, sources in sources_by_chunk_id.items()
        if len(sources) > 1
    }
    if duplicate_sources:
        preview = "; ".join(
            f"{chunk_id} -> {', '.join(sources[:3])}"
            for chunk_id, sources in sorted(duplicate_sources.items())[:10]
        )
        raise ValueError(
            "Phát hiện chunk_id bị trùng trong input indexing. "
            f"Tổng số ID trùng: {len(duplicate_sources)}. "
            f"Ví dụ: {preview}"
        )

    return all_chunks
