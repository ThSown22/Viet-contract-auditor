from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class IndexingCheckpoint:
    completed_chunk_ids: set[str] = field(default_factory=set)
    failed_chunks: dict[str, str] = field(default_factory=dict)
    updated_at: str = ""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_empty_checkpoint() -> IndexingCheckpoint:
    return IndexingCheckpoint(updated_at=utc_now_iso())


def load_checkpoint(checkpoint_path: Path) -> IndexingCheckpoint:
    if not checkpoint_path.exists():
        return create_empty_checkpoint()

    with checkpoint_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    return IndexingCheckpoint(
        completed_chunk_ids=set(data.get("completed_chunk_ids") or []),
        failed_chunks=dict(data.get("failed_chunks") or {}),
        updated_at=str(data.get("updated_at") or utc_now_iso()),
    )


def checkpoint_to_dict(checkpoint: IndexingCheckpoint) -> dict:
    return {
        "completed_chunk_ids": sorted(checkpoint.completed_chunk_ids),
        "failed_chunks": checkpoint.failed_chunks,
        "updated_at": checkpoint.updated_at,
    }


def save_checkpoint(checkpoint_path: Path, checkpoint: IndexingCheckpoint) -> None:
    checkpoint.updated_at = utc_now_iso()
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    temp_path = checkpoint_path.with_suffix(".tmp")
    try:
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(checkpoint_to_dict(checkpoint), handle, ensure_ascii=False, indent=2)
        temp_path.replace(checkpoint_path)
    except Exception:
        if temp_path.exists():
            temp_path.unlink()
        raise


def mark_chunk_completed(checkpoint: IndexingCheckpoint, chunk_id: str) -> None:
    checkpoint.completed_chunk_ids.add(chunk_id)
    checkpoint.failed_chunks.pop(chunk_id, None)
    checkpoint.updated_at = utc_now_iso()


def mark_chunk_failed(checkpoint: IndexingCheckpoint, chunk_id: str, reason: str) -> None:
    checkpoint.failed_chunks[chunk_id] = reason.strip() or "Unknown indexing error"
    checkpoint.updated_at = utc_now_iso()


def is_chunk_completed(checkpoint: IndexingCheckpoint, chunk_id: str) -> bool:
    return chunk_id in checkpoint.completed_chunk_ids
