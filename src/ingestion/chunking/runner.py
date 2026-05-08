from __future__ import annotations

import json
import logging
import os
import sys
import tempfile
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = PROJECT_ROOT / "src" / "ingestion" / "config" / "chunking.yaml"
LOG_DIR = PROJECT_ROOT / "logs"

if __package__ in {None, ""}:
    sys.path.insert(0, str(PROJECT_ROOT))
    from src.ingestion.chunking.chunker import VietnameseLegalChunker
    from src.ingestion.schemas.models import ChunkingConfig
else:
    from .chunker import VietnameseLegalChunker
    from ..schemas.models import ChunkingConfig


logger = logging.getLogger(__name__)


def configure_stdout() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def should_log_to_file() -> bool:
    return os.getenv("LOG_TO_FILE", "true").lower() == "true"


def build_log_handlers() -> list[logging.Handler]:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if should_log_to_file():
        LOG_DIR.mkdir(exist_ok=True)
        handlers.append(logging.FileHandler(LOG_DIR / "chunking.log", encoding="utf-8"))
    return handlers


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=build_log_handlers(),
)


def load_config(config_path: Path | None = None) -> dict:
    config_path = config_path or CONFIG_PATH
    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def resolve_data_root() -> Path:
    return Path(os.getenv("DATA_DIR", str(PROJECT_ROOT)))


def is_processed_output(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 0


def resolve_metadata_path(input_dir: Path, stem: str) -> Path:
    return input_dir.parent / "ingestion" / "scraped_content" / f"{stem}.json"


def load_document_metadata(input_dir: Path, txt_file: Path) -> tuple[str, str]:
    fallback_name = txt_file.stem.replace("_", " ").strip()
    fallback_id = txt_file.stem
    metadata_path = resolve_metadata_path(input_dir, txt_file.stem)

    if not metadata_path.exists():
        return fallback_name, fallback_id

    try:
        with metadata_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception as exc:
        logger.warning("Failed to load metadata for %s: %s", txt_file.name, exc)
        return fallback_name, fallback_id

    law_name = str(data.get("law_name") or data.get("title") or fallback_name).strip()
    law_id = str(data.get("law_id") or fallback_id).strip()
    return law_name or fallback_name, law_id or fallback_id


def write_chunks_atomic(output_file: Path, chunks: list) -> None:
    temp_fd, temp_path = tempfile.mkstemp(
        dir=output_file.parent,
        prefix=f".{output_file.stem}_",
        suffix=".tmp",
    )
    try:
        with os.fdopen(temp_fd, "w", encoding="utf-8") as handle:
            for chunk in chunks:
                handle.write(chunk.model_dump_json(ensure_ascii=False) + "\n")
        Path(temp_path).replace(output_file)
    except Exception:
        temp_file = Path(temp_path)
        if temp_file.exists():
            temp_file.unlink()
        raise


def main() -> int:
    configure_stdout()
    logger.info("\n%s", "=" * 80)
    logger.info("START PHASE 4: SEMANTIC CHUNKING PIPELINE")
    logger.info("%s", "=" * 80)

    config_data = load_config()
    config = ChunkingConfig(**config_data["chunking"])
    data_root = resolve_data_root()
    input_dir = data_root / config_data["input"]["base_dir"]
    output_dir = data_root / config_data["output"]["base_dir"]
    output_dir.mkdir(parents=True, exist_ok=True)
    force_rebuild = os.getenv("FORCE_REBUILD", "false").lower() == "true"

    txt_files = sorted(input_dir.glob("*.txt"))
    if not txt_files:
        logger.error("No TXT files found in %s", input_dir)
        return 1

    logger.info("Input: %s", input_dir)
    logger.info("Output: %s", output_dir)
    logger.info(
        "Config: min=%s max=%s overlap=%s",
        config.target_min_tokens,
        config.target_max_tokens,
        config.overlap_tokens,
    )

    chunker = VietnameseLegalChunker(config)
    stats = {"success": 0, "failed": 0, "total": len(txt_files)}

    for txt_file in txt_files:
        output_file = output_dir / f"{txt_file.stem}_chunks.{config_data['output']['format']}"
        if is_processed_output(output_file) and not force_rebuild:
            logger.info("SKIP: %s (already processed)", output_file.name)
            stats["success"] += 1
            continue

        logger.info("\n%s", "=" * 60)
        logger.info("Processing: %s", txt_file.name)

        try:
            full_text = txt_file.read_text(encoding="utf-8")
            law_name, law_id = load_document_metadata(input_dir, txt_file)
            logger.info("Law: %s", law_name)
            logger.info("Law ID: %s", law_id)
            logger.info("Input length: %s chars", f"{len(full_text):,}")

            chunks = chunker.chunk_document(law_name=law_name, full_text=full_text, law_id=law_id)
            if not chunks:
                raise ValueError("No chunks generated")

            token_counts = [chunk.token_count for chunk in chunks]
            in_range = sum(
                1
                for token_count in token_counts
                if config.target_min_tokens <= token_count <= config.target_max_tokens
            )

            write_chunks_atomic(output_file, chunks)

            logger.info("Generated %s chunks", len(chunks))
            logger.info("Token range: %s-%s", min(token_counts), max(token_counts))
            logger.info("Token avg: %s", sum(token_counts) // len(token_counts))
            logger.info("In-range: %s/%s (%.0f%%)", in_range, len(chunks), (in_range / len(chunks)) * 100)
            logger.info("Saved: %s", output_file.name)
            stats["success"] += 1

        except Exception as exc:
            logger.error("Failed to process %s: %s", txt_file.name, exc, exc_info=True)
            stats["failed"] += 1

    logger.info("\n%s", "=" * 80)
    logger.info("SEMANTIC CHUNKING COMPLETED")
    logger.info("Success: %s/%s", stats["success"], stats["total"])
    logger.info("Failed: %s/%s", stats["failed"], stats["total"])
    logger.info("Output: %s", output_dir)
    logger.info("%s", "=" * 80)
    return 0 if stats["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
