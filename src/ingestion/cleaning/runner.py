"""Phase 2.5 runner - content cleaning pipeline."""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
    from text_cleaner import VietnameseLegalTextCleaner
    from validator import CleanedContentValidator
else:
    from .text_cleaner import VietnameseLegalTextCleaner
    from .validator import CleanedContentValidator

from src.ingestion.scraping.normalizers.structured_parser import reconstruct_structured_text


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = PROJECT_ROOT / "data"
INGESTION_DIR = DATA_DIR / "ingestion"
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "cleaning.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def count_articles(text: str) -> int:
    count = 0
    for line in text.splitlines():
        if not line.startswith("Điều "):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        if parts[1].rstrip(".:").isdigit():
            count += 1
    return count


def resolve_input_text(data: dict) -> str:
    structured_text = data.get("structured_text")
    clean_text = data.get("clean_text", "")
    article_count = int(data.get("article_count") or 0)
    if structured_text and article_count and len(structured_text) >= max(5000, int(len(clean_text) * 0.6)):
        return structured_text

    articles = data.get("articles") or []
    if articles:
        normalized_articles = []
        for article in articles:
            title = str((article or {}).get("title", "")).strip()
            text = str((article or {}).get("text", "")).strip()
            if not title:
                continue
            normalized_articles.append(type("Article", (), {"title": title, "text": text})())
        if normalized_articles:
            candidate = reconstruct_structured_text(normalized_articles)
            if len(candidate) >= max(5000, int(len(clean_text) * 0.6)):
                return candidate

    return clean_text


def main() -> int:
    logger.info("\n" + "=" * 80)
    logger.info("START PHASE 2.5: CONTENT CLEANING PIPELINE")
    logger.info("=" * 80)

    input_dir = INGESTION_DIR / "scraped_content"
    output_dir = DATA_DIR / "processed"
    output_dir.mkdir(parents=True, exist_ok=True)

    cleaner = VietnameseLegalTextCleaner()
    validator = CleanedContentValidator(
        min_chars=5000,
        required_keywords=["Điều 1"],
        blacklist_keywords=[
            "VĂN PHÒNG QUỐC HỘI",
            "Căn cứ Hiến pháp",
            "Quảng cáo",
        ],
    )

    json_files = sorted(input_dir.glob("*.json"))
    if not json_files:
        logger.error("No JSON files found in %s", input_dir)
        return 1

    stats = {"success": 0, "failed": 0, "total": len(json_files)}

    for json_file in json_files:
        try:
            logger.info("\n%s", "=" * 60)
            logger.info("Processing: %s", json_file.name)
            output_file = output_dir / f"{json_file.stem}.txt"

            with json_file.open("r", encoding="utf-8") as handle:
                data = json.load(handle)

            raw_text = resolve_input_text(data)
            law_name = data.get("law_name", json_file.stem)

            if not raw_text:
                logger.error("Missing clean_text/structured_text")
                stats["failed"] += 1
                continue

            logger.info("  Input: %s chars", f"{len(raw_text):,}")

            cleaned = cleaner.clean(raw_text, law_name=law_name)
            removed = len(raw_text) - len(cleaned)
            logger.info(
                "  After cleaning: %s chars (removed %s)",
                f"{len(cleaned):,}",
                f"{removed:,}",
            )

            is_valid, error_message = validator.validate(cleaned, law_name)
            if not is_valid:
                logger.error("Validation failed: %s", error_message)
                if output_file.exists():
                    output_file.unlink()
                    logger.info("  Removed stale output: %s", output_file.name)
                stats["failed"] += 1
                continue

            output_file.write_text(cleaned, encoding="utf-8")

            logger.info("  Articles: %s", count_articles(cleaned))
            logger.info("Saved: %s", output_file.name)
            stats["success"] += 1

        except Exception as exc:
            logger.error("Error processing %s: %s", json_file.name, exc, exc_info=True)
            stats["failed"] += 1

    logger.info("\n" + "=" * 80)
    logger.info("CLEANING PIPELINE COMPLETED")
    logger.info("Success: %s/%s", stats["success"], stats["total"])
    logger.info("Failed: %s/%s", stats["failed"], stats["total"])
    logger.info("Output: %s", output_dir)
    logger.info("=" * 80)
    return 0 if stats["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
