import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import List

import jsonlines

logger = logging.getLogger(__name__)


class StateManager:
    """Manage discovery JSONL state updates for Phase 2."""

    def __init__(self, jsonl_path: str):
        self.jsonl_path = Path(jsonl_path)
        if not self.jsonl_path.exists():
            raise FileNotFoundError(f"Discovery JSONL not found: {jsonl_path}")
        logger.info("StateManager initialized for %s", self.jsonl_path)

    def mark_processed(self, urls: List[str], backup: bool = True) -> int:
        """Mark matching URLs as processed and rewrite the JSONL file atomically."""
        return self._update_records(urls, backup, self._mark_record_processed, "processed")

    def reset_processed(self, urls: List[str], backup: bool = True) -> int:
        """Reset matching URLs so they can be scraped again."""
        return self._update_records(urls, backup, self._reset_record_processed, "reset")

    def get_unprocessed_urls(self) -> List[dict]:
        unprocessed = [record for record in self.get_all_urls() if not record.get("is_processed", False)]
        logger.info("Found %s unprocessed URLs", len(unprocessed))
        return unprocessed

    def get_all_urls(self) -> List[dict]:
        with jsonlines.open(self.jsonl_path) as reader:
            records = list(reader)
        logger.info("Loaded %s discovery records", len(records))
        return records

    def _create_backup(self) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = self.jsonl_path.parent / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        backup_path = backup_dir / f"{self.jsonl_path.stem}_backup_{timestamp}.jsonl"
        shutil.copy2(self.jsonl_path, backup_path)
        logger.info("Backup created: %s", backup_path)
        return backup_path

    def _update_records(self, urls: List[str], backup: bool, updater, action: str) -> int:
        logger.info("%s %s URLs", action.capitalize(), len(urls))
        if backup:
            self._create_backup()

        url_set = {str(url) for url in urls}
        records = self.get_all_urls()
        updated_count = 0

        for record in records:
            if str(record.get("url", "")) not in url_set:
                continue
            updated_count += int(updater(record))

        self._write_records(records)
        logger.info("%s %s records", action.capitalize(), updated_count)
        return updated_count

    def _write_records(self, records: List[dict]) -> None:
        temp_path = self.jsonl_path.with_suffix(".tmp")
        try:
            with jsonlines.open(temp_path, mode="w") as writer:
                writer.write_all(records)
            temp_path.replace(self.jsonl_path)
        except Exception:
            if temp_path.exists():
                temp_path.unlink()
            raise

    @staticmethod
    def _mark_record_processed(record: dict) -> bool:
        if record.get("is_processed", False):
            return False
        record["is_processed"] = True
        record["processed_at"] = datetime.now().isoformat()
        return True

    @staticmethod
    def _reset_record_processed(record: dict) -> bool:
        updated = bool(record.get("is_processed", False))
        record["is_processed"] = False
        record.pop("processed_at", None)
        return updated
