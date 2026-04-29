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

        logger.info("Marking %s URLs as processed", len(urls))
        if backup:
            self._create_backup()

        records = []
        with jsonlines.open(self.jsonl_path) as reader:
            for obj in reader:
                records.append(obj)

        url_set = {str(url) for url in urls}
        updated_count = 0
        for record in records:
            if str(record.get("url", "")) in url_set and not record.get("is_processed", False):
                record["is_processed"] = True
                record["processed_at"] = datetime.now().isoformat()
                updated_count += 1

        temp_path = self.jsonl_path.with_suffix(".tmp")
        with jsonlines.open(temp_path, mode="w") as writer:
            writer.write_all(records)
        temp_path.replace(self.jsonl_path)

        logger.info("Marked %s records as processed", updated_count)
        return updated_count

    def get_unprocessed_urls(self) -> List[dict]:
        unprocessed = []
        with jsonlines.open(self.jsonl_path) as reader:
            for obj in reader:
                if not obj.get("is_processed", False):
                    unprocessed.append(obj)
        logger.info("Found %s unprocessed URLs", len(unprocessed))
        return unprocessed

    def _create_backup(self) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = self.jsonl_path.parent / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        backup_path = backup_dir / f"{self.jsonl_path.stem}_backup_{timestamp}.jsonl"
        shutil.copy2(self.jsonl_path, backup_path)
        logger.info("Backup created: %s", backup_path)
        return backup_path
