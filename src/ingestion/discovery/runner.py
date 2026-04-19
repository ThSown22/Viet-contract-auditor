import os
import re
import yaml
import hashlib
import jsonlines
import logging
import glob
import threading
from collections import namedtuple
from datetime import datetime, timedelta
from typing import List
from urllib.parse import urlparse, urlunparse
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.utils.logger_config import setup_global_logging
from src.ingestion.discovery.engines.google_serper import GoogleSerperEngine
from src.ingestion.schemas.models import DiscoveredLink

setup_global_logging(log_file_name="discovery.log")
logger = logging.getLogger(__name__)

_ENGINE_REGISTRY = {
    "serper": GoogleSerperEngine,
}


def _hash_dict(raw: dict, fields: list) -> str:
    parts = []
    for f in fields:
        v = raw.get(f)
        if v is not None:
            parts.append(str(v).strip().lower())
    return hashlib.md5(" ".join(parts).encode("utf-8")).hexdigest()


SearchTask = namedtuple("SearchTask", ["query", "query_id", "priority", "target_law_name", "target_law_aliases"])


def _dedup_key(url: str) -> str:
    """
    Dedup key duy nhat cho ca load history va save moi.
    Dung normalized URL string (KHONG hash) de so sanh truc tiep trong seen_keys.
    """
    try:
        parsed = urlparse(str(url))
        path = parsed.path.rstrip("/")
        domain = parsed.netloc.lower().replace("www.", "")

        if path.lower() in ("/index.html", "/index.htm", "/default.aspx"):
            path = ""

        if domain == "congbao.chinhphu.vn":
            path = re.sub(r"\.htm$", ".html", path, flags=re.IGNORECASE)
            path = re.sub(r"(-\d{2,7})/\d+\.html?$", r"\1.html", path, flags=re.IGNORECASE)
            path = re.sub(r"(-\d{2,7})/\d+$", r"\1", path, flags=re.IGNORECASE)

        clean = urlunparse((parsed.scheme, domain, path, "", "", ""))
        return clean
    except Exception:
        return str(url).lower()


class DiscoveryRunner:
    def __init__(self, config_path: str = "src/ingestion/config/discovery.yaml"):
        self.config_path = config_path
        self.config = self._load_config()
        engine_name = self.config.get("discovery_settings", {}).get("engine", "serper")
        if engine_name is None:
            engine_name = "serper"
        engine_key = str(engine_name).strip().lower()
        if not engine_key:
            engine_key = "serper"

        engine_class = _ENGINE_REGISTRY.get(engine_key)
        if engine_class is None:
            available = ", ".join(sorted(_ENGINE_REGISTRY.keys()))
            raise ValueError(
                f"Khong nhan ra discovery engine '{engine_key}'. Ho tro: {available}"
            )

        self.engine = engine_class(self.config)
        logger.info(f" Da chon discovery engine: {engine_key}")

        self.output_dir = self.config["storage"]["output_dir"]
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = self.config["storage"]["filename_format"].replace("{timestamp}", timestamp)
        self.output_file = os.path.join(self.output_dir, filename)

        self.file_lock = threading.Lock()
        self.seen_keys: set[str] = set()

        self._init_deduplication()

        resume_cfg = self.config.get("resume_logic", {})
        if resume_cfg.get("skip_if_processed", False):
            self._load_history_state()

    def _load_history_state(self):
        logger.info(f" Dang kiem tra lich su tai {self.output_dir}...")

        resume_cfg = self.config.get("resume_logic", {})
        refresh_days = resume_cfg.get("refresh_interval_days", 7)
        cutoff = datetime.now() - timedelta(days=refresh_days)

        history_files = glob.glob(os.path.join(self.output_dir, "*.jsonl"))
        current_basename = os.path.basename(self.output_file)
        history_files = [
            f for f in history_files
            if os.path.basename(f) != current_basename
        ]
        scanned_count = len(history_files)
        logger.info(f"  Quet: {scanned_count} file(s) .jsonl trong thu muc.")

        if not history_files:
            logger.info(f"  Ly do = 0: khong co file .jsonl nao trong thu muc.")
        else:
            stale_total = 0
            parse_fail_total = 0
            for fp in history_files:
                try:
                    with jsonlines.open(fp) as rdr:
                        for obj in rdr:
                            ts = obj.get("discovered_at", "")
                            try:
                                d = datetime.fromisoformat(ts)
                            except Exception:
                                parse_fail_total += 1
                                continue
                            if d < cutoff:
                                stale_total += 1
                except Exception:
                    pass
            if scanned_count == 0:
                logger.info(f"  Ly do = 0: khong co file .jsonl nao trong thu muc.")
            elif stale_total > 0 and parse_fail_total == 0:
                logger.info(
                    f"  Ly do = 0: tat ca {stale_total} ban ghi da stale "
                    f"(> {refresh_days} ngay), {scanned_count} file(s) da quet."
                )
            elif parse_fail_total > 0:
                logger.info(
                    f"  Ly do = 0: {parse_fail_total} ban ghi khong parse duoc discovered_at, "
                    f"{scanned_count} file(s) da quet."
                )
            else:
                logger.info(f"  Ly do = 0: khong ro, {scanned_count} file(s) da quet.")

        count = 0
        for file_path in history_files:
            try:
                with jsonlines.open(file_path) as reader:
                    for obj in reader:
                        raw_ts = obj.get("discovered_at", "")
                        try:
                            rec_date = datetime.fromisoformat(raw_ts)
                        except Exception:
                            rec_date = datetime.min

                        if rec_date < cutoff:
                            continue

                        key = self._dedup_key(obj.get("url", ""))
                        self.seen_keys.add(key)
                        count += 1
            except Exception as e:
                logger.warning(f" Khong the doc file lich su {file_path}: {e}")

        logger.info(
            f" Da nap {count} ban ghi cu (trong vong {refresh_days} ngay). "
            f"Discovery se tu dong bo qua chung."
        )

    def _load_config(self) -> dict:
        with open(self.config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _init_deduplication(self):
        dedup_cfg = self.config.get("storage", {}).get("deduplication", {})
        method = dedup_cfg.get("method", "content_hash")
        scope = dedup_cfg.get("scope", "global")
        fields = dedup_cfg.get("fields", ["title"])

        logger.info(
            f" Deduplication config: method={method}, scope={scope}, fields={fields}"
        )

        if method not in ("content_hash", "url_only", "title_only"):
            logger.warning(
                f" Unknown deduplication method '{method}', falling back to 'content_hash'."
            )
            method = "content_hash"
        self.dedup_method = method
        self.dedup_scope = scope
        self.dedup_fields = fields

    def _dedup_key(self, url: str) -> str:
        """
        Instance method wrapper: gọi module-level _dedup_key để test có thể import trực tiếp.
        """
        return _dedup_key(url)

    def _save_results(self, results: List[DiscoveredLink]):
        """
        Khuz trung + ghi file.
        Dung _dedup_key() nhu _load_history_state de dam bao idempotent.
        """
        pending: dict[str, DiscoveredLink] = {}

        for link in results:
            key = self._dedup_key(str(link.url))
            if key not in pending or len(link.snippet or "") > len(pending[key].snippet or ""):
                pending[key] = link

        valid_records = []
        for key, link in pending.items():
            if key not in self.seen_keys:
                self.seen_keys.add(key)
                valid_records.append(link.model_dump(mode="json"))

        if valid_records:
            with self.file_lock:
                with jsonlines.open(self.output_file, mode="a") as writer:
                    writer.write_all(valid_records)
                logger.info(f"Da luu them {len(valid_records)} link moi vao {self.output_file}")

    def run(self):
        logger.info(" BAT DAU PIPELINE DISCOVERY DA LUONG ...")

        target_laws = self.config.get("target_laws", [])
        query_templates = self.config.get("query_templates", [])

        tasks = []
        for law in target_laws:
            law_name = law["name"]
            keywords = law.get("keywords", [])

            for template in query_templates:
                template_id = template["id"]
                priority = template["priority"]
                query_text = template["template"].replace("{law_name}", law_name)
                tasks.append(SearchTask(
                    query=query_text,
                    query_id=template_id,
                    priority=priority,
                    target_law_name=law_name,
                    target_law_aliases=law.get("aliases", []),
                ))

            for kw in keywords:
                tasks.append(SearchTask(
                    query=kw,
                    query_id="keyword",
                    priority=2,
                    target_law_name=law_name,
                    target_law_aliases=law.get("aliases", []),
                ))

        tasks.sort(key=lambda t: t.priority)
        logger.info(f" Da xay dung {len(tasks)} search tasks (da sap xep theo priority).")

        total_raw = 0
        total_saved = 0
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_task = {
                executor.submit(
                    self.engine.search,
                    task.query,
                    task.query_id,
                    task.target_law_name,
                    task.target_law_aliases,
                ): task
                for task in tasks
            }

            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    results = future.result()
                    if results:
                        total_raw += len(results)
                        prev_count = len(self.seen_keys)
                        self._save_results(results)
                        saved = len(self.seen_keys) - prev_count
                        total_saved += saved
                except Exception as e:
                    logger.error(
                        f" Loi khi xu ly query '{task.query}' (query_id={task.query_id}): {e}",
                        exc_info=True,
                    )

        logger.info(f" PIPELINE HOAN TAT. Tong cong nat duoc {total_raw} raw results, {total_saved} links moi thuc luu.")


if __name__ == "__main__":
    runner = DiscoveryRunner()
    runner.run()
