import argparse
import json
import logging
import re
from collections import defaultdict
from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Optional

import yaml

from src.ingestion.schemas.models import ScrapedContent, ScrapingResult
from src.ingestion.scraping.normalizers.content_cleaner import normalize_text
from src.ingestion.scraping.scrapers.congbao_scraper import CongbaoScraper
from src.ingestion.scraping.scrapers.tvpl_scraper import TVPLScraper
from src.ingestion.scraping.state_manager import StateManager
from src.utils.logger_config import setup_global_logging

setup_global_logging(log_file_name="scraping.log")
logger = logging.getLogger(__name__)


class ScrapingCoordinator:
    """Main orchestrator for Phase 2 scraping."""

    def __init__(
        self,
        discovery_file: str,
        scraping_config_path: str = "src/ingestion/config/scraping.yaml",
        sources_config_path: str = "src/ingestion/config/sources.yaml",
    ):
        self.discovery_file = discovery_file

        with open(scraping_config_path, "r", encoding="utf-8") as handle:
            self.scraping_config = yaml.safe_load(handle)
        with open(sources_config_path, "r", encoding="utf-8") as handle:
            self.sources_config = yaml.safe_load(handle)
        with open("src/ingestion/config/discovery.yaml", "r", encoding="utf-8") as handle:
            self.discovery_config = yaml.safe_load(handle)

        self.state_manager = StateManager(discovery_file)
        self.scrapers = self._init_scrapers()
        self.output_dir = Path(self.scraping_config["output"]["base_dir"])
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.source_priority = self.scraping_config.get("scraping_settings", {}).get("source_priority", [])
        self._last_unmatched_records: List[dict] = []
        logger.info("ScrapingCoordinator initialized")

    def run(self, selected_laws: Optional[List[str]] = None) -> dict:
        """Run the scraping pipeline and return a summary dict."""

        logger.info("=" * 80)
        logger.info("START PHASE 2 SCRAPING PIPELINE")
        logger.info("=" * 80)

        normalized_filter = {normalize_text(item) for item in selected_laws or []}
        unprocessed = self.state_manager.get_unprocessed_urls()
        if not unprocessed:
            logger.warning("No unprocessed URLs found")
            return {"success_count": 0, "fail_count": 0, "group_count": 0}

        grouped = self._group_by_canonical_law(unprocessed)
        if normalized_filter:
            grouped = {
                law_name: data
                for law_name, data in grouped.items()
                if normalize_text(law_name) in normalized_filter
            }

        logger.info("Grouped into %s law buckets", len(grouped))
        success_count = 0
        fail_count = 0

        for law_name, url_group in grouped.items():
            logger.info("-" * 80)
            logger.info("Processing law: %s", law_name)
            logger.info("URLs in group: %s", len(url_group["urls"]))

            priority_chain = self._build_priority_chain(law_name, url_group)
            for index, url_info in enumerate(priority_chain, start=1):
                logger.info("Priority %s [%s] %s", index, url_info.get("type", "UNKNOWN"), url_info["url"])

            result = self._scrape_with_fallback(law_name, priority_chain)
            if result.success and result.content:
                output_path = self._save_content(law_name, result.content)
                logger.info("Saved content to %s", output_path)
                if self.scraping_config.get("state_management", {}).get("update_discovery_jsonl", True):
                    all_urls = [item["url"] for item in url_group["urls"]]
                    updated = self.state_manager.mark_processed(
                        all_urls,
                        backup=self.scraping_config.get("state_management", {}).get("backup_before_update", True),
                    )
                    logger.info("Marked %s URLs as processed for %s", updated, law_name)
                success_count += 1
            else:
                logger.error("Failed law %s: %s", law_name, result.error_message)
                fail_count += 1

        if not normalized_filter and self._last_unmatched_records:
            skipped_urls = [record["url"] for record in self._last_unmatched_records]
            updated = self.state_manager.mark_processed(
                skipped_urls,
                backup=self.scraping_config.get("state_management", {}).get("backup_before_update", True),
            )
            logger.info("Marked %s unmatched URLs as processed after review", updated)

        logger.info("=" * 80)
        logger.info("SCRAPING SUMMARY success=%s fail=%s total=%s", success_count, fail_count, len(grouped))
        logger.info("=" * 80)
        return {"success_count": success_count, "fail_count": fail_count, "group_count": len(grouped)}

    def _init_scrapers(self) -> Dict[str, object]:
        default_config = self.sources_config.get("default", {})
        validation_config = self.scraping_config.get("content_validation", {})
        scrapers: Dict[str, object] = {}

        tvpl_config = self._merge_source_config(default_config, self.sources_config["sources"]["thuvienphapluat.vn"])
        congbao_config = self._merge_source_config(default_config, self.sources_config["sources"]["congbao.chinhphu.vn"])

        scrapers["thuvienphapluat.vn"] = TVPLScraper(tvpl_config, validation_config=validation_config)
        scrapers["congbao.chinhphu.vn"] = CongbaoScraper(congbao_config, validation_config=validation_config)
        logger.info("Initialized %s scrapers", len(scrapers))
        return scrapers

    def _group_by_canonical_law(self, records: List[dict]) -> Dict[str, dict]:
        grouped = defaultdict(lambda: {"urls": []})
        self._last_unmatched_records = []
        target_laws = self.discovery_config.get("target_laws", [])

        for record in records:
            title = record.get("title", "")
            normalized_title = normalize_text(title)
            matched_law = None

            for law in target_laws:
                names = [law["name"]] + law.get("aliases", [])
                if any(normalize_text(name) in normalized_title for name in names):
                    matched_law = law["name"]
                    break

            if not matched_law:
                logger.warning("Could not map canonical law for title: %s", title[:120])
                self._last_unmatched_records.append(record)
                continue

            grouped[matched_law]["urls"].append(
                {
                    "url": record["url"],
                    "law_id": record.get("law_id"),
                    "title": record.get("title"),
                    "source_domain": record.get("source_domain"),
                    "effective_date": record.get("effective_date"),
                }
            )

        return dict(grouped)

    def _build_priority_chain(self, law_name: str, url_group: dict) -> List[dict]:
        """
        Build priority chain dynamically with regex matching and candidate sorting.

        Workflow:
        1. Evaluate each configured priority rule in order.
        2. Find all URLs matching the rule patterns.
        3. Sort candidates when the rule requests it.
        4. Pick the best unmatched candidate as the rule winner.

        Returns:
            Ordered list of primary -> fallback candidates.
        """
        canonical_config = self.scraping_config.get("canonical_laws", {}).get(law_name)
        urls = list(url_group.get("urls", []))
        if not canonical_config:
            logger.warning("No config for %s, using heuristic fallback", law_name)
            return self._auto_prioritize_urls(law_name, urls)

        priority_rules = canonical_config.get("priority_rules", [])
        if not priority_rules:
            logger.warning("No priority_rules for %s, using heuristic fallback", law_name)
            return self._auto_prioritize_urls(law_name, urls)

        chain: List[dict] = []
        seen_urls = set()

        for rule in priority_rules:
            candidates = self._find_matching_urls(urls, rule)
            if not candidates:
                logger.debug("No candidates for %s rule on %s", rule.get("type", "UNKNOWN"), law_name)
                continue

            logger.info("Found %s candidates for %s rule on %s", len(candidates), rule.get("type", "UNKNOWN"), law_name)
            if rule.get("sort_by"):
                candidates = self._sort_candidates(candidates, rule)
            else:
                candidates = self._sort_by_source_priority(candidates)

            winner = next((item for item in candidates if item["url"] not in seen_urls), None)
            if winner and winner.get("law_id"):
                same_law_id = [
                    item for item in candidates
                    if item.get("law_id") == winner.get("law_id") and item["url"] not in seen_urls
                ]
                if len(same_law_id) > 1:
                    winner = self._sort_by_source_priority(same_law_id)[0]
            if not winner:
                continue

            selected = {
                **winner,
                "type": rule["type"],
                "reason": rule.get("reason", ""),
            }
            chain.append(selected)
            seen_urls.add(selected["url"])
            logger.info(
                "Winner for %s rule on %s: %s | %s",
                rule.get("type", "UNKNOWN"),
                law_name,
                selected.get("law_id", "N/A"),
                (selected.get("title") or "")[:80],
            )

        if not chain:
            logger.warning("No URLs matched configured rules for %s, using heuristic fallback", law_name)
            return self._auto_prioritize_urls(law_name, urls)

        return chain

    def _find_matching_urls(self, urls: List[dict], rule: dict) -> List[dict]:
        """
        Return URLs matching a priority rule.

        Required checks:
        - `law_id_pattern` must match `law_id`
        - `title_pattern` must match `title`
        Optional check:
        - `exclude_pattern` must not match `title` or `law_id`
        """
        law_id_pattern = rule.get("law_id_pattern")
        title_pattern = rule.get("title_pattern")
        exclude_pattern = rule.get("exclude_pattern")

        if not law_id_pattern or not title_pattern:
            logger.error("Rule missing required patterns: %s", rule)
            return []

        matches: List[dict] = []
        for url_info in urls:
            law_id = url_info.get("law_id") or ""
            title = url_info.get("title") or ""

            if not re.search(law_id_pattern, law_id, re.IGNORECASE):
                continue
            if not re.search(title_pattern, title, re.IGNORECASE):
                continue

            exclude_haystack = " ".join([law_id, title])
            if exclude_pattern and re.search(exclude_pattern, exclude_haystack, re.IGNORECASE):
                logger.debug("Excluded candidate for %s rule: %s", rule.get("type", "UNKNOWN"), title[:80])
                continue

            matches.append({**url_info})

        return matches

    def _sort_candidates(self, candidates: List[dict], rule: dict) -> List[dict]:
        """
        Sort candidates by a configured field, usually `effective_date`.

        Missing or coarse dates are normalized so newer complete dates still win.
        Source priority remains the tiebreaker.
        """
        sort_by = rule.get("sort_by", "effective_date")
        sort_order = rule.get("sort_order", "desc")

        def _normalized_value(url_info: dict) -> str:
            raw_value = url_info.get(sort_by)
            if raw_value is None or raw_value == "":
                return "00000000"

            text_value = str(raw_value).replace("??", "00").replace("-", "")
            return text_value.ljust(8, "0")

        def _source_rank(url_info: dict) -> int:
            domain = url_info.get("source_domain", "")
            try:
                return len(self.source_priority) - self.source_priority.index(domain)
            except ValueError:
                return 0

        reverse = sort_order == "desc"
        sorted_candidates = sorted(
            candidates,
            key=lambda item: (_normalized_value(item), _source_rank(item)),
            reverse=reverse,
        )

        logger.debug("Sorted %s candidates by %s (%s)", len(sorted_candidates), sort_by, sort_order)
        return sorted_candidates

    def _auto_prioritize_urls(self, law_name: str, urls: List[dict]) -> List[dict]:
        """
        Heuristic fallback when config is missing or no rule matches.

        Priority order:
        1. VBHN candidates first
        2. Newer effective_date first
        3. Configured source priority as tiebreaker
        """
        logger.info("Using heuristic auto-prioritization for %s", law_name)

        def _source_score(source_domain: str) -> int:
            try:
                return len(self.source_priority) - self.source_priority.index(source_domain)
            except ValueError:
                return 0

        def _priority_score(url_info: dict) -> tuple[int, str, int]:
            law_id = (url_info.get("law_id") or "").upper()
            effective_date = str(url_info.get("effective_date") or "1900-01-01").replace("-", "").replace("?", "0")
            return (
                1 if "VBHN" in law_id else 0,
                effective_date.ljust(8, "0"),
                _source_score(url_info.get("source_domain") or ""),
            )

        sorted_urls = sorted(urls, key=_priority_score, reverse=True)
        prioritized: List[dict] = []
        for url_info in sorted_urls:
            title = (url_info.get("title") or "").lower()
            law_id = (url_info.get("law_id") or "").upper()
            if "VBHN" in law_id:
                url_type = "VBHN"
            elif "sửa đổi" in title:
                url_type = "AMENDMENT"
            else:
                url_type = "ORIGINAL"
            prioritized.append({**url_info, "type": url_type, "reason": "Heuristic fallback"})

        if prioritized:
            logger.info("Heuristic picked %s for %s", prioritized[0].get("law_id", "N/A"), law_name)
        return prioritized

    def _scrape_with_fallback(self, law_name: str, priority_chain: List[dict]) -> ScrapingResult:
        for index, url_info in enumerate(priority_chain):
            url = url_info["url"]
            source_domain = url_info.get("source_domain")
            law_id = url_info.get("law_id")
            scraper = self.scrapers.get(source_domain)
            logger.info("Attempt %s/%s: %s", index + 1, len(priority_chain), url)
            if not scraper:
                logger.error("No scraper registered for %s", source_domain)
                continue

            result = scraper.scrape(url, law_name, law_id)
            result.fallback_level = index
            if result.success and result.content:
                if not result.content.effective_date and url_info.get("effective_date"):
                    result.content.effective_date = url_info["effective_date"]
                if result.content.law_id == "unknown" and law_id:
                    result.content.law_id = law_id
                logger.info("Success at fallback level %s", index)
                return result

            logger.warning("Attempt failed for %s: %s", url, result.error_message)

        attempted_url = priority_chain[0]["url"] if priority_chain else ""
        return ScrapingResult(
            success=False,
            error_message=f"All {len(priority_chain)} URLs failed",
            attempted_url=attempted_url,
        )

    def _save_content(self, law_name: str, content: ScrapedContent) -> Path:
        canonical_config = self.scraping_config.get("canonical_laws", {}).get(law_name, {})
        filename = canonical_config.get("output_filename", f"{normalize_text(law_name).replace(' ', '_')}.json")
        output_path = self.output_dir / filename
        payload = content.model_dump(mode="json")
        if self.scraping_config.get("output", {}).get("exclude_raw_html", False):
            payload.pop("raw_html", None)
        with open(output_path, "w", encoding=self.scraping_config.get("output", {}).get("encoding", "utf-8")) as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        return output_path

    def _merge_source_config(self, default_config: dict, source_config: dict) -> dict:
        merged = deepcopy(default_config)
        for key, value in source_config.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key].update(value)
            else:
                merged[key] = value

        scraping_settings = self.scraping_config.get("scraping_settings", {})
        merged["timeout"] = scraping_settings.get("timeout_seconds", merged.get("timeout", 30))
        merged["retry_limit"] = scraping_settings.get("retry_attempts", merged.get("retry_limit", 3))
        merged["delay_between_retries_sec"] = scraping_settings.get(
            "delay_between_retries_sec", merged.get("delay_between_retries_sec", 2)
        )
        merged.setdefault("headers", {})
        merged["headers"]["User-Agent"] = scraping_settings.get(
            "user_agent", merged["headers"].get("User-Agent", "Mozilla/5.0")
        )
        return merged

    def _sort_by_source_priority(self, items: List[dict]) -> List[dict]:
        def sort_key(item: dict) -> tuple[int, str]:
            domain = item.get("source_domain", "")
            try:
                priority_index = self.source_priority.index(domain)
            except ValueError:
                priority_index = len(self.source_priority)
            return (priority_index, item.get("url", ""))

        return sorted(items, key=sort_key)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Phase 2 scraping pipeline.")
    parser.add_argument("discovery_file", help="Path to discovery JSONL file.")
    parser.add_argument("--law", dest="laws", action="append", help="Limit run to specific law name. Repeatable.")
    return parser


def main() -> int:
    parser = build_argument_parser()
    args = parser.parse_args()
    coordinator = ScrapingCoordinator(args.discovery_file)
    coordinator.run(selected_laws=args.laws)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
