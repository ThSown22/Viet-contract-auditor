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
        self.scraping_config = self._load_yaml(scraping_config_path)
        self.sources_config = self._load_yaml(sources_config_path)
        self.discovery_config = self._load_yaml("src/ingestion/config/discovery.yaml")

        self.state_manager = StateManager(discovery_file)
        self.source_priority = self.scraping_config.get("scraping_settings", {}).get("source_priority", [])
        self.law_match_terms = self._build_law_match_terms()
        self.output_dir = Path(self.scraping_config["output"]["base_dir"])
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.scrapers = self._init_scrapers()
        self._last_unmatched_count = 0
        logger.info("ScrapingCoordinator initialized")

    def run(self, selected_laws: Optional[List[str]] = None, force: bool = False) -> dict:
        logger.info("=" * 80)
        logger.info("START PHASE 2 SCRAPING PIPELINE")
        logger.info("=" * 80)

        grouped = self._prepare_groups(selected_laws, force)
        if not grouped:
            return {"success_count": 0, "fail_count": 0, "group_count": 0}

        success_count = 0
        fail_count = 0
        logger.info("Grouped into %s law buckets", len(grouped))

        for law_name, url_group in grouped.items():
            logger.info("-" * 80)
            logger.info("Processing law: %s", law_name)
            logger.info("URLs in group: %s", len(url_group["urls"]))

            priority_chain = self._build_priority_chain(law_name, url_group["urls"])
            for index, url_info in enumerate(priority_chain, start=1):
                logger.info("Priority %s [%s] %s", index, url_info.get("type", "UNKNOWN"), url_info["url"])

            result = self._scrape_with_fallback(law_name, priority_chain)
            if not (result.success and result.content):
                logger.error("Failed law %s: %s", law_name, result.error_message)
                fail_count += 1
                continue

            output_path = self._save_content(law_name, result.content)
            logger.info("Saved content to %s", output_path)

            if self.scraping_config.get("state_management", {}).get("update_discovery_jsonl", True):
                updated = self.state_manager.mark_processed(
                    [item["url"] for item in url_group["urls"]],
                    backup=self._backup_enabled(),
                )
                logger.info("Marked %s URLs as processed for %s", updated, law_name)

            success_count += 1

        if self._last_unmatched_count:
            logger.warning(
                "Left %s unmatched discovery URLs unprocessed for manual review",
                self._last_unmatched_count,
            )

        logger.info("=" * 80)
        logger.info("SCRAPING SUMMARY success=%s fail=%s total=%s", success_count, fail_count, len(grouped))
        logger.info("=" * 80)
        return {"success_count": success_count, "fail_count": fail_count, "group_count": len(grouped)}

    def _prepare_groups(self, selected_laws: Optional[List[str]], force: bool) -> Dict[str, dict]:
        records = self.state_manager.get_all_urls() if force else self.state_manager.get_unprocessed_urls()
        if not records:
            logger.warning("No discovery URLs found for this run")
            return {}

        grouped = self._group_by_canonical_law(records)
        if selected_laws:
            allowed = {normalize_text(item) for item in selected_laws}
            grouped = {law_name: data for law_name, data in grouped.items() if normalize_text(law_name) in allowed}

        if not grouped:
            logger.warning("No matching law buckets found")
            return {}

        if force:
            updated = self.state_manager.reset_processed(self._collect_urls(grouped), backup=self._backup_enabled())
            logger.info("Reset %s URLs to rerun Phase 2", updated)

        return grouped

    def _init_scrapers(self) -> Dict[str, object]:
        default_config = self.sources_config.get("default", {})
        validation_config = self.scraping_config.get("content_validation", {})
        source_map = self.sources_config["sources"]
        return {
            "thuvienphapluat.vn": TVPLScraper(
                self._merge_source_config(default_config, source_map["thuvienphapluat.vn"]),
                validation_config=validation_config,
            ),
            "congbao.chinhphu.vn": CongbaoScraper(
                self._merge_source_config(default_config, source_map["congbao.chinhphu.vn"]),
                validation_config=validation_config,
            ),
        }

    def _group_by_canonical_law(self, records: List[dict]) -> Dict[str, dict]:
        grouped = defaultdict(lambda: {"urls": []})
        self._last_unmatched_count = 0
        law_id_to_name: Dict[str, str] = {}
        unresolved: List[dict] = []

        for record in records:
            matched_law = self._infer_law_name(record)
            if not matched_law:
                unresolved.append(record)
                continue

            grouped[matched_law]["urls"].append(self._to_url_info(record))
            law_id = self._normalize_law_id(record.get("law_id"))
            if law_id:
                law_id_to_name[law_id] = matched_law

        for record in unresolved:
            law_id = self._normalize_law_id(record.get("law_id"))
            matched_law = law_id_to_name.get(law_id)
            if matched_law:
                grouped[matched_law]["urls"].append(self._to_url_info(record))
                continue

            title = record.get("title", "")
            logger.warning("Could not map canonical law for title: %s", title[:120])
            self._last_unmatched_count += 1

        return dict(grouped)

    def _build_priority_chain(self, law_name: str, urls: List[dict]) -> List[dict]:
        canonical_config = self.scraping_config.get("canonical_laws", {}).get(law_name)
        priority_rules = canonical_config.get("priority_rules", []) if canonical_config else []
        if not priority_rules:
            logger.warning("No priority rules for %s, using heuristic fallback", law_name)
            return self._auto_prioritize_urls(law_name, urls)

        chain: List[dict] = []
        seen_urls = set()
        for rule in priority_rules:
            candidates = self._find_matching_urls(urls, rule)
            if not candidates:
                logger.debug("No candidates for %s rule on %s", rule.get("type", "UNKNOWN"), law_name)
                continue

            logger.info("Found %s candidates for %s rule on %s", len(candidates), rule.get("type", "UNKNOWN"), law_name)
            ordered = self._sort_rule_candidates(candidates, rule)
            queued = [
                {**candidate, "type": rule["type"], "reason": rule.get("reason", "")}
                for candidate in ordered
                if candidate["url"] not in seen_urls
            ]
            chain.extend(queued)
            seen_urls.update(item["url"] for item in queued)

            if queued:
                logger.info("Queued %s candidates for %s rule on %s", len(queued), rule.get("type", "UNKNOWN"), law_name)

        return chain or self._auto_prioritize_urls(law_name, urls)

    def _infer_law_name(self, record: dict) -> Optional[str]:
        haystack = self._normalize_match_text(
            " ".join(
                filter(
                    None,
                    [
                        record.get("title", ""),
                        record.get("snippet", ""),
                        record.get("search_query", ""),
                        record.get("url", ""),
                    ],
                )
            )
        )
        if not haystack:
            return None

        best_match: Optional[str] = None
        best_score = (0, 0)
        for law_name, terms in self.law_match_terms.items():
            score = self._score_law_match(haystack, terms)
            if score > best_score:
                best_match = law_name
                best_score = score

        return best_match if best_score > (0, 0) else None

    def _build_law_match_terms(self) -> Dict[str, set[str]]:
        terms_by_law: Dict[str, set[str]] = {}
        for law in self.discovery_config.get("target_laws", []):
            candidates = [law.get("name", ""), *law.get("aliases", []), *law.get("keywords", [])]
            terms = {self._normalize_match_text(candidate) for candidate in candidates}
            terms_by_law[law["name"]] = {term for term in terms if len(term) >= 6}
        return terms_by_law

    @staticmethod
    def _score_law_match(haystack: str, terms: set[str]) -> tuple[int, int]:
        matched_lengths = [len(term) for term in terms if term in haystack]
        if not matched_lengths:
            return (0, 0)
        return (len(matched_lengths), max(matched_lengths))

    def _find_matching_urls(self, urls: List[dict], rule: dict) -> List[dict]:
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
            if exclude_pattern and re.search(exclude_pattern, f"{law_id} {title}", re.IGNORECASE):
                continue
            matches.append(dict(url_info))
        return matches

    def _sort_rule_candidates(self, candidates: List[dict], rule: dict) -> List[dict]:
        if rule.get("sort_by"):
            return self._sort_candidates(candidates, rule["sort_by"], rule.get("sort_order", "desc"))
        return self._sort_by_source_priority(candidates)

    def _sort_candidates(self, candidates: List[dict], sort_by: str, sort_order: str = "desc") -> List[dict]:
        def normalized_value(url_info: dict) -> str:
            value = str(url_info.get(sort_by) or "").replace("??", "00").replace("-", "")
            return (value or "00000000").ljust(8, "0")

        reverse = sort_order == "desc"
        ordered = sorted(
            candidates,
            key=lambda item: (normalized_value(item), self._source_rank(item.get("source_domain", ""))),
            reverse=reverse,
        )
        logger.debug("Sorted %s candidates by %s (%s)", len(ordered), sort_by, sort_order)
        return ordered

    def _auto_prioritize_urls(self, law_name: str, urls: List[dict]) -> List[dict]:
        logger.info("Using heuristic auto-prioritization for %s", law_name)

        def priority_score(url_info: dict) -> tuple[int, str, int]:
            law_id = (url_info.get("law_id") or "").upper()
            effective_date = str(url_info.get("effective_date") or "1900-01-01").replace("-", "").replace("?", "0")
            return (1 if "VBHN" in law_id else 0, effective_date.ljust(8, "0"), self._source_rank(url_info.get("source_domain", "")))

        prioritized = []
        for url_info in sorted(urls, key=priority_score, reverse=True):
            title = self._normalize_match_text(url_info.get("title") or "")
            law_id = (url_info.get("law_id") or "").upper()
            url_type = "VBHN" if "VBHN" in law_id else "AMENDMENT" if "sua doi" in title else "ORIGINAL"
            prioritized.append({**url_info, "type": url_type, "reason": "Heuristic fallback"})

        if prioritized:
            logger.info("Heuristic picked %s for %s", prioritized[0].get("law_id", "N/A"), law_name)
        return prioritized

    def _scrape_with_fallback(self, law_name: str, priority_chain: List[dict]) -> ScrapingResult:
        best_result: Optional[ScrapingResult] = None
        best_score: Optional[float] = None

        for index, url_info in enumerate(priority_chain):
            url = url_info["url"]
            scraper = self.scrapers.get(url_info.get("source_domain"))
            logger.info("Attempt %s/%s: %s", index + 1, len(priority_chain), url)
            if not scraper:
                logger.error("No scraper registered for %s", url_info.get("source_domain"))
                continue

            result = scraper.scrape(url, law_name, url_info.get("law_id"))
            result.fallback_level = index
            if not (result.success and result.content):
                logger.warning("Attempt failed for %s: %s", url, result.error_message)
                continue

            content = result.content
            if not content.effective_date and url_info.get("effective_date"):
                content.effective_date = url_info["effective_date"]
            if content.law_id == "unknown" and url_info.get("law_id"):
                content.law_id = url_info["law_id"]

            score = self._score_content_quality(content)
            logger.info(
                "Success at fallback level %s with quality score %.2f | source=%s | articles=%s",
                index,
                score,
                content.source_domain,
                content.article_count,
            )
            if best_score is None or score > best_score:
                best_result = result
                best_score = score

        if best_result and best_result.content:
            logger.info(
                "Selected best candidate for %s: %s | source=%s | score=%.2f",
                law_name,
                best_result.content.source_url,
                best_result.content.source_domain,
                best_score or 0.0,
            )
            return best_result

        attempted_url = priority_chain[0]["url"] if priority_chain else ""
        return ScrapingResult(success=False, error_message=f"All {len(priority_chain)} URLs failed", attempted_url=attempted_url)

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

        settings = self.scraping_config.get("scraping_settings", {})
        merged["timeout"] = settings.get("timeout_seconds", merged.get("timeout", 30))
        merged["retry_limit"] = settings.get("retry_attempts", merged.get("retry_limit", 3))
        merged["delay_between_retries_sec"] = settings.get(
            "delay_between_retries_sec",
            merged.get("delay_between_retries_sec", 2),
        )
        merged.setdefault("headers", {})
        merged["headers"]["User-Agent"] = settings.get("user_agent", merged["headers"].get("User-Agent", "Mozilla/5.0"))
        return merged

    def _sort_by_source_priority(self, items: List[dict]) -> List[dict]:
        return sorted(items, key=lambda item: (self._source_sort_index(item.get("source_domain", "")), item.get("url", "")))

    def _source_rank(self, domain: str) -> int:
        return len(self.source_priority) - self._source_sort_index(domain)

    def _source_sort_index(self, domain: str) -> int:
        try:
            return self.source_priority.index(domain)
        except ValueError:
            return len(self.source_priority)

    def _score_content_quality(self, content: ScrapedContent) -> float:
        article_numbers = self._extract_article_numbers(content)
        transitions = list(zip(article_numbers, article_numbers[1:]))
        monotonic_steps = sum(1 for left, right in transitions if right == left + 1)
        monotonic_breaks = sum(1 for left, right in transitions if right <= left)
        gap_breaks = sum(1 for left, right in transitions if right > left + 1)
        coverage_ratio = len(content.structured_text or "") / max(len(content.clean_text or ""), 1)
        short_orphans = min(self._count_short_orphans(content.structured_text or content.clean_text), 20)
        definition_anomalies = self._count_definition_anomalies(content)

        score = (content.article_count * 50) + (len(content.clean_text or "") / 1000)
        score += 100 if content.has_structure else 0
        score += min(coverage_ratio, 1.0) * 100
        score += ((monotonic_steps / len(transitions)) * 500) if transitions else 0
        score += 25 if article_numbers and article_numbers[0] == 1 else 0
        score += self._source_rank(content.source_domain)
        score -= (monotonic_breaks * 200) + (gap_breaks * 50) + (short_orphans * 10) + (definition_anomalies * 700)
        if "sua doi" in self._normalize_match_text(content.title) and content.article_count < 20:
            score -= 1000
        return score

    @staticmethod
    def _extract_article_numbers(content: ScrapedContent) -> List[int]:
        numbers: List[int] = []
        for article in content.articles:
            if match := re.search(r"__(\d+)$", article.article_id):
                numbers.append(int(match.group(1)))
        return numbers

    @staticmethod
    def _count_short_orphans(text: str) -> int:
        count = 0
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue

            normalized = normalize_text(stripped)
            if re.match(r"^(dieu|chuong|muc|phan thu|\d+[\.:]|[a-zd]\))", normalized):
                continue

            if len(stripped.split()) <= 2 and stripped[:1].isalpha():
                count += 1

        return count

    @staticmethod
    def _count_definition_anomalies(content: ScrapedContent) -> int:
        anomalies = 0
        for article in content.articles:
            title = normalize_text(article.title)
            body = article.text or ""
            body_normalized = normalize_text(body)
            if "giai thich tu ngu" not in title and "duoc hieu nhu sau" not in body_normalized:
                continue

            lines = [line.strip() for line in body.splitlines() if line.strip()]
            numbered = [(index, int(match.group(1))) for index, line in enumerate(lines) if (match := re.match(r"^(\d+)\.\s+", line))]
            if not numbered:
                continue

            first_index, previous_number = numbered[0]
            if previous_number > 1:
                anomalies += previous_number - 1 + int(first_index > 0)

            for _, current_number in numbered[1:]:
                if current_number != previous_number + 1:
                    anomalies += abs(current_number - previous_number - 1) or 1
                previous_number = current_number

            for line in lines[first_index:]:
                normalized_line = normalize_text(line)
                if re.match(r"^(\d+)\.\s+", line) or "duoc hieu nhu sau" in normalized_line:
                    continue
                if len(line.split()) <= 12 and line[:1].isalpha() and line[:1].isupper():
                    anomalies += 1
                    break

        return anomalies

    def _backup_enabled(self) -> bool:
        return self.scraping_config.get("state_management", {}).get("backup_before_update", True)

    @staticmethod
    def _collect_urls(grouped: Dict[str, dict]) -> List[str]:
        return [item["url"] for data in grouped.values() for item in data["urls"]]

    @staticmethod
    def _to_url_info(record: dict) -> dict:
        return {
            "url": record["url"],
            "law_id": record.get("law_id"),
            "title": record.get("title"),
            "source_domain": record.get("source_domain"),
            "effective_date": record.get("effective_date"),
        }

    @staticmethod
    def _normalize_law_id(value: Optional[str]) -> str:
        return re.sub(r"\s+", "", str(value or "")).upper()

    @staticmethod
    def _normalize_match_text(value: str) -> str:
        repaired = ScrapingCoordinator._repair_mojibake(value)
        normalized = normalize_text(repaired)
        return re.sub(r"[^a-z0-9]+", " ", normalized).strip()

    @staticmethod
    def _repair_mojibake(value: str) -> str:
        text = str(value or "")
        if not text or not any(marker in text for marker in ("Ã", "Ä", "áº", "Ã‚")):
            return text

        for encoding in ("cp1252", "latin1"):
            try:
                repaired = text.encode(encoding).decode("utf-8")
            except (UnicodeEncodeError, UnicodeDecodeError):
                continue
            if repaired.count("ï¿½") <= text.count("ï¿½"):
                return repaired

        return text

    @staticmethod
    def _load_yaml(path: str) -> dict:
        with open(path, "r", encoding="utf-8") as handle:
            return yaml.safe_load(handle)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Phase 2 scraping pipeline.")
    parser.add_argument("discovery_file", help="Path to discovery JSONL file.")
    parser.add_argument("--law", dest="laws", action="append", help="Limit run to specific law name. Repeatable.")
    parser.add_argument("--force", action="store_true", help="Rerun selected laws even if discovery marks them processed.")
    return parser


def main() -> int:
    parser = build_argument_parser()
    args = parser.parse_args()
    coordinator = ScrapingCoordinator(args.discovery_file)
    coordinator.run(selected_laws=args.laws, force=args.force)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
