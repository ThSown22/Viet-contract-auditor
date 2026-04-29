import logging
import os
import re
import threading
import time
import unicodedata
from datetime import datetime
from typing import List, Optional
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv

from src.ingestion.schemas.models import DiscoveredLink

load_dotenv()

logger = logging.getLogger(__name__)

# Cover VBHN plus existing QH/ND and other government document formats.
LAW_ID_PATTERN = re.compile(
    r"(?:so\s*)?("
    r"\d{1,4}/VBHN-[A-Z]{2,6}"
    r"|"
    r"\d{1,3}/\d{4}/QH\d{1,2}"
    r"|"
    r"\d{1,3}/\d{4}/(?:ND|NĐ)-CP"
    r"|"
    r"\d{1,3}/\d{4}/[A-ZĐ]{2,4}(?:-[A-ZĐ]{2,4})?"
    r")",
    re.IGNORECASE,
)

MAX_RATE_LIMIT_RETRIES = 2

# Config aliases -> canonical domains.
SOURCE_ALIAS_MAP = {
    "tvpl": "thuvienphapluat.vn",
    "congbao": "congbao.chinhphu.vn",
}

# Patterns used to filter out cross-law noise.
CROSS_PATTERNS = {
    "bo luat dan su": [
        r"\b(thi\s*hanh\s*an\s*dan\s*su)\b",
        r"\b(to\s*tung\s*dan\s*su)\b",
        r"\b(to\s*dung\s*dan\s*su)\b",
        r"\b(bo\s*luat\s*to\s*tung)\b",
        r"\b(luat\s*to\s*tung)\b",
        r"\b(hinh\s*su)\b",
        r"\b(hanh\s*chinh)\b",
        r"\b(luat\s*lao\s*dong)\b",
        r"\b(dau\s*tu)\b",
    ],
    "luat doanh nghiep": [
        r"\b(luat\s*dau\s*tu)\b",
        r"\b(chung\s*khoan)\b",
        r"\b(so\s*huu\s*tri\s*tue)\b",
        r"\b(canh\s*tranh)\b",
    ],
    "luat trong tai thuong mai": [
        r"\b(trong\s*tai\s*quoc\s*te)\b",
        r"\b(hoa\s*giai)\b",
        r"\b(to\s*tung)\b",
    ],
}


def _normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value or "")
    ascii_only = "".join(char for char in normalized if unicodedata.category(char) != "Mn")
    return ascii_only.replace("đ", "d").replace("Đ", "D").lower()


class GoogleSerperEngine:
    def __init__(self, config: dict):
        raw_keys = {
            key: value for key, value in os.environ.items()
            if key.startswith("SERPER_KEY_") and value.strip()
        }
        self.api_keys = [raw_keys[key] for key in sorted(raw_keys.keys())]

        if not self.api_keys:
            single_key = os.getenv("SERPER_API_KEY")
            if single_key:
                self.api_keys = [single_key]
            else:
                logger.critical("Khong tim thay bat ky API Key nao!")
                raise ValueError("Thieu SERPER_API_KEY.")

        self.settings = config.get("discovery_settings", {})
        self.rate_limit = config.get("rate_limit", {})
        self.current_key_idx = 0
        self.call_count = 0
        self.lock = threading.Lock()
        self.url = "https://google.serper.dev/search"
        self.source_priority = self.settings.get("source_priority", [])
        self.path_denylist = self.settings.get("path_denylist", [])

        raw_allowlist = self.settings.get("path_allowlist", {})
        self.path_allowlist: dict[str, list[str]] = {}
        for domain, prefixes in raw_allowlist.items():
            canon = domain.lower().replace("www.", "")
            if isinstance(prefixes, list):
                self.path_allowlist[canon] = prefixes

        logger.info(
            "GoogleSerperEngine da san sang: %s keys, priority=%s, allowlist=%s, denylist=%s paths.",
            len(self.api_keys),
            self.source_priority,
            self.path_allowlist,
            len(self.path_denylist),
        )

    def _rotate_key(self):
        with self.lock:
            self.current_key_idx = (self.current_key_idx + 1) % len(self.api_keys)
            self.call_count = 0
            logger.warning("Da xoay API Key. Hien dung Key so %s", self.current_key_idx + 1)

    def get_headers(self):
        limit = self.rate_limit.get("rotate_keys_every_n_calls", 100)
        if self.call_count >= limit:
            self._rotate_key()
        self.call_count += 1
        return {
            "X-API-KEY": self.api_keys[self.current_key_idx],
            "Content-Type": "application/json",
        }

    def search(
        self,
        query: str,
        query_id: str = "",
        target_law_name: str = "",
        target_law_aliases: list = None,
        _retry_count: int = 0,
    ) -> List[DiscoveredLink]:
        """
        Search Serper and return filtered discovery results.
        Retries on 403/429 after rotating to the next key.
        """
        if target_law_aliases is None:
            target_law_aliases = []

        target_domains = self.settings.get("target_domains", [])
        if target_domains:
            domain_filter = " OR ".join([f"site:{domain}" for domain in target_domains])
            search_query = f"{query} ({domain_filter})"
        else:
            search_query = query

        payload = {
            "q": search_query,
            "gl": "vn",
            "hl": "vi",
            "num": self.settings.get("max_results", 10),
        }

        logger.info("Thuc hien tim kiem Google: '%s'", query)
        time.sleep(self.rate_limit.get("delay_between_queries_sec", 1))

        try:
            response = requests.post(self.url, headers=self.get_headers(), json=payload, timeout=30)
            if response.status_code in [403, 429]:
                if _retry_count >= MAX_RATE_LIMIT_RETRIES:
                    logger.error(
                        "API Key #%s loi %s. Da dat gioi han retry cho query '%s'.",
                        self.current_key_idx + 1,
                        response.status_code,
                        query,
                    )
                    return []

                logger.warning(
                    "API Key #%s hit limit %s. Rotating and retrying (%s/%s) cho query '%s'.",
                    self.current_key_idx + 1,
                    response.status_code,
                    _retry_count + 1,
                    MAX_RATE_LIMIT_RETRIES,
                    query,
                )
                self._rotate_key()
                backoff_delay = self.rate_limit.get("delay_between_queries_sec", 2) * (2 ** _retry_count)
                time.sleep(backoff_delay)
                return self.search(
                    query=query,
                    query_id=query_id,
                    target_law_name=target_law_name,
                    target_law_aliases=target_law_aliases,
                    _retry_count=_retry_count + 1,
                )

            response.raise_for_status()
            data = response.json()

            discovered_links = []
            for item in data.get("organic", []):
                link_url = item.get("link", "")
                if not link_url:
                    continue

                title = item.get("title", "No Title")
                snippet = item.get("snippet", "")
                domain = self._extract_domain(link_url)

                if not self._is_official_document_url(link_url):
                    logger.info("  [FILTERED] Not official doc path: '%s'", title[:60])
                    continue

                if self._is_path_denied(link_url):
                    logger.info("  [FILTERED] Path nhieu: '%s'", title[:60])
                    continue

                if not self._is_target_law_match(title, snippet, target_law_name, target_law_aliases):
                    logger.info("  [FILTERED] Khong match ten luat: '%s'", title[:60])
                    continue

                law_id = self._extract_law_id(title, snippet)
                effective_date = self._extract_effective_date(title, snippet)

                discovered_links.append(
                    DiscoveredLink(
                        url=link_url,
                        title=title,
                        source_domain=domain,
                        search_query=query,
                        query_id=query_id,
                        snippet=snippet,
                        is_processed=False,
                        law_id=law_id,
                        effective_date=effective_date,
                    )
                )

            if self.source_priority:
                discovered_links.sort(key=lambda result: self._domain_priority(result.source_domain))

            logger.info(" Da tim %s ket qua cho: '%s'", len(discovered_links), query)
            return discovered_links

        except Exception as exc:
            logger.error("Loi khi discovery: %s", str(exc), exc_info=True)
            return []

    def _is_official_document_url(self, url: str) -> bool:
        """
        Keep URLs whose path matches the configured allowlist for that domain.
        If no allowlist is configured, keep everything for backward compatibility.
        """
        if not self.path_allowlist:
            return True

        domain = self._extract_domain(url)
        allowed_prefixes = self.path_allowlist.get(domain, [])
        if not allowed_prefixes:
            return False

        parsed_path = urlparse(url).path
        for prefix in allowed_prefixes:
            if parsed_path.startswith(prefix):
                return True
        return False

    def _extract_domain(self, url: str) -> str:
        return urlparse(url).netloc.replace("www.", "")

    def _is_path_denied(self, url: str) -> bool:
        if not self.path_denylist:
            return False
        for denied in self.path_denylist:
            if denied in url:
                return True
        return False

    def _is_target_law_match(
        self,
        title: str,
        snippet: str,
        target_law_name: str,
        target_law_aliases: list,
    ) -> bool:
        """
        Filter noise by exact law name and configured aliases.
        """
        if not target_law_name:
            return True

        combined = _normalize_text(f"{title} {snippet}")
        target_lower = _normalize_text(target_law_name)

        if target_lower in combined:
            return True

        for alias in target_law_aliases:
            if _normalize_text(alias) in combined:
                return True

        cross_patterns = CROSS_PATTERNS.get(target_lower, [])
        for pattern in cross_patterns:
            if re.search(pattern, combined, re.IGNORECASE):
                return False

        return False

    def _extract_law_id(self, title: str, snippet: str) -> Optional[str]:
        text = _normalize_text(f"{title} {snippet}")
        match = LAW_ID_PATTERN.search(text)
        if match:
            return match.group(1).strip().upper()
        return None

    def _extract_effective_date(self, title: str, snippet: str) -> Optional[str]:
        """
        Extract the most useful effective date from title/snippet text.

        Priority:
        1. "Hieu luc" labels.
        2. Full Vietnamese day/month/year text.
        3. Text after "Ban hanh".
        4. First fallback DD/MM/YYYY.
        5. Standalone year.

        Years must be within [2000, current_year + 1]. Year 1900 is a known
        placeholder from Congbao and must be blocked.
        """
        text = _normalize_text(f"{title} {snippet}")
        current_year = datetime.now().year

        def _parse_dmy(day: str, month: str, year_text: str) -> Optional[str]:
            try:
                year = int(year_text)
                if year == 1900:
                    return None
                if 2000 <= year <= current_year + 1:
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            except (TypeError, ValueError):
                return None
            return None

        def _parse_year(year_text: str) -> Optional[str]:
            try:
                year = int(year_text)
            except (TypeError, ValueError):
                return None
            if 2000 <= year <= current_year + 1:
                return f"{year}-??-??"
            return None

        eff_label = re.compile(
            r"hieu\s*luc\s*(?:ke\s*tu\s*)?(?:ngay\s+)?[:\-\s]*"
            r"(\d{1,2})\s*(?:thang)?[\/\-\s]+(\d{1,2})\s*(?:nam)?[\/\-\s]+(\d{4})",
            re.IGNORECASE,
        )
        match = eff_label.search(text)
        if match:
            result = _parse_dmy(match.group(1), match.group(2), match.group(3))
            if result:
                return result

        date_viet = re.compile(
            r"ngay\s+(\d{1,2})\s+thang\s+(\d{1,2})\s+nam\s+(\d{4})",
            re.IGNORECASE,
        )
        match = date_viet.search(text)
        if match:
            result = _parse_dmy(match.group(1), match.group(2), match.group(3))
            if result:
                return result

        banhanh_split = re.compile(r"ban\s*hanh\s+", re.IGNORECASE)
        parts = banhanh_split.split(text, maxsplit=1)
        if len(parts) == 2:
            tail = parts[1]
            match = eff_label.search(tail)
            if match:
                result = _parse_dmy(match.group(1), match.group(2), match.group(3))
                if result:
                    return result

            date_nolabel = re.compile(
                r"(?<![A-Z0-9/])(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})(?![A-Z0-9/\-])",
                re.IGNORECASE,
            )
            match = date_nolabel.search(tail)
            if match:
                result = _parse_dmy(match.group(1), match.group(2), match.group(3))
                if result:
                    return result

        date_full = re.compile(
            r"(?:ngay\s+)?(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})",
            re.IGNORECASE,
        )
        match = date_full.search(text)
        if match:
            result = _parse_dmy(match.group(1), match.group(2), match.group(3))
            if result:
                return result

        year_pat = re.compile(r"(?<![A-Z0-9/])(\d{4})(?![A-Z0-9/\-])")
        match = year_pat.search(text)
        if match:
            return _parse_year(match.group(1))

        return None

    def _domain_priority(self, domain: str) -> int:
        for index, alias in enumerate(self.source_priority):
            canonical = SOURCE_ALIAS_MAP.get(alias, alias)
            if canonical == domain:
                return index
        return len(self.source_priority)
