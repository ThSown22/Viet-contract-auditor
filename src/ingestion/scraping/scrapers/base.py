import logging
import time
from abc import ABC, abstractmethod
from typing import Optional
from urllib.parse import urlparse

import requests

from src.ingestion.schemas.models import ScrapedContent, ScrapingResult
from src.ingestion.scraping.normalizers.content_cleaner import ContentCleaner

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """Common fetch-clean-validate flow for each source."""

    def __init__(
        self,
        config: dict,
        validation_config: Optional[dict] = None,
        cleaner: Optional[ContentCleaner] = None,
    ):
        self.config = config
        self.validation_config = validation_config or {}
        self.cleaner = cleaner or ContentCleaner(self.validation_config)
        self.timeout = int(config.get("timeout", 30))
        self.headers = config.get("headers", {})
        self.retry_limit = int(config.get("retry_limit", 3))
        self.retry_delay = int(config.get("delay_between_retries_sec", 2))

    def scrape(self, url: str, law_name: str, law_id: Optional[str] = None) -> ScrapingResult:
        """Fetch, extract, clean, and validate a single page."""

        started_at = time.perf_counter()
        logger.info("Start scrape: %s", url)

        try:
            html = self._fetch_html(url)
            if not html:
                return ScrapingResult(
                    success=False,
                    error_message="Failed to fetch HTML",
                    attempted_url=url,
                )

            raw_content = self._extract_content(html, url)
            if not raw_content:
                return ScrapingResult(
                    success=False,
                    error_message="Failed to extract content from HTML",
                    attempted_url=url,
                )

            metadata = self._extract_metadata(html, url)
            clean_text = self._clean_content(raw_content)
            if not self._validate_content(clean_text):
                return ScrapingResult(
                    success=False,
                    error_message="Content validation failed",
                    attempted_url=url,
                )

            duration = round(time.perf_counter() - started_at, 3)
            content = ScrapedContent(
                law_name=law_name,
                law_id=(law_id or metadata.get("law_id") or "unknown"),
                source_url=url,
                source_domain=self._extract_domain(url),
                title=metadata.get("title") or "Unknown",
                raw_html=html[:5000],
                clean_text=clean_text,
                effective_date=metadata.get("effective_date"),
                published_at=metadata.get("published_at"),
                char_count=len(clean_text),
                word_count=len(clean_text.split()),
                has_structure=self.cleaner.has_structure(clean_text),
                scraping_duration_sec=duration,
                validation_passed=True,
            )
            logger.info("Scrape success: %s (%s chars)", url, content.char_count)
            return ScrapingResult(success=True, content=content, attempted_url=url)
        except Exception as exc:
            logger.error("Scrape failed: %s - %s", url, exc, exc_info=True)
            return ScrapingResult(success=False, error_message=str(exc), attempted_url=url)

    def _fetch_html(self, url: str) -> Optional[str]:
        """
        Fetch HTML với retry logic:
        
        Try 1 → Fail → Wait 2s
        Try 2 → Fail → Wait 2s
        Try 3 → Fail → Give up → Return None
        """
        for attempt in range(1, self.retry_limit + 1):
            try:
                logger.debug("Fetch attempt %s/%s: %s", attempt, self.retry_limit, url)
                response = requests.get(url, headers=self.headers, timeout=self.timeout)
                response.raise_for_status()
                response.encoding = response.apparent_encoding or "utf-8"
                return response.text
            except Exception as exc:
                logger.warning("Fetch attempt %s failed for %s: %s", attempt, url, exc)
                if attempt == self.retry_limit:
                    return None
                time.sleep(self.retry_delay)
        return None

    @abstractmethod
    def _extract_content(self, html: str, url: str) -> Optional[str]:
        raise NotImplementedError

    @abstractmethod
    def _extract_metadata(self, html: str, url: str) -> dict:
        raise NotImplementedError

    def _clean_content(self, raw_text: str) -> str:
        return self.cleaner.clean(raw_text)

    def _validate_content(self, clean_text: str) -> bool:
        return self.cleaner.validate(clean_text)

    def _extract_domain(self, url: str) -> str:
        return urlparse(url).netloc.replace("www.", "")
