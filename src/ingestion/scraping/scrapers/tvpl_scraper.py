import logging
import re
from typing import Optional

from lxml import html as lxml_html

from src.ingestion.scraping.normalizers.content_cleaner import normalize_text
from src.ingestion.scraping.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

LAW_ID_REGEX = re.compile(
    r"(\d{1,4}/VBHN-[A-Z]{2,6}|\d{1,3}/\d{4}/QH\d{1,2}|\d{1,3}/\d{4}/(?:ND|NĐ)-CP)",
    re.IGNORECASE,
)


class TVPLScraper(BaseScraper):
    """Scraper for thuvienphapluat.vn."""

    def _extract_content(self, html: str, url: str) -> Optional[str]:
        try:
            tree = lxml_html.fromstring(html)
            content_nodes = tree.xpath(
                "//*[contains(concat(' ', normalize-space(@class), ' '), ' content1 ')]"
            )
            if not content_nodes:
                logger.error("TVPL content selector not found for %s", url)
                return None
            content_node = content_nodes[0]

            for xpath_query in [
                ".//script",
                ".//style",
                ".//nav",
                ".//footer",
                ".//noscript",
                ".//iframe",
                ".//*[contains(concat(' ', normalize-space(@class), ' '), ' box-comment ')]",
                ".//*[contains(concat(' ', normalize-space(@class), ' '), ' tools ')]",
                ".//*[contains(concat(' ', normalize-space(@class), ' '), ' social-share ')]",
                ".//*[contains(concat(' ', normalize-space(@class), ' '), ' advertisement ')]",
                ".//*[contains(concat(' ', normalize-space(@class), ' '), ' ads ')]",
                ".//*[contains(concat(' ', normalize-space(@class), ' '), ' banner ')]",
            ]:
                for node in content_node.xpath(xpath_query):
                    node.drop_tree()

            return "\n".join(part.strip() for part in content_node.itertext() if part and part.strip())
        except Exception as exc:
            logger.error("TVPL content extraction failed: %s", exc, exc_info=True)
            return None

    def _extract_metadata(self, html: str, url: str) -> dict:
        tree = lxml_html.fromstring(html)
        metadata: dict[str, str] = {}

        title_nodes = tree.xpath("//title") or tree.xpath("//h1")
        if title_nodes:
            metadata["title"] = " ".join(title_nodes[0].itertext()).strip()

        combined_text = f"{metadata.get('title', '')}\n{' '.join(part.strip() for part in tree.itertext() if part and part.strip())}\n{url}"
        law_id = self._extract_law_id(combined_text)
        if law_id:
            metadata["law_id"] = law_id

        normalized_text = normalize_text(combined_text)
        published = re.search(r"ngay\s*(\d{1,2})\s*thang\s*(\d{1,2})\s*nam\s*(\d{4})", normalized_text)
        if published:
            day, month, year = published.groups()
            metadata["published_at"] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"

        effective = re.search(
            r"co\s*hieu\s*luc\s*thi\s*hanh\s*tu\s*ngay\s*(\d{1,2})\s*thang\s*(\d{1,2})\s*nam\s*(\d{4})",
            normalized_text,
        )
        if effective:
            day, month, year = effective.groups()
            metadata["effective_date"] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"

        return metadata

    def _extract_law_id(self, text: str) -> Optional[str]:
        normalized = normalize_text(text).upper()
        match = LAW_ID_REGEX.search(normalized)
        if match:
            return match.group(1).upper()
        return None
