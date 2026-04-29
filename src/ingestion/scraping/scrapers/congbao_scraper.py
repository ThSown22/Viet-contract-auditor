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


class CongbaoScraper(BaseScraper):
    """Scraper for congbao.chinhphu.vn."""

    def _extract_content(self, html: str, url: str) -> Optional[str]:
        try:
            tree = lxml_html.fromstring(html)
            content_nodes = tree.xpath(
                "//*[contains(concat(' ', normalize-space(@class), ' '), ' box-section--main ')]"
                "//*[contains(concat(' ', normalize-space(@class), ' '), ' text ')]"
            )
            if not content_nodes:
                logger.warning("Congbao primary selector missed for %s", url)
                content_nodes = tree.xpath(
                    "//*[contains(concat(' ', normalize-space(@class), ' '), ' box-section--main ')]"
                )
            if not content_nodes:
                logger.error("Congbao content extraction failed for %s", url)
                return None

            texts = []
            for node in content_nodes:
                for xpath_query in [".//script", ".//style", ".//nav", ".//footer", ".//noscript", ".//iframe"]:
                    for tag in node.xpath(xpath_query):
                        tag.drop_tree()
                texts.append("\n".join(part.strip() for part in node.itertext() if part and part.strip()))
            combined = "\n".join(part for part in texts if part.strip())
            if len(combined) < 200 and re.search(r"\.pdf", html, re.IGNORECASE):
                logger.warning("Congbao page appears to expose metadata plus PDF only: %s", url)
                return None
            return combined
        except Exception as exc:
            logger.error("Congbao content extraction failed: %s", exc, exc_info=True)
            return None

    def _extract_metadata(self, html: str, url: str) -> dict:
        tree = lxml_html.fromstring(html)
        metadata: dict[str, str] = {}

        title_nodes = tree.xpath(
            "//*[self::h1 and contains(concat(' ', normalize-space(@class), ' '), ' title ')]"
        ) or tree.xpath("//h1") or tree.xpath("//title")
        if title_nodes:
            metadata["title"] = " ".join(title_nodes[0].itertext()).strip()

        combined_text = f"{metadata.get('title', '')}\n{' '.join(part.strip() for part in tree.itertext() if part and part.strip())}\n{url}"
        law_id = self._extract_law_id(combined_text)
        if law_id:
            metadata["law_id"] = law_id

        normalized_text = normalize_text(combined_text)
        published = re.search(r"ban\s*hanh:\s*(\d{2})/(\d{2})/(\d{4})", normalized_text)
        if published:
            day, month, year = published.groups()
            metadata["published_at"] = f"{year}-{month}-{day}"

        effective = re.search(r"hieu\s*luc:\s*(\d{2})/(\d{2})/(\d{4})", normalized_text)
        if effective:
            day, month, year = effective.groups()
            if year != "1900":
                metadata["effective_date"] = f"{year}-{month}-{day}"

        return metadata

    def _extract_law_id(self, text: str) -> Optional[str]:
        normalized = normalize_text(text).upper()
        match = LAW_ID_REGEX.search(normalized)
        if match:
            return match.group(1).upper()
        return None
