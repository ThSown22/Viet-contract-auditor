import logging
import re
from io import BytesIO
from typing import Optional
from urllib.parse import urljoin

from pypdf import PdfReader
from lxml import html as lxml_html

from src.ingestion.scraping.normalizers.content_cleaner import normalize_text
from src.ingestion.scraping.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)
STRIP_XPATHS = [".//script", ".//style", ".//nav", ".//footer", ".//noscript", ".//iframe"]

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
                content_nodes = tree.xpath("//*[contains(concat(' ', normalize-space(@class), ' '), ' box-section--main ')]")
            if not content_nodes:
                logger.error("Congbao content extraction failed for %s", url)
                return None

            combined = "\n".join(filter(None, (self._node_text(node) for node in content_nodes)))
            if len(combined) < 200:
                pdf_text = self._extract_pdf_fallback(html, url)
                if pdf_text:
                    return pdf_text
                if re.search(r"\.pdf", html, re.IGNORECASE):
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

    def _extract_pdf_fallback(self, html: str, url: str) -> Optional[str]:
        pdf_urls = self._extract_download_links(html, url)
        if not pdf_urls:
            return None

        parts = [text.strip() for pdf_url in pdf_urls if (text := self._download_pdf_text(pdf_url))]
        merged = "\n\n".join(parts).strip()
        if merged:
            logger.info("Congbao PDF fallback succeeded for %s with %s file(s)", url, len(parts))
        return merged or None

    def _extract_download_links(self, html: str, base_url: str) -> list[str]:
        tree = lxml_html.fromstring(html)
        hrefs = tree.xpath(
            "//*[contains(concat(' ', normalize-space(@class), ' '), ' box-option-right ')]"
            "//a[contains(@href, '.pdf')]/@href"
        )
        if not hrefs:
            hrefs = tree.xpath("//a[contains(@href, '.pdf')]/@href")

        urls = [urljoin(base_url, href) for href in hrefs]
        return list(dict.fromkeys(urls))

    def _download_pdf_text(self, pdf_url: str) -> Optional[str]:
        try:
            response = self._request(pdf_url)
            if not response:
                return None
            reader = PdfReader(BytesIO(response))
            pages = [(page.extract_text() or "").strip() for page in reader.pages]
            return "\n".join(page for page in pages if page)
        except Exception as exc:
            logger.warning("Congbao PDF extraction failed for %s: %s", pdf_url, exc)
            return None

    def _request(self, url: str) -> Optional[bytes]:
        verify_ssl = self.config.get("file_extractors", {}).get("ssl_verify", True)
        for attempt in range(1, self.retry_limit + 1):
            try:
                logger.debug("Download attempt %s/%s: %s", attempt, self.retry_limit, url)
                response = self._http_get(url, verify=verify_ssl)
                response.raise_for_status()
                return response.content
            except Exception as exc:
                logger.warning("Download attempt %s failed for %s: %s", attempt, url, exc)
                if attempt == self.retry_limit:
                    return None
        return None

    @staticmethod
    def _node_text(node) -> str:
        for xpath_query in STRIP_XPATHS:
            for tag in node.xpath(xpath_query):
                tag.drop_tree()
        return "\n".join(part.strip() for part in node.itertext() if part and part.strip())
