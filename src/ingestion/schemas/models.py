from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, HttpUrl


class DiscoveredLink(BaseModel):
    url: HttpUrl
    title: str
    source_domain: str
    search_query: str
    discovered_at: datetime = Field(default_factory=datetime.now)
    snippet: Optional[str] = None
    is_processed: bool = False
    query_id: str = ""
    law_id: Optional[str] = None
    effective_date: Optional[str] = None


class ScrapedContent(BaseModel):
    """Normalized content saved by Phase 2 scraping. - Là cái gì đc lưu vào jsonl."""

    law_name: str
    law_id: str
    source_url: HttpUrl
    source_domain: str
    title: str

    raw_html: Optional[str] = None
    clean_text: str

    scraped_at: datetime = Field(default_factory=datetime.now)
    effective_date: Optional[str] = None
    published_at: Optional[str] = None

    char_count: int = 0
    word_count: int = 0
    has_structure: bool = False

    scraping_duration_sec: Optional[float] = None
    validation_passed: bool = False


class ScrapingResult(BaseModel):
    """Single scrape attempt result."""

    success: bool
    content: Optional[ScrapedContent] = None
    error_message: Optional[str] = None
    attempted_url: str
    fallback_level: int = 0
