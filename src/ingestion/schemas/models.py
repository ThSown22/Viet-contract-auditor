from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, HttpUrl, model_validator


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


class ScrapedArticle(BaseModel):
    article_id: str
    title: str
    text: str


class ScrapedContent(BaseModel):
    """Normalized content saved by Phase 2 scraping. - Là cái gì đc lưu vào jsonl."""

    law_name: str
    law_id: str
    source_url: HttpUrl
    source_domain: str
    title: str

    raw_html: Optional[str] = None
    clean_text: str
    structured_text: Optional[str] = None
    articles: list[ScrapedArticle] = Field(default_factory=list)

    scraped_at: datetime = Field(default_factory=datetime.now)
    effective_date: Optional[str] = None
    published_at: Optional[str] = None

    char_count: int = 0
    word_count: int = 0
    article_count: int = 0
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


class ArticleBlock(BaseModel):
    """Intermediate representation of one parsed article."""

    number: int
    title: str
    full_text: str
    token_count: int = 0


class LegalChunk(BaseModel):
    """Final output chunk schema."""

    chunk_id: str
    law_id: str
    law_name: str
    article_ids: list[str]
    article_titles: list[str]
    text: str
    token_count: int
    char_count: int
    has_overlap: bool
    prev_chunk_id: Optional[str] = None
    next_chunk_id: Optional[str] = None


class ChunkingConfig(BaseModel):
    """Config validation schema."""

    target_min_tokens: int = Field(ge=1)
    target_max_tokens: int = Field(ge=1)
    overlap_tokens: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_token_bounds(self) -> "ChunkingConfig":
        if self.target_max_tokens < self.target_min_tokens:
            raise ValueError("target_max_tokens must be >= target_min_tokens")
        return self
