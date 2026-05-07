import re

from src.ingestion.legal_text_patterns import ARTICLE_HEADER_RE, NUMBERED_ITEM_RE, is_top_level
from src.ingestion.schemas.models import ScrapedArticle


BODY_PREFIX_RE = re.compile(
    r"^(?:Trong|Trường hợp|Khi|Nếu|Theo|Đối với|Tại|Luật này|Bộ luật này)\b",
    re.IGNORECASE,
)
BODY_VERB_RE = re.compile(
    r"\b(được|là|có|phải|bị|thì|mà|không|bao gồm|quy định|chịu|làm)\b",
    re.IGNORECASE,
)


def parse_articles(text: str, law_id: str) -> list[ScrapedArticle]:
    prepared_text = _prepare_text_for_parsing(text or "")
    matches = list(ARTICLE_HEADER_RE.finditer(prepared_text))
    if not matches:
        return []

    articles: list[ScrapedArticle] = []
    for index, match in enumerate(matches):
        start = match.start()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(prepared_text)
        block = prepared_text[start:end].strip()
        article = _parse_article_block(block, law_id)
        if article:
            articles.append(article)
    return articles


def reconstruct_structured_text(articles: list[ScrapedArticle]) -> str:
    parts: list[str] = []
    for article in articles:
        if article.text:
            parts.append(f"{article.title}\n{article.text}")
        else:
            parts.append(article.title)
    return "\n\n".join(parts).strip()


def _parse_article_block(block: str, law_id: str) -> ScrapedArticle | None:
    lines = [line.strip() for line in block.splitlines()]
    if not lines:
        return None

    match = ARTICLE_HEADER_RE.match(lines[0])
    if not match:
        return None

    article_number = match.group(1)
    title_parts = [f"Điều {article_number}. {match.group(2).strip()}".strip()]
    body_lines: list[str] = []

    for line in lines[1:]:
        if not line:
            if body_lines and body_lines[-1] != "":
                body_lines.append("")
            continue

        if not body_lines and _is_title_continuation(line):
            title_parts.append(line)
            continue

        body_lines.append(line)

    return ScrapedArticle(
        article_id=f"{law_id}__{article_number}",
        title=" ".join(part for part in title_parts if part).strip(),
        text="\n".join(body_lines).strip(),
    )


def _is_title_continuation(line: str) -> bool:
    if is_top_level(line) or NUMBERED_ITEM_RE.match(line):
        return False
    if BODY_PREFIX_RE.match(line):
        return False
    if line.endswith("."):
        return False

    head = " ".join(line.split()[:8])
    if BODY_VERB_RE.search(head) and len(line.split()) >= 4:
        return False

    return len(line.split()) <= 18


def _prepare_text_for_parsing(text: str) -> str:
    prepared = text.replace("\r\n", "\n").replace("\r", "\n")
    prepared = re.sub(r"Điều\s*\n\s*(\d+[\.:])", r"Điều \1", prepared)
    prepared = re.sub(r"(?<=[\.;])\s*(Điều\s+\d+[\.:])", r"\n\1", prepared)
    return prepared
