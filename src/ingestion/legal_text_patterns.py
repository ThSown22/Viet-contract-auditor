"""Shared structural patterns for Vietnamese legal text."""

from __future__ import annotations

import re


PART_ORDINAL_RE = r"(?:[IVXLC\d]+|[A-Za-zÀ-ỹĐđ]+)"

ARTICLE_HEADER_RE = re.compile(r"^Điều\s+(\d+)[\.:]\s*(.*)$", re.IGNORECASE | re.MULTILINE)
ARTICLE_NUMBER_RE = re.compile(r"^Điều\s+(\d+)[\.:]", re.IGNORECASE)
CHAPTER_MARKER_RE = re.compile(r"^Chương\s+[IVXLC\d]+\.?(?:\s+.*)?$", re.IGNORECASE)
SECTION_MARKER_RE = re.compile(r"^Mục\s+(?:\d+|[IVXLC]+)\.?(?:\s+.*)?$", re.IGNORECASE)
PART_MARKER_RE = re.compile(rf"^Phần\s+thứ\s+{PART_ORDINAL_RE}\.?(?:\s+.*)?$", re.IGNORECASE)
SUBSECTION_MARKER_RE = re.compile(r"^Tiểu mục\s+(?:\d+|[IVXLC]+)\.?(?:\s+.*)?$", re.IGNORECASE)
TOP_LEVEL_RE = re.compile(
    rf"^(?:Chương\s+[IVXLC\d]+\.?(?:\s+.*)?|Mục\s+(?:\d+|[IVXLC]+)\.?(?:\s+.*)?|Phần\s+thứ\s+{PART_ORDINAL_RE}\.?(?:\s+.*)?|Tiểu mục\s+(?:\d+|[IVXLC]+)\.?(?:\s+.*)?|Điều\s+\d+[\.:])",
    re.IGNORECASE,
)
NUMBERED_ITEM_RE = re.compile(r"^(?:\d+[\.:](?:\s|$)|[a-zđ]\)\s|[-•])", re.IGNORECASE)
LETTERED_ITEM_RE = re.compile(r"^[a-zđ]\)\s", re.IGNORECASE)
DEFINITION_NUMBER_RE = re.compile(r"^(\d+)\.\s*(.*)$")


STRUCTURAL_MARKER_PATTERNS = (
    CHAPTER_MARKER_RE,
    SECTION_MARKER_RE,
    PART_MARKER_RE,
    SUBSECTION_MARKER_RE,
)


def is_top_level(line: str) -> bool:
    """Return True for chapter/section/article markers."""

    return bool(TOP_LEVEL_RE.match((line or "").strip()))


def is_structural_marker(line: str) -> bool:
    """Return True for standalone chapter/section/part headings."""

    stripped = (line or "").strip()
    return any(pattern.match(stripped) for pattern in STRUCTURAL_MARKER_PATTERNS)


def extract_article_number(line: str) -> int | None:
    """Extract the article number from an article header."""

    match = ARTICLE_NUMBER_RE.match((line or "").strip())
    return int(match.group(1)) if match else None
