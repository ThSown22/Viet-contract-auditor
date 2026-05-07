"""Validation helpers for cleaned legal content."""

from __future__ import annotations

import logging
import re
from typing import Optional

from src.ingestion.legal_text_patterns import (
    ARTICLE_NUMBER_RE,
    DEFINITION_NUMBER_RE,
    NUMBERED_ITEM_RE,
    is_structural_marker,
    is_top_level,
)


logger = logging.getLogger(__name__)


class CleanedContentValidator:
    """Validate quality requirements for cleaned text output."""

    def __init__(
        self,
        min_chars: int = 5000,
        required_keywords: list[str] | None = None,
        blacklist_keywords: list[str] | None = None,
    ) -> None:
        self.min_chars = min_chars
        self.required_keywords = required_keywords or ["Điều 1"]
        self.blacklist_keywords = blacklist_keywords or [
            "Quảng cáo",
            "Đăng nhập",
            "404 Not Found",
            "VĂN PHÒNG QUỐC HỘI",
            "Căn cứ Hiến pháp",
        ]

    def validate(self, text: str, law_name: str) -> tuple[bool, Optional[str]]:
        """Validate cleaned text quality against the article-level contract."""

        if len(text) < self.min_chars:
            return False, f"Text quá ngắn: {len(text)} < {self.min_chars} chars"

        missing = [keyword for keyword in self.required_keywords if keyword not in text]
        if missing:
            return False, f"Thiếu keywords: {missing}"

        lowered_text = text.lower()
        found_blacklist = [keyword for keyword in self.blacklist_keywords if keyword.lower() in lowered_text]
        if found_blacklist:
            return False, f"Vẫn còn noise: {found_blacklist}"

        if not text.startswith("Điều "):
            return False, "Text phải bắt đầu bằng Điều (header hoặc Chương/Mục chưa được loại bỏ)"

        if structural_markers := self.find_structural_markers(text):
            return False, f"Vẫn còn marker cấu trúc: {', '.join(structural_markers[:3])}"

        if anomalies := self.find_definition_anomalies(text):
            return False, f"Definition block lỗi: {'; '.join(anomalies[:3])}"

        if anomalies := self.find_structural_anomalies(text):
            return False, f"Structural corruption: {'; '.join(anomalies[:3])}"

        logger.info(" Validation passed: %s (%s chars)", law_name, f"{len(text):,}")
        return True, None

    def find_structural_markers(self, text: str) -> list[str]:
        markers: list[str] = []
        for line in text.splitlines():
            stripped = line.strip()
            if stripped and is_structural_marker(stripped):
                markers.append(stripped)
        return markers

    def find_definition_anomalies(self, text: str) -> list[str]:
        anomalies: list[str] = []
        for article_number, block in self._iter_definition_blocks(text):
            anomalies.extend(self._find_definition_block_anomalies(article_number, block))
        return anomalies

    def find_structural_anomalies(self, text: str) -> list[str]:
        anomalies: list[str] = []

        if re.search(r":\s+(?:[b-z]|\u0111)\)\s+", text, re.IGNORECASE):
            anomalies.append("inline lettered list starts from b)/c) right after ':'")

        return anomalies

    def _iter_definition_blocks(self, text: str) -> list[tuple[int, list[str]]]:
        blocks: list[tuple[int, list[str]]] = []
        lines = text.splitlines()
        index = 0

        while index < len(lines):
            line = lines[index].strip()
            match = ARTICLE_NUMBER_RE.match(line)
            if not match:
                index += 1
                continue

            article_number = int(match.group(1))
            block: list[str] = []
            index += 1

            while index < len(lines):
                current = lines[index].strip()
                if is_top_level(current):
                    break
                block.append(current)
                index += 1

            if any("được hiểu như sau:" in current.lower() for current in block):
                blocks.append((article_number, block))

        return blocks

    def _find_definition_block_anomalies(self, article_number: int, block: list[str]) -> list[str]:
        anomalies: list[str] = []
        numbered_lines = [line.strip() for line in block if DEFINITION_NUMBER_RE.match(line.strip())]
        if not numbered_lines:
            return [f"Điều {article_number}: missing numbered definition entries"]

        expected = 1
        numbering_started = False

        for line in block:
            stripped = line.strip()
            if not stripped or "được hiểu như sau:" in stripped.lower():
                continue

            match = DEFINITION_NUMBER_RE.match(stripped)
            if match:
                numbering_started = True
                current = int(match.group(1))
                if current != expected:
                    anomalies.append(f"Điều {article_number}: expected {expected}. but found {current}.")
                    expected = current
                expected += 1
                continue

            if numbering_started and self._looks_like_orphan_definition_line(stripped):
                anomalies.append(f"Điều {article_number}: orphan definition fragment `{stripped}`")
                break

        return anomalies

    @staticmethod
    def _looks_like_orphan_definition_line(line: str) -> bool:
        if not line or is_top_level(line) or NUMBERED_ITEM_RE.match(line):
            return False
        if re.search(r"[.;:]$", line):
            return False
        return line[:1].isupper() and len(line.split()) <= 12
