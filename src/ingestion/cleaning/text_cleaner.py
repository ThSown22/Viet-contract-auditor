"""Cleaning utilities for Vietnamese legal text."""

from __future__ import annotations

import html
import re
import unicodedata

from src.ingestion.legal_text_patterns import (
    ARTICLE_HEADER_RE,
    ARTICLE_NUMBER_RE,
    DEFINITION_NUMBER_RE,
    NUMBERED_ITEM_RE,
    extract_article_number,
    is_structural_marker,
    is_top_level,
)


class VietnameseLegalTextCleaner:
    """Clean scraped legal text down to article-level content."""

    START_PATTERNS = (
        re.compile(r"(?im)^Chương\b"),
        re.compile(r"(?im)^Điều\s+1[\.:]\s*"),
    )
    FOOTNOTE_RE = re.compile(r"(?:(?<=\s)|^)\[\d+\](?=\s|$)")
    INLINE_ARTICLE_RE = re.compile(r"\s+Điều\s+(\d+\.\s*\S.*)$")
    DEFINITION_ENTRY_RE = re.compile(
        r"^[A-ZĐĂÂÊÔƠƯ][^.!?:]{0,120}\s+(?:là|bao gồm|hình thức|phương thức)\b",
        re.IGNORECASE,
    )
    DEFINITION_VERB_RE = re.compile(r"\b(là|bao gồm|hình thức|phương thức)\b", re.IGNORECASE)
    EMBEDDED_DEFINITION_SPLIT_RE = re.compile(
        r"\s+(?=[A-ZĐĂÂÊÔƠƯ][^.!?:]{0,120}\s+(?:là|bao gồm|hình thức|phương thức)\b)",
        re.IGNORECASE,
    )
    BODY_PREFIX_RE = re.compile(
        r"^(?:Trong|Trường hợp|Khi|Nếu|Theo|Đối với|Tại|Luật này|Bộ luật này|Chính phủ|Tòa án|Tổ chức|Cơ quan|Hội đồng|Ủy ban|Lệ phí)\b",
        re.IGNORECASE,
    )
    BODY_VERB_RE = re.compile(
        r"\b(được|là|có|phải|bị|thì|mà|không|bao gồm|quy định|chịu|làm)\b",
        re.IGNORECASE,
    )
    TITLE_CONNECTOR_RE = re.compile(r"\b(của|với|tại|đến|về|theo|trong|từ|đối|trên|dưới)$", re.IGNORECASE)
    NUMBERED_CLAUSE_RE = re.compile(r"^(?:\d+[\.:]|[a-zÄ‘]\))\s*", re.IGNORECASE)
    TRAILING_MARKERS = (
        "Nơi nhận:",
        "XÁC THỰC VĂN BẢN",
        "CHỦ TỊCH QUỐC HỘI",
        "CHỦ NHIỆM",
        "có căn cứ ban hành như sau:",
        "Điều 2 và Điều 3 Luật số",
    )
    DEFINITION_CONTINUATION_PREFIXES = (
        "Nếu ",
        "Trường hợp ",
        "Trong trường hợp ",
        "Theo ",
        "Khi ",
        "Đối với ",
        "Tại ",
        "Cộng hòa ",
        "Chính phủ ",
    )

    TOKEN_PARTS_RE = re.compile(r"^([^A-Za-zÀ-ỹĐ-đ]*)([A-Za-zÀ-ỹĐ-đ]+)([^A-Za-zÀ-ỹĐ-đ]*)$")
    OCR_ONSETS = {
        "b",
        "c",
        "ch",
        "d",
        "đ",
        "g",
        "gh",
        "gi",
        "h",
        "k",
        "kh",
        "l",
        "m",
        "n",
        "ng",
        "ngh",
        "nh",
        "p",
        "ph",
        "q",
        "qu",
        "r",
        "s",
        "t",
        "th",
        "tr",
        "v",
        "x",
    }
    VOWELS = set("aeiouyăâêôơư")
    PUBLICATION_ARTIFACT_RE = re.compile(
        r"^\d*\s*cong\s+bao\s*/\s*so\s+[\d\s+./-]+(?:/\s*ngay\s+\d{1,2}[-/]\d{1,2}[-/]\d{4})?\s*\d*$",
        re.IGNORECASE,
    )
    INLINE_PUBLICATION_ARTIFACT_RE = re.compile(
        r"\s*(?:\d+\s+)?CÔNG\s+BÁO\s*/\s*Số.*$",
        re.IGNORECASE,
    )

    SOURCE_ARTIFACT_PATTERNS = (
        re.compile(r"^\(?xem\s+tiep\s+cong\s+bao\s+so\b.*\)?$", re.IGNORECASE),
        re.compile(r"^\(?tiep\s+theo\s+cong\s+bao\s+so\b.*\)?$", re.IGNORECASE),
        re.compile(r"^phan\s+van\s+ban\s+quy\s+pham\s+phap\s+luat$", re.IGNORECASE),
        re.compile(r"^chu\s+tich\s+nuoc\s*-\s*quoc\s+hoi$", re.IGNORECASE),
        re.compile(r"^luat\s+so\s+\d+/\d{4}/[a-z0-9-]+\b.*$", re.IGNORECASE),
    )

    def clean(self, raw_text: str, law_name: str | None = None) -> str:
        """Run the cleaning pipeline."""

        text = self.remove_document_header(raw_text)
        text = self.normalize_linebreaks(text)
        text = self.repair_article_markers(text)
        text = self.repair_article_headers(text)
        text = self.repair_definition_blocks(text)
        text = self.repair_inline_lettered_items(text)
        text = self.repair_orphan_lines(text)
        text = self.remove_structural_markers(text)
        text = self.apply_production_cleaning(text)
        text = self.repair_ocr_spacing(text)
        text = self.deduplicate_lines(text)
        text = self._remove_trailing_appendix(text)
        return text.strip()

    def remove_document_header(self, raw_text: str) -> str:
        """Trim everything before the first real content marker."""

        text = self._normalize_newlines(raw_text)
        positions = [match.start() for pattern in self.START_PATTERNS if (match := pattern.search(text))]
        if not positions:
            return text.strip()
        return text[min(positions) :].lstrip()

    def normalize_linebreaks(self, text: str) -> str:
        """Merge obvious wrapped lines while preserving legal structure."""

        lines = [re.sub(r"[ \t]+", " ", line).strip() for line in self._normalize_newlines(text).split("\n")]
        folded: list[str] = []

        for line in lines:
            if not line:
                if folded and folded[-1] != "":
                    folded.append("")
                continue

            if folded and self._should_merge_lines(folded[-1], line):
                folded[-1] = f"{folded[-1]} {line}"
                continue

            if folded and self._needs_leading_blank(line, folded[-1]):
                folded.append("")

            folded.append(line)

        return re.sub(r"\n{3,}", "\n\n", "\n".join(folded)).strip()

    def repair_article_markers(self, text: str) -> str:
        """Recover article headers that were broken by OCR or bad line wraps."""

        lines = text.split("\n")
        repaired: list[str] = []
        index = 0

        while index < len(lines):
            line = lines[index].strip()

            if re.search(r"\bĐiều\s*$", line) and index + 1 < len(lines):
                next_line = lines[index + 1].strip()
                if re.match(r"^\d+\.\s+\S", next_line):
                    prefix = re.sub(r"\s*Điều\s*$", "", line).rstrip()
                    if prefix:
                        repaired.append(prefix)
                    repaired.append(f"Điều {next_line}")
                    index += 2
                    continue

            embedded = self.INLINE_ARTICLE_RE.search(line)
            if embedded and not line.startswith("Điều "):
                prefix = line[: embedded.start()].rstrip()
                if prefix:
                    repaired.append(prefix)
                repaired.append(f"Điều {embedded.group(1).strip()}")
                index += 1
                continue

            repaired.append(line)
            index += 1

        repaired = self._repair_missing_article_numbers(repaired)
        return self._renumber_out_of_sequence_articles(repaired)

    def repair_article_headers(self, text: str) -> str:
        """Rebuild split article titles and move bodies to the next line."""

        lines = text.split("\n")
        repaired: list[str] = []
        index = 0

        while index < len(lines):
            line = lines[index].strip()
            match = ARTICLE_HEADER_RE.match(line)
            if not match:
                repaired.append(line)
                index += 1
                continue

            number, remainder = match.groups()
            title, pending_body = self._split_inline_article_content(remainder)
            title_parts = [title] if title else []
            index += 1

            while index < len(lines):
                current = lines[index].strip()
                if not current:
                    if title_parts:
                        break
                    index += 1
                    continue
                if is_top_level(current) or NUMBERED_ITEM_RE.match(current):
                    break
                if self._looks_like_body_start(current):
                    if self._should_extend_article_title(title_parts, current, self._next_nonempty_line(lines, index + 1)):
                        title_parts.append(current)
                        index += 1
                        continue
                    break
                title_parts.append(current)
                index += 1

            header = f"Điều {number}. {' '.join(part.strip(' ,') for part in title_parts if part).strip()}".strip()
            repaired.append(header)

            if pending_body:
                repaired.append(pending_body)

        return "\n".join(repaired)

    def repair_definition_blocks(self, text: str) -> str:
        """Normalize definition-list articles into stable numbered entries."""

        lines = text.split("\n")
        repaired: list[str] = []
        index = 0

        while index < len(lines):
            line = lines[index].strip()
            repaired.append(line)
            index += 1

            if "được hiểu như sau:" not in line.lower():
                continue

            block: list[str] = []
            while index < len(lines):
                current = lines[index].strip()
                if not current or is_top_level(current):
                    break
                block.append(current)
                index += 1

            repaired.extend(self._normalize_definition_block(block))

        return "\n".join(repaired)

    def repair_inline_lettered_items(self, text: str) -> str:
        """Split inline a), b), c) subpoints onto separate lines."""

        split_re = re.compile(r"(?<=[:;])\s+(?=[a-zđ]\)\s+[A-ZĐĂÂÊÔƠƯ])", re.IGNORECASE)
        repaired: list[str] = []

        for raw_line in text.split("\n"):
            line = raw_line.strip()
            if len(re.findall(r"[a-zđ]\)\s+", line, re.IGNORECASE)) < 2:
                repaired.append(line)
                continue

            parts = [part.strip() for part in split_re.split(line) if part.strip()]
            repaired.extend(parts if len(parts) > 1 else [line])

        return "\n".join(repaired)

    def repair_orphan_lines(self, text: str) -> str:
        """Merge short OCR fragments back into their surrounding lines."""

        repaired: list[str] = []

        for raw_line in text.split("\n"):
            line = raw_line.strip()
            if not line:
                if repaired and repaired[-1] != "":
                    repaired.append("")
                continue

            if not repaired:
                repaired.append(line)
                continue

            if self._should_merge_orphan(repaired[-1], line):
                repaired[-1] = f"{repaired[-1]} {line}"
                continue

            repaired.append(line)

        return "\n".join(repaired)

    def remove_structural_markers(self, text: str) -> str:
        """Remove chapter and section headers so only article-level content remains."""

        cleaned_lines: list[str] = []
        skip_title = False

        for line in text.split("\n"):
            stripped = line.strip()

            if is_structural_marker(stripped):
                skip_title = True
                continue

            if skip_title:
                if not stripped:
                    continue
                if stripped.startswith("Điều ") or NUMBERED_ITEM_RE.match(stripped):
                    skip_title = False
                elif self._is_uppercase_heading(stripped) and len(stripped) < 120:
                    continue
                else:
                    skip_title = False

            cleaned_lines.append(line)

        return "\n".join(cleaned_lines)

    def apply_production_cleaning(self, text: str) -> str:
        """Apply HTML, Unicode, and whitespace cleanup."""

        cleaned = self._normalize_newlines(text)
        cleaned = html.unescape(html.unescape(cleaned))
        cleaned = re.sub(r"</?[^>]+>", " ", cleaned)
        cleaned = cleaned.replace("\xa0", " ").replace("\u200b", "").replace("\ufeff", "")
        cleaned = unicodedata.normalize("NFC", cleaned)
        cleaned = self.FOOTNOTE_RE.sub(" ", cleaned)
        cleaned = "".join(char for char in cleaned if char in "\n\t" or unicodedata.category(char)[0] != "C")

        lines = []
        for line in cleaned.split("\n"):
            normalized = re.sub(r"[ \t]+", " ", line).strip()
            normalized = self.INLINE_PUBLICATION_ARTIFACT_RE.sub("", normalized)
            normalized = re.sub(r"\s+([,.;:])", r"\1", normalized)
            if self._is_publication_artifact_line(normalized) or self._is_source_artifact_line(normalized):
                continue
            lines.append(normalized)

        collapsed = self._remove_blank_lines_between_list_items(lines)
        return re.sub(r"\n{3,}", "\n\n", "\n".join(collapsed)).strip()

    def deduplicate_lines(self, text: str) -> str:
        """Remove consecutive duplicate lines while preserving paragraph breaks."""

        deduplicated: list[str] = []
        previous = None

        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                if deduplicated and deduplicated[-1] != "":
                    deduplicated.append("")
                previous = ""
                continue

            if stripped == previous:
                continue

            deduplicated.append(stripped)
            previous = stripped

        return "\n".join(deduplicated).strip()

    def repair_ocr_spacing(self, text: str) -> str:
        """Repair words split by OCR into multiple tiny fragments."""

        repaired_lines = [self._repair_ocr_line(line) for line in text.split("\n")]
        return "\n".join(repaired_lines)

    def _is_publication_artifact_line(self, line: str) -> bool:
        normalized = self._normalize_ascii(line)
        return bool(self.PUBLICATION_ARTIFACT_RE.match(normalized))

    def _is_source_artifact_line(self, line: str) -> bool:
        normalized = self._normalize_ascii(line)
        return any(pattern.match(normalized) for pattern in self.SOURCE_ARTIFACT_PATTERNS)

    def _remove_blank_lines_between_list_items(self, lines: list[str]) -> list[str]:
        collapsed: list[str] = []
        for index, line in enumerate(lines):
            if line:
                collapsed.append(line)
                continue

            previous = next((item for item in reversed(collapsed) if item), "")
            following = next((item for item in lines[index + 1 :] if item), "")
            if previous and following and self.NUMBERED_CLAUSE_RE.match(previous) and self.NUMBERED_CLAUSE_RE.match(following):
                continue

            collapsed.append(line)

        return collapsed

    def _normalize_definition_block(self, block: list[str]) -> list[str]:
        entries: list[list[str]] = []
        current: list[str] = []

        for raw_line in block:
            for line in self._split_embedded_definition_lines(self._clean_definition_line(raw_line)):
                if not line:
                    continue

                if match := DEFINITION_NUMBER_RE.match(line):
                    if current:
                        entries.append(current)
                    current = [match.group(2).strip()] if match.group(2).strip() else []
                    continue

                if current and self._starts_new_definition_entry(line, " ".join(current)):
                    entries.append(current)
                    current = [line]
                    continue

                current.append(line)

        if current:
            entries.append(current)

        normalized = [self._collapse_definition_entry(entry) for entry in entries]
        return [f"{index}. {text}" for index, text in enumerate((item for item in normalized if item), start=1)]

    def _repair_ocr_line(self, line: str) -> str:
        if not line or not self._line_needs_ocr_repair(line):
            return line

        tokens = line.split()
        repaired: list[str] = []
        index = 0

        while index < len(tokens):
            if index + 1 < len(tokens):
                merged = self._merge_ocr_pair(tokens[index], tokens[index + 1], aggressive=True)
                if merged is not None:
                    tokens[index + 1] = merged
                    index += 1
                    continue

            repaired.append(tokens[index])
            index += 1

        return " ".join(repaired)

    def _line_needs_ocr_repair(self, line: str) -> bool:
        tokens = line.split()
        cores = [parts[1] for parts in (self._split_token_parts(token) for token in tokens) if parts]
        singletons = sum(1 for core in cores if len(core) == 1)
        if singletons >= 2:
            return True

        return any(
            self._merge_ocr_pair(current, nxt, aggressive=False) is not None
            for current, nxt in zip(tokens, tokens[1:])
        )

    def _merge_ocr_pair(self, current: str, nxt: str, aggressive: bool) -> str | None:
        current_parts = self._split_token_parts(current)
        next_parts = self._split_token_parts(nxt)
        if current_parts is None or next_parts is None:
            return None

        current_prefix, current_core, current_suffix = current_parts
        next_prefix, next_core, next_suffix = next_parts
        if current_prefix or current_suffix or next_prefix:
            return None
        if not current_core or not next_core or not (current_core.isalpha() and next_core.isalpha()):
            return None

        current_norm = self._normalize_ascii(current_core)
        next_norm = self._normalize_ascii(next_core)
        if not current_norm or not next_norm:
            return None

        next_starts_with_vowel = next_norm[0] in self.VOWELS
        if current_norm in self.OCR_ONSETS and next_starts_with_vowel and len(next_core) <= 4:
            return f"{current_core}{next_core}{next_suffix}"

        if not aggressive:
            return None

        current_ends_with_vowel = current_norm[-1] in self.VOWELS
        if (
            current_ends_with_vowel
            and current_core.isascii()
            and next_starts_with_vowel
            and len(current_core) <= 4
            and len(next_core) <= 3
        ):
            return f"{current_core}{next_core}{next_suffix}"

        return None

    def _repair_missing_article_numbers(self, lines: list[str]) -> list[str]:
        repaired = list(lines)
        index = 0

        while index < len(repaired):
            orphan_title = self._extract_orphan_article_title(repaired[index])
            if orphan_title is None:
                index += 1
                continue

            start = index
            titles: list[str] = []
            while index < len(repaired):
                orphan_title = self._extract_orphan_article_title(repaired[index])
                if orphan_title is None:
                    break
                titles.append(orphan_title)
                index += 1

            previous_number = self._previous_article_number(repaired, start)
            next_number = self._next_article_number(repaired, index)
            missing_count = (next_number - previous_number - 1) if previous_number is not None and next_number is not None else 0

            if previous_number is None or next_number is None or missing_count <= 0 or len(titles) != missing_count:
                continue

            for offset, title in enumerate(titles, start=1):
                repaired[start + offset - 1] = f"Điều {previous_number + offset}. {title}"

        return repaired

    def _renumber_out_of_sequence_articles(self, lines: list[str]) -> str:
        repaired = list(lines)
        article_indexes = [(index, extract_article_number(line)) for index, line in enumerate(repaired)]
        article_indexes = [(index, number) for index, number in article_indexes if number is not None]

        for position, (index, current_number) in enumerate(article_indexes):
            if position == 0 or position == len(article_indexes) - 1:
                continue

            previous_number = article_indexes[position - 1][1]
            next_number = article_indexes[position + 1][1]
            if current_number > previous_number or next_number <= previous_number:
                continue

            expected = previous_number + 1
            repaired[index] = re.sub(r"^Điều\s+\d+([\.:])", rf"Điều {expected}\1", repaired[index], count=1)

        return "\n".join(repaired)

    def _split_inline_article_content(self, text: str) -> tuple[str, str | None]:
        content = (text or "").strip()
        if not content:
            return "", None

        if match := re.search(r"\s(?=(?:\d+[\.:]\s+\S))", content):
            return content[: match.start()].strip(), content[match.start() + 1 :].strip()

        for match in re.finditer(r"\s+(?=[A-ZĐ])", content):
            title = content[: match.start()].strip()
            body = content[match.end() :].strip()
            if not title or not body:
                continue
            if not 2 <= len(title.split()) <= 18:
                continue
            if re.search(r"[.;:]$", title):
                continue
            if self.TITLE_CONNECTOR_RE.search(title):
                continue
            if len(title.split()) <= 2 and not re.search(r"[,;:]", body) and not body.endswith(":"):
                continue
            if self._starts_with_split_proper_noun(body):
                continue
            if self._looks_like_inline_body(body):
                return title, body

        return content, None

    def _should_merge_lines(self, previous: str, current: str) -> bool:
        if not previous or not current:
            return False
        if is_top_level(current) or NUMBERED_ITEM_RE.match(current):
            return False
        if previous in {"Chương", "Mục"}:
            return False
        if previous == "Điều" and re.match(r"^\d+[\.:]\s*", current):
            return True
        if re.match(r"^Điều\s+\d+[\.:]\s*$", previous):
            return True
        if ARTICLE_NUMBER_RE.match(previous):
            if self._starts_with_lowercase(current):
                return True
            return not self._looks_like_body_start(current)
        if self._is_uppercase_heading(previous):
            return True
        return self._starts_with_lowercase(current) or not re.search(r"[.!?:]$", previous)

    def _should_merge_orphan(self, previous: str, current: str) -> bool:
        if is_top_level(current) or NUMBERED_ITEM_RE.match(current):
            return False
        if ARTICLE_NUMBER_RE.match(previous):
            return False
        if self._starts_with_lowercase(current):
            return True
        if self._ends_with_split_proper_noun(previous):
            return True
        if len(current.split()) <= 2 and current[:1].isalpha():
            return True
        if re.search(r"\b(tại|theo|của|trong|khoản|điểm)$", previous, re.IGNORECASE):
            return True
        return previous.endswith((",", ";", ":"))

    def _looks_like_body_start(self, line: str) -> bool:
        if not line or is_top_level(line) or NUMBERED_ITEM_RE.match(line):
            return False
        if self.BODY_PREFIX_RE.match(line):
            return True
        head = " ".join(line.split()[:12])
        return bool(self.BODY_VERB_RE.search(head)) and len(line.split()) >= 4

    def _looks_like_inline_body(self, line: str) -> bool:
        if self._looks_like_body_start(line):
            return True

        words = line.split()
        if len(words) < 8:
            return False

        head = " ".join(words[:40])
        if line.endswith(":"):
            return True
        if re.search(r"[,;]", head) and self.BODY_VERB_RE.search(head):
            return True
        return len(words) >= 14 and bool(self.BODY_VERB_RE.search(head))

    def _should_extend_article_title(self, title_parts: list[str], current: str, next_line: str | None) -> bool:
        if not title_parts or not next_line or not NUMBERED_ITEM_RE.match(next_line):
            return False
        if len(" ".join(title_parts).split()) > 3:
            return False
        if re.search(r"[.;:!?]$", current) or re.search(r"[,;]", current):
            return False
        return len(current.split()) <= 12

    @staticmethod
    def _next_nonempty_line(lines: list[str], start: int) -> str | None:
        for index in range(start, len(lines)):
            candidate = lines[index].strip()
            if candidate:
                return candidate
        return None

    def _starts_new_definition_entry(self, line: str, current_text: str) -> bool:
        candidate = line.lstrip(".:; ").strip()
        if self._starts_definition_continuation(candidate):
            return False
        if not (self._is_definition_lead(candidate) or self._looks_like_definition_term(candidate)):
            return False
        if re.search(r"[.!?]$", current_text):
            return True
        return bool(self.BODY_VERB_RE.search(current_text)) and len(current_text.split()) >= 8

    def _looks_like_definition_term(self, line: str) -> bool:
        candidate = line.lstrip(".:; ").strip()
        if not candidate or is_top_level(candidate) or NUMBERED_ITEM_RE.match(candidate):
            return False
        if self._starts_definition_continuation(candidate):
            return False
        if re.search(r"[.;:]$", candidate):
            return False
        return candidate[:1].isupper() and len(candidate.split()) <= 12

    def _starts_definition_continuation(self, line: str) -> bool:
        candidate = line.lstrip(".:; ").strip()
        return self._starts_with_lowercase(candidate) or candidate.startswith(self.DEFINITION_CONTINUATION_PREFIXES)

    @staticmethod
    def _starts_with_split_proper_noun(line: str) -> bool:
        words = line.split()
        if len(words) < 2:
            return False
        first = re.sub(r"[^\wÀ-ỹĐđ-]", "", words[0])
        second = re.sub(r"[^\wÀ-ỹĐđ-]", "", words[1])
        return bool(first and second) and len(first) <= 4 and first[:1].isupper() and second[:1].isupper()

    def _clean_definition_line(self, line: str) -> str:
        cleaned = self.FOOTNOTE_RE.sub(" ", line)
        cleaned = re.sub(r"[ \t]+", " ", cleaned).strip()
        return cleaned.lstrip(". ").strip()

    def _collapse_definition_entry(self, entry: list[str]) -> str:
        parts: list[str] = []

        for line in entry:
            if not line:
                continue
            if parts and self._should_join_definition_part(parts[-1], line):
                parts[-1] = f"{parts[-1]} {line}".strip()
                continue
            parts.append(line)

        return " ".join(part.strip() for part in parts if part).strip()

    def _split_embedded_definition_lines(self, line: str) -> list[str]:
        if not line:
            return []

        split_at = None
        split_candidate = None

        for match in self.EMBEDDED_DEFINITION_SPLIT_RE.finditer(line):
            prefix = line[: match.start()].strip()
            candidate = line[match.end() :].lstrip(".:; ").strip()
            if not candidate or self._starts_definition_continuation(candidate):
                continue
            if not self._is_definition_lead(candidate):
                continue
            if len(prefix.split()) < 8 or not self.BODY_VERB_RE.search(prefix):
                continue
            split_at = match.start()
            split_candidate = candidate

        if split_at is None or split_candidate is None:
            return [line]

        return [line[:split_at].strip(), *self._split_embedded_definition_lines(split_candidate)]

    def _is_definition_lead(self, line: str) -> bool:
        match = self.DEFINITION_VERB_RE.search(line)
        if not match:
            return False

        lead = line[: match.start()].strip()
        if not lead or len(lead.split()) > 8:
            return False
        if self._starts_definition_continuation(lead):
            return False
        if re.search(r"\b(thì|phải|được)\b", lead, re.IGNORECASE):
            return False
        return lead[:1].isupper()

    def _should_join_definition_part(self, previous: str, current: str) -> bool:
        if self._starts_definition_continuation(current):
            return True
        if not re.search(r"[.!?]$", previous):
            return True
        return False

    def _extract_orphan_article_title(self, line: str) -> str | None:
        match = re.match(r"^Điều\s+(?!\d+[\.:])(.+)$", line.strip())
        if not match:
            return None

        title = match.group(1).strip()
        if not title or is_top_level(title):
            return None
        if title[:1].islower():
            return title[:1].upper() + title[1:]
        return title

    def _previous_article_number(self, lines: list[str], index: int) -> int | None:
        for current in range(index - 1, -1, -1):
            if match := ARTICLE_NUMBER_RE.match(lines[current].strip()):
                return int(match.group(1))
        return None

    def _next_article_number(self, lines: list[str], index: int) -> int | None:
        for current in range(index, len(lines)):
            if match := ARTICLE_NUMBER_RE.match(lines[current].strip()):
                return int(match.group(1))
        return None

    def _remove_trailing_appendix(self, text: str) -> str:
        cut_positions: list[int] = []
        minimum_tail_offset = 0 if len(text) < 5000 else max(1000, len(text) // 3)

        for marker in self.TRAILING_MARKERS:
            marker_index = text.find(marker)
            if marker_index >= minimum_tail_offset:
                cut_positions.append(marker_index)

        if not cut_positions:
            return text.strip()
        return text[: min(cut_positions)].rstrip()

    @staticmethod
    def _normalize_newlines(text: str) -> str:
        return (text or "").replace("\r\n", "\n").replace("\r", "\n")

    @classmethod
    def _split_token_parts(cls, token: str) -> tuple[str, str, str] | None:
        if not token:
            return None
        match = cls.TOKEN_PARTS_RE.match(token)
        if not match:
            return None
        return match.group(1), match.group(2), match.group(3)

    @staticmethod
    def _needs_leading_blank(line: str, previous: str) -> bool:
        return bool(previous) and is_top_level(line)

    @staticmethod
    def _starts_with_lowercase(line: str) -> bool:
        for char in line.lstrip():
            if char.isalpha():
                return char.islower()
        return False

    @staticmethod
    def _is_uppercase_heading(line: str) -> bool:
        letters = [char for char in line if char.isalpha()]
        return bool(letters) and line == line.upper()

    @staticmethod
    def _ends_with_split_proper_noun(line: str) -> bool:
        parts = line.split()
        if not parts:
            return False
        last_word = parts[-1]
        return len(last_word) <= 6 and last_word[:1].isupper() and not line.endswith((".", "!", "?"))

    @staticmethod
    def _normalize_ascii(value: str) -> str:
        normalized = unicodedata.normalize("NFD", value or "")
        ascii_only = "".join(char for char in normalized if unicodedata.category(char) != "Mn")
        return ascii_only.replace("đ", "d").replace("Đ", "d").lower()
