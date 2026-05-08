from __future__ import annotations

import logging
import math
import re
import unicodedata

import spacy
import tiktoken

from ..legal_text_patterns import (
    ARTICLE_PATTERN,
    CLAUSE_PATTERN,
    extract_article_number,
    extract_article_title,
)
from ..schemas.models import ArticleBlock, ChunkingConfig, LegalChunk


logger = logging.getLogger(__name__)


class VietnameseLegalChunker:
    def __init__(self, config: ChunkingConfig):
        self.config = config
        self.tokenizer = tiktoken.encoding_for_model("gpt-4")
        self.nlp = spacy.blank("xx")
        self.nlp.add_pipe("sentencizer")
        self.nlp.max_length = 5_000_000

    def count_tokens(self, text: str) -> int:
        """Accurate token count - no character-based estimation."""

        if not text:
            return 0
        return len(self.tokenizer.encode(text, disallowed_special=()))

    @property
    def raw_min_tokens(self) -> int:
        return max(1, self.config.target_min_tokens - self.config.overlap_tokens)

    @property
    def raw_max_tokens(self) -> int:
        safety_margin = max(50, self.config.overlap_tokens)
        return max(self.raw_min_tokens, self.config.target_max_tokens - self.config.overlap_tokens - safety_margin)

    @property
    def packing_unit_max(self) -> int:
        return min(self.raw_max_tokens, max(350, self.raw_min_tokens // 2))

    def extract_articles(self, full_text: str) -> list[ArticleBlock]:
        """Split a cleaned legal document into article blocks."""

        normalized_text = (full_text or "").strip()
        if not normalized_text:
            return []

        article_matches = list(ARTICLE_PATTERN.finditer(normalized_text))
        if not article_matches:
            return [
                ArticleBlock(
                    number=0,
                    title="",
                    full_text=normalized_text,
                    token_count=self.count_tokens(normalized_text),
                )
            ]

        articles: list[ArticleBlock] = []
        for index, match in enumerate(article_matches):
            start = match.start()
            end = article_matches[index + 1].start() if index + 1 < len(article_matches) else len(normalized_text)
            article_text = normalized_text[start:end].strip()
            header = match.group(1).strip()
            articles.append(
                ArticleBlock(
                    number=extract_article_number(header) or 0,
                    title=extract_article_title(header),
                    full_text=article_text,
                    token_count=self.count_tokens(article_text),
                )
            )

        return articles

    def merge_short_articles(self, articles: list[ArticleBlock]) -> list[list[ArticleBlock]]:
        """Merge adjacent short articles until the minimum target is reached."""

        if not articles:
            return []

        groups: list[list[ArticleBlock]] = []
        current_group: list[ArticleBlock] = []
        current_tokens = 0

        for article in articles:
            article_tokens = article.token_count or self.count_tokens(article.full_text)

            if article_tokens >= self.raw_min_tokens:
                if current_group:
                    groups.append(current_group)
                    current_group = []
                    current_tokens = 0
                groups.append([article])
                continue

            if (
                current_group
                and current_tokens >= self.raw_min_tokens
                and current_tokens + article_tokens > self.raw_max_tokens
            ):
                groups.append(current_group)
                current_group = []
                current_tokens = 0

            current_group.append(article)
            current_tokens += article_tokens

            if current_tokens >= self.raw_min_tokens:
                groups.append(current_group)
                current_group = []
                current_tokens = 0

        if current_group:
            if groups:
                last_group_tokens = sum(item.token_count or self.count_tokens(item.full_text) for item in groups[-1])
                if last_group_tokens + current_tokens <= self.raw_max_tokens:
                    groups[-1].extend(current_group)
                else:
                    groups.append(current_group)
            else:
                groups.append(current_group)

        return groups

    def split_article_by_clauses(self, article: ArticleBlock) -> list[str]:
        """Split a long article by clause boundaries, with sentence fallback."""

        text = article.full_text.strip()
        if not text:
            return []
        if article.token_count and article.token_count <= self.raw_max_tokens:
            return [text]

        clause_matches = list(CLAUSE_PATTERN.finditer(text))
        if len(clause_matches) <= 1:
            return self._split_text_by_sentences(text)

        header = text[: clause_matches[0].start()].strip()
        segments = []
        for index, match in enumerate(clause_matches):
            start = match.start()
            end = clause_matches[index + 1].start() if index + 1 < len(clause_matches) else len(text)
            segments.append(text[start:end].strip())

        clause_chunks = self._group_segments_with_header(header, segments, text)
        if self._should_fallback_to_sentence_split(clause_chunks):
            fallback_header, fallback_body = self._split_header_and_body(text)
            return self._split_text_by_token_windows(fallback_header, fallback_body)
        return clause_chunks

    def _get_overlap_text(self, text: str, from_end: bool = True) -> str:
        """Return sentence-aligned overlap text using accurate token counting."""

        stripped = (text or "").strip()
        if not stripped or self.config.overlap_tokens <= 0:
            return ""
        if self.count_tokens(stripped) <= self.config.overlap_tokens:
            return stripped

        sentences = [sent.text.strip() for sent in self.nlp(stripped).sents if sent.text.strip()]
        if not sentences:
            tokens = self.tokenizer.encode(stripped, disallowed_special=())
            window = tokens[-self.config.overlap_tokens :] if from_end else tokens[: self.config.overlap_tokens]
            return self.tokenizer.decode(window).strip()

        overlap_sentences: list[str] = []
        total_tokens = 0
        iterable = reversed(sentences) if from_end else sentences

        for sentence in iterable:
            sentence_tokens = self.count_tokens(sentence)
            if total_tokens + sentence_tokens > self.config.overlap_tokens and overlap_sentences:
                break
            total_tokens += sentence_tokens
            if from_end:
                overlap_sentences.insert(0, sentence)
            else:
                overlap_sentences.append(sentence)

        return " ".join(overlap_sentences).strip()

    def chunk_document(self, law_name: str, full_text: str, law_id: str = "") -> list[LegalChunk]:
        """Chunk a cleaned law document into semantic chunks for retrieval."""

        articles = self.extract_articles(full_text)
        raw_candidates: list[dict[str, object]] = []

        for article in articles:
            raw_candidates.extend(self._build_raw_chunks([article], law_id))

        raw_chunks = self._pack_raw_chunks(raw_candidates)

        law_prefix = self._make_law_prefix(law_name)
        chunks: list[LegalChunk] = []

        for index, chunk_data in enumerate(raw_chunks):
            text = str(chunk_data["text"]).strip()
            has_overlap = False

            if index > 0:
                overlap_text = self._get_overlap_text(str(raw_chunks[index - 1]["text"]), from_end=True)
                if overlap_text:
                    text = f"{overlap_text}\n\n{text}"
                    has_overlap = True

            chunk_id = self._make_chunk_id(
                law_prefix=law_prefix,
                article_numbers=list(chunk_data["article_numbers"]),
                part_index=int(chunk_data["part_index"]),
            )
            chunk = LegalChunk(
                chunk_id=chunk_id,
                law_id=law_id,
                law_name=law_name,
                article_ids=list(chunk_data["article_ids"]),
                article_titles=list(chunk_data["article_titles"]),
                text=text,
                token_count=self.count_tokens(text),
                char_count=len(text),
                has_overlap=has_overlap,
            )
            chunks.append(chunk)

        for index, chunk in enumerate(chunks):
            chunk.prev_chunk_id = chunks[index - 1].chunk_id if index > 0 else None
            chunk.next_chunk_id = chunks[index + 1].chunk_id if index + 1 < len(chunks) else None

        return chunks

    def _build_raw_chunks(self, group: list[ArticleBlock], law_id: str) -> list[dict[str, object]]:
        """Create raw chunk payloads before overlap and linking are applied."""

        group_text = "\n\n".join(article.full_text for article in group).strip()
        group_tokens = self.count_tokens(group_text)

        if len(group) == 1 and group_tokens > self.packing_unit_max:
            article = group[0]
            sub_chunks = self._split_article_for_packing(article)
            return [
                {
                    "text": sub_text,
                    "token_count": self.count_tokens(sub_text),
                    "article_numbers": [article.number],
                    "article_ids": [self._build_article_id(law_id, article.number)],
                    "article_titles": [article.title],
                    "part_index": index if len(sub_chunks) > 1 else 0,
                }
                for index, sub_text in enumerate(sub_chunks, start=1)
            ]

        if group_tokens <= self.raw_max_tokens:
            return [
                {
                    "text": group_text,
                    "token_count": group_tokens,
                    "article_numbers": [article.number for article in group],
                    "article_ids": [self._build_article_id(law_id, article.number) for article in group],
                    "article_titles": [article.title for article in group],
                    "part_index": 0,
                }
            ]

        raw_chunks: list[dict[str, object]] = []
        current_group: list[ArticleBlock] = []
        for article in group:
            candidate_group = current_group + [article]
            candidate_text = "\n\n".join(item.full_text for item in candidate_group).strip()
            if current_group and self.count_tokens(candidate_text) > self.raw_max_tokens:
                raw_chunks.append(self._build_group_chunk(current_group, law_id))
                current_group = [article]
                continue
            current_group = candidate_group

        if current_group:
            if len(current_group) == 1 and self.count_tokens(current_group[0].full_text) > self.raw_max_tokens:
                raw_chunks.extend(self._build_raw_chunks(current_group, law_id))
            else:
                raw_chunks.append(self._build_group_chunk(current_group, law_id))

        return raw_chunks

    def _build_group_chunk(self, group: list[ArticleBlock], law_id: str) -> dict[str, object]:
        text = "\n\n".join(article.full_text for article in group).strip()
        return {
            "text": text,
            "token_count": self.count_tokens(text),
            "article_numbers": [article.number for article in group],
            "article_ids": [self._build_article_id(law_id, article.number) for article in group],
            "article_titles": [article.title for article in group],
            "part_index": 0,
        }

    def _split_article_for_packing(self, article: ArticleBlock) -> list[str]:
        if article.token_count <= self.packing_unit_max:
            return [article.full_text.strip()]

        clause_parts = self.split_article_by_clauses(article)
        packed_parts: list[str] = []

        for part in clause_parts:
            if self.count_tokens(part) <= self.packing_unit_max:
                packed_parts.append(part)
                continue

            header, body = self._split_header_and_body(part)
            packed_parts.extend(self._split_text_by_token_windows(header, body, max_tokens=self.packing_unit_max))

        return [part for part in packed_parts if part.strip()]

    def _split_text_by_sentences(self, text: str) -> list[str]:
        """Split a long text by sentence boundaries, preserving the article header."""

        stripped = (text or "").strip()
        if not stripped:
            return []
        if self.count_tokens(stripped) <= self.raw_max_tokens:
            return [stripped]

        header, body = self._split_header_and_body(stripped)
        if not body:
            return self._split_text_by_token_windows(header, "")

        sentences = [sent.text.strip() for sent in self.nlp(body).sents if sent.text.strip()]
        if not sentences:
            return self._split_text_by_token_windows(header, body)

        chunks: list[str] = []
        current_segments: list[str] = []

        for sentence in sentences:
            single_sentence_chunk = self._compose_chunk_text(header, [sentence])
            if self.count_tokens(single_sentence_chunk) > self.raw_max_tokens:
                if current_segments:
                    chunks.append(self._compose_chunk_text(header, current_segments))
                    current_segments = []
                chunks.extend(self._split_text_by_token_windows(header, sentence))
                continue

            candidate = self._compose_chunk_text(header, current_segments + [sentence])
            if current_segments and self.count_tokens(candidate) > self.raw_max_tokens:
                chunks.append(self._compose_chunk_text(header, current_segments))
                current_segments = [sentence]
            else:
                current_segments.append(sentence)

        if current_segments:
            chunks.append(self._compose_chunk_text(header, current_segments))

        return [chunk for chunk in chunks if chunk.strip()]

    def _group_segments_with_header(self, header: str, segments: list[str], fallback_text: str) -> list[str]:
        """Group segments under a shared header without exceeding the max target."""

        if not segments:
            return self._split_text_by_sentences(fallback_text)

        chunks: list[str] = []
        current_segments: list[str] = []

        for segment in segments:
            single_segment_chunk = self._compose_chunk_text(header, [segment])
            if self.count_tokens(single_segment_chunk) > self.raw_max_tokens:
                if current_segments:
                    chunks.append(self._compose_chunk_text(header, current_segments))
                    current_segments = []
                chunks.extend(self._split_text_by_sentences(single_segment_chunk))
                continue

            candidate = self._compose_chunk_text(header, current_segments + [segment])
            if current_segments and self.count_tokens(candidate) > self.raw_max_tokens:
                chunks.append(self._compose_chunk_text(header, current_segments))
                current_segments = [segment]
            else:
                current_segments.append(segment)

        if current_segments:
            chunks.append(self._compose_chunk_text(header, current_segments))

        return [chunk for chunk in chunks if chunk.strip()] or self._split_text_by_sentences(fallback_text)

    def _split_text_by_token_windows(self, header: str, body: str, max_tokens: int | None = None) -> list[str]:
        """Fallback splitter for texts that remain too long after sentence parsing."""

        max_tokens = max_tokens or self.raw_max_tokens
        header = (header or "").strip()
        body = (body or "").strip()
        if not body:
            encoded = self.tokenizer.encode(header, disallowed_special=())
            window_count = max(1, math.ceil(len(encoded) / max_tokens))
            base_size, remainder = divmod(len(encoded), window_count)
            windows = []
            cursor = 0
            for index in range(window_count):
                size = base_size + (1 if index < remainder else 0)
                windows.append(encoded[cursor : cursor + size])
                cursor += size
            return [self.tokenizer.decode(window).strip() for window in windows if window]

        header_tokens = self.tokenizer.encode(header, disallowed_special=()) if header else []
        body_tokens = self.tokenizer.encode(body, disallowed_special=())
        available_tokens = max_tokens - len(header_tokens)
        window_capacity = max(1, available_tokens - 1) if header_tokens else max_tokens
        window_count = max(1, math.ceil(len(body_tokens) / window_capacity))
        base_size, remainder = divmod(len(body_tokens), window_count)

        windows = []
        cursor = 0
        for index in range(window_count):
            size = base_size + (1 if index < remainder else 0)
            body_chunk = self.tokenizer.decode(body_tokens[cursor : cursor + size]).strip()
            cursor += size
            windows.append(self._compose_chunk_text(header, [body_chunk]))
        return [window for window in windows if window.strip()]

    def _should_fallback_to_sentence_split(self, chunks: list[str]) -> bool:
        if len(chunks) <= 1:
            return False

        token_counts = [self.count_tokens(chunk) for chunk in chunks]
        in_range = sum(self.raw_min_tokens <= count <= self.raw_max_tokens for count in token_counts)
        compliance = in_range / len(token_counts)
        minimum_reasonable = max(50, int(self.raw_min_tokens * 0.7))
        return compliance < 0.75 or min(token_counts) < minimum_reasonable

    def _pack_raw_chunks(self, raw_chunks: list[dict[str, object]]) -> list[dict[str, object]]:
        """Pack sequential raw units toward the target window before overlap is added."""

        if not raw_chunks:
            return []

        packed: list[dict[str, object]] = []
        current = raw_chunks[0].copy()

        for candidate in raw_chunks[1:]:
            combined = self._merge_raw_chunk_dicts(current, candidate)
            combined_tokens = int(combined["token_count"])
            current_tokens = int(current["token_count"])

            if combined_tokens <= self.raw_max_tokens:
                current = combined
                continue

            if current_tokens < self.raw_min_tokens and not packed:
                first_chunk_limit = self.config.target_max_tokens
                if combined_tokens <= first_chunk_limit:
                    current = combined
                    continue

            packed.append(current)
            current = candidate.copy()

        if packed and int(current["token_count"]) < self.raw_min_tokens:
            trailing_merge = self._merge_raw_chunk_dicts(packed[-1], current)
            if int(trailing_merge["token_count"]) <= self.config.target_max_tokens:
                packed[-1] = trailing_merge
            else:
                packed.append(current)
        else:
            packed.append(current)

        return packed

    def _merge_raw_chunk_dicts(self, left: dict[str, object], right: dict[str, object]) -> dict[str, object]:
        text = f"{str(left['text']).strip()}\n\n{str(right['text']).strip()}".strip()
        article_numbers = list(left["article_numbers"]) + list(right["article_numbers"])
        article_ids = list(dict.fromkeys(list(left["article_ids"]) + list(right["article_ids"])))
        article_titles = list(dict.fromkeys(list(left["article_titles"]) + list(right["article_titles"])))
        return {
            "text": text,
            "token_count": self.count_tokens(text),
            "article_numbers": article_numbers,
            "article_ids": article_ids,
            "article_titles": article_titles,
            "part_index": 0,
        }

    @staticmethod
    def _split_header_and_body(text: str) -> tuple[str, str]:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if not lines:
            return "", ""
        if len(lines) == 1:
            return lines[0], ""
        return lines[0], "\n".join(lines[1:]).strip()

    @staticmethod
    def _compose_chunk_text(header: str, segments: list[str]) -> str:
        parts = [header] if header else []
        parts.extend(segment.strip() for segment in segments if segment and segment.strip())
        return "\n".join(parts).strip()

    @staticmethod
    def _build_article_id(law_id: str, article_number: int) -> str:
        base = (law_id or "law").strip() or "law"
        return f"{base}__{article_number}"

    def _make_chunk_id(self, law_prefix: str, article_numbers: list[int], part_index: int = 0) -> str:
        if article_numbers and article_numbers[0] > 0:
            if len(article_numbers) == 1:
                article_part = f"dieu{article_numbers[0]}"
            else:
                article_part = f"dieu{article_numbers[0]}_dieu{article_numbers[-1]}"
            if part_index > 0:
                article_part = f"{article_part}_p{part_index}"
            return f"{law_prefix}_{article_part}"
        return f"{law_prefix}_chunk{part_index or 1}"

    @staticmethod
    def _make_law_prefix(law_name: str) -> str:
        simplified = unicodedata.normalize("NFD", law_name or "")
        simplified = "".join(char for char in simplified if unicodedata.category(char) != "Mn")
        simplified = simplified.replace("đ", "d").replace("Đ", "D")
        simplified = re.sub(r"\([^)]*\)", " ", simplified)
        simplified = re.sub(r"\d+", " ", simplified)
        words = re.findall(r"[A-Za-z]+", simplified.lower())
        prefix = "".join(word[0] for word in words[:6])
        return prefix or "law"
