# ============================================================================
# VIET-CONTRACT AUDITOR - Phase 2
# Module: semantic_chunker.py
# Mô tả: Bộ tách văn bản luật Việt Nam theo cấu trúc ngữ nghĩa (semantic).
#         Sử dụng Regex để nhận diện cấu trúc phân cấp của văn bản luật VN
#         và spaCy để xử lý sentence boundary detection.
# ============================================================================
#
# === GIẢI THÍCH CẤU TRÚC VĂN BẢN LUẬT VIỆT NAM ===
#
# Văn bản luật VN có cấu trúc phân cấp nghiêm ngặt:
#
#   Phần    (Part)        -> Phần thứ nhất, Phần thứ hai, ...
#   Chương  (Chapter)     -> Chương I, Chương II, Chương III, ...
#   Mục     (Section)     -> Mục 1, Mục 2, ...
#   Điều    (Article)     -> Điều 1, Điều 2, ... Điều 689
#   Khoản   (Clause)      -> 1., 2., 3., ... (số + dấu chấm ở đầu dòng)
#   Điểm    (Point)       -> a), b), c), ... (chữ cái + dấu ngoặc)
#
# Nguyên tắc chunking:
# - Đơn vị chunk chính: MỖI "Điều" (Article) = 1 chunk
# - Nếu 1 Điều quá dài (>1200 tokens): Tách tiếp theo "Khoản" (Clause)
# - Nếu 1 Điều quá ngắn (<200 tokens): Gộp với Điều liền kề
# - Overlap: ~100 tokens từ cuối chunk trước sang đầu chunk sau
# ============================================================================

import re
import logging
from dataclasses import dataclass, field

import spacy

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# REGEX PATTERNS CHO CẤU TRÚC LUẬT VIỆT NAM
# ---------------------------------------------------------------------------

# Pattern 1: Nhận diện "Điều X." hoặc "Điều X:" (Article)
# -------------------------------------------------------
# Giải thích regex: r"^(Điều\s+\d+[\.:]\s*.*)"
#   ^           : Bắt đầu dòng (với re.MULTILINE)
#   Điều        : Literal "Điều" (từ khóa article trong luật VN)
#   \s+         : 1+ khoảng trắng
#   \d+         : 1+ chữ số (số điều: 1, 2, ..., 689)
#   [\.:]       : Dấu chấm hoặc dấu hai chấm kết thúc số điều
#   \s*         : 0+ khoảng trắng
#   (.*)        : Phần còn lại = tiêu đề của Điều
ARTICLE_PATTERN = re.compile(
    r"^(Điều\s+\d+[\.:]\s*.*)",
    re.MULTILINE | re.UNICODE
)

# Pattern 2: Nhận diện "Khoản" = số + dấu chấm ở đầu dòng (Clause)
# -------------------------------------------------------------------
# Giải thích regex: r"^\s*(\d+)\.\s+"
#   ^           : Bắt đầu dòng
#   \s*         : 0+ khoảng trắng (indent)
#   (\d+)       : Capture group - số khoản (1, 2, 3, ...)
#   \.          : Dấu chấm sau số
#   \s+         : 1+ khoảng trắng sau dấu chấm
CLAUSE_PATTERN = re.compile(
    r"^\s*(\d+)\.\s+",
    re.MULTILINE | re.UNICODE
)

# Pattern 3: Nhận diện "Điểm" = chữ cái + dấu ngoặc đơn (Point)
# ----------------------------------------------------------------
# Giải thích regex: r"^\s*([a-zđ])\)\s+"
#   ^           : Bắt đầu dòng
#   \s*         : 0+ khoảng trắng (indent)
#   ([a-zđ])    : Capture - chữ cái thường (a, b, c, ... đ)
#   \)          : Dấu ngoặc đóng
#   \s+         : 1+ khoảng trắng
POINT_PATTERN = re.compile(
    r"^\s*([a-zđ])\)\s+",
    re.MULTILINE | re.UNICODE
)

# Pattern 4: Nhận diện "Chương" (Chapter) - dùng cho metadata
# ------------------------------------------------------------
# Giải thích: "Chương" + space + số La Mã hoặc số thường
CHAPTER_PATTERN = re.compile(
    r"^(Chương\s+[IVXLCDM\d]+[\.:]*\s*.*)",
    re.MULTILINE | re.UNICODE
)

# Pattern 5: Nhận diện "Mục" (Section) - dùng cho metadata
# ----------------------------------------------------------
SECTION_PATTERN = re.compile(
    r"^(Mục\s+\d+[\.:]*\s*.*)",
    re.MULTILINE | re.UNICODE
)


# ---------------------------------------------------------------------------
# DATA CLASSES
# ---------------------------------------------------------------------------

@dataclass
class LegalChunk:
    """Đại diện cho 1 chunk văn bản luật đã tách."""
    document_name: str            # Tên văn bản gốc (e.g., "Bộ luật Dân sự 2015")
    chunk_id: str                 # ID duy nhất (e.g., "blds2015_dieu_001")
    text: str                     # Nội dung chunk
    article_number: int = 0       # Số Điều (nếu có)
    article_title: str = ""       # Tiêu đề Điều (nếu có)
    chapter_context: str = ""     # Chương chứa Điều này
    section_context: str = ""     # Mục chứa Điều này
    token_count: int = 0          # Số token ước tính
    has_overlap: bool = False     # Chunk này có chứa overlap không
    source_law_id: str = ""       # law_id nguồn (e.g., "91/2015/qh13")


@dataclass
class ArticleBlock:
    """Khối trung gian: 1 Điều hoàn chỉnh trước khi chunking."""
    number: int                   # Số Điều
    title: str                    # Tiêu đề Điều
    full_text: str                # Toàn bộ nội dung Điều
    chapter: str = ""             # Chương chứa
    section: str = ""             # Mục chứa
    char_count: int = 0           # Độ dài ký tự

    def __post_init__(self):
        self.char_count = len(self.full_text)


# ---------------------------------------------------------------------------
# VIETNAMESE LEGAL SEMANTIC CHUNKER
# ---------------------------------------------------------------------------

class VietnameseLegalChunker:
    """
    Bộ tách văn bản luật Việt Nam theo cấu trúc ngữ nghĩa.

    Workflow:
    1. Tách văn bản thành các "Điều" (Article) blocks
    2. Với mỗi Điều:
       - Nếu kích thước OK (800-1200 tokens) -> giữ nguyên = 1 chunk
       - Nếu quá dài -> tách theo "Khoản" (Clause)
       - Nếu quá ngắn -> gộp với Điều sau
    3. Thêm overlap giữa các chunks liền kề
    """

    def __init__(
        self,
        target_min_tokens: int = 800,
        target_max_tokens: int = 1200,
        overlap_tokens: int = 100,
        chars_per_token: float = 3.5,   # Tiếng Việt ~3.5 ký tự/token (ước lượng)
    ):
        """
        Args:
            target_min_tokens: Số token tối thiểu mỗi chunk.
            target_max_tokens: Số token tối đa mỗi chunk.
            overlap_tokens: Số token overlap giữa 2 chunk liền kề.
            chars_per_token: Tỷ lệ kcự tự/token (dùng để ước lượng).
                             Tiếng Việt: ~3-4 chars/token tùy LLM tokenizer.
        """
        self.target_min_tokens = target_min_tokens
        self.target_max_tokens = target_max_tokens
        self.overlap_tokens = overlap_tokens
        self.chars_per_token = chars_per_token

        # Tính ngưỡng theo ký tự (chars) để so sánh nhanh
        self._min_chars = int(target_min_tokens * chars_per_token)
        self._max_chars = int(target_max_tokens * chars_per_token)
        self._overlap_chars = int(overlap_tokens * chars_per_token)

        # Khởi tạo spaCy với blank MULTILINGUAL model ('xx')
        # Lý do dùng 'xx' thay vì 'vi': spacy.blank('vi') yêu cầu thư viện
        # 'pyvi' (tokenizer tiếng Việt) gây lỗi ImportError khi chưa cài.
        # 'xx' (multilingual) là blank model không cần dependency ngoài,
        # hoạt động tốt cho sentence boundary detection dựa trên dấu câu.
        self._nlp = spacy.blank("xx")
        # Sentencizer rule-based: tách câu theo dấu . ? ! - không cần ML model
        self._nlp.add_pipe("sentencizer")
        # Tăng giới hạn ký tự (văn bản luật BLDS 2015 ~700K ký tự)
        self._nlp.max_length = 5_000_000

        logger.info(
            f"Khởi tạo VietnameseLegalChunker: "
            f"target=[{target_min_tokens}-{target_max_tokens}] tokens, "
            f"overlap={overlap_tokens} tokens, "
            f"~chars=[{self._min_chars}-{self._max_chars}]"
        )

    def estimate_tokens(self, text: str) -> int:
        """Ước lượng số token từ độ dài ký tự."""
        return int(len(text) / self.chars_per_token)

    # -------------------------------------------------------------------
    # BƯỚC 1: TÁCH VĂN BẢN THÀNH CÁC "ĐIỀU" (ARTICLE) BLOCKS
    # -------------------------------------------------------------------

    def extract_articles(self, full_text: str) -> list[ArticleBlock]:
        """
        Tách toàn bộ văn bản luật thành các block theo "Điều".

        Thuật toán:
        1. Tìm tất cả vị trí "Điều X." trong text
        2. Text giữa 2 vị trí "Điều" liên tiếp = nội dung của Điều trước
        3. Theo dõi Chương/Mục hiện tại để gắn metadata

        Args:
            full_text: Toàn bộ nội dung 1 văn bản luật.

        Returns:
            Danh sách ArticleBlock.
        """
        articles: list[ArticleBlock] = []

        # Tìm tất cả match của "Điều X." kèm vị trí
        article_matches = list(ARTICLE_PATTERN.finditer(full_text))

        if not article_matches:
            logger.warning("Không tìm thấy 'Điều' nào trong văn bản!")
            # Fallback: trả về toàn bộ text như 1 block
            return [ArticleBlock(number=0, title="(Toàn văn)", full_text=full_text)]

        logger.info(f"Tìm thấy {len(article_matches)} Điều trong văn bản")

        # Theo dõi Chương và Mục hiện tại
        current_chapter = ""
        current_section = ""

        # Tìm tất cả Chương và Mục với vị trí
        chapter_positions = [
            (m.start(), m.group(1).strip())
            for m in CHAPTER_PATTERN.finditer(full_text)
        ]
        section_positions = [
            (m.start(), m.group(1).strip())
            for m in SECTION_PATTERN.finditer(full_text)
        ]

        for i, match in enumerate(article_matches):
            # --- Trích số Điều và tiêu đề ---
            header_line = match.group(1).strip()
            article_num_match = re.search(r"Điều\s+(\d+)", header_line)
            article_num = int(article_num_match.group(1)) if article_num_match else i + 1

            # Tiêu đề = phần sau "Điều X." (nếu có)
            title_match = re.search(r"Điều\s+\d+[\.:]\s*(.*)", header_line)
            article_title = title_match.group(1).strip() if title_match else ""

            # --- Trích nội dung Điều ---
            start_pos = match.start()
            # End = vị trí bắt đầu của Điều tiếp theo (hoặc cuối text)
            end_pos = (
                article_matches[i + 1].start()
                if i + 1 < len(article_matches)
                else len(full_text)
            )
            article_text = full_text[start_pos:end_pos].strip()

            # --- Xác định Chương/Mục chứa Điều này ---
            for pos, chap_name in reversed(chapter_positions):
                if pos <= start_pos:
                    current_chapter = chap_name
                    break

            for pos, sec_name in reversed(section_positions):
                if pos <= start_pos:
                    current_section = sec_name
                    break

            articles.append(ArticleBlock(
                number=article_num,
                title=article_title,
                full_text=article_text,
                chapter=current_chapter,
                section=current_section,
            ))

        return articles

    # -------------------------------------------------------------------
    # BƯỚC 2: TÁCH ĐIỀU QUÁ DÀI THEO KHOẢN
    # -------------------------------------------------------------------

    def split_article_by_clauses(self, article: ArticleBlock) -> list[str]:
        """
        Tách 1 Điều dài thành các phần nhỏ hơn theo ranh giới "Khoản".

        Thuật toán:
        1. Tìm tất cả vị trí "X." (Khoản) trong Điều
        2. Gộp các Khoản liên tiếp cho đến khi đạt ngưỡng max_chars
        3. Mỗi nhóm Khoản = 1 sub-chunk

        Args:
            article: ArticleBlock cần tách.

        Returns:
            List các đoạn text đã tách.
        """
        text = article.full_text

        # Tìm tất cả vị trí bắt đầu Khoản
        clause_matches = list(CLAUSE_PATTERN.finditer(text))

        if len(clause_matches) <= 1:
            # Không đủ Khoản để tách -> giữ nguyên
            return [text]

        # Tạo danh sách các segment (mỗi segment = 1 Khoản)
        segments: list[str] = []

        # Header = phần trước Khoản đầu tiên (dòng "Điều X. Title")
        header = text[: clause_matches[0].start()].strip()

        for i, match in enumerate(clause_matches):
            start = match.start()
            end = (
                clause_matches[i + 1].start()
                if i + 1 < len(clause_matches)
                else len(text)
            )
            segments.append(text[start:end].strip())

        # Gộp các Khoản thành sub-chunks đạt kích thước mục tiêu
        sub_chunks: list[str] = []  
        current_group: list[str] = []
        current_len = len(header)

        for segment in segments:
            seg_len = len(segment)

            if current_len + seg_len > self._max_chars and current_group:
                # Đã đạt ngưỡng -> tạo sub-chunk
                chunk_text = header + "\n" + "\n".join(current_group)
                sub_chunks.append(chunk_text.strip())
                current_group = [segment]
                current_len = len(header) + seg_len
            else:
                current_group.append(segment)
                current_len += seg_len

        
        if current_group:
            chunk_text = header + "\n" + "\n".join(current_group)
            sub_chunks.append(chunk_text.strip())

        return sub_chunks

    # -------------------------------------------------------------------
    # BƯỚC 3: GỘP CÁC ĐIỀU QUÁ NGẮN
    # -------------------------------------------------------------------

    def merge_short_articles(self, articles: list[ArticleBlock]) -> list[list[ArticleBlock]]:
        """
        Gộp các Điều quá ngắn lại với nhau.

        Thuật toán:
        - Duyệt tuần tự qua các Điều
        - Gộp các Điều liên tiếp cho đến khi tổng chars >= min_chars
        - Chỉ gộp các Điều cùng Chương

        Args:
            articles: Danh sách ArticleBlock.

        Returns:
            List các nhóm ArticleBlock.
        """
        groups: list[list[ArticleBlock]] = []
        current_group: list[ArticleBlock] = []
        current_chars = 0

        for article in articles:
            # Nếu article quá dài, nó sẽ tự thành 1 nhóm riêng
            if article.char_count >= self._min_chars:
                # Flush nhóm hiện tại nếu có
                if current_group:
                    groups.append(current_group)
                    current_group = []
                    current_chars = 0
                groups.append([article])
            else:
                # Article ngắn -> gộp vào nhóm hiện tại
                # Kiểm tra cùng Chương
                if (current_group and
                    current_group[-1].chapter != article.chapter and
                    current_chars >= self._min_chars):
                    # Khác Chương và nhóm hiện tại đủ lớn -> flush
                    groups.append(current_group)
                    current_group = []
                    current_chars = 0

                current_group.append(article)
                current_chars += article.char_count

                # Kiểm tra đã đủ lớn chưa
                if current_chars >= self._min_chars:
                    groups.append(current_group)
                    current_group = []
                    current_chars = 0

        # Phần còn lại
        if current_group:
            if groups:
                # Gộp vào nhóm cuối cùng nếu quá nhỏ
                groups[-1].extend(current_group)
            else:
                groups.append(current_group)

        return groups

    # -------------------------------------------------------------------
    # BƯỚC 4: TẠO OVERLAP GIỮA CÁC CHUNKS
    # -------------------------------------------------------------------

    def _get_overlap_text(self, text: str, from_end: bool = True) -> str:
        """
        Trích đoạn overlap từ đầu hoặc cuối của text.
        Sử dụng spaCy sentencizer để đảm bảo KHÔNG cắt giữa câu.

        Args:
            text: Văn bản nguồn.
            from_end: True = lấy từ cuối text, False = lấy từ đầu.

        Returns:
            Đoạn text overlap (trọn câu).
        """
        if len(text) <= self._overlap_chars:
            return text

        # Dùng spaCy để tách câu
        doc = self._nlp(text)
        sentences = list(doc.sents)

        if not sentences:
            # Fallback: cắt theo ký tự
            if from_end:
                return text[-self._overlap_chars:]
            return text[: self._overlap_chars]

        # Tích lũy câu cho đến khi đạt overlap_chars
        overlap_sentences: list[str] = []
        total_len = 0

        if from_end:
            # Lấy từ cuối text
            for sent in reversed(sentences):
                sent_text = sent.text.strip()
                if total_len + len(sent_text) > self._overlap_chars and overlap_sentences:
                    break
                overlap_sentences.insert(0, sent_text)
                total_len += len(sent_text)
        else:
            # Lấy từ đầu text
            for sent in sentences:
                sent_text = sent.text.strip()
                if total_len + len(sent_text) > self._overlap_chars and overlap_sentences:
                    break
                overlap_sentences.append(sent_text)
                total_len += len(sent_text)

        return " ".join(overlap_sentences)

    # -------------------------------------------------------------------
    # BƯỚC 5: PIPELINE CHÍNH - CHUNK TOÀN BỘ VĂN BẢN
    # -------------------------------------------------------------------

    def chunk_document(
        self, document_name: str, full_text: str, law_id: str = ""
    ) -> list[LegalChunk]:
        """
        Pipeline chính: Tách 1 văn bản luật thành các chunks ngữ nghĩa.

        Flow:
        1. extract_articles() -> Tách theo Điều
        2. merge_short_articles() -> Gộp Điều ngắn
        3. split_article_by_clauses() -> Tách Điều dài
        4. Thêm overlap
        5. Tạo LegalChunk objects

        Args:
            document_name: Tên văn bản (e.g., "Bộ luật Dân sự 2015").
            full_text: Toàn bộ nội dung văn bản.
            law_id: Mã số văn bản để gắn vào metadata mỗi chunk.

        Returns:
            Danh sách LegalChunk.
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"CHUNKING: {document_name}")
        logger.info(f"Độ dài gốc: {len(full_text):,} ký tự")
        logger.info(f"{'='*60}")

        # === BƯỚC 1: Tách theo Điều ===
        articles = self.extract_articles(full_text)
        logger.info(f"Bước 1 - Tách Điều: {len(articles)} Điều")

        # === BƯỚC 2: Gộp Điều ngắn ===
        article_groups = self.merge_short_articles(articles)
        logger.info(f"Bước 2 - Gộp Điều ngắn: {len(article_groups)} nhóm")

        # === BƯỚC 3: Tạo raw chunks (tách Điều dài) ===
        raw_chunks: list[dict] = []

        for group in article_groups:
            # Gộp text của nhóm
            group_text = "\n\n".join(a.full_text for a in group)
            group_articles = [a.number for a in group]
            group_chapter = group[0].chapter
            group_section = group[0].section

            # Kiểm tra kích thước
            if len(group_text) > self._max_chars:
                # Quá dài -> tách theo Khoản (chỉ tách article đầu tiên trong nhóm)
                # Thực tế nhóm dài thường chỉ có 1 article
                for article in group:
                    sub_chunks = self.split_article_by_clauses(article)
                    for j, sub_text in enumerate(sub_chunks):
                        raw_chunks.append({
                            "text": sub_text,
                            "articles": [article.number],
                            "chapter": article.chapter,
                            "section": article.section,
                            "title": article.title,
                            "sub_part": j + 1 if len(sub_chunks) > 1 else 0,
                        })
            else:
                raw_chunks.append({
                    "text": group_text,
                    "articles": group_articles,
                    "chapter": group_chapter,
                    "section": group_section,
                    "title": group[0].title if len(group) == 1 else "",
                    "sub_part": 0,
                })

        logger.info(f"Bước 3 - Raw chunks: {len(raw_chunks)} chunks")

        # === BƯỚC 4: Thêm overlap ===
        final_chunks: list[LegalChunk] = []

        # Tạo prefix ngắn cho chunk_id
        doc_prefix = self._make_doc_prefix(document_name)

        for i, chunk_data in enumerate(raw_chunks):
            text = chunk_data["text"]
            has_overlap = False

            # Thêm overlap từ chunk trước (nếu không phải chunk đầu tiên)
            if i > 0 and raw_chunks[i - 1]["text"]:
                prev_overlap = self._get_overlap_text(
                    raw_chunks[i - 1]["text"], from_end=True
                )
                if prev_overlap:
                    text = f"[...overlap...]\n{prev_overlap}\n[...end overlap...]\n\n{text}"
                    has_overlap = True

            # Tạo chunk_id
            articles = chunk_data["articles"]
            if articles and articles[0] > 0:
                art_str = f"dieu_{articles[0]:03d}"
                if len(articles) > 1:
                    art_str += f"_to_{articles[-1]:03d}"
                if chunk_data["sub_part"] > 0:
                    art_str += f"_p{chunk_data['sub_part']}"
            else:
                art_str = f"chunk_{i+1:04d}"

            chunk_id = f"{doc_prefix}_{art_str}"

            final_chunks.append(LegalChunk(
                document_name=document_name,
                chunk_id=chunk_id,
                text=text,
                article_number=articles[0] if articles else 0,
                article_title=chunk_data["title"],
                chapter_context=chunk_data["chapter"],
                section_context=chunk_data["section"],
                token_count=self.estimate_tokens(text),
                has_overlap=has_overlap,
                source_law_id=law_id,
            ))

        # === LOG thống kê ===
        token_counts = [c.token_count for c in final_chunks]
        if token_counts:
            logger.info(f"Bước 4 - Kết quả cuối cùng: {len(final_chunks)} chunks")
            logger.info(f"  Token stats: min={min(token_counts)}, "
                       f"max={max(token_counts)}, "
                       f"avg={sum(token_counts)//len(token_counts)}")
            # Đếm chunks nằm trong target range
            in_range = sum(
                1 for t in token_counts
                if self.target_min_tokens <= t <= self.target_max_tokens
            )
            logger.info(f"  Chunks trong target range: {in_range}/{len(final_chunks)} "
                       f"({100*in_range//len(final_chunks)}%)")

        return final_chunks

    @staticmethod
    def _make_doc_prefix(document_name: str) -> str:
        """Tạo prefix ngắn gọn cho chunk_id từ tên văn bản."""
        name_lower = document_name.lower()

        if "dân sự" in name_lower:
            prefix = "blds"
        elif "doanh nghiệp" in name_lower:
            prefix = "ldn"
        elif "trọng tài" in name_lower:
            prefix = "lttm"
        elif "thương mại" in name_lower:
            prefix = "ltm"
        elif "lao động" in name_lower:
            prefix = "blld"
        elif "nhà ở" in name_lower:
            prefix = "lno"
        elif "bất động sản" in name_lower or "kinh doanh bds" in name_lower:
            prefix = "lkdbds"
        else:
            # Viết tắt từ tên: lấy chữ cái đầu mỗi từ
            words = re.findall(r"\w+", name_lower)
            prefix = "".join(w[0] for w in words if w[0].isalpha())

        # Lấy năm (4 chữ số)
        year_match = re.search(r"(\d{4})", document_name)
        year = year_match.group(1) if year_match else ""

        return f"{prefix}{year}"


# ---------------------------------------------------------------------------
# TIỆN ÍCH: In phân tích regex cho 1 đoạn text mẫu
# ---------------------------------------------------------------------------

def debug_regex_analysis(text: str, max_chars: int = 2000) -> None:
    """
    In ra phân tích regex trên 1 đoạn text mẫu.
    Hữu ích cho debug và kiểm tra regex hoạt động đúng.
    """
    sample = text[:max_chars]
    print(f"\n=== DEBUG: Phân tích Regex trên {max_chars} ký tự đầu ===\n")

    print("--- Điều (Article) ---")
    for m in ARTICLE_PATTERN.finditer(sample):
        print(f"  Vị trí {m.start():5d}: {m.group(1)[:80]}")

    print("\n--- Khoản (Clause) ---")
    for m in CLAUSE_PATTERN.finditer(sample):
        line_start = sample.rfind("\n", 0, m.start()) + 1
        context = sample[line_start : m.end() + 40].strip()
        print(f"  Vị trí {m.start():5d}: Khoản {m.group(1)} -> {context[:60]}")

    print("\n--- Điểm (Point) ---")
    for m in POINT_PATTERN.finditer(sample):
        line_start = sample.rfind("\n", 0, m.start()) + 1
        context = sample[line_start : m.end() + 40].strip()
        print(f"  Vị trí {m.start():5d}: Điểm {m.group(1)}) -> {context[:60]}")

    print("\n--- Chương (Chapter) ---")
    for m in CHAPTER_PATTERN.finditer(sample):
        print(f"  Vị trí {m.start():5d}: {m.group(1)[:80]}")

    print("\n--- Mục (Section) ---")
    for m in SECTION_PATTERN.finditer(sample):
        print(f"  Vị trí {m.start():5d}: {m.group(1)[:80]}")


# ---------------------------------------------------------------------------
# TEST/DEBUG
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")

    # Test với văn bản mẫu
    sample_text = """
Chương I. QUY ĐỊNH CHUNG

Điều 1. Phạm vi điều chỉnh
Bộ luật này quy định địa vị pháp lý, chuẩn mực pháp lý về cách ứng xử của cá nhân, pháp nhân; quyền, nghĩa vụ về nhân thân và tài sản của cá nhân, pháp nhân trong các quan hệ được hình thành trên cơ sở bình đẳng, tự do ý chí, độc lập về tài sản và tự chịu trách nhiệm.

Điều 2. Công nhận, tôn trọng, bảo vệ và bảo đảm quyền dân sự
1. Ở nước Cộng hoà xã hội chủ nghĩa Việt Nam, các quyền dân sự được công nhận, tôn trọng, bảo vệ và bảo đảm theo Hiến pháp và pháp luật.
2. Quyền dân sự chỉ có thể bị hạn chế theo quy định của luật trong trường hợp cần thiết vì lý do quốc phòng, an ninh quốc gia, trật tự, an toàn xã hội, đạo đức xã hội, sức khỏe của cộng đồng.

Điều 3. Các nguyên tắc cơ bản của pháp luật dân sự
1. Mọi cá nhân, pháp nhân đều bình đẳng, không được lấy bất kỳ lý do nào để phân biệt đối xử.
a) Cá nhân có quyền tự do giao kết hợp đồng;
b) Pháp nhân được tự do hoạt động trong phạm vi pháp luật cho phép.
2. Cá nhân, pháp nhân xác lập, thực hiện, chấm dứt quyền, nghĩa vụ dân sự của mình trên cơ sở tự do, tự nguyện cam kết, thỏa thuận.
"""

    # Debug regex
    debug_regex_analysis(sample_text)

    # Test chunking
    chunker = VietnameseLegalChunker(
        target_min_tokens=50,   # Giảm min cho test
        target_max_tokens=200,  # Giảm max cho test
        overlap_tokens=20,
    )

    chunks = chunker.chunk_document("Bộ luật Dân sự 2015 (Test)", sample_text)
    print(f"\n=== KẾT QUẢ: {len(chunks)} chunks ===")
    for chunk in chunks:
        print(f"\n--- {chunk.chunk_id} ---")
        print(f"  Token: {chunk.token_count}, Overlap: {chunk.has_overlap}")
        print(f"  Chương: {chunk.chapter_context}")
        print(f"  Text[:100]: {chunk.text[:100]}...")
