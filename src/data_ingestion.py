# ============================================================================
# VIET-CONTRACT AUDITOR - Phase 1 & 2
# Module: data_ingestion.py
# Phiên bản: Multi-law (Tier 1: HuggingFace | Tier 2: Local .txt)
#
# === THỰC TẾ DATASET (61,425 records, ~34MB) ===
# Columns: ['law_id', 'article_id', 'title', 'text']
# Mỗi record = 1 Điều, có 3,271 law_id khác nhau.
#
# === CHIẾN LƯỢC ĐA-LUẬT ===
# Tier 1 (HuggingFace) – Đã xác nhận qua EDA:
#   91/2015/qh13  Bộ luật Dân sự 2015        (688 Điều)
#   59/2020/qh14  Luật Doanh nghiệp 2020      (217 Điều)
#   54/2010/qh12  Luật Trọng tài TM 2010      ( 80 Điều)
#
# Tier 2 (Local .txt) – Đặt file vào data/raw/ để tự động bổ sung:
#   Luật_Thương_mại_2005.txt      → 36/2005/qh11  Luật Thương mại 2005
#   Bộ_luật_Lao_động_2019.txt     → 45/2019/qh14  Bộ luật Lao động 2019
#   Luật_Nhà_ở_2014.txt           → 65/2014/qh13  Luật Nhà ở 2014
#   Luật_Kinh_doanh_BDS_2014.txt  → 66/2014/qh13  Luật Kinh doanh BĐS 2014
#   (Tải toàn văn từ thuvienphapluat.vn, lưu với tên tương ứng)
# ============================================================================

import re
import html
import logging
import os
import unicodedata

from datasets import load_dataset

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CẤU HÌNH: DANH SÁCH VĂN BẢN LUẬT MỤC TIÊU
# ---------------------------------------------------------------------------

# Tier 1: Confirmed có trong HuggingFace dataset (EDA ngày 2026-03-09)
HF_LAWS: dict[str, str] = {
    "91/2015/qh13": "Bộ luật Dân sự 2015",
    "59/2020/qh14": "Luật Doanh nghiệp 2020",
    "54/2010/qh12": "Luật Trọng tài Thương mại 2010",
}

# Tier 2: Key = tên file trong data/raw/, Value = (law_id, law_name)
# Pipeline sẽ TỰ ĐỘNG load nếu file tồn tại, bỏ qua nếu không có.
LOCAL_LAWS: dict[str, tuple[str, str]] = {
    "Luật_Thương_mại_2005.txt":        ("36/2005/qh11", "Luật Thương mại 2005"),
    "Bộ_luật_Lao_động_2019.txt":       ("45/2019/qh14", "Bộ luật Lao động 2019"),
    "Luật_Nhà_ở_2014.txt":             ("65/2014/qh13", "Luật Nhà ở 2014"),
    "Luật_Kinh_doanh_BDS_2014.txt":    ("66/2014/qh13", "Luật Kinh doanh Bất động sản 2014"),
}

# Regex nhận diện "Điều X." trong file local (dùng khi parse .txt)
_LOCAL_ARTICLE_RE = re.compile(
    r"^(Điều\s+\d+[\.:]\s*.*)",
    re.MULTILINE | re.UNICODE,
)


# ---------------------------------------------------------------------------
# TIER 1: HuggingFace
# ---------------------------------------------------------------------------

def load_legal_corpus() -> list[dict]:
    """
    Tải toàn bộ dataset 'NghiemAbe/Legal-Corpus-Zalo' từ HuggingFace.

    Returns:
        List ~61,425 records dạng dict với keys: law_id, article_id, title, text.
    """
    logger.info("Đang tải dataset 'NghiemAbe/Legal-Corpus-Zalo' từ HuggingFace...")
    dataset = load_dataset("NghiemAbe/Legal-Corpus-Zalo")

    all_records: list[dict] = []
    for split_name in dataset:
        count = len(dataset[split_name])
        logger.info(f"  -> Split '{split_name}': {count:,} records")
        all_records.extend(dict(r) for r in dataset[split_name])

    logger.info(f"Tổng records: {len(all_records):,} (~34MB toàn bộ dataset)")
    return all_records


def filter_law_articles(records: list[dict], law_id: str, law_name: str) -> list[dict]:
    """
    Lọc records theo law_id và sắp xếp theo thứ tự Điều.

    Args:
        records:  Toàn bộ records từ HuggingFace.
        law_id:   Mã số văn bản (e.g., "91/2015/qh13").
        law_name: Tên văn bản (chỉ dùng cho logging).

    Returns:
        Danh sách records đã lọc và sorted theo article_id.

    Raises:
        ValueError: Nếu không tìm thấy records nào cho law_id.
    """
    logger.info(f"Lọc {law_name} (law_id='{law_id}')...")
    filtered = [r for r in records if r.get("law_id") == law_id]

    if not filtered:
        raise ValueError(
            f"Không tìm thấy records nào với law_id='{law_id}'. "
            f"Kiểm tra HF_LAWS config hoặc EDA lại dataset."
        )

    def _sort_key(r: dict) -> int:
        raw = str(r.get("article_id", "0"))
        if "__" in raw:
            raw = raw.split("__")[-1]
        try:
            return int(raw)
        except ValueError:
            return 0

    filtered.sort(key=_sort_key)
    total_chars = sum(len(str(r.get("text", ""))) for r in filtered)
    logger.info(
        f"  -> {len(filtered):,} Điều "
        f"(article_id: {filtered[0]['article_id']} ... {filtered[-1]['article_id']}), "
        f"~{total_chars:,} ký tự raw"
    )
    return filtered


# ---------------------------------------------------------------------------
# TIER 2: Local .txt files
# ---------------------------------------------------------------------------

def load_from_local_txt(filepath: str, law_id: str, law_name: str) -> list[dict]:
    """
    Parse file .txt cục bộ chứa toàn văn một văn bản luật.

    Yêu cầu định dạng: Mỗi Điều bắt đầu bằng "Điều X. Tên điều" trên dòng riêng.
    Ví dụ:
        Điều 1. Phạm vi điều chỉnh
        Luật này quy định...

        Điều 2. Giải thích từ ngữ
        Trong Luật này, các từ ngữ dưới đây được hiểu như sau:

    Args:
        filepath: Đường dẫn đầy đủ đến file .txt.
        law_id:   Mã số văn bản để gắn vào mỗi record.
        law_name: Tên văn bản (chỉ cho logging).

    Returns:
        Danh sách records dạng dict tương thích schema HF dataset.
        Trả về [] nếu file không có Điều nào.
    """
    logger.info(f"Tải local: '{os.path.basename(filepath)}'...")
    with open(filepath, encoding="utf-8") as f:
        raw = f.read()

    text = clean_text(raw)
    matches = list(_LOCAL_ARTICLE_RE.finditer(text))

    if not matches:
        logger.warning(f"  ⚠️ Không tìm 'Điều' nào trong '{filepath}'. Bỏ qua.")
        return []

    records: list[dict] = []
    for i, match in enumerate(matches):
        start = match.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        article_full = text[start:end].strip()

        header = match.group(1).strip()
        art_num_m = re.search(r"Điều\s+(\d+)", header)
        art_num = int(art_num_m.group(1)) if art_num_m else i + 1

        # Body = phần sau dòng tiêu đề
        first_nl = article_full.find("\n")
        body = article_full[first_nl:].strip() if first_nl != -1 else ""

        records.append({
            "law_id":     law_id,
            "article_id": f"{law_id}__{art_num}",
            "title":      header,
            "text":       body,
        })

    logger.info(f"  -> {len(records)} Điều từ '{os.path.basename(filepath)}'")
    return records


# ---------------------------------------------------------------------------
# ENTRY POINT ĐA-NGUỒN
# ---------------------------------------------------------------------------

def load_all_sources(raw_dir: str) -> dict[str, dict]:
    """
    Tải tất cả nguồn dữ liệu: HF dataset (Tier 1) + local .txt files (Tier 2).

    Args:
        raw_dir: Đường dẫn thư mục chứa các file local (data/raw/).

    Returns:
        dict[law_id → {"name": str, "records": list[dict], "source": "huggingface"|"local"}]
        Thứ tự: Tier 1 trước, Tier 2 sau.
    """
    result: dict[str, dict] = {}

    # --- Tier 1: HuggingFace ---
    logger.info("=" * 55)
    logger.info("TIER 1: HuggingFace dataset")
    logger.info("=" * 55)
    hf_records = load_legal_corpus()

    for law_id, law_name in HF_LAWS.items():
        try:
            records = filter_law_articles(hf_records, law_id, law_name)
            result[law_id] = {"name": law_name, "records": records, "source": "huggingface"}
        except ValueError as e:
            logger.error(f"  ❌ Bỏ qua {law_id}: {e}")

    # --- Tier 2: Local files ---
    logger.info("=" * 55)
    logger.info("TIER 2: Local .txt files")
    logger.info("=" * 55)
    tier2_found = 0
    for filename, (law_id, law_name) in LOCAL_LAWS.items():
        filepath = os.path.join(raw_dir, filename)
        if not os.path.exists(filepath):
            logger.debug(f"  ⏭️  Không có file: {filename} (bỏ qua)")
            continue
        records = load_from_local_txt(filepath, law_id, law_name)
        if records:
            result[law_id] = {"name": law_name, "records": records, "source": "local"}
            tier2_found += 1

    if tier2_found == 0:
        logger.info("  (Chưa có file local nào. Đặt .txt vào data/raw/ để bổ sung.)")

    # --- Tổng kết ---
    logger.info("=" * 55)
    logger.info(f"✅ Tổng: {len(result)} văn bản luật sẵn sàng")
    for lid, info in result.items():
        logger.info(
            f"  [{info['source']:12s}] {lid:<22} "
            f"{len(info['records']):>4} Điều | {info['name']}"
        )
    logger.info("=" * 55)
    return result


# ---------------------------------------------------------------------------
# TEXT UTILITIES (dùng chung Tier 1 & Tier 2)
# ---------------------------------------------------------------------------

def clean_text(raw_text: str) -> str:
    """
    Làm sạch văn bản luật thô.

    1. Decode HTML entities (&amp; → &, ...)
    2. Loại bỏ HTML/XML tags còn sót
    3. Unicode NFC (quan trọng cho tiếng Việt có dấu)
    4. Loại bỏ control characters
    5. Chuẩn hóa line breaks (\\r\\n → \\n, nhiều \\n → ≤2)
    6. Trim whitespace thừa mỗi dòng
    """
    if not raw_text or not isinstance(raw_text, str):
        return ""

    text = html.unescape(html.unescape(raw_text))
    text = re.sub(r"<[^>]+>", " ", text)
    text = unicodedata.normalize("NFC", text)
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]", "", text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.split("\n")]
    text = "\n".join(lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def reconstruct_full_text(records: list[dict]) -> str:
    """
    Ghép title + text của mỗi Điều thành 1 chuỗi văn bản liên tục.

    Args:
        records: List records đã sorted theo article_id.

    Returns:
        Văn bản đầy đủ đã làm sạch.
    """
    parts: list[str] = []
    for record in records:
        title = clean_text(str(record.get("title", "")))
        body = clean_text(str(record.get("text", "")))
        if title and body:
            parts.append(f"{title}\n{body}")
        elif body:
            parts.append(body)
        elif title:
            parts.append(title)

    full_text = "\n\n".join(parts)
    logger.info(
        f"Tái cấu trúc: {len(records)} Điều → {len(full_text):,} ký tự "
        f"(~{len(full_text) // 3500:,} trang A4)"
    )
    return full_text


def save_raw_document(text: str, output_dir: str, law_name: str) -> str:
    """
    Lưu văn bản đã reconstruct ra file .txt để kiểm tra thủ công.

    Args:
        text:       Nội dung văn bản.
        output_dir: Thư mục đầu ra.
        law_name:   Tên văn bản (dùng làm tên file).

    Returns:
        Đường dẫn file đã lưu.
    """
    os.makedirs(output_dir, exist_ok=True)
    safe_name = re.sub(r"[^\w\s-]", "", law_name).strip().replace(" ", "_")
    filepath = os.path.join(output_dir, f"{safe_name}.txt")
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(text)
    logger.info(f"Đã lưu: {filepath} ({len(text):,} ký tự)")
    return filepath


# ---------------------------------------------------------------------------
# TEST/DEBUG: Chạy độc lập để kiểm tra
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import os as _os
    _root = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
    _raw_dir = _os.path.join(_root, "data", "raw")

    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    sources = load_all_sources(_raw_dir)

    print(f"\n✅ Loaded {len(sources)} laws:")
    for lid, info in sources.items():
        print(f"  {lid}: {info['name']} ({len(info['records'])} Điều) [{info['source']}]")
