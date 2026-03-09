# ============================================================================
# VIET-CONTRACT AUDITOR - Phase 1 & 2
# Module: main.py  (Multi-law pipeline)
# Mô tả: Orchestrate ETL pipeline cho nhiều văn bản luật.
#
# === NGUỒN DỮ LIỆU ===
# Tier 1 (HuggingFace): Tự động load – 3 luật xác nhận trong dataset
#   91/2015/qh13  Bộ luật Dân sự 2015        (688 Điều)
#   59/2020/qh14  Luật Doanh nghiệp 2020      (217 Điều)
#   54/2010/qh12  Luật Trọng tài TM 2010      ( 80 Điều)
#
# Tier 2 (Local .txt): Tự động phát hiện – đặt file vào data/raw/:
#   Luật_Thương_mại_2005.txt, Bộ_luật_Lao_động_2019.txt, v.v.
#
# Chạy: uv run python src/main.py
# ============================================================================

import json
import logging
import os
import sys
from datetime import datetime

from data_ingestion import (
    load_all_sources,
    reconstruct_full_text,
    save_raw_document,
)
from semantic_chunker import (
    VietnameseLegalChunker,
    LegalChunk,
)

# ---------------------------------------------------------------------------
# CẤU HÌNH
# ---------------------------------------------------------------------------

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(_ROOT, "data", "raw")
PROCESSED_DIR = os.path.join(_ROOT, "data", "processed")
OUTPUT_JSON = os.path.join(PROCESSED_DIR, "processed_legal_chunks.json")

CHUNKER_CONFIG = {
    "target_min_tokens": 800,
    "target_max_tokens": 1200,
    "overlap_tokens": 100,
    "chars_per_token": 3.5,
}

LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"


# ---------------------------------------------------------------------------
# SERIALIZATION
# ---------------------------------------------------------------------------

def chunks_to_dicts(chunks: list[LegalChunk]) -> list[dict]:
    """
    Chuyển LegalChunk → list of dicts để JSON serialize.

    Schema:
    {
        "document_name": "Bộ luật Dân sự 2015",
        "chunk_id":       "blds2015_dieu_001_to_006",
        "text":           "Điều 1. Phạm vi điều chỉnh\\n...",
        "metadata": {
            "law_id":          "91/2015/qh13",
            "article_number":  1,
            "article_title":   "Phạm vi điều chỉnh",
            "chapter":         "Chương I. QUY ĐỊNH CHUNG",
            "section":         "",
            "estimated_tokens": 936,
            "has_overlap":     false
        }
    }
    """
    return [
        {
            "document_name": c.document_name,
            "chunk_id": c.chunk_id,
            "text": c.text,
            "metadata": {
                "law_id": c.source_law_id,
                "article_number": c.article_number,
                "article_title": c.article_title,
                "chapter": c.chapter_context,
                "section": c.section_context,
                "estimated_tokens": c.token_count,
                "has_overlap": c.has_overlap,
            },
        }
        for c in chunks
    ]


def save_output(chunks_dicts: list[dict], pipeline_meta: dict) -> None:
    """Lưu toàn bộ chunks ra file JSON."""
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    payload = {
        "metadata": pipeline_meta,
        "total_chunks": len(chunks_dicts),
        "chunks": chunks_dicts,
    }

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    file_mb = os.path.getsize(OUTPUT_JSON) / (1024 * 1024)
    logging.getLogger(__name__).info(
        f"Đã lưu: {OUTPUT_JSON} ({file_mb:.2f} MB, {len(chunks_dicts)} chunks)"
    )


# ---------------------------------------------------------------------------
# THỐNG KÊ
# ---------------------------------------------------------------------------

def _law_stats(chunks: list[LegalChunk], law_id: str, law_name: str, source: str) -> dict:
    """Tính thống kê cho 1 văn bản luật."""
    tokens = [c.token_count for c in chunks]
    mn, mx = CHUNKER_CONFIG["target_min_tokens"], CHUNKER_CONFIG["target_max_tokens"]
    in_range = sum(1 for t in tokens if mn <= t <= mx)
    return {
        "law_id": law_id,
        "name": law_name,
        "source": source,
        "chunks": len(chunks),
        "total_tokens": sum(tokens),
        "min_tokens": min(tokens),
        "max_tokens": max(tokens),
        "avg_tokens": sum(tokens) // len(tokens),
        "pct_in_range": round(100 * in_range / len(chunks), 1) if chunks else 0,
        "chunks_with_overlap": sum(1 for c in chunks if c.has_overlap),
    }


def print_statistics(all_chunks: list[LegalChunk], per_law: list[dict]) -> dict:
    """In bảng thống kê tổng hợp và per-law."""
    tokens = [c.token_count for c in all_chunks]
    mn, mx = CHUNKER_CONFIG["target_min_tokens"], CHUNKER_CONFIG["target_max_tokens"]
    in_range = sum(1 for t in tokens if mn <= t <= mx)
    with_overlap = sum(1 for c in all_chunks if c.has_overlap)

    print("\n" + "=" * 65)
    print("📊  THỐNG KÊ CHUNKING – TỔNG HỢP")
    print("=" * 65)
    print(f"  {'Văn bản':<35} {'#Chunks':>7} {'AvgTok':>7} {'InRange':>8}")
    print(f"  {'-'*60}")
    for s in per_law:
        print(
            f"  {s['name']:<35} {s['chunks']:>7} "
            f"{s['avg_tokens']:>7} {s['pct_in_range']:>7}%"
        )
    print(f"  {'─'*60}")
    print(f"  {'TỔNG CỘNG':<35} {len(all_chunks):>7} "
          f"{sum(tokens)//len(tokens):>7} "
          f"{round(100*in_range/len(all_chunks),1):>7}%")
    print("=" * 65)
    print(f"  Tổng chunks       : {len(all_chunks)}")
    print(f"  Tổng tokens (est) : {sum(tokens):,}")
    print(f"  Token/chunk       : min={min(tokens)} | max={max(tokens)} | avg={sum(tokens)//len(tokens)}")
    print(f"  Trong [{mn}-{mx}]  : {in_range}/{len(all_chunks)} ({round(100*in_range/len(all_chunks),1)}%)")
    print(f"  Có overlap        : {with_overlap} chunks")
    print("=" * 65)

    stats = {
        "total_laws": len(per_law),
        "total_chunks": len(all_chunks),
        "total_tokens_estimated": sum(tokens),
        "min_tokens": min(tokens),
        "max_tokens": max(tokens),
        "avg_tokens": sum(tokens) // len(tokens),
        "chunks_in_target_range": in_range,
        "pct_in_range": round(100 * in_range / len(all_chunks), 1),
        "chunks_with_overlap": with_overlap,
        "per_law": per_law,
    }
    return stats


# ---------------------------------------------------------------------------
# PIPELINE CHÍNH
# ---------------------------------------------------------------------------

def run_pipeline() -> None:
    """
    Multi-law ETL Pipeline:
      Extract  → load_all_sources() [HF Tier1 + local Tier2]
      Transform → reconstruct_full_text() + VietnameseLegalChunker per law
      Load     → chunks_to_dicts() + save_output()
    """
    logger = logging.getLogger(__name__)
    start_time = datetime.now()

    print("=" * 65)
    print("🏛️  VIET-CONTRACT AUDITOR – Phase 1 & 2 (Multi-Law Pipeline)")
    print(f"    Thời gian: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    # ── EXTRACT ──────────────────────────────────────────────────────────
    print("\n📥 EXTRACT: Tải tất cả nguồn dữ liệu...")
    law_sources = load_all_sources(RAW_DIR)
    if not law_sources:
        logger.error("Không có nguồn dữ liệu nào – dừng pipeline.")
        sys.exit(1)

    print(f"\n   ✅ {len(law_sources)} văn bản luật sẵn sàng:")
    for lid, info in law_sources.items():
        tier = "HF" if info["source"] == "huggingface" else "local"
        print(f"      [{tier}] {info['name']} ({len(info['records'])} Điều)")

    # ── TRANSFORM + LOAD (per law) ────────────────────────────────────────
    chunker = VietnameseLegalChunker(**CHUNKER_CONFIG)
    all_chunks: list[LegalChunk] = []
    per_law_stats: list[dict] = []

    for law_id, info in law_sources.items():
        law_name = info["name"]
        records = info["records"]
        source = info["source"]

        print(f"\n{'─'*65}")
        print(f"📖 {law_name}  [{source}]")
        print(f"{'─'*65}")

        # TRANSFORM 1: Reconstruct full text
        full_text = reconstruct_full_text(records)

        # Lưu raw text chỉ cho HF (local đã có file gốc rồi)
        if source == "huggingface":
            save_raw_document(full_text, RAW_DIR, law_name)

        print(f"   ✅ {len(records)} Điều → {len(full_text):,} ký tự")

        # TRANSFORM 2: Semantic Chunking
        print(f"   ✂️  Chunking...")
        chunks = chunker.chunk_document(law_name, full_text, law_id=law_id)
        print(f"   ✅ {len(chunks)} chunks")

        if not chunks:
            logger.warning(f"  ⚠️ {law_name}: Không tạo được chunk nào, bỏ qua.")
            continue

        all_chunks.extend(chunks)
        per_law_stats.append(_law_stats(chunks, law_id, law_name, source))

    if not all_chunks:
        logger.error("Tổng chunk = 0 – kiểm tra lại pipeline.")
        sys.exit(1)

    # ── LOAD ─────────────────────────────────────────────────────────────
    print(f"\n{'─'*65}")
    print("💾 LOAD: Serialize → JSON...")
    chunks_dicts = chunks_to_dicts(all_chunks)

    pipeline_meta = {
        "project": "Viet-Contract Auditor",
        "phase": "Phase 1 & 2 – Multi-Law Data Preprocessing",
        "created_at": datetime.now().isoformat(),
        "source_dataset": "NghiemAbe/Legal-Corpus-Zalo (Tier 1) + Local .txt (Tier 2)",
        "total_laws": len(per_law_stats),
        "laws": per_law_stats,
        "chunker_config": CHUNKER_CONFIG,
    }

    save_output(chunks_dicts, pipeline_meta)
    print(f"   ✅ {OUTPUT_JSON}")

    # ── STATISTICS ────────────────────────────────────────────────────────
    stats = print_statistics(all_chunks, per_law_stats)

    stats_path = os.path.join(PROCESSED_DIR, "chunking_stats.json")
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\n✅ Pipeline hoàn tất trong {elapsed:.1f}s")


# ---------------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt="%H:%M:%S")
    run_pipeline()
