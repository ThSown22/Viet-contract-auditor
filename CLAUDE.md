# Viet-Contract Auditor — CLAUDE.md

## Quick commands

```bash
uv run python src/main.py              # Full ETL pipeline (Phase 1 & 2)
uv run python src/data_ingestion.py   # Debug: ingestion only
uv run python src/semantic_chunker.py # Debug: chunker only
uv add <package>                       # Install dependency (never use pip)
```

> All Python MUST go through `uv run`. Runtime: Python 3.11, venv at `.venv/`.

---

## Project in one paragraph

**Goal:** Take a Vietnamese contract (PDF/Word) → output a legal audit report listing violations, legal references, and suggested fixes. Core stack: LightRAG (graph-based RAG over Vietnamese law) + LangGraph multi-agent pipeline. Current status: **Phase 1–2 done** (ETL + chunking). **Phase 3 is next** (LightRAG graph indexing).

---

## Repo layout

```
src/
  main.py              # ETL orchestrator — run this for Phase 1 & 2
  data_ingestion.py    # HuggingFace + local .txt loader
  semantic_chunker.py  # VietnameseLegalChunker (regex-based, Article-level)
data/
  raw/                 # .txt law files (gitignored) + HF cache
  processed/           # processed_legal_chunks.json, chunking_stats.json (gitignored)
notebooks/             # Colab notebooks (Phase 2 graph indexing: phase2_lightrag_indexing_vllm.ipynb)
```

---

## Phase status & what's next

| Phase | What | Status |
|-------|------|--------|
| 1–2 | ETL: ingest laws → semantic chunks → `processed_legal_chunks.json` | ✅ Done |
| 3 | LightRAG graph indexing (Colab A100, vLLM local) | 🔧 In progress |
| 4 | LangGraph agents: Router → Retrieval → Audit → Generator | ⬜ Next |
| 5 | Streamlit UI | ⬜ Pending |
| 6 | Evaluator (LLM-as-Judge, recall/precision) | ⬜ Pending |

---

## Hard rules — never violate these

**Data segregation:** `NghiemAbe/Legal-Corpus-Zalo` (filtered to 3 laws below) → Knowledge Graph only. `vietnamese-legal-qa` and dummy contracts → evaluation only. Never cross these.

**Chunking:** Always regex-based, always split at `Điều \d+\.` boundaries. Never character-split. Never modify `VietnameseLegalChunker` without running the full pipeline to verify stats match `chunking_stats.json`.

**LightRAG prompts:** The Vietnamese NER prompt in `notebooks/` uses LightRAG's native pipe-delimited tuple format — `('entity'|name|type|description)`. Never convert to JSON format. This is a known footgun (breaks parser silently, 0 entities extracted).

**Imports:** Never add `import lightrag` or `import langgraph` to `src/` modules. These are Phase 3+ dependencies, kept in Colab notebooks until Phase 4 scaffolding begins.

---

## Data sources

**Tier 1 — HuggingFace (auto-loaded):**
- `91/2015/qh13` → Bộ luật Dân sự 2015 (118 chunks, avg 973 tok)
- `59/2020/qh14` → Luật Doanh nghiệp 2020 (115 chunks, avg 892 tok)
- `54/2010/qh12` → Luật Trọng tài Thương mại 2010 (23 chunks, avg 974 tok)

**Tier 2 — Local .txt (drop into `data/raw/`, pipeline auto-detects):**
- `Luật_Thương_mại_2005.txt` → `36/2005/qh11`
- `Bộ_luật_Lao_động_2019.txt` → `45/2019/qh14`

Total current corpus: **256 chunks, ~240K tokens**.

---

## Key data schemas

**`processed_legal_chunks.json`** (output of `src/main.py`):
```json
{
  "metadata": { "total_laws": 3, "chunker_config": {...} },
  "total_chunks": 256,
  "chunks": [{
    "document_name": "Bộ luật Dân sự 2015",
    "chunk_id": "blds2015_dieu_001_to_006",
    "text": "Điều 1. ...",
    "metadata": {
      "law_id": "91/2015/qh13",
      "article_number": 1,
      "article_title": "Phạm vi điều chỉnh",
      "chapter": "Chương I. QUY ĐỊNH CHUNG",
      "estimated_tokens": 936,
      "has_overlap": false
    }
  }]
}
```

**Audit Agent output (Phase 4 target):**
```json
[{
  "clause": "exact quote from contract",
  "violation": "why it violates law",
  "reference_law": "Điều X, Luật Y",
  "suggested_fix": "corrected phrasing"
}]
```

---

## LLM & embedding config (Phase 3+)

| Component | Value |
|-----------|-------|
| LLM (local, Colab) | `Qwen/Qwen2.5-32B-Instruct-AWQ` via vLLM subprocess |
| LLM (API fallback) | `gpt-4o-mini` |
| Embeddings | `paraphrase-multilingual-MiniLM-L12-v2` (local) or `text-embedding-3-small` |
| Graph DB | NetworkX (local) → Neo4j if scale needed |
| Vector DB | NanoVectorDB (default in LightRAG) |
| LightRAG entity types | `CHỦ_THỂ`, `HÀNH_VI`, `QUYỀN_NGHĨA_VỤ`, `CHẾ_TÀI`, `VĂN_BẢN_PHÁP_LUẬT` |
| LightRAG relation types | `Bị cấm`, `Được phép`, `Quy định tại`, `Xử phạt bằng` |

---

## Git commit conventions

Use **Conventional Commits**. Format: `type(scope): message` — all lowercase, imperative mood, no period.

```
feat(phase3): add vllm server startup cell to colab notebook
fix(chunker): handle articles with no khoản boundary
fix(ingestion): skip malformed records with missing law_id
refactor(chunker): extract _make_doc_prefix to static method
test(chunker): add edge case for articles exceeding 1439 tokens
docs(claude): update phase status table
chore(deps): add lightrag-hku, sentence-transformers to pyproject
```

**Scopes:** `phase3`, `phase4`, `phase5`, `phase6`, `chunker`, `ingestion`, `pipeline`, `agents`, `ui`, `eval`, `deps`, `claude`

**One logical change per commit.** If you touch both `src/` and `notebooks/`, split into two commits.

---

## How to work on this codebase

**Before starting any task:** Read the relevant source file(s) completely. Never edit based on partial context.

**When modifying `src/semantic_chunker.py`:** Always run `uv run python src/main.py` after and confirm the final stats table matches `chunking_stats.json`. If numbers diverge, the change broke something.

**When modifying `src/data_ingestion.py`:** The HF dataset download is ~34MB and slow. Use `uv run python src/data_ingestion.py` to test in isolation before running the full pipeline.

**When working on Phase 3 (graph indexing):** All graph work lives in `notebooks/phase2_lightrag_indexing_vllm.ipynb`. The `src/` package does not import LightRAG. Keep them separate until Phase 4 integration.

**Error handling standard:** Every function that calls HuggingFace, OpenAI API, or file I/O must have explicit try/except with logged errors — not bare `except Exception: pass`. Log the error, then either raise or return a typed fallback.

**When adding a new law source:** Add to `HF_LAWS` or `LOCAL_LAWS` in `data_ingestion.py`, run pipeline, verify new chunks appear in stats, update the "Data sources" section above.