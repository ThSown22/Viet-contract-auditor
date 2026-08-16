2

# Viet Contract Auditor

Viet Contract Auditor là hệ thống hỗ trợ rà soát hợp đồng tiếng Việt bằng hướng Graph RAG và multi-agent. Nhánh này tập trung vào hai phần chính:

- Pipeline tự động thu thập, làm sạch, chunking, indexing văn bản pháp luật.
- Runtime kiểm toán hợp đồng với LangGraph multi-agent, LightRAG retrieval, Streamlit UI, sample contracts, reports và evaluation scripts.

![Kiến trúc multi-agent pipeline](Arch-diagramjpg.jpg)

## Kiến Trúc Chính

### Data Ingestion

Pipeline ingestion nằm trong `src/ingestion/`:

- `discovery`: tìm nguồn văn bản pháp luật.
- `scraping`: crawl HTML từ nguồn luật.
- `cleaning`: chuẩn hóa nội dung văn bản.
- `chunking`: chia văn bản luật thành các đoạn phù hợp.
- `indexing`: tạo LightRAG artifacts trong `data/index`.
- `publish_storage`: nạp index vào PostgreSQL, Neo4j và Qdrant khi storage sẵn sàng.

### Multi-Agent Audit

Pipeline audit nằm trong `src/agents/` và được điều phối bởi `src/agents/orchestrator.py`.

Luồng LangGraph:

```text
router -> preprocessor -> retrieval -> context_validator -> audit -> critic -> generator
```

Vai trò chính:

- `router`: phân loại domain hợp đồng và tách điều khoản.
- `preprocessor`: chuẩn hóa alias pháp lý, token hóa tiếng Việt, phát hiện cross-reference.
- `retrieval`: truy vấn LightRAG hybrid để lấy ngữ cảnh pháp lý.
- `context_validator`: kiểm tra chất lượng context bằng heuristic.
- `audit`: phát hiện vi phạm và sinh finding có cấu trúc.
- `critic`: kiểm tra hallucination, phủ định, nitpicking và confidence.
- `generator`: tổng hợp báo cáo Markdown cuối.

Chi tiết luồng nằm ở `docs/MAS_pipeline_flow.md`.

## Cấu Trúc Repo

```text
src/
  ingestion/                 Custom crawl -> clean -> chunk -> index pipeline
  agents/                    LangGraph audit agents
  core/                      Shared state, prompts, LLM config, LightRAG client, rerank
  ui/                        Streamlit UI và reusable components
  run_audit.py               CLI kiểm toán hợp đồng
  e2e_eval.py                Evaluation theo ground truth
  benchmark_*.py             Benchmark, diagnosis, calibration cho retrieval/rerank

result-example/              Hợp đồng mẫu và ground truth
reports/                     Báo cáo mẫu và metrics đã chạy
data/index/                  LightRAG artifacts sinh từ ingestion
docs/MAS_pipeline_flow.md    Tài liệu luồng multi-agent
```

## Chạy CLI Audit

```powershell
python src\run_audit.py result-example\HDLD\HDLD_ThucHanh_01.docx --output reports\final_outputs\hdld_report.md
```

Lưu ý: phần runtime retrieval cần storage và API key phù hợp nếu chạy thật.

## Chạy UI

```powershell
streamlit run src\ui\streamlit_app.py
```

UI upload hợp đồng, gọi `src/run_audit.py`, hiển thị tiến trình, metrics và báo cáo Markdown.

## Evaluation Và Benchmark

Chạy end-to-end evaluation:

```powershell
python src\e2e_eval.py --groundtruth "result-example\HDLD\groundtruth_hdld_01_test copy.json"
```

Chạy rerank benchmark:

```powershell
python src\benchmark_rerank_ab.py --groundtruth "result-example\HDLD\groundtruth_hdld_01_test copy.json"
```

Các artifact mẫu đã có trong:

- `result-example/`
- `reports/final_outputs/`
- `reports/metrics/`

## Storage Runtime

Hệ thống dùng ba backend chính:

- PostgreSQL: lưu LightRAG KV/doc/chunk/status records.
- Neo4j: lưu graph thực thể và quan hệ pháp lý.
- Qdrant: lưu vector embeddings để semantic search.
