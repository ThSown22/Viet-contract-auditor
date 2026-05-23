from __future__ import annotations

import asyncio
import logging
import os
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import yaml
from pydantic import BaseModel, Field

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
    from src.ingestion.indexing.checkpoint import (
        IndexingCheckpoint,
        create_empty_checkpoint,
        load_checkpoint,
        mark_chunk_completed,
        mark_chunk_failed,
        save_checkpoint,
    )
    from src.ingestion.indexing.formatter import format_chunk_documents
    from src.ingestion.indexing.lightrag_client import (
        build_client_config_from_runtime,
        build_lightrag_client,
        index_documents,
    )
    from src.ingestion.indexing.loader import load_chunks
    from src.ingestion.indexing.publish_storage import (
        build_publish_config_from_runtime,
        publish_index_to_storage,
    )
else:
    from .checkpoint import (
        IndexingCheckpoint,
        create_empty_checkpoint,
        load_checkpoint,
        mark_chunk_completed,
        mark_chunk_failed,
        save_checkpoint,
    )
    from .formatter import format_chunk_documents
    from .lightrag_client import (
        build_client_config_from_runtime,
        build_lightrag_client,
        index_documents,
    )
    from .loader import load_chunks
    from .publish_storage import (
        build_publish_config_from_runtime,
        publish_index_to_storage,
    )


PROJECT_ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = PROJECT_ROOT / "src" / "ingestion" / "config" / "indexing.yaml"
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE_NAME = "indexing.log"
LIGHTRAG_LOG_FILE_NAME = "indexing.lightrag.log"

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

logger = logging.getLogger(__name__)


class InputConfig(BaseModel):
    base_dir: str
    pattern: str = Field(min_length=1)


class OutputConfig(BaseModel):
    base_dir: str


class CheckpointConfig(BaseModel):
    filename: str = Field(min_length=1)


class IndexingRuntimeConfig(BaseModel):
    batch_size: int = Field(ge=1)
    max_retries: int = Field(ge=0)
    max_async: int = Field(ge=1)
    resume: bool = True


class EmbeddingConfig(BaseModel):
    model_env: str = Field(min_length=1)
    default_model_name: str = Field(min_length=1)


class LLMConfig(BaseModel):
    api_key_env: str = Field(min_length=1)
    model_env: str = Field(min_length=1)
    default_model_name: str = Field(min_length=1)
    max_tokens: int = Field(ge=1)
    temperature: float = Field(ge=0.0)


class LightRAGConfig(BaseModel):
    chunk_token_size: int = Field(ge=1)
    chunk_overlap_token_size: int = Field(ge=0)
    enable_rerank: bool = False
    enable_llm_cache: bool = False


class PublishConfig(BaseModel):
    enabled: bool = False
    verify_after_publish: bool = True
    batch_size_kv: int = Field(default=200, ge=1)
    batch_size_graph: int = Field(default=200, ge=1)
    batch_size_vector: int = Field(default=100, ge=1)


class IndexingConfig(BaseModel):
    input: InputConfig
    output: OutputConfig
    checkpoint: CheckpointConfig
    indexing: IndexingRuntimeConfig
    embedding: EmbeddingConfig
    llm: LLMConfig
    lightrag: LightRAGConfig
    publish: PublishConfig = Field(default_factory=PublishConfig)


@dataclass(frozen=True, slots=True)
class ResolvedRuntimeConfig:
    project_root: Path
    config_path: Path
    data_root: Path
    input_dir: Path
    input_pattern: str
    output_dir: Path
    checkpoint_path: Path
    lightrag_log_file_path: Path
    batch_size: int
    max_retries: int
    max_async: int
    resume: bool
    llm_model_name: str
    embedding_model_name: str
    llm_api_key_env: str
    llm_api_key: str
    llm_max_tokens: int
    llm_temperature: float
    lightrag_chunk_token_size: int
    lightrag_chunk_overlap_token_size: int
    lightrag_enable_rerank: bool
    lightrag_enable_llm_cache: bool
    publish_enabled: bool
    publish_verify_after_publish: bool
    publish_batch_size_kv: int
    publish_batch_size_graph: int
    publish_batch_size_vector: int


def should_log_to_file() -> bool:
    return os.getenv("LOG_TO_FILE", "true").lower() == "true"


def build_log_handlers() -> list[logging.Handler]:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if should_log_to_file():
        LOG_DIR.mkdir(exist_ok=True)
        handlers.append(logging.FileHandler(LOG_DIR / LOG_FILE_NAME, encoding="utf-8"))
    return handlers


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=build_log_handlers(),
)


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue

        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def load_config(config_path: Path | None = None) -> IndexingConfig:
    resolved_path = config_path or CONFIG_PATH
    with resolved_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    return IndexingConfig.model_validate(data)


def resolve_data_root() -> Path:
    return Path(os.getenv("DATA_DIR", str(PROJECT_ROOT)))


def resolve_path(base: Path, raw_path: str) -> Path:
    candidate = Path(raw_path)
    if candidate.is_absolute():
        return candidate
    return (base / candidate).resolve()


def resolve_model_name(env_name: str, default_model_name: str) -> str:
    value = os.getenv(env_name, "").strip()
    return value or default_model_name


def resolve_runtime_config(config: IndexingConfig) -> ResolvedRuntimeConfig:
    data_root = resolve_data_root()
    input_dir = resolve_path(data_root, config.input.base_dir)
    output_dir = resolve_path(data_root, config.output.base_dir)
    checkpoint_path = output_dir / config.checkpoint.filename
    llm_api_key = os.getenv(config.llm.api_key_env, "").strip()

    return ResolvedRuntimeConfig(
        project_root=PROJECT_ROOT,
        config_path=CONFIG_PATH,
        data_root=data_root,
        input_dir=input_dir,
        input_pattern=config.input.pattern,
        output_dir=output_dir,
        checkpoint_path=checkpoint_path,
        lightrag_log_file_path=(LOG_DIR / LIGHTRAG_LOG_FILE_NAME).resolve(),
        batch_size=config.indexing.batch_size,
        max_retries=config.indexing.max_retries,
        max_async=config.indexing.max_async,
        resume=config.indexing.resume,
        llm_model_name=resolve_model_name(config.llm.model_env, config.llm.default_model_name),
        embedding_model_name=resolve_model_name(
            config.embedding.model_env,
            config.embedding.default_model_name,
        ),
        llm_api_key_env=config.llm.api_key_env,
        llm_api_key=llm_api_key,
        llm_max_tokens=config.llm.max_tokens,
        llm_temperature=config.llm.temperature,
        lightrag_chunk_token_size=config.lightrag.chunk_token_size,
        lightrag_chunk_overlap_token_size=config.lightrag.chunk_overlap_token_size,
        lightrag_enable_rerank=config.lightrag.enable_rerank,
        lightrag_enable_llm_cache=config.lightrag.enable_llm_cache,
        publish_enabled=config.publish.enabled,
        publish_verify_after_publish=config.publish.verify_after_publish,
        publish_batch_size_kv=config.publish.batch_size_kv,
        publish_batch_size_graph=config.publish.batch_size_graph,
        publish_batch_size_vector=config.publish.batch_size_vector,
    )


def validate_runtime_config(runtime: ResolvedRuntimeConfig) -> list[str]:
    errors: list[str] = []

    if not runtime.llm_api_key and not runtime.publish_enabled:
        errors.append(
            f"Thiếu API key bắt buộc trong biến môi trường {runtime.llm_api_key_env}"
        )

    if not runtime.input_dir.exists():
        errors.append(f"Không tìm thấy thư mục input: {runtime.input_dir}")
    elif not runtime.input_dir.is_dir():
        errors.append(f"Đường dẫn input không phải thư mục: {runtime.input_dir}")

    input_files = list(sorted(runtime.input_dir.glob(runtime.input_pattern)))
    if runtime.input_dir.exists() and not input_files:
        errors.append(
            f"Không có file nào khớp pattern '{runtime.input_pattern}' trong {runtime.input_dir}"
        )

    if runtime.batch_size < runtime.max_async:
        logger.warning(
            "batch_size (%s) nhỏ hơn max_async (%s); mức song song có thể chưa được tận dụng hết",
            runtime.batch_size,
            runtime.max_async,
        )

    if runtime.output_dir == runtime.input_dir:
        errors.append("Thư mục output phải khác thư mục input")

    if runtime.checkpoint_path.parent != runtime.output_dir:
        errors.append(
            "File checkpoint phải nằm trong thư mục output đã cấu hình để resume nhất quán"
        )

    return errors


def bootstrap_output_directory(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)


def count_input_files(input_dir: Path, pattern: str) -> int:
    return sum(1 for _ in input_dir.glob(pattern))


def log_startup_summary(runtime: ResolvedRuntimeConfig) -> None:
    logger.info("File cấu hình: %s", runtime.config_path)
    logger.info("Data root: %s", runtime.data_root)
    logger.info(
        "Thư mục input: %s (%s file khớp '%s')",
        runtime.input_dir,
        count_input_files(runtime.input_dir, runtime.input_pattern),
        runtime.input_pattern,
    )
    logger.info("Thư mục output: %s", runtime.output_dir)
    logger.info("File checkpoint: %s", runtime.checkpoint_path)
    logger.info("Model LLM: %s", runtime.llm_model_name)
    logger.info("Model embedding: %s", runtime.embedding_model_name)
    logger.info(
        "Cấu hình indexing: batch_size=%s max_retries=%s max_async=%s resume=%s",
        runtime.batch_size,
        runtime.max_retries,
        runtime.max_async,
        runtime.resume,
    )
    logger.info(
        "Cấu hình LightRAG: chunk_token_size=%s overlap=%s rerank=%s llm_cache=%s",
        runtime.lightrag_chunk_token_size,
        runtime.lightrag_chunk_overlap_token_size,
        runtime.lightrag_enable_rerank,
        runtime.lightrag_enable_llm_cache,
    )
    logger.info(
        "Publish config: enabled=%s verify=%s kv_batch=%s graph_batch=%s vector_batch=%s",
        runtime.publish_enabled,
        runtime.publish_verify_after_publish,
        runtime.publish_batch_size_kv,
        runtime.publish_batch_size_graph,
        runtime.publish_batch_size_vector,
    )


def load_or_reset_checkpoint(runtime: ResolvedRuntimeConfig) -> IndexingCheckpoint:
    if runtime.resume:
        return load_checkpoint(runtime.checkpoint_path)

    logger.info("resume=false, tạo checkpoint mới và bỏ qua trạng thái cũ")
    return create_empty_checkpoint()


def select_documents_for_indexing(
    documents: list[dict[str, Any]],
    checkpoint: IndexingCheckpoint,
    *,
    resume: bool,
) -> list[dict[str, Any]]:
    if not resume:
        return documents

    return [
        document
        for document in documents
        if str(document.get("chunk_id") or "").strip() not in checkpoint.completed_chunk_ids
    ]


def build_batches(documents: list[dict[str, Any]], batch_size: int) -> list[list[dict[str, Any]]]:
    return [documents[index : index + batch_size] for index in range(0, len(documents), batch_size)]


def build_batch_track_id(batch_index: int) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"insert_batch{batch_index}_{timestamp}_{uuid4().hex[:8]}"


def _get_status_field(status_doc: Any, field_name: str) -> Any:
    if hasattr(status_doc, field_name):
        return getattr(status_doc, field_name)
    if isinstance(status_doc, dict):
        return status_doc.get(field_name)
    return None


async def log_track_snapshot(
    rag: Any,
    track_id: str,
    *,
    context: str,
) -> None:
    if not hasattr(rag, "aget_docs_by_track_id"):
        return

    try:
        docs = await rag.aget_docs_by_track_id(track_id)
    except Exception as exc:
        logger.warning("Không thể đọc trạng thái track_id=%s sau %s: %s", track_id, context, exc)
        return

    if not docs:
        logger.warning("Không tìm thấy document nào cho track_id=%s sau %s", track_id, context)
        return

    status_counts: Counter[str] = Counter()
    failed_docs: list[str] = []
    processing_docs: list[str] = []

    for doc_id, status_doc in docs.items():
        status = str(_get_status_field(status_doc, "status") or "unknown")
        status_counts[status] += 1

        if status == "failed":
            error_msg = str(_get_status_field(status_doc, "error_msg") or "").strip()
            failed_docs.append(f"{doc_id} ({error_msg or 'unknown error'})")
        elif status == "processing":
            processing_docs.append(str(doc_id))

    counts_text = ", ".join(
        f"{status}={count}" for status, count in sorted(status_counts.items())
    )
    logger.warning(
        "Snapshot sau %s | track_id=%s | total=%s | %s",
        context,
        track_id,
        len(docs),
        counts_text,
    )
    if failed_docs:
        logger.warning("Document failed trong track_id=%s: %s", track_id, "; ".join(failed_docs[:5]))
    if processing_docs:
        logger.warning(
            "Document còn trạng thái processing trong track_id=%s: %s",
            track_id,
            ", ".join(processing_docs[:5]),
        )


async def run_indexing_pipeline(
    runtime: ResolvedRuntimeConfig,
    documents: list[dict[str, Any]],
    checkpoint: IndexingCheckpoint,
) -> int:
    async def run_publish_step() -> int:
        if not runtime.publish_enabled:
            return 0

        publish_config = build_publish_config_from_runtime(runtime)
        logger.info(
            "Bat dau publish artifact tu %s len production storages",
            publish_config.index_dir,
        )
        try:
            summary = await publish_index_to_storage(publish_config)
        except Exception as exc:
            logger.error("Publish artifact that bai: %s", exc, exc_info=True)
            return 1

        logger.info(
            "Publish hoan tat | kv=%s | graph=%s | vectors=%s",
            summary.get("kv_counts"),
            summary.get("graph"),
            summary.get("vectors"),
        )
        verification = summary.get("verification")
        if verification is not None:
            logger.info("Publish verification: %s", verification)
        return 0

    pending_documents = select_documents_for_indexing(
        documents,
        checkpoint,
        resume=runtime.resume,
    )

    logger.info("Tổng số document cần index trong lần chạy này: %s", len(pending_documents))
    logger.info(
        "Số document bỏ qua do đã completed trước đó: %s",
        len(documents) - len(pending_documents),
    )

    if not pending_documents:
        save_checkpoint(runtime.checkpoint_path, checkpoint)
        logger.info("Không còn document nào cần index")
        return await run_publish_step()

    client_config = build_client_config_from_runtime(runtime)
    rag = await build_lightrag_client(client_config)
    batches = build_batches(pending_documents, runtime.batch_size)
    failed_batches = 0

    for batch_index, batch_documents in enumerate(batches, start=1):
        batch_chunk_ids = [str(document["chunk_id"]).strip() for document in batch_documents]
        total_attempts = runtime.max_retries + 1
        batch_track_id = build_batch_track_id(batch_index)
        logger.info(
            "Xử lý batch %s/%s gồm %s document | track_id=%s",
            batch_index,
            len(batches),
            len(batch_documents),
            batch_track_id,
        )

        for attempt in range(1, total_attempts + 1):
            try:
                track_id = await index_documents(rag, batch_documents, track_id=batch_track_id)
                for chunk_id in batch_chunk_ids:
                    mark_chunk_completed(checkpoint, chunk_id)
                save_checkpoint(runtime.checkpoint_path, checkpoint)

                logger.info(
                    "Batch %s/%s thành công | track_id=%s | completed=%s | failed=%s",
                    batch_index,
                    len(batches),
                    track_id,
                    len(checkpoint.completed_chunk_ids),
                    len(checkpoint.failed_chunks),
                )
                break
            except KeyboardInterrupt:
                await log_track_snapshot(
                    rag,
                    batch_track_id,
                    context=f"batch {batch_index}/{len(batches)} bị gián đoạn",
                )
                logger.error(
                    "Batch %s/%s bị gián đoạn bởi KeyboardInterrupt | track_id=%s",
                    batch_index,
                    len(batches),
                    batch_track_id,
                )
                raise
            except Exception as exc:
                error_message = str(exc).strip() or exc.__class__.__name__
                await log_track_snapshot(
                    rag,
                    batch_track_id,
                    context=f"batch {batch_index}/{len(batches)} lỗi lần {attempt}/{total_attempts}",
                )
                if attempt < total_attempts:
                    logger.warning(
                        "Batch %s/%s thất bại lần %s/%s, sẽ thử lại: %s",
                        batch_index,
                        len(batches),
                        attempt,
                        total_attempts,
                        error_message,
                    )
                    await asyncio.sleep(min(attempt, 3))
                    continue

                failed_batches += 1
                for chunk_id in batch_chunk_ids:
                    mark_chunk_failed(checkpoint, chunk_id, error_message)
                save_checkpoint(runtime.checkpoint_path, checkpoint)

                logger.error(
                    "Batch %s/%s thất bại sau %s lần: %s",
                    batch_index,
                    len(batches),
                    total_attempts,
                    error_message,
                )

    if failed_batches:
        logger.error("Hoàn tất với %s batch thất bại", failed_batches)
        return 1

    logger.info("Indexing hoàn tất thành công")
    return await run_publish_step()


def main() -> int:
    logger.info("\n%s", "=" * 80)
    logger.info("BẮT ĐẦU PHASE 5: PIPELINE INDEXING LIGHTRAG")
    logger.info("%s", "=" * 80)

    load_env_file(PROJECT_ROOT / ".env")

    try:
        config = load_config()
        runtime = resolve_runtime_config(config)
        errors = validate_runtime_config(runtime)
        if errors:
            for error in errors:
                logger.error(error)
            return 1

        bootstrap_output_directory(runtime.output_dir)
        log_startup_summary(runtime)

        checkpoint = load_or_reset_checkpoint(runtime)
        chunks = load_chunks(runtime.input_dir, runtime.input_pattern)
        documents = format_chunk_documents(chunks)

        logger.info("Tổng số chunk đã load: %s", len(chunks))
        logger.info("Tổng số document đã format: %s", len(documents))
        logger.info(
            "Checkpoint hiện tại: completed=%s failed=%s",
            len(checkpoint.completed_chunk_ids),
            len(checkpoint.failed_chunks),
        )
        if documents:
            sample_document = documents[0]
            logger.info("Document mẫu: %s", sample_document["chunk_id"])
            logger.info(
                "Preview text index: %s",
                str(sample_document["text"])[:250].replace("\n", " | "),
            )

        result = asyncio.run(run_indexing_pipeline(runtime, documents, checkpoint))
        logger.info("%s", "=" * 80)
        return result

    except Exception as exc:
        logger.error("Khởi tạo pipeline indexing thất bại: %s", exc, exc_info=True)
        return 1
    except KeyboardInterrupt:
        logger.error("Pipeline indexing bị dừng bởi KeyboardInterrupt hoặc terminal interruption")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
