from __future__ import annotations

import logging
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any

from lightrag import LightRAG
from lightrag.llm.openai import openai_complete_if_cache, openai_embed
from lightrag.prompt import PROMPTS
from lightrag.utils import EmbeddingFunc, setup_logger

from .prompts import apply_vietnamese_legal_prompts


logger = logging.getLogger(__name__)
EMBEDDING_DIM_ENV = "OPENAI_EMBEDDING_DIM"


@dataclass(frozen=True, slots=True)
class LightRAGClientConfig:
    working_dir: Path
    log_file_path: Path
    llm_model_name: str
    llm_api_key: str
    llm_max_tokens: int
    llm_temperature: float
    embedding_model_name: str
    max_async: int
    chunk_token_size: int
    chunk_overlap_token_size: int
    enable_rerank: bool = False
    enable_llm_cache: bool = False
    openai_api_base: str | None = None


def _normalize_api_base(base_url: str | None = None) -> str | None:
    candidates = (
        base_url,
        os.getenv("OPENAI_API_BASE"),
        os.getenv("OPENAI_BASE_URL"),
    )
    for candidate in candidates:
        value = (candidate or "").strip()
        if value:
            return value
    return None


def _validate_client_config(config: LightRAGClientConfig) -> None:
    errors: list[str] = []

    if not config.llm_model_name.strip():
        errors.append("Thiếu tên model LLM")
    if not config.embedding_model_name.strip():
        errors.append("Thiếu tên model embedding")
    if not config.llm_api_key.strip():
        errors.append("Thiếu OPENAI_API_KEY để khởi tạo LightRAG client")
    if config.max_async < 1:
        errors.append("max_async phải lớn hơn hoặc bằng 1")
    if config.chunk_token_size < 1:
        errors.append("chunk_token_size phải lớn hơn hoặc bằng 1")
    if config.chunk_overlap_token_size < 0:
        errors.append("chunk_overlap_token_size không được âm")
    if config.chunk_overlap_token_size >= config.chunk_token_size:
        errors.append("chunk_overlap_token_size phải nhỏ hơn chunk_token_size")
    if config.enable_rerank:
        errors.append("enable_rerank=true chưa được hỗ trợ trong pipeline hiện tại")

    if errors:
        raise ValueError("; ".join(errors))


def _apply_prompt_overrides() -> None:
    apply_vietnamese_legal_prompts(PROMPTS)


def _configure_lightrag_logger(config: LightRAGClientConfig) -> None:
    setup_logger(
        "lightrag",
        level=os.getenv("LIGHTRAG_LOG_LEVEL", "INFO"),
        log_file_path=str(config.log_file_path),
        enable_file_logging=True,
    )


def _resolve_embedding_dimension(config: LightRAGClientConfig) -> int:
    raw_env_value = os.getenv(EMBEDDING_DIM_ENV, "").strip()
    if raw_env_value:
        try:
            value = int(raw_env_value)
        except ValueError as exc:
            raise ValueError(
                f"{EMBEDDING_DIM_ENV} phải là số nguyên dương, hiện tại nhận được: {raw_env_value}"
            ) from exc
        if value < 1:
            raise ValueError(f"{EMBEDDING_DIM_ENV} phải lớn hơn hoặc bằng 1")
        return value

    default_model_name = (openai_embed.model_name or "").strip()
    default_dimension = int(openai_embed.embedding_dim or 0)
    if config.embedding_model_name == default_model_name and default_dimension > 0:
        return default_dimension

    raise ValueError(
        "Không xác định được embedding_dim cho model "
        f"{config.embedding_model_name}. "
        f"Hãy khai báo {EMBEDDING_DIM_ENV} trong .env "
        "hoặc dùng model mặc định text-embedding-3-small."
    )


def _build_embedding_func(config: LightRAGClientConfig) -> EmbeddingFunc:
    embedding_dim = _resolve_embedding_dimension(config)
    embedding_callable = partial(
        openai_embed.func,
        model=config.embedding_model_name,
        api_key=config.llm_api_key,
        base_url=config.openai_api_base,
    )

    logger.info(
        "Khởi tạo embedding function: model=%s dim=%s",
        config.embedding_model_name,
        embedding_dim,
    )

    return EmbeddingFunc(
        embedding_dim=embedding_dim,
        func=embedding_callable,
        max_token_size=config.chunk_token_size,
        model_name=config.embedding_model_name,
    )


def _build_llm_model_func(config: LightRAGClientConfig):
    async def llm_model_func(
        prompt: str,
        system_prompt: str | None = None,
        history_messages: list[dict[str, Any]] | None = None,
        **kwargs: Any,
    ) -> str:
        request_kwargs = dict(kwargs)
        request_kwargs.setdefault("max_tokens", config.llm_max_tokens)
        request_kwargs.setdefault("temperature", config.llm_temperature)

        return await openai_complete_if_cache(
            config.llm_model_name,
            prompt,
            system_prompt=system_prompt,
            history_messages=history_messages or [],
            api_key=config.llm_api_key,
            base_url=config.openai_api_base,
            **request_kwargs,
        )

    return llm_model_func


def _is_tiktoken_cache_error(exc: Exception) -> bool:
    message = str(exc)
    return (
        "openaipublic.blob.core.windows.net" in message
        or "cl100k_base.tiktoken" in message
        or "Failed to establish a new connection" in message
    )


def _rewrite_runtime_error(exc: Exception) -> Exception:
    if _is_tiktoken_cache_error(exc):
        return RuntimeError(
            "Thiếu tokenizer cache của tiktoken hoặc môi trường hiện tại đang chặn mạng. "
            "Lần chạy đầu cần tải file tokenizer như cl100k_base trước khi index thật."
        )
    return exc


async def build_lightrag_client(config: LightRAGClientConfig) -> LightRAG:
    _validate_client_config(config)
    _configure_lightrag_logger(config)
    _apply_prompt_overrides()

    config.working_dir.mkdir(parents=True, exist_ok=True)

    embedding_func = _build_embedding_func(config)
    llm_model_func = _build_llm_model_func(config)

    logger.info(
        "Khởi tạo LightRAG client: llm=%s embedding=%s output=%s",
        config.llm_model_name,
        config.embedding_model_name,
        config.working_dir,
    )

    try:
        rag = LightRAG(
            working_dir=str(config.working_dir),
            llm_model_name=config.llm_model_name,
            llm_model_func=llm_model_func,
            llm_model_max_async=config.max_async,
            llm_model_kwargs={
                "max_tokens": config.llm_max_tokens,
                "temperature": config.llm_temperature,
            },
            embedding_func=embedding_func,
            embedding_func_max_async=config.max_async,
            chunk_token_size=config.chunk_token_size,
            chunk_overlap_token_size=config.chunk_overlap_token_size,
            tiktoken_model_name=config.llm_model_name,
            enable_llm_cache=config.enable_llm_cache,
        )
        await rag.initialize_storages()
    except Exception as exc:
        raise _rewrite_runtime_error(exc) from exc

    logger.info("LightRAG client đã sẵn sàng")
    return rag


def build_client_config_from_runtime(runtime: Any) -> LightRAGClientConfig:
    return LightRAGClientConfig(
        working_dir=Path(runtime.output_dir),
        log_file_path=Path(runtime.lightrag_log_file_path),
        llm_model_name=str(runtime.llm_model_name),
        llm_api_key=str(runtime.llm_api_key),
        llm_max_tokens=int(runtime.llm_max_tokens),
        llm_temperature=float(runtime.llm_temperature),
        embedding_model_name=str(runtime.embedding_model_name),
        max_async=int(runtime.max_async),
        chunk_token_size=int(runtime.lightrag_chunk_token_size),
        chunk_overlap_token_size=int(runtime.lightrag_chunk_overlap_token_size),
        enable_rerank=bool(runtime.lightrag_enable_rerank),
        enable_llm_cache=bool(runtime.lightrag_enable_llm_cache),
        openai_api_base=_normalize_api_base(),
    )


def _extract_index_payload(documents: Sequence[Mapping[str, Any]]) -> tuple[list[str], list[str]]:
    if not documents:
        raise ValueError("Danh sách document rỗng")

    seen_chunk_ids: set[str] = set()
    texts: list[str] = []
    chunk_ids: list[str] = []

    for index, document in enumerate(documents, start=1):
        chunk_id = str(document.get("chunk_id") or "").strip()
        text = str(document.get("text") or "").strip()

        if not chunk_id:
            raise ValueError(f"Document thứ {index} thiếu chunk_id")
        if not text:
            raise ValueError(f"Document {chunk_id} thiếu text để index")
        if chunk_id in seen_chunk_ids:
            raise ValueError(f"Chunk ID bị trùng trong batch index: {chunk_id}")

        seen_chunk_ids.add(chunk_id)
        chunk_ids.append(chunk_id)
        texts.append(text)

    return texts, chunk_ids


async def index_documents(
    rag: LightRAG,
    documents: Sequence[Mapping[str, Any]],
    *,
    track_id: str | None = None,
) -> str:
    texts, chunk_ids = _extract_index_payload(documents)

    logger.info("Bắt đầu index %s document vào LightRAG", len(texts))
    try:
        resolved_track_id = await rag.ainsert(texts, ids=chunk_ids, track_id=track_id)
    except Exception as exc:
        raise _rewrite_runtime_error(exc) from exc

    logger.info("Đã enqueue indexing với track_id=%s", resolved_track_id)
    return resolved_track_id
