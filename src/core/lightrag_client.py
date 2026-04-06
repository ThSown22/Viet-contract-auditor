"""LightRAG production client — STUB implementation.

# TODO: replace stub functions with real implementation when OPENAI_API_KEY is available.
#
# Real implementation notes (see init_storage.py:_build_rag() lines 261-296 as template):
#
#   from functools import partial
#   from lightrag import LightRAG
#   from lightrag.llm.openai import gpt_4o_mini_complete, openai_embed
#   from lightrag.utils import EmbeddingFunc
#   from lightrag.base import QueryParam
#
#   embed_func = EmbeddingFunc(
#       embedding_dim=384,           # matches stored Qdrant vectors (indexed with MiniLM-L12-v2)
#       func=partial(openai_embed.func, model="text-embedding-3-small"),
#       max_token_size=8192,
#       send_dimensions=True,        # passes dimensions=384 to OpenAI API (Matryoshka truncation)
#   )
#
#   rag = LightRAG(
#       working_dir=os.getenv("LIGHTRAG_WORKING_DIR", "./lightrag_index"),
#       workspace=os.getenv("WORKSPACE", "viet_contract_prod"),
#       kv_storage="PGKVStorage",
#       doc_status_storage="PGDocStatusStorage",
#       graph_storage="Neo4JStorage",
#       vector_storage="QdrantVectorDBStorage",
#       llm_model_func=gpt_4o_mini_complete,
#       embedding_func=embed_func,
#   )
#   await rag.initialize_storages()
#
#   Smoke test: embed = await embed_func(["test"]); assert embed.shape[1] == 384
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


async def get_rag_client():
    """Return the shared LightRAG client (singleton).

    # TODO: replace with real implementation:
    #   - Load .env credentials (OPENAI_API_KEY, NEO4J_*, QDRANT_*, POSTGRES_*)
    #   - Initialize LightRAG with production storage backends
    #   - Run embedding smoke test (assert dim == 384)
    #   - Cache in module-level _rag_instance with asyncio.Lock
    """
    logger.warning("STUB: get_rag_client() — returning None, no storage connection")
    return None


async def query_hybrid(rag, clause_text: str, top_k: int = 10) -> str:
    """Query LightRAG in hybrid mode (Neo4j graph + Qdrant vector + PG KV).

    # TODO: replace with real implementation:
    #   from lightrag.base import QueryParam
    #   return await rag.aquery(
    #       clause_text,
    #       param=QueryParam(mode="hybrid", top_k=top_k, only_need_context=True),
    #   )
    """
    logger.warning("STUB: query_hybrid() — returning placeholder context")
    preview = clause_text[:80].replace("\n", " ")
    return (
        f"[STUB] Chưa có dữ liệu pháp lý thực.\n"
        f"Cần OPENAI_API_KEY và kết nối storage để truy vấn LightRAG.\n"
        f"Điều khoản: {preview}..."
    )
