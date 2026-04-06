"""Retrieval Agent — queries LightRAG to build legal context.

Inputs:  AuditState.chunks, AuditState.contract_domain
Outputs: AuditState.legal_context

# TODO: replace stub with real LightRAG hybrid queries when OPENAI_API_KEY is available:
#   - asyncio.gather with semaphore (max 3 concurrent) over chunks
#   - query_hybrid(rag, clause_text, top_k=10) per clause
#   - Trim each result to max 3000 chars to avoid downstream token overflow
#   - Deduplicate passages by hashing the first 100 chars of each paragraph
#   - CRITICAL: reads from production storage ONLY — never from lightrag_index/ JSON files
"""

from __future__ import annotations

import logging

from core.lightrag_client import get_rag_client, query_hybrid
from core.state import AuditState

logger = logging.getLogger(__name__)


async def retrieval_node(state: AuditState) -> dict:
    """LangGraph node: retrieve relevant law passages for each contract clause."""
    chunks = state.get("chunks", [])

    if not chunks:
        logger.warning("retrieval_agent: no chunks to retrieve for")
        return {"legal_context": ""}

    # TODO: replace with real implementation:
    #   import asyncio, hashlib
    #   rag = await get_rag_client()
    #   sem = asyncio.Semaphore(3)
    #   async def _query(clause: str) -> str:
    #       async with sem:
    #           try:
    #               result = await query_hybrid(rag, clause, top_k=10)
    #               return result[:3000]  # cap to avoid token overflow
    #           except Exception as exc:
    #               logger.warning("retrieval_agent: query failed for clause: %s", exc)
    #               return ""
    #   raw_results = await asyncio.gather(*[_query(c) for c in chunks])
    #   # Deduplicate passages
    #   seen: set[str] = set()
    #   sections = []
    #   for i, (chunk, result) in enumerate(zip(chunks, raw_results), 1):
    #       if result:
    #           key = hashlib.md5(result[:100].encode()).hexdigest()
    #           if key not in seen:
    #               seen.add(key)
    #               sections.append(f"### Điều khoản {i}\n{result}\n")
    #   legal_context = "\n".join(sections)

    logger.warning("STUB: retrieval_agent — returning placeholder legal_context for %d clause(s)", len(chunks))

    sections: list[str] = []
    for i, chunk in enumerate(chunks, 1):
        stub_result = await query_hybrid(None, chunk)
        preview = chunk[:60].replace("\n", " ")
        sections.append(f"### Điều khoản {i}: {preview}...\n{stub_result}\n")

    legal_context = "\n".join(sections)
    logger.info("retrieval_agent: assembled legal_context (%d chars)", len(legal_context))
    return {"legal_context": legal_context}
