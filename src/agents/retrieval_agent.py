"""Retrieval Agent — queries LightRAG to build legal context.

Inputs:  AuditState.chunks, AuditState.contract_domain
Outputs: AuditState.legal_context

Queries LightRAG hybrid (Neo4j graph + Qdrant vector + PG KV) for each clause.
Semaphore(3) caps concurrent queries.
Deduplicates passages by MD5 of first 100 chars.
Caps each result to 3000 chars to avoid downstream token overflow.
Reads from production storage only — never from lightrag_index/ JSON files.
"""

from __future__ import annotations

import asyncio
import hashlib
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

    rag = await get_rag_client()
    sem = asyncio.Semaphore(3)

    async def _query(clause: str) -> str:
        async with sem:
            try:
                result = await query_hybrid(rag, clause, top_k=5)
                return (result or "")[:1000]
            except Exception as exc:
                logger.warning("retrieval_agent: query failed: %s", exc)
                return ""

    raw_results = await asyncio.gather(*[_query(c) for c in chunks])

    seen: set[str] = set()
    sections: list[str] = []
    for i, (chunk, result) in enumerate(zip(chunks, raw_results), 1):
        if result:
            key = hashlib.md5(result[:100].encode()).hexdigest()
            if key not in seen:
                seen.add(key)
                sections.append(f"### Điều khoản {i}\n{result}\n")

    legal_context = "\n".join(sections)
    logger.info(
        "retrieval_agent: %d/%d clauses with context, %d unique sections, %d total chars",
        sum(1 for r in raw_results if r),
        len(chunks),
        len(sections),
        len(legal_context),
    )
    return {"legal_context": legal_context}
