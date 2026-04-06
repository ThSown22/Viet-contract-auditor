"""Router Agent — classifies contract domain and splits into clauses.

Inputs:  AuditState.contract_text
Outputs: AuditState.contract_domain, AuditState.chunks

Classification logic:
  1. Keyword-based (pure regex, no API call)
  2. Stub fallback when keywords are ambiguous: default to "Thương mại" + log warning
     # TODO: replace stub with gpt-4o-mini call using ROUTER_SYSTEM_PROMPT

Clause splitting logic:
  1. Pure-regex split at Điều boundaries
  2. If < 3 clauses found: keep as-is + log warning
     # TODO: replace stub fallback with gpt-4o-mini call using CLAUSE_SPLIT_SYSTEM_PROMPT
"""

from __future__ import annotations

import logging

from core.legal_patterns import classify_domain_by_keywords, split_contract_into_clauses
from core.state import AuditState

logger = logging.getLogger(__name__)


async def router_node(state: AuditState) -> dict:
    """LangGraph node: classify domain and split contract into clauses."""
    contract_text = state.get("contract_text", "")

    if not contract_text.strip():
        logger.error("router_agent: contract_text is empty")
        return {
            "error": "router_agent: contract_text is empty",
            "contract_domain": "",
            "chunks": [],
        }

    # --- Domain classification ---
    domain = classify_domain_by_keywords(contract_text)

    if domain is None:
        # TODO: replace stub with gpt-4o-mini API call:
        #   from openai import AsyncOpenAI
        #   import json
        #   from core.prompts import ROUTER_SYSTEM_PROMPT
        #   client = AsyncOpenAI()
        #   try:
        #       response = await client.chat.completions.create(
        #           model="gpt-4o-mini",
        #           messages=[
        #               {"role": "system", "content": ROUTER_SYSTEM_PROMPT},
        #               {"role": "user", "content": contract_text[:8000]},
        #           ],
        #       )
        #       result = json.loads(response.choices[0].message.content)
        #       domain = result.get("domain", "Thương mại")
        #   except Exception as exc:
        #       logger.error("router_agent: LLM classification failed: %s", exc)
        #       domain = "Thương mại"
        logger.warning("STUB: router_agent — domain unclear from keywords, defaulting to 'Thương mại'")
        domain = "Thương mại"

    logger.info("router_agent: domain=%s", domain)

    # --- Clause splitting ---
    chunks = split_contract_into_clauses(contract_text)

    if len(chunks) < 3:
        # TODO: replace stub fallback with gpt-4o-mini call using CLAUSE_SPLIT_SYSTEM_PROMPT
        logger.warning(
            "STUB: router_agent — only %d clause(s) found by regex, using as-is", len(chunks)
        )

    logger.info("router_agent: %d clause(s)", len(chunks))
    return {"contract_domain": domain, "chunks": chunks}


def route_after_router(state: AuditState) -> str:
    """Conditional edge: route to 'retrieval' on success, 'generator' on error."""
    if state.get("error"):
        return "error"
    if not state.get("chunks"):
        return "error"
    return "ok"
