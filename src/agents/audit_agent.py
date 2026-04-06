"""Audit Agent — cross-checks contract clauses against legal context.

Inputs:  AuditState.chunks, AuditState.legal_context
Outputs: AuditState.audit_findings, AuditState.confidence_score

# TODO: replace stub with real gpt-4o-mini CoT calls when OPENAI_API_KEY is available:
#   - For each clause, call gpt-4o-mini with AUDIT_SYSTEM_PROMPT + clause + legal_context
#   - response_format={"type": "json_object"} for structured output
#   - Process in batches of 5 with asyncio.gather + semaphore(5)
#   - Validate each finding has: clause, violation, reference_law, suggested_fix keys
#   - confidence_score = fraction of clauses with non-empty reference_law
#   - Single clause failure → log + continue; >50% failure → set state["error"]
"""

from __future__ import annotations

import logging

from core.state import AuditState

logger = logging.getLogger(__name__)

_REQUIRED_FINDING_KEYS = {"clause", "violation", "reference_law", "suggested_fix"}


async def audit_node(state: AuditState) -> dict:
    """LangGraph node: detect violations in each contract clause."""
    chunks = state.get("chunks", [])

    if not chunks:
        logger.warning("audit_agent: no chunks to audit")
        return {"audit_findings": [], "confidence_score": 0.0}

    # TODO: replace with real implementation:
    #   import asyncio, json
    #   from openai import AsyncOpenAI
    #   from core.prompts import AUDIT_SYSTEM_PROMPT
    #   client = AsyncOpenAI()
    #   sem = asyncio.Semaphore(5)
    #   legal_context = state.get("legal_context", "")
    #   all_findings: list[dict] = []
    #   failed = 0
    #
    #   async def _audit_clause(chunk: str) -> list[dict]:
    #       async with sem:
    #           try:
    #               response = await client.chat.completions.create(
    #                   model="gpt-4o-mini",
    #                   response_format={"type": "json_object"},
    #                   messages=[{
    #                       "role": "user",
    #                       "content": AUDIT_SYSTEM_PROMPT.format(
    #                           clause=chunk,
    #                           legal_context=legal_context[:3000],
    #                       ),
    #                   }],
    #               )
    #               data = json.loads(response.choices[0].message.content)
    #               findings = data if isinstance(data, list) else data.get("findings", [])
    #               return [f for f in findings if _REQUIRED_FINDING_KEYS.issubset(f)]
    #           except Exception as exc:
    #               logger.warning("audit_agent: clause failed: %s", exc)
    #               return []
    #
    #   results = await asyncio.gather(*[_audit_clause(c) for c in chunks])
    #   for r in results:
    #       if r is None:
    #           failed += 1
    #       else:
    #           all_findings.extend(r)
    #
    #   if failed > len(chunks) // 2:
    #       return {"audit_findings": all_findings, "confidence_score": 0.0,
    #               "error": f"audit_agent: {failed}/{len(chunks)} clauses failed"}
    #
    #   scored = sum(1 for f in all_findings if f.get("reference_law"))
    #   confidence = scored / len(all_findings) if all_findings else 0.0

    logger.warning("STUB: audit_agent — placeholder findings for %d clause(s)", len(chunks))

    stub_findings = [
        {
            "clause": chunk[:120].replace("\n", " "),
            "violation": "[STUB] Chưa phân tích — cần OPENAI_API_KEY để chạy kiểm toán thực",
            "reference_law": "",
            "suggested_fix": "[STUB] Chưa có khuyến nghị sửa đổi",
        }
        for chunk in chunks
    ]

    # confidence_score = 0.0 in stub mode (no reference_law populated)
    confidence_score = 0.0

    logger.info(
        "audit_agent: %d stub finding(s), confidence=%.2f",
        len(stub_findings),
        confidence_score,
    )
    return {"audit_findings": stub_findings, "confidence_score": confidence_score}
