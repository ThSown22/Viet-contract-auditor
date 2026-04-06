"""LangGraph StateGraph wiring for the audit pipeline.

Graph:
    START → router → [ok: retrieval | error: generator] → audit → generator → END

This is the only file in src/ that imports from langgraph.
"""

from __future__ import annotations

import logging

from langgraph.graph import END, StateGraph

from agents.audit_agent import audit_node
from agents.generator_agent import generator_node
from agents.retrieval_agent import retrieval_node
from agents.router_agent import route_after_router, router_node
from core.state import AuditState

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Build and compile the StateGraph
# ---------------------------------------------------------------------------

_builder = StateGraph(AuditState)

_builder.add_node("router", router_node)
_builder.add_node("retrieval", retrieval_node)
_builder.add_node("audit", audit_node)
_builder.add_node("generator", generator_node)

_builder.set_entry_point("router")
_builder.add_conditional_edges(
    "router",
    route_after_router,
    {"ok": "retrieval", "error": "generator"},
)
_builder.add_edge("retrieval", "audit")
_builder.add_edge("audit", "generator")
_builder.add_edge("generator", END)

app = _builder.compile()

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def run_audit(contract_text: str) -> AuditState:
    """Run the full 4-agent audit pipeline on a contract text string.

    Returns the final AuditState. Check:
      state["final_report"] for the Markdown output
      state["error"]        for pipeline failures
      state["confidence_score"] for retrieval quality (0.0 in stub mode)
    """
    initial: AuditState = {
        "contract_text": contract_text,
        "contract_domain": "",
        "chunks": [],
        "legal_context": "",
        "audit_findings": [],
        "final_report": "",
        "confidence_score": 0.0,
        "error": None,
    }

    logger.info("run_audit: starting pipeline (%d chars)", len(contract_text))
    result: AuditState = await app.ainvoke(initial)
    logger.info(
        "run_audit: done — domain=%s, chunks=%d, findings=%d, confidence=%.2f",
        result.get("contract_domain"),
        len(result.get("chunks", [])),
        len(result.get("audit_findings", [])),
        result.get("confidence_score", 0.0),
    )
    return result
