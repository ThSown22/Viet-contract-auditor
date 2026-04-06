"""Shared AuditState TypedDict for the LangGraph audit pipeline."""

from __future__ import annotations

from typing import TypedDict


class AuditState(TypedDict):
    """State passed between all agents in the audit pipeline."""

    contract_text: str          # raw input contract
    contract_domain: str        # "Dân sự" | "Thương mại" | "Lao động" | "Doanh nghiệp"
    chunks: list[str]           # contract split into clauses
    legal_context: str          # Markdown assembled from LightRAG queries
    audit_findings: list[dict]  # [{clause, violation, reference_law, suggested_fix}]
    final_report: str           # formatted Vietnamese Markdown report
    confidence_score: float     # 0.0–1.0
    error: str | None           # per-agent error propagation
