"""Generator Agent — formats audit findings into a Vietnamese Markdown report.

Inputs:  AuditState.audit_findings, AuditState.contract_domain, AuditState.confidence_score
Outputs: AuditState.final_report

Two paths:
  - confidence_score >= 0.3: call gpt-4o-mini with GENERATOR_SYSTEM_PROMPT
    # TODO: replace stub with real LLM call when OPENAI_API_KEY is available
  - confidence_score < 0.3: pure template formatter (always active in stub mode)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from core.state import AuditState

logger = logging.getLogger(__name__)


def _template_report(state: AuditState) -> str:
    """Build a Markdown audit report purely from template (no LLM call)."""
    findings = state.get("audit_findings", [])
    domain = state.get("contract_domain", "Chưa xác định")
    confidence = state.get("confidence_score", 0.0)
    chunks = state.get("chunks", [])
    error = state.get("error")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    lines: list[str] = [
        "# Báo cáo Kiểm toán Hợp đồng",
        "",
        "## Tóm tắt",
        "",
    ]

    if error:
        lines.append(f"**Lỗi pipeline:** {error}")
    elif not findings:
        lines.append(
            "Không phát hiện vi phạm nào. "
            "_(Lưu ý: đang chạy ở chế độ STUB — cần OPENAI_API_KEY để phân tích thực sự)_"
        )
    else:
        stub_note = " _(STUB — cần OPENAI_API_KEY)_" if confidence == 0.0 else ""
        lines.append(
            f"Đã kiểm tra **{len(chunks)}** điều khoản, "
            f"phát hiện **{len(findings)}** mục cần xem xét{stub_note}."
        )

    lines += [
        "",
        "## Chi tiết vi phạm",
        "",
    ]

    if not findings:
        lines.append("_(Không có vi phạm nào được phát hiện)_")
    else:
        for i, f in enumerate(findings, 1):
            clause_preview = f.get("clause", "")[:120]
            ref = f.get("reference_law") or "_(chưa xác định)_"
            lines += [
                f"### Vi phạm {i}",
                f"- **Điều khoản:** {clause_preview}",
                f"- **Vi phạm:** {f.get('violation', '')}",
                f"- **Căn cứ pháp lý:** {ref}",
                f"- **Khuyến nghị sửa đổi:** {f.get('suggested_fix', '')}",
                "",
            ]

    lines += [
        "## Khuyến nghị chung",
        "",
        "_(Xem chi tiết từng vi phạm ở trên. "
        "Cần OPENAI_API_KEY để có khuyến nghị tổng hợp từ AI.)_",
        "",
        "---",
        f"*Lĩnh vực: **{domain}** | Điểm tin cậy: {confidence:.2f} | "
        f"Điều khoản: {len(chunks)} | Vi phạm: {len(findings)} | {now}*",
    ]

    return "\n".join(lines)


async def generator_node(state: AuditState) -> dict:
    """LangGraph node: format audit findings into a Vietnamese Markdown report."""
    confidence = state.get("confidence_score", 0.0)
    findings = state.get("audit_findings", [])
    domain = state.get("contract_domain", "")
    error = state.get("error")

    if confidence >= 0.3 and findings and not error:
        # TODO: replace with gpt-4o-mini call when OPENAI_API_KEY is available:
        #   import json
        #   from openai import AsyncOpenAI
        #   from core.prompts import GENERATOR_SYSTEM_PROMPT
        #   client = AsyncOpenAI()
        #   try:
        #       response = await client.chat.completions.create(
        #           model="gpt-4o-mini",
        #           messages=[{
        #               "role": "user",
        #               "content": GENERATOR_SYSTEM_PROMPT.format(
        #                   domain=domain,
        #                   findings_json=json.dumps(findings, ensure_ascii=False, indent=2),
        #               ),
        #           }],
        #       )
        #       report = response.choices[0].message.content
        #       return {"final_report": report}
        #   except Exception as exc:
        #       logger.error("generator_agent: LLM call failed: %s", exc)
        #       # fall through to template
        pass

    logger.warning(
        "STUB: generator_agent — template formatter (confidence=%.2f)", confidence
    )
    report = _template_report(state)
    logger.info("generator_agent: report generated (%d chars)", len(report))
    return {"final_report": report}
