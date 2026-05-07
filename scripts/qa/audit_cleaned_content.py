"""Audit cleaned legal text outputs for residual structural or source artifacts."""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.legal_text_patterns import is_structural_marker


CLEANED_DIR = PROJECT_ROOT / "data" / "ingestion" / "cleaned_content"
BLANK_ARTICLE_RE = re.compile(r"^Điều\s+\d+\.$")
ORPHAN_LINE_RE = re.compile(r"^[A-Za-zÀ-ỹĐđ]{1,12}(?:\s+[A-Za-zÀ-ỹĐđ]{1,12})?$")
ARTIFACT_MARKERS = (
    "CÔNG BÁO",
    "VĂN PHÒNG QUỐC HỘI",
    "Xem tiếp Công báo",
    "Tiếp theo Công báo",
    "PHẦN VĂN BẢN QUY PHẠM PHÁP LUẬT",
)
LIST_PREFIXES = tuple(f"{number}." for number in range(1, 10)) + ("a)", "b)", "c)", "d)", "đ)")


@dataclass(slots=True)
class AuditFinding:
    file_name: str
    line_no: int
    reason: str
    line: str


def audit_file(path: Path) -> list[AuditFinding]:
    findings: list[AuditFinding] = []
    for line_no, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        if any(marker in line for marker in ARTIFACT_MARKERS):
            findings.append(AuditFinding(path.name, line_no, "source_artifact", line))
            continue
        if is_structural_marker(line):
            findings.append(AuditFinding(path.name, line_no, "structural_marker", line))
            continue
        if BLANK_ARTICLE_RE.match(line):
            findings.append(AuditFinding(path.name, line_no, "blank_article_header", line))
            continue
        if ORPHAN_LINE_RE.match(line) and not line.startswith(("Điều ", *LIST_PREFIXES)):
            findings.append(AuditFinding(path.name, line_no, "short_orphan_line", line))
    return findings


def main() -> int:
    files = sorted(CLEANED_DIR.glob("*.txt"))
    if not files:
        print(f"No cleaned files found in {CLEANED_DIR}")
        return 1

    findings = [finding for path in files for finding in audit_file(path)]
    if not findings:
        print(f"Audit passed: {len(files)} file(s), 0 findings.")
        return 0

    print(f"Audit found {len(findings)} issue(s) across {len(files)} file(s):")
    for finding in findings:
        print(f"{finding.file_name}:{finding.line_no} [{finding.reason}] {finding.line}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
