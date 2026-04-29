import re
import unicodedata
from typing import List


def normalize_text(value: str) -> str:
    """Return lowercase ASCII-ish text for matching."""

    normalized = unicodedata.normalize("NFD", value or "")
    ascii_only = "".join(char for char in normalized if unicodedata.category(char) != "Mn")
    return ascii_only.replace("đ", "d").replace("Đ", "D").lower()


class ContentCleaner:
    """Clean extracted raw legal text while preserving structure."""

    DEFAULT_ARTIFACTS = [
        "trang chu",
        "quay lai",
        "in trang",
        "chia se",
        "facebook",
        "twitter",
        "zalo",
        "email",
        "dang nhap",
        "dang ky",
        "quang cao",
        "lien he",
        "menu",
        "luoc do",
        "thuoc tinh",
        "hoi dap",
        "tin tuc",
    ]

    def __init__(self, validation_config: dict | None = None):
        self.validation_config = validation_config or {}
        blacklist = self.validation_config.get("blacklist_keywords", [])
        self.blacklist = set(self.DEFAULT_ARTIFACTS + [normalize_text(item) for item in blacklist])

    def clean(self, raw_text: str) -> str:
        """Remove obvious UI artifacts and normalize spacing."""

        text = (raw_text or "").replace("\r\n", "\n").replace("\r", "\n").replace("\xa0", " ")
        lines = [self._normalize_line(line) for line in text.split("\n")]

        cleaned_lines: List[str] = []
        previous_line = ""
        for line in lines:
            if not line:
                if cleaned_lines and cleaned_lines[-1] != "":
                    cleaned_lines.append("")
                continue

            if self._is_artifact_line(line):
                continue

            if line == previous_line:
                continue

            cleaned_lines.append(line)
            previous_line = line

        cleaned = "\n".join(cleaned_lines)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
        return cleaned.strip()

    def validate(self, clean_text: str) -> bool:
        """Apply config-driven quality checks."""

        if not clean_text:
            return False

        min_length = int(self.validation_config.get("min_length_chars", 5000))
        if len(clean_text) < min_length:
            return False

        normalized = normalize_text(clean_text)

        required_keywords = self.validation_config.get("required_keywords", [])
        if required_keywords:
            required = [normalize_text(item) for item in required_keywords]
            if not any(item in normalized for item in required):
                return False

        blacklist_keywords = self.validation_config.get("blacklist_keywords", [])
        if blacklist_keywords:
            blacklist = [normalize_text(item) for item in blacklist_keywords]
            if any(item in normalized for item in blacklist):
                return False

        return True

    def has_structure(self, clean_text: str) -> bool:
        normalized = normalize_text(clean_text)
        return any(token in normalized for token in ["dieu 1", "khoan 1", "chuong i"])

    def _normalize_line(self, line: str) -> str:
        stripped = re.sub(r"[ \t]+", " ", line.strip())
        return stripped

    def _is_artifact_line(self, line: str) -> bool:
        normalized = normalize_text(line)
        if not normalized:
            return True
        if normalized in self.blacklist:
            return True
        if len(normalized) < 3:
            return True
        if any(keyword in normalized for keyword in self.blacklist):
            return True
        if normalized.startswith("tai ve") and len(normalized) < 120:
            return True
        return False
