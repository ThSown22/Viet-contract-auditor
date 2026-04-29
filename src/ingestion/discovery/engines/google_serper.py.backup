import os
import re
import time
import requests
import threading
import logging
from typing import List, Optional
from datetime import datetime
from urllib.parse import urlparse
from dotenv import load_dotenv
from src.ingestion.schemas.models import DiscoveredLink

load_dotenv()

logger = logging.getLogger(__name__)

# Regex trích xuất số hiệu luật
LAW_ID_PATTERN = re.compile(
    r'(?:số\s*)?(\d{1,3}/\d{4}/[A-Z]{2}\d{2,4}(?:-[A-Z]{2})?|'  # QH13, NĐ-CP
    r'\d{1,3}/\d{4}/[A-Z]{2}\d{2,4}(?:-[A-Z]{2})?|'                # without "số"
    r'(?:Nghị\s+định|Luật|Nghị\t+định)\s+(?:số\s+)?(\d+/\d{4}/[A-Z]{2}-\d{1,2}))',
    re.IGNORECASE
)

# Alias mapping: config tvpl/congbao → domain thực tế
SOURCE_ALIAS_MAP = {
    "tvpl": "thuvienphapluat.vn",
    "congbao": "congbao.chinhphu.vn",
}

# Patterns cross-law cần lọc (dùng cho bổ sung exact match)
CROSS_PATTERNS = {
    "bộ luật dân sự": [
        r'\b(thi\s*hành\s*án\s*dân\s*sự)\b',
        r'\b(tố\s*tụng\s*dân\s*sự)\b',
        r'\b(tố\s*dụng\s*dân\s*sự)\b',
        r'\b(bộ\s*luật\s*tố\s*tụng)\b',
        r'\b(luật\s*tố\s*tụng)\b',
        r'\b(hình\s*sự)\b',
        r'\b(hành\s*chính)\b',
        r'\b(luật\s*lao\s*động)\b',
        r'\b(đầu\s*tư)\b',
    ],
    "luật doanh nghiệp": [
        r'\b(luật\s*đầu\s*tư)\b',
        r'\b(chứng\s*khoán)\b',
        r'\b(sở\s*hữu\s*trí\s*tuệ)\b',
        r'\b(cạnh\s*tranh)\b',
    ],
    "luật trọng tài thương mại": [
        r'\b(trọng\s*tài\s*quốc\s*tế)\b',
        r'\b(hoà\s*giải)\b',
        r'\b(tố\s*tụng)\b',
    ],
}


class GoogleSerperEngine:
    def __init__(self, config: dict):
        # ── Multi-Key ──────────────────────────────────────────────────────────
        raw_keys = {
            k: v for k, v in os.environ.items()
            if k.startswith("SERPER_KEY_") and v.strip()
        }
        self.api_keys = [raw_keys[k] for k in sorted(raw_keys.keys())]

        if not self.api_keys:
            single_key = os.getenv("SERPER_API_KEY")
            if single_key:
                self.api_keys = [single_key]
            else:
                logger.critical("Khong tim thay bat ky API Key nao!")
                raise ValueError("Thieu SERPER_API_KEY.")

        # ── Config ────────────────────────────────────────────────────────────
        self.settings = config.get("discovery_settings", {})
        self.rate_limit = config.get("rate_limit", {})
        self.current_key_idx = 0
        self.call_count = 0
        self.lock = threading.Lock()
        self.url = "https://google.serper.dev/search"
        self.source_priority = self.settings.get("source_priority", [])
        # Path denylist cho bài viết/Q&A (lớp 2)
        self.path_denylist = self.settings.get("path_denylist", [])
        # Path allowlist cho văn bản chính thức (lớp 1: /van-ban/)
        raw_allowlist = self.settings.get("path_allowlist", {})
        # Chuẩn hóa: keys có thể có hoặc không có domain prefix, giá trị là list
        self.path_allowlist: dict[str, list[str]] = {}
        for domain, prefixes in raw_allowlist.items():
            canon = domain.lower().replace("www.", "")
            if isinstance(prefixes, list):
                self.path_allowlist[canon] = prefixes
        logger.info(f"GoogleSerperEngine da san sang: {len(self.api_keys)} keys, "
                    f"priority={self.source_priority}, "
                    f"allowlist={self.path_allowlist}, denylist={len(self.path_denylist)} paths.")

    def _rotate_key(self):
        with self.lock:
            self.current_key_idx = (self.current_key_idx + 1) % len(self.api_keys)
            self.call_count = 0
            logger.warning(f"Da xoay API Key. Hien dung Key so {self.current_key_idx + 1}")

    def get_headers(self):
        limit = self.rate_limit.get("rotate_keys_every_n_calls", 100)
        if self.call_count >= limit:
            self._rotate_key()
        self.call_count += 1
        return {
            "X-API-KEY": self.api_keys[self.current_key_idx],
            "Content-Type": "application/json"
        }

    def search(
        self,
        query: str,
        query_id: str = "",
        target_law_name: str = "",
        target_law_aliases: list = None,
    ) -> List[DiscoveredLink]:
        """
        Tim kiem va tra ve List[DiscoveredLink] da duoc loc.
        Loc nhieu: cross-law, path denylist, sap xep theo source_priority.
        """
        if target_law_aliases is None:
            target_law_aliases = []

        # ── Domain Locking ───────────────────────────────────────────────────
        target_domains = self.settings.get("target_domains", [])
        if target_domains:
            domain_filter = " OR ".join([f"site:{d}" for d in target_domains])
            search_query = f"{query} ({domain_filter})"
        else:
            search_query = query

        payload = {
            "q": search_query,
            "gl": "vn",
            "hl": "vi",
            "num": self.settings.get("max_results", 10)
        }

        logger.info(f"Thuc hien tim kiem Google: '{query}'")
        time.sleep(self.rate_limit.get("delay_between_queries_sec", 1))

        try:
            response = requests.post(self.url, headers=self.get_headers(), json=payload, timeout=30)
            if response.status_code in [403, 429]:
                logger.error(f"API Key #{self.current_key_idx + 1} loi {response.status_code}.")
                self._rotate_key()
                return []
            response.raise_for_status()
            data = response.json()

            discovered_links = []
            for item in data.get("organic", []):
                link_url = item.get("link", "")
                if not link_url:
                    continue

                title = item.get("title", "No Title")
                snippet = item.get("snippet", "")
                domain = self._extract_domain(link_url)

                # ── Filter 0: Path allowlist (van-ban/ chinh thuc) ─────────────
                if not self._is_official_document_url(link_url):
                    logger.info(f"  [FILTERED] Not official doc path: '{title[:60]}'")
                    continue

                # ── Filter 1: Path denylist (Q&A / bai viet) ─────────────────
                if self._is_path_denied(link_url):
                    logger.info(f"  [FILTERED] Path nhieu: '{title[:60]}'")
                    continue

                # ── Filter 2: Cross-law / exact name match ───────────────────
                if not self._is_target_law_match(title, snippet, target_law_name, target_law_aliases):
                    logger.info(f"  [FILTERED] Khong match ten luat: '{title[:60]}'")
                    continue

                # ── Extract metadata ───────────────────────────────────────────
                law_id = self._extract_law_id(title, snippet)
                effective_date = self._extract_effective_date(title, snippet)

                discovered_links.append(DiscoveredLink(
                    url=link_url,
                    title=title,
                    source_domain=domain,
                    search_query=query,
                    query_id=query_id,
                    snippet=snippet,
                    is_processed=False,
                    law_id=law_id,
                    effective_date=effective_date,
                ))

            # ── Sort by domain priority ───────────────────────────────────────
            if self.source_priority:
                discovered_links.sort(key=lambda r: self._domain_priority(r.source_domain))

            logger.info(f" Da tim {len(discovered_links)} ket qua cho: '{query}'")
            return discovered_links

        except Exception as e:
            logger.error(f"Loi khi discovery: {str(e)}", exc_info=True)
            return []

    def _is_official_document_url(self, url: str) -> bool:
        """
        Filter lớp 1: chỉ giữ URL có path thuộc allowlist của domain đó.
        Nếu allowlist trống → cho phép tất cả (backward compatible).
        """
        if not self.path_allowlist:
            return True
        domain = self._extract_domain(url)
        allowed_prefixes = self.path_allowlist.get(domain, [])
        if not allowed_prefixes:
            return False
        # Parse path riêng rồi mới so sánh prefix — KHÔNG dùng startswith trên full URL
        parsed_path = urlparse(url).path
        for prefix in allowed_prefixes:
            if parsed_path.startswith(prefix):
                return True
        return False

    def _extract_domain(self, url: str) -> str:
        return urlparse(url).netloc.replace("www.", "")

    def _is_path_denied(self, url: str) -> bool:
        """Loc URL chua path trong denylist (bai viet/Q&A khong phai van ban chinh thuc)."""
        if not self.path_denylist:
            return False
        for denied in self.path_denylist:
            if denied in url:
                return True
        return False

    def _is_target_law_match(
        self,
        title: str,
        snippet: str,
        target_law_name: str,
        target_law_aliases: list,
    ) -> bool:
        """
        Loc ket qua nhiễu bang Exact Name + Aliases matching.

        Neu KHONG chua ten chinh thuc hoac aliases → loai (nhiễu cross-law).
        Neu chua → giu lai.

        Bo sung them cross-pattern regex de bat nhung bien the nhiễu
        (thi hanh an dan su, to tung, ...) van bi loai.
        """
        if not target_law_name:
            return True  # fallback: khong biet → giu

        combined = f"{title} {snippet}".lower()
        target_lower = target_law_name.lower()

        # Check 1: exact ten chinh thuc
        if target_lower in combined:
            return True

        # Check 2: aliases hop le
        for alias in target_law_aliases:
            if alias.lower() in combined:
                return True

        # Check 3: cross-pattern (neu co) → neu gap thi LOẠI
        cross_patterns = CROSS_PATTERNS.get(target_lower, [])
        for pattern in cross_patterns:
            if re.search(pattern, combined, re.IGNORECASE):
                return False  # nhiễu cross-law

        # Khong gap ten chinh thuc, aliases, hay cross-pattern
        # → cham chan: tra ve False (loai) thay vi True (giu)
        return False

    def _extract_law_id(self, title: str, snippet: str) -> Optional[str]:
        text = f"{title} {snippet}"
        match = LAW_ID_PATTERN.search(text)
        if match:
            for g in match.groups():
                if g:
                    return g.strip()
        return None

    def _extract_effective_date(self, title: str, snippet: str) -> Optional[str]:
        """
        Trích xuất ngày có hiệu lực.

        Priority:
        1. "Hiệu lực:" / "Hiệu lực kể từ ngày" → lấy ngày theo sau nhãn (cho phép : / -)
        2. "ngày DD tháng MM năm YYYY" (định dạng đầy đủ từ tiếng Việt)
        3. Tách tại "ban hành", chỉ lấy phần sau
        4. Fallback: match DD/MM/YYYY đầu tiên
        5. Standalone year

        Year phải trong [2000, current_year+1]; 1900 là placeholder giả → block.
        """
        text = f"{title} {snippet}"
        current_year = datetime.now().year

        def _parse_dmy(d: str, m: str, y: str) -> Optional[str]:
            year = int(y)
            if 2000 <= year <= current_year + 1:
                return f"{year}-{m.zfill(2)}-{d.zfill(2)}"
            return None

        def _parse_year(y: str) -> Optional[str]:
            year = int(y)
            if 2000 <= year <= current_year + 1:
                return f"{year}-??-??"
            return None

        # ── Priority 1: "Hiệu lực" có : / - / kể từ ─────────────────────────
        # Cho phep: "Hiệu lực: 01/01/2026", "Hiệu lực 01/01/2026",
        #           "Hiệu lực kể từ ngày 01/01/2026"
        EFF_LABEL = re.compile(
            r'hiệu\s*lực\s*(?:kể\s*từ\s*)?(?:ngày\s+)?[:\-\s]*'
            r'(\d{1,2})\s*(?:tháng)?[\/\-\s]+(\d{1,2})\s*(?:năm)?[\/\-\s]+(\d{4})',
            re.IGNORECASE
        )
        match = EFF_LABEL.search(text)
        if match:
            result = _parse_dmy(match.group(1), match.group(2), match.group(3))
            if result:
                return result

        # ── Priority 2: "ngày DD tháng MM năm YYYY" (tiếng Việt đầy đủ) ─────
        DATE_VIET = re.compile(
            r'ngày\s+(\d{1,2})\s+tháng\s+(\d{1,2})\s+năm\s+(\d{4})',
            re.IGNORECASE
        )
        match = DATE_VIET.search(text)
        if match:
            result = _parse_dmy(match.group(1), match.group(2), match.group(3))
            if result:
                return result

        # ── Priority 3: tách tại "ban hành", lấy phần sau ───────────────────
        BANHANH_SPLIT = re.compile(r'ban\s*hành\s+', re.IGNORECASE)
        parts = BANHANH_SPLIT.split(text, maxsplit=1)
        if len(parts) == 2:
            tail = parts[1]
            # Ưu tiên "hiệu lực" trong tail
            match = EFF_LABEL.search(tail)
            if match:
                result = _parse_dmy(match.group(1), match.group(2), match.group(3))
                if result:
                    return result
            # Fallback: DD/MM/YYYY không nhãn trong tail
            DATE_NOLABEL = re.compile(
                r'(?<![A-Z0-9/])(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})(?![A-Z0-9/\-])',
                re.IGNORECASE
            )
            match = DATE_NOLABEL.search(tail)
            if match:
                result = _parse_dmy(match.group(1), match.group(2), match.group(3))
                if result:
                    return result

        # ── Priority 4: fallback toàn bộ text ────────────────────────────────
        DATE_FULL = re.compile(
            r'(?:ngày\s+)?(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})',
            re.IGNORECASE
        )
        match = DATE_FULL.search(text)
        if match:
            result = _parse_dmy(match.group(1), match.group(2), match.group(3))
            if result:
                return result

        # ── Priority 5: standalone year ──────────────────────────────────────
        YEAR_PAT = re.compile(r'(?<![A-Z0-9/])(\d{4})(?![A-Z0-9/\-])')
        match = YEAR_PAT.search(text)
        if match:
            return _parse_year(match.group(1))

        return None

    def _domain_priority(self, domain: str) -> int:
        for i, alias in enumerate(self.source_priority):
            canonical = SOURCE_ALIAS_MAP.get(alias, alias)
            if canonical == domain:
                return i
        return len(self.source_priority)
