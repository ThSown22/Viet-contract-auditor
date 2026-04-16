import os
import time
import requests
import threading
import logging
from typing import List, Optional
from datetime import datetime
from dotenv import load_dotenv
from src.ingestion.schemas.models import DiscoveredLink

load_dotenv()

# Khởi tạo logger cho module này
logger = logging.getLogger(__name__)

class GoogleSerperEngine:
    def __init__(self, config: dict):
        # 1. Cơ chế Multi-Key
        raw_keys = {
            k: v for k, v in os.environ.items() 
            if k.startswith("SERPER_KEY_") and v.strip()
        }
        self.api_keys = [raw_keys[k] for k in sorted(raw_keys.keys())]
        
        if not self.api_keys:
            # Fallback nếu chỉ có 1 key đơn giản
            single_key = os.getenv("SERPER_API_KEY")
            if single_key: 
                self.api_keys = [single_key]
            else: 
                # Dùng CRITICAL vì nếu không có key, hệ thống không thể khởi động
                logger.critical(" Không tìm thấy bất kỳ API Key nào trong file .env!")
                raise ValueError("Thiếu SERPER_API_KEY.")

        # 2. Cấu hình vận hành
        self.settings = config.get("discovery_settings", {})
        self.rate_limit = config.get("rate_limit", {})
        self.current_key_idx = 0
        self.call_count = 0
        self.lock = threading.Lock()
        self.url = "https://google.serper.dev/search"

        logger.info(f"GoogleSerperEngine đã sẵn sàng với {len(self.api_keys)} API keys.") 

    def _rotate_key(self):
        with self.lock:
            self.current_key_idx = (self.current_key_idx + 1) % len(self.api_keys)
            self.call_count = 0
            logger.warning(f"Đã xoay API Key. Hiện đang sử dụng Key số {self.current_key_idx + 1}")

    def get_headers(self):
        limit = self.rate_limit.get("rotate_keys_every_n_calls", 100)
        if self.call_count >= limit:
            self._rotate_key()
        
        self.call_count += 1
        return {
            "X-API-KEY": self.api_keys[self.current_key_idx],
            "Content-Type": "application/json"
        }

    def search(self, query: str) -> List[DiscoveredLink]:
        """
        Tìm kiếm và trả về List[DiscoveredLink] đã được lọc domain.
        """
        # 3. Tối ưu Query cho Pháp Luật (Domain Locking)
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

        # 4. Kiểm soát nhịp độ (Rate Limiting)
        logger.info(f"Đang thực hiện tìm kiếm Google: '{query}'")
        time.sleep(self.rate_limit.get("delay_between_queries_sec", 1))

        try:
            response = requests.post(self.url, headers=self.get_headers(), json=payload, timeout=30)
            
            # Tự động xoay key nếu gặp lỗi giới hạn
            if response.status_code in [403, 429]:
                logger.error(f"API Key số {self.current_key_idx + 1} báo lỗi {response.status_code} (Hết hạn/Giới hạn).")
                self._rotate_key()
                return []
                
            response.raise_for_status()
            data = response.json()
            
            discovered_links = []
            for item in data.get("organic", []):
                link_url = item.get("link")
                if not link_url: continue

                # 5. Ép vào Schema Pydantic (Độ chính xác pháp lý)
                discovered_links.append(DiscoveredLink(
                    url=link_url,
                    title=item.get("title", "No Title"),
                    source_domain=self._extract_domain(link_url),
                    search_query=query,
                    snippet=item.get("snippet", ""),
                    is_processed=False
                ))
            logger.info(f" Đã tìm thấy {len(discovered_links)} kết quả cho: '{query}'")
            return discovered_links

        except Exception as e:
            # exc_info=True giúp ghi lại toàn bộ stack trace (vết lỗi) để dễ debug
            logger.error(f"Lỗi khi thực hiện Discovery: {str(e)}", exc_info=True)
            return []

    def _extract_domain(self, url: str) -> str:
        from urllib.parse import urlparse
        return urlparse(url).netloc.replace("www.", "")