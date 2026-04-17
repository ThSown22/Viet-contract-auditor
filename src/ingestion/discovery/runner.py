import os
import yaml
import hashlib
import jsonlines
import logging
import threading # <-- Cần thiết để khóa luồng khi ghi file
from datetime import datetime
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.utils.logger_config import setup_global_logging
from src.ingestion.discovery.engines.google_serper import GoogleSerperEngine
from src.ingestion.schemas.models import DiscoveredLink

# Kích hoạt nhạc trưởng logging
setup_global_logging(log_file_name="discovery.log")
logger = logging.getLogger(__name__)

_ENGINE_REGISTRY = {
    "serper": GoogleSerperEngine,
}

class DiscoveryRunner:
    def __init__(self, config_path: str = "src/ingestion/config/discovery.yaml"):
        self.config_path = config_path
        self.config = self._load_config()
        engine_name = self.config.get("discovery_settings", {}).get("engine", "serper")
        if engine_name is None:
            engine_name = "serper"
        engine_key = str(engine_name).strip().lower()
        if not engine_key:
            engine_key = "serper"

        engine_class = _ENGINE_REGISTRY.get(engine_key)
        if engine_class is None:
            available = ", ".join(sorted(_ENGINE_REGISTRY.keys()))
            raise ValueError(
                f"Không nhận ra discovery engine '{engine_key}'. Hỗ trợ: {available}"
            )

        self.engine = engine_class(self.config)
        logger.info(f" Đã chọn discovery engine: {engine_key}")
        
        # Folder lưu kết quả
        self.output_dir = self.config["storage"]["output_dir"]
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        # Cấu hình file output
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = self.config["storage"]["filename_format"].replace("{timestamp}", timestamp)
        self.output_file = os.path.join(self.output_dir, filename)

        # Ổ khóa để ghi file an toàn khi chạy đa luồng
        self.file_lock = threading.Lock()
        
        # Tập hợp các mã băm để khử trùng ngay lập tức (In-memory Dedupe)
        self.seen_hashes = set()

    def _load_config(self) -> dict:
        with open(self.config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    def _generate_content_hash(self, link: DiscoveredLink) -> str:
        """
        Tạo mã vân tay (Fingerprint) cho văn bản luật.
        Skill: Content-based Hashing thay vì URL-based.
        """
        # Hash dựa trên Tiêu đề (Vì tiêu đề luật thường chứa số hiệu và tên)
        return hashlib.md5(link.title.strip().lower().encode('utf-8')).hexdigest()

    def _save_results(self, results: List[DiscoveredLink]):
        """
        Hàm xử lý lưu trữ tập trung.
        Nhiệm vụ: Khử trùng và Ghi file an toàn.
        """
        valid_records = []
        
        for link in results:
            content_hash = self._generate_content_hash(link)
            
            # Kiểm tra xem bộ luật này đã tìm thấy trong lượt chạy này chưa
            if content_hash not in self.seen_hashes:
                self.seen_hashes.add(content_hash)
                # Chuyển Pydantic model thành Dict để ghi JSONL
                valid_records.append(link.model_dump())

        # LOCK: Chỉ 1 luồng được mở file và ghi tại 1 thời điểm
        if valid_records:
            with self.file_lock:
                with jsonlines.open(self.output_file, mode='a') as writer:
                    writer.write_all(valid_records)
                logger.info(f"Đã lưu thêm {len(valid_records)} link mới vào {self.output_file}")

    def run(self):
        logger.info(" BẮT ĐẦU PIPELINE DISCOVERY ĐA LUỒNG (DE VERSION)...")
        
        target_laws = self.config.get("target_laws", [])
        query_templates = self.config.get("query_templates", [])
        
        # Tạo danh sách Query (Task Queue)
        tasks = []
        for law in target_laws:
            for template in query_templates:
                query = template["template"].replace("{law_name}", law["name"])
                tasks.append(query)

        # CHẠY ĐA LUỒNG
        total_discovered = 0
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_query = {executor.submit(self.engine.search, q): q for q in tasks}
            
            for future in as_completed(future_to_query):
                query = future_to_query[future]
                try:
                    results = future.result()
                    if results:
                        self._save_results(results)
                        total_discovered += len(results)
                except Exception as e:
                    logger.error(f"❌ Lỗi khi xử lý query '{query}': {e}", exc_info=True)

        logger.info(f"🏁 PIPELINE HOÀN TẤT. Tổng cộng nhặt được {total_discovered} link.")

if __name__ == "__main__":
    runner = DiscoveryRunner()
    runner.run()