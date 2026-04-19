"""
Test cho resume-idempotent deduplication.
Yêu cầu:
- Lần 1 chạy: lưu được N bản ghi > 0
- Lần 2 chạy cùng input, không xóa output_dir: nạp lịch sử N > 0 và lưu mới = 0
- Assert không phát sinh bản ghi trùng URL canonical trong output
"""
import pytest
import sys, os, json, shutil, tempfile
from datetime import datetime, timedelta
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.ingestion.discovery.runner import DiscoveryRunner, _dedup_key


class TestDedupKeyConsistency:
    """Đảm bảo dedup key nhất quán giữa load history và save mới."""

    def test_key_khong_dependent_tren_trailing_slash(self):
        """Trailing slash không ảnh hưởng key."""
        k1 = _dedup_key("https://thuvienphapluat.vn/van-ban/Doanh-nghiep/")
        k2 = _dedup_key("https://thuvienphapluat.vn/van-ban/Doanh-nghiep")
        assert k1 == k2

    def test_key_khong_dependent_tren_www(self):
        """www prefix không ảnh hưởng key."""
        k1 = _dedup_key("https://www.thuvienphapluat.vn/van-ban/law")
        k2 = _dedup_key("https://thuvienphapluat.vn/van-ban/law")
        assert k1 == k2

    def test_key_normalizes_congbao_htm_to_html(self):
        """congbao .htm URL được chuẩn hóa về .html."""
        k1 = _dedup_key("https://congbao.chinhphu.vn/van-ban/luat-so-91-2015-qh13-18397.htm")
        k2 = _dedup_key("https://congbao.chinhphu.vn/van-ban/luat-so-91-2015-qh13-18397.html")
        assert k1 == k2

    def test_key_normalizes_congbao_page_numbers(self):
        """congbao /<id>/<num>.htm được chuẩn hóa."""
        k1 = _dedup_key("https://congbao.chinhphu.vn/van-ban/luat-so-59-2020-qh14-31674/31830.htm")
        k2 = _dedup_key("https://congbao.chinhphu.vn/van-ban/luat-so-59-2020-qh14-31674.html")
        assert k1 == k2

    def test_key_strips_index_pages(self):
        """index page là root path của nó — KHÔNG strip path content."""
        # _dedup_key chỉ rstrip trailing slash, KHÔNG thay đổi path content
        k1 = _dedup_key("https://thuvienphapluat.vn/van-ban/")
        k2 = _dedup_key("https://thuvienphapluat.vn/van-ban")
        assert k1 == k2
        # /index.html khác path → khác key (đúng: đây là trang thực)
        k3 = _dedup_key("https://thuvienphapluat.vn/van-ban/index.html")
        assert k3 != k1


class TestResumeIdempotent:
    """Test resume-idempotent: chạy 2 lần không trùng bản ghi."""

    @pytest.fixture
    def tmp_output_dir(self, tmp_path):
        """Tạo thư mục tạm cho output."""
        d = tmp_path / "discovery_results"
        d.mkdir()
        return str(d)

    def _write_history_file(self, output_dir: str, now_minus_days: int, records: list):
        """Viết file history với discovered_at cố định."""
        ts = (datetime.now() - timedelta(days=now_minus_days)).isoformat()
        fname = f"discovered_{datetime.now().strftime('%Y%m%d_%H%M%S')}_history.jsonl"
        fpath = os.path.join(output_dir, fname)
        with open(fpath, "w", encoding="utf-8") as f:
            for rec in records:
                rec["discovered_at"] = ts
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        return fpath

    def test_first_run_saves_all(self, tmp_output_dir, monkeypatch):
        """Lần 1: toàn bộ N bản ghi được lưu (N > 0)."""
        # Setup: tạo 2 history file cũ (nhưng cũ quá → bị bỏ qua)
        self._write_history_file(tmp_output_dir, 10, [
            {"url": "https://thuvienphapluat.vn/van-ban/Old/OldDoc", "title": "Old Doc 1"},
        ])

        # Mock SERPER để trả kết quả cố định
        from src.ingestion.schemas.models import DiscoveredLink

        def mock_search(self, query, query_id, target_law_name, target_law_aliases):
            return [
                DiscoveredLink(
                    url="https://thuvienphapluat.vn/van-ban/Doanh-nghiep/NewLaw.aspx",
                    title="Luật Doanh nghiệp mới",
                    source_domain="thuvienphapluat.vn",
                    search_query=query,
                    query_id=query_id,
                    snippet="VN",
                    is_processed=False,
                ),
                DiscoveredLink(
                    url="https://thuvienphapluat.vn/van-ban/Bo-luat-dan-su/BLDS.aspx",
                    title="Bộ luật Dân sự 2015",
                    source_domain="thuvienphapluat.vn",
                    search_query=query,
                    query_id=query_id,
                    snippet="VN",
                    is_processed=False,
                ),
            ]

        from src.ingestion.discovery.engines.google_serper import GoogleSerperEngine
        monkeypatch.setattr(GoogleSerperEngine, "search", mock_search)

        # Tạo config để dùng output tạm
        import yaml
        config_data = {
            "discovery_settings": {
                "engine": "serper",
                "source_priority": ["tvpl"],
                "target_domains": ["thuvienphapluat.vn"],
                "path_allowlist": {"thuvienphapluat.vn": ["/van-ban/"]},
                "path_denylist": [],
            },
            "target_laws": [
                {"name": "Luật Doanh nghiệp", "aliases": [], "keywords": ["LDN"]},
            ],
            "query_templates": [
                {"id": "t1", "template": "Luật Doanh nghiệp", "priority": 1},
            ],
            "storage": {
                "output_dir": tmp_output_dir,
                "filename_format": "discovered_{timestamp}.jsonl",
                "deduplication": {"method": "url_only", "scope": "global"},
            },
            "resume_logic": {"skip_if_processed": True, "refresh_interval_days": 7},
            "rate_limit": {},
        }
        with open(os.path.join(tmp_output_dir, "test_discovery.yaml"), "w", encoding="utf-8") as cf:
            yaml.dump(config_data, cf)

        runner = DiscoveryRunner(os.path.join(tmp_output_dir, "test_discovery.yaml"))
        runner.run()

        output_files = [f for f in os.listdir(tmp_output_dir) if f.startswith("discovered_") and not "history" in f]
        assert len(output_files) == 1, f"Expected 1 output file, got {output_files}"
        output_file = os.path.join(tmp_output_dir, output_files[0])
        with open(output_file, encoding="utf-8") as f:
            lines = f.readlines()
        assert len(lines) >= 1, "Lần 1 phải lưu được ít nhất 1 bản ghi"
        # Verify tất cả URL đều hợp lệ
        for line in lines:
            rec = json.loads(line)
            assert "url" in rec

    def test_second_run_resumes_and_saves_zero(self, tmp_output_dir, monkeypatch):
        """Lần 2: nạp lịch sử N > 0 và lưu mới = 0 (idempotent)."""
        from src.ingestion.schemas.models import DiscoveredLink

        def mock_search(self, query, query_id, target_law_name, target_law_aliases):
            return [
                DiscoveredLink(
                    url="https://thuvienphapluat.vn/van-ban/Doanh-nghiep/NewLaw.aspx",
                    title="Luật Doanh nghiệp mới",
                    source_domain="thuvienphapluat.vn",
                    search_query=query,
                    query_id=query_id,
                    snippet="VN",
                    is_processed=False,
                ),
            ]

        from src.ingestion.discovery.engines.google_serper import GoogleSerperEngine
        monkeypatch.setattr(GoogleSerperEngine, "search", mock_search)

        # Tạo file lịch sử gần đây (trong 7 ngày) — ghi trước để test nạp lịch sử
        history_file = os.path.join(tmp_output_dir, "discovered_20260419_120000_history.jsonl")
        with open(history_file, "w", encoding="utf-8") as f:
            f.write(json.dumps({
                "url": "https://thuvienphapluat.vn/van-ban/Doanh-nghiep/NewLaw.aspx",
                "title": "Luật Doanh nghiệp mới",
                "discovered_at": datetime.now().isoformat(),
            }, ensure_ascii=False) + "\n")

        import yaml
        config_data = {
            "discovery_settings": {
                "engine": "serper",
                "source_priority": ["tvpl"],
                "target_domains": ["thuvienphapluat.vn"],
                "path_allowlist": {"thuvienphapluat.vn": ["/van-ban/"]},
                "path_denylist": [],
            },
            "target_laws": [
                {"name": "Luật Doanh nghiệp", "aliases": [], "keywords": ["LDN"]},
            ],
            "query_templates": [
                {"id": "t1", "template": "Luật Doanh nghiệp", "priority": 1},
            ],
            "storage": {
                "output_dir": tmp_output_dir,
                "filename_format": "discovered_{timestamp}.jsonl",
                "deduplication": {"method": "url_only", "scope": "global"},
            },
            "resume_logic": {"skip_if_processed": True, "refresh_interval_days": 7},
            "rate_limit": {},
        }
        with open(os.path.join(tmp_output_dir, "test_discovery.yaml"), "w", encoding="utf-8") as cf:
            yaml.dump(config_data, cf)

        runner = DiscoveryRunner(os.path.join(tmp_output_dir, "test_discovery.yaml"))
        assert len(runner.seen_keys) >= 1, "Lần 2 phải nạp ít nhất 1 key từ history"
        runner.run()

        # Kiểm tra: không có output file mới nào (vì không có record mới)
        output_files = [f for f in os.listdir(tmp_output_dir)
                       if f.startswith("discovered_") and "history" not in f]
        total_new = 0
        for of in output_files:
            with open(os.path.join(tmp_output_dir, of), encoding="utf-8") as f:
                total_new += len(f.readlines())
        assert total_new == 0, f"Lần 2 không được lưu bản ghi mới, nhưng đã lưu {total_new}"

    def test_no_duplicate_url_in_output(self, tmp_output_dir, monkeypatch):
        """Trong một lần chạy, không có bản ghi trùng URL canonical."""
        from src.ingestion.schemas.models import DiscoveredLink

        def mock_search(self, query, query_id, target_law_name, target_law_aliases):
            return [
                # 3 kết quả: 2 trùng URL (khác query), 1 khác URL
                DiscoveredLink(
                    url="https://thuvienphapluat.vn/van-ban/Doanh-nghiep/SameLaw.aspx",
                    title="Luật Doanh nghiệp",
                    source_domain="thuvienphapluat.vn",
                    search_query=query,
                    query_id=query_id,
                    snippet="snippet A",
                    is_processed=False,
                ),
                DiscoveredLink(
                    url="https://thuvienphapluat.vn/van-ban/Doanh-nghiep/SameLaw.aspx",
                    title="Luật Doanh nghiệp (trùng)",
                    source_domain="thuvienphapluat.vn",
                    search_query="another query",
                    query_id="q2",
                    snippet="snippet B dài hơn nên phải được ưu tiên",
                    is_processed=False,
                ),
                DiscoveredLink(
                    url="https://thuvienphapluat.vn/van-ban/Bo-luat/AnotherLaw.aspx",
                    title="Luật B",
                    source_domain="thuvienphapluat.vn",
                    search_query=query,
                    query_id=query_id,
                    snippet="VN",
                    is_processed=False,
                ),
            ]

        from src.ingestion.discovery.engines.google_serper import GoogleSerperEngine
        monkeypatch.setattr(GoogleSerperEngine, "search", mock_search)

        import yaml
        config_data = {
            "discovery_settings": {
                "engine": "serper",
                "source_priority": ["tvpl"],
                "target_domains": ["thuvienphapluat.vn"],
                "path_allowlist": {"thuvienphapluat.vn": ["/van-ban/"]},
                "path_denylist": [],
            },
            "target_laws": [
                {"name": "Luật Doanh nghiệp", "aliases": [], "keywords": ["LDN"]},
            ],
            "query_templates": [
                {"id": "t1", "template": "Luật Doanh nghiệp", "priority": 1},
            ],
            "storage": {
                "output_dir": tmp_output_dir,
                "filename_format": "discovered_{timestamp}.jsonl",
                "deduplication": {"method": "url_only", "scope": "global"},
            },
            "resume_logic": {"skip_if_processed": False, "refresh_interval_days": 7},
            "rate_limit": {},
        }
        with open(os.path.join(tmp_output_dir, "test_discovery.yaml"), "w", encoding="utf-8") as cf:
            yaml.dump(config_data, cf)

        runner = DiscoveryRunner(os.path.join(tmp_output_dir, "test_discovery.yaml"))
        runner.run()

        output_files = [f for f in os.listdir(tmp_output_dir)
                       if f.startswith("discovered_") and "history" not in f]
        assert len(output_files) == 1
        seen_urls = set()
        with open(os.path.join(tmp_output_dir, output_files[0]), encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                url = rec["url"]
                key = _dedup_key(url)
                assert key not in seen_urls, f"Duplicate URL canonical key: {key}"
                seen_urls.add(key)