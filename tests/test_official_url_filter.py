"""
Test cho official URL filter (path allowlist).
Yêu cầu:
- URL /chinh-sach-phap-luat-moi/... phải bị loại
- URL /van-ban/... phải được giữ (nếu match tên luật)
"""
import pytest
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.ingestion.discovery.engines.google_serper import GoogleSerperEngine


@pytest.fixture
def engine():
    config = {
        "discovery_settings": {
            "engine": "serper",
            "source_priority": ["tvpl", "congbao"],
            "target_domains": [
                "thuvienphapluat.vn",
                "congbao.chinhphu.vn",
            ],
            "path_allowlist": {
                "thuvienphapluat.vn": ["/van-ban/"],
                "congbao.chinhphu.vn": ["/van-ban/"],
            },
            "path_denylist": [
                "/hoi-dap-phap-luat/",
                "/tin-tuc/",
            ],
        },
        "rate_limit": {},
    }
    return GoogleSerperEngine(config)


class TestOfficialDocumentUrlFilter:
    """Test suite cho _is_official_document_url."""

    def test_noise_url_chinh_sach_phap_luat_duoc_loai(self, engine):
        """URL chứa /chinh-sach-phap-luat-moi/ phải bị loại."""
        url = (
            "https://thuvienphapluat.vn/chinh-sach-phap-luat-moi/vn/ho-tro-phap-luat/"
            "chinh-sach-moi/192/van-ban-hop-nhat-luat-doanh-nghiep-moi-nhat-2025"
        )
        assert engine._is_official_document_url(url) is False

    def test_noise_url_ho_tro_phap_luat_duoc_loai(self, engine):
        """URL chứa /ho-tro-phap-luat/ phải bị loại."""
        url = (
            "https://thuvienphapluat.vn/chinh-sach-phap-luat-moi/vn/ho-tro-phap-luat/"
            "chinh-sach-moi/92564/van-ban-hop-nhat-luat-trong-tai-thuong-mai"
        )
        assert engine._is_official_document_url(url) is False

    def test_official_van_ban_url_duoc_giu(self, engine):
        """URL chứa /van-ban/ phải được giữ."""
        url = "https://thuvienphapluat.vn/van-ban/Doanh-nghiep/Van-ban-hop-nhat-67-VBHN-VPQH-2025-Luat-Doanh-nghiep-671127.aspx"
        assert engine._is_official_document_url(url) is True

    def test_official_van_ban_congbao_duoc_giu(self, engine):
        """URL /van-ban/ của congbao.chinhphu.vn phải được giữ."""
        url = "https://congbao.chinhphu.vn/van-ban/luat-so-91-2015-qh13-18397.htm"
        assert engine._is_official_document_url(url) is True

    def test_unknown_domain_false(self, engine):
        """Domain không có trong allowlist → False."""
        url = "https://other-site.vn/van-ban/some-law"
        assert engine._is_official_document_url(url) is False

    def test_empty_allowlist_allows_all(self):
        """Nếu allowlist rỗng → cho phép tất cả (backward compatible)."""
        config = {
            "discovery_settings": {
                "engine": "serper",
                "path_allowlist": {},
            },
            "rate_limit": {},
        }
        engine = GoogleSerperEngine(config)
        assert engine._is_official_document_url("https://example.com/any-path") is True

    def test_subpath_van_ban_giu(self, engine):
        """/van-ban/ với sub-path sâu vẫn được giữ."""
        url = "https://thuvienphapluat.vn/van-ban/Thuong-mai/Van-ban-hop-nhat-60-VBHN-VPQH-2025-Luat-Trong-tai-thuong-mai-669808.aspx"
        assert engine._is_official_document_url(url) is True