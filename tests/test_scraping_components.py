import json
import shutil
import sys
from pathlib import Path

import jsonlines

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ingestion.schemas.models import ScrapedArticle, ScrapedContent, ScrapingResult
from src.ingestion.scraping.coordinator import ScrapingCoordinator
from src.ingestion.scraping.normalizers.content_cleaner import ContentCleaner
from src.ingestion.scraping.normalizers.structured_parser import parse_articles, reconstruct_structured_text
from src.ingestion.scraping.scrapers.congbao_scraper import CongbaoScraper
from src.ingestion.scraping.state_manager import StateManager


def make_workspace(name: str) -> Path:
    workspace = Path("tests/runtime") / name
    if workspace.exists():
        shutil.rmtree(workspace)
    workspace.mkdir(parents=True, exist_ok=True)
    return workspace


def make_scored_content(source_url: str, source_domain: str, articles: list[ScrapedArticle]) -> ScrapedContent:
    full_text = "\n\n".join(f"{article.title}\n{article.text}".strip() for article in articles)
    return ScrapedContent(
        law_name="Luáº­t máº«u",
        law_id="1/2026/QH15",
        source_url=source_url,
        source_domain=source_domain,
        title="Luáº­t máº«u",
        clean_text=full_text,
        structured_text=full_text,
        articles=articles,
        article_count=len(articles),
        has_structure=True,
        validation_passed=True,
    )


def test_content_cleaner_removes_artifacts_and_keeps_structure():
    cleaner = ContentCleaner({"min_length_chars": 10, "required_keywords": ["Dieu 1"], "blacklist_keywords": ["Dang nhap"]})
    raw_text = """
    Trang chu
    Dieu 1. Pham vi dieu chinh

    Khoan 1. Noi dung.
    Dang nhap
    """
    cleaned = cleaner.clean(raw_text)
    assert "Trang chu" not in cleaned
    assert "Dang nhap" not in cleaned
    assert "Dieu 1. Pham vi dieu chinh" in cleaned
    assert cleaner.has_structure(cleaned)


def test_content_cleaner_preserves_legal_lines_with_blacklist_terms_inside():
    cleaner = ContentCleaner({})
    raw_text = """
    Dang ky
    Ho so dang ky doanh nghiep tu nhan
    1. Giay de nghi dang ky doanh nghiep.
    2. Ban sao Giay chung nhan dang ky dau tu.
    """

    cleaned = cleaner.clean(raw_text)

    assert "Dang ky" not in cleaned
    assert "Ho so dang ky doanh nghiep tu nhan" in cleaned
    assert "1. Giay de nghi dang ky doanh nghiep." in cleaned
    assert "2. Ban sao Giay chung nhan dang ky dau tu." in cleaned


def test_content_cleaner_removes_compound_navigation_lines():
    cleaner = ContentCleaner({})
    raw_text = """
    Trang chu | Dang nhap | Dang ky
    Dieu 1. Noi dung
    """

    cleaned = cleaner.clean(raw_text)

    assert "Trang chu | Dang nhap | Dang ky" not in cleaned
    assert "Dieu 1. Noi dung" in cleaned


def test_content_cleaner_validation_does_not_reject_legal_registration_terms():
    cleaner = ContentCleaner({"min_length_chars": 10, "blacklist_keywords": ["Dang ky", "Dang nhap"]})
    text = "Dieu 19. Ho so dang ky doanh nghiep tu nhan\n1. Giay de nghi dang ky doanh nghiep."

    assert cleaner.validate(text) is True


def test_state_manager_marks_processed():
    workspace = make_workspace("state_manager")
    jsonl_path = workspace / "discovered.jsonl"
    with jsonlines.open(jsonl_path, mode="w") as writer:
        writer.write_all([
            {"url": "https://example.com/a", "title": "A", "source_domain": "example.com", "search_query": "", "is_processed": False},
            {"url": "https://example.com/b", "title": "B", "source_domain": "example.com", "search_query": "", "is_processed": False},
        ])

    updated = StateManager(str(jsonl_path)).mark_processed(["https://example.com/a"], backup=True)
    assert updated == 1

    with jsonlines.open(jsonl_path) as reader:
        saved = list(reader)
    assert saved[0]["is_processed"] is True
    assert saved[1]["is_processed"] is False
    assert any((jsonl_path.parent / "backups").iterdir())


def test_state_manager_can_reset_processed():
    workspace = make_workspace("state_manager_reset")
    jsonl_path = workspace / "discovered.jsonl"
    with jsonlines.open(jsonl_path, mode="w") as writer:
        writer.write_all([
            {"url": "https://example.com/a", "title": "A", "source_domain": "example.com", "search_query": "", "is_processed": True, "processed_at": "2026-05-05T01:00:00"},
            {"url": "https://example.com/b", "title": "B", "source_domain": "example.com", "search_query": "", "is_processed": False},
        ])

    updated = StateManager(str(jsonl_path)).reset_processed(["https://example.com/a"], backup=True)
    assert updated == 1

    with jsonlines.open(jsonl_path) as reader:
        saved = list(reader)
    assert saved[0]["is_processed"] is False
    assert "processed_at" not in saved[0]
    assert saved[1]["is_processed"] is False


def test_output_json_shape_round_trip():
    workspace = make_workspace("output_shape")
    path = workspace / "content.json"
    path.write_text(
        json.dumps(
            {
                "law_name": "Luat Doanh nghiep",
                "law_id": "67/VBHN-VPQH",
                "source_url": "https://example.com",
                "source_domain": "example.com",
                "title": "Van ban hop nhat",
                "clean_text": "Dieu 1\nKhoan 1",
                "char_count": 15,
                "word_count": 4,
                "has_structure": True,
                "validation_passed": True,
            }
        ),
        encoding="utf-8",
    )
    assert json.loads(path.read_text(encoding="utf-8"))["law_id"] == "67/VBHN-VPQH"


def test_structured_parser_builds_article_records():
    text = (
        "Chương I\n"
        "NHỮNG QUY ĐỊNH CHUNG\n"
        "Điều 1. Phạm vi điều chỉnh\n"
        "Luật này quy định về thẩm quyền.\n\n"
        "Điều 2. Thẩm quyền giải quyết\n"
        "các tranh chấp của Trọng tài\n"
        "1. Tranh chấp giữa các bên.\n"
    )
    articles = parse_articles(text, "54/2010/qh12")

    assert len(articles) == 2
    assert articles[0].article_id == "54/2010/qh12__1"
    assert articles[0].title == "Điều 1. Phạm vi điều chỉnh"
    assert articles[0].text == "Luật này quy định về thẩm quyền."
    assert articles[1].title == "Điều 2. Thẩm quyền giải quyết các tranh chấp của Trọng tài"
    assert articles[1].text == "1. Tranh chấp giữa các bên."

    reconstructed = reconstruct_structured_text(articles)
    assert "Điều 1. Phạm vi điều chỉnh\nLuật này quy định về thẩm quyền." in reconstructed
    assert "Điều 2. Thẩm quyền giải quyết các tranh chấp của Trọng tài\n1. Tranh chấp giữa các bên." in reconstructed


def test_structured_parser_keeps_multiline_article_title():
    text = (
        "Điều 4. Áp dụng\n"
        "Bộ luật dân sự\n"
        "1. Bộ luật này là luật chung điều chỉnh các quan hệ dân sự.\n"
    )

    articles = parse_articles(text, "91/2015/qh13")

    assert len(articles) == 1
    assert articles[0].title == "Điều 4. Áp dụng Bộ luật dân sự"
    assert articles[0].text == "1. Bộ luật này là luật chung điều chỉnh các quan hệ dân sự."


def test_quality_score_prefers_monotonic_article_sequence():
    coordinator = ScrapingCoordinator.__new__(ScrapingCoordinator)
    coordinator.source_priority = ["thuvienphapluat.vn", "congbao.chinhphu.vn"]

    good_content = make_scored_content(
        "https://example.com/good",
        "congbao.chinhphu.vn",
        [
            ScrapedArticle(article_id="1/2026/QH15__1", title="Äiá»u 1. A", text="x"),
            ScrapedArticle(article_id="1/2026/QH15__2", title="Äiá»u 2. B", text="y"),
            ScrapedArticle(article_id="1/2026/QH15__3", title="Äiá»u 3. C", text="z"),
        ],
    )
    bad_content = make_scored_content(
        "https://example.com/bad",
        "thuvienphapluat.vn",
        [
            ScrapedArticle(article_id="1/2026/QH15__1", title="Äiá»u 1. A", text="x"),
            ScrapedArticle(article_id="1/2026/QH15__5", title="Äiá»u 5. B", text="y"),
            ScrapedArticle(article_id="1/2026/QH15__2", title="Äiá»u 2. C", text="z"),
        ],
    )
    assert coordinator._score_content_quality(good_content) > coordinator._score_content_quality(bad_content)


def test_quality_score_penalizes_broken_definition_numbering():
    coordinator = ScrapingCoordinator.__new__(ScrapingCoordinator)
    coordinator.source_priority = ["thuvienphapluat.vn", "congbao.chinhphu.vn"]

    clean_definition = make_scored_content(
        "https://example.com/clean",
        "congbao.chinhphu.vn",
        [
            ScrapedArticle(
                article_id="1/2026/QH15__4",
                title="Äiá»u 4. Giáº£i thÃ­ch tá»« ngá»¯",
                text="Trong Luáº­t nÃ y, cÃ¡c tá»« ngá»¯ dÆ°á»›i Ä‘Ã¢y Ä‘Æ°á»£c hiá»ƒu nhÆ° sau:\n1. A lÃ  ...\n2. B lÃ  ...\n3. C lÃ  ...",
            )
        ],
    )
    broken_definition = make_scored_content(
        "https://example.com/broken",
        "congbao.chinhphu.vn",
        [
            ScrapedArticle(
                article_id="1/2026/QH15__4",
                title="Äiá»u 4. Giáº£i thÃ­ch tá»« ngá»¯",
                text="Trong Luáº­t nÃ y, cÃ¡c tá»« ngá»¯ dÆ°á»›i Ä‘Ã¢y Ä‘Æ°á»£c hiá»ƒu nhÆ° sau:\n1. A lÃ  ...\nB lÃ  ...\n4. C lÃ  ...",
            )
        ],
    )
    assert coordinator._score_content_quality(clean_definition) > coordinator._score_content_quality(broken_definition)


def test_coordinator_leaves_unmatched_urls_unprocessed():
    class DummyStateManager:
        def __init__(self) -> None:
            self.mark_calls: list[list[str]] = []

        def mark_processed(self, urls, backup=True):
            self.mark_calls.append(list(urls))
            return len(urls)

    coordinator = ScrapingCoordinator.__new__(ScrapingCoordinator)
    coordinator.scraping_config = {"state_management": {"update_discovery_jsonl": True}}
    coordinator.state_manager = DummyStateManager()
    coordinator._last_unmatched_count = 2

    coordinator._prepare_groups = lambda selected_laws, force: {
        "Luáº­t máº«u": {"urls": [{"url": "https://example.com/law", "source_domain": "example.com"}]}
    }
    coordinator._build_priority_chain = lambda law_name, urls: []
    coordinator._scrape_with_fallback = lambda law_name, chain: ScrapingResult(
        success=False,
        attempted_url="https://example.com/law",
        error_message="failed",
    )

    result = coordinator.run()

    assert result["fail_count"] == 1
    assert coordinator.state_manager.mark_calls == []


def test_coordinator_infers_law_from_config_terms_when_title_is_mojibake():
    coordinator = ScrapingCoordinator.__new__(ScrapingCoordinator)
    coordinator.discovery_config = {
        "target_laws": [
            {"name": "Bá»™ luáº­t DÃ¢n sá»±", "aliases": ["bo luat dan su"], "keywords": []},
            {"name": "Luáº­t Doanh nghiá»‡p", "aliases": ["luat doanh nghiep"], "keywords": []},
            {"name": "Luáº­t Trá»ng tÃ i thÆ°Æ¡ng máº¡i", "aliases": ["luat trong tai thuong mai"], "keywords": []},
        ]
    }
    coordinator.law_match_terms = coordinator._build_law_match_terms()
    record = {
        "url": "https://thuvienphapluat.vn/van-ban/Thu-tuc-To-tung/Luat-Trong-tai-thuong-mai-2010-108083.aspx",
        "title": "LuÃ¡ÂºÂ­t TrÃ¡Â»Âng tÃƒÂ i thÃ†Â°Ã†Â¡ng mÃ¡ÂºÂ¡i 2010 sÃ¡Â»â€˜ 54/2010/QH12 ÃƒÂ¡p dÃ¡Â»Â¥ng 2024 ...",
        "snippet": "ToÃƒÂ n vÃ„Æ’n LuÃ¡ÂºÂ­t TrÃ¡Â»Âng tÃƒÂ i thÃ†Â°Ã†Â¡ng mÃ¡ÂºÂ¡i 2010...",
        "law_id": "54/2010/QH12",
    }

    matched = coordinator._infer_law_name(record)

    assert matched == "Luáº­t Trá»ng tÃ i thÆ°Æ¡ng máº¡i"


def test_coordinator_reuses_law_id_mapping_for_unmatched_records():
    coordinator = ScrapingCoordinator.__new__(ScrapingCoordinator)
    coordinator.discovery_config = {
        "target_laws": [
            {"name": "Luật Trọng tài thương mại", "aliases": ["luat trong tai thuong mai"], "keywords": []},
        ]
    }
    coordinator.law_match_terms = coordinator._build_law_match_terms()

    grouped = coordinator._group_by_canonical_law(
        [
            {
                "url": "https://thuvienphapluat.vn/van-ban/Thu-tuc-To-tung/Luat-Trong-tai-thuong-mai-2010-108083.aspx",
                "title": "Luật Trọng tài thương mại 2010",
                "snippet": "",
                "search_query": "",
                "source_domain": "thuvienphapluat.vn",
                "law_id": "54/2010/QH12",
            },
            {
                "url": "https://congbao.chinhphu.vn/van-ban/luat-so-54-2010-qh12-2994.htm",
                "title": "Luáº­t sá»‘ 54/2010/QH12 luáº­t Trá»ng tÃ i thÆ°Æ¡ng máº¡i. - CÃ´ng bÃ¡o",
                "snippet": "",
                "search_query": "",
                "source_domain": "congbao.chinhphu.vn",
                "law_id": "54/2010/QH12",
            },
        ]
    )

    assert len(grouped["Luật Trọng tài thương mại"]["urls"]) == 2
    assert coordinator._last_unmatched_count == 0


def test_congbao_scraper_extracts_pdf_links_in_order():
    scraper = CongbaoScraper({"headers": {}, "timeout": 30, "retry_limit": 1}, validation_config={})
    html = """
    <div class="box-option-right">
      <ul class="list--open">
        <li><a class="item-open" href="/files/law-part-1.pdf">Part 1</a></li>
        <li><a class="item-open" href="https://congbao.chinhphu.vn/files/law-part-2.pdf">Part 2</a></li>
      </ul>
    </div>
    """
    assert scraper._extract_download_links(html, "https://congbao.chinhphu.vn/van-ban/example.htm") == [
        "https://congbao.chinhphu.vn/files/law-part-1.pdf",
        "https://congbao.chinhphu.vn/files/law-part-2.pdf",
    ]
