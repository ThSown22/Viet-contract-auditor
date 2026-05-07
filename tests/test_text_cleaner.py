"""Unit tests for VietnameseLegalTextCleaner."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ingestion.cleaning.text_cleaner import VietnameseLegalTextCleaner
from src.ingestion.cleaning.validator import CleanedContentValidator


def test_remove_document_header():
    cleaner = VietnameseLegalTextCleaner()

    text = """
VĂN PHÒNG QUỐC HỘI
-------
CỘNG HÒA XÃ HỘI CHỦ NGHĨA VIỆT NAM
Số: 60/VBHN-VPQH

Chương I
NHỮNG QUY ĐỊNH CHUNG
Điều 1. Test
"""

    result = cleaner.remove_document_header(text)

    assert result.startswith("Chương I")
    assert "VĂN PHÒNG" not in result
    assert "Số: 60" not in result


def test_normalize_linebreaks():
    cleaner = VietnameseLegalTextCleaner()

    text = "Điều 2. Thẩm quyền giải quyết\ncác tranh chấp của Trọng tài\n1. Tranh chấp phát sinh từ hoạt động\nthương mại."
    result = cleaner.normalize_linebreaks(text)

    assert result == (
        "Điều 2. Thẩm quyền giải quyết các tranh chấp của Trọng tài\n"
        "1. Tranh chấp phát sinh từ hoạt động thương mại."
    )


def test_repair_article_markers():
    cleaner = VietnameseLegalTextCleaner()

    text = (
        "2. Người thành niên có năng lực hành vi dân sự đầy đủ theo Bộ luật này Điều\n"
        "21. Người chưa thành niên\n"
        "1. Người chưa thành niên là người chưa đủ mười tám tuổi.\n\n"
        "Điều 81. Tài sản của pháp nhân\n"
        "Nội dung điều 81.\n\n"
        "Điều 1. Pháp nhân được thành lập\n"
        "Nội dung kế tiếp.\n\n"
        "Điều 83. Cơ cấu tổ chức của pháp nhân\n"
    )

    result = cleaner.repair_article_markers(text)

    assert "2. Người thành niên có năng lực hành vi dân sự đầy đủ theo Bộ luật này" in result
    assert "\nĐiều 21. Người chưa thành niên\n" in result
    assert "\nĐiều 82. Pháp nhân được thành lập\n" in result


def test_repair_article_headers():
    cleaner = VietnameseLegalTextCleaner()

    text = (
        "Điều 16. Các hành vi bị\n"
        "nghiêm cấm\n"
        "1. Cấm kê khai giả mạo.\n\n"
        "Điều 2. Đối tượng áp dụng 1. Doanh nghiệp.\n"
        "2. Cơ quan, tổ chức có liên quan.\n\n"
        "Điều 18. Không hạn chế năng lực pháp luật dân sự của cá nhân Năng lực pháp luật dân sự của cá nhân không bị hạn chế, trừ trường hợp luật có quy định khác.\n"
    )

    result = cleaner.repair_article_headers(text)

    assert "Điều 16. Các hành vi bị nghiêm cấm\n1. Cấm kê khai giả mạo." in result
    assert "Điều 2. Đối tượng áp dụng\n1. Doanh nghiệp.\n2. Cơ quan, tổ chức có liên quan." in result
    assert (
        "Điều 18. Không hạn chế năng lực pháp luật dân sự của cá nhân\n"
        "Năng lực pháp luật dân sự của cá nhân không bị hạn chế, trừ trường hợp luật có quy định khác."
    ) in result


def test_repair_article_headers_keeps_multiline_title_together():
    cleaner = VietnameseLegalTextCleaner()

    text = (
        "Điều 4. Áp dụng\n"
        "Bộ luật dân sự\n"
        "1. Bộ luật này là luật chung điều chỉnh các quan hệ dân sự.\n"
    )

    result = cleaner.repair_article_headers(text)

    assert result.startswith("Điều 4. Áp dụng Bộ luật dân sự")
    assert "\n1. Bộ luật này là luật chung điều chỉnh các quan hệ dân sự." in result


def test_repair_article_headers_recovers_title_after_blank_line():
    cleaner = VietnameseLegalTextCleaner()

    text = (
        "Điều 142.\n"
        "\n"
        "Chương trình và nội dung họp Đại hội đồng cổ đông\n"
        "1. Người triệu tập họp Đại hội đồng cổ đông phải chuẩn bị.\n"
    )

    result = cleaner.repair_article_headers(text)

    assert result.startswith("Điều 142. Chương trình và nội dung họp Đại hội đồng cổ đông")
    assert "\n1. Người triệu tập họp Đại hội đồng cổ đông phải chuẩn bị." in result


def test_repair_article_headers_splits_inline_bodies_and_extends_short_titles():
    cleaner = VietnameseLegalTextCleaner()

    text = (
        "Điều 6. Tòa án từ chối thụ lý\n"
        "trong trường hợp có thỏa thuận trọng tài\n"
        "Trong trường hợp các bên tranh chấp đã có thỏa thuận trọng tài.\n"
        "Điều 7. Xác định Tòa án có thẩm quyền đối với hoạt động trọng tài\n"
        "1. Trường hợp các bên đã có thỏa thuận lựa chọn.\n"
        "Điều 72. Lệ phí tòa án liên quan đến Trọng tài Lệ phí về yêu cầu Tòa án chỉ định Trọng tài viên, áp dụng biện pháp khẩn cấp tạm thời được thực hiện theo quy định.\n"
        "Điều 74. Hình thức hoạt động của Tổ chức trọng tài nước ngoài tại Việt Nam Tổ chức trọng tài nước ngoài hoạt động tại Việt Nam dưới các hình thức sau đây:\n"
        "1. Chi nhánh.\n"
    )

    result = cleaner.repair_article_headers(cleaner.normalize_linebreaks(text))

    assert "Điều 6. Tòa án từ chối thụ lý trong trường hợp có thỏa thuận trọng tài\nTrong trường hợp các bên tranh chấp đã có thỏa thuận trọng tài." in result
    assert "Điều 7. Xác định Tòa án có thẩm quyền đối với hoạt động trọng tài\n1. Trường hợp các bên đã có thỏa thuận lựa chọn." in result
    assert "Điều 72. Lệ phí tòa án liên quan đến Trọng tài\nLệ phí về yêu cầu Tòa án chỉ định Trọng tài viên, áp dụng biện pháp khẩn cấp tạm thời được thực hiện theo quy định." in result
    assert "Điều 74. Hình thức hoạt động của Tổ chức trọng tài nước ngoài tại Việt Nam\nTổ chức trọng tài nước ngoài hoạt động tại Việt Nam dưới các hình thức sau đây:\n1. Chi nhánh." in result


def test_repair_definition_blocks():
    cleaner = VietnameseLegalTextCleaner()

    text = (
        "Điều 3. Giải thích từ ngữ\n"
        "Trong Luật này, các từ ngữ dưới đây được hiểu như sau:\n"
        "Trọng tài thương mại\n"
        "là phương thức giải quyết tranh chấp.\n"
        "Thỏa thuận trọng tài\n"
        "là thỏa thuận giữa các bên.\n"
        "Địa điểm giải quyết tranh chấp\n"
        "là nơi Hội đồng trọng tài tiến hành giải quyết tranh chấp.\n"
        "Nếu địa điểm giải quyết tranh chấp được tiến hành trên lãnh thổ Việt Nam thì phán quyết phải được coi là tuyên tại Việt Nam.\n"
        "10.\n"
        "Phán quyết trọng tài\n"
        "là quyết định cuối cùng.\n"
    )

    result = cleaner.repair_definition_blocks(text)

    assert "\n1. Trọng tài thương mại là phương thức giải quyết tranh chấp." in result
    assert "\n2. Thỏa thuận trọng tài là thỏa thuận giữa các bên." in result
    assert (
        "\n3. Địa điểm giải quyết tranh chấp là nơi Hội đồng trọng tài tiến hành giải quyết tranh chấp. "
        "Nếu địa điểm giải quyết tranh chấp được tiến hành trên lãnh thổ Việt Nam thì phán quyết phải được coi là tuyên tại Việt Nam."
    ) in result
    assert "\n4. Phán quyết trọng tài là quyết định cuối cùng." in result


def test_repair_definition_blocks_splits_orphan_entries_and_renumbers():
    cleaner = VietnameseLegalTextCleaner()

    text = (
        "Điều 3. Giải thích từ ngữ\n"
        "Trong Luật này, các từ ngữ dưới đây được hiểu như sau:\n"
        "1. Trọng tài thương mại là phương thức giải quyết tranh chấp.\n"
        "3. Tranh chấp có yếu tố nước ngoài là tranh chấp phát sinh trong quan hệ thương mại.\n"
        "Trọng tài viên\n"
        "là người được các bên lựa chọn.\n"
        "10. Phán quyết trọng tài là quyết định cuối cùng.\n"
    )

    result = cleaner.repair_definition_blocks(text)
    lines = [line for line in result.splitlines() if line.startswith(("1.", "2.", "3.", "4."))]

    assert lines == [
        "1. Trọng tài thương mại là phương thức giải quyết tranh chấp.",
        "2. Tranh chấp có yếu tố nước ngoài là tranh chấp phát sinh trong quan hệ thương mại.",
        "3. Trọng tài viên là người được các bên lựa chọn.",
        "4. Phán quyết trọng tài là quyết định cuối cùng.",
    ]


def test_repair_definition_blocks_splits_embedded_entries():
    cleaner = VietnameseLegalTextCleaner()

    text = (
        "Điều 3. Giải thích từ ngữ\n"
        "Trong Luật này, các từ ngữ dưới đây được hiểu như sau:\n"
        "3. Tranh chấp có yếu tố nước ngoài là tranh chấp phát sinh trong quan hệ thương mại, quan hệ pháp luật khác có yếu tố nước ngoài được quy định tại Bộ luật dân sự Trọng tài viên là người được các bên lựa chọn.\n"
        "4. Quyết định trọng tài là quyết định của Hội đồng trọng tài.\n"
    )

    result = cleaner.repair_definition_blocks(text)

    assert "\n1. Tranh chấp có yếu tố nước ngoài là tranh chấp phát sinh trong quan hệ thương mại, quan hệ pháp luật khác có yếu tố nước ngoài được quy định tại Bộ luật dân sự" in result
    assert "\n2. Trọng tài viên là người được các bên lựa chọn." in result
    assert "\n3. Quyết định trọng tài là quyết định của Hội đồng trọng tài." in result


def test_repair_inline_lettered_items():
    cleaner = VietnameseLegalTextCleaner()

    text = (
        "23. Người có liên quan là cá nhân, tổ chức có quan hệ trực tiếp hoặc gián tiếp với doanh nghiệp trong các trường hợp sau đây: "
        "a) Công ty mẹ, người quản lý và người đại diện theo pháp luật của công ty mẹ; "
        "b) Công ty con, người quản lý và người đại diện theo pháp luật của công ty con; "
        "c) Cá nhân, tổ chức có khả năng chi phối hoạt động của doanh nghiệp đó."
    )

    result = cleaner.repair_inline_lettered_items(text)

    assert "23. Người có liên quan là cá nhân, tổ chức có quan hệ trực tiếp hoặc gián tiếp với doanh nghiệp trong các trường hợp sau đây:" in result
    assert "\na) Công ty mẹ, người quản lý và người đại diện theo pháp luật của công ty mẹ;" in result
    assert "\nb) Công ty con, người quản lý và người đại diện theo pháp luật của công ty con;" in result
    assert "\nc) Cá nhân, tổ chức có khả năng chi phối hoạt động của doanh nghiệp đó." in result


def test_repair_orphan_lines():
    cleaner = VietnameseLegalTextCleaner()

    text = (
        "Nếu địa điểm giải quyết tranh chấp được tiến hành trên lãnh thổ\n"
        "Việt\n"
        "Nam thì phán quyết phải được coi là tuyên tại Việt Nam.\n\n"
        "quy định tại\n"
        "Điều 20 của Luật này\n"
        "đề nghị thành lập.\n"
    )

    result = cleaner.repair_orphan_lines(text)

    assert "trên lãnh thổ Việt Nam thì phán quyết" in result
    assert "quy định tại Điều 20 của Luật này đề nghị thành lập." in result


def test_repair_orphan_lines_keeps_article_body_on_next_line():
    cleaner = VietnameseLegalTextCleaner()

    text = (
        "Điều 73. Điều kiện hoạt động của Tổ chức trọng tài nước ngoài tại Việt Nam\n"
        "Tổ chức trọng tài nước ngoài đã được thành lập và đang hoạt động hợp pháp.\n"
    )

    result = cleaner.repair_orphan_lines(text)

    assert result.strip() == text.strip()


def test_remove_structural_markers():
    cleaner = VietnameseLegalTextCleaner()

    text = (
        "Điều 15. Hủy quyết định cá biệt trái pháp luật\n"
        "Nội dung điều 15.\n\n"
        "Chương III\n"
        "CÁ NHÂN\n\n"
        "Mục 1.\n"
        "NĂNG LỰC PHÁP LUẬT DÂN SỰ, NĂNG LỰC HÀNH VI DÂN SỰ CỦA CÁ NHÂN\n\n"
        "Điều 16. Năng lực pháp luật dân sự của cá nhân\n"
        "Nội dung điều 16.\n"
    )

    result = cleaner.remove_structural_markers(text)

    assert "Chương III" not in result
    assert "CÁ NHÂN" not in result
    assert "Mục 1." not in result
    assert "NĂNG LỰC PHÁP LUẬT DÂN SỰ" not in result
    assert "Điều 16. Năng lực pháp luật dân sự của cá nhân" in result


def test_deduplicate_lines():
    cleaner = VietnameseLegalTextCleaner()

    text = "Line A\nLine A\nLine B\n\nLine B\nLine C"
    result = cleaner.deduplicate_lines(text)

    assert result == "Line A\nLine B\n\nLine B\nLine C"


def test_repair_ocr_spacing():
    cleaner = VietnameseLegalTextCleaner()

    text = (
        "2. Cá nhân, pháp nhân xác lập, thực hiện, chấm dứt quyền, nghĩa vụ dân sự của mình trên c ơ s ở t ự do, "
        "t ự nguy ện cam k ết, th ỏa thu ận. M ọi cam k ết, th ỏa thu ận không vi phạm điều cấm của luật.\n"
        "4. Tr ường h ợp có s ự khác nhau giữa quy định c ủa B ộ lu ật này và điều ước quốc tế thì áp dụng quy định khác.\n"
        "2. Luật khác có liên quan điều chỉnh quan hệ dân sự trong các l ĩnh vực cụ thể.\n"
        "5. Cá nhân, pháp nhân ph ải tự chịu trách nhi ệm về việc không thực hiện."
    )

    result = cleaner.repair_ocr_spacing(text)

    assert "trên cơ sở tự do, tự nguyện cam kết, thỏa thuận." in result
    assert "Mọi cam kết, thỏa thuận không vi phạm điều cấm của luật." in result
    assert "4. Trường hợp có sự khác nhau giữa quy định của Bộ luật này và điều ước quốc tế thì áp dụng quy định khác." in result
    assert "các lĩnh vực cụ thể." in result
    assert "phải tự chịu trách nhiệm" in result


def test_apply_production_cleaning_removes_publication_footer_between_list_items():
    cleaner = VietnameseLegalTextCleaner()

    text = (
        "Điều 3. Các nguyên tắc cơ bản của pháp luật dân sự\n"
        "4. Việc xác lập, thực hiện, chấm dứt quyền, nghĩa vụ dân sự không được xâm phạm đến lợi ích quốc gia.\n"
        "4 CÔNG BÁO/Số 1243 + 1244/Ngày 28-12-2015\n"
        "\n"
        "5. Cá nhân, pháp nhân phải tự chịu trách nhiệm về việc không thực hiện.\n"
        "CÔNG BÁO/Số 1243 + 1244/Ngày 28-12-2015 5\n"
    )

    result = cleaner.apply_production_cleaning(text)

    assert "CÔNG BÁO/Số 1243 + 1244/Ngày 28-12-2015" not in result
    assert "CÔNG BÁO/Số 1243 + 1244/Ngày 28-12-2015 5" not in result
    assert "lợi ích quốc gia.\n5. Cá nhân" in result


def test_apply_production_cleaning_removes_inline_publication_artifacts():
    cleaner = VietnameseLegalTextCleaner()

    text = (
        "b) Có sự đồng ý của cha, mẹ; 10 CÔNG BÁO/Số 1243 + 1244/Ngày 28-12-2015\n"
        "1. Trường hợp sau đây: CÔNG BÁO/Số 1243 + 1244/Ngày 28-12-2015 35\n"
    )

    result = cleaner.apply_production_cleaning(text)

    assert "CÔNG BÁO/Số 1243 + 1244/Ngày 28-12-2015" not in result
    assert "b) Có sự đồng ý của cha, mẹ;" in result
    assert "1. Trường hợp sau đây:" in result


def test_apply_production_cleaning_removes_source_continuation_artifacts():
    cleaner = VietnameseLegalTextCleaner()

    text = (
        "Điều 364. Lỗi trong trách nhiệm dân sự\n"
        "Lỗi trong trách nhiệm dân sự bao gồm lỗi cố ý, lỗi vô ý.\n"
        "(Xem tiếp Công báo số 1245 + 1246)\n"
        "PHẦN VĂN BẢN QUY PHẠM PHÁP LUẬT\n"
        "CHỦ TỊCH NƯỚC - QUỐC HỘI\n"
        "Luật số 91/2015/QH13 ngày 24 tháng 11 năm 2015 Bộ luật dân sự\n"
        "Phần thứ ba NGHĨA VỤ VÀ HỢP ĐỒNG\n"
        "(Tiếp theo Công báo số 1243 + 1244)\n"
        "Điều 365. Chuyển giao quyền yêu cầu\n"
    )

    result = cleaner.clean(text)

    assert "Xem tiếp Công báo" not in result
    assert "Tiếp theo Công báo" not in result
    assert "PHẦN VĂN BẢN QUY PHẠM PHÁP LUẬT" not in result
    assert "CHỦ TỊCH NƯỚC - QUỐC HỘI" not in result
    assert "Luật số 91/2015/QH13" not in result
    assert "Phần thứ ba NGHĨA VỤ VÀ HỢP ĐỒNG" not in result
    assert "Điều 365. Chuyển giao quyền yêu cầu" in result


def test_validator_rejects_structural_markers():
    validator = CleanedContentValidator(min_chars=10)
    text = "Điều 1. Phạm vi điều chỉnh\nNội dung.\nChương II\nDOANH NGHIỆP"

    is_valid, error_message = validator.validate(text, "Luật mẫu")

    assert is_valid is False
    assert "marker cấu trúc" in (error_message or "")


def test_validator_rejects_broken_definition_numbering():
    validator = CleanedContentValidator(min_chars=10)
    text = (
        "Điều 1. Phạm vi điều chỉnh\nNội dung.\n\n"
        "Điều 3. Giải thích từ ngữ\n"
        "Trong Luật này, các từ ngữ dưới đây được hiểu như sau:\n"
        "1. A là ...\n"
        "3. B là ...\n"
    )

    is_valid, error_message = validator.validate(text, "Luật mẫu")

    assert is_valid is False
    assert "expected 2." in (error_message or "")


def test_full_pipeline():
    cleaner = VietnameseLegalTextCleaner()

    raw_text = """
VĂN PHÒNG QUỐC HỘI
Số: 60/VBHN-VPQH

Căn cứ Hiến pháp...

Chương I
NHỮNG QUY ĐỊNH CHUNG
Điều 1.
Phạm vi điều chỉnh
Luật này quy định về thẩm quyền...

Chương III
CÁ NHÂN

Mục 1.
NĂNG LỰC PHÁP LUẬT DÂN SỰ, NĂNG LỰC HÀNH VI DÂN SỰ CỦA CÁ NHÂN

Điều 16. Năng lực pháp luật dân sự của cá nhân
1. Mọi cá nhân đều có năng lực pháp luật dân sự.

Điều 3. Giải thích từ ngữ
Trong Luật này, các từ ngữ dưới đây được hiểu như sau:
Trọng tài thương mại
là phương thức giải quyết tranh chấp.
Thỏa thuận trọng tài
là thỏa thuận giữa các bên.

Nơi nhận:
- Văn phòng Chính phủ
"""

    result = cleaner.clean(raw_text)

    assert result.startswith("Điều 1. Phạm vi điều chỉnh")
    assert "VĂN PHÒNG" not in result
    assert "Số: 60" not in result
    assert "Chương I" not in result
    assert "Chương III" not in result
    assert "Mục 1." not in result
    assert "CÁ NHÂN" not in result
    assert "Nơi nhận" not in result
    assert "Điều 16. Năng lực pháp luật dân sự của cá nhân" in result
    assert "\n1. Trọng tài thương mại là phương thức giải quyết tranh chấp." in result
    assert "\n2. Thỏa thuận trọng tài là thỏa thuận giữa các bên." in result
