from __future__ import annotations

VIETNAMESE_LEGAL_ENTITY_TYPES = [
    "CHỦ_THỂ",
    "KHÁI_NIỆM_PHÁP_LÝ",
    "HÀNH_VI_PHÁP_LÝ",
    "QUYỀN_NGHĨA_VỤ",
    "CHẾ_TÀI",
    "VIỆN_DẪN_PHÁP_LUẬT",
]


VIETNAMESE_LEGAL_ENTITY_EXTRACTION_PROMPT = """-Goal-
Bạn là chuyên gia phân tích pháp lý Việt Nam. Nhiệm vụ: trích xuất thực thể pháp lý và mối quan hệ có căn cứ trực tiếp từ văn bản được cung cấp, sau đó tổng hợp thành danh sách theo đúng định dạng của LightRAG.

-Nguyên tắc bắt buộc-
1. Chỉ dùng thông tin xuất hiện trực tiếp trong văn bản. Nếu văn bản không nói rõ thì không được suy diễn, không được bịa thực thể, không được bịa quan hệ.
2. Giữ nguyên viện dẫn pháp luật khi có thể, ví dụ: "Điều 430", "khoản 2 Điều 5", "điểm a khoản 1 Điều 10".
3. Văn bản đầu vào trong pipeline này thường có header:
   - "Nguồn luật: ..."
   - "Mã luật: ..."
   - "Tiêu đề điều: ..."
   Hãy dùng các thông tin đó để hiểu ngữ cảnh, xác định văn bản nguồn, và thêm trích dẫn nguồn trong mô tả khi phù hợp. Không tạo thực thể thừa chỉ vì header xuất hiện.
4. Nếu cùng một thực thể xuất hiện nhiều lần với cùng ý nghĩa trong cùng chunk, chỉ tạo một thực thể đại diện rõ ràng nhất. Không lặp lại cùng một entity dưới nhiều biến thể gần nghĩa.
5. Chỉ tạo quan hệ khi văn bản thể hiện rõ mối liên hệ pháp lý giữa hai thực thể.
6. Mọi mô tả thực thể và quan hệ phải ngắn gọn, chính xác, và nếu có thể thì kèm trích dẫn nguồn ở dạng: [Nguồn: <mã_luật> | <viện_dẫn>].
7. Không trả lời bằng giải thích tự do. Chỉ trả về tuple theo định dạng yêu cầu.
8. Với thực thể nhiều từ, phải giữ cụm đầy đủ có nghĩa pháp lý hoàn chỉnh; không tự ý cắt ngắn thành từng từ rời.
9. Nếu một thực thể có alias hoặc cách gọi rút gọn xuất hiện trong chunk, ưu tiên tên đầy đủ, rõ nghĩa nhất làm entity_name; alias chỉ nên được nhắc trong mô tả nếu thật sự cần.
10. Với viện dẫn pháp luật như tên luật, điều, khoản, điểm, hãy giữ nguyên cấu trúc viện dẫn gốc thay vì diễn đạt lại.
11. Chỉ tạo thực thể loại VIỆN_DẪN_PHÁP_LUẬT khi viện dẫn đó có vai trò pháp lý thực chất trong chunk, ví dụ được dùng để xác định căn cứ pháp lý, quyền, nghĩa vụ, chế tài hoặc khái niệm. Không cần tạo entity chỉ vì thấy xuất hiện một viện dẫn đơn lẻ.
12. Nếu điều luật hoặc tiêu đề điều nêu tên hoàn chỉnh của một quyền, nghĩa vụ, hành vi hoặc khái niệm pháp lý, hãy ưu tiên dùng chính tên đầy đủ đó làm entity_name.
13. Không rút gọn thực thể pháp lý thành head noun mơ hồ. Ví dụ: ưu tiên "QUYỀN CỦA CÁ NHÂN ĐỐI VỚI HÌNH ẢNH" hơn "HÌNH ẢNH", ưu tiên "QUYỀN ĐƯỢC KHAI SINH, KHAI TỬ" hơn "KHAI SINH", ưu tiên "QUYỀN XÁC ĐỊNH, XÁC ĐỊNH LẠI DÂN TỘC" hơn "DÂN TỘC".
14. Nếu nội dung điều luật mô tả một quyền hoặc nghĩa vụ của chủ thể đối với một đối tượng cụ thể, entity_name của vế đích phải là tên quyền/nghĩa vụ đầy đủ thay vì chỉ là đối tượng hoặc danh từ rút gọn.

-Steps-
1. Xác định tất cả thực thể. Với mỗi thực thể, trích xuất:
   - entity_name: Tên đầy đủ (viết hoa)
   - entity_type: Một trong các loại: {entity_types}
   - entity_description: Mô tả ngắn gọn, chính xác, có thể bao gồm trích dẫn điều khoản, ví dụ: "Bên mua có quyền yêu cầu giao hàng đúng hạn [Nguồn: 91/2015/QH13 | Điều 430]"
   Format: ('entity'{{tuple_delimiter}}<entity_name>{{tuple_delimiter}}<entity_type>{{tuple_delimiter}}<entity_description>)

   Hướng dẫn phân loại:
   - CHỦ_THỂ: cá nhân, pháp nhân, doanh nghiệp, cổ đông, trọng tài viên, tòa án, bên mua, bên bán...
   - KHÁI_NIỆM_PHÁP_LÝ: hợp đồng mua bán, thỏa thuận trọng tài, doanh nghiệp nhà nước, pháp nhân, cổ phần, phần vốn góp...
   - HÀNH_VI_PHÁP_LÝ: giao tài sản, đăng ký doanh nghiệp, thanh toán, góp vốn, giải quyết tranh chấp...
   - QUYỀN_NGHĨA_VỤ: quyền yêu cầu, nghĩa vụ giao hàng, trách nhiệm bồi thường, quyền của cá nhân đối với hình ảnh, quyền xác định, xác định lại dân tộc...
   - CHẾ_TÀI: phạt, bồi thường, hủy bỏ, vô hiệu, xử phạt...
   - VIỆN_DẪN_PHÁP_LUẬT: Bộ luật Dân sự 2015, Luật Doanh nghiệp 2020, Điều 430, khoản 2 Điều 5, điểm a khoản 1 Điều 10...

   Quy tắc đặt entity_name:
   - Ưu tiên tên đầy đủ, rõ nghĩa, ở dạng viết hoa.
   - Nếu tiêu đề điều đã nêu tên pháp lý đầy đủ, ưu tiên bám sát tên đó.
   - Không cắt tên thành mảnh quá nhỏ nếu văn bản đang nói về một khái niệm pháp lý hoàn chỉnh.
   - Không tạo entity chỉ từ từ chung chung vô nghĩa như "quy định", "trường hợp", "bên" nếu không đủ ngữ cảnh pháp lý.
   - Nếu có nhiều cách gọi gần nhau cho cùng một thực thể trong chunk, chọn một tên chuẩn nhất và dùng nhất quán.

2. Xác định tất cả quan hệ GIỮA các thực thể đã tìm được. Với mỗi quan hệ:
   - source_entity, target_entity: tên thực thể (phải khớp bước 1)
   - relationship_description: Mô tả quan hệ pháp lý rõ ràng, tự nhiên, nêu rõ bản chất quyền/nghĩa vụ/hành vi/chế tài, kèm trích dẫn điều khoản nếu có
   - relationship_strength: Số từ 1-10, trong đó:
     1-3 = liên hệ yếu hoặc gián tiếp
     4-7 = liên hệ rõ nhưng không phải trọng tâm chính
     8-10 = liên hệ trực tiếp, cốt lõi, được quy định rõ trong chunk
   Format: ('relationship'{{tuple_delimiter}}<source_entity>{{tuple_delimiter}}<target_entity>{{tuple_delimiter}}<relationship_description>{{tuple_delimiter}}<relationship_strength>)

   Quy tắc tạo quan hệ:
   - Chỉ tạo quan hệ khi có động từ, nghĩa vụ, quyền, chế tài, hoặc liên kết pháp lý rõ trong văn bản.
   - Không tạo quan hệ chỉ vì hai thực thể cùng xuất hiện gần nhau.
   - Nếu văn bản nêu một chủ thể có quyền/nghĩa vụ/hành vi đối với thực thể khác, ưu tiên mô tả quan hệ đó bằng tên thực thể đầy đủ từ bước 1.
   - Không dùng mô tả cụt hoặc chỉ lặp lại tên hai thực thể. Mô tả phải diễn đạt được nội dung pháp lý của mối quan hệ.
   - Nếu không xác định được bản chất quan hệ từ văn bản, bỏ quan hệ đó thay vì đoán.

3. Trả về danh sách tuple, dùng **{{record_delimiter}}** phân cách.

4. Kết thúc bằng {{completion_delimiter}}

-Ví dụ-
Văn bản: "[Nguồn: 91/2015/QH13 | Điều 430] Bên bán có nghĩa vụ giao tài sản đúng thời hạn đã thỏa thuận."

Output:
('entity'{{tuple_delimiter}}BÊN BÁN{{tuple_delimiter}}CHỦ_THỂ{{tuple_delimiter}}Bên bán trong hợp đồng mua bán [Nguồn: 91/2015/QH13 | Điều 430])
{{record_delimiter}}
('entity'{{tuple_delimiter}}GIAO TÀI SẢN ĐÚNG THỜI HẠN{{tuple_delimiter}}HÀNH_VI_PHÁP_LÝ{{tuple_delimiter}}Hành vi giao tài sản đúng thời hạn thỏa thuận [Nguồn: 91/2015/QH13 | Điều 430])
{{record_delimiter}}
('relationship'{{tuple_delimiter}}BÊN BÁN{{tuple_delimiter}}GIAO TÀI SẢN ĐÚNG THỜI HẠN{{tuple_delimiter}}Bên bán có nghĩa vụ giao tài sản đúng thời hạn theo Điều 430 Bộ luật Dân sự 2015{{tuple_delimiter}}9)
{{completion_delimiter}}

-Văn bản thực tế-
######################
{input_text}
######################
Output:
"""


VIETNAMESE_LEGAL_KEYWORDS_EXTRACTION_PROMPT = """-Mục tiêu-
Phân tích câu truy vấn pháp lý và trích xuất từ khóa theo hai cấp độ để phục vụ truy xuất trên kho dữ liệu pháp luật Việt Nam.

-Nguyên tắc-
1. Chuẩn hóa truy vấn thành các chủ đề pháp lý cấp cao và các thuật ngữ/viện dẫn cụ thể.
2. Giữ lại tên luật, điều, khoản, điểm, chủ thể, hành vi và chế tài nếu chúng xuất hiện trong câu hỏi.
3. Không thêm từ khóa không có căn cứ trong truy vấn.
4. Không giải thích thêm. Chỉ trả về JSON thuần hợp lệ.

-Định dạng đầu ra- JSON thuần (không có backtick):
{{"high_level_keywords": ["chủ đề pháp lý cấp cao"], "low_level_keywords": ["điều khoản, thuật ngữ cụ thể"]}}

Hướng dẫn:
- high_level_keywords: chủ đề rộng như "hợp đồng mua bán", "quản trị doanh nghiệp", "trọng tài thương mại", "quyền và nghĩa vụ dân sự"
- low_level_keywords: thuật ngữ cụ thể như "Điều 430", "bên mua", "đăng ký doanh nghiệp", "phán quyết trọng tài", "cổ đông sáng lập"

-Câu truy vấn-
{query}
"""


def apply_vietnamese_legal_prompts(prompts: dict) -> dict:
    prompts["DEFAULT_ENTITY_TYPES"] = VIETNAMESE_LEGAL_ENTITY_TYPES
    prompts["entity_extraction"] = VIETNAMESE_LEGAL_ENTITY_EXTRACTION_PROMPT
    prompts["keywords_extraction"] = VIETNAMESE_LEGAL_KEYWORDS_EXTRACTION_PROMPT
    return prompts
