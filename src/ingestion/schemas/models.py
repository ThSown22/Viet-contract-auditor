from pydantic import BaseModel, Field, HttpUrl
from typing import Optional
from datetime import datetime


class DiscoveredLink(BaseModel):
    url: HttpUrl
    title: str
    source_domain: str  # Ví dụ: thuvienphapluat.vn
    search_query: str   # Từ khóa nào đã tìm ra nó
    discovered_at: datetime = Field(default_factory=datetime.now)
    snippet: Optional[str] = None  # Đoạn mô tả ngắn trên Google
    is_processed: bool = False    # Đánh dấu đã cào nội dung chưa
    query_id: str = ""            # ID của query template đã sinh ra kết quả này
    law_id: Optional[str] = None   # Số hiệu luật (VD: 91/2015/QH13)
    effective_date: Optional[str] = None  # Ngày có hiệu lực (VD: 2015-01-01)