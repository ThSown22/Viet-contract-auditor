from pydantic import BaseModel, HttpUrl
from typing import Optional, List
from datetime import datetime

class DiscoveredLink(BaseModel):
    url: HttpUrl
    title: str
    source_domain: str  # Ví dụ: thuvienphapluat.vn
    search_query: str   # Từ khóa nào đã tìm ra nó
    discovered_at: datetime = datetime.now()
    snippet: Optional[str] = None # Đoạn mô tả ngắn trên Google
    is_processed: bool = False    # Đánh dấu đã cào nội dung chưa