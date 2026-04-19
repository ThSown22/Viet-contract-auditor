import logging
import os
from logging.handlers import TimedRotatingFileHandler

def setup_global_logging(log_file_name="viet_contract.log"):
    # 1. Tạo folder logs nếu chưa có
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 2. Định dạng Log: [Thời gian] - [Tên File] - [Cấp độ] - [Nội dung]
    log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # 3. Handler ghi vào file (Xoay vòng mỗi ngày, giữ 30 ngày)
    file_handler = TimedRotatingFileHandler(
        filename=os.path.join(log_dir, log_file_name),
        when="midnight",
        interval=1,
        backupCount=30,
        encoding="utf-8"
    )
    file_handler.setFormatter(log_format)

    # 4. Handler in ra màn hình (Để mày theo dõi trực tiếp)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)

    # 5. Cấu hình Logger gốc (Root Logger)
    # Chỉ setup 1 lần — tránh nhân bản handler khi import nhiều lần trong cùng process
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return 

    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    logging.info(" Hệ thống Logging đã được kích hoạt trên toàn bộ hệ thống.")