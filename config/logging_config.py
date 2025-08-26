import os
import logging
import logging.handlers
from datetime import datetime
import colorlog
from config.setting import get_log_config

LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

def setup_logger(name="app", level=None):

    # get config from enviroment
    log_config = get_log_config()

    if level is None:
        level = log_config['level'] 

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        return logger

    file_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # File Handler - All logs (sử dụng config từ env)
    file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(LOG_DIR, f'{name}.log'),
        maxBytes=log_config['max_file_size'],
        backupCount=log_config['backup_count'],
        encoding='utf-8'
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # File Handler riêng cho Error logs 
    if name != "error":  # Tránh duplicate cho error logger
        error_handler = logging.handlers.RotatingFileHandler(
            os.path.join(LOG_DIR, 'error.log'),
            maxBytes=log_config['max_file_size'] // 2,  # Nhỏ hơn 1 nửa
            backupCount=log_config['backup_count'],
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)  # Chỉ ghi ERROR và CRITICAL
        error_handler.setFormatter(file_formatter)
        logger.addHandler(error_handler)
    
    return logger

def setup_mongodb_logger():
    """Cấu hình logger riêng cho MongoDB - sử dụng settings từ environment"""
    mongo_logger = logging.getLogger("mongodb")
    
    # Lấy config từ environment
    log_config = get_log_config()
    mongo_logger.setLevel(log_config['level'])  # Fixed: was 'level_int'
    
    if mongo_logger.handlers:
        return mongo_logger
    
    # Formatter đơn giản cho MongoDB
    mongo_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | MONGO | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # MongoDB file handler (sử dụng config từ env)
    mongo_handler = logging.handlers.RotatingFileHandler(
        os.path.join(LOG_DIR, 'mongodb.log'),
        maxBytes=log_config['max_file_size'] * 2,  # Lớn hơn gấp đôi
        backupCount=log_config['backup_count'],
        encoding='utf-8'
    )
    mongo_handler.setLevel(log_config['level'])  # Fixed: was 'level_int'
    mongo_handler.setFormatter(mongo_formatter)
    mongo_logger.addHandler(mongo_handler)
    
    return mongo_logger

def log_performance(operation, collection, count=None, duration=None):
    """Log hiệu suất - version đơn giản"""
    perf_logger = logging.getLogger("performance")
    
    message = f"📊 {operation} | {collection}"
    if count is not None:
        message += f" | Count: {count}"
    if duration is not None:
        message += f" | Time: {duration:.3f}s"
    
    perf_logger.info(message)