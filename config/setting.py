import os
from dotenv import load_dotenv

# Load environment variables từ file .env
load_dotenv()

# MongoDB connection
MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_PORT = os.getenv("MONGO_PORT")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE')
# MongoDB URI - ưu tiên MONGO_URI nếu có, nếu không thì build từ components
MONGO_URI = os.getenv('MONGO_URI')
if not MONGO_URI:
    if MONGO_USER and MONGO_PASSWORD:
        # Có authentication
        MONGO_URI = f'mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource={MONGO_AUTH_SOURCE}'
    else:
        # Không có authentication
        MONGO_URI = f'mongodb://{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}'

# Logging Settings
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
LOG_MAX_FILE_SIZE = int(os.getenv('LOG_MAX_FILE_SIZE', '10485760'))  # 10MB default
LOG_BACKUP_COUNT = int(os.getenv('LOG_BACKUP_COUNT', '5'))

def get_database():
    return {
        'host': MONGO_HOST,
        'port': MONGO_PORT,
        'database': MONGO_DB,
        'username': MONGO_USER,
        'password': MONGO_PASSWORD
    }

def get_log_config():
    return {
        'level': LOG_LEVEL,
        'max_file_size': LOG_MAX_FILE_SIZE,
        'backup_count': LOG_BACKUP_COUNT
        }
