from pymongo import MongoClient
from config.logging_config import setup_logger, setup_mongodb_logger
from config.setting import get_database as get_db_config, MONGO_URI, MONGO_DB

# Setup loggers
app_logger = setup_logger("database")
mongo_logger = setup_mongodb_logger()

def get_database():
    """Tạo kết nối đến MongoDB sử dụng config từ environment"""
    try:
        db_config = get_db_config()

        app_logger.info("CConnecting to MongoDB...")
        mongo_logger.debug(f"Using URI: {MONGO_URI.split('@')[0] if '@' in MONGO_URI else MONGO_URI}@***")
        mongo_logger.debug(f"Database: {db_config['database']}")
        mongo_logger.debug(f"Host: {db_config['host']}:{db_config['port']}")
        
        # Tạo connection
        client = MongoClient(MONGO_URI)
        
        # Test connection
        client.admin.command('ping')

        app_logger.info(f"Connected to MongoDB: {MONGO_DB}")
        mongo_logger.info(f"Connected successfully to database: {MONGO_DB}")
        
        return client[MONGO_DB]
        
    except Exception as e:
        app_logger.error(f"Error connecting to MongoDB: {e}")
        mongo_logger.error(f"Connection failed: {str(e)}")
        return None

def get_client():
    """Trả về MongoDB client (để quản lý connection)"""
    try:
        client = MongoClient(MONGO_URI)
        client.admin.command('ping')
        return client
    except Exception as e:
        app_logger.error(f"Error creating MongoDB client: {e}")
        return None

def test_connection():
    """Test kết nối MongoDB và hiển thị thông tin"""
    try:
        app_logger.info("Testing MongoDB connection...")
        
        client = get_client()
        if client is None:
            return False
            
        # Lấy thông tin server
        server_info = client.server_info()
        db = client[MONGO_DB]
        
        app_logger.info(f"MongoDB Server Version: {server_info['version']}")
        app_logger.info(f"Connected to database: {MONGO_DB}")
        
        # Liệt kê collections
        collections = db.list_collection_names()
        if collections:
            app_logger.info(f"Collections: {', '.join(collections)}")
        else:
            app_logger.info("No collections found")
            
        client.close()
        return True
        
    except Exception as e:
        app_logger.error(f"Connection test failed: {e}")
        return False

def close_connection(client):
    """Đóng kết nối MongoDB"""
    if client is not None:
        try:
            client.close()
            app_logger.info("Connection closed MongoDB")
            mongo_logger.info("Connection closed successfully")
        except Exception as e:
            app_logger.error(f"Error closing connection: {e}")
            mongo_logger.error(f"Error closing connection: {str(e)}")

def get_unique_ips_in_batches(collection_name="summary", limit=100000):
    """Lấy danh sách IP distinct theo batch"""
    try:
        db = get_database()
        if db is None:
            return

        collection = db[collection_name]
        app_logger.info(f"Streaming distinct IPs from collection: {collection_name}")

        cursor = collection.aggregate(
            [
                {"$group": {"_id": "$ip"}}
            ],
            allowDiskUse=True
        )

        batch = []
        for doc in cursor:
            batch.append(doc["_id"])
            if len(batch) >= limit:
                yield batch
                batch = []

        if batch:
            yield batch

    except Exception as e:
        app_logger.error(f"❌ Error getting unique IPs in batches: {e}")
        return
