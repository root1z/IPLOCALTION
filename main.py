import os
import sys
import json
import csv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from IP2Location import IP2Location
from config.database import get_database, test_connection, get_unique_ips_in_batches
from config.logging_config import setup_logger

# Setup logger
logger = setup_logger("main")

# Global locks for thread-safe operations
db_lock = Lock()
file_lock = Lock()

def load_ip2location_database(bin_file_path="IP-COUNTRY-REGION-CITY.BIN"):
    """Khởi tạo IP2Location database"""
    try:
        if not os.path.exists(bin_file_path):
            raise FileNotFoundError(f"IP2Location database file not found: {bin_file_path}")
        
        logger.info(f"🔍 Loading IP2Location database: {bin_file_path}")
        db = IP2Location(bin_file_path, "SHARED_MEMORY")
        logger.info("✅ IP2Location database loaded successfully")
        return db
        
    except Exception as e:
        logger.error(f"❌ Failed to load IP2Location database: {e}")
        raise

def get_location_info(ip_address, db):
    """Lấy thông tin location từ IP address"""
    try:
        logger.debug(f"🔍 Querying location for IP: {ip_address}")
        record = db.get_all(ip_address)
        
        if record and record.country_long:
            location_info = {
                'ip': ip_address,
                'country': record.country_long,
                'country_code': record.country_short,
                'region': record.region,
                'city': record.city
            }
            logger.info(f"Found location: {location_info['country']}, {location_info['city']}")
            return location_info
        else:
            logger.warning(f"No location data found for IP: {ip_address}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Error getting location for IP {ip_address}: {e}")
        return None
        
def process_single_ip_with_csv(ip_data, ip2location_db, output_csv_path):
    """Xử lý một IP đơn lẻ trong thread riêng và ghi CSV"""
    try:
        ip_address = ip_data['ip']
        location_info = get_location_info(ip_address, ip2location_db)
        
        if location_info:
            # Ghi CSV (thread-safe)
            with file_lock:
                with open(output_csv_path, mode='a', newline='', encoding='utf-8') as csvfile:
                    fieldnames = ['ip', 'country', 'country_code', 'region', 'city']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    
                    writer.writerow(location_info)

            logger.debug(f"✅ Processed IP {ip_address} with location: {location_info['country']}, {location_info['city']}")
            return True
        else:
            logger.warning(f"⚠️ No location data for IP: {ip_address}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error processing IP {ip_data.get('ip', 'unknown')}: {e}")
        return False


def process_ips_from_mongodb_multithread(collection_name="summary", batch_size=10000, output_csv_path="data/ip_locations.csv", max_workers=30):
    """Xử lý IPs từ MongoDB collection với multithreading và ghi CSV"""
    try:
        # Kết nối MongoDB
        db = get_database()
        if db is None:
            logger.error("❌ Cannot connect to MongoDB")
            return False
        
        collection = db[collection_name]
        logger.info(f"📊 Processing IPs from collection: {collection_name} with {max_workers} threads")
        
        # Chuẩn bị CSV
        os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
        csv_file_exists = os.path.exists(output_csv_path)
        
        # Load IP2Location database
        ip2location_db = load_ip2location_database()
        
        # Xử lý IPs với ThreadPoolExecutor
        processed = 0
        failed = 0
        
        # Duyệt theo batch các IP duy nhất
        for ip_batch in get_unique_ips_in_batches(collection_name, batch_size):
            batch_total = len(ip_batch)
            logger.info(f"📊 Processing batch of {batch_total} IPs (total processed: {processed + failed})")
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit tất cả IPs trong batch
                future_to_ip = {
                    executor.submit(process_single_ip_with_csv, {'ip': ip}, ip2location_db, output_csv_path): ip 
                    for ip in ip_batch
                }
                
                # Xử lý kết quả khi hoàn thành
                batch_processed = 0
                batch_failed = 0
                for future in as_completed(future_to_ip):
                    ip = future_to_ip[future]
                    try:
                        result = future.result()
                        if result:
                            processed += 1
                            batch_processed += 1
                        else:
                            failed += 1
                            batch_failed += 1
                    except Exception as e:
                        logger.error(f"❌ Exception in thread for IP {ip}: {e}")
                        failed += 1
                        batch_failed += 1
                
                logger.info(f"✅ Batch completed: {batch_processed}/{batch_total} IPs processed successfully")
        
        logger.info(f"🎉 Completed processing {processed} IPs successfully, {failed} failed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error processing IPs from MongoDB: {e}")
        return False


def process_all_ips(collection_name="summary", batch_size=10000, use_multithread=True, max_workers=30, output_json_path=None):
    """Xử lý tất cả IPs từ MongoDB"""
    if use_multithread:
        logger.info(f"📊 Option 1: Processing all IPs with multithreading ({max_workers} threads)...")
        return process_ips_from_mongodb_multithread(collection_name, batch_size, max_workers=max_workers, output_json_path=output_json_path)
    else:
        logger.info("📊 Option 1: Processing all IPs with single thread...")
        # Reuse multithread pipeline with a single worker
        return process_ips_from_mongodb_multithread(collection_name, batch_size, max_workers=1, output_json_path=output_json_path)

def main():
    """Hàm main chính với options mới và multithreading"""
    try:
        logger.info("🚀 Starting IP Location Processor with Multithreading Support")
        
        # Test MongoDB connection
        if not test_connection():
            logger.error("❌ MongoDB connection failed. Exiting...")
            return False
        
        # Options xử lý
        collection_name = "summary"  # Tên collection MongoDB
        max_workers = 30  # Số lượng threads tối đa
    
        
        # Chạy luồng xử lý mới: unique IPs -> IP2Location -> lưu Mongo + CSV
        success = process_ips_from_mongodb_multithread(collection_name=collection_name, batch_size=10000, output_csv_path='data/ip_locations.csv',max_workers=max_workers)

        if success:
            logger.info("🎉 IP Location processing finished successfully!")
        else:
            logger.error("💥 Some IP Location processing failed!")
        
        return success
        
    except KeyboardInterrupt:
        logger.info("⏹️ Process interrupted by user")
        return False
    except Exception as e:
        logger.error(f"💥 Unexpected error in main: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
