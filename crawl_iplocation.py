import os
import sys
import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from IP2Location import IP2Location
from config.database import get_database, test_connection, get_unique_ips_in_batches
from config.logging_config import setup_logger

# Setup logger
logger = setup_logger("main")
start = time.time()
db_lock = Lock()

def load_ip2location_database(bin_file_path="IP-COUNTRY-REGION-CITY.BIN"):
    """Khởi tạo IP2Location database"""
    try:
        if not os.path.exists(bin_file_path):
            raise FileNotFoundError(f"IP2Location database file not found: {bin_file_path}")
        
        logger.info(f"Loading IP2Location database: {bin_file_path}")
        
        # Dùng FILE_IO để tránh lỗi seek
        db = IP2Location(bin_file_path, "SHARED_MEMORY")

        logger.info("IP2Location database loaded successfully")
        return db
        
    except Exception as e:
        logger.error(f"Failed to load IP2Location database: {e}")
        raise

def get_location_info(ip_address, db):
    """Lấy thông tin location từ IP address"""
    try:
        record = db.get_all(ip_address)
        
        if record and record.country_long:
            location_info = { 
                'ip': ip_address,
                'country': record.country_long,
                'country_code': record.country_short,
                'region': record.region,
                'city': record.city
            }
            return location_info
        else:
            return None
            
    except Exception as e:
        logger.error(f"Error getting location for IP {ip_address}: {e}")
        return None

def process_single_ip(ip_address, ip2location_db):
    """Xử lý 1 IP và trả về location_info"""
    return get_location_info(ip_address, ip2location_db)

def process_ips_from_mongodb_multithread(collection_name="summary", batch_size=100000, output_csv_path="data/ip_locations.csv", max_workers=30):
    """Xử lý IPs từ MongoDB collection với multithreading và lưu CSV theo batch"""
    try:
        # Kết nối MongoDB
        db = get_database()
        if db is None:
            logger.error("Cannot connect to MongoDB")
            return False
        
        logger.info(f"Processing IPs from collection: {collection_name} with {max_workers} threads")
        
        # Load IP2Location database
        ip2location_db = load_ip2location_database()
        
        # Chuẩn bị CSV
        os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
        csv_file_exists = os.path.exists(output_csv_path)
        if not csv_file_exists:
            with open(output_csv_path, mode='w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=['ip', 'country', 'country_code', 'region', 'city'])
                writer.writeheader()
        
        # Counters
        processed = 0
        failed = 0
        
        # Duyệt theo batch
        for ip_batch in get_unique_ips_in_batches(collection_name, batch_size):
            batch_start = time.time()
            batch_total = len(ip_batch)
            logger.info(f"Processing batch of {batch_total} IPs (total processed: {processed + failed})")
            
            batch_results = []
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_ip = {executor.submit(process_single_ip, ip, ip2location_db): ip for ip in ip_batch}
                
                for future in as_completed(future_to_ip):
                    ip = future_to_ip[future]
                    try:
                        result = future.result()
                        if result:
                            batch_results.append(result)
                            processed += 1
                        else:
                            failed += 1
                    except Exception as e:
                        logger.error(f"Exception in thread for IP {ip}: {e}")
                        failed += 1

            # Ghi 1 lần sau khi batch xong
            if batch_results:
                with open(output_csv_path, mode='a', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=['ip', 'country', 'country_code', 'region', 'city'])
                    writer.writerows(batch_results)
            
            batch_end = time.time()
            elapsed = batch_end - batch_start
            ips_per_sec = batch_total / elapsed if elapsed > 0 else 0
            logger.info(f"Batch completed: {len(batch_results)}/{batch_total} IPs in {elapsed:.2f}s ({ips_per_sec:.2f} IPs/sec)")
        
        logger.info(f"Completed processing {processed} IPs successfully, {failed} failed")
        return True
        
    except Exception as e:
        logger.error(f"Error processing IPs from MongoDB: {e}")
        return False

def main():
    """Hàm main chính"""
    try:
        logger.info("Starting IP Location Processor with Multithreading Support")
        
        # Test MongoDB connection
        if not test_connection():
            logger.error("MongoDB connection failed. Exiting...")
            return False
        
        collection_name = "summary"
        max_workers = 30
    
        success = process_ips_from_mongodb_multithread(
            collection_name=collection_name,
            batch_size=100000,
            output_csv_path='data/ip_locations.csv',
            max_workers=max_workers
        )

        if success:
            logger.info("IP Location processing finished successfully!")
        else:
            logger.error("Some IP Location processing failed!")
        
        return success
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        return False
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
        return False

if __name__ == "__main__":
    success = main()
    end = time.time()
    elapsed = end - start
    logger.info(f"Process finished in {elapsed:.2f} seconds")
    sys.exit(0 if success else 1)
