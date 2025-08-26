#!/usr/bin/env python3
"""
Simple test script để test multithreading
Chạy nhanh để kiểm tra xem multithreading có hoạt động không
"""

import time
from main import (
    test_connection,
    process_all_ips,
    setup_logger
)

# Setup logger
logger = setup_logger("test_multithread")

def quick_test():
    """Test nhanh multithreading với ít IPs"""
    logger.info("🧪 === QUICK MULTITHREADING TEST ===")
    
    # Test MongoDB connection
    if not test_connection():
        logger.error("❌ MongoDB connection failed")
        return False
    
    # Test với batch size nhỏ và ít threads
    logger.info("🚀 Testing multithreading with 5 workers, batch size 10")
    
    start_time = time.time()
    success = process_all_ips(
        collection_name="ips",
        batch_size=10,  # Chỉ 10 IPs để test nhanh
        use_multithread=True,
        max_workers=5
    )
    end_time = time.time()
    
    if success:
        duration = end_time - start_time
        logger.info(f"✅ Multithreading test completed in {duration:.2f} seconds")
        return True
    else:
        logger.error("❌ Multithreading test failed")
        return False

def compare_single_vs_multi():
    """So sánh nhanh single vs multithread"""
    logger.info("⚡ === QUICK PERFORMANCE COMPARISON ===")
    
    if not test_connection():
        logger.error("❌ MongoDB connection failed")
        return False
    
    # Test single thread
    logger.info("🐌 Testing single-threaded processing...")
    start_time = time.time()
    success1 = process_all_ips(
        collection_name="ips",
        batch_size=10,
        use_multithread=False
    )
    single_time = time.time() - start_time
    
    if not success1:
        logger.error("❌ Single-threaded test failed")
        return False
    
    # Test multithread
    logger.info("🚀 Testing multithreaded processing...")
    start_time = time.time()
    success2 = process_all_ips(
        collection_name="ips",
        batch_size=10,
        use_multithread=True,
        max_workers=5
    )
    multi_time = time.time() - start_time
    
    if not success2:
        logger.error("❌ Multithreaded test failed")
        return False
    
    # So sánh kết quả
    logger.info("📊 === QUICK COMPARISON RESULTS ===")
    logger.info(f"🏁 Single-threaded: {single_time:.2f}s")
    logger.info(f"⚡ Multithreaded: {multi_time:.2f}s")
    
    if multi_time < single_time:
        speedup = single_time / multi_time
        logger.info(f"🎉 Multithreading is {speedup:.2f}x faster!")
    else:
        logger.info("⚠️ Multithreading didn't show improvement (expected for small batches)")
    
    return True

def main():
    """Main test function"""
    try:
        logger.info("🧪 Starting Quick Multithreading Tests")
        
        # Test cơ bản
        if not quick_test():
            return False
        
        # So sánh performance
        if not compare_single_vs_multi():
            return False
        
        logger.info("🎉 All tests completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"💥 Test failed with error: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 