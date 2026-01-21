#!/usr/bin/env python3
"""
GA4 Data Sync Service for Windows
Chạy liên tục trong background và sync dữ liệu định kỳ
"""

import time
import schedule
import logging
import sys
import os
from datetime import datetime
from app import fetch_ga4_realtime_data, save_to_database

# Cấu hình logging
log_file = 'sync_service.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Cấu hình
SYNC_INTERVAL_MINUTES = 2  # Sync mỗi 2 phút
MAX_LOG_SIZE_MB = 10  # Giới hạn file log 10MB

def rotate_log():
    """Xoay vòng log file nếu quá lớn"""
    try:
        if os.path.exists(log_file):
            size_mb = os.path.getsize(log_file) / (1024 * 1024)
            if size_mb > MAX_LOG_SIZE_MB:
                # Backup log cũ
                backup_name = f"sync_service_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
                os.rename(log_file, backup_name)
                logging.info(f"Rotated log file to {backup_name}")
                
                # Xóa backup cũ hơn 7 ngày
                for f in os.listdir('.'):
                    if f.startswith('sync_service_') and f.endswith('.log'):
                        file_time = os.path.getmtime(f)
                        if time.time() - file_time > 7 * 86400:  # 7 ngày
                            os.remove(f)
                            logging.info(f"Deleted old log: {f}")
    except Exception as e:
        logging.error(f"Error rotating log: {e}")

def sync_job():
    """Job sync dữ liệu từ GA4"""
    try:
        logging.info("=" * 60)
        logging.info("🔄 Starting GA4 data sync...")
        
        # Fetch dữ liệu
        start_time = time.time()
        ga4_data = fetch_ga4_realtime_data()
        
        # Đếm số records
        users_5min = len(ga4_data['users_5min'].rows)
        users_30min = len(ga4_data['users_30min'].rows)
        device_data = len(ga4_data['by_device'].rows)
        page_data = len(ga4_data['by_page'].rows)
        
        logging.info(f"   📍 Location (5min): {users_5min} rows")
        logging.info(f"   📍 Location (30min): {users_30min} rows")
        logging.info(f"   📱 Device: {device_data} rows")
        logging.info(f"   📄 Page: {page_data} rows")
        
        # Lưu vào database
        save_to_database(ga4_data)
        
        elapsed = time.time() - start_time
        logging.info(f"✅ Sync completed in {elapsed:.2f} seconds")
        logging.info(f"   Next sync: {datetime.now() + timedelta(minutes=SYNC_INTERVAL_MINUTES)}")
        
        # Rotate log nếu cần
        rotate_log()
        
    except Exception as e:
        logging.error(f"❌ Sync failed: {e}")
        logging.exception("Full error traceback:")

def check_health():
    """Kiểm tra health của service"""
    try:
        import psycopg2
        from dotenv import load_dotenv
        
        load_dotenv()
        
        # Test DB connection
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        conn.close()
        
        logging.info("💚 Health check: Database OK")
        return True
    except Exception as e:
        logging.error(f"💔 Health check failed: {e}")
        return False

def main():
    """Main service loop"""
    logging.info("=" * 60)
    logging.info("🚀 GA4 DATA SYNC SERVICE STARTING")
    logging.info("=" * 60)
    logging.info(f"   Sync interval: {SYNC_INTERVAL_MINUTES} minutes")
    logging.info(f"   Log file: {os.path.abspath(log_file)}")
    logging.info(f"   Working directory: {os.getcwd()}")
    logging.info("=" * 60)
    
    # Health check ban đầu
    if not check_health():
        logging.error("Initial health check failed! Please check configuration.")
        sys.exit(1)
    
    # Sync ngay lập tức lần đầu
    sync_job()
    
    # Schedule sync định kỳ
    schedule.every(SYNC_INTERVAL_MINUTES).minutes.do(sync_job)
    
    # Schedule health check mỗi giờ
    schedule.every(1).hours.do(check_health)
    
    logging.info(f"✅ Service is running. Press Ctrl+C to stop.")
    
    # Main loop
    try:
        while True:
            schedule.run_pending()
            time.sleep(30)  # Check mỗi 30 giây
    except KeyboardInterrupt:
        logging.info("\n👋 Service stopped by user (Ctrl+C)")
        sys.exit(0)
    except Exception as e:
        logging.error(f"💥 Service crashed: {e}")
        logging.exception("Full error traceback:")
        sys.exit(1)

if __name__ == "__main__":
    # Import thêm cho health check
    from datetime import timedelta
    main()