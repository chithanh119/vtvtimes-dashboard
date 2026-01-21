#!/usr/bin/env python3
"""
Script để sync dữ liệu lần đầu từ GA4 vào PostgreSQL
Chạy file này trước khi khởi động Flask app
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

print("=" * 60)
print("🚀 GA4 TO POSTGRESQL - INITIAL DATA SYNC")
print("=" * 60)

# Bước 1: Kiểm tra các biến môi trường
print("\n📋 Step 1: Checking environment variables...")

required_vars = [
    'DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD',
    'GA4_PROPERTY_ID', 'GOOGLE_APPLICATION_CREDENTIALS'
]

missing_vars = []
for var in required_vars:
    value = os.getenv(var)
    if not value:
        missing_vars.append(var)
        print(f"   ❌ {var}: MISSING")
    else:
        # Ẩn password khi hiển thị
        if 'PASSWORD' in var:
            print(f"   ✅ {var}: ********")
        else:
            print(f"   ✅ {var}: {value}")

if missing_vars:
    print("\n❌ ERROR: Missing required environment variables!")
    print(f"   Please set: {', '.join(missing_vars)}")
    sys.exit(1)

print("   ✅ All environment variables configured!")

# Bước 2: Kiểm tra kết nối Database
print("\n📋 Step 2: Testing PostgreSQL connection...")

try:
    import psycopg2
    
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    db_version = cursor.fetchone()[0]
    print(f"   ✅ Connected to PostgreSQL!")
    print(f"   📊 Version: {db_version[:50]}...")
    
    # Kiểm tra các bảng cần thiết
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        AND table_name IN ('user_by_location', 'user_by_source', 'views_by_page')
    """)
    existing_tables = [row[0] for row in cursor.fetchall()]
    
    print(f"\n   📊 Existing tables: {len(existing_tables)}/3")
    for table in ['user_by_location', 'user_by_source', 'views_by_page']:
        if table in existing_tables:
            print(f"      ✅ {table}")
        else:
            print(f"      ❌ {table} - MISSING!")
    
    if len(existing_tables) != 3:
        print("\n   ⚠️  WARNING: Some tables are missing!")
        print("   Please run the SQL script to create tables first.")
        response = input("\n   Do you want to continue anyway? (y/n): ")
        if response.lower() != 'y':
            sys.exit(1)
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"   ❌ Database connection failed!")
    print(f"   Error: {e}")
    sys.exit(1)

# Bước 3: Kiểm tra Google Analytics credentials
print("\n📋 Step 3: Testing Google Analytics API connection...")

try:
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import RunRealtimeReportRequest, Dimension, Metric
    
    # Thiết lập credentials
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    
    if not os.path.exists(credentials_path):
        print(f"   ❌ Credentials file not found: {credentials_path}")
        sys.exit(1)
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    
    # Test connection
    client = BetaAnalyticsDataClient()
    property_id = os.getenv('GA4_PROPERTY_ID')
    
    # Thử lấy dữ liệu đơn giản
    request = RunRealtimeReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="city")],
        metrics=[Metric(name="activeUsers")],
        limit=1
    )
    
    response = client.run_realtime_report(request)
    print(f"   ✅ Connected to Google Analytics!")
    print(f"   📊 Property ID: {property_id}")
    print(f"   📊 Test query returned: {len(response.rows)} row(s)")
    
except FileNotFoundError:
    print(f"   ❌ Credentials file not found!")
    print(f"   Path: {credentials_path}")
    sys.exit(1)
except Exception as e:
    print(f"   ❌ Google Analytics API connection failed!")
    print(f"   Error: {e}")
    print("\n   Possible issues:")
    print("   - Invalid credentials file")
    print("   - Wrong GA4_PROPERTY_ID")
    print("   - Service account not granted access in GA4")
    sys.exit(1)

# Bước 4: Fetch dữ liệu từ GA4
print("\n📋 Step 4: Fetching data from Google Analytics...")

try:
    from app import fetch_ga4_realtime_data
    
    print("   ⏳ Fetching realtime data from GA4...")
    ga4_data = fetch_ga4_realtime_data()
    
    # Kiểm tra dữ liệu đã lấy được
    users_5min_count = len(ga4_data['users_5min'].rows)
    users_30min_count = len(ga4_data['users_30min'].rows)
    source_count = len(ga4_data['by_device'].rows)
    page_count = len(ga4_data['by_page'].rows)
    
    print(f"   ✅ Data fetched successfully!")
    print(f"      📍 Location data (5min): {users_5min_count} rows")
    print(f"      📍 Location data (30min): {users_30min_count} rows")
    print(f"      📊 Source data: {source_count} rows")
    print(f"      📄 Page data: {page_count} rows")
    
    if users_5min_count == 0 and users_30min_count == 0:
        print("\n   ⚠️  WARNING: No active users found!")
        print("   This might be normal if your site has no traffic right now.")
    
except Exception as e:
    print(f"   ❌ Failed to fetch data from GA4!")
    print(f"   Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Bước 5: Lưu dữ liệu vào Database
print("\n📋 Step 5: Saving data to PostgreSQL...")

try:
    from app import save_to_database
    
    print("   ⏳ Writing data to database...")
    save_to_database(ga4_data)
    print("   ✅ Data saved successfully!")
    
    # Verify data đã được lưu
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM user_by_location")
    location_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM user_by_source")
    source_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM views_by_page")
    page_count = cursor.fetchone()[0]
    
    print(f"\n   📊 Database verification:")
    print(f"      📍 user_by_location: {location_count} rows")
    print(f"      📊 user_by_source: {source_count} rows")
    print(f"      📄 views_by_page: {page_count} rows")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"   ❌ Failed to save data to database!")
    print(f"   Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Hoàn thành
print("\n" + "=" * 60)
print("✅ INITIAL SYNC COMPLETED SUCCESSFULLY!")
print("=" * 60)
print("\nNext steps:")
print("1. Run Flask app: python app.py")
print("2. Access dashboard: http://localhost:5000")
print("3. Setup auto-sync: python sync_cron.py")
print("\nHappy analyzing! 📊")