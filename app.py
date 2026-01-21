from flask import Flask, render_template, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunRealtimeReportRequest,
    Dimension,
    Metric
)
from datetime import datetime
import json

# Load biến môi trường
load_dotenv()

app = Flask(__name__)

# Cấu hình Database
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'ga4_analytics'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

# Cấu hình GA4
GA4_PROPERTY_ID = os.getenv('GA4_PROPERTY_ID')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

# Mapping tọa độ các tỉnh/thành Việt Nam
VIETNAM_CITIES = {
    'Hanoi': {'lat': 21.0285, 'lng': 105.8542, 'name': 'Hà Nội'},
    'Ho Chi Minh City': {'lat': 10.8231, 'lng': 106.6297, 'name': 'TP. Hồ Chí Minh'},
    'Da Nang': {'lat': 16.0544, 'lng': 108.2022, 'name': 'Đà Nẵng'},
    'Hai Phong': {'lat': 20.8449, 'lng': 106.6881, 'name': 'Hải Phòng'},
    'Can Tho': {'lat': 10.0452, 'lng': 105.7469, 'name': 'Cần Thơ'},
    'Bien Hoa': {'lat': 10.9470, 'lng': 106.8196, 'name': 'Biên Hòa'},
    'Hue': {'lat': 16.4637, 'lng': 107.5909, 'name': 'Huế'},
    'Nha Trang': {'lat': 12.2388, 'lng': 109.1967, 'name': 'Nha Trang'},
    'Buon Ma Thuot': {'lat': 12.6675, 'lng': 108.0378, 'name': 'Buôn Ma Thuột'},
    'Quy Nhon': {'lat': 13.7830, 'lng': 109.2192, 'name': 'Quy Nhơn'},
    'Vung Tau': {'lat': 10.3460, 'lng': 107.0843, 'name': 'Vũng Tàu'},
    'Thai Nguyen': {'lat': 21.5670, 'lng': 105.8252, 'name': 'Thái Nguyên'},
    'Nam Dinh': {'lat': 20.4388, 'lng': 106.1621, 'name': 'Nam Định'},
    'Vinh': {'lat': 18.6796, 'lng': 105.6813, 'name': 'Vinh'},
}

def get_db_connection():
    """Tạo kết nối tới PostgreSQL"""
    return psycopg2.connect(**DB_CONFIG)

def fetch_ga4_realtime_data():
    """Lấy dữ liệu realtime từ GA4"""
    client = BetaAnalyticsDataClient()
    
    # Request cho Active Users by Location (5 phút)
    request_5min = RunRealtimeReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[Dimension(name="city")],
        metrics=[Metric(name="activeUsers")],
        minute_ranges=[{"name": "0-5 minutes ago", "start_minutes_ago": 5}]
    )
    
    # Request cho Active Users by Location (29 phút - GA4 Standard limit)
    request_30min = RunRealtimeReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[Dimension(name="city")],
        metrics=[Metric(name="activeUsers")],
        minute_ranges=[{"name": "0-29 minutes ago", "start_minutes_ago": 29}]
    )
    
    # Request cho Active Users by Device Category (thay cho Source)
    # Realtime API không hỗ trợ firstUserSource
    request_device = RunRealtimeReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[Dimension(name="deviceCategory")],
        metrics=[Metric(name="activeUsers")]
    )
    
    # Request cho Views by Page - sử dụng unifiedScreenName cho Realtime
    request_page = RunRealtimeReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        dimensions=[Dimension(name="unifiedScreenName")],
        metrics=[Metric(name="screenPageViews")]
    )
    
    # Thực hiện các request
    response_5min = client.run_realtime_report(request_5min)
    response_30min = client.run_realtime_report(request_30min)
    response_device = client.run_realtime_report(request_device)
    response_page = client.run_realtime_report(request_page)
    
    return {
        'users_5min': response_5min,
        'users_30min': response_30min,
        'by_device': response_device,  # Đổi từ by_source sang by_device
        'by_page': response_page
    }

def save_to_database(ga4_data):
    """Lưu dữ liệu GA4 vào PostgreSQL"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Xóa dữ liệu cũ
        cursor.execute("TRUNCATE user_by_location, user_by_source, views_by_page")
        
        # Lưu dữ liệu location
        users_5min_dict = {}
        for row in ga4_data['users_5min'].rows:
            city = row.dimension_values[0].value
            users = int(row.metric_values[0].value)
            users_5min_dict[city] = users
        
        users_30min_dict = {}
        for row in ga4_data['users_30min'].rows:
            city = row.dimension_values[0].value
            users = int(row.metric_values[0].value)
            users_30min_dict[city] = users
        
        # Kết hợp và insert vào DB
        all_cities = set(users_5min_dict.keys()) | set(users_30min_dict.keys())
        
        for city in all_cities:
            if city in VIETNAM_CITIES:
                city_info = VIETNAM_CITIES[city]
                cursor.execute("""
                    INSERT INTO user_by_location 
                    (province, city, latitude, longitude, active_users_5min, active_users_30min)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    city_info['name'], 
                    city, 
                    city_info['lat'], 
                    city_info['lng'],
                    users_5min_dict.get(city, 0),
                    users_30min_dict.get(city, 0)
                ))
        
        # Lưu dữ liệu device (thay cho source)
        for row in ga4_data['by_device'].rows:
            device = row.dimension_values[0].value
            users = int(row.metric_values[0].value)
            cursor.execute("""
                INSERT INTO user_by_source (source, active_users)
                VALUES (%s, %s)
            """, (device, users))
        
        # Lưu dữ liệu page views
        for row in ga4_data['by_page'].rows:
            page_name = row.dimension_values[0].value
            views = int(row.metric_values[0].value)
            cursor.execute("""
                INSERT INTO views_by_page (page_title, screen_name, views)
                VALUES (%s, %s, %s)
            """, (page_name, page_name, views))
        
        conn.commit()
        print("✓ Data saved successfully!")
        
    except Exception as e:
        conn.rollback()
        print(f"✗ Error saving data: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

# Routes
@app.route('/')
def index():
    """Trang chủ dashboard"""
    return render_template('index.html')

@app.route('/api/map-data')
def get_map_data():
    """API trả về dữ liệu cho bản đồ"""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    cursor.execute("""
        SELECT province, city, latitude, longitude, 
               active_users_5min, active_users_30min
        FROM user_by_location
        ORDER BY active_users_30min DESC
    """)
    
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    
    return jsonify(data)

@app.route('/api/active-users')
def get_active_users():
    """API trả về tổng số active users"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT active_users_5m FROM realtime_active_users ORDER BY recorded_at DESC LIMIT 1")
    users_5min = cursor.fetchone()[0] or 0
    
    cursor.execute("SELECT active_users_30m FROM realtime_active_users ORDER BY recorded_at DESC LIMIT 1")
    users_30min = cursor.fetchone()[0] or 0
    
    cursor.close()
    conn.close()
    
    return jsonify({
        'users_5min': users_5min,
        'users_30min': users_30min
    })

@app.route('/api/users-by-source')
def get_users_by_source():
    """API trả về users theo device/source"""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    cursor.execute("""
        SELECT source, active_users
        FROM user_by_source
        ORDER BY active_users DESC
        LIMIT 10
    """)
    
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    
    return jsonify(data)

@app.route('/api/views-by-page')
def get_views_by_page():
    """API trả về views theo page"""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    cursor.execute("""
        SELECT page_title, screen_name, views
        FROM views_by_page
        ORDER BY views DESC
        LIMIT 10
    """)
    
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    
    return jsonify(data)

@app.route('/api/refresh')
def refresh_data():
    """API để refresh dữ liệu từ GA4"""
    try:
        ga4_data = fetch_ga4_realtime_data()
        save_to_database(ga4_data)
        return jsonify({'status': 'success', 'message': 'Data refreshed successfully'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

# Facebook API Endpoints
@app.route('/api/facebook/daily_insights')
def get_facebook_insights():
    """API trả về tổng hợp metrics Facebook 7 ngày"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT 
               value
            FROM facebook_page_insights_daily
            WHERE date >= CURRENT_DATE - INTERVAL '1 days'
            AND report_type = 'Views'
        """)
        fb_views = cursor.fetchone()[0] or 0

        cursor.execute("""
            SELECT 
               value
            FROM facebook_page_insights_daily
            WHERE date >= CURRENT_DATE - INTERVAL '1 days'
            AND report_type = 'Viewers'
        """)
        fb_viewers = cursor.fetchone()[0] or 0
        
        cursor.execute("""
            SELECT 
               value
            FROM facebook_page_insights_daily
            WHERE date >= CURRENT_DATE - INTERVAL '1 days'
            AND report_type = 'Visits'
        """)
        fb_visits = cursor.fetchone()[0] or 0

        cursor.execute("""
            SELECT 
               value
            FROM facebook_page_insights_daily
            WHERE date >= CURRENT_DATE - INTERVAL '1 days'
            AND report_type = 'Follows'
        """)
        fb_follows = cursor.fetchone()[0] or 0
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'fb_views': fb_views,
            'fb_viewers': fb_viewers,
            'fb_visits': fb_visits,
            'fb_follows': fb_follows
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/facebook/refresh')
def refresh_facebook_data():
    """API để refresh dữ liệu Facebook"""
    try:
        from save_facebook_data import fetch_and_save_all_facebook_data
        fetch_and_save_all_facebook_data()
        return jsonify({'status': 'success', 'message': 'Facebook data refreshed successfully'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)