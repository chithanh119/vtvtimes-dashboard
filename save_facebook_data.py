#!/usr/bin/env python3
"""
Lưu dữ liệu Facebook vào PostgreSQL Database
Updated: Loại bỏ hoàn toàn deprecated metrics
"""

import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv
from datetime import datetime
from facebook_api import (
    FacebookAPI, 
    format_insights_for_database, 
    format_posts_for_database
)

load_dotenv()

# Database config
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'ga4_analytics'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

def get_db_connection():
    """Tạo kết nối database"""
    return psycopg2.connect(**DB_CONFIG)

def save_page_insights(insights_data):
    """
    Lưu Page Insights vào database
    Chỉ lưu active metrics, không lưu deprecated metrics:
    - ❌ page_engaged_users
    - ❌ page_views_unique  
    - ❌ page_fan_adds
    - ❌ page_fan_removes
    
    Args:
        insights_data: List of dicts với page insights theo ngày
    """
    if not insights_data:
        print("⚠️  No page insights data to save")
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        for data in insights_data:
            # Chỉ lưu active metrics
            cursor.execute("""
                INSERT INTO facebook_page_insights 
                (insight_date, page_views, page_impressions, 
                 page_impressions_unique, page_post_engagements, 
                 page_posts_impressions, page_reactions, page_video_views)
                VALUES (%(insight_date)s, %(page_views)s, %(page_impressions)s, 
                        %(page_impressions_unique)s, %(page_post_engagements)s,
                        %(page_posts_impressions)s, %(page_reactions)s, %(page_video_views)s)
                ON CONFLICT (insight_date) 
                DO UPDATE SET
                    page_views = EXCLUDED.page_views,
                    page_impressions = EXCLUDED.page_impressions,
                    page_impressions_unique = EXCLUDED.page_impressions_unique,
                    page_post_engagements = EXCLUDED.page_post_engagements,
                    page_posts_impressions = EXCLUDED.page_posts_impressions,
                    page_reactions = EXCLUDED.page_reactions,
                    page_video_views = EXCLUDED.page_video_views,
                    updated_at = CURRENT_TIMESTAMP
            """, {
                'insight_date': data.get('insight_date'),
                'page_views': data.get('page_views', 0),
                'page_impressions': data.get('page_impressions', 0),
                'page_impressions_unique': data.get('page_impressions_unique', 0),
                'page_post_engagements': data.get('page_post_engagements', 0),
                'page_posts_impressions': data.get('page_posts_impressions', 0),
                'page_reactions': data.get('page_reactions', 0),
                'page_video_views': data.get('page_video_views', 0)
            })
        
        conn.commit()
        print(f"✅ Saved {len(insights_data)} days of page insights")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error saving page insights: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def save_page_stats(stats_data):
    """
    Lưu current page stats (fan count, followers) vào database
    Thay thế cho deprecated metrics: page_fan_adds, page_fan_removes
    
    Args:
        stats_data: Dict với fan_count và followers_count
    """
    if not stats_data:
        print("⚠️  No page stats data to save")
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        from datetime import date
        today = date.today()
        
        cursor.execute("""
            INSERT INTO facebook_page_stats (stat_date, fan_count, followers_count)
            VALUES (%s, %s, %s)
            ON CONFLICT (stat_date)
            DO UPDATE SET
                fan_count = EXCLUDED.fan_count,
                followers_count = EXCLUDED.followers_count,
                created_at = CURRENT_TIMESTAMP
        """, (
            today,
            stats_data.get('fan_count', 0),
            stats_data.get('followers_count', 0)
        ))
        
        conn.commit()
        print(f"✅ Saved page stats (Fans: {stats_data.get('fan_count', 0):,}, Followers: {stats_data.get('followers_count', 0):,})")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error saving page stats: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def save_post_insights(posts_data):
    """
    Lưu Post Insights vào database
    
    Args:
        posts_data: List of dicts với post insights
    """
    if not posts_data:
        print("⚠️  No post insights data to save")
        return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        for post in posts_data:
            cursor.execute("""
                INSERT INTO facebook_post_insights 
                (post_id, post_message, post_type, created_time, post_impressions,
                 post_impressions_unique, post_engaged_users, post_reactions,
                 post_comments, post_shares, post_clicks, video_views)
                VALUES (%(post_id)s, %(post_message)s, %(post_type)s, %(created_time)s,
                        %(post_impressions)s, %(post_impressions_unique)s, 
                        %(post_engaged_users)s, %(post_reactions)s,
                        %(post_comments)s, %(post_shares)s, %(post_clicks)s, %(video_views)s)
                ON CONFLICT (post_id)
                DO UPDATE SET
                    post_impressions = EXCLUDED.post_impressions,
                    post_impressions_unique = EXCLUDED.post_impressions_unique,
                    post_engaged_users = EXCLUDED.post_engaged_users,
                    post_reactions = EXCLUDED.post_reactions,
                    post_comments = EXCLUDED.post_comments,
                    post_shares = EXCLUDED.post_shares,
                    post_clicks = EXCLUDED.post_clicks,
                    video_views = EXCLUDED.video_views,
                    updated_at = CURRENT_TIMESTAMP
            """, post)
        
        conn.commit()
        print(f"✅ Saved {len(posts_data)} posts insights")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error saving post insights: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def calculate_and_save_summary_metrics():
    """
    Tính toán và lưu metrics tổng hợp cho dashboard
    Sử dụng active metrics, không dùng deprecated metrics
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Tính metrics từ page insights (chỉ dùng active metrics)
        cursor.execute("""
            INSERT INTO facebook_metrics_summary 
            (metric_date, total_views, total_viewers, total_engagement, engagement_rate)
            SELECT 
                insight_date as metric_date,
                page_views as total_views,
                page_impressions_unique as total_viewers,
                page_post_engagements as total_engagement,
                CASE 
                    WHEN page_impressions > 0 
                    THEN ROUND((page_post_engagements::numeric / page_impressions::numeric) * 100, 2)
                    ELSE 0 
                END as engagement_rate
            FROM facebook_page_insights
            WHERE insight_date >= CURRENT_DATE - INTERVAL '7 days'
            ON CONFLICT (metric_date)
            DO UPDATE SET
                total_views = EXCLUDED.total_views,
                total_viewers = EXCLUDED.total_viewers,
                total_engagement = EXCLUDED.total_engagement,
                engagement_rate = EXCLUDED.engagement_rate,
                created_at = CURRENT_TIMESTAMP
        """)
        
        conn.commit()
        print("✅ Calculated and saved summary metrics")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error calculating summary metrics: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def calculate_fan_growth():
    """
    Tính toán fan growth từ daily fan count
    Thay thế cho deprecated page_fan_adds và page_fan_removes
    
    Returns:
        Dict với fan growth stats
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Lấy fan count hôm nay và hôm qua
        cursor.execute("""
            SELECT 
                stat_date,
                fan_count,
                LAG(fan_count) OVER (ORDER BY stat_date) as previous_fan_count
            FROM facebook_page_stats
            WHERE stat_date >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY stat_date DESC
            LIMIT 1
        """)
        
        result = cursor.fetchone()
        
        if result and result[2]:  # Có data hôm qua
            today_fans = result[1]
            yesterday_fans = result[2]
            fan_growth = today_fans - yesterday_fans
            
            print(f"📊 Fan Growth Analysis:")
            print(f"   Today: {today_fans:,} fans")
            print(f"   Yesterday: {yesterday_fans:,} fans")
            print(f"   Net Change: {fan_growth:+,} fans")
            
            return {
                'today_fans': today_fans,
                'yesterday_fans': yesterday_fans,
                'fan_growth': fan_growth
            }
        else:
            print("⚠️  Not enough data to calculate fan growth (need at least 2 days)")
            return None
            
    except Exception as e:
        print(f"❌ Error calculating fan growth: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

def get_sync_summary():
    """
    Lấy tổng kết sau khi sync
    
    Returns:
        Dict với summary stats
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        summary = {}
        
        # Count records
        cursor.execute("SELECT COUNT(*) FROM facebook_page_insights WHERE insight_date >= CURRENT_DATE - INTERVAL '7 days'")
        summary['page_insights_count'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM facebook_page_stats")
        summary['page_stats_count'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM facebook_post_insights WHERE created_time >= CURRENT_TIMESTAMP - INTERVAL '7 days'")
        summary['posts_count'] = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM facebook_metrics_summary WHERE metric_date >= CURRENT_DATE - INTERVAL '7 days'")
        summary['summary_count'] = cursor.fetchone()[0]
        
        # Get latest metrics
        cursor.execute("""
            SELECT 
                SUM(total_views) as total_views_7d,
                SUM(total_viewers) as total_viewers_7d,
                SUM(total_engagement) as total_engagement_7d,
                ROUND(AVG(engagement_rate), 2) as avg_engagement_rate
            FROM facebook_metrics_summary
            WHERE metric_date >= CURRENT_DATE - INTERVAL '7 days'
        """)
        
        metrics = cursor.fetchone()
        if metrics:
            summary['total_views_7d'] = metrics[0] or 0
            summary['total_viewers_7d'] = metrics[1] or 0
            summary['total_engagement_7d'] = metrics[2] or 0
            summary['avg_engagement_rate'] = metrics[3] or 0
        
        return summary
        
    except Exception as e:
        print(f"❌ Error getting sync summary: {e}")
        return {}
    finally:
        cursor.close()
        conn.close()

def fetch_and_save_all_facebook_data():
    """
    Main function: Fetch tất cả dữ liệu Facebook và lưu vào DB
    Updated: Không query deprecated metrics
    """
    print("=" * 70)
    print("🔄 FETCHING FACEBOOK DATA")
    print("=" * 70)
    print("")
    print("ℹ️  Note: Deprecated metrics excluded from sync:")
    print("   ❌ page_engaged_users")
    print("   ❌ page_views_unique")
    print("   ❌ page_fan_adds")
    print("   ❌ page_fan_removes")
    print("=" * 70)
    
    try:
        # Khởi tạo Facebook API
        fb = FacebookAPI()
        
        # Test connection
        print("\n1. Testing Facebook connection...")
        if not fb.test_connection():
            raise Exception("Facebook connection failed")
        
        # Fetch Current Page Stats (thay thế cho page_fan_adds/removes)
        print("\n2. Fetching current page stats...")
        stats = fb.get_page_current_stats()
        if stats:
            print(f"   📊 Fan Count: {stats.get('fan_count', 0):,}")
            print(f"   📊 Followers: {stats.get('followers_count', 0):,}")
        else:
            print("   ⚠️  Could not fetch page stats")
        
        # Save Page Stats
        print("\n3. Saving page stats to database...")
        save_page_stats(stats)
        
        # Calculate Fan Growth
        print("\n4. Calculating fan growth...")
        fan_growth = calculate_fan_growth()
        
        # Fetch Page Insights (chỉ active metrics)
        print("\n5. Fetching page insights (last 7 days)...")
        print("   ✅ Active metrics only (no deprecated)")
        insights_raw = fb.get_page_summary_metrics()
        insights_formatted = format_insights_for_database(insights_raw)
        print(f"   📊 Fetched {len(insights_formatted)} days of data")
        
        # Save Page Insights
        print("\n6. Saving page insights to database...")
        save_page_insights(insights_formatted)
        
        # Fetch Posts
        print("\n7. Fetching posts with insights...")
        posts_raw = fb.get_posts_with_insights_last_7_days()
        posts_formatted = format_posts_for_database(posts_raw)
        print(f"   📝 Fetched {len(posts_formatted)} posts")
        
        # Save Posts
        print("\n8. Saving post insights to database...")
        save_post_insights(posts_formatted)
        
        # Calculate Summary
        print("\n9. Calculating summary metrics...")
        calculate_and_save_summary_metrics()
        
        # Get Summary
        print("\n10. Getting sync summary...")
        summary = get_sync_summary()
        
        print("\n" + "=" * 70)
        print("✅ FACEBOOK DATA SYNC COMPLETED SUCCESSFULLY")
        print("=" * 70)
        
        if summary:
            print("\n📊 SYNC SUMMARY:")
            print(f"   Database Records:")
            print(f"      • Page Insights (7 days): {summary.get('page_insights_count', 0)}")
            print(f"      • Page Stats: {summary.get('page_stats_count', 0)}")
            print(f"      • Posts (7 days): {summary.get('posts_count', 0)}")
            print(f"      • Summary Metrics: {summary.get('summary_count', 0)}")
            
            print(f"\n   7-Day Totals:")
            print(f"      • Total Views: {summary.get('total_views_7d', 0):,}")
            print(f"      • Total Viewers: {summary.get('total_viewers_7d', 0):,}")
            print(f"      • Total Engagement: {summary.get('total_engagement_7d', 0):,}")
            print(f"      • Avg Engagement Rate: {summary.get('avg_engagement_rate', 0)}%")
        
        print("\n" + "=" * 70)
        
    except Exception as e:
        print("\n" + "=" * 70)
        print("❌ FACEBOOK DATA SYNC FAILED")
        print("=" * 70)
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    fetch_and_save_all_facebook_data()