#!/usr/bin/env python3
"""
Facebook Graph API Data Fetcher
Lấy dữ liệu từ Facebook Fanpage Insights
Updated: Chỉ sử dụng active metrics (no deprecated)
"""

import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json

load_dotenv()

class FacebookAPI:
    def __init__(self):
        self.access_token = os.getenv('FACEBOOK_PAGE_ACCESS_TOKEN')
        self.page_id = os.getenv('FACEBOOK_PAGE_ID')
        self.base_url = 'https://graph.facebook.com/v24.0'
        
        if not self.access_token or not self.page_id:
            raise ValueError("Missing FACEBOOK_PAGE_ACCESS_TOKEN or FACEBOOK_PAGE_ID in .env")
    
    def _make_request(self, endpoint, params=None):
        """Helper để gọi Facebook Graph API"""
        url = f"{self.base_url}/{endpoint}"
        
        if params is None:
            params = {}
        
        params['access_token'] = self.access_token
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error calling Facebook API: {e}")
            if hasattr(e.response, 'text'):
                print(f"Response: {e.response.text}")
            raise
    
    def get_page_insights(self, metrics, since_days=7):
        """
        Lấy Page Insights trong N ngày gần nhất
        
        Args:
            metrics: List các metric cần lấy
            since_days: Số ngày trở lại từ hôm nay
        
        Returns:
            Dict chứa dữ liệu insights
        """
        since_date = (datetime.now() - timedelta(days=since_days)).strftime('%Y-%m-%d')
        until_date = datetime.now().strftime('%Y-%m-%d')
        
        endpoint = f"{self.page_id}/insights"
        params = {
            'metric': ','.join(metrics),
            'since': since_date,
            'until': until_date,
            'period': 'day'
        }
        
        data = self._make_request(endpoint, params)
        return data
    
    def get_page_posts(self, limit=100):
        """
        Lấy danh sách posts gần đây
        
        Args:
            limit: Số lượng posts tối đa
        
        Returns:
            List các posts
        """
        endpoint = f"{self.page_id}/posts"
        params = {
            'fields': 'id,message,created_time,type,permalink_url',
            'limit': limit
        }
        
        data = self._make_request(endpoint, params)
        return data.get('data', [])
    
    def get_post_insights(self, post_id, metrics):
        """
        Lấy insights cho một post cụ thể
        
        Args:
            post_id: ID của post
            metrics: List các metric cần lấy
        
        Returns:
            Dict chứa insights của post
        """
        endpoint = f"{post_id}/insights"
        params = {
            'metric': ','.join(metrics)
        }
        
        try:
            data = self._make_request(endpoint, params)
            return data
        except Exception as e:
            print(f"Error getting insights for post {post_id}: {e}")
            return {'data': []}
    
    def get_posts_with_insights_last_7_days(self):
        """
        Lấy tất cả posts trong 7 ngày gần nhất kèm insights
        
        Returns:
            List các posts với insights đầy đủ
        """
        # Lấy posts
        since_date = int((datetime.now() - timedelta(days=7)).timestamp())
        
        endpoint = f"{self.page_id}/posts"
        params = {
            'fields': 'id,message,created_time,type,permalink_url',
            'since': since_date,
            'limit': 100
        }
        
        posts_data = self._make_request(endpoint, params)
        posts = posts_data.get('data', [])
        
        # Lấy insights cho từng post
        post_metrics = [
            'post_impressions',
            'post_impressions_unique',
            'post_engaged_users',
            'post_reactions_by_type_total',
            'post_clicks'
        ]
        
        posts_with_insights = []
        
        for post in posts:
            post_id = post['id']
            
            # Lấy insights
            insights = self.get_post_insights(post_id, post_metrics)
            
            # Lấy engagement metrics từ post object
            engagement_endpoint = post_id
            engagement_params = {
                'fields': 'shares,comments.summary(true),reactions.summary(true)'
            }
            
            try:
                engagement_data = self._make_request(engagement_endpoint, engagement_params)
            except:
                engagement_data = {}
            
            # Kết hợp dữ liệu
            post_info = {
                'id': post_id,
                'message': post.get('message', ''),
                'type': post.get('type', ''),
                'created_time': post.get('created_time', ''),
                'permalink_url': post.get('permalink_url', ''),
                'insights': insights.get('data', []),
                'shares': engagement_data.get('shares', {}).get('count', 0),
                'comments': engagement_data.get('comments', {}).get('summary', {}).get('total_count', 0),
                'reactions': engagement_data.get('reactions', {}).get('summary', {}).get('total_count', 0)
            }
            
            posts_with_insights.append(post_info)
        
        return posts_with_insights
    
    def get_page_summary_metrics(self):
        """
        Lấy metrics tổng quan của page trong 7 ngày
        Chỉ sử dụng ACTIVE metrics (không deprecated)
        
        Returns:
            Dict chứa các metrics tổng hợp
        """
        # Chỉ các metrics còn active (Updated 2024)
        page_metrics = [
            'page_views_total',
            'page_impressions',
            'page_impressions_unique',
            'page_post_engagements',
            'page_posts_impressions',
            'page_actions_post_reactions_total',
            'page_video_views'
        ]
        
        insights = self.get_page_insights(page_metrics, since_days=7)
        
        return insights
    
    def get_page_current_stats(self):
        """
        Lấy thống kê hiện tại của page (không phụ thuộc insights API)
        Thay thế cho deprecated page_fan_adds, page_fan_removes
        
        Returns:
            Dict chứa stats hiện tại
        """
        try:
            endpoint = self.page_id
            params = {
                'fields': 'fan_count,followers_count,name,about,category'
            }
            data = self._make_request(endpoint, params)
            return data
        except Exception as e:
            print(f"Error getting page stats: {e}")
            return {}
    
    def test_connection(self):
        """
        Test kết nối với Facebook API
        
        Returns:
            Bool - True nếu kết nối thành công
        """
        try:
            endpoint = self.page_id
            params = {'fields': 'name,fan_count,followers_count'}
            data = self._make_request(endpoint, params)
            
            print(f"✅ Connected to Facebook Page: {data.get('name')}")
            print(f"   Fans: {data.get('fan_count', 0):,}")
            print(f"   Followers: {data.get('followers_count', 0):,}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to Facebook API: {e}")
            return False


def format_insights_for_database(insights_data):
    """
    Format dữ liệu insights từ Facebook API sang format cho database
    Chỉ xử lý ACTIVE metrics
    
    Args:
        insights_data: Raw data từ Facebook API
    
    Returns:
        List of dicts sẵn sàng insert vào database
    """
    formatted_data = []
    
    if not insights_data or 'data' not in insights_data:
        return formatted_data
    
    # Tổ chức data theo ngày
    data_by_date = {}
    
    for metric in insights_data['data']:
        metric_name = metric['name']
        
        for value_item in metric.get('values', []):
            date = value_item.get('end_time', '').split('T')[0]
            value = value_item.get('value', 0)
            
            if date not in data_by_date:
                data_by_date[date] = {'insight_date': date}
            
            # Map metric names (chỉ active metrics)
            if metric_name == 'page_views_total':
                data_by_date[date]['page_views'] = value
            elif metric_name == 'page_impressions':
                data_by_date[date]['page_impressions'] = value
            elif metric_name == 'page_impressions_unique':
                data_by_date[date]['page_impressions_unique'] = value
            elif metric_name == 'page_post_engagements':
                data_by_date[date]['page_post_engagements'] = value
            elif metric_name == 'page_posts_impressions':
                data_by_date[date]['page_posts_impressions'] = value
            elif metric_name == 'page_actions_post_reactions_total':
                data_by_date[date]['page_reactions'] = value
            elif metric_name == 'page_video_views':
                data_by_date[date]['page_video_views'] = value
    
    formatted_data = list(data_by_date.values())
    return formatted_data


def format_posts_for_database(posts_data):
    """
    Format posts data cho database
    
    Args:
        posts_data: List posts với insights
    
    Returns:
        List of dicts sẵn sàng insert vào database
    """
    formatted_posts = []
    
    for post in posts_data:
        # Parse insights
        insights_dict = {}
        for insight in post.get('insights', []):
            metric_name = insight['name']
            
            # Lấy value (có thể là number hoặc dict)
            values = insight.get('values', [])
            if values:
                value = values[0].get('value', 0)
                
                # Xử lý reactions (là dict)
                if metric_name == 'post_reactions_by_type_total':
                    if isinstance(value, dict):
                        insights_dict['post_reactions'] = sum(value.values())
                    else:
                        insights_dict['post_reactions'] = 0
                else:
                    insights_dict[metric_name] = value
        
        formatted_post = {
            'post_id': post['id'],
            'post_message': post.get('message', '')[:500],  # Limit length
            'post_type': post.get('type', ''),
            'created_time': post.get('created_time', ''),
            'post_impressions': insights_dict.get('post_impressions', 0),
            'post_impressions_unique': insights_dict.get('post_impressions_unique', 0),
            'post_engaged_users': insights_dict.get('post_engaged_users', 0),
            'post_reactions': insights_dict.get('post_reactions', 0),
            'post_comments': post.get('comments', 0),
            'post_shares': post.get('shares', 0),
            'post_clicks': insights_dict.get('post_clicks', 0),
            'video_views': 0  # Cần query riêng cho video
        }
        
        formatted_posts.append(formatted_post)
    
    return formatted_posts


# Test script
if __name__ == "__main__":
    print("=" * 60)
    print("FACEBOOK API TEST")
    print("=" * 60)
    
    try:
        fb = FacebookAPI()
        
        # Test connection
        print("\n1. Testing connection...")
        if fb.test_connection():
            
            # Get current page stats
            print("\n2. Fetching current page stats...")
            stats = fb.get_page_current_stats()
            if stats:
                print(f"   Page Name: {stats.get('name')}")
                print(f"   Category: {stats.get('category')}")
                print(f"   Total Fans: {stats.get('fan_count', 0):,}")
                print(f"   Followers: {stats.get('followers_count', 0):,}")
            
            # Get page insights
            print("\n3. Fetching page insights (last 7 days)...")
            insights = fb.get_page_summary_metrics()
            formatted = format_insights_for_database(insights)
            print(f"   ✅ Got {len(formatted)} days of data")
            
            # Get posts
            print("\n4. Fetching posts with insights...")
            posts = fb.get_posts_with_insights_last_7_days()
            formatted_posts = format_posts_for_database(posts)
            print(f"   ✅ Got {len(formatted_posts)} posts")
            
            # Display sample
            if formatted:
                print("\n5. Sample insights data (latest day):")
                print(json.dumps(formatted[-1], indent=2))
            
            if formatted_posts:
                print("\n6. Sample post data:")
                sample = formatted_posts[0].copy()
                if 'post_message' in sample and len(sample['post_message']) > 100:
                    sample['post_message'] = sample['post_message'][:100] + '...'
                print(json.dumps(sample, indent=2, default=str))
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()