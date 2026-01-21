import os
import requests
import psycopg2
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
from refresh_facebook_token import FacebookTokenManager

# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_FILE = os.path.join(SCRIPT_DIR, ".env")

# Load environment variables from script directory
load_dotenv(ENV_FILE)

def get_valid_page_token():
    """
    Get a valid page access token, refreshing if necessary
    """
    # First, try to use existing token
    load_dotenv(ENV_FILE, override=True)  # Always reload to get latest values
    current_token = os.getenv("FACEBOOK_PAGE_ACCESS_TOKEN")
    
    if current_token:
        manager = FacebookTokenManager(ENV_FILE)
        token_info = manager.verify_token(current_token)
        
        if token_info and token_info.get("is_valid"):
            # Check expiry
            expires_at = token_info.get("expires_at", 0)
            
            if expires_at == 0:  # Never expires
                print("✓ Using existing valid page token (never expires)")
                return current_token
            
            # Check if expiring soon (within 1 hour)
            seconds_until_expiry = expires_at - datetime.now().timestamp()
            
            if seconds_until_expiry > 3600:  # More than 1 hour
                hours = seconds_until_expiry / 3600
                print(f"✓ Using existing valid page token (expires in {hours:.1f} hours)")
                return current_token
            else:
                print(f"⚠ Page token expires soon ({seconds_until_expiry/60:.0f} minutes) - refreshing...")
    
    # Token invalid or expiring soon - refresh
    print("Refreshing access token...")
    manager = FacebookTokenManager(ENV_FILE)
    if manager.refresh_tokens():
        # Reload env after refresh and get the new token
        load_dotenv(ENV_FILE, override=True)
        new_token = os.getenv("FACEBOOK_PAGE_ACCESS_TOKEN")
        
        print(f"DEBUG: New token from .env: {new_token[:20] if new_token else 'None'}...")
        
        if new_token:
            # Verify the new token works
            token_info = manager.verify_token(new_token)
            if token_info and token_info.get("is_valid"):
                print("✓ Successfully obtained and verified refreshed page token")
                return new_token
            else:
                print(f"✗ Refreshed token is invalid: {token_info}")
                return None
        else:
            print("✗ Could not retrieve new token from .env file")
            return None
    
    print("✗ Token refresh failed")
    return None

def fetch_facebook_insights():
    """
    Fetch Facebook insights and save to database
    """
    # Get valid token
    ACCESS_TOKEN = get_valid_page_token()
    
    if not ACCESS_TOKEN:
        print("✗ Failed to get valid access token")
        return False
    
    # Facebook API config
    PAGE_ID = os.getenv("FACEBOOK_PAGE_ID")
    API_VERSION = os.getenv("FACEBOOK_API_VERSION")
    INSIGHTS_URL = f"https://graph.facebook.com/{API_VERSION}/{PAGE_ID}/insights"
    
    # Your specific metrics
    METRICS = {
        "page_views_total": "Visits",
        "page_media_view": "Views",
        "page_impressions_unique": "Viewers",
        "page_daily_follows": "Follows"
    }
    
    # Calculate date range - get yesterday's data
    today = date.today()
    since = today - timedelta(days=2)
    until = today - timedelta(days=1)
    
    print(f"\nFetching Facebook insights from {since} to {until}")
    
    params = {
        "metric": ",".join(METRICS.keys()),
        "period": "day",
        "since": since.isoformat(),
        "until": until.isoformat(),
        "access_token": ACCESS_TOKEN
    }
    
    try:
        response = requests.get(INSIGHTS_URL, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        if "error" in data:
            print(f"✗ Facebook API Error: {data['error']}")
            
            # If token error, try refreshing once
            if data['error'].get('code') in [190, 102]:
                print("Token error detected - attempting refresh...")
                ACCESS_TOKEN = get_valid_page_token()
                if ACCESS_TOKEN:
                    params['access_token'] = ACCESS_TOKEN
                    response = requests.get(INSIGHTS_URL, params=params)
                    response.raise_for_status()
                    data = response.json()
                else:
                    return False
        
        insights_data = data.get("data", [])
        
        if not insights_data:
            print("No data returned from Facebook API")
            return False
        
        print(f"✓ Fetched {len(insights_data)} metrics from Facebook")
        
        # PostgreSQL connection
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        cursor = conn.cursor()
        
        # Save to PostgreSQL
        records_saved = 0
        for metric_block in insights_data:
            metric_name = metric_block["name"]
            report_type = METRICS.get(metric_name, "Unknown")
            
            for entry in metric_block.get("values", []):
                date_value = datetime.fromisoformat(
                    entry["end_time"].replace("Z", "+00:00")
                ).date()
                value = entry.get("value", 0)
                
                cursor.execute("""
                    INSERT INTO facebook_page_insights_daily
                    (metric, report_type, date, value)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (metric, date)
                    DO UPDATE SET value = EXCLUDED.value
                """, (metric_name, report_type, date_value, value))
                records_saved += 1
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✓ Successfully saved {records_saved} records to database")
        return True
        
    except requests.exceptions.HTTPError as e:
        print(f"✗ HTTP Error: {e}")
        try:
            error_detail = response.json()
            print(f"Error details: {error_detail}")
        except:
            print(f"Response text: {response.text}")
        return False
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("="*60)
    print("FACEBOOK INSIGHTS SYNC")
    print("="*60)
    
    success = fetch_facebook_insights()
    
    if success:
        print("\n✓ Facebook daily insights synced successfully")
    else:
        print("\n✗ Failed to sync Facebook insights")