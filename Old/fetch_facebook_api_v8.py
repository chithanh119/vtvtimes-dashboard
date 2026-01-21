import os
import requests
import psycopg2
from datetime import datetime, timedelta, date
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Facebook API config
PAGE_ID = os.getenv("FACEBOOK_PAGE_ID")
ACCESS_TOKEN = os.getenv("FACEBOOK_PAGE_ACCESS_TOKEN")
API_VERSION = os.getenv("FACEBOOK_API_VERSION", "v21.0")

print("="*60)
print("FACEBOOK API TOKEN DIAGNOSTICS")
print("="*60)

# Step 1: Debug the token
DEBUG_URL = f"https://graph.facebook.com/{API_VERSION}/debug_token"
params = {
    "input_token": ACCESS_TOKEN,
    "access_token": ACCESS_TOKEN
}

print("\n1. Checking token validity...")
try:
    print(f"\n   Request URL: {DEBUG_URL}")
    print(f"   Params: {params}")
    response = requests.get(DEBUG_URL, params=params)
    print(f"   Response Status: {response.status_code}")
    print(f"   Response: {response.text}")
    debug_data = response.json()
    
    if "data" in debug_data:
        token_info = debug_data["data"]
        print(f"   Token Type: {token_info.get('type', 'Unknown')}")
        print(f"   App ID: {token_info.get('app_id', 'Unknown')}")
        print(f"   Valid: {token_info.get('is_valid', False)}")
        print(f"   Expires: {token_info.get('expires_at', 'Never')}")
        
        if "scopes" in token_info:
            print(f"   Permissions: {', '.join(token_info['scopes'])}")
    else:
        print(f"   Error: {debug_data}")
except Exception as e:
    print(f"   Could not debug token: {e}")

# Step 2: Try to get Page Access Token from User Token
print("\n2. Attempting to get Page Access Token...")
ACCOUNTS_URL = f"https://graph.facebook.com/{API_VERSION}/me/accounts"
params = {
    "access_token": ACCESS_TOKEN
}

try:
    print(f"\n   Request URL: {ACCOUNTS_URL}")
    print(f"   Params: {params}")
    response = requests.get(ACCOUNTS_URL, params=params)
    print(f"   Response Status: {response.status_code}")
    print(f"   Response: {response.text}")
    accounts_data = response.json()
    
    if "data" in accounts_data:
        pages = accounts_data["data"]
        print(f"   Found {len(pages)} page(s) accessible with this token")
        
        # Find the matching page
        page_token = None
        for page in pages:
            print(f"   - {page['name']} (ID: {page['id']})")
            if page['id'] == PAGE_ID:
                page_token = page['access_token']
                print(f"     ✓ This is your target page!")
                print(f"     ✓ Page Access Token obtained")
                
                # Save to .env suggestion
                print(f"\n   IMPORTANT: Update your .env file with:")
                print(f"   FACEBOOK_PAGE_ACCESS_TOKEN={page_token}")
        
        if not page_token:
            print(f"\n   ✗ Page ID {PAGE_ID} not found in accessible pages")
            print(f"   Make sure you have admin access to this page")
            exit(1)
            
        # Use the page token for the rest of the script
        ACCESS_TOKEN = page_token
        
    else:
        print(f"   Error: {accounts_data}")
        print("\n   Your token might be a Page Token already, but it's invalid or expired")
        print("   Please generate a new Page Access Token from:")
        print("   https://developers.facebook.com/tools/explorer/")
        exit(1)
        
except Exception as e:
    print(f"   Error: {e}")
    exit(1)

print("\n" + "="*60)
print("FETCHING INSIGHTS DATA")
print("="*60)

INSIGHTS_URL = f"https://graph.facebook.com/{API_VERSION}/{PAGE_ID}/insights"

# Your specific metrics
METRICS = {
    "page_views_total": "Visits",
    "page_media_view": "Views",
    "page_impressions_unique": "Viewers",
    "page_daily_follows": "Follows"
}

# Use recent past dates (hardcoded to avoid 2026 issue)
since = date(2026, 1, 18)
until = date(2026, 1, 19)

print(f"\nFetching data from {since} to {until}")

params = {
    "metric": ",".join(METRICS.keys()),
    "period": "day",
    "since": since.isoformat(),
    "until": until.isoformat(),
    "access_token": ACCESS_TOKEN
}

try:
    print(f"\nRequest URL: {INSIGHTS_URL}")
    print(f"Params: {params}")
    response = requests.get(INSIGHTS_URL, params=params)
    print(f"Response Status: {response.status_code}")
    print(f"Response: {response.text[:500]}...")  # Print first 500 chars
    response.raise_for_status()
    
    data = response.json()
    
    if "error" in data:
        print(f"\n✗ Facebook API Error: {data['error']}")
        exit(1)
    
    insights_data = data.get("data", [])
    
    if not insights_data:
        print("No data returned from Facebook API")
        exit(1)
    
    print(f"✓ Successfully fetched {len(insights_data)} metrics from Facebook")
    
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
        
        print(f"\nProcessing {metric_name} ({report_type}):")
        
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
            print(f"  {date_value}: {value}")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"\n{'='*60}")
    print(f"✓ SUCCESS: Saved {records_saved} records to database")
    print(f"{'='*60}")
    
except requests.exceptions.HTTPError as e:
    print(f"\n✗ HTTP Error: {e}")
    try:
        error_detail = response.json()
        print(f"Error details: {error_detail}")
    except:
        print(f"Response text: {response.text}")
    exit(1)
    
except Exception as e:
    print(f"\n✗ Error: {e}")
    import traceback
    traceback.print_exc()
    exit(1)