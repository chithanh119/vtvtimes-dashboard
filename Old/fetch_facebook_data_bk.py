import os
import requests
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Facebook API config
PAGE_ID = os.getenv("FACEBOOK_PAGE_ID")
ACCESS_TOKEN = os.getenv("FACEBOOK_PAGE_ACCESS_TOKEN")
API_VERSION = os.getenv("FACEBOOK_API_VERSION")

INSIGHTS_URL = f"https://graph.facebook.com/{API_VERSION}/{PAGE_ID}/insights"

# Metric mapping
METRICS = {
    "page_media_view": "Views",
    "page_impressions_unique": "Viewers",
    "page_views_total": "Visits",
    "page_daily_follows": "Follows"
}

# PostgreSQL connection
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)
cursor = conn.cursor()

# Fetch insights
params = {
    "metric": ",".join(METRICS.keys()),
    "period": "day",
    "access_token": ACCESS_TOKEN
}

response = requests.get(INSIGHTS_URL, params=params)
response.raise_for_status()

data = response.json().get("data", [])

# Save to PostgreSQL
for metric_block in data:
    metric_name = metric_block["name"]
    report_type = METRICS.get(metric_name, "Unknown")

    for entry in metric_block.get("values", []):
        date = datetime.fromisoformat(
            entry["end_time"].replace("Z", "")
        ).date()
        value = entry.get("value", 0)

        cursor.execute("""
            INSERT INTO facebook_page_insights_daily
            (metric, report_type, date, value)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (metric, date)
            DO UPDATE SET value = EXCLUDED.value
        """, (metric_name, report_type, date, value))

conn.commit()
cursor.close()
conn.close()

print("Facebook daily insights synced successfully.")
