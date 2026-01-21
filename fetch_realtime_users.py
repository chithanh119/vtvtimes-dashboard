from datetime import datetime, timezone

from google.analytics.data_v1beta.types import RunRealtimeReportRequest

from config import GA4_PROPERTY_ID
from ga_client import get_ga_client
from db import get_connection


def fetch_realtime_active_users():
    client = get_ga_client()

    request_active_users_5m = RunRealtimeReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        metrics=[
            {"name": "activeUsers"}
        ],
        minute_ranges=[{"name": "0-5 minutes ago", "start_minutes_ago": 5}]
    )

    request_active_users_30m = RunRealtimeReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        metrics=[
            {"name": "activeUsers"}
        ],
        minute_ranges=[{"name": "0-29 minutes ago", "start_minutes_ago": 29}]
    )


    response_5m = client.run_realtime_report(
        request=request_active_users_5m,
        timeout=30
    )

    response_30m = client.run_realtime_report(
        request=request_active_users_30m,
        timeout=30
    )

    active_users_5m = 0
    if response_5m.rows:
        active_users_5m = int(response_5m.rows[0].metric_values[0].value)

    active_users_30m = 0
    if response_30m.rows:
        active_users_30m = int(response_30m.rows[0].metric_values[0].value)

    return active_users_5m, active_users_30m

def save_to_db(active_users_5m: int, active_users_30m: int):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE realtime_active_users")
    cur.execute("""
         INSERT INTO realtime_active_users (active_users_5m, recorded_at, active_users_30m)
         VALUES (%s, %s, %s)
     """, (
         active_users_5m,
         datetime.now(timezone.utc),
         active_users_30m
     ))

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    try:
        active_users_5m, active_users_30m = fetch_realtime_active_users()
        save_to_db(active_users_5m, active_users_30m)
        print(f"[OK] Realtime active users: {active_users_5m}, {active_users_30m}")
    except Exception as e:
        print(f"[ERROR] {e}")
