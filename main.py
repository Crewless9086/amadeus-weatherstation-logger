import os
import json
from datetime import datetime
from typing import Any, Optional

import requests
import gspread
from google.oauth2.service_account import Credentials
import pytz
from dateutil import parser as dateparser


# ============================
# CONFIG (via Render Env Vars)
# ============================
SHEET_TAB_NAME = "Current_Conditions"

TIMEZONE = os.getenv("TIMEZONE", "Africa/Johannesburg")
TZ = pytz.timezone(TIMEZONE)

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()

# Store the entire service account JSON contents in this env var (as text)
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "").strip()

WCOM_API_KEY = os.getenv("WCOM_API_KEY", "").strip()
STATION_ID = os.getenv("STATION_ID", "").strip()

# Duplicate/too-soon window (seconds). Default 60s.
DUP_WINDOW_SEC = int(os.getenv("DUP_WINDOW_SEC", "60"))

# Weather.com endpoint
URL_CURRENT = (
    "https://api.weather.com/v2/pws/observations/current"
    "?stationId={stationId}&format=json&units=m&apiKey={apiKey}"
)


# ============================
# HELPERS
# ============================
def require_env() -> None:
    missing = []
    for k, v in [
        ("GOOGLE_SHEET_ID", GOOGLE_SHEET_ID),
        ("GOOGLE_SERVICE_ACCOUNT_FILE", GOOGLE_SERVICE_ACCOUNT_FILE),
        ("WCOM_API_KEY", WCOM_API_KEY),
        ("STATION_ID", STATION_ID),
    ]:
        if not v:
            missing.append(k)
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")


def parse_any_datetime(value: Any) -> Optional[datetime]:
    """
    Parses timestamps from Google Sheets or API.
    Returns timezone-aware datetime in Africa/Johannesburg.
    """
    if value is None or value == "":
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return TZ.localize(value)
        return value.astimezone(TZ)

    try:
        dt = dateparser.parse(str(value))
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = TZ.localize(dt)
        else:
            dt = dt.astimezone(TZ)
        return dt
    except Exception:
        return None


def get_gspread_client() -> gspread.Client:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    info = json.loads(GOOGLE_SERVICE_ACCOUNT_FILE)
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def safe_fetch_current() -> tuple[int, Optional[dict], str]:
    """
    Returns (status_code, json_or_none, raw_text)
    Handles 204 and JSON parse safely.
    """
    url = URL_CURRENT.format(stationId=STATION_ID, apiKey=WCOM_API_KEY)
    r = requests.get(url, timeout=30)
    status = r.status_code
    text = r.text or ""

    if status == 204:
        return status, None, text

    if status != 200:
        return status, None, text

    if not text.strip():
        return status, None, text

    try:
        return status, r.json(), text
    except Exception:
        return status, None, text


def get_last_logged_timestamp(ws) -> Optional[datetime]:
    """
    Reads the last non-empty timestamp in column A (excluding header).
    Works even if sheet has blank rows.
    """
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return None

    # Column A index = 0
    for row in reversed(values[1:]):  # skip header
        if row and len(row) > 0 and str(row[0]).strip():
            return parse_any_datetime(row[0])

    return None


# ============================
# MAIN LOGIC
# ============================
def main():
    require_env()

    # 1) Fetch API
    status, data, raw = safe_fetch_current()

    if status == 204:
        print("⚠️ 204 No new data from station (skipped).")
        return

    if status != 200 or not data:
        # Print short chunk so you can debug in Render logs
        print(f"❌ API error. status={status} body={raw[:300]}")
        return

    obs_list = data.get("observations") or []
    if not obs_list:
        print("⚠️ API returned 200 but no observations (skipped).")
        return

    obs = obs_list[0]

    # 2) Convert obsTimeLocal to Africa/Johannesburg and format required string
    obs_time_local = obs.get("obsTimeLocal")
    if not obs_time_local:
        print("⚠️ Missing obsTimeLocal in API response (skipped).")
        return

    obs_ts = parse_any_datetime(obs_time_local)
    if not obs_ts:
        print(f"⚠️ Could not parse obsTimeLocal={obs_time_local} (skipped).")
        return

    # Force TZ explicitly and format exactly as requested
    obs_ts = obs_ts.astimezone(TZ)
    timestamp_str = obs_ts.strftime("%Y-%m-%d %H:%M:%S")

    # 3) Open sheet
    gc = get_gspread_client()
    sh = gc.open_by_key(GOOGLE_SHEET_ID)
    ws = sh.worksheet(SHEET_TAB_NAME)

    # 4) Duplicate/old guard
    last_ts = get_last_logged_timestamp(ws)

    if last_ts is not None:
        # If station/API timestamp is not newer, skip (prevents weird backfill/reconnect issues)
        if obs_ts <= last_ts:
            print(f"⏭️ Skipped: API timestamp not newer. api={timestamp_str} last={last_ts.strftime('%Y-%m-%d %H:%M:%S')}")
            return

        diff_sec = (obs_ts - last_ts).total_seconds()
        if diff_sec < DUP_WINDOW_SEC:
            print(f"⏭️ Skipped: too soon/duplicate (<{DUP_WINDOW_SEC}s). api={timestamp_str}")
            return

    # 5) Map fields (metric is where most values live)
    m = obs.get("metric") or {}

    row = [
        timestamp_str,                 # Timestamp (SA time) YYYY-MM-DD HH:MM:SS
        m.get("temp", ""),            # Temperature (°C)
        m.get("windSpeed", ""),       # Wind Speed (km/h)
        m.get("windGust", ""),        # Wind Gust (km/h)
        obs.get("winddir", ""),       # Wind Direction (°)
        m.get("precipRate", ""),      # Rain Rate (mm/h)
        m.get("precipTotal", ""),     # Total Rain (mm)
        m.get("pressure", ""),        # Pressure (hPa)
        obs.get("humidity", ""),      # Humidity (%)
    ]

    # 6) Append
    ws.append_row(row, value_input_option="USER_ENTERED")
    print(f"✅ Logged Current_Conditions at {timestamp_str}")


if __name__ == "__main__":
    main()
