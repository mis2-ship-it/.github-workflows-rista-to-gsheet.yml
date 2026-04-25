import os
import json
import time
import jwt
import requests
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

print("🚀 Live Script Started")

# ---------------- AUTH ---------------- #

API_KEY = os.environ["API_KEY"]
SECRET_KEY = os.environ["SECRET_KEY"]

def get_token():
    payload = {"iss": API_KEY, "iat": int(time.time())}
    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")

def headers():
    return {
        "x-api-key": API_KEY,
        "x-api-token": get_token(),
        "content-type": "application/json"
    }

# ---------------- GOOGLE ---------------- #

import json
import os
import gspread
from google.oauth2.service_account import Credentials

# 🔐 Load credentials (NO CHANGE)
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

client = gspread.authorize(creds)

# 🔥 CHANGE ONLY THIS LINE ↓
spreadsheet = client.open_by_url(
    "https://docs.google.com/spreadsheets/d/1CVUS-BSBfDIoQI4Yk2GB4_Zp1CIJRF-9YRfpvCih-FM/edit"
)

print("✅ Connected to NEW Google Sheet")

# ---------------- FETCH BRANCH ---------------- #

b_url = "https://api.ristaapps.com/v1/branch/list"
b_resp = requests.get(b_url, headers=headers())

print("Branch API Response:", b_resp.text[:500])  # debug

if b_resp.status_code != 200:
    raise Exception(f"Branch API failed: {b_resp.text}")

data = b_resp.json()

# 🔥 Handle multiple formats safely
if isinstance(data, dict):
    data = data.get("data", [])

if not isinstance(data, list):
    raise Exception("❌ Unexpected branch response format")

branches = [
    b["branchCode"] for b in data
    if isinstance(b, dict) and b.get("status") == "Active"
]

print("🏪 Branch count:", len(branches))

print("Branches:", len(branches))

# ---------------- DATE LOGIC ---------------- #

now = datetime.now()
today = now.strftime("%Y-%m-%d")
last_week = (now - timedelta(days=7)).strftime("%Y-%m-%d")

# ---------------- FETCH SALES ---------------- #

def fetch_sales(day):
    print(f"\n📥 Fetching sales for {day}")
    all_data = []

    for b in branches:
        last_key = None

        while True:
            params = {
                "branch": b,
                "day": day
            }

            if last_key:
                params["lastKey"] = last_key

            r = requests.get(
                "https://api.ristaapps.com/v1/sales/summary",
                headers=headers(),
                params=params,
                timeout=30
            )

            if r.status_code != 200:
                print(f"⚠️ Error {b}: {r.text}")
                break

            js = r.json()
            data = js.get("data", [])

            if not data:
                break

            df = pd.json_normalize(data)
            all_data.append(df)

            last_key = js.get("lastKey")
            if not last_key:
                break

    if not all_data:
        print("❌ No data fetched")
        return pd.DataFrame()

    final_df = pd.concat(all_data, ignore_index=True)

    print("📊 Rows fetched:", final_df.shape)

    return final_df

# ---------------- RUN ---------------- #

today_df = fetch_sales(today)
lastweek_df = fetch_sales(last_week)

if today_df.empty:
    print("❌ No data fetched")
    exit()

# ---------------- TIME FILTER ---------------- #

now_time = datetime.now().time()

today_df["invoiceDate"] = pd.to_datetime(today_df["invoiceDate"])
lastweek_df["invoiceDate"] = pd.to_datetime(lastweek_df["invoiceDate"])

today_df = today_df[today_df["invoiceDate"].dt.time <= now_time]
lastweek_df = lastweek_df[lastweek_df["invoiceDate"].dt.time <= now_time]

# ---------------- KPI ---------------- #

today_sales = today_df["netAmount"].astype(float).sum()
lastweek_sales = lastweek_df["netAmount"].astype(float).sum()

growth = ((today_sales - lastweek_sales) / lastweek_sales * 100) if lastweek_sales else 0

summary = pd.DataFrame({
    "Metric": ["Today Sales", "Last Week Sales", "Growth %"],
    "Value": [today_sales, lastweek_sales, round(growth, 2)]
})

# ---------------- GSHEET ---------------- #

# ---------------- GOOGLE SHEETS SETUP ---------------- #

import gspread
from google.oauth2.service_account import Credentials
import json
import os

# 🔐 Load credentials from GitHub Secret
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

client = gspread.authorize(creds)

# 👉 ADD YOUR GOOGLE SHEET LINK HERE
spreadsheet = client.open_by_url(
    "https://docs.google.com/spreadsheets/d/1CVUS-BSBfDIoQI4Yk2GB4_Zp1CIJRF-9YRfpvCih-FM/edit"
)

print("✅ Connected to Google Sheet")


# ---------------- PUSH FUNCTION ---------------- #

def push(sheet_name, df):
    try:
        print(f"\n📤 Updating sheet: {Sales Dashboard}")

        # Try opening sheet
        try:
            ws = spreadsheet.worksheet(sheet_name)
        except:
            print(f"⚠️ Sheet '{sheet_name}' not found → creating new one")
            ws = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="20")

        # Clean data
        df = df.fillna("").astype(str)

        # Clear old data
        ws.clear()

        # Upload new data
        ws.update(
            [df.columns.tolist()] + df.values.tolist(),
            value_input_option="USER_ENTERED"
        )

        print(f"✅ {sheet_name} updated | Rows: {len(df)}")

    except Exception as e:
        print(f"❌ Error updating {sheet_name}: {e}")


# ---------------- PUSH DATA ---------------- #

print("\n📊 Pushing data to Google Sheets...")

push("Summary", summary)
push("Raw Data", today_df)

print("\n🎉 Google Sheet Update Completed Successfully")
