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

creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])

creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

client = gspread.authorize(creds)

spreadsheet = client.open_by_url(
    "https://docs.google.com/spreadsheets/d/1CVUS-BSBfDIoQI4Yk2GB4_Zp1CIJRF-9YRfpvCih-FM/edit"
)

print("✅ Connected to Google Sheet")

# ---------------- FETCH BRANCH ---------------- #

b_resp = requests.get("https://api.ristaapps.com/v1/branch/list", headers=headers())

data = b_resp.json()
if isinstance(data, dict):
    data = data.get("data", [])

branches = [
    b["branchCode"] for b in data
    if isinstance(b, dict) and b.get("status") == "Active"
]

print("🏪 Branch count:", len(branches))

# ---------------- DATE ---------------- #

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
            params = {"branch": b, "day": day}

            if last_key:
                params["lastKey"] = last_key

            r = requests.get(
                "https://api.ristaapps.com/v1/sales/summary",
                headers=headers(),
                params=params
            )

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
        return pd.DataFrame()

    final_df = pd.concat(all_data, ignore_index=True)
    print("📊 Rows:", final_df.shape)

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

# ---------------- PUSH ---------------- #

def push(sheet_name, df):
    print(f"\n📤 Updating sheet: {sheet_name}")

    try:
        ws = spreadsheet.worksheet(sheet_name)
    except:
        ws = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="20")

    df = df.fillna("").astype(str)

    ws.clear()
    ws.update([df.columns.tolist()] + df.values.tolist())

    print(f"✅ {sheet_name} updated")

print("\n📊 Pushing data...")

push("Summary", summary)
push("Raw Data", today_df)

print("\n🎉 SUCCESS")
