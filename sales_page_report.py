import os
import json
import time
import jwt
import requests
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

print("🚀 Script Started")

# =========================================
# AUTH
# =========================================

API_KEY = os.environ["API_KEY"]
SECRET_KEY = os.environ["SECRET_KEY"]


def get_token():
    payload = {
        "iss": API_KEY,
        "iat": int(time.time())
    }

    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")


def headers():
    return {
        "x-api-key": API_KEY,
        "x-api-token": get_token(),
        "content-type": "application/json"
    }

# =========================================
# GOOGLE AUTH
# =========================================

creds = Credentials.from_service_account_info(
    json.loads(os.environ["GOOGLE_CREDENTIALS"]),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

client = gspread.authorize(creds)

# ---------------- GOOGLE SHEET ---------------- #

spreadsheet = client.open_by_key(
    "19z6KkVBFoLC33_wcNqVhDLyQEC2dDQ8YQE0gE38BhVg"
)

print("✅ Connected to Google Sheet")

# ---------------- WORKSHEET ---------------- #

try:
    ws = spreadsheet.worksheet(yesterday)
    ws.clear()

except:
    ws = spreadsheet.add_worksheet(
        title=yesterday,
        rows="50000",
        cols="200"
    )

print(f"✅ Worksheet Ready: {yesterday}")

# =========================================
# FETCH ACTIVE BRANCHES
# =========================================

b_url = "https://api.ristaapps.com/v1/branch/list"

b_resp = requests.get(b_url, headers=headers())

b_json = b_resp.json()

if isinstance(b_json, dict):
    b_data = b_json.get("data", [])
else:
    b_data = b_json

branch_df = pd.DataFrame(b_data)

branch_df = branch_df[branch_df["status"] == "Active"]

branches = branch_df["branchCode"].tolist()

print("🏪 Active Branches:", len(branches))

# =========================================
# DATE
# =========================================

business_day = datetime.now() - timedelta(days=1)

yesterday = business_day.strftime("%Y-%m-%d")

print("📅 Fetching Date:", yesterday)

# =========================================
# FETCH SALES DATA
# =========================================

s_url = "https://api.ristaapps.com/v1/sales/page"

sales_data = []

for branch in branches:

    print(f"Fetching: {branch}")

    last_key = None

    while True:

        params = {
            "branch": branch,
            "day": yesterday
        }

        if last_key:
            params["lastKey"] = last_key

        try:

            response = requests.get(
                s_url,
                headers=headers(),
                params=params,
                timeout=60
            )

            if response.status_code != 200:
                print(f"❌ Failed {branch}")
                break

            js = response.json()

            data = js.get("data", [])

            if not data:
                break

            df = pd.json_normalize(data)

            sales_data.append(df)

            last_key = js.get("lastKey")

            if not last_key:
                break

        except Exception as e:
            print("❌ Error:", str(e))
            break

# =========================================
# CONCAT RAW DATA
# =========================================

if not sales_data:
    print("❌ No data fetched")
    exit()

sales_df = pd.concat(sales_data, ignore_index=True)

print("✅ Raw Rows:", len(sales_df))
print("✅ Raw Columns:", len(sales_df.columns))

# =========================================
# EXPLODE ITEMS
# =========================================

if "items" in sales_df.columns:

    exploded_df = sales_df.explode("items")

    item_df = pd.json_normalize(
        exploded_df["items"]
    ).add_prefix("item_")

    exploded_df = exploded_df.drop(columns=["items"])

    final_df = pd.concat(
        [
            exploded_df.reset_index(drop=True),
            item_df.reset_index(drop=True)
        ],
        axis=1
    )

else:

    final_df = sales_df.copy()

print("✅ Final Rows:", len(final_df))
print("✅ Final Columns:", len(final_df.columns))

# =========================================
# CLEAN
# =========================================

final_df = final_df.fillna("")

final_df = final_df.astype(str)

# =========================================
# PUSH TO GOOGLE SHEET
# =========================================

worksheet.update(
    [final_df.columns.tolist()] + final_df.values.tolist(),
    value_input_option="USER_ENTERED"
)

print("✅ Data Uploaded Successfully")
print("📄 Final Sheet URL:")
print(spreadsheet.url)

