import os
import json
import time
import jwt
import requests
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

print("🚀 Soldout Script Started")

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

    return jwt.encode(
        payload,
        SECRET_KEY,
        algorithm="HS256"
    )

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

spreadsheet = client.open_by_key(
    "19z6KkVBFoLC33_wcNqVhDLyQEC2dDQ8YQE0gE38BhVg"
)

print("✅ Connected to Google Sheet")

# =========================================
# DATE
# =========================================

yesterday = (
    datetime.now() - timedelta(days=1)
).strftime("%Y-%m-%d")

sheet_name = f"Soldout_{yesterday}"

# =========================================
# SHEET
# =========================================

try:

    ws = spreadsheet.worksheet(sheet_name)
    ws.clear()

except:

    ws = spreadsheet.add_worksheet(
        title=sheet_name,
        rows="500",
        cols="100"
    )

# =========================================
# BRANCHES
# =========================================

branch_resp = requests.get(
    "https://api.ristaapps.com/v1/branch/list",
    headers=headers()
)

branch_json = branch_resp.json()

if isinstance(branch_json, dict):
    branch_data = branch_json.get("data", [])
else:
    branch_data = branch_json

branch_df = pd.DataFrame(branch_data)

branch_df = branch_df[
    branch_df["status"] == "Active"
]

branches = branch_df["branchCode"].tolist()

print("🏪 Branch Count:", len(branches))

# =========================================
# FETCH DATA
# =========================================

all_data = []

url = "https://api.ristaapps.com/v1/items/soldout/history"

for branch in branches:

    print(f"Fetching: {branch}")

    params = {
        "branch": branch,
        "day": yesterday
    }

    try:

        response = requests.get(
            url,
            headers=headers(),
            params=params,
            timeout=60
        )

        if response.status_code != 200:

            print(f"❌ Failed: {branch}")

            continue

        js = response.json()

        if isinstance(js, dict):
            data = js.get("data", [])
        else:
            data = js

        if not data:
            continue

        df = pd.json_normalize(data)

        df["branchCode"] = branch

        all_data.append(df)

    except Exception as e:

        print("❌ Error:", str(e))

# =========================================
# FINAL DF
# =========================================

if not all_data:

    print("❌ No data fetched")
    exit()

final_df = pd.concat(
    all_data,
    ignore_index=True
)

final_df = final_df.fillna("").astype(str)

print("✅ Rows:", len(final_df))
print("✅ Columns:", len(final_df.columns))

# =========================================
# PUSH
# =========================================

ws.update(
    [final_df.columns.tolist()] +
    final_df.values.tolist(),
    value_input_option="USER_ENTERED"
)

print("✅ Soldout Data Uploaded")
print(spreadsheet.url)
