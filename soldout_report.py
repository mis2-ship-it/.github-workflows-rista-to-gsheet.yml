import os
import json
import time
import jwt
import requests
import pandas as pd
import gspread

from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

print("🚀 Soldout Alert Script Started")

# =========================================================
# AUTH
# =========================================================

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

# =========================================================
# GOOGLE AUTH
# =========================================================

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

print("✅ Connected Google Sheet")

# =========================================================
# DATE
# =========================================================

business_day = (
    datetime.now() - timedelta(days=1)
).strftime("%Y-%m-%d")

print("📅 Business Day:", business_day)

# =========================================================
# HELP SHEET
# =========================================================

help_ws = spreadsheet.worksheet("Help Sheet")

help_df = pd.DataFrame(
    help_ws.get_all_records()
)

# =========================================================
# REQUIRED COLUMNS
# =========================================================

required_cols = [
    "branchCode",
    "Store Name",
    "Ownership",
    "AM Email",
    "RM Email",
    "AM Name",
    "CC Mail",
    "Region"
]

for c in required_cols:

    if c not in help_df.columns:
        help_df[c] = ""

# =========================================================
# FILTER ONLY COCO
# =========================================================

help_df = help_df[
    help_df["Ownership"] == "COCO"
]

# =========================================================
# BRANCHES
# =========================================================

branches = help_df["branchCode"].dropna().unique().tolist()

print("🏪 COCO Branch Count:", len(branches))

# =========================================================
# FETCH SOLDOUT DATA
# =========================================================

url = "https://api.ristaapps.com/v1/items/soldout/history"

all_data = []

for branch in branches:

    print(f"Fetching: {branch}")

    params = {
        "branch": branch,
        "day": business_day
    }

    try:

        r = requests.get(
            url,
            headers=headers(),
            params=params,
            timeout=60
        )

        if r.status_code != 200:

            print(f"❌ Failed: {branch}")

            continue

        js = r.json()

        if isinstance(js, dict):
            data = js.get("data", [])
        else:
            data = js

        if not data:
            continue

        df = pd.json_normalize(data)

        all_data.append(df)

    except Exception as e:

        print("❌ Error:", str(e))

# =========================================================
# CONCAT
# =========================================================

if not all_data:

    print("❌ No Soldout Data")
    exit()

final_df = pd.concat(
    all_data,
    ignore_index=True
)

print("✅ Rows:", len(final_df))

# =========================================================
# FILTERS
# =========================================================

if "eventType" in final_df.columns:

    final_df = final_df[
        final_df["eventType"] == "OUT"
    ]

if "statusType" in final_df.columns:

    final_df = final_df[
        final_df["statusType"] == "Direct"
    ]

# =========================================
# CLEAN COLUMN NAMES
# =========================================

help_df.columns = help_df.columns.str.strip()
final_df.columns = final_df.columns.str.strip()

print("HELP SHEET COLUMNS:")
print(help_df.columns.tolist())

print("FINAL DF COLUMNS:")
print(final_df.columns.tolist())

# =========================================
# HELP SHEET
# =========================================

help_ws = spreadsheet.worksheet("Help Sheet")

help_data = help_ws.get_all_records()

help_df = pd.DataFrame(help_data)

# CLEAN HEADERS
help_df.columns = help_df.columns.str.strip()

# REMOVE INVALID BRANCHES
help_df = help_df[
    help_df["branchCode"].notna()
]

help_df = help_df[
    help_df["branchCode"] != "#N/A"
]

# FINAL DF CLEAN
final_df.columns = final_df.columns.str.strip()

print("HELP COLUMNS:")
print(help_df.columns.tolist())

print("FINAL COLUMNS:")
print(final_df.columns.tolist())

# =========================================
# MERGE
# =========================================

final_df = final_df.merge(
    help_df,
    on="branchCode",
    how="left"
)

# =========================================================
# BUSINESS DATE
# =========================================================

if "eventBusinessDay" in final_df.columns:

    final_df["Business Date"] = final_df["eventBusinessDay"]

# =========================================================
# PUSH RAW DATA
# =========================================================

sheet_name = f"Soldout_{business_day}"

try:

    ws = spreadsheet.worksheet(sheet_name)

except:

    ws = spreadsheet.add_worksheet(
        title=sheet_name,
        rows="5000",
        cols="100"
    )

ws.clear()

push_df = final_df.head(1000).fillna("").astype(str)

ws.update(
    [push_df.columns.tolist()] +
    push_df.values.tolist()
)

print("✅ Raw Data Pushed")

# =========================================================
# CATEGORY WISE - MATERIAL
# =========================================================

material_df = final_df[
    final_df["itemType"] == "Material"
]

material_summary = pd.pivot_table(
    material_df,
    index="categoryName",
    columns="Region",
    values="itemName",
    aggfunc="count",
    fill_value=0
).reset_index()

# =========================================================
# CATEGORY WISE - PRODUCT
# =========================================================

product_df = final_df[
    final_df["itemType"] == "Product"
]

product_summary = pd.pivot_table(
    product_df,
    index="categoryName",
    columns="Region",
    values="itemName",
    aggfunc="count",
    fill_value=0
).reset_index()

# =========================================================
# STORE WISE
# =========================================================

store_summary = pd.pivot_table(
    final_df,
    index="Store Name",
    columns="categoryName",
    values="itemName",
    aggfunc="count",
    fill_value=0
).reset_index()

# =========================================================
# PUSH SUMMARIES
# =========================================================

def push_sheet(name, df):

    try:
        ws = spreadsheet.worksheet(name)

    except:
        ws = spreadsheet.add_worksheet(
            title=name,
            rows="3000",
            cols="100"
        )

    ws.clear()

    df = df.fillna("").astype(str)

    ws.update(
        [df.columns.tolist()] +
        df.values.tolist()
    )

push_sheet(
    f"Material_Soldout_{business_day}",
    material_summary
)

push_sheet(
    f"Product_Soldout_{business_day}",
    product_summary
)

push_sheet(
    f"Store_Soldout_{business_day}",
    store_summary
)

print("✅ Summary Sheets Pushed")

# =========================================================
# HTML TABLE
# =========================================================

def to_html(df):

    return df.to_html(
        index=False,
        border=0
    ).replace(
        '<table border="0" class="dataframe">',
        '''
        <table style="
        border-collapse:collapse;
        width:100%;
        font-family:Arial;
        font-size:12px;
        ">
        '''
    )

# =========================================================
# SUMMARY MAIL
# =========================================================

EMAIL_USER = os.environ["EMAIL_USER"]
EMAIL_PASS = os.environ["EMAIL_PASS"]

cc_mails = (
    help_df["CC Mail"]
    .dropna()
    .unique()
    .tolist()
)

summary_html = f"""

<h2>📦 Material Soldout</h2>

{to_html(material_summary)}

<br><br>

<h2>🍔 Product Soldout</h2>

{to_html(product_summary)}

<br><br>

<h2>🏪 Store Wise Soldout</h2>

{to_html(store_summary)}

"""

msg = MIMEMultipart()

msg["From"] = EMAIL_USER
msg["To"] = ",".join(cc_mails)

msg["Subject"] = (
    f"📦 Soldout Summary - {business_day}"
)

msg.attach(
    MIMEText(summary_html, "html")
)

server = smtplib.SMTP(
    "smtp.gmail.com",
    587
)

server.starttls()

server.login(
    EMAIL_USER,
    EMAIL_PASS
)

server.sendmail(
    EMAIL_USER,
    cc_mails,
    msg.as_string()
)

server.quit()

print("✅ Summary Mail Sent")

# =========================================================
# ALERT MAILS
# =========================================================

grouped = final_df.groupby("Store Name")

for store, sdf in grouped:

    try:

        am_mail = sdf["AM Email"].dropna().iloc[0]
        rm_mail = sdf["RM Email"].dropna().iloc[0]
        am_name = sdf["AM Name"].dropna().iloc[0]

        receivers = []

        if am_mail:
            receivers.append(am_mail)

        if rm_mail:
            receivers.append(rm_mail)

        if not receivers:
            continue

        store_table = pd.pivot_table(
            sdf,
            index="Store Name",
            columns="categoryName",
            values="itemName",
            aggfunc="count",
            fill_value=0
        ).reset_index()

        body = f"""

        <p>
        Hi {am_name},
        </p>

        <p>
        Please check and reply what was the reason for soldout immediately.
        </p>

        <br>

        {to_html(store_table)}

        """

        msg = MIMEMultipart()

        msg["From"] = EMAIL_USER

        msg["To"] = ",".join(receivers)

        msg["Subject"] = (
            f"🚨 Soldout Alert - {store}"
        )

        msg.attach(
            MIMEText(body, "html")
        )

        server = smtplib.SMTP(
            "smtp.gmail.com",
            587
        )

        server.starttls()

        server.login(
            EMAIL_USER,
            EMAIL_PASS
        )

        server.sendmail(
            EMAIL_USER,
            receivers,
            msg.as_string()
        )

        server.quit()

        print(f"✅ Alert Sent: {store}")

    except Exception as e:

        print(
            f"❌ Alert Failed {store}:",
            str(e)
        )

print("🎉 SOLDOUT SCRIPT COMPLETED")
