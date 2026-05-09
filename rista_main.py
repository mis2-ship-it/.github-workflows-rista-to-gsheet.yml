# =========================================================
# RISTA LIVE ANALYTICS DASHBOARD
# =========================================================
print("SCRIPT STARTED")

import os
import json
import time
import jwt
import requests
import pandas as pd
import gspread
import smtplib

from datetime import datetime, timezone

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from google.oauth2.service_account import Credentials

# =========================================================
# CONFIG
# =========================================================

BASE_URL = "https://api.ristaapps.com/v1"

API_KEY = os.getenv("API_KEY", "")
SECRET_KEY = os.getenv("SECRET_KEY", "")

GOOGLE_SHEET_ID = "12oI9rtQreA0XI5eTiLZEgc2TVPm9DRgbf2TXTArEpBY"

GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv(
    "GOOGLE_SERVICE_ACCOUNT_JSON",
    ""
)

EMAIL_HOST = os.getenv(
    "EMAIL_HOST",
    "smtp.gmail.com"
)

EMAIL_PORT = int(
    os.getenv("EMAIL_PORT", "587")
)

EMAIL_USER = os.getenv(
    "EMAIL_USER",
    ""
)

EMAIL_PASSWORD = os.getenv(
    "EMAIL_PASSWORD",
    ""
)

EMAIL_TO = os.getenv(
    "EMAIL_TO",
    ""
)

TIMEOUT = 60

# =========================================================
# AUTH
# =========================================================

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
        "Content-Type": "application/json",
        "Accept": "application/json"
    }


# =========================================================
# LOGGER
# =========================================================

def log(message):

    ts = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S UTC")

    print(f"[{ts}] {message}")


# =========================================================
# GOOGLE SHEETS
# =========================================================

def get_gspread_client():

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    credentials = Credentials.from_service_account_info(
        json.loads(GOOGLE_SERVICE_ACCOUNT_JSON),
        scopes=scopes
    )

    return gspread.authorize(credentials)


def get_or_create_worksheet(
    spreadsheet,
    title
):

    try:

        return spreadsheet.worksheet(title)

    except Exception:

        return spreadsheet.add_worksheet(
            title=title,
            rows="5000",
            cols="100"
        )


def upload_df(
    spreadsheet,
    sheet_name,
    df
):

    worksheet = get_or_create_worksheet(
        spreadsheet,
        sheet_name
    )

    worksheet.clear()

    if df.empty:

        df = pd.DataFrame([{
            "message": "No Data"
        }])

    df = df.fillna("")

    values = [
        df.columns.tolist()
    ] + df.astype(str).values.tolist()

    worksheet.update(
        values=values,
        range_name="A1"
    )

# =========================================================
# HELP SHEET MAPPING
# =========================================================

def get_help_sheet_mapping(
    spreadsheet
):

    try:

        ws = spreadsheet.worksheet(
            "Help"
        )

    except Exception:

        log("Help sheet not found")

        return pd.DataFrame()

    data = ws.get_all_records()

    df = pd.DataFrame(data)

    log(f"Help Sheet Rows: {len(df)}")

    return df

# =========================================================
# FILTER STORES
# =========================================================

def filter_mapped_branches(
    branch_df,
    help_df
):

    if help_df.empty:
        return branch_df

    possible_cols = [
        "branchCode",
        "Branch_Code",
        "Store Code",
        "storeCode"
    ]

    help_col = None

    for c in possible_cols:

        if c in help_df.columns:

            help_col = c
            break

    if help_col is None:

        log("Branch code column missing in Help sheet")

        return branch_df

    valid_codes = (
        help_df[help_col]
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    out = branch_df[
        branch_df["branchCode"]
        .astype(str)
        .isin(valid_codes)
    ]

    log(f"Mapped Stores: {len(out)}")

    return out    

# =========================================================
# API CALL
# =========================================================

def rista_get(
    endpoint,
    params=None
):

    url = BASE_URL + endpoint

    log(f"Calling {url}")

    response = requests.get(
        url,
        headers=headers(),
        params=params,
        timeout=TIMEOUT
    )

    log(
        f"Status Code: {response.status_code}"
    )

    print(
        response.text[:1000]
    )

    response.raise_for_status()

    return response.json()

# =========================================================
# NORMALIZE
# =========================================================

def normalize_response(data):

    if isinstance(data, dict):

        for key in [
            "data",
            "results",
            "items",
            "records"
        ]:

            if key in data:

                return pd.json_normalize(
                    data[key]
                )

        return pd.json_normalize(data)

    if isinstance(data, list):

        return pd.json_normalize(data)

    return pd.DataFrame()

# =========================================================
# FETCH DASHBOARDS
# =========================================================

def fetch_branch_dashboard():

    data = rista_get(
        "/branch/list"
    )

    return normalize_response(data)
# =========================================================
# ITEM SALES DASHBOARD
# =========================================================

def fetch_item_sales(branch_code):

    params = {

        "branch": branch_code,

        "day": datetime.now().strftime(
            "%Y-%m-%d"
        ),

        "page": 1,

        "pageSize": 500
    }

    data = rista_get(
        "/sales/page",
        params=params
    )

    return normalize_response(data)


def fetch_discount_dashboard():

    data = rista_get(
        "/analytics/discount/transactions"
    )

    return normalize_response(data)


def fetch_sales_summary():

    data = rista_get(
        "/analytics/custom/sales/summary"
    )

    return normalize_response(data)


def fetch_soldout_dashboard():

    data = rista_get(
        "/items/soldout/history"
    )

    if isinstance(data, dict):

        return pd.json_normalize(
            data.get("data", [])
        )

    return pd.DataFrame()


def fetch_inventory_dashboard():

    data = rista_get(
        "/inventory/item/stock"
    )

    if isinstance(data, dict):

        return pd.json_normalize(
            data.get("data", [])
        )

    return pd.DataFrame()

# =========================================================
# EMAIL HTML
# =========================================================

def dataframe_to_html_table(
    df,
    max_rows=20
):

    if df is None or df.empty:

        return "<p>No Data Available</p>"

    return df.head(max_rows).to_html(
        index=False,
        border=1
    )


def build_email_html(
    summary_df,
    item_sales_df,
    discount_df,
    soldout_df,
    inventory_df
):

    html = f"""
    <html>

    <body style="font-family: Arial;">

        <h2>Rista Live Dashboard Report</h2>

        <h3>Summary Dashboard</h3>

        {dataframe_to_html_table(summary_df)}

        <h3>Item Sales Dashboard</h3>

        {dataframe_to_html_table(item_sales_df)}

        <h3>Discount Dashboard</h3>

        {dataframe_to_html_table(discount_df)}

        <h3>Soldout Dashboard</h3>

        {dataframe_to_html_table(soldout_df)}

        <h3>Inventory Dashboard</h3>

        {dataframe_to_html_table(inventory_df)}

    </body>

    </html>
    """

    return html

# =========================================================
# EMAIL SEND
# =========================================================

def send_email(
    subject,
    html_body
):

    if not EMAIL_TO:

        log("EMAIL_TO not configured")

        return

    recipients = [
        x.strip()
        for x in EMAIL_TO.split(",")
        if x.strip()
    ]

    msg = MIMEMultipart(
        "alternative"
    )

    msg["Subject"] = subject
    msg["From"] = EMAIL_USER
    msg["To"] = ", ".join(
        recipients
    )

    msg.attach(
        MIMEText(
            html_body,
            "html"
        )
    )

    with smtplib.SMTP(
        EMAIL_HOST,
        EMAIL_PORT
    ) as server:

        server.starttls()

        server.login(
            EMAIL_USER,
            EMAIL_PASSWORD
        )

        server.sendmail(
            EMAIL_USER,
            recipients,
            msg.as_string()
        )

    log("Email Sent Successfully")


# Main   

def main():

    print("MAIN STARTED")

    log("Starting Dashboard Automation")

    gc = get_gspread_client()

    spreadsheet = gc.open_by_key(
        GOOGLE_SHEET_ID
    )

    help_df = get_help_sheet_mapping(
        spreadsheet
    )

    branch_df = fetch_branch_list()

    mapped_branch_df = filter_mapped_branches(
        branch_df,
        help_df
    )

    print("BRANCHES FETCHED")
    # =====================================
    # FETCH SALES DATA
    # =====================================

    all_item_sales = []

    for _, row in mapped_branch_df.iterrows():

        branch_code = row["branchCode"]

        branch_name = row["branchName"]

        log(f"Processing {branch_name}")

        item_sales_df = fetch_item_sales(
            branch_code
        )

        item_sales_df["branchName"] = branch_name

        all_item_sales.append(
            item_sales_df
        )

    # =====================================
    # FINAL ITEM SALES
    # =====================================

    final_item_sales_df = pd.concat(
        all_item_sales,
        ignore_index=True
    ) if all_item_sales else pd.DataFrame()

    # =====================================
    # OTHER DASHBOARDS
    # =====================================

    discount_df = fetch_discount_dashboard()

    cancellation_df = build_cancellation_dashboard(
        final_item_sales_df
    )

    hourly_df = build_hourly_dashboard(
        final_item_sales_df
    )

    channel_df = build_channel_analytics(
        final_item_sales_df
    )

    inventory_df = build_inventory_analysis(
        final_item_sales_df
    )

    sla_df = build_sla_tracking(
        final_item_sales_df
    )

    rca_df = build_rca_analysis(
        final_item_sales_df
    )

    # =====================================
    # UPLOAD TO GSHEET
    # =====================================

    upload_df(
        spreadsheet,
        "item_sales_dashboard",
        final_item_sales_df
    )

    upload_df(
        spreadsheet,
        "discount_dashboard",
        discount_df
    )

    upload_df(
        spreadsheet,
        "cancellation_dashboard",
        cancellation_df
    )

    upload_df(
        spreadsheet,
        "hourly_live_dashboard",
        hourly_df
    )

    upload_df(
        spreadsheet,
        "channel_analytics",
        channel_df
    )

    upload_df(
        spreadsheet,
        "inventory_soldout_analysis",
        inventory_df
    )

    upload_df(
        spreadsheet,
        "store_sla_tracking",
        sla_df
    )

    upload_df(
        spreadsheet,
        "rca_analysis",
        rca_df
    )

    log("Automation Completed Successfully")

# =========================================================

if __name__ == "__main__":

    print("CALLING MAIN")

    main()
