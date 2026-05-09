# =========================================================
# RISTA LIVE ANALYTICS DASHBOARD
# =========================================================

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
# LOGGER
# =========================================================

def log(message):

    ts = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S UTC")

    print(f"[{ts}] {message}")

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


def fetch_item_sales():

    params = {
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

# =========================================================
# MAIN
# =========================================================

def main():

    log(
        "Starting Dashboard Automation"
    )

    gc = get_gspread_client()

    spreadsheet = gc.open_by_key(
        GOOGLE_SHEET_ID
    )

    # =====================================================
    # FETCH DATA
    # =====================================================

    branch_df = fetch_branch_dashboard()

    item_sales_df = fetch_item_sales()

    discount_df = fetch_discount_dashboard()

    sales_summary_df = fetch_sales_summary()

    soldout_df = fetch_soldout_dashboard()

    inventory_df = fetch_inventory_dashboard()

    # =====================================================
    # CANCELLATION DASHBOARD
    # =====================================================

    cancel_cols = [
        c for c in sales_summary_df.columns
        if "cancel" in c.lower()
    ]

    if cancel_cols:

        cancellation_df = (
            sales_summary_df[
                cancel_cols
            ].copy()
        )

    else:

        cancellation_df = pd.DataFrame()

    # =====================================================
    # SUMMARY DASHBOARD
    # =====================================================

    summary_df = pd.DataFrame([{

        "Run Time UTC":
            datetime.now(
                timezone.utc
            ).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),

        "Total Stores":
            len(branch_df),

        "Item Sales Rows":
            len(item_sales_df),

        "Discount Rows":
            len(discount_df),

        "Sales Summary Rows":
            len(sales_summary_df),

        "Soldout Rows":
            len(soldout_df),

        "Inventory Rows":
            len(inventory_df)

    }])

    # =====================================================
    # UPLOAD TO SHEETS
    # =====================================================

    upload_df(
        spreadsheet,
        "branch_dashboard",
        branch_df
    )

    upload_df(
        spreadsheet,
        "item_sales_dashboard",
        item_sales_df
    )

    upload_df(
        spreadsheet,
        "discount_dashboard",
        discount_df
    )

    upload_df(
        spreadsheet,
        "sales_summary_dashboard",
        sales_summary_df
    )

    upload_df(
        spreadsheet,
        "cancellation_dashboard",
        cancellation_df
    )

    upload_df(
        spreadsheet,
        "soldout_dashboard",
        soldout_df
    )

    upload_df(
        spreadsheet,
        "inventory_dashboard",
        inventory_df
    )

    upload_df(
        spreadsheet,
        "summary_dashboard",
        summary_df
    )

    # =====================================================
    # EMAIL REPORT
    # =====================================================

    html_body = build_email_html(
        summary_df,
        item_sales_df,
        discount_df,
        soldout_df,
        inventory_df
    )

    send_email(
        subject="Rista Live Dashboard Report",
        html_body=html_body
    )

    log(
        "Dashboard Automation Completed Successfully"
    )

# =========================================================

if __name__ == "__main__":

    main()
