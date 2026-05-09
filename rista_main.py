# =========================================================
# RISTA LIVE ANALYTICS DASHBOARD
# Endpoint Used:
# /sale/resource
# =========================================================

import os
import json
import time
import jwt
import requests
import pandas as pd
import numpy as np
import gspread

from datetime import datetime, timezone
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

#=========================================================
# LOGGER
#=========================================================

def log(msg):

    ts = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S UTC")

    print(f"[{ts}] {msg}")

#=========================================================
# GSHEET
#=========================================================

def get_gspread_client():

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = Credentials.from_service_account_info(
        json.loads(GOOGLE_SERVICE_ACCOUNT_JSON),
        scopes=scopes
    )

    return gspread.authorize(creds)


def get_sheet(spreadsheet, title):

    try:
        return spreadsheet.worksheet(title)

    except Exception:

        return spreadsheet.add_worksheet(
            title=title,
            rows="5000",
            cols="100"
        )


def upload_df(spreadsheet, sheet_name, df):

    ws = get_sheet(
        spreadsheet,
        sheet_name
    )

    ws.clear()

    if df.empty:
        df = pd.DataFrame([{
            "message": "No Data"
        }])

    df = df.fillna("")

    values = [df.columns.tolist()] + df.astype(str).values.tolist()

    ws.update(
        values=values,
        range_name="A1"
    )

# =========================================================
# FETCH SALES RESOURCE
# =========================================================

def fetch_sales_resource():

    url = BASE_URL + "/sale/resource"

    log(f"Calling {url}")

    response = requests.get(
        url,
        headers=headers(),
        timeout=TIMEOUT
    )

    log(f"Status Code: {response.status_code}")

    response.raise_for_status()

    data = response.json()

    if isinstance(data, dict):

        for k in ["data", "results", "items"]:
            if k in data:
                return pd.json_normalize(data[k])

    if isinstance(data, list):
        return pd.json_normalize(data)

    return pd.DataFrame()

# =========================================================
# ITEM SALES DASHBOARD
# =========================================================

def build_item_sales_dashboard(df):

    possible_item_cols = [
        "itemName",
        "item_name",
        "name"
    ]

    possible_sales_cols = [
        "total",
        "amount",
        "netAmount",
        "sales"
    ]

    item_col = None
    sales_col = None

    for c in possible_item_cols:
        if c in df.columns:
            item_col = c
            break

    for c in possible_sales_cols:
        if c in df.columns:
            sales_col = c
            break

    if not item_col or not sales_col:
        return pd.DataFrame()

    out = (
        df.groupby(item_col)[sales_col]
        .sum()
        .reset_index()
        .sort_values(
            sales_col,
            ascending=False
        )
    )

    return out

# =========================================================
# CANCELLATION DASHBOARD
# =========================================================

def build_cancellation_dashboard(df):

    cancel_cols = [
        c for c in df.columns
        if "cancel" in c.lower()
    ]

    if not cancel_cols:
        return pd.DataFrame()

    frames = []

    for col in cancel_cols:

        temp = (
            df.groupby(col)
            .size()
            .reset_index(name="count")
        )

        temp["metric"] = col

        frames.append(temp)

    return pd.concat(
        frames,
        ignore_index=True
    )

# =========================================================
# HOURLY LIVE DASHBOARD
# =========================================================

def build_hourly_dashboard(df):

    time_cols = [
        "createdDate",
        "createdAt",
        "billDate",
        "invoiceDate"
    ]

    detected = None

    for c in time_cols:
        if c in df.columns:
            detected = c
            break

    if detected is None:
        return pd.DataFrame()

    df[detected] = pd.to_datetime(
        df[detected],
        errors="coerce"
    )

    df["hour"] = df[detected].dt.hour

    amount_col = None

    for c in [
        "total",
        "amount",
        "netAmount"
    ]:
        if c in df.columns:
            amount_col = c
            break

    if amount_col is None:
        return pd.DataFrame()

    out = (
        df.groupby("hour")[amount_col]
        .sum()
        .reset_index()
    )

    return out

# =========================================================
# CHANNEL ANALYTICS
# =========================================================

def build_channel_analytics(df):

    channel_cols = [
        "channel",
        "channelName",
        "source"
    ]

    detected = None

    for c in channel_cols:
        if c in df.columns:
            detected = c
            break

    if detected is None:
        return pd.DataFrame()

    out = (
        df.groupby(detected)
        .size()
        .reset_index(name="orders")
        .sort_values(
            "orders",
            ascending=False
        )
    )

    return out

# =========================================================
# INVENTORY / SOLDOUT ANALYSIS
# =========================================================

def build_inventory_analysis(df):

    soldout_cols = [
        c for c in df.columns
        if "sold" in c.lower()
        or "stock" in c.lower()
    ]

    if not soldout_cols:
        return pd.DataFrame()

    return df[soldout_cols].copy()

# =========================================================
# STORE SLA TRACKING
# =========================================================

def build_sla_tracking(df):

    prep_cols = [
        c for c in df.columns
        if "prep" in c.lower()
        or "sla" in c.lower()
        or "time" in c.lower()
    ]

    if not prep_cols:
        return pd.DataFrame()

    return df[prep_cols].copy()

# =========================================================
# RCA ANALYSIS
# =========================================================

def build_rca_analysis(df):

    rca = []

    if "orderStatus" in df.columns:

        failed = df[
            df["orderStatus"]
            .astype(str)
            .str.contains(
                "cancel",
                case=False,
                na=False
            )
        ]

        rca.append({
            "issue": "Cancelled Orders",
            "count": len(failed)
        })

    if "stockStatus" in df.columns:

        soldout = df[
            df["stockStatus"]
            .astype(str)
            .str.contains(
                "out",
                case=False,
                na=False
            )
        ]

        rca.append({
            "issue": "Out Of Stock",
            "count": len(soldout)
        })

    return pd.DataFrame(rca)

# =========================================================
# SUMMARY
# =========================================================

def build_summary_dataframe(
    raw_df,
    item_sales,
    cancellations,
    hourly,
    channels,
    inventory,
    sla,
    rca
):

    summary = pd.DataFrame([{

        "Run Time UTC":
            datetime.now(
                timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S"),

        "Raw Rows":
            len(raw_df),

        "Item Sales Rows":
            len(item_sales),

        "Cancellation Rows":
            len(cancellations),

        "Hourly Rows":
            len(hourly),

        "Channel Rows":
            len(channels),

        "Inventory Rows":
            len(inventory),

        "SLA Rows":
            len(sla),

        "RCA Rows":
            len(rca)

    }])

    return summary


# =========================================================
# EMAIL HTML
# =========================================================

def dataframe_to_html_table(df, max_rows=20):

    if df is None or df.empty:
        return "<p>No Data Available</p>"

    return df.head(max_rows).to_html(
        index=False,
        border=1
    )


def build_email_html(
    summary_df,
    item_sales,
    cancellations,
    hourly,
    channels,
    inventory,
    sla,
    rca
):

    html = f"""
    <html>

    <body style="font-family: Arial, sans-serif;">

        <h2>Rista Live Dashboard Report</h2>

        <h3>Summary</h3>

        {dataframe_to_html_table(summary_df)}

        <h3>Item Sales Dashboard</h3>

        {dataframe_to_html_table(item_sales)}

        <h3>Cancellation Dashboard</h3>

        {dataframe_to_html_table(cancellations)}

        <h3>Hourly Dashboard</h3>

        {dataframe_to_html_table(hourly)}

        <h3>Channel Analytics</h3>

        {dataframe_to_html_table(channels)}

        <h3>Inventory / Soldout Analysis</h3>

        {dataframe_to_html_table(inventory)}

        <h3>Store SLA Tracking</h3>

        {dataframe_to_html_table(sla)}

        <h3>RCA Analysis</h3>

        {dataframe_to_html_table(rca)}

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

    msg = MIMEMultipart("alternative")

    msg["Subject"] = subject
    msg["From"] = EMAIL_USER
    msg["To"] = ", ".join(recipients)

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

    log("Starting Dashboard Automation")

    gc = get_gspread_client()

    spreadsheet = gc.open_by_key(
        GOOGLE_SHEET_ID
    )

    # =====================================================
    # FETCH RAW DATA
    # =====================================================

    raw_df = fetch_sales_resource()

    # =====================================================
    # BUILD DASHBOARDS
    # =====================================================

    item_sales = build_item_sales_dashboard(
        raw_df
    )

    cancellations = build_cancellation_dashboard(
        raw_df
    )

    hourly = build_hourly_dashboard(
        raw_df
    )

    channels = build_channel_analytics(
        raw_df
    )

    inventory = build_inventory_analysis(
        raw_df
    )

    sla = build_sla_tracking(
        raw_df
    )

    rca = build_rca_analysis(
        raw_df
    )

    summary_df = build_summary_dataframe(
        raw_df,
        item_sales,
        cancellations,
        hourly,
        channels,
        inventory,
        sla,
        rca
    )

    # =====================================================
    # UPLOAD TO GSHEET
    # =====================================================

    upload_df(
        spreadsheet,
        "raw_data",
        raw_df
    )

    upload_df(
        spreadsheet,
        "item_sales_dashboard",
        item_sales
    )

    upload_df(
        spreadsheet,
        "cancellation_dashboard",
        cancellations
    )

    upload_df(
        spreadsheet,
        "hourly_live_dashboard",
        hourly
    )

    upload_df(
        spreadsheet,
        "channel_analytics",
        channels
    )

    upload_df(
        spreadsheet,
        "inventory_soldout_analysis",
        inventory
    )

    upload_df(
        spreadsheet,
        "store_sla_tracking",
        sla
    )

    upload_df(
        spreadsheet,
        "rca_analysis",
        rca
    )

    upload_df(
        spreadsheet,
        "summary_dashboard",
        summary_df
    )

    log("Google Sheet Upload Completed")

    # =====================================================
    # EMAIL REPORT
    # =====================================================

    html_body = build_email_html(
        summary_df,
        item_sales,
        cancellations,
        hourly,
        channels,
        inventory,
        sla,
        rca
    )

    send_email(
        subject="Rista Live Dashboard Report",
        html_body=html_body
    )

    log("Automation Completed Successfully")


# =========================================================
# START
# =========================================================

if __name__ == "__main__":
    main()
