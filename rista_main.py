import os
import json
import smtplib
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials


# =========================
# CONFIG
# =========================

RISTA_BASE_URL = "https://api.ristaapps.com/v1"
RISTA_API_KEY = os.getenv("API_KEY", "")

RISTA_HEADERS = {
    "Authorization": f"Bearer {RISTA_API_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

GOOGLE_SHEET_ID = "12oI9rtQreA0XI5eTiLZEgc2TVPm9DRgbf2TXTArEpBY"
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

EMAIL_HOST = os.getenv("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_USER = os.getenv("EMAIL_USER", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
EMAIL_TO = os.getenv("EMAIL_TO", "")


TIMEOUT_SECONDS = 60


# =========================
# HELPERS
# =========================

def log(message):
    current_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{current_ts}] {message}")


def parse_json_response(response):
    try:
        return response.json()
    except Exception:
        return {"raw_text": response.text}


def normalize_to_dataframe(data):

    if data is None:
        return pd.DataFrame()

    if isinstance(data, list):
        return pd.json_normalize(data)

    if isinstance(data, dict):

        for key in ["data", "results", "items", "records", "response"]:
            if key in data and isinstance(data[key], list):
                return pd.json_normalize(data[key])

        return pd.json_normalize(data)

    return pd.DataFrame()


def safe_string(value):
    if value is None:
        return ""
    return str(value)


def truncate_sheet_rows(df, max_rows=5000):

    if len(df) > max_rows:
        return df.head(max_rows)

    return df


# =========================
# RISTA API
# =========================

def rista_get(endpoint, params=None):

    url = RISTA_BASE_URL + endpoint

    log(f"Calling {url}")

    response = requests.get(
        url,
        headers=RISTA_HEADERS,
        params=params,
        timeout=TIMEOUT_SECONDS
    )

    response.raise_for_status()

    return parse_json_response(response)


def fetch_all_data():

    endpoint_map = {
        "sales_summary": {
            "endpoint": "/analytics/custom/sales/summary",
            "params": {
                "business_id": BUSINESS_ID,
                "branch_id": BRANCH_ID
            }
        },

        "discount_transactions": {
            "endpoint": "/analytics/discount/transactions",
            "params": {
                "business_id": BUSINESS_ID,
                "branch_id": BRANCH_ID
            }
        },

        "soldout_history": {
            "endpoint": "/items/soldout/history",
            "params": {
                "branch_id": BRANCH_ID,
                "business_day": BUSINESS_DAY
            }
        },

        "inventory_items": {
            "endpoint": "/inventory/items",
            "params": {
                "business_id": BUSINESS_ID
            }
        },

        "outofstock": {
            "endpoint": "/sale/item/outofstock",
            "params": {
                "business_id": BUSINESS_ID,
                "branch_id": BRANCH_ID
            }
        }
    }

    collected = {}

    for name, config in endpoint_map.items():

        try:

            result_json = rista_get(
                config["endpoint"],
                config["params"]
            )

            df = normalize_to_dataframe(result_json)

            df["source_endpoint"] = config["endpoint"]

            df["fetched_at_utc"] = datetime.now(
                timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S")

            collected[name] = {
                "json": result_json,
                "df": df
            }

            log(f"{name} fetched successfully")

        except Exception as e:

            log(f"{name} failed: {e}")

            collected[name] = {
                "json": {"error": str(e)},
                "df": pd.DataFrame([{
                    "error": str(e)
                }])
            }

    return collected


# =========================
# GOOGLE SHEETS
# =========================

def get_gspread_client():

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    credentials_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)

    credentials = Credentials.from_service_account_info(
        credentials_info,
        scopes=scopes
    )

    return gspread.authorize(credentials)


def get_or_create_worksheet(spreadsheet, title):

    try:
        worksheet = spreadsheet.worksheet(title)

    except Exception:
        worksheet = spreadsheet.add_worksheet(
            title=title,
            rows="2000",
            cols="50"
        )

    return worksheet


def upload_dataframe_to_worksheet(spreadsheet, sheet_name, df):

    worksheet = get_or_create_worksheet(spreadsheet, sheet_name)

    df = truncate_sheet_rows(df)

    if df.empty:
        df = pd.DataFrame([{"message": "No data"}])

    df = df.fillna("")

    records = [df.columns.tolist()] + df.astype(str).values.tolist()

    worksheet.clear()

    worksheet.update(
        "A1",
        records,
        value_input_option="RAW"
    )


def update_google_sheets(collected):

    gc = get_gspread_client()

    spreadsheet = gc.open_by_key(GOOGLE_SHEET_ID)

    for sheet_name, payload in collected.items():

        upload_dataframe_to_worksheet(
            spreadsheet,
            sheet_name,
            payload["df"]
        )

    summary_df = build_summary_dataframe(collected)

    upload_dataframe_to_worksheet(
        spreadsheet,
        "dashboard_summary",
        summary_df
    )

    return summary_df


# =========================
# SUMMARY
# =========================

def safe_count(df):

    if df is None or df.empty:
        return 0

    return len(df)


def build_summary_dataframe(collected):

    summary_data = []

    for name, payload in collected.items():

        summary_data.append({
            "dataset": name,
            "rows": safe_count(payload["df"])
        })

    return pd.DataFrame(summary_data)


# =========================
# EMAIL HTML
# =========================

def dataframe_to_html_table(df, max_rows=20):

    if df is None or df.empty:
        return "<p>No Data</p>"

    return df.head(max_rows).to_html(
        index=False,
        border=1
    )


def build_email_html(collected, summary_df):

    html = f"""
    <html>
    <body style="font-family: Arial;">

    <h2>Rista Hourly Report</h2>

    <h3>Summary</h3>

    {dataframe_to_html_table(summary_df)}

    <h3>Sales Summary</h3>

    {dataframe_to_html_table(collected["sales_summary"]["df"])}

    <h3>Discount Transactions</h3>

    {dataframe_to_html_table(collected["discount_transactions"]["df"])}

    <h3>Soldout History</h3>

    {dataframe_to_html_table(collected["soldout_history"]["df"])}

    <h3>Inventory Items</h3>

    {dataframe_to_html_table(collected["inventory_items"]["df"])}

    <h3>Out Of Stock</h3>

    {dataframe_to_html_table(collected["outofstock"]["df"])}

    </body>
    </html>
    """

    return html


# =========================
# EMAIL SEND
# =========================

def send_email(
    subject,
    html_body,
    recipients,
    smtp_host,
    smtp_port,
    smtp_user,
    smtp_password
):

    if isinstance(recipients, str):
        recipients = [
            item.strip()
            for item in recipients.split(",")
            if item.strip()
        ]

    msg = MIMEMultipart("alternative")

    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = ", ".join(recipients)

    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP(smtp_host, smtp_port) as server:

        server.starttls()

        server.login(
            smtp_user,
            smtp_password
        )

        server.sendmail(
            smtp_user,
            recipients,
            msg.as_string()
        )


# =========================
# MAIN
# =========================

def main():

    log("Starting automation")

    all_data = fetch_all_data()

    summary_df = update_google_sheets(all_data)

    html_body = build_email_html(
        all_data,
        summary_df
    )

    send_email(
        subject="Rista Hourly Report",
        html_body=html_body,
        recipients=EMAIL_TO,
        smtp_host=EMAIL_HOST,
        smtp_port=EMAIL_PORT,
        smtp_user=EMAIL_USER,
        smtp_password=EMAIL_PASSWORD
    )

    log("Automation completed successfully")


if __name__ == "__main__":
    main()
