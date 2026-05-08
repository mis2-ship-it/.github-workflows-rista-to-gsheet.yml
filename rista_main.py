import os
import json
import smtplib
import traceback
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

RISTA_BASE_URL = os.getenv("https://api.ristaapps.com/v1", "").rstrip("/")
RISTA_API_KEY = os.getenv("API_KEY", "")

# If Rista uses a different auth header, change this section only
RISTA_HEADERS = {
    "Authorization": "Bearer " + RISTA_API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json"
}

GOOGLE_SHEET_ID = os.getenv("12oI9rtQreA0XI5eTiLZEgc2TVPm9DRgbf2TXTArEpBY", "")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")

EMAIL_HOST = os.getenv("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_USER = os.getenv("EMAIL_USER", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
EMAIL_TO = os.getenv("EMAIL_TO", "")

# Optional params
BUSINESS_ID = os.getenv("BUSINESS_ID", "")
BRANCH_ID = os.getenv("BRANCH_ID", "")
BUSINESS_DAY = os.getenv("BUSINESS_DAY", "")

TIMEOUT_SECONDS = 60


# =========================
# HELPERS
# =========================

def log(message):
    current_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print("[" + current_ts + "] " + str(message))


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
        # Common API patterns
        for candidate_key in ["data", "results", "items", "records", "response"]:
            if candidate_key in data and isinstance(data[candidate_key], list):
                return pd.json_normalize(data[candidate_key])

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
    log("Calling " + url)
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
        cleaned_params = {}
        for key, value in config["params"].items():
            if safe_string(value).strip() != "":
                cleaned_params[key] = value

        try:
            result_json = rista_get(config["endpoint"], cleaned_params)
            df = normalize_to_dataframe(result_json)
            df["source_endpoint"] = config["endpoint"]
            df["fetched_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            collected[name] = {
                "json": result_json,
                "df": df
            }
            log(name + " fetched successfully with " + str(len(df)) + " rows")
        except Exception as fetch_error:
            log("Failed for " + name + ": " + str(fetch_error))
            collected[name] = {
                "json": {"error": str(fetch_error)},
                "df": pd.DataFrame([{
                    "error": str(fetch_error),
                    "source_endpoint": config["endpoint"],
                    "fetched_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
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
    credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
    return gspread.authorize(credentials)


def get_or_create_worksheet(spreadsheet, title, rows=2000, cols=50):
    try:
        worksheet = spreadsheet.worksheet(title)
    except Exception:
        worksheet = spreadsheet.add_worksheet(title=title, rows=str(rows), cols=str(cols))
    return worksheet


def upload_dataframe_to_worksheet(spreadsheet, sheet_name, df):
    worksheet = get_or_create_worksheet(spreadsheet, sheet_name)

    df_to_write = truncate_sheet_rows(df.copy(), max_rows=5000)

    if df_to_write.empty:
        df_to_write = pd.DataFrame([{"message": "No data returned"}])

    df_to_write.columns = [str(col)[:100] for col in df_to_write.columns]
    df_to_write = df_to_write.fillna("")

    records = [df_to_write.columns.tolist()] + df_to_write.astype(str).values.tolist()

    worksheet.clear()
    worksheet.update("A1", records, value_input_option="RAW")


def update_google_sheets(collected):
    gc_client = get_gspread_client()
    spreadsheet = gc_client.open_by_key(GOOGLE_SHEET_ID)

    for sheet_name, payload in collected.items():
        upload_dataframe_to_worksheet(spreadsheet, sheet_name, payload["df"])

    summary_df = build_summary_dataframe(collected)
    upload_dataframe_to_worksheet(spreadsheet, "dashboard_summary", summary_df)

    return summary_df


# =========================
# SUMMARY BUILDING
# =========================

def safe_count(df):
    if df is None or df.empty:
        return 0
    if "error" in df.columns and len(df) == 1:
        return 0
    return len(df)


def extract_inventory_metrics(inventory_df):
    metrics = {
        "inventory_total_items": safe_count(inventory_df),
        "inventory_zero_stock_items": 0,
        "inventory_low_stock_items": 0
    }

    if inventory_df is None or inventory_df.empty:
        return metrics

    possible_stock_cols = ["stock", "quantity", "available_stock", "current_stock", "item_stock"]
    detected_stock_col = None

    for col_name in possible_stock_cols:
        if col_name in inventory_df.columns:
            detected_stock_col = col_name
            break

    if detected_stock_col is not None:
        stock_series = pd.to_numeric(inventory_df[detected_stock_col], errors="coerce")
        metrics["inventory_zero_stock_items"] = int((stock_series <= 0).fillna(False).sum())
        metrics["inventory_low_stock_items"] = int(((stock_series > 0) & (stock_series <= 5)).fillna(False).sum())

    return metrics


def extract_availability_metrics(outofstock_df, soldout_df):
    metrics = {
        "outofstock_records": safe_count(outofstock_df),
        "soldout_history_records": safe_count(soldout_df),
        "estimated_available_items": ""
    }
    return metrics


def extract_sales_metrics(sales_df, discount_df):
    metrics = {
        "sales_summary_rows": safe_count(sales_df),
        "discount_transaction_rows": safe_count(discount_df),
        "estimated_cancellations": 0
    }

    combined_candidates = []
    if sales_df is not None and not sales_df.empty:
        combined_candidates.append(sales_df)
    if discount_df is not None and not discount_df.empty:
        combined_candidates.append(discount_df)

    for candidate_df in combined_candidates:
        for col_name in candidate_df.columns:
            col_lower = str(col_name).lower()
            if "cancel" in col_lower:
                cancel_series = pd.to_numeric(candidate_df[col_name], errors="coerce")
                metrics["estimated_cancellations"] = int(cancel_series.fillna(0).sum())
                return metrics

    return metrics


def derive_restaurant_status(collected):
    has_any_success = False

    for payload in collected.values():
        df = payload["df"]
        if not df.empty and "error" not in df.columns:
            has_any_success = True
            break

    return "Online" if has_any_success else "Offline"


def build_summary_dataframe(collected):
    sales_df = collected["sales_summary"]["df"]
    discount_df = collected["discount_transactions"]["df"]
    soldout_df = collected["soldout_history"]["df"]
    inventory_df = collected["inventory_items"]["df"]
    outofstock_df = collected["outofstock"]["df"]

    inventory_metrics = extract_inventory_metrics(inventory_df)
    availability_metrics = extract_availability_metrics(outofstock_df, soldout_df)
    sales_metrics = extract_sales_metrics(sales_df, discount_df)
    restaurant_status = derive_restaurant_status(collected)

    summary_data = [
        {"metric": "run_time_utc", "value": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")},
        {"metric": "restaurant_status", "value": restaurant_status},
        {"metric": "inventory_total_items", "value": inventory_metrics["inventory_total_items"]},
        {"metric": "inventory_zero_stock_items", "value": inventory_metrics["inventory_zero_stock_items"]},
        {"metric": "inventory_low_stock_items", "value": inventory_metrics["inventory_low_stock_items"]},
        {"metric": "outofstock_records", "value": availability_metrics["outofstock_records"]},
        {"metric": "soldout_history_records", "value": availability_metrics["soldout_history_records"]},
        {"metric": "sales_summary_rows", "value": sales_metrics["sales_summary_rows"]},
        {"metric": "discount_transaction_rows", "value": sales_metrics["discount_transaction_rows"]},
        {"metric": "estimated_cancellations", "value": sales_metrics["estimated_cancellations"]}
    ]

    return pd.DataFrame(summary_data)


# =========================
# EMAIL
# =========================

def build_email_html(collected, summary_df):

    sales_df = collected["sales_summary"]["df"]
    discount_df = collected["discount_transactions"]["df"]
    soldout_df = collected["soldout_history"]["df"]
    inventory_df = collected["inventory_items"]["df"]
    outofstock_df = collected["outofstock"]["df"]

    summary_map = dict(zip(summary_df["metric"], summary_df["value"]))

    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif;">

        <h2>Rista Hourly Report</h2>

        <p><b>Run Time UTC:</b> {safe_string(summary_map.get("run_time_utc"))}</p>

        <p><b>Restaurant Status:</b> {safe_string(summary_map.get("restaurant_status"))}</p>

        <h3>Key Metrics</h3>

        <table border="1" cellpadding="5" cellspacing="0">
            <tr>
                <th>Metric</th>
                <th>Value</th>
            </tr>

            <tr>
                <td>Total Inventory Items</td>
                <td>{safe_string(summary_map.get("inventory_total_items"))}</td>
            </tr>

            <tr>
                <td>Zero Stock Items</td>
                <td>{safe_string(summary_map.get("inventory_zero_stock_items"))}</td>
            </tr>

            <tr>
                <td>Low Stock Items</td>
                <td>{safe_string(summary_map.get("inventory_low_stock_items"))}</td>
            </tr>

            <tr>
                <td>Out of Stock Records</td>
                <td>{safe_string(summary_map.get("outofstock_records"))}</td>
            </tr>

            <tr>
                <td>Sold Out History Records</td>
                <td>{safe_string(summary_map.get("soldout_history_records"))}</td>
            </tr>

            <tr>
                <td>Discount Transactions</td>
                <td>{safe_string(summary_map.get("discount_transaction_rows"))}</td>
            </tr>

            <tr>
                <td>Estimated Cancellations</td>
                <td>{safe_string(summary_map.get("estimated_cancellations"))}</td>
            </tr>

        </table>

        <h3>Inventory Snapshot</h3>
        {dataframe_to_html_table(inventory_df, 20)}

        <h3>Sold Out History</h3>
        {dataframe_to_html_table(soldout_df, 20)}

        <h3>Out of Stock / Partial Acceptance</h3>
        {dataframe_to_html_table(outofstock_df, 20)}

        <h3>Discount Transactions</h3>
        {dataframe_to_html_table(discount_df, 20)}

        <h3>Sales Summary</h3>
        {dataframe_to_html_table(sales_df, 20)}

        <p style="margin-top:20px;">
            This is an automated hourly email from your Rista reporting workflow.
        </p>

    </body>
    </html>
    """

            return html

# =========================
# EMAIL SEND
# =========================

def send_email(subject, html_body, recipients, smtp_host, smtp_port, smtp_user, smtp_password):
    if isinstance(recipients, str):
        recipients = [item.strip() for item in recipients.split(",") if item.strip()]

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = ", ".join(recipients)

    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, recipients, msg.as_string())


# =========================
# MAIN
# =========================

def main():
    log("Starting Rista hourly automation")

    all_data = fetch_all_data()

    update_google_sheets(all_data)

    html_body = build_html_email(all_data)

    send_email(
        subject="Rista Hourly Report",
        html_body=html_body,
        recipients=EMAIL_TO,
        smtp_host=SMTP_HOST,
        smtp_port=SMTP_PORT,
        smtp_user=SMTP_USER,
        smtp_password=SMTP_PASSWORD
    )

    log("Rista hourly automation completed successfully")


if __name__ == "__main__":
    main()
