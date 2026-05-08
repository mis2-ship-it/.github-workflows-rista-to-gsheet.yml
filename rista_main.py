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


# =========================================================
# CONFIG
# =========================================================

RISTA_BASE_URL = "https://api.ristaapps.com/v1"

RISTA_API_KEY = os.getenv("API_KEY", "")

RISTA_HEADERS = {
    "api_key": RISTA_API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json"
}

GOOGLE_SHEET_ID = "12oI9rtQreA0XI5eTiLZEgc2TVPm9DRgbf2TXTArEpBY"

GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv(
    "GOOGLE_SERVICE_ACCOUNT_JSON",
    ""
)

EMAIL_HOST = os.getenv("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.getenv("EMAIL_PORT", "587"))
EMAIL_USER = os.getenv("EMAIL_USER", "")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
EMAIL_TO = os.getenv("EMAIL_TO", "")

TIMEOUT_SECONDS = 60


# =========================================================
# HELPERS
# =========================================================

def log(message):

    current_ts = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S UTC")

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

        if "data" in data:
            return pd.json_normalize(data["data"])

        return pd.json_normalize(data)

    return pd.DataFrame()


def truncate_sheet_rows(df, max_rows=5000):

    if len(df) > max_rows:
        return df.head(max_rows)

    return df


# =========================================================
# RISTA API
# =========================================================

def rista_get(endpoint, params=None):

    url = RISTA_BASE_URL + endpoint

    log(f"Calling {url}")

    response = requests.get(
        url,
        headers=RISTA_HEADERS,
        params=params,
        timeout=TIMEOUT_SECONDS
    )

    log(f"Status Code: {response.status_code}")

    response.raise_for_status()

    return parse_json_response(response)


# =========================================================
# GOOGLE SHEETS
# =========================================================

def get_gspread_client():

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    credentials_info = json.loads(
        GOOGLE_SERVICE_ACCOUNT_JSON
    )

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


def upload_dataframe_to_worksheet(
    spreadsheet,
    sheet_name,
    df
):

    worksheet = get_or_create_worksheet(
        spreadsheet,
        sheet_name
    )

    df = truncate_sheet_rows(df)

    if df.empty:
        df = pd.DataFrame([{
            "message": "No Data"
        }])

    df = df.fillna("")

    records = [
        df.columns.tolist()
    ] + df.astype(str).values.tolist()

    worksheet.clear()

    worksheet.update(
        values=records,
        range_name="A1",
        value_input_option="RAW"
    )


# =========================================================
# HELP SHEET STORE MAPPING
# =========================================================

def get_help_sheet_mapping(spreadsheet):

    worksheet = spreadsheet.worksheet("Help")

    data = worksheet.get_all_records()

    help_df = pd.DataFrame(data)

    log(f"Help Sheet Rows: {len(help_df)}")

    return help_df


# =========================================================
# FETCH BRANCHES
# =========================================================

def fetch_branch_list():

    result = rista_get("/branch/list")

    df = normalize_to_dataframe(result)

    return df


# =========================================================
# FILTER BRANCHES
# =========================================================

def filter_mapped_branches(
    branch_df,
    help_df
):

    if help_df.empty:
        return branch_df

    help_stores = help_df["Store_Name"].astype(str).str.strip()

    branch_df["branchName"] = (
        branch_df["branchName"]
        .astype(str)
        .str.strip()
    )

    filtered_df = branch_df[
        branch_df["branchName"].isin(help_stores)
    ]

    log(f"Mapped Stores: {len(filtered_df)}")

    return filtered_df


# =========================================================
# SOLDOUT HISTORY
# =========================================================

def fetch_soldout_history(branch_name):

    try:

        result = rista_get(
            "/items/soldout/history"
        )

        df = normalize_to_dataframe(result)

        df["branchName"] = branch_name

        return df

    except Exception as e:

        log(f"Soldout failed for {branch_name}: {e}")

        return pd.DataFrame()


# =========================================================
# INVENTORY STOCK
# =========================================================

def fetch_inventory_stock(branch_name):

    try:

        result = rista_get(
            "/inventory/item/stock"
        )

        df = normalize_to_dataframe(result)

        df["branchName"] = branch_name

        return df

    except Exception as e:

        log(f"Inventory failed for {branch_name}: {e}")

        return pd.DataFrame()


# =========================================================
# SUMMARY
# =========================================================

def build_summary_dataframe(
    branch_df,
    soldout_df,
    inventory_df
):

    summary = pd.DataFrame([{

        "Run Time UTC":
            datetime.now(
                timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S"),

        "Total Stores":
            len(branch_df),

        "Soldout Records":
            len(soldout_df),

        "Inventory Records":
            len(inventory_df)

    }])

    return summary


# =========================================================
# EMAIL HTML
# =========================================================

def dataframe_to_html_table(df, max_rows=20):

    if df.empty:
        return "<p>No Data</p>"

    return df.head(max_rows).to_html(
        index=False,
        border=1
    )


def build_email_html(
    summary_df,
    soldout_df,
    inventory_df
):

    html = f"""
    <html>

    <body style="font-family: Arial;">

    <h2>Rista Hourly Automation</h2>

    <h3>Summary</h3>

    {dataframe_to_html_table(summary_df)}

    <h3>Soldout History</h3>

    {dataframe_to_html_table(soldout_df)}

    <h3>Inventory Stock</h3>

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
        MIMEText(html_body, "html")
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


# =========================================================
# MAIN
# =========================================================

def main():

    log("Starting automation")

    gc = get_gspread_client()

    spreadsheet = gc.open_by_key(
        GOOGLE_SHEET_ID
    )

    # =====================================================
    # HELP SHEET
    # =====================================================

    help_df = get_help_sheet_mapping(
        spreadsheet
    )

    # =====================================================
    # BRANCH LIST
    # =====================================================

    branch_df = fetch_branch_list()

    mapped_branch_df = filter_mapped_branches(
        branch_df,
        help_df
    )

    # =====================================================
    # FETCH DATA
    # =====================================================

    soldout_frames = []

    inventory_frames = []

    for _, row in mapped_branch_df.iterrows():

        branch_name = row.get(
            "branchName",
            ""
        )

        log(f"Processing {branch_name}")

        soldout_df = fetch_soldout_history(
            branch_name
        )

        inventory_df = fetch_inventory_stock(
            branch_name
        )

        soldout_frames.append(
            soldout_df
        )

        inventory_frames.append(
            inventory_df
        )

    # =====================================================
    # FINAL DATAFRAMES
    # =====================================================

    final_soldout_df = pd.concat(
        soldout_frames,
        ignore_index=True
    ) if soldout_frames else pd.DataFrame()

    final_inventory_df = pd.concat(
        inventory_frames,
        ignore_index=True
    ) if inventory_frames else pd.DataFrame()

    summary_df = build_summary_dataframe(
        mapped_branch_df,
        final_soldout_df,
        final_inventory_df
    )

    # =====================================================
    # UPLOAD TO SHEETS
    # =====================================================

    upload_dataframe_to_worksheet(
        spreadsheet,
        "branch_list",
        mapped_branch_df
    )

    upload_dataframe_to_worksheet(
        spreadsheet,
        "soldout_history",
        final_soldout_df
    )

    upload_dataframe_to_worksheet(
        spreadsheet,
        "inventory_stock",
        final_inventory_df
    )

    upload_dataframe_to_worksheet(
        spreadsheet,
        "summary",
        summary_df
    )

    # =====================================================
    # EMAIL
    # =====================================================

    html_body = build_email_html(
        summary_df,
        final_soldout_df,
        final_inventory_df
    )

    send_email(
        "Rista Hourly Automation",
        html_body
    )

    log("Automation completed successfully")


if __name__ == "__main__":
    main()
