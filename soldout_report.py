# =========================================================
# IMPORTS
# =========================================================

import os
import json
import time
import jwt
import requests
import pandas as pd
import gspread
import smtplib

from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

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
    json.loads(
        os.environ["GOOGLE_CREDENTIALS"]
    ),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

client = gspread.authorize(creds)


# =========================================================
# GOOGLE SHEET
# =========================================================

spreadsheet = client.open_by_key(
    "19z6KkVBFoLC33_wcNqVhDLyQEC2dDQ8YQE0gE38BhVg"
)

print("✅ Connected Google Sheet")


# =========================================================
# DATE
# =========================================================

business_day = datetime.now().strftime(
    "%Y-%m-%d"
)

print("📅 Business Day:", business_day)

# =========================================================
# HELP SHEET
# =========================================================

help_ws = spreadsheet.worksheet(
    "Help Sheet"
)

help_df = pd.DataFrame(
    help_ws.get_all_records()
)

print("HELP SHEET COLUMNS:")
print(help_df.columns.tolist())


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

for col in required_cols:

    if col not in help_df.columns:
        help_df[col] = ""


# =========================================================
# CLEAN DATA
# =========================================================

help_df["branchCode"] = (
    help_df["branchCode"]
    .astype(str)
    .str.strip()
)

help_df["Ownership"] = (
    help_df["Ownership"]
    .astype(str)
    .str.upper()
    .str.strip()
)

help_df["Region"] = (
    help_df["Region"]
    .astype(str)
    .str.upper()
    .replace({
        "KERALA": "KL",
        "KL": "KL",
        "KARNATAKA": "KA",
        "MAHARASHTRA": "MH",
        "TAMIL NADU": "TN"
    })
)


# =========================================================
# FILTER COCO ONLY
# =========================================================

help_df = help_df[
    help_df["Ownership"] == "COCO"
]

help_df = help_df[
    help_df["branchCode"] != ""
]

help_df = help_df[
    help_df["branchCode"] != "#N/A"
]


print(
    "🏪 COCO Branch Count:",
    len(help_df)
)


# =========================================================
# BRANCH LIST
# =========================================================

branches = (
    help_df["branchCode"]
    .dropna()
    .unique()
    .tolist()
)

# =========================================================
# FETCH SOLDOUT DATA
# =========================================================

url = (
    "https://api.ristaapps.com/"
    "v1/items/soldout/history"
)

all_data = []

for branch in branches:

    print(f"Fetching: {branch}")

    try:

        response = requests.get(
            url,
            headers=headers(),
            params={
                "branch": branch,
                "day": business_day
            },
            timeout=60
        )

        if response.status_code != 200:

            print(
                f"❌ Failed: {branch}"
            )

            continue

        response_json = response.json()

        if isinstance(response_json, dict):

            data = response_json.get(
                "data",
                []
            )

        else:
            data = response_json

        if not data:
            continue

        df = pd.json_normalize(data)

        # add branchCode manually
        df["branchCode"] = branch

        all_data.append(df)

    except Exception as e:

        print(
            f"❌ Error {branch}:",
            str(e)
        )


#=========================================================
# CONCAT
#=========================================================

if len(all_data) == 0:

    print("❌ No Data")
    exit()

final_df = pd.concat(
    all_data,
    ignore_index=True
)

print("✅ Rows:", len(final_df))


print("FINAL DF COLUMNS:")
print(final_df.columns.tolist())

# =========================================================
# FILTER DATA
# =========================================================

final_df = final_df.fillna("")

# eventType = OUT
if "eventType" in final_df.columns:

    final_df = final_df[
        final_df["eventType"]
        .astype(str)
        .str.upper()
        == "OUT"
    ]


# statusType = Direct
if "statusType" in final_df.columns:

    final_df = final_df[
        final_df["statusType"]
        .astype(str)
        .str.upper()
        == "DIRECT"
    ]


# =========================================================
# BUSINESS DATE
# =========================================================

if "eventBusinessDay" in final_df.columns:

    final_df["Business Date"] = (
        final_df["eventBusinessDay"]
    )

else:

    final_df["Business Date"] = (
        business_day
    )


# =========================================================
# MERGE HELP SHEET
# =========================================================

help_merge_cols = [
    "branchCode",
    "Store Name",
    "AM Email",
    "RM Email",
    "AM Name",
    "CC Mail",
    "Region"
]

final_df["branchCode"] = (
    final_df["branchCode"]
    .astype(str)
    .str.strip()
)

help_df["branchCode"] = (
    help_df["branchCode"]
    .astype(str)
    .str.strip()
)

final_df = final_df.merge(
    help_df[help_merge_cols],
    on="branchCode",
    how="left"
)

print("✅ Merge Completed")
print("Final Rows:", len(final_df))


# =========================================================
# REQUIRED OUTPUT COLUMNS
# =========================================================

required_output_cols = [
    "Business Date",
    "branchCode",
    "Store Name",
    "Region",
    "itemType",
    "categoryName",
    "itemName",
    "eventDate",
    "userName",
    "statusType",
    "eventType",
    "AM Email",
    "RM Email",
    "AM Name",
    "CC Mail"
]

for col in required_output_cols:

    if col not in final_df.columns:
        final_df[col] = ""


final_df = final_df[
    required_output_cols
]

print("✅ Filter Completed")
print(final_df.head())

# =========================================================
# WORKSHEET FUNCTION
# =========================================================

def get_or_create_sheet(sheet_name):

    try:

        ws = spreadsheet.worksheet(
            sheet_name
        )

    except:

        ws = spreadsheet.add_worksheet(
            title=sheet_name,
            rows=1000,
            cols=100
        )

    ws.clear()

    return ws

# =========================================================
# REFRESH SHEET FUNCTION
# =========================================================

def refresh_sheet(sheet_name, df):

    try:
        ws = spreadsheet.worksheet(sheet_name)

    except:
        ws = spreadsheet.add_worksheet(
            title=sheet_name,
            rows="5000",
            cols="200"
        )

    ws.clear()

    if len(df) == 0:
        ws.update([["No Data"]])
        return

    df = df.fillna("")

    ws.update(
        [df.columns.tolist()] + df.values.tolist(),
        value_input_option="USER_ENTERED"
    )

    print(f"✅ Refreshed: {sheet_name}")


# =========================================================
# RAW DATA SHEET
# =========================================================

raw_ws = get_or_create_sheet(
    "Raw_Data"
)

raw_ws.update(
    [final_df.columns.tolist()]
    + final_df.astype(str).values.tolist()
)

print("✅ Raw_Data Updated")


# =========================================================
# SOLDOUT REPORT
# =========================================================

soldout_report = final_df.copy()

soldout_ws = get_or_create_sheet(
    "Soldout_Report"
)

soldout_ws.update(
    [soldout_report.columns.tolist()]
    + soldout_report.astype(str)
    .values.tolist()
)

print("✅ Soldout_Report Updated")

# =========================================================
# REGION CLEANUP
# =========================================================

final_df["Region"] = (
    final_df["Region"]
    .astype(str)
    .str.strip()
    .replace({
        "Kerala": "KL",
        "KA": "KA",
        "MH": "MH",
        "TN": "TN",
        "KL": "KL"
    })
)


# =========================================================
# MATERIAL SUMMARY
# =========================================================

material_df = final_df[
    final_df["itemType"]
    .str.upper()
    == "MATERIAL"
].copy()

material_summary = pd.pivot_table(
    material_df,
    index="categoryName",
    columns="Region",
    values="itemName",
    aggfunc="count",
    fill_value=0
).reset_index()

for col in ["KA", "MH", "TN", "KL"]:

    if col not in material_summary.columns:
        material_summary[col] = 0

material_summary = material_summary[
    ["categoryName", "KA", "MH", "TN", "KL"]
]

material_summary = material_summary.replace(
    0,
    "-"
)


# =========================================================
# PRODUCT SUMMARY
# =========================================================

product_df = final_df[
    final_df["itemType"]
    .str.upper()
    == "PRODUCT"
].copy()

product_summary = pd.pivot_table(
    product_df,
    index="categoryName",
    columns="Region",
    values="itemName",
    aggfunc="count",
    fill_value=0
).reset_index()

for col in ["KA", "MH", "TN", "KL"]:

    if col not in product_summary.columns:
        product_summary[col] = 0

product_summary = product_summary[
    ["categoryName", "KA", "MH", "TN", "KL"]
]

product_summary = product_summary.replace(
    0,
    "-"
)


# =========================================================
# STORE SUMMARY
# =========================================================

store_summary = pd.pivot_table(
    final_df,
    index="Store Name",
    columns="categoryName",
    values="itemName",
    aggfunc="count",
    fill_value=0
).reset_index()

store_summary = store_summary.replace(
    0,
    "-"
)

# =========================================================
# REPORT TABLES
# =========================================================

material_report = final_df[
    final_df["itemType"] == "Material"
].copy()

product_report = final_df[
    final_df["itemType"] == "Product"
].copy()

store_report = final_df.copy()

# Replace blanks
material_report = material_report.fillna("-")
product_report = product_report.fillna("-")
store_report = store_report.fillna("-")

# =========================================================
# PUSH SUMMARY SHEETS
# =========================================================

summary_dict = {
    "Material_Summary":
        material_summary,

    "Product_Summary":
        product_summary,

    "Store_Summary":
        store_summary,

    "Material_Report":
        material_df,

    "Product_Report":
        product_df,

    "Store_Report":
        final_df
}

for sheet_name, df_sheet in summary_dict.items():

    ws = get_or_create_sheet(
        sheet_name
    )

    ws.update(
        [df_sheet.columns.tolist()]
        + df_sheet.astype(str)
        .values.tolist()
    )

    print(f"✅ {sheet_name} Updated")


# =========================================================
# REFRESH ALL SHEETS
# =========================================================

refresh_sheet("Raw_Data", final_df)

refresh_sheet(
    "Material_Summary",
    material_summary
)

refresh_sheet(
    "Product_Summary",
    product_summary
)

refresh_sheet(
    "Store_Summary",
    store_summary
)

refresh_sheet(
    "Soldout_Report",
    final_df
)

refresh_sheet(
    "Material_Report",
    material_report
)

refresh_sheet(
    "Product_Report",
    product_report
)

refresh_sheet(
    "Store_Report",
    store_report
)

print("✅ Summary Sheets Pushed")

# =========================================================
# HTML FORMAT
# =========================================================

def format_html_table(df):

    if len(df) == 0:
        return "<p>No Data Available</p>"

    df = df.copy()

    df = df.fillna("-")
    df = df.replace(0, "-")
    df = df.replace("0", "-")

    html = df.to_html(
        index=False,
        border=0
    )

    style = """
    <style>

    body{
        font-family: Arial, sans-serif;
    }

    table{
        border-collapse: collapse;
        width: 100%;
        font-size: 13px;
    }

    th{
        background-color: #1F4E78;
        color: white;
        padding: 8px;
        border: 1px solid #D9D9D9;
        text-align: center;
    }

    td{
        border: 1px solid #D9D9D9;
        padding: 7px;
        text-align: center;
    }

    tr:nth-child(even){
        background-color: #F7F7F7;
    }

    h2{
        color:#1F4E78;
    }

    </style>
    """

    return style + html


# =========================================================
# EMAIL CONFIG
# =========================================================

EMAIL_USER = os.environ["EMAIL_USER"]
EMAIL_PASSWORD = os.environ["EMAIL_PASSWORD"]


# =========================================================
# SEND MAIL FUNCTION
# =========================================================

def send_mail(to_list, subject, html_body):

    try:

        msg = MIMEMultipart()

        msg["From"] = EMAIL_USER
        msg["To"] = ", ".join(to_list)
        msg["Subject"] = subject

        msg.attach(
            MIMEText(html_body, "html")
        )

        server = smtplib.SMTP(
            "smtp.gmail.com",
            587
        )

        server.starttls()

        server.login(
            EMAIL_USER,
            EMAIL_PASSWORD
        )

        server.sendmail(
            EMAIL_USER,
            to_list,
            msg.as_string()
        )

        server.quit()

        print(f"✅ Mail Sent: {subject}")

    except Exception as e:
        print(f"❌ Mail Failed: {str(e)}")



# =========================================================
# STEP 9 : SUMMARY MAIL
# =========================================================

EMAIL_USER = os.environ["EMAIL_USER"]
EMAIL_PASSWORD = os.environ["EMAIL_PASSWORD"]


def style_html_table(df):

    if df.empty:
        return "<p>No Soldout Data</p>"

    df = df.copy()

    df = df.replace(0, "-")
    df = df.fillna("-")

    return (
        df.to_html(index=True, border=0)
        .replace(
            '<table border="1" class="dataframe">',
            '''
            <table style="
            border-collapse:collapse;
            width:100%;
            font-family:Arial;
            font-size:13px;
            ">
            '''
        )
        .replace(
            "<th>",
            '''
            <th style="
            background:#1F4E78;
            color:white;
            border:1px solid #d9d9d9;
            padding:8px;
            text-align:center;
            ">
            '''
        )
        .replace(
            "<td>",
            '''
            <td style="
            border:1px solid #d9d9d9;
            padding:8px;
            text-align:center;
            ">
            '''
        )
    )


summary_html = f"""
            <html>
            <body style="font-family:Arial;">
            
            <h2>📦 Material Soldout _ Region Wise</h2>
            {style_html_table(material_summary)}
            
            <br>
            
            <h2>🍔 Product Soldout _ Region Wise</h2>
            {style_html_table(product_summary)}
            
            <br>
            
            <h2>🏪 Store Wise Soldout _ Category Wise</h2>
            {style_html_table(store_summary)}
            
            <br>
            
            <p>
            Regards,<br>
            Rista Soldout Automation
            </p>
            
            </body>
            </html>
            """


# CC MAIL LIST
cc_mails = []

for mail in help_df["CC Mail"].dropna().unique():

    split_mails = str(mail).split(",")

    for m in split_mails:

        clean_mail = m.strip()

        if clean_mail:
            cc_mails.append(clean_mail)

cc_mails = list(set(cc_mails))

print("📧 Summary Mail Count:", len(cc_mails))


try:

    msg = MIMEMultipart()

    msg["From"] = EMAIL_USER
    msg["To"] = ", ".join(cc_mails)

    msg["Subject"] = (
        f"Soldout Summary Report - {business_day}"
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
        EMAIL_PASSWORD
    )

    server.sendmail(
        EMAIL_USER,
        cc_mails,
        msg.as_string()
    )

    server.quit()

    print("✅ ONE Summary Mail Sent")

except Exception as e:

    print("❌ Summary Mail Failed:", str(e))

# =========================================================
# STEP 10 : ALERT MAIL
# =========================================================

for store in final_df["Store Name"].dropna().unique():

    try:

        store_df = final_df[
            final_df["Store Name"] == store
        ].copy()

        if store_df.empty:
            continue

        am_email = (
            store_df["AM Email"]
            .dropna()
            .astype(str)
            .iloc[0]
        )

        rm_email = (
            store_df["RM Email"]
            .dropna()
            .astype(str)
            .iloc[0]
        )

        am_name = (
            store_df["AM Name"]
            .dropna()
            .astype(str)
            .iloc[0]
        )

        if not am_email:
            continue

        recipients = []

        for mail in [am_email, rm_email]:

            split_mails = str(mail).split(",")

            for m in split_mails:

                clean_mail = m.strip()

                if clean_mail:
                    recipients.append(clean_mail)

        recipients = list(set(recipients))

        # ---------------- DETAIL TABLE ---------------- #

        store_html = store_df[
            [
                "Store Name",
                "itemType",
                "categoryName",
                "itemName",
                "eventDate",
                "userName"
            ]
        ].copy()
        
        store_html.columns = [
            "Store Name",
            "Item Type",
            "Category",
            "Item Name",
            "Event Time",
            "User Name"
        ]
        
        store_html = (
            store_html.fillna("-")
            .replace(0, "-")
        )
        
        html_table = style_html_table(store_html)
        
        body = f"""
        <html>
        <body>
        
        <p>
        Hi {am_name},
        </p>
        
        <p>
        Please check below soldout items and
        reply immediately with reason for soldout.
        </p>
        
        {html_table}
        
        <br>
        
        <p>
        Regards,<br>
        MIS Team
        </p>
        
        </body>
        </html>
        """

        msg = MIMEMultipart()

        msg["From"] = EMAIL_USER

        msg["To"] = ", ".join(recipients)

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
            EMAIL_PASSWORD
        )

        server.sendmail(
            EMAIL_USER,
            recipients,
            msg.as_string()
        )

        server.quit()

        print(f"✅ Alert Sent: {store}")

    except Exception as e:

        print(
            f"❌ Alert Failed {store}: {str(e)}"
        )


print("🎉 SOLDOUT SCRIPT COMPLETED")


