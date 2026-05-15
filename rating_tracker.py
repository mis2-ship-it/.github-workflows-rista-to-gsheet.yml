import requests
import pandas as pd
import gspread
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import re

# =========================================
# GOOGLE AUTH
# =========================================

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_name(
    "service_account.json",
    scope
)

client = gspread.authorize(creds)

spreadsheet = client.open_by_key(
    "179JtVxWo1jEBwy0DXtV6uf22WQV-z6DiZ6qvTqxeh64"
)

mapping_sheet = spreadsheet.worksheet("RID_Mapping")
output_sheet = spreadsheet.worksheet("Rating_Output")

# =========================================
# LOAD RID DATA
# =========================================

raw_data = mapping_sheet.get("A:F")

headers_row = raw_data[0]
rows = raw_data[1:]

mapping_data = []

for row in rows:

    # Skip blank rows
    if len(row) == 0:
        continue

    # Fill missing columns
    while len(row) < 6:
        row.append("")

    row_dict = dict(zip(headers_row, row))

    # FILTER ONLY COCO
    if str(row_dict.get("Store Type", "")).strip().upper() == "COCO":
        mapping_data.append(row_dict)

print(f"✅ COCO Stores Found: {len(mapping_data)}")

# =========================================
# HEADERS
# =========================================

headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": "https://www.swiggy.com/",
    "Origin": "https://www.swiggy.com"
}

final_rows = []

print("🚀 Script Started")

# =========================================
# LOOP STORES
# =========================================

for row in mapping_data:

    s_rid = str(row.get("S_RID", "")).strip()
    z_rid = str(row.get("Z_RID", "")).strip()

    brand = row.get("Brand Name", "")
    store = row.get("Store Name", "")
    region = row.get("Region", "")

    # =====================================
    # SWIGGY FETCH
    # =====================================

    s_rating = ""
    s_reviews = ""

    try:

        swiggy_url = (
            f"https://www.swiggy.com/dapi/menu/pl?"
            f"page-type=REGULAR_MENU"
            f"&complete-menu=true"
            f"&lat=15.3647"
            f"&lng=75.1240"
            f"&restaurantId={s_rid}"
        )

        response = requests.get(
            swiggy_url,
            headers=headers,
            timeout=20
        )

        if response.status_code == 200:

            json_data = response.json()

            cards = json_data.get("data", {}).get("cards", [])

            info = None

            for card in cards:
                try:
                    info = card["card"]["card"]["info"]
                    break
                except:
                    continue

            if info:

                s_rating = info.get("avgRating", "")
                s_reviews = info.get("totalRatingsString", "")

                print(f"✅ Swiggy Done - {store}")

            else:
                print(f"❌ Swiggy Info Missing - {store}")

        else:
            print(
                f"❌ Swiggy API Failed - "
                f"{store} - Status: {response.status_code}"
            )

    except Exception as e:
        print(f"❌ Swiggy Error - {store} - {e}")

    # =====================================
    # ZOMATO FETCH
    # =====================================

    z_rating = ""
    z_reviews = ""

    try:

        zomato_url = f"https://www.zomato.com/restaurant/{z_rid}"

        response = requests.get(
            zomato_url,
            headers=headers,
            timeout=20
        )

        soup = BeautifulSoup(
            response.text,
            "html.parser"
        )

        page_text = soup.get_text(" ", strip=True)

        rating_match = re.findall(
            r'\b\d\.\d\b',
            page_text
        )

        review_match = re.findall(
            r'([\d\,]+)\s*reviews',
            page_text,
            re.IGNORECASE
        )

        if rating_match:
            z_rating = rating_match[0]

        if review_match:
            z_reviews = review_match[0]

        print(f"✅ Zomato Done - {store}")

    except Exception as e:
        print(f"❌ Zomato Error - {store} - {e}")

    # =====================================
    # FINAL OUTPUT
    # =====================================

    final_rows.append([
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        brand,
        store,
        region,
        s_rating,
        z_rating,
        s_reviews,
        z_reviews
    ])

# =========================================
# UPDATE GSHEET
# =========================================

if final_rows:

    existing_data = output_sheet.get_all_values()

    next_row = len(existing_data) + 1

    output_sheet.update(
        f"A{next_row}:H{next_row + len(final_rows) - 1}",
        final_rows
    )

    print("✅ Google Sheet Updated")

else:
    print("❌ No Data Found")

print("🏁 Script Completed")
