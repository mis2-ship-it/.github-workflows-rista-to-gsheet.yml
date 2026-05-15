import requests
import pandas as pd
import gspread
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import re

# =========================================
# GOOGLE SHEET NAME
# =========================================

SHEET_NAME = "Live RID List"

# =========================================
# GOOGLE AUTH
# =========================================

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_name(
    "GOOGLE_SERVICE_ACCOUNT_JSON",
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

mapping_data = mapping_sheet.get_all_records()

headers = {
    "User-Agent": "Mozilla/5.0"
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
            print(f"❌ Swiggy API Failed - {store}")

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

        rating_match = re.search(r'(\d\.\d)', page_text)

        reviews_match = re.search(
            r'(\d+[\,]?\d*) Reviews',
            page_text
        )

        if rating_match:
            z_rating = rating_match.group(1)

        if reviews_match:
            z_reviews = reviews_match.group(1)

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
        s_rating,
        z_rating,
        s_reviews,
        z_reviews
    ])

# =========================================
# UPDATE GSHEET
# =========================================

if final_rows:

    output_sheet.append_rows(
        final_rows,
        value_input_option="USER_ENTERED"
    )

    print("✅ Google Sheet Updated")

else:
    print("❌ No Data Found")

print("🏁 Script Completed")
