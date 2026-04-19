import json
import os
import zipfile
from datetime import datetime, timezone

import requests

from db.database import get_db_connection
from paths import ALL_PRICES_TODAY_PATH, ALL_PRICES_TODAY_ZIP_PATH
from settings import MTGJSON_ALL_PRICES_TODAY_URL


def ensure_price_download_directories():
    os.makedirs(os.path.dirname(ALL_PRICES_TODAY_PATH), exist_ok=True)


def download_all_prices_today_json():
    ensure_price_download_directories()

    headers = {
        "User-Agent": "iMomir/1.0",
        "Accept": "application/zip,application/octet-stream;q=0.9,*/*;q=0.8",
    }

    response = requests.get(
        MTGJSON_ALL_PRICES_TODAY_URL,
        headers=headers,
        timeout=300,
    )
    response.raise_for_status()

    with open(ALL_PRICES_TODAY_ZIP_PATH, "wb") as file_handle:
        file_handle.write(response.content)

    with zipfile.ZipFile(ALL_PRICES_TODAY_ZIP_PATH, "r") as zip_ref:
        zip_members = zip_ref.namelist()
        target_member = None

        for member_name in zip_members:
            if member_name.lower().endswith(".json"):
                target_member = member_name
                break

        if not target_member:
            raise ValueError("AllPricesToday zip did not contain a JSON file.")

        with zip_ref.open(target_member) as source_file:
            with open(ALL_PRICES_TODAY_PATH, "wb") as output_file:
                output_file.write(source_file.read())


def _get_latest_price_from_points(price_points):
    if not isinstance(price_points, dict) or not price_points:
        return None, None

    latest_date = None
    latest_value = None

    for date_key, value in price_points.items():
        if latest_date is None or str(date_key) > str(latest_date):
            latest_date = str(date_key)
            latest_value = value

    try:
        latest_value = float(latest_value)
    except (TypeError, ValueError):
        latest_value = None

    return latest_date, latest_value


def import_all_prices_today_into_database():
    if not os.path.exists(ALL_PRICES_TODAY_PATH):
        raise FileNotFoundError("AllPricesToday.json was not found.")

    with open(ALL_PRICES_TODAY_PATH, "r", encoding="utf-8") as file_handle:
        raw_json = json.load(file_handle)

    price_data = raw_json.get("data", {})
    if not isinstance(price_data, dict):
        raise ValueError("AllPricesToday.json did not contain a valid data object.")

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM card_prices")

    updated_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    inserted_count = 0

    for card_uuid, provider_block in price_data.items():
        if not isinstance(provider_block, dict):
            continue

        paper_block = provider_block.get("paper") or {}
        tcgplayer_block = paper_block.get("tcgplayer") or {}
        retail_block = tcgplayer_block.get("retail") or {}

        if not isinstance(retail_block, dict):
            continue

        normal_date, normal_price = _get_latest_price_from_points(retail_block.get("normal"))
        foil_date, foil_price = _get_latest_price_from_points(retail_block.get("foil"))
        etched_date, etched_price = _get_latest_price_from_points(retail_block.get("etched"))

        currency = tcgplayer_block.get("currency") or "USD"
        price_date = max(
            [d for d in [normal_date, foil_date, etched_date] if d],
            default=None,
        )

        cursor.execute(
            """
            INSERT INTO card_prices (
                card_uuid,
                tcgplayer_normal_price,
                tcgplayer_foil_price,
                tcgplayer_etched_price,
                currency,
                price_date,
                updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                card_uuid,
                normal_price,
                foil_price,
                etched_price,
                currency,
                price_date,
                updated_at_utc,
            ),
        )

        inserted_count += 1

    conn.commit()
    conn.close()

    return inserted_count


def get_card_price_by_uuid(card_uuid, finish_type="normal", price_source="tcgplayer-retail"):
    normalized_finish = (finish_type or "normal").strip().lower()

    if normalized_finish not in {"normal", "foil", "etched"}:
        normalized_finish = "normal"

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            tcgplayer_normal_price,
            tcgplayer_foil_price,
            tcgplayer_etched_price,
            currency,
            price_date
        FROM card_prices
        WHERE card_uuid = ?
        """,
        ((card_uuid or "").strip(),),
    )

    row = cursor.fetchone()
    conn.close()

    if not row:
        return {
            "price": None,
            "currency": "USD",
            "price_date": None,
            "source": price_source,
            "finish": normalized_finish,
        }

    price_value = None
    if normalized_finish == "foil":
        price_value = row["tcgplayer_foil_price"]
    elif normalized_finish == "etched":
        price_value = row["tcgplayer_etched_price"]
    else:
        price_value = row["tcgplayer_normal_price"]

    return {
        "price": price_value,
        "currency": row["currency"] or "USD",
        "price_date": row["price_date"],
        "source": price_source,
        "finish": normalized_finish,
    }


def get_finish_type_for_pack_card(card_entry):
    if int(card_entry.get("sheet_is_foil") or 0) == 1:
        return "foil"
    return "normal"


def enrich_pack_cards_with_prices(cards, display_prices=True, price_source="tcgplayer-retail"):
    enriched_cards = []

    for card_entry in cards or []:
        enriched_entry = dict(card_entry)

        finish_type = get_finish_type_for_pack_card(card_entry)

        enriched_entry["finish_type"] = finish_type
        enriched_entry["special_badges"] = []

        if finish_type == "foil":
            enriched_entry["special_badges"].append("Foil")

        if display_prices:
            price_info = get_card_price_by_uuid(
                card_entry.get("card_uuid"),
                finish_type=finish_type,
                price_source=price_source,
            )
        else:
            price_info = {
                "price": None,
                "currency": "USD",
                "price_date": None,
                "source": price_source,
                "finish": finish_type,
            }

        enriched_entry["price_info"] = price_info
        enriched_cards.append(enriched_entry)

    return enriched_cards