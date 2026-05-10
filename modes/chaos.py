import json
import os
import random
import re
from datetime import datetime, timezone
from io import BytesIO

from flask import url_for
from pypdf import PdfWriter

from settings import (
    ALLOWED_CHAOS_BOOSTER_TYPES,
    CHAOS_DUPLICATE_CONTROL_ENABLED,
    CHAOS_DUPLICATE_CONTROL_TYPES,
    CHAOS_DUPLICATE_MAX_REROLLS,
    CHAOS_DUPLICATE_REROLL_CHANCE,
    CHAOS_PACK_TYPE_OPTIONS,
)
from db.database import get_config, get_db_connection, get_selected_set_codes

def get_pack_type_code_for_tracking(booster_name):
    value = (booster_name or "").strip().lower()

    if not value:
        return "O"

    if "collector" in value:
        return "C"

    if "jumpstart" in value:
        return "J"

    if "vip" in value:
        return "V"

    if "six" in value:
        return "6"

    if "premium" in value:
        return "P"

    if "play" in value:
        return "P"

    if "draft" in value:
        return "D"

    if "set" in value:
        return "S"

    if "booster" in value or "core" in value:
        return "B"

    if "promo" in value or "sample" in value:
        return "O"

    return "O"


def build_pack_tracking_suffix(set_code, booster_name, booster_index):
    alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    modulus = len(alphabet) ** 4

    seed_text = (
        f"{(set_code or '').strip().upper()}|"
        f"{(booster_name or '').strip().lower()}|"
        f"{int(booster_index or 0)}|"
        f"{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}|"
        f"{random.randint(0, 999999999)}"
    )

    value = 0
    for character in seed_text:
        value = ((value * 131) + ord(character)) % modulus

    chars = []
    for _ in range(4):
        chars.append(alphabet[value % len(alphabet)])
        value //= len(alphabet)

    return "".join(reversed(chars))


def build_pack_tracking_code(set_code, booster_name, booster_index):
    clean_set_code = (set_code or "").strip().upper()
    pack_type_code = get_pack_type_code_for_tracking(booster_name)
    suffix = build_pack_tracking_suffix(clean_set_code, booster_name, booster_index)

    return f"{clean_set_code}.{pack_type_code}.{suffix}"

def normalize_chaos_booster_key(booster_name):
    value = (booster_name or "").strip().lower()

    if not value:
        return "default"
    if "collector" in value:
        return "collector"
    if "jumpstart" in value:
        return "jumpstart"
    if "play" in value:
        return "play"
    if "set" in value:
        return "set"
    if "draft" in value:
        return "draft"

    return value.replace(" ", "_")


def normalize_booster_type_for_filter(booster_name):
    value = (booster_name or "").strip().lower()
    value = value.replace(" booster", "")
    value = value.replace(" boosters", "")
    return value


def parse_chaos_pack_types_config(raw_value):
    allowed_pack_types = {item["value"] for item in CHAOS_PACK_TYPE_OPTIONS}

    selected_pack_types = set()
    for item in (raw_value or "").split(","):
        normalized_item = (item or "").strip().lower()
        if normalized_item and normalized_item in allowed_pack_types:
            selected_pack_types.add(normalized_item)

    if not selected_pack_types:
        selected_pack_types = set(ALLOWED_CHAOS_BOOSTER_TYPES)

    return selected_pack_types


def get_selected_chaos_pack_types(config=None):
    if config is None:
        config = get_config()

    return parse_chaos_pack_types_config(config.get("chaos_pack_types", ""))


def build_chaos_pack_types_config_value(selected_pack_types):
    allowed_pack_types = [item["value"] for item in CHAOS_PACK_TYPE_OPTIONS]

    normalized_selected = []
    selected_lookup = {str(item).strip().lower() for item in (selected_pack_types or [])}

    for pack_type in allowed_pack_types:
        if pack_type in selected_lookup:
            normalized_selected.append(pack_type)

    return ",".join(normalized_selected)


def get_chaos_pack_type_label_map():
    return {
        item["value"]: item["label"]
        for item in CHAOS_PACK_TYPE_OPTIONS
    }


def normalize_chaos_pack_display_name(display_name):
    value = (display_name or "").strip()
    if not value:
        return value

    match = re.search(r"\(([^()]*)\)\s*$", value)
    if not match:
        return value

    set_code = (match.group(1) or "").strip().upper()
    normalized_value = value[:match.start()] + f"({set_code})"
    return normalized_value


def get_set_name_from_code(set_code):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT set_name
        FROM sets
        WHERE set_code = ?
        """,
        ((set_code or "").strip().upper(),),
    )

    row = cursor.fetchone()
    conn.close()

    if row and row["set_name"]:
        return row["set_name"]

    return (set_code or "").strip().upper()


def build_default_chaos_pack_display_name(set_code, booster_name):
    set_name = get_set_name_from_code(set_code)

    clean_booster_name = (booster_name or "").strip().lower()
    booster_label_map = get_chaos_pack_type_label_map()

    display_booster_name = booster_label_map.get(
        clean_booster_name,
        " ".join(word.capitalize() for word in clean_booster_name.split()) if clean_booster_name else "Booster Pack"
    )

    set_code_clean = (set_code or "").strip().upper()
    return normalize_chaos_pack_display_name(
        f"{set_name} - {display_booster_name} ({set_code_clean})"
    )


def get_chaos_pack_art_relpath(set_code, booster_name, static_folder):
    normalized_booster_key = normalize_chaos_booster_key(booster_name)

    set_code_variants = []
    raw_set_code = (set_code or "").strip()

    if raw_set_code:
        set_code_variants.append(raw_set_code)
        if raw_set_code.upper() not in set_code_variants:
            set_code_variants.append(raw_set_code.upper())
        if raw_set_code.lower() not in set_code_variants:
            set_code_variants.append(raw_set_code.lower())

    for set_code_variant in set_code_variants:
        direct_relpath = f"img/pack_art/{set_code_variant}/{normalized_booster_key}.png"
        direct_abspath = os.path.join(static_folder, direct_relpath.replace("/", os.sep))

        if os.path.exists(direct_abspath):
            return direct_relpath

    for set_code_variant in set_code_variants:
        default_relpath = f"img/pack_art/{set_code_variant}/default.png"
        default_abspath = os.path.join(static_folder, default_relpath.replace("/", os.sep))

        if os.path.exists(default_abspath):
            return default_relpath

    return "img/pack_art/_fallback/booster_default.png"


def get_chaos_pack_art_info(set_code, booster_name, static_folder):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT display_name, image_path, is_fallback
        FROM chaos_pack_art
        WHERE set_code = ?
          AND booster_name = ?
        """,
        (
            (set_code or "").strip().upper(),
            (booster_name or "").strip().lower(),
        ),
    )

    row = cursor.fetchone()
    conn.close()

    default_display_name = build_default_chaos_pack_display_name(set_code, booster_name)
    default_image_path = get_chaos_pack_art_relpath(set_code, booster_name, static_folder)

    if row:
        display_name = normalize_chaos_pack_display_name(
            (row["display_name"] or "").strip() or default_display_name
        )
        image_path = (row["image_path"] or "").strip() or default_image_path

        static_abs_path = os.path.join(static_folder, image_path.replace("/", os.sep))
        if not os.path.exists(static_abs_path):
            image_path = default_image_path

        return {
            "display_name": display_name,
            "image_path": image_path,
            "is_fallback": int(row["is_fallback"] or 0),
        }

    return {
        "display_name": default_display_name,
        "image_path": default_image_path,
        "is_fallback": 1,
    }


def clear_chaos_pack_history():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM chaos_pack_history")

    conn.commit()
    conn.close()


def record_chaos_pack_history(set_code, booster_name, booster_index, pack_display_name):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO chaos_pack_history (
            set_code,
            booster_name,
            booster_index,
            pack_display_name,
            opened_at_utc
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            (set_code or "").strip().upper(),
            (booster_name or "").strip().lower(),
            int(booster_index),
            (pack_display_name or "").strip(),
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        ),
    )

    conn.commit()
    conn.close()


def get_chaos_opened_pack_keys():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT set_code, booster_name, booster_index
        FROM chaos_pack_history
        """
    )

    rows = cursor.fetchall()
    conn.close()

    opened_keys = set()

    for row in rows:
        opened_keys.add(
            (
                (row["set_code"] or "").strip().upper(),
                (row["booster_name"] or "").strip().lower(),
                int(row["booster_index"] or 0),
            )
        )

    return opened_keys


def choose_weighted_row(rows, weight_key):
    candidate_rows = []
    total_weight = 0.0

    for row in rows:
        try:
            weight = float(row.get(weight_key, 0) or 0)
        except (TypeError, ValueError):
            weight = 0.0

        if weight <= 0:
            continue

        total_weight += weight
        candidate_rows.append((row, total_weight))

    if not candidate_rows:
        return None

    roll = random.uniform(0, total_weight)

    for row, running_total in candidate_rows:
        if roll <= running_total:
            return row

    return candidate_rows[-1][0]


def get_chaos_pack_variants(set_code, booster_name):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            set_code,
            booster_name,
            booster_index,
            booster_weight
        FROM chaos_booster_variants
        WHERE set_code = ?
          AND booster_name = ?
        ORDER BY booster_index ASC
        """,
        (
            (set_code or "").strip().upper(),
            (booster_name or "").strip().lower(),
        ),
    )

    rows = cursor.fetchall()
    conn.close()

    variants = []

    for row in rows:
        variants.append({
            "set_code": row["set_code"],
            "booster_name": row["booster_name"],
            "booster_index": int(row["booster_index"] or 0),
            "booster_weight": float(row["booster_weight"] or 0),
        })

    return variants


def get_eligible_chaos_packs(static_folder):
    config = get_config()
    selected_set_codes = get_selected_set_codes()

    conn = get_db_connection()
    cursor = conn.cursor()

    params = []
    where_conditions = []

    if config.get("all_sets_enabled") == "0" and selected_set_codes:
        placeholders = ",".join(["?"] * len(selected_set_codes))
        where_conditions.append(f"cbv.set_code IN ({placeholders})")
        params.extend(sorted(selected_set_codes))

    where_clause = ""
    if where_conditions:
        where_clause = "WHERE " + " AND ".join(where_conditions)

    cursor.execute(
        f"""
        SELECT
            cbv.set_code,
            cbv.booster_name,
            COUNT(*) AS variant_count,
            SUM(cbv.booster_weight) AS total_variant_weight
        FROM chaos_booster_variants cbv
        {where_clause}
        GROUP BY cbv.set_code, cbv.booster_name
        ORDER BY cbv.set_code ASC, cbv.booster_name ASC
        """,
        params,
    )

    rows = cursor.fetchall()
    conn.close()

    packs = []
    selected_chaos_pack_types = get_selected_chaos_pack_types(config)

    for row in rows:
        booster_name_raw = row["booster_name"]
        booster_type = normalize_booster_type_for_filter(booster_name_raw)

        if booster_type not in ALLOWED_CHAOS_BOOSTER_TYPES:
            continue

        if booster_type not in selected_chaos_pack_types:
            continue

        art_info = get_chaos_pack_art_info(row["set_code"], booster_name_raw, static_folder)

        packs.append({
            "set_code": row["set_code"],
            "booster_name": booster_name_raw,
            "display_name": art_info["display_name"],
            "image_path": art_info["image_path"],
            "image_src": url_for("static", filename=art_info["image_path"]),
            "is_fallback": int(art_info["is_fallback"] or 0),
            "variant_count": int(row["variant_count"] or 0),
            "total_variant_weight": float(row["total_variant_weight"] or 0),
            "booster_type": booster_type,
        })

    return packs


def get_eligible_chaos_packs_for_spin(static_folder):
    packs = get_eligible_chaos_packs(static_folder)
    config = get_config()

    if config.get("allow_repeats") != "0":
        return packs

    opened_keys = get_chaos_opened_pack_keys()

    filtered_packs = []
    for pack in packs:
        set_code = (pack["set_code"] or "").strip().upper()
        booster_name = (pack["booster_name"] or "").strip().lower()

        variants = get_chaos_pack_variants(set_code, booster_name)

        has_unused_variant = False
        for variant in variants:
            pack_key = (
                set_code,
                booster_name,
                int(variant["booster_index"] or 0),
            )

            if pack_key not in opened_keys:
                has_unused_variant = True
                break

        if has_unused_variant:
            filtered_packs.append(pack)

    return filtered_packs


def get_chaos_booster_variant_contents(set_code, booster_name, booster_index):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            set_code,
            booster_name,
            booster_index,
            sheet_name,
            sheet_picks
        FROM chaos_booster_variant_contents
        WHERE set_code = ?
          AND booster_name = ?
          AND booster_index = ?
        ORDER BY sheet_name ASC
        """,
        (
            (set_code or "").strip().upper(),
            (booster_name or "").strip().lower(),
            int(booster_index),
        ),
    )

    rows = cursor.fetchall()
    conn.close()

    contents = []
    for row in rows:
        contents.append({
            "set_code": row["set_code"],
            "booster_name": row["booster_name"],
            "booster_index": int(row["booster_index"] or 0),
            "sheet_name": row["sheet_name"],
            "sheet_picks": int(row["sheet_picks"] or 1),
        })

    return contents


def get_chaos_sheet_cards(set_code, booster_name, sheet_name):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            cbsc.card_uuid,
            cbsc.card_weight,
            cc.card_name,
            cc.rarity,
            cc.type_line,
            cc.image_url,
            cc.scryfall_id,
            cc.collector_number
        FROM chaos_booster_sheet_cards cbsc
        INNER JOIN chaos_cards cc
            ON cc.card_uuid = cbsc.card_uuid
        WHERE cbsc.set_code = ?
          AND cbsc.booster_name = ?
          AND cbsc.sheet_name = ?
          AND cc.is_booster = 1
        """,
        (
            (set_code or "").strip().upper(),
            (booster_name or "").strip().lower(),
            sheet_name,
        ),
    )

    rows = cursor.fetchall()
    conn.close()

    cards = []
    for row in rows:
        cards.append({
            "card_uuid": row["card_uuid"],
            "card_weight": float(row["card_weight"] or 1),
            "card_name": row["card_name"],
            "rarity": row["rarity"],
            "type_line": row["type_line"],
            "image_url": row["image_url"],
            "scryfall_id": row["scryfall_id"],
            "collector_number": row["collector_number"],
        })

    return cards


def get_chaos_sheet_info(set_code, booster_name, sheet_name):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            set_code,
            booster_name,
            sheet_name,
            sheet_is_foil,
            sheet_has_balance_colors,
            sheet_total_weight
        FROM chaos_booster_sheets
        WHERE set_code = ?
          AND booster_name = ?
          AND sheet_name = ?
        LIMIT 1
        """,
        (
            (set_code or "").strip().upper(),
            (booster_name or "").strip().lower(),
            sheet_name,
        ),
    )

    row = cursor.fetchone()
    conn.close()

    if not row:
        return {
            "set_code": (set_code or "").strip().upper(),
            "booster_name": (booster_name or "").strip().lower(),
            "sheet_name": sheet_name,
            "sheet_is_foil": 0,
            "sheet_has_balance_colors": 0,
            "sheet_total_weight": 0.0,
        }

    return {
        "set_code": row["set_code"],
        "booster_name": row["booster_name"],
        "sheet_name": row["sheet_name"],
        "sheet_is_foil": int(row["sheet_is_foil"] or 0),
        "sheet_has_balance_colors": int(row["sheet_has_balance_colors"] or 0),
        "sheet_total_weight": float(row["sheet_total_weight"] or 0),
    }


def open_chaos_pack_once(set_code, booster_name, booster_index, write_debug_log_fn):
    variant_contents = get_chaos_booster_variant_contents(set_code, booster_name, booster_index)
    opened_cards = []

    for content_row in variant_contents:
        sheet_name = content_row["sheet_name"]
        sheet_picks = int(content_row["sheet_picks"] or 1)

        sheet_info = get_chaos_sheet_info(set_code, booster_name, sheet_name)
        sheet_cards = get_chaos_sheet_cards(set_code, booster_name, sheet_name)
        if not sheet_cards:
            continue

        available_cards = list(sheet_cards)

        for _ in range(sheet_picks):
            chosen_card = choose_weighted_row(available_cards, "card_weight")
            if not chosen_card:
                break

            booster_type = normalize_booster_type_for_filter(booster_name)

            if (
                CHAOS_DUPLICATE_CONTROL_ENABLED
                and booster_type in CHAOS_DUPLICATE_CONTROL_TYPES
            ):
                existing_card_names = {
                    (c.get("card_name") or "").strip().lower()
                    for c in opened_cards
                }

                chosen_card_name = (chosen_card.get("card_name") or "").strip().lower()

                if chosen_card_name in existing_card_names:
                    write_debug_log_fn(
                        f"CHAOS DUPLICATE DETECTED | card={chosen_card['card_name']} | booster={booster_name}"
                    )

                    if random.random() < CHAOS_DUPLICATE_REROLL_CHANCE:
                        write_debug_log_fn(
                            f"CHAOS DUPLICATE REROLL ATTEMPT | card={chosen_card['card_name']}"
                        )

                        reroll_attempts = 0
                        replacement_card = chosen_card

                        while reroll_attempts < CHAOS_DUPLICATE_MAX_REROLLS:
                            reroll_attempts += 1

                            candidate = choose_weighted_row(available_cards, "card_weight")
                            if not candidate:
                                break

                            candidate_name = (candidate.get("card_name") or "").strip().lower()

                            if candidate_name not in existing_card_names:
                                replacement_card = candidate
                                write_debug_log_fn(
                                    f"CHAOS DUPLICATE REROLL SUCCESS | old_card={chosen_card['card_name']} | new_card={candidate['card_name']} | attempts={reroll_attempts}"
                                )
                                break

                        chosen_card = replacement_card
                    else:
                        write_debug_log_fn(
                            f"CHAOS DUPLICATE KEPT | card={chosen_card['card_name']} | reason=chance_roll_failed"
                        )

            opened_cards.append({
                "set_code": (set_code or "").strip().upper(),
                "booster_name": (booster_name or "").strip().lower(),
                "booster_index": int(booster_index),
                "sheet_name": sheet_name,
                "sheet_is_foil": int(sheet_info["sheet_is_foil"] or 0),
                "sheet_has_balance_colors": int(sheet_info["sheet_has_balance_colors"] or 0),
                "sheet_total_weight": float(sheet_info["sheet_total_weight"] or 0),
                "card_uuid": chosen_card["card_uuid"],
                "card_name": chosen_card["card_name"],
                "rarity": chosen_card["rarity"],
                "type_line": chosen_card["type_line"],
                "image_url": chosen_card["image_url"],
                "scryfall_id": chosen_card["scryfall_id"],
                "collector_number": chosen_card["collector_number"],
            })

            write_debug_log_fn(
                f"CHAOS PACK PICK | set={set_code} | booster={booster_name} | booster_index={booster_index} | "
                f"sheet={sheet_name} | foil={sheet_info['sheet_is_foil']} | "
                f"card={chosen_card['card_name']} | rarity={chosen_card['rarity']}"
            )

    return opened_cards


def open_chaos_pack_with_bonus_rule(set_code, booster_name, booster_index, write_debug_log_fn):
    first_pack_cards = open_chaos_pack_once(set_code, booster_name, booster_index, write_debug_log_fn)
    all_cards = list(first_pack_cards)

    bonus_pack_opened = False

    if len(first_pack_cards) < 11:
        bonus_pack_opened = True
        second_pack_cards = open_chaos_pack_once(set_code, booster_name, booster_index, write_debug_log_fn)
        all_cards.extend(second_pack_cards)

    return {
        "cards": all_cards,
        "bonus_pack_opened": bonus_pack_opened,
        "total_cards": len(all_cards),
    }


def set_chaos_session_state(state_key, state_value):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO chaos_session_state (state_key, state_value)
        VALUES (?, ?)
        ON CONFLICT(state_key) DO UPDATE SET state_value = excluded.state_value
        """,
        (
            (state_key or "").strip(),
            json.dumps(state_value),
        ),
    )

    conn.commit()
    conn.close()


def get_chaos_session_state(state_key, default_value=None):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT state_value
        FROM chaos_session_state
        WHERE state_key = ?
        """,
        ((state_key or "").strip(),),
    )

    row = cursor.fetchone()
    conn.close()

    if not row:
        return default_value

    try:
        return json.loads(row["state_value"])
    except Exception:
        return default_value


def clear_chaos_session_state(state_key):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        DELETE FROM chaos_session_state
        WHERE state_key = ?
        """,
        ((state_key or "").strip(),),
    )

    conn.commit()
    conn.close()


def get_pending_chaos_spin_result():
    return get_chaos_session_state("pending_spin_result", default_value=None)


def is_chaos_basic_land_card(card_entry):
    type_line = (card_entry.get("type_line") or "").strip().lower()
    card_name = (card_entry.get("card_name") or "").strip().lower()

    if "basic land" in type_line:
        return True

    return card_name in {
        "plains",
        "island",
        "swamp",
        "mountain",
        "forest",
        "wastes",
        "snow-covered plains",
        "snow-covered island",
        "snow-covered swamp",
        "snow-covered mountain",
        "snow-covered forest",
        "snow-covered wastes",
    }


def is_chaos_wildcard_slot(card_entry):
    sheet_name = (card_entry.get("sheet_name") or "").strip().lower()
    type_line = (card_entry.get("type_line") or "").strip().lower()

    wildcard_terms = [
        "wildcard",
        "list",
        "archive",
        "mystical",
        "bonus",
        "boosterfun",
        "booster_fun",
        "showcase",
        "special",
        "source",
        "story",
        "guest",
        "commander",
        "masterpiece",
        "expedition",
        "invention",
        "breaking news",
        "big score",
        "enchanting tales",
        "multiverse legends",
        "retro",
    ]

    for term in wildcard_terms:
        if term in sheet_name:
            return True

    if any(term in type_line for term in ["token", "emblem", "card", "art series"]):
        return True

    return False


def get_chaos_pack_sort_bucket(card_entry, booster_name):
    rarity = (card_entry.get("rarity") or "").strip().lower()
    sheet_is_foil = int(card_entry.get("sheet_is_foil") or 0)
    booster_key = normalize_chaos_booster_key(booster_name)

    if is_chaos_basic_land_card(card_entry):
        return "land"

    if is_chaos_wildcard_slot(card_entry):
        return "wildcard"

    if booster_key != "collector" and sheet_is_foil == 1:
        return "foil"

    if rarity == "common":
        return "common"

    if rarity == "uncommon":
        return "uncommon"

    if rarity in {"rare", "mythic", "mythic rare"}:
        return "rare_mythic"

    if sheet_is_foil == 1:
        return "foil"

    return "wildcard"


def get_chaos_booster_sort_profile(booster_name):
    booster_key = normalize_chaos_booster_key(booster_name)

    if booster_key in {"play", "draft"}:
        return ["common", "uncommon", "rare_mythic", "wildcard", "foil", "land"]

    if booster_key == "collector":
        return ["common", "uncommon", "land", "wildcard", "rare_mythic", "foil"]

    if booster_key == "set":
        return ["land", "common", "uncommon", "wildcard", "rare_mythic", "foil"]

    if booster_key == "jumpstart":
        return None

    return ["common", "uncommon", "rare_mythic", "wildcard", "foil", "land"]


def get_chaos_bucket_secondary_sort_key(card_entry):
    rarity = (card_entry.get("rarity") or "").strip().lower()
    card_name = (card_entry.get("card_name") or "").strip().lower()
    collector_number = (card_entry.get("collector_number") or "").strip().lower()

    rarity_rank = {
        "common": 0,
        "uncommon": 1,
        "rare": 2,
        "mythic": 3,
        "mythic rare": 3,
    }.get(rarity, 9)

    return (
        rarity_rank,
        card_name,
        collector_number,
    )


def sort_opened_chaos_pack_cards(cards, booster_name, write_debug_log_fn):
    if not cards:
        return []

    sort_profile = get_chaos_booster_sort_profile(booster_name)

    if sort_profile is None:
        return list(cards)

    bucket_rank = {bucket_name: index for index, bucket_name in enumerate(sort_profile)}

    def sort_key(card_entry):
        bucket_name = get_chaos_pack_sort_bucket(card_entry, booster_name)
        primary_rank = bucket_rank.get(bucket_name, 999)
        secondary_key = get_chaos_bucket_secondary_sort_key(card_entry)

        return (primary_rank, secondary_key)

    sorted_cards = sorted(cards, key=sort_key)

    write_debug_log_fn(
        f"CHAOS PACK SORT | booster={booster_name} | "
        f"profile={' > '.join(sort_profile)} | "
        f"before={len(cards)} | after={len(sorted_cards)}"
    )

    for card_entry in sorted_cards:
        write_debug_log_fn(
            f"CHAOS PACK SORT CARD | booster={booster_name} | "
            f"bucket={get_chaos_pack_sort_bucket(card_entry, booster_name)} | "
            f"sheet={card_entry.get('sheet_name')} | "
            f"foil={card_entry.get('sheet_is_foil')} | "
            f"rarity={card_entry.get('rarity')} | "
            f"card={card_entry.get('card_name')}"
        )

    return sorted_cards


def choose_random_eligible_chaos_pack_variant(static_folder):
    eligible_packs = get_eligible_chaos_packs_for_spin(static_folder)

    if not eligible_packs:
        return None

    chosen_pack = random.choice(eligible_packs)

    variants = get_chaos_pack_variants(
        chosen_pack["set_code"],
        chosen_pack["booster_name"],
    )

    config = get_config()
    if config.get("allow_repeats") == "0":
        opened_keys = get_chaos_opened_pack_keys()
        variants = [
            variant for variant in variants
            if (
                (variant["set_code"] or "").strip().upper(),
                (variant["booster_name"] or "").strip().lower(),
                int(variant["booster_index"] or 0),
            ) not in opened_keys
        ]

    chosen_variant = choose_weighted_row(variants, "booster_weight")

    if not chosen_variant:
        return None

    return {
        "pack": chosen_pack,
        "variant": chosen_variant,
    }

def build_opened_chaos_pack_state(
    set_code,
    booster_name,
    booster_index,
    pack_display_name,
    write_debug_log_fn,
):
    open_result = open_chaos_pack_with_bonus_rule(
        set_code,
        booster_name,
        booster_index,
        write_debug_log_fn,
    )

    cards = open_result["cards"]

    if not cards:
        raise ValueError("Chaos Draft pack opened but no cards were generated.")

    cards = sort_opened_chaos_pack_cards(cards, booster_name, write_debug_log_fn)

    pack_tracking_code = build_pack_tracking_code(
        set_code,
        booster_name,
        booster_index,
    )

    opened_pack_state = {
        "set_code": (set_code or "").strip().upper(),
        "booster_name": (booster_name or "").strip().lower(),
        "booster_index": int(booster_index),
        "display_name": (pack_display_name or "").strip(),
        "pack_tracking_code": pack_tracking_code,
        "bonus_pack_opened": bool(open_result.get("bonus_pack_opened")),
        "total_cards": len(cards),
        "cards": cards,
    }

    set_chaos_session_state("pending_opened_pack", opened_pack_state)

    record_chaos_pack_history(
        set_code,
        booster_name,
        booster_index,
        pack_display_name,
    )

    return opened_pack_state

def build_chaos_spin_result(static_folder, write_debug_log_fn=None):
    eligible_packs = get_eligible_chaos_packs_for_spin(static_folder)

    if not eligible_packs:
        return None

    shuffled_packs = list(eligible_packs)
    random.shuffle(shuffled_packs)

    display_packs = shuffled_packs[:15]

    if not display_packs:
        return None

    winning_stop_index = random.randint(0, len(display_packs) - 1)
    winning_pack = display_packs[winning_stop_index]

    variants = get_chaos_pack_variants(
        winning_pack["set_code"],
        winning_pack["booster_name"],
    )

    config = get_config()
    if config.get("allow_repeats") == "0":
        opened_keys = get_chaos_opened_pack_keys()
        variants = [
            variant for variant in variants
            if (
                (variant["set_code"] or "").strip().upper(),
                (variant["booster_name"] or "").strip().lower(),
                int(variant["booster_index"] or 0),
            ) not in opened_keys
        ]

    chosen_variant = choose_weighted_row(variants, "booster_weight")

    if not chosen_variant:
        return None

    opened_pack_state = build_opened_chaos_pack_state(
        chosen_variant["set_code"],
        chosen_variant["booster_name"],
        chosen_variant["booster_index"],
        winning_pack["display_name"],
        write_debug_log_fn or (lambda message: None),
    )

    spin_result = {
        "display_packs": display_packs,
        "winning_pack": {
            "set_code": winning_pack["set_code"],
            "booster_name": winning_pack["booster_name"],
            "display_name": winning_pack["display_name"],
            "image_src": winning_pack["image_src"],
            "variant_count": winning_pack.get("variant_count", 0),
            "total_variant_weight": winning_pack.get("total_variant_weight", 0),
        },
        "chosen_variant": {
            "set_code": chosen_variant["set_code"],
            "booster_name": chosen_variant["booster_name"],
            "booster_index": chosen_variant["booster_index"],
            "booster_weight": chosen_variant["booster_weight"],
        },
        "winning_stop_index": winning_stop_index,
        "opened_pack_ready": True,
        "opened_pack_total_cards": int(opened_pack_state.get("total_cards") or 0),
        "bonus_pack_opened": bool(opened_pack_state.get("bonus_pack_opened")),
    }

    set_chaos_session_state("pending_spin_result", spin_result)

    return spin_result

def parse_custom_pack_decklist_text(decklist_text):
    parsed_cards = []

    for raw_line in (decklist_text or "").splitlines():
        line = (raw_line or "").strip()

        if not line:
            continue

        if line.startswith("#") or line.startswith("//"):
            continue

        if line.lower().startswith("sideboard"):
            continue

        if line.lower().startswith("sb:"):
            line = line[3:].strip()

        quantity = 1
        quantity_match = re.match(r"^(\d+)\s*x?\s+(.+)$", line, flags=re.IGNORECASE)

        if quantity_match:
            try:
                quantity = int(quantity_match.group(1))
            except ValueError:
                quantity = 1

            line = quantity_match.group(2).strip()

        requested_set_code = ""
        requested_collector_number = ""

        specific_printing_match = re.match(
            r"^(.*?)\s+\(([A-Za-z0-9]{2,8})\)\s+([A-Za-z0-9\-]+)\s*$",
            line,
        )

        if specific_printing_match:
            line = specific_printing_match.group(1).strip()
            requested_set_code = specific_printing_match.group(2).strip().upper()
            requested_collector_number = specific_printing_match.group(3).strip()
        else:
            set_only_match = re.match(
                r"^(.*?)\s+\(([A-Za-z0-9]{2,8})\)\s*$",
                line,
            )

            if set_only_match:
                line = set_only_match.group(1).strip()
                requested_set_code = set_only_match.group(2).strip().upper()

            bracket_set_match = re.match(
                r"^(.*?)\s+\[([A-Za-z0-9]{2,8})\]\s*$",
                line,
            )

            if bracket_set_match:
                line = bracket_set_match.group(1).strip()
                requested_set_code = bracket_set_match.group(2).strip().upper()

        # Remove Arena-style trailing annotations if present.
        line = re.sub(r"\s+\*\w+\*$", "", line).strip()

        if not line:
            continue

        if quantity < 1:
            quantity = 1

        if quantity > 99:
            quantity = 99

        parsed_cards.append({
            "card_name": line,
            "quantity": quantity,
            "set_code": requested_set_code,
            "collector_number": requested_collector_number,
        })

    return parsed_cards

def resolve_custom_pack_card_by_name(
    card_name,
    preferred_set_code=None,
    requested_set_code=None,
    requested_collector_number=None,
):
    clean_name = (card_name or "").strip()
    clean_preferred_set_code = (preferred_set_code or "").strip().upper()
    clean_requested_set_code = (requested_set_code or "").strip().upper()
    clean_collector_number = (requested_collector_number or "").strip()

    if not clean_name:
        return None

    conn = get_db_connection()
    cursor = conn.cursor()

    if clean_requested_set_code and clean_collector_number:
        cursor.execute(
            """
            SELECT
                card_uuid,
                set_code,
                card_name,
                rarity,
                type_line,
                image_url,
                scryfall_id,
                collector_number
            FROM chaos_cards
            WHERE set_code = ?
              AND LOWER(COALESCE(collector_number, '')) = LOWER(?)
            ORDER BY
                CASE WHEN LOWER(card_name) = LOWER(?) THEN 0 ELSE 1 END,
                is_booster DESC,
                collector_number ASC
            LIMIT 1
            """,
            (
                clean_requested_set_code,
                clean_collector_number,
                clean_name,
            ),
        )

        row = cursor.fetchone()
        if row:
            conn.close()
            return row

    if clean_requested_set_code:
        cursor.execute(
            """
            SELECT
                card_uuid,
                set_code,
                card_name,
                rarity,
                type_line,
                image_url,
                scryfall_id,
                collector_number
            FROM chaos_cards
            WHERE set_code = ?
              AND LOWER(card_name) = LOWER(?)
            ORDER BY is_booster DESC, collector_number ASC
            LIMIT 1
            """,
            (
                clean_requested_set_code,
                clean_name,
            ),
        )

        row = cursor.fetchone()
        if row:
            conn.close()
            return row

    if clean_preferred_set_code:
        cursor.execute(
            """
            SELECT
                card_uuid,
                set_code,
                card_name,
                rarity,
                type_line,
                image_url,
                scryfall_id,
                collector_number
            FROM chaos_cards
            WHERE set_code = ?
              AND LOWER(card_name) = LOWER(?)
            ORDER BY is_booster DESC, collector_number ASC
            LIMIT 1
            """,
            (
                clean_preferred_set_code,
                clean_name,
            ),
        )

        row = cursor.fetchone()
        if row:
            conn.close()
            return row

    cursor.execute(
        """
        SELECT
            card_uuid,
            set_code,
            card_name,
            rarity,
            type_line,
            image_url,
            scryfall_id,
            collector_number
        FROM chaos_cards
        WHERE LOWER(card_name) = LOWER(?)
        ORDER BY set_code ASC, is_booster DESC, collector_number ASC
        LIMIT 1
        """,
        (clean_name,),
    )

    row = cursor.fetchone()
    conn.close()
    return row

def get_custom_pack_populate_options_for_set(set_code):
    clean_set_code = (set_code or "").strip().upper()

    if not clean_set_code:
        return []

    config = get_config()
    selected_pack_types = get_selected_chaos_pack_types(config)
    booster_label_map = get_chaos_pack_type_label_map()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            booster_name,
            COUNT(*) AS variant_count,
            SUM(booster_weight) AS total_variant_weight
        FROM chaos_booster_variants
        WHERE set_code = ?
        GROUP BY booster_name
        ORDER BY booster_name ASC
        """,
        (clean_set_code,),
    )

    rows = cursor.fetchall()
    conn.close()

    options = []

    for row in rows:
        booster_name = (row["booster_name"] or "").strip().lower()
        booster_type = normalize_booster_type_for_filter(booster_name)

        if booster_type not in ALLOWED_CHAOS_BOOSTER_TYPES:
            continue

        if booster_type not in selected_pack_types:
            continue

        display_label = booster_label_map.get(
            booster_type,
            " ".join(word.capitalize() for word in booster_name.split()) if booster_name else "Booster"
        )

        options.append({
            "set_code": clean_set_code,
            "booster_name": booster_name,
            "booster_type": booster_type,
            "label": display_label,
            "variant_count": int(row["variant_count"] or 0),
            "total_variant_weight": float(row["total_variant_weight"] or 0),
        })

    return options


def build_custom_pack_decklist_line(card_entry):
    card_name = (card_entry.get("card_name") or "").strip()
    set_code = (card_entry.get("set_code") or "").strip().upper()
    collector_number = (card_entry.get("collector_number") or "").strip()

    if not card_name:
        return ""

    if set_code and collector_number:
        return f"1 {card_name} ({set_code}) {collector_number}"

    if set_code:
        return f"1 {card_name} ({set_code})"

    return f"1 {card_name}"


def populate_custom_pack_decklist_from_booster(set_code, booster_name, existing_decklist_text, write_debug_log_fn=None):
    clean_set_code = (set_code or "").strip().upper()
    clean_booster_name = (booster_name or "").strip().lower()

    if not clean_set_code:
        return {
            "ok": False,
            "message": "Set Code is required.",
        }

    if not clean_booster_name:
        return {
            "ok": False,
            "message": "Booster type is required.",
        }

    available_options = get_custom_pack_populate_options_for_set(clean_set_code)
    allowed_booster_names = {
        (option.get("booster_name") or "").strip().lower()
        for option in available_options
    }

    if clean_booster_name not in allowed_booster_names:
        return {
            "ok": False,
            "message": "That booster type is not available for this set with the current Chaos Draft filters.",
        }

    variants = get_chaos_pack_variants(clean_set_code, clean_booster_name)
    chosen_variant = choose_weighted_row(variants, "booster_weight")

    if not chosen_variant:
        return {
            "ok": False,
            "message": "No eligible variants were found for that set and booster type.",
        }

    open_result = open_chaos_pack_with_bonus_rule(
        chosen_variant["set_code"],
        chosen_variant["booster_name"],
        chosen_variant["booster_index"],
        write_debug_log_fn or (lambda message: None),
    )

    generated_cards = open_result.get("cards") or []

    if not generated_cards:
        return {
            "ok": False,
            "message": "The selected booster generated no cards.",
        }

    generated_cards = sort_opened_chaos_pack_cards(
        generated_cards,
        clean_booster_name,
        write_debug_log_fn or (lambda message: None),
    )

    existing_lines = []
    for raw_line in (existing_decklist_text or "").splitlines():
        clean_line = (raw_line or "").strip()
        if clean_line:
            existing_lines.append(clean_line)

    parsed_existing = parse_custom_pack_decklist_text("\n".join(existing_lines))
    existing_total = sum(int(item.get("quantity") or 1) for item in parsed_existing)

    target_total = len(generated_cards)
    missing_count = max(0, target_total - existing_total)

    if missing_count <= 0:
        return {
            "ok": True,
            "message": "Decklist already has enough cards for this booster.",
            "decklist_text": "\n".join(existing_lines).strip(),
            "added_count": 0,
            "generated_pack_total": target_total,
            "booster_name": clean_booster_name,
            "set_code": clean_set_code,
        }

    existing_names = {
        (item.get("card_name") or "").strip().lower()
        for item in parsed_existing
    }

    added_lines = []

    # First pass: prefer cards not already present by name.
    for card in generated_cards:
        if len(added_lines) >= missing_count:
            break

        card_name = (card.get("card_name") or "").strip()
        if not card_name:
            continue

        if card_name.lower() in existing_names:
            continue

        decklist_line = build_custom_pack_decklist_line(card)
        if decklist_line:
            added_lines.append(decklist_line)

    # Second pass: if the generated pack overlaps with existing cards, allow duplicates to fill count.
    if len(added_lines) < missing_count:
        for card in generated_cards:
            if len(added_lines) >= missing_count:
                break

            decklist_line = build_custom_pack_decklist_line(card)
            if decklist_line:
                added_lines.append(decklist_line)

    combined_lines = list(existing_lines)
    combined_lines.extend(added_lines[:missing_count])

    decklist_text = "\n".join(combined_lines).strip()

    if write_debug_log_fn:
        write_debug_log_fn(
            f"CUSTOM PACK POPULATE | set={clean_set_code} | booster={clean_booster_name} | "
            f"target_total={target_total} | existing_total={existing_total} | added={len(added_lines[:missing_count])}"
        )

    return {
        "ok": True,
        "message": f"Populated {len(added_lines[:missing_count])} card(s) from {clean_set_code} {clean_booster_name}.",
        "decklist_text": decklist_text,
        "added_count": len(added_lines[:missing_count]),
        "generated_pack_total": target_total,
        "booster_name": clean_booster_name,
        "set_code": clean_set_code,
    }

def create_custom_pack_preview_for_manage_packs(set_code, pack_name, decklist_text, write_debug_log_fn=None):
    clean_set_code = (set_code or "").strip().upper()
    clean_pack_name = (pack_name or "").strip()

    if not clean_set_code:
        return {
            "ok": False,
            "message": "Set Code is required.",
        }

    if not clean_pack_name:
        return {
            "ok": False,
            "message": "Pack Name is required.",
        }

    parsed_lines = parse_custom_pack_decklist_text(decklist_text)

    if not parsed_lines:
        return {
            "ok": False,
            "message": "Decklist text did not contain any cards.",
        }

    cards = []
    unresolved_cards = []

    for parsed_line in parsed_lines:
        requested_card_name = parsed_line["card_name"]
        quantity = int(parsed_line["quantity"] or 1)

        resolved_row = resolve_custom_pack_card_by_name(
            requested_card_name,
            preferred_set_code=clean_set_code,
            requested_set_code=parsed_line.get("set_code"),
            requested_collector_number=parsed_line.get("collector_number"),
        )

        if not resolved_row:
            unresolved_cards.append(requested_card_name)
            continue

        for _ in range(quantity):
            cards.append({
                "set_code": (resolved_row["set_code"] or clean_set_code or "").strip().upper(),
                "booster_name": "custom",
                "booster_index": 0,
                "sheet_name": "custom_import",
                "sheet_is_foil": 0,
                "sheet_has_balance_colors": 0,
                "sheet_total_weight": 0,
                "card_uuid": resolved_row["card_uuid"],
                "card_name": resolved_row["card_name"],
                "rarity": resolved_row["rarity"],
                "type_line": resolved_row["type_line"],
                "image_url": resolved_row["image_url"],
                "scryfall_id": resolved_row["scryfall_id"],
                "collector_number": resolved_row["collector_number"],
            })

    if unresolved_cards:
        unresolved_preview = ", ".join(unresolved_cards[:10])
        if len(unresolved_cards) > 10:
            unresolved_preview += f", and {len(unresolved_cards) - 10} more"

        return {
            "ok": False,
            "message": f"Could not resolve these card name(s): {unresolved_preview}",
        }

    if not cards:
        return {
            "ok": False,
            "message": "No cards could be resolved from the decklist.",
        }

    pack_tracking_code = build_pack_tracking_code(
        clean_set_code,
        "custom",
        0,
    )

    display_name = f"{clean_pack_name} ({clean_set_code})"

    preview_pack_state = {
        "set_code": clean_set_code,
        "booster_name": "custom",
        "booster_index": 0,
        "display_name": display_name,
        "pack_display_name": display_name,
        "pack_tracking_code": pack_tracking_code,
        "bonus_pack_opened": False,
        "total_cards": len(cards),
        "cards": cards,
        "manage_pack_preview": True,
        "custom_pack": True,
    }

    set_chaos_session_state("pending_manage_pack_preview", preview_pack_state)

    if write_debug_log_fn:
        write_debug_log_fn(
            f"MANAGE PACKS CUSTOM PREVIEW | tracking_code={pack_tracking_code} | "
            f"set={clean_set_code} | pack_name={clean_pack_name} | cards={len(cards)}"
        )

    return {
        "ok": True,
        "message": "Custom pack generated. Review it before saving.",
        "pack_tracking_code": pack_tracking_code,
        "set_code": clean_set_code,
        "booster_name": "custom",
        "booster_index": 0,
        "pack_display_name": display_name,
        "total_cards": len(cards),
        "bonus_pack_opened": False,
    }

def search_manage_pack_options(static_folder, search_text, limit=30):
    search_value = (search_text or "").strip().lower()

    if not search_value:
        return []

    eligible_packs = get_eligible_chaos_packs(static_folder)
    matched_packs = []

    for pack in eligible_packs:
        set_code = (pack.get("set_code") or "").strip().upper()
        booster_name = (pack.get("booster_name") or "").strip().lower()
        display_name = (pack.get("display_name") or "").strip()
        booster_type = (pack.get("booster_type") or "").strip().lower()

        haystack = " ".join([
            set_code.lower(),
            booster_name,
            display_name.lower(),
            booster_type,
        ])

        if search_value not in haystack:
            continue

        matched_packs.append({
            "set_code": set_code,
            "booster_name": booster_name,
            "display_name": display_name,
            "image_src": pack.get("image_src") or "",
            "variant_count": int(pack.get("variant_count") or 0),
            "total_variant_weight": float(pack.get("total_variant_weight") or 0),
            "booster_type": booster_type,
        })

        if len(matched_packs) >= int(limit or 30):
            break

    return matched_packs

def create_random_pack_preview_for_manage_packs(static_folder, write_debug_log_fn=None):
    chosen_result = choose_random_eligible_chaos_pack_variant(static_folder)

    if not chosen_result:
        return {
            "ok": False,
            "message": "No eligible Chaos Draft packs were available to add.",
        }

    chosen_pack = chosen_result["pack"]
    chosen_variant = chosen_result["variant"]

    open_result = open_chaos_pack_with_bonus_rule(
        chosen_variant["set_code"],
        chosen_variant["booster_name"],
        chosen_variant["booster_index"],
        write_debug_log_fn or (lambda message: None),
    )

    cards = open_result["cards"]

    if not cards:
        return {
            "ok": False,
            "message": "The selected pack generated no cards.",
        }

    cards = sort_opened_chaos_pack_cards(
        cards,
        chosen_variant["booster_name"],
        write_debug_log_fn or (lambda message: None),
    )

    pack_tracking_code = build_pack_tracking_code(
        chosen_variant["set_code"],
        chosen_variant["booster_name"],
        chosen_variant["booster_index"],
    )

    preview_pack_state = {
        "set_code": (chosen_variant["set_code"] or "").strip().upper(),
        "booster_name": (chosen_variant["booster_name"] or "").strip().lower(),
        "booster_index": int(chosen_variant["booster_index"] or 0),
        "display_name": (chosen_pack["display_name"] or "").strip(),
        "pack_display_name": (chosen_pack["display_name"] or "").strip(),
        "pack_tracking_code": pack_tracking_code,
        "bonus_pack_opened": bool(open_result.get("bonus_pack_opened")),
        "total_cards": len(cards),
        "cards": cards,
        "manage_pack_preview": True,
    }

    set_chaos_session_state("pending_manage_pack_preview", preview_pack_state)

    if write_debug_log_fn:
        write_debug_log_fn(
            f"MANAGE PACKS RANDOM PREVIEW | tracking_code={pack_tracking_code} | "
            f"set={chosen_variant['set_code']} | booster={chosen_variant['booster_name']} | "
            f"booster_index={chosen_variant['booster_index']} | cards={len(cards)}"
        )

    return {
        "ok": True,
        "message": "Random pack generated. Review it before saving.",
        "pack_tracking_code": pack_tracking_code,
        "set_code": chosen_variant["set_code"],
        "booster_name": chosen_variant["booster_name"],
        "booster_index": chosen_variant["booster_index"],
        "pack_display_name": chosen_pack["display_name"],
        "total_cards": len(cards),
        "bonus_pack_opened": bool(open_result.get("bonus_pack_opened")),
    }

def create_specific_pack_preview_for_manage_packs(set_code, booster_name, static_folder, write_debug_log_fn=None):
    clean_set_code = (set_code or "").strip().upper()
    clean_booster_name = (booster_name or "").strip().lower()

    if not clean_set_code or not clean_booster_name:
        return {
            "ok": False,
            "message": "Set code and booster name are required.",
        }

    eligible_packs = get_eligible_chaos_packs(static_folder)

    chosen_pack = None
    for pack in eligible_packs:
        if (
            (pack.get("set_code") or "").strip().upper() == clean_set_code
            and (pack.get("booster_name") or "").strip().lower() == clean_booster_name
        ):
            chosen_pack = pack
            break

    if not chosen_pack:
        return {
            "ok": False,
            "message": "The selected pack is not currently eligible.",
        }

    variants = get_chaos_pack_variants(clean_set_code, clean_booster_name)
    chosen_variant = choose_weighted_row(variants, "booster_weight")

    if not chosen_variant:
        return {
            "ok": False,
            "message": "No eligible variants were found for the selected pack.",
        }

    open_result = open_chaos_pack_with_bonus_rule(
        chosen_variant["set_code"],
        chosen_variant["booster_name"],
        chosen_variant["booster_index"],
        write_debug_log_fn or (lambda message: None),
    )

    cards = open_result["cards"]

    if not cards:
        return {
            "ok": False,
            "message": "The selected pack generated no cards.",
        }

    cards = sort_opened_chaos_pack_cards(
        cards,
        chosen_variant["booster_name"],
        write_debug_log_fn or (lambda message: None),
    )

    pack_tracking_code = build_pack_tracking_code(
        chosen_variant["set_code"],
        chosen_variant["booster_name"],
        chosen_variant["booster_index"],
    )

    preview_pack_state = {
        "set_code": (chosen_variant["set_code"] or "").strip().upper(),
        "booster_name": (chosen_variant["booster_name"] or "").strip().lower(),
        "booster_index": int(chosen_variant["booster_index"] or 0),
        "display_name": (chosen_pack["display_name"] or "").strip(),
        "pack_display_name": (chosen_pack["display_name"] or "").strip(),
        "pack_tracking_code": pack_tracking_code,
        "bonus_pack_opened": bool(open_result.get("bonus_pack_opened")),
        "total_cards": len(cards),
        "cards": cards,
        "manage_pack_preview": True,
    }

    set_chaos_session_state("pending_manage_pack_preview", preview_pack_state)

    if write_debug_log_fn:
        write_debug_log_fn(
            f"MANAGE PACKS SEARCH RANDOM PREVIEW | tracking_code={pack_tracking_code} | "
            f"set={chosen_variant['set_code']} | booster={chosen_variant['booster_name']} | "
            f"booster_index={chosen_variant['booster_index']} | cards={len(cards)}"
        )

    return {
        "ok": True,
        "message": "Pack generated. Review it before saving.",
        "pack_tracking_code": pack_tracking_code,
        "set_code": chosen_variant["set_code"],
        "booster_name": chosen_variant["booster_name"],
        "booster_index": chosen_variant["booster_index"],
        "pack_display_name": chosen_pack["display_name"],
        "total_cards": len(cards),
        "bonus_pack_opened": bool(open_result.get("bonus_pack_opened")),
    }

def build_chaos_pack_pdf_from_variant(
    set_code,
    booster_name,
    booster_index,
    pack_display_name,
    build_chaos_pack_pdf_fn,
    write_debug_log_fn,
):
    open_result = open_chaos_pack_with_bonus_rule(
        set_code,
        booster_name,
        booster_index,
        write_debug_log_fn,
    )

    cards = open_result["cards"]

    if not cards:
        raise ValueError("Chaos Draft pack opened but no cards were generated.")

    cards = sort_opened_chaos_pack_cards(cards, booster_name, write_debug_log_fn)

    record_chaos_pack_history(
        set_code,
        booster_name,
        booster_index,
        pack_display_name,
    )

    pack_tracking_code = build_pack_tracking_code(
        set_code,
        booster_name,
        booster_index,
    )

    return build_chaos_pack_pdf_fn(
        cards,
        pack_display_name,
        set_code=set_code,
        booster_name=booster_name,
        pack_tracking_code=pack_tracking_code,
    )

def save_opened_chaos_pack_to_tracking_db(opened_pack=None, campaign_id=None):
    if opened_pack is None:
        opened_pack = get_chaos_session_state("pending_opened_pack", default_value=None)

    if not opened_pack:
        return {
            "ok": False,
            "message": "No opened Chaos Draft pack is available to save.",
            "already_saved": False,
            "tracked_pack_id": None,
        }

    pack_tracking_code = (opened_pack.get("pack_tracking_code") or "").strip().upper()
    set_code = (opened_pack.get("set_code") or "").strip().upper()
    booster_name = (opened_pack.get("booster_name") or "").strip().lower()
    display_name = (opened_pack.get("display_name") or "").strip()
    cards = opened_pack.get("cards") or []

    try:
        booster_index = int(opened_pack.get("booster_index") or 0)
    except (TypeError, ValueError):
        booster_index = 0

    if not pack_tracking_code:
        return {
            "ok": False,
            "message": "Opened Chaos Draft pack does not have a tracking code.",
            "already_saved": False,
            "tracked_pack_id": None,
        }

    if not set_code or not booster_name or not display_name:
        return {
            "ok": False,
            "message": "Opened Chaos Draft pack data is incomplete.",
            "already_saved": False,
            "tracked_pack_id": None,
        }

    if not cards:
        return {
            "ok": False,
            "message": "Opened Chaos Draft pack does not contain any cards.",
            "already_saved": False,
            "tracked_pack_id": None,
        }

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    try:
        parsed_campaign_id = int(campaign_id) if campaign_id is not None else None
    except (TypeError, ValueError):
        parsed_campaign_id = None

    opened_pack["campaign_id"] = parsed_campaign_id

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT tracked_pack_id
            FROM tracked_chaos_packs
            WHERE pack_tracking_code = ?
            """,
            (pack_tracking_code,),
        )
        existing_row = cursor.fetchone()

        if existing_row:
            tracked_pack_id = int(existing_row["tracked_pack_id"])
            conn.commit()
            conn.close()

            link_tracked_pack_to_campaign(
                tracked_pack_id,
                campaign_id=parsed_campaign_id,
                campaign_enabled=True,
            )

            return {
                "ok": True,
                "message": f"Pack {pack_tracking_code} was already saved and linked to this campaign.",
                "already_saved": True,
                "tracked_pack_id": tracked_pack_id,
                "pack_tracking_code": pack_tracking_code,
            }

        cursor.execute(
            """
            INSERT INTO tracked_chaos_packs (
                pack_tracking_code,
                set_code,
                booster_name,
                booster_index,
                pack_display_name,
                total_cards,
                bonus_pack_opened,
                added_at_utc,
                last_opened_at_utc,
                opened_count,
                campaign_id,
                source_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pack_tracking_code,
                set_code,
                booster_name,
                booster_index,
                display_name,
                int(opened_pack.get("total_cards") or len(cards)),
                1 if opened_pack.get("bonus_pack_opened") else 0,
                now_utc,
                None,
                0,
                parsed_campaign_id,
                json.dumps(opened_pack),
            ),
        )

        tracked_pack_id = cursor.lastrowid

        for card_order, card in enumerate(cards, start=1):
            cursor.execute(
                """
                INSERT INTO tracked_chaos_pack_cards (
                    tracked_pack_id,
                    card_order,
                    card_uuid,
                    card_name,
                    set_code,
                    booster_name,
                    booster_index,
                    sheet_name,
                    sheet_is_foil,
                    sheet_has_balance_colors,
                    sheet_total_weight,
                    rarity,
                    type_line,
                    image_url,
                    scryfall_id,
                    collector_number
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    tracked_pack_id,
                    card_order,
                    (card.get("card_uuid") or "").strip(),
                    (card.get("card_name") or "").strip(),
                    (card.get("set_code") or set_code or "").strip().upper(),
                    (card.get("booster_name") or booster_name or "").strip().lower(),
                    int(card.get("booster_index") or booster_index),
                    (card.get("sheet_name") or "").strip(),
                    int(card.get("sheet_is_foil") or 0),
                    int(card.get("sheet_has_balance_colors") or 0),
                    float(card.get("sheet_total_weight") or 0),
                    (card.get("rarity") or "").strip(),
                    (card.get("type_line") or "").strip(),
                    (card.get("image_url") or "").strip(),
                    (card.get("scryfall_id") or "").strip(),
                    (card.get("collector_number") or "").strip(),
                ),
            )

        conn.commit()
        conn.close()

        link_tracked_pack_to_campaign(
            tracked_pack_id,
            campaign_id=parsed_campaign_id,
            campaign_enabled=True,
        )

        return {
            "ok": True,
            "message": f"Saved pack {pack_tracking_code} to the Pack Tracking Database.",
            "already_saved": False,
            "tracked_pack_id": tracked_pack_id,
            "pack_tracking_code": pack_tracking_code,
        }

    finally:
        try:
            conn.close()
        except Exception:
            pass

def get_campaign_membership_key(campaign_id=None):
    parsed_campaign_id = normalize_optional_int(campaign_id)

    if parsed_campaign_id is None:
        return "__none__"

    return str(parsed_campaign_id)


def ensure_tracked_pack_campaign_schema():
    ensure_chaos_campaign_schema()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS tracked_chaos_pack_campaigns (
            tracked_pack_campaign_id INTEGER PRIMARY KEY AUTOINCREMENT,
            tracked_pack_id INTEGER NOT NULL,
            campaign_key TEXT NOT NULL,
            campaign_id INTEGER NULL,
            campaign_enabled INTEGER NOT NULL DEFAULT 1,
            added_at_utc TEXT NOT NULL,
            UNIQUE (tracked_pack_id, campaign_key),
            FOREIGN KEY (tracked_pack_id) REFERENCES tracked_chaos_packs (tracked_pack_id)
        )
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tracked_pack_campaigns_campaign
        ON tracked_chaos_pack_campaigns (campaign_key, campaign_enabled, tracked_pack_id)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tracked_pack_campaigns_pack
        ON tracked_chaos_pack_campaigns (tracked_pack_id, campaign_key)
        """
    )

    cursor.execute("PRAGMA table_info(tracked_chaos_packs)")
    tracked_pack_columns = {row[1] for row in cursor.fetchall()}

    # Migration bridge:
    # Existing installs stored campaign membership directly on tracked_chaos_packs.campaign_id.
    # Copy that into the new membership table without changing tracking codes.
    if "campaign_id" in tracked_pack_columns:
        cursor.execute(
            """
            INSERT OR IGNORE INTO tracked_chaos_pack_campaigns (
                tracked_pack_id,
                campaign_key,
                campaign_id,
                campaign_enabled,
                added_at_utc
            )
            SELECT
                tracked_pack_id,
                CASE
                    WHEN campaign_id IS NULL THEN '__none__'
                    ELSE CAST(campaign_id AS TEXT)
                END AS campaign_key,
                campaign_id,
                COALESCE(campaign_enabled, 1),
                COALESCE(added_at_utc, strftime('%Y-%m-%d %H:%M:%S UTC', 'now'))
            FROM tracked_chaos_packs
            """
        )

    conn.commit()
    conn.close()


def link_tracked_pack_to_campaign(tracked_pack_id, campaign_id=None, campaign_enabled=True):
    ensure_tracked_pack_campaign_schema()

    parsed_tracked_pack_id = normalize_optional_int(tracked_pack_id)
    parsed_campaign_id = normalize_optional_int(campaign_id)

    if parsed_tracked_pack_id is None:
        return False

    campaign_key = get_campaign_membership_key(parsed_campaign_id)
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO tracked_chaos_pack_campaigns (
            tracked_pack_id,
            campaign_key,
            campaign_id,
            campaign_enabled,
            added_at_utc
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(tracked_pack_id, campaign_key) DO UPDATE SET
            campaign_enabled = excluded.campaign_enabled
        """,
        (
            parsed_tracked_pack_id,
            campaign_key,
            parsed_campaign_id,
            1 if campaign_enabled else 0,
            now_utc,
        ),
    )

    conn.commit()
    conn.close()

    return True


def get_campaign_pack_import_options(current_campaign_id=None):
    ensure_tracked_pack_campaign_schema()

    parsed_current_campaign_id = normalize_optional_int(current_campaign_id)
    current_campaign_key = get_campaign_membership_key(parsed_current_campaign_id)

    conn = get_db_connection()
    cursor = conn.cursor()

    options = []

    cursor.execute(
        """
        SELECT
            tpc.campaign_key,
            tpc.campaign_id,
            COALESCE(cc.campaign_name, 'No Campaign') AS campaign_name,
            COUNT(DISTINCT tpc.tracked_pack_id) AS pack_count
        FROM tracked_chaos_pack_campaigns tpc
        LEFT JOIN chaos_campaigns cc
            ON cc.campaign_id = tpc.campaign_id
        WHERE tpc.campaign_key <> ?
          AND COALESCE(tpc.campaign_enabled, 1) = 1
        GROUP BY
            tpc.campaign_key,
            tpc.campaign_id,
            COALESCE(cc.campaign_name, 'No Campaign')
        HAVING COUNT(DISTINCT tpc.tracked_pack_id) > 0
        ORDER BY
            CASE WHEN tpc.campaign_id IS NULL THEN 0 ELSE 1 END,
            campaign_name COLLATE NOCASE ASC
        """,
        (current_campaign_key,),
    )

    rows = cursor.fetchall()
    conn.close()

    for row in rows:
        options.append({
            "campaign_id": "__none__" if row["campaign_id"] is None else int(row["campaign_id"]),
            "campaign_name": row["campaign_name"] or "No Campaign",
            "pack_count": int(row["pack_count"] or 0),
        })

    return options


def get_importable_campaign_pack_rows(static_folder, source_campaign_id=None, target_campaign_id=None):
    ensure_tracked_pack_campaign_schema()

    source_campaign_key = get_campaign_membership_key(
        None if source_campaign_id in {None, "", "__none__"} else source_campaign_id
    )
    target_campaign_key = get_campaign_membership_key(target_campaign_id)

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            tcp.tracked_pack_id,
            tcp.pack_tracking_code,
            tcp.set_code,
            tcp.booster_name,
            tcp.booster_index,
            tcp.pack_display_name,
            tcp.total_cards,
            tcp.bonus_pack_opened,
            tcp.added_at_utc,
            tcp.last_opened_at_utc,
            tcp.opened_count,
            COALESCE(source_membership.campaign_enabled, 1) AS campaign_enabled,
            CASE
                WHEN target_membership.tracked_pack_id IS NOT NULL THEN 1
                ELSE 0
            END AS already_in_target
        FROM tracked_chaos_pack_campaigns source_membership
        INNER JOIN tracked_chaos_packs tcp
            ON tcp.tracked_pack_id = source_membership.tracked_pack_id
        LEFT JOIN tracked_chaos_pack_campaigns target_membership
            ON target_membership.tracked_pack_id = tcp.tracked_pack_id
           AND target_membership.campaign_key = ?
        WHERE source_membership.campaign_key = ?
          AND COALESCE(source_membership.campaign_enabled, 1) = 1
        ORDER BY tcp.added_at_utc DESC, tcp.tracked_pack_id DESC
        """,
        (
            target_campaign_key,
            source_campaign_key,
        ),
    )

    rows = cursor.fetchall()
    conn.close()

    packs = []

    for row in rows:
        set_code = (row["set_code"] or "").strip().upper()
        booster_name = (row["booster_name"] or "").strip().lower()
        art_info = get_chaos_pack_art_info(set_code, booster_name, static_folder)

        packs.append({
            "tracked_pack_id": int(row["tracked_pack_id"]),
            "pack_tracking_code": (row["pack_tracking_code"] or "").strip().upper(),
            "set_code": set_code,
            "booster_name": booster_name,
            "booster_index": int(row["booster_index"] or 0),
            "pack_display_name": (row["pack_display_name"] or "").strip(),
            "total_cards": int(row["total_cards"] or 0),
            "bonus_pack_opened": bool(row["bonus_pack_opened"]),
            "added_at_utc": row["added_at_utc"] or "",
            "last_opened_at_utc": row["last_opened_at_utc"] or "",
            "opened_count": int(row["opened_count"] or 0),
            "campaign_enabled": int(row["campaign_enabled"] or 0) == 1,
            "already_in_target": int(row["already_in_target"] or 0) == 1,
            "image_src": url_for("static", filename=art_info["image_path"]),
        })

    return packs


def import_tracked_packs_from_campaign(source_pack_ids, target_campaign_id=None):
    ensure_tracked_pack_campaign_schema()

    pack_ids = normalize_tracked_pack_id_list(source_pack_ids)

    if not pack_ids:
        return {
            "ok": False,
            "message": "No packs were selected.",
            "imported_count": 0,
            "skipped_count": 0,
        }

    target_campaign_key = get_campaign_membership_key(target_campaign_id)
    parsed_target_campaign_id = normalize_optional_int(target_campaign_id)
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    conn = get_db_connection()
    cursor = conn.cursor()

    imported_count = 0
    skipped_count = 0

    try:
        for tracked_pack_id in pack_ids:
            cursor.execute(
                """
                SELECT tracked_pack_id
                FROM tracked_chaos_packs
                WHERE tracked_pack_id = ?
                """,
                (tracked_pack_id,),
            )

            if not cursor.fetchone():
                skipped_count += 1
                continue

            cursor.execute(
                """
                SELECT tracked_pack_campaign_id
                FROM tracked_chaos_pack_campaigns
                WHERE tracked_pack_id = ?
                  AND campaign_key = ?
                """,
                (
                    tracked_pack_id,
                    target_campaign_key,
                ),
            )

            if cursor.fetchone():
                skipped_count += 1
                continue

            cursor.execute(
                """
                INSERT INTO tracked_chaos_pack_campaigns (
                    tracked_pack_id,
                    campaign_key,
                    campaign_id,
                    campaign_enabled,
                    added_at_utc
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    tracked_pack_id,
                    target_campaign_key,
                    parsed_target_campaign_id,
                    1,
                    now_utc,
                ),
            )

            imported_count += 1

        conn.commit()

        return {
            "ok": True,
            "message": f"Imported {imported_count} pack(s). Skipped {skipped_count} already-linked or missing pack(s).",
            "imported_count": imported_count,
            "skipped_count": skipped_count,
        }

    except Exception as exc:
        conn.rollback()
        return {
            "ok": False,
            "message": str(exc),
            "imported_count": imported_count,
            "skipped_count": skipped_count,
        }
    finally:
        conn.close()

def get_campaign_pack_art_image_src(set_code, booster_name, static_folder):
    art_info = get_chaos_pack_art_info(set_code, booster_name, static_folder)
    image_path = (art_info.get("image_path") or "").strip()

    if not image_path:
        image_path = "img/pack_art/_fallback/booster_default.png"

    return url_for("static", filename=image_path)

def get_tracked_chaos_packs_for_campaign_spin(static_folder, campaign_id=None, excluded_tracked_pack_ids=None):
    ensure_tracked_pack_campaign_schema()

    campaign_key = get_campaign_membership_key(campaign_id)

    conn = get_db_connection()
    cursor = conn.cursor()

    params = [campaign_key]

    excluded_ids = []
    for raw_pack_id in excluded_tracked_pack_ids or []:
        try:
            parsed_pack_id = int(raw_pack_id)
        except (TypeError, ValueError):
            continue

        if parsed_pack_id > 0 and parsed_pack_id not in excluded_ids:
            excluded_ids.append(parsed_pack_id)

    excluded_sql = ""
    if excluded_ids:
        placeholders = ",".join(["?"] * len(excluded_ids))
        excluded_sql = f"AND tcp.tracked_pack_id NOT IN ({placeholders})"
        params.extend(excluded_ids)

    cursor.execute(
        f"""
        SELECT
            tcp.tracked_pack_id,
            tcp.pack_tracking_code,
            tcp.set_code,
            tcp.booster_name,
            tcp.booster_index,
            tcp.pack_display_name,
            tcp.total_cards,
            tcp.bonus_pack_opened,
            tcp.added_at_utc,
            tcp.last_opened_at_utc,
            tcp.opened_count,
            tpc.campaign_id,
            COALESCE(tpc.campaign_enabled, 1) AS campaign_enabled
        FROM tracked_chaos_pack_campaigns tpc
        INNER JOIN tracked_chaos_packs tcp
            ON tcp.tracked_pack_id = tpc.tracked_pack_id
        WHERE tpc.campaign_key = ?
          AND COALESCE(tpc.campaign_enabled, 1) = 1
          {excluded_sql}
        ORDER BY tpc.added_at_utc DESC, tcp.tracked_pack_id DESC
        """,
        params,
    )

    rows = cursor.fetchall()
    conn.close()

    packs = []

    for row in rows:
        set_code = (row["set_code"] or "").strip().upper()
        booster_name = (row["booster_name"] or "").strip().lower()
        display_name = (row["pack_display_name"] or "").strip()

        if not display_name:
            display_name = build_default_chaos_pack_display_name(set_code, booster_name)

        packs.append({
            "tracked_pack_id": int(row["tracked_pack_id"]),
            "pack_tracking_code": (row["pack_tracking_code"] or "").strip().upper(),
            "set_code": set_code,
            "booster_name": booster_name,
            "booster_index": int(row["booster_index"] or 0),
            "display_name": display_name,
            "image_src": get_campaign_pack_art_image_src(set_code, booster_name, static_folder),
            "variant_count": 1,
            "total_variant_weight": 1,
            "total_cards": int(row["total_cards"] or 0),
            "bonus_pack_opened": bool(row["bonus_pack_opened"]),
            "added_at_utc": row["added_at_utc"] or "",
            "last_opened_at_utc": row["last_opened_at_utc"] or "",
            "opened_count": int(row["opened_count"] or 0),
            "campaign_id": int(row["campaign_id"]) if row["campaign_id"] is not None else None,
        })

    return packs

def get_tracked_chaos_pack_cards(tracked_pack_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            tracked_pack_card_id,
            tracked_pack_id,
            card_order,
            card_uuid,
            card_name,
            set_code,
            booster_name,
            booster_index,
            sheet_name,
            sheet_is_foil,
            sheet_has_balance_colors,
            sheet_total_weight,
            rarity,
            type_line,
            image_url,
            scryfall_id,
            collector_number
        FROM tracked_chaos_pack_cards
        WHERE tracked_pack_id = ?
        ORDER BY card_order ASC
        """,
        (int(tracked_pack_id),),
    )

    rows = cursor.fetchall()
    conn.close()

    cards = []

    for row in rows:
        cards.append({
            "card_uuid": row["card_uuid"],
            "card_name": row["card_name"],
            "set_code": row["set_code"],
            "booster_name": row["booster_name"],
            "booster_index": int(row["booster_index"] or 0),
            "sheet_name": row["sheet_name"],
            "sheet_is_foil": int(row["sheet_is_foil"] or 0),
            "sheet_has_balance_colors": int(row["sheet_has_balance_colors"] or 0),
            "sheet_total_weight": float(row["sheet_total_weight"] or 0),
            "rarity": row["rarity"],
            "type_line": row["type_line"],
            "image_url": row["image_url"],
            "scryfall_id": row["scryfall_id"],
            "collector_number": row["collector_number"],
        })

    return cards

def ensure_chaos_campaign_schema():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS chaos_campaigns (
            campaign_id INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_name TEXT NOT NULL UNIQUE,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at_utc TEXT NOT NULL
        )
        """
    )

    tables_to_patch = {
        "chaos_players": "campaign_id INTEGER NULL",
        "tracked_chaos_packs": "campaign_id INTEGER NULL",
        "tracked_chaos_pack_openings": "campaign_id INTEGER NULL",
    }

    for table_name, column_definition in tables_to_patch.items():
        cursor.execute(f"PRAGMA table_info({table_name})")
        existing_columns = {row[1] for row in cursor.fetchall()}

        if "campaign_id" not in existing_columns:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_definition}")

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_chaos_campaigns_active
        ON chaos_campaigns (is_active, campaign_name)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_chaos_players_campaign
        ON chaos_players (campaign_id, is_active, player_name)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tracked_chaos_packs_campaign
        ON tracked_chaos_packs (campaign_id, campaign_enabled, added_at_utc)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tracked_chaos_pack_openings_campaign
        ON tracked_chaos_pack_openings (campaign_id, opened_at_utc)
        """
    )

    conn.commit()
    conn.close()


def get_chaos_campaigns(include_disabled=True):
    ensure_chaos_campaign_schema()

    conn = get_db_connection()
    cursor = conn.cursor()

    where_clause = ""
    if not include_disabled:
        where_clause = "WHERE is_active = 1"

    cursor.execute(
        f"""
        SELECT
            campaign_id,
            campaign_name,
            is_active,
            created_at_utc
        FROM chaos_campaigns
        {where_clause}
        ORDER BY is_active DESC, campaign_name COLLATE NOCASE ASC
        """
    )

    rows = cursor.fetchall()
    conn.close()

    campaigns = []

    for row in rows:
        campaigns.append({
            "campaign_id": int(row["campaign_id"]),
            "campaign_name": row["campaign_name"] or "",
            "is_active": int(row["is_active"] or 0) == 1,
            "created_at_utc": row["created_at_utc"] or "",
        })

    return campaigns


def get_chaos_campaign_by_id(campaign_id):
    ensure_chaos_campaign_schema()

    try:
        parsed_campaign_id = int(campaign_id)
    except (TypeError, ValueError):
        return None

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            campaign_id,
            campaign_name,
            is_active,
            created_at_utc
        FROM chaos_campaigns
        WHERE campaign_id = ?
        """,
        (parsed_campaign_id,),
    )

    row = cursor.fetchone()
    conn.close()

    if not row:
        return None

    return {
        "campaign_id": int(row["campaign_id"]),
        "campaign_name": row["campaign_name"] or "",
        "is_active": int(row["is_active"] or 0) == 1,
        "created_at_utc": row["created_at_utc"] or "",
    }


def create_chaos_campaign(campaign_name):
    ensure_chaos_campaign_schema()

    clean_campaign_name = (campaign_name or "").strip()

    if not clean_campaign_name:
        return {
            "ok": False,
            "message": "Campaign Name is required.",
            "campaign_id": None,
        }

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO chaos_campaigns (
                campaign_name,
                is_active,
                created_at_utc
            )
            VALUES (?, ?, ?)
            """,
            (
                clean_campaign_name,
                1,
                now_utc,
            ),
        )

        campaign_id = cursor.lastrowid
        conn.commit()

        return {
            "ok": True,
            "message": f"Added campaign {clean_campaign_name}.",
            "campaign_id": int(campaign_id),
        }
    except Exception as exc:
        conn.rollback()
        return {
            "ok": False,
            "message": str(exc),
            "campaign_id": None,
        }
    finally:
        conn.close()


def update_chaos_campaign(campaign_id, campaign_name=None, is_active=None):
    ensure_chaos_campaign_schema()

    try:
        parsed_campaign_id = int(campaign_id)
    except (TypeError, ValueError):
        return {
            "ok": False,
            "message": "Invalid campaign ID.",
        }

    existing_campaign = get_chaos_campaign_by_id(parsed_campaign_id)
    if not existing_campaign:
        return {
            "ok": False,
            "message": "Campaign was not found.",
        }

    new_campaign_name = existing_campaign["campaign_name"]
    if campaign_name is not None:
        new_campaign_name = (campaign_name or "").strip()

    if not new_campaign_name:
        return {
            "ok": False,
            "message": "Campaign Name is required.",
        }

    new_is_active = 1 if existing_campaign["is_active"] else 0
    if is_active is not None:
        new_is_active = 1 if is_active else 0

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            UPDATE chaos_campaigns
            SET campaign_name = ?,
                is_active = ?
            WHERE campaign_id = ?
            """,
            (
                new_campaign_name,
                new_is_active,
                parsed_campaign_id,
            ),
        )

        conn.commit()

        if new_is_active == 0:
            selected_campaign_id = get_selected_chaos_campaign_id()
            if selected_campaign_id == parsed_campaign_id:
                clear_chaos_session_state("selected_chaos_campaign_id")

        return {
            "ok": True,
            "message": f"Updated campaign {new_campaign_name}.",
        }
    except Exception as exc:
        conn.rollback()
        return {
            "ok": False,
            "message": str(exc),
        }
    finally:
        conn.close()


def delete_chaos_campaign(campaign_id):
    ensure_chaos_campaign_schema()

    try:
        parsed_campaign_id = int(campaign_id)
    except (TypeError, ValueError):
        return {
            "ok": False,
            "message": "Invalid campaign ID.",
        }

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE chaos_players
        SET campaign_id = NULL
        WHERE campaign_id = ?
        """,
        (parsed_campaign_id,),
    )

    cursor.execute(
        """
        DELETE FROM tracked_chaos_pack_campaigns
        WHERE campaign_key = ?
        """,
        (str(parsed_campaign_id),),
    )

    cursor.execute(
        """
        UPDATE tracked_chaos_packs
        SET campaign_id = NULL
        WHERE campaign_id = ?
        """,
        (parsed_campaign_id,),
    )

    cursor.execute(
        """
        UPDATE tracked_chaos_pack_openings
        SET campaign_id = NULL
        WHERE campaign_id = ?
        """,
        (parsed_campaign_id,),
    )

    cursor.execute(
        """
        DELETE FROM chaos_campaigns
        WHERE campaign_id = ?
        """,
        (parsed_campaign_id,),
    )

    deleted_count = cursor.rowcount

    conn.commit()
    conn.close()

    if deleted_count <= 0:
        return {
            "ok": False,
            "message": "Campaign was not found.",
        }

    selected_campaign_id = get_selected_chaos_campaign_id()
    if selected_campaign_id == parsed_campaign_id:
        clear_chaos_session_state("selected_chaos_campaign_id")

    return {
        "ok": True,
        "message": "Campaign deleted. Linked players, packs, and openings were moved to No Campaign.",
    }


def get_selected_chaos_campaign_id():
    raw_value = get_chaos_session_state("selected_chaos_campaign_id", default_value=None)

    try:
        parsed_campaign_id = int(raw_value)
    except (TypeError, ValueError):
        return None

    campaign = get_chaos_campaign_by_id(parsed_campaign_id)
    if not campaign or not campaign["is_active"]:
        clear_chaos_session_state("selected_chaos_campaign_id")
        return None

    return parsed_campaign_id


def set_selected_chaos_campaign_id(campaign_id):
    try:
        parsed_campaign_id = int(campaign_id)
    except (TypeError, ValueError):
        clear_chaos_session_state("selected_chaos_campaign_id")
        return {
            "ok": True,
            "selected_chaos_campaign_id": None,
            "campaign_name": "No Campaign",
        }

    campaign = get_chaos_campaign_by_id(parsed_campaign_id)
    if not campaign or not campaign["is_active"]:
        return {
            "ok": False,
            "message": "Selected campaign is not active.",
            "selected_chaos_campaign_id": None,
        }

    set_chaos_session_state("selected_chaos_campaign_id", parsed_campaign_id)

    return {
        "ok": True,
        "selected_chaos_campaign_id": parsed_campaign_id,
        "campaign_name": campaign["campaign_name"],
    }

def migrate_chaos_players_remove_global_unique_constraint(cursor):
    cursor.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type = 'table'
          AND name = 'chaos_players'
        """
    )

    table_row = cursor.fetchone()
    table_sql = (table_row["sql"] or "") if table_row else ""

    if "player_name TEXT NOT NULL UNIQUE" not in table_sql:
        return

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS chaos_players_new (
            player_id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_name TEXT NOT NULL,
            portrait_image_path TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at_utc TEXT NOT NULL,
            campaign_id INTEGER NULL
        )
        """
    )

    cursor.execute("PRAGMA table_info(chaos_players)")
    existing_columns = {row[1] for row in cursor.fetchall()}

    campaign_select_sql = "campaign_id" if "campaign_id" in existing_columns else "NULL AS campaign_id"
    portrait_select_sql = "portrait_image_path" if "portrait_image_path" in existing_columns else "NULL AS portrait_image_path"
    active_select_sql = "is_active" if "is_active" in existing_columns else "1 AS is_active"
    created_select_sql = "created_at_utc" if "created_at_utc" in existing_columns else "'1970-01-01 00:00:00 UTC' AS created_at_utc"

    cursor.execute(
        f"""
        INSERT INTO chaos_players_new (
            player_id,
            player_name,
            portrait_image_path,
            is_active,
            created_at_utc,
            campaign_id
        )
        SELECT
            player_id,
            player_name,
            {portrait_select_sql},
            {active_select_sql},
            {created_select_sql},
            {campaign_select_sql}
        FROM chaos_players
        """
    )

    cursor.execute("DROP TABLE chaos_players")
    cursor.execute("ALTER TABLE chaos_players_new RENAME TO chaos_players")

def ensure_campaign_player_schema():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS chaos_players (
            player_id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_name TEXT NOT NULL,
            portrait_image_path TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at_utc TEXT NOT NULL,
            campaign_id INTEGER NULL
        )
        """
    )

    migrate_chaos_players_remove_global_unique_constraint(cursor)

    cursor.execute("PRAGMA table_info(chaos_players)")
    existing_columns = {row[1] for row in cursor.fetchall()}

    if "portrait_image_path" not in existing_columns:
        cursor.execute("ALTER TABLE chaos_players ADD COLUMN portrait_image_path TEXT")

    if "is_active" not in existing_columns:
        cursor.execute("ALTER TABLE chaos_players ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")

    if "created_at_utc" not in existing_columns:
        cursor.execute("ALTER TABLE chaos_players ADD COLUMN created_at_utc TEXT")

    if "campaign_id" not in existing_columns:
        cursor.execute("ALTER TABLE chaos_players ADD COLUMN campaign_id INTEGER NULL")

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_chaos_players_campaign_name
        ON chaos_players (campaign_id, player_name)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_chaos_players_campaign_active
        ON chaos_players (campaign_id, is_active, player_name)
        """
    )

    conn.commit()
    conn.close()


def get_campaign_players(include_disabled=True, campaign_id=None):
    ensure_campaign_player_schema()

    try:
        parsed_campaign_id = int(campaign_id) if campaign_id is not None else None
    except (TypeError, ValueError):
        parsed_campaign_id = None

    conn = get_db_connection()
    cursor = conn.cursor()

    where_conditions = []
    params = []

    if parsed_campaign_id is None:
        where_conditions.append("campaign_id IS NULL")
    else:
        where_conditions.append("campaign_id = ?")
        params.append(parsed_campaign_id)

    if not include_disabled:
        where_conditions.append("is_active = 1")

    where_clause = "WHERE " + " AND ".join(where_conditions)

    cursor.execute(
        f"""
        SELECT
            player_id,
            player_name,
            portrait_image_path,
            is_active,
            created_at_utc,
            campaign_id
        FROM chaos_players
        {where_clause}
        ORDER BY is_active DESC, player_name COLLATE NOCASE ASC
        """,
        params,
    )

    rows = cursor.fetchall()
    conn.close()

    players = []

    for row in rows:
        players.append({
            "player_id": int(row["player_id"]),
            "player_name": row["player_name"] or "",
            "portrait_image_path": row["portrait_image_path"] or "",
            "is_active": int(row["is_active"] or 0) == 1,
            "created_at_utc": row["created_at_utc"] or "",
            "campaign_id": int(row["campaign_id"]) if row["campaign_id"] is not None else None,
        })

    return players

def campaign_player_name_exists(player_name, campaign_id=None, exclude_player_id=None):
    ensure_campaign_player_schema()

    clean_player_name = (player_name or "").strip()

    if not clean_player_name:
        return False

    try:
        parsed_campaign_id = int(campaign_id) if campaign_id is not None else None
    except (TypeError, ValueError):
        parsed_campaign_id = None

    try:
        parsed_exclude_player_id = int(exclude_player_id) if exclude_player_id is not None else None
    except (TypeError, ValueError):
        parsed_exclude_player_id = None

    conn = get_db_connection()
    cursor = conn.cursor()

    where_conditions = ["LOWER(player_name) = LOWER(?)"]
    params = [clean_player_name]

    if parsed_campaign_id is None:
        where_conditions.append("campaign_id IS NULL")
    else:
        where_conditions.append("campaign_id = ?")
        params.append(parsed_campaign_id)

    if parsed_exclude_player_id is not None:
        where_conditions.append("player_id <> ?")
        params.append(parsed_exclude_player_id)

    cursor.execute(
        f"""
        SELECT player_id
        FROM chaos_players
        WHERE {" AND ".join(where_conditions)}
        LIMIT 1
        """,
        params,
    )

    row = cursor.fetchone()
    conn.close()

    return row is not None

def get_campaign_player_by_id(player_id):
    ensure_campaign_player_schema()

    try:
        parsed_player_id = int(player_id)
    except (TypeError, ValueError):
        return None

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            player_id,
            player_name,
            portrait_image_path,
            is_active,
            created_at_utc,
            campaign_id
        FROM chaos_players
        WHERE player_id = ?
        """,
        (parsed_player_id,),
    )

    row = cursor.fetchone()
    conn.close()

    if not row:
        return None

    return {
        "player_id": int(row["player_id"]),
        "player_name": row["player_name"] or "",
        "portrait_image_path": row["portrait_image_path"] or "",
        "is_active": int(row["is_active"] or 0) == 1,
        "created_at_utc": row["created_at_utc"] or "",
        "campaign_id": int(row["campaign_id"]) if row["campaign_id"] is not None else None,
    }


def create_campaign_player(player_name, portrait_image_path="", campaign_id=None):
    ensure_campaign_player_schema()

    clean_player_name = (player_name or "").strip()

    if not clean_player_name:
        return {
            "ok": False,
            "message": "Screen Name is required.",
            "player_id": None,
        }
    
    try:
        parsed_campaign_id = int(campaign_id) if campaign_id is not None else None
    except (TypeError, ValueError):
        parsed_campaign_id = None

    if campaign_player_name_exists(clean_player_name, campaign_id=parsed_campaign_id):
        return {
            "ok": False,
            "message": f"Player {clean_player_name} already exists in this campaign.",
            "player_id": None,
        }

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    try:
        parsed_campaign_id = int(campaign_id) if campaign_id is not None else None
    except (TypeError, ValueError):
        parsed_campaign_id = None

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO chaos_players (
                player_name,
                portrait_image_path,
                is_active,
                created_at_utc,
                campaign_id
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                clean_player_name,
                (portrait_image_path or "").strip(),
                1,
                now_utc,
                parsed_campaign_id,
            ),
        )

        player_id = cursor.lastrowid
        conn.commit()

        return {
            "ok": True,
            "message": f"Added player {clean_player_name}.",
            "player_id": int(player_id),
        }
    except Exception as exc:
        conn.rollback()
        return {
            "ok": False,
            "message": str(exc),
            "player_id": None,
        }
    finally:
        conn.close()


def update_campaign_player(player_id, player_name=None, portrait_image_path=None, is_active=None):
    ensure_campaign_player_schema()

    try:
        parsed_player_id = int(player_id)
    except (TypeError, ValueError):
        return {
            "ok": False,
            "message": "Invalid player ID.",
        }

    existing_player = get_campaign_player_by_id(parsed_player_id)
    if not existing_player:
        return {
            "ok": False,
            "message": "Player was not found.",
        }

    new_player_name = existing_player["player_name"]
    if player_name is not None:
        new_player_name = (player_name or "").strip()

    if not new_player_name:
        return {
            "ok": False,
            "message": "Screen Name is required.",
        }

    if campaign_player_name_exists(
        new_player_name,
        campaign_id=existing_player.get("campaign_id"),
        exclude_player_id=parsed_player_id,
    ):
        return {
            "ok": False,
            "message": f"Player {new_player_name} already exists in this campaign.",
        }

    new_portrait_image_path = existing_player["portrait_image_path"]
    if portrait_image_path is not None:
        new_portrait_image_path = (portrait_image_path or "").strip()

    new_is_active = 1 if existing_player["is_active"] else 0
    if is_active is not None:
        new_is_active = 1 if is_active else 0

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            UPDATE chaos_players
            SET player_name = ?,
                portrait_image_path = ?,
                is_active = ?
            WHERE player_id = ?
            """,
            (
                new_player_name,
                new_portrait_image_path,
                new_is_active,
                parsed_player_id,
            ),
        )

        conn.commit()

        return {
            "ok": True,
            "message": f"Updated player {new_player_name}.",
        }
    except Exception as exc:
        conn.rollback()
        return {
            "ok": False,
            "message": str(exc),
        }
    finally:
        conn.close()


def delete_campaign_player(player_id):
    ensure_campaign_player_schema()

    try:
        parsed_player_id = int(player_id)
    except (TypeError, ValueError):
        return {
            "ok": False,
            "message": "Invalid player ID.",
        }

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE tracked_chaos_pack_openings
        SET opened_by_player_id = NULL
        WHERE opened_by_player_id = ?
        """,
        (parsed_player_id,),
    )

    cursor.execute(
        """
        DELETE FROM chaos_players
        WHERE player_id = ?
        """,
        (parsed_player_id,),
    )

    deleted_count = cursor.rowcount

    conn.commit()
    conn.close()

    if deleted_count <= 0:
        return {
            "ok": False,
            "message": "Player was not found.",
        }

    selected_player_id = get_selected_campaign_player_id()
    if selected_player_id == parsed_player_id:
        clear_chaos_session_state("selected_campaign_player_id")

    return {
        "ok": True,
        "message": "Player deleted.",
    }


def get_selected_campaign_player_id(campaign_id=None):
    raw_value = get_chaos_session_state("selected_campaign_player_id", default_value=None)

    try:
        parsed_player_id = int(raw_value)
    except (TypeError, ValueError):
        return None

    try:
        parsed_campaign_id = int(campaign_id) if campaign_id is not None else None
    except (TypeError, ValueError):
        parsed_campaign_id = None

    player = get_campaign_player_by_id(parsed_player_id)
    if not player or not player["is_active"]:
        clear_chaos_session_state("selected_campaign_player_id")
        return None

    if player.get("campaign_id") != parsed_campaign_id:
        clear_chaos_session_state("selected_campaign_player_id")
        return None

    return parsed_player_id


def set_selected_campaign_player_id(player_id, campaign_id=None):
    try:
        parsed_player_id = int(player_id)
    except (TypeError, ValueError):
        clear_chaos_session_state("selected_campaign_player_id")
        return {
            "ok": True,
            "selected_campaign_player_id": None,
        }

    try:
        parsed_campaign_id = int(campaign_id) if campaign_id is not None else None
    except (TypeError, ValueError):
        parsed_campaign_id = None

    player = get_campaign_player_by_id(parsed_player_id)
    if not player or not player["is_active"]:
        return {
            "ok": False,
            "message": "Selected player is not active.",
            "selected_campaign_player_id": None,
        }

    if player.get("campaign_id") != parsed_campaign_id:
        return {
            "ok": False,
            "message": "Selected player does not belong to the current campaign.",
            "selected_campaign_player_id": None,
        }

    set_chaos_session_state("selected_campaign_player_id", parsed_player_id)

    return {
        "ok": True,
        "selected_campaign_player_id": parsed_player_id,
        "player_name": player["player_name"],
    }

def get_campaign_player_import_options(current_campaign_id=None):
    ensure_chaos_campaign_schema()

    try:
        parsed_current_campaign_id = int(current_campaign_id) if current_campaign_id is not None else None
    except (TypeError, ValueError):
        parsed_current_campaign_id = None

    options = []

    conn = get_db_connection()
    cursor = conn.cursor()

    if parsed_current_campaign_id is not None:
        cursor.execute(
            """
            SELECT COUNT(*) AS player_count
            FROM chaos_players
            WHERE campaign_id IS NULL
            """
        )

        no_campaign_row = cursor.fetchone()
        no_campaign_count = int(no_campaign_row["player_count"] or 0) if no_campaign_row else 0

        if no_campaign_count > 0:
            options.append({
                "campaign_id": "__none__",
                "campaign_name": "No Campaign",
                "player_count": no_campaign_count,
            })

    cursor.execute(
        """
        SELECT
            c.campaign_id,
            c.campaign_name,
            COUNT(p.player_id) AS player_count
        FROM chaos_campaigns c
        LEFT JOIN chaos_players p
            ON p.campaign_id = c.campaign_id
        WHERE c.is_active = 1
        GROUP BY c.campaign_id, c.campaign_name
        ORDER BY c.campaign_name COLLATE NOCASE ASC
        """
    )

    rows = cursor.fetchall()
    conn.close()

    for row in rows:
        source_campaign_id = int(row["campaign_id"])

        if parsed_current_campaign_id is not None and source_campaign_id == parsed_current_campaign_id:
            continue

        player_count = int(row["player_count"] or 0)
        if player_count <= 0:
            continue

        options.append({
            "campaign_id": source_campaign_id,
            "campaign_name": row["campaign_name"] or "",
            "player_count": player_count,
        })

    return options


def import_campaign_players_from_campaign(source_campaign_id=None, target_campaign_id=None):
    ensure_campaign_player_schema()

    try:
        parsed_source_campaign_id = int(source_campaign_id) if source_campaign_id not in {None, ""} else None
    except (TypeError, ValueError):
        parsed_source_campaign_id = None

    try:
        parsed_target_campaign_id = int(target_campaign_id) if target_campaign_id is not None else None
    except (TypeError, ValueError):
        parsed_target_campaign_id = None

    if parsed_source_campaign_id == parsed_target_campaign_id:
        return {
            "ok": False,
            "message": "Source and target campaign are the same.",
            "imported_count": 0,
            "skipped_count": 0,
        }

    conn = get_db_connection()
    cursor = conn.cursor()

    source_where = "campaign_id IS NULL"
    source_params = []

    if parsed_source_campaign_id is not None:
        source_where = "campaign_id = ?"
        source_params.append(parsed_source_campaign_id)

    cursor.execute(
        f"""
        SELECT
            player_name,
            portrait_image_path,
            is_active
        FROM chaos_players
        WHERE {source_where}
        ORDER BY player_name COLLATE NOCASE ASC
        """,
        source_params,
    )

    source_rows = cursor.fetchall()

    imported_count = 0
    skipped_count = 0
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    try:
        for row in source_rows:
            player_name = (row["player_name"] or "").strip()
            if not player_name:
                skipped_count += 1
                continue

            target_where = "campaign_id IS NULL"
            target_params = [player_name]

            if parsed_target_campaign_id is not None:
                target_where = "campaign_id = ?"
                target_params = [player_name, parsed_target_campaign_id]

            cursor.execute(
                f"""
                SELECT player_id
                FROM chaos_players
                WHERE LOWER(player_name) = LOWER(?)
                  AND {target_where}
                LIMIT 1
                """,
                target_params,
            )

            existing_target_row = cursor.fetchone()
            if existing_target_row:
                skipped_count += 1
                continue

            cursor.execute(
                """
                INSERT INTO chaos_players (
                    player_name,
                    portrait_image_path,
                    is_active,
                    created_at_utc,
                    campaign_id
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    player_name,
                    row["portrait_image_path"] or "",
                    int(row["is_active"] or 0),
                    now_utc,
                    parsed_target_campaign_id,
                ),
            )

            imported_count += 1

        conn.commit()
    except Exception as exc:
        conn.rollback()
        return {
            "ok": False,
            "message": str(exc),
            "imported_count": imported_count,
            "skipped_count": skipped_count,
        }
    finally:
        conn.close()

    return {
        "ok": True,
        "message": f"Imported {imported_count} player(s). Skipped {skipped_count} duplicate player(s).",
        "imported_count": imported_count,
        "skipped_count": skipped_count,
    }

def ensure_chaos_draft_game_schema():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS chaos_draft_games (
            draft_game_id INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id INTEGER NULL,
            draft_name TEXT,
            started_at_utc TEXT NOT NULL,
            completed_at_utc TEXT,
            packs_per_player INTEGER NOT NULL DEFAULT 3,
            is_active INTEGER NOT NULL DEFAULT 1
        )
        """
    )

    cursor.execute("PRAGMA table_info(tracked_chaos_pack_openings)")
    opening_columns = {row[1] for row in cursor.fetchall()}

    if "draft_game_id" not in opening_columns:
        cursor.execute("ALTER TABLE tracked_chaos_pack_openings ADD COLUMN draft_game_id INTEGER NULL")

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_chaos_draft_games_campaign_active
        ON chaos_draft_games (campaign_id, is_active, started_at_utc)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tracked_openings_draft_game_pack
        ON tracked_chaos_pack_openings (draft_game_id, tracked_pack_id)
        """
    )

    conn.commit()
    conn.close()


def normalize_optional_int(value):
    try:
        return int(value) if value is not None and value != "" else None
    except (TypeError, ValueError):
        return None


def get_selected_chaos_draft_game_id():
    raw_value = get_chaos_session_state("selected_chaos_draft_game_id", default_value=None)
    return normalize_optional_int(raw_value)


def set_selected_chaos_draft_game_id(draft_game_id):
    parsed_draft_game_id = normalize_optional_int(draft_game_id)

    if parsed_draft_game_id is None:
        clear_chaos_session_state("selected_chaos_draft_game_id")
        return {
            "ok": True,
            "selected_chaos_draft_game_id": None,
        }

    set_chaos_session_state("selected_chaos_draft_game_id", parsed_draft_game_id)

    return {
        "ok": True,
        "selected_chaos_draft_game_id": parsed_draft_game_id,
    }


def get_chaos_draft_game_by_id(draft_game_id):
    ensure_chaos_draft_game_schema()

    parsed_draft_game_id = normalize_optional_int(draft_game_id)
    if parsed_draft_game_id is None:
        return None

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            draft_game_id,
            campaign_id,
            draft_name,
            started_at_utc,
            completed_at_utc,
            packs_per_player,
            is_active
        FROM chaos_draft_games
        WHERE draft_game_id = ?
        """,
        (parsed_draft_game_id,),
    )

    row = cursor.fetchone()
    conn.close()

    if not row:
        return None

    return {
        "draft_game_id": int(row["draft_game_id"]),
        "campaign_id": int(row["campaign_id"]) if row["campaign_id"] is not None else None,
        "draft_name": row["draft_name"] or "",
        "started_at_utc": row["started_at_utc"] or "",
        "completed_at_utc": row["completed_at_utc"] or "",
        "packs_per_player": int(row["packs_per_player"] or 3),
        "is_active": int(row["is_active"] or 0) == 1,
    }


def get_active_chaos_draft_game(campaign_id=None):
    ensure_chaos_draft_game_schema()

    parsed_campaign_id = normalize_optional_int(campaign_id)

    conn = get_db_connection()
    cursor = conn.cursor()

    if parsed_campaign_id is None:
        cursor.execute(
            """
            SELECT
                draft_game_id,
                campaign_id,
                draft_name,
                started_at_utc,
                completed_at_utc,
                packs_per_player,
                is_active
            FROM chaos_draft_games
            WHERE campaign_id IS NULL
              AND is_active = 1
            ORDER BY started_at_utc DESC, draft_game_id DESC
            LIMIT 1
            """
        )
    else:
        cursor.execute(
            """
            SELECT
                draft_game_id,
                campaign_id,
                draft_name,
                started_at_utc,
                completed_at_utc,
                packs_per_player,
                is_active
            FROM chaos_draft_games
            WHERE campaign_id = ?
              AND is_active = 1
            ORDER BY started_at_utc DESC, draft_game_id DESC
            LIMIT 1
            """,
            (parsed_campaign_id,),
        )

    row = cursor.fetchone()
    conn.close()

    if not row:
        return None

    return {
        "draft_game_id": int(row["draft_game_id"]),
        "campaign_id": int(row["campaign_id"]) if row["campaign_id"] is not None else None,
        "draft_name": row["draft_name"] or "",
        "started_at_utc": row["started_at_utc"] or "",
        "completed_at_utc": row["completed_at_utc"] or "",
        "packs_per_player": int(row["packs_per_player"] or 3),
        "is_active": int(row["is_active"] or 0) == 1,
    }


def create_chaos_draft_game(campaign_id=None, packs_per_player=3):
    ensure_chaos_draft_game_schema()

    parsed_campaign_id = normalize_optional_int(campaign_id)

    try:
        parsed_packs_per_player = int(packs_per_player)
    except (TypeError, ValueError):
        parsed_packs_per_player = 3

    if parsed_packs_per_player < 1:
        parsed_packs_per_player = 1

    if parsed_packs_per_player > 12:
        parsed_packs_per_player = 12

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    conn = get_db_connection()
    cursor = conn.cursor()

    if parsed_campaign_id is None:
        cursor.execute(
            """
            UPDATE chaos_draft_games
            SET is_active = 0,
                completed_at_utc = COALESCE(completed_at_utc, ?)
            WHERE campaign_id IS NULL
              AND is_active = 1
            """,
            (now_utc,),
        )
    else:
        cursor.execute(
            """
            UPDATE chaos_draft_games
            SET is_active = 0,
                completed_at_utc = COALESCE(completed_at_utc, ?)
            WHERE campaign_id = ?
              AND is_active = 1
            """,
            (
                now_utc,
                parsed_campaign_id,
            ),
        )

    cursor.execute(
        """
        INSERT INTO chaos_draft_games (
            campaign_id,
            draft_name,
            started_at_utc,
            completed_at_utc,
            packs_per_player,
            is_active
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            parsed_campaign_id,
            None,
            now_utc,
            None,
            parsed_packs_per_player,
            1,
        ),
    )

    draft_game_id = cursor.lastrowid

    conn.commit()
    conn.close()

    set_selected_chaos_draft_game_id(draft_game_id)

    return {
        "ok": True,
        "message": "New draft started.",
        "draft_game_id": int(draft_game_id),
        "campaign_id": parsed_campaign_id,
        "started_at_utc": now_utc,
        "packs_per_player": parsed_packs_per_player,
    }


def get_selected_or_create_chaos_draft_game(campaign_id=None):
    ensure_chaos_draft_game_schema()

    parsed_campaign_id = normalize_optional_int(campaign_id)
    selected_draft_game_id = get_selected_chaos_draft_game_id()

    if selected_draft_game_id is not None:
        selected_game = get_chaos_draft_game_by_id(selected_draft_game_id)

        if (
            selected_game
            and selected_game["is_active"]
            and selected_game.get("campaign_id") == parsed_campaign_id
        ):
            return selected_game

        clear_chaos_session_state("selected_chaos_draft_game_id")

    active_game = get_active_chaos_draft_game(campaign_id=parsed_campaign_id)

    if active_game:
        set_selected_chaos_draft_game_id(active_game["draft_game_id"])
        return active_game

    create_result = create_chaos_draft_game(
        campaign_id=parsed_campaign_id,
        packs_per_player=3,
    )

    return {
        "draft_game_id": int(create_result["draft_game_id"]),
        "campaign_id": parsed_campaign_id,
        "draft_name": "",
        "started_at_utc": create_result["started_at_utc"],
        "completed_at_utc": "",
        "packs_per_player": int(create_result["packs_per_player"] or 3),
        "is_active": True,
    }


def get_draft_game_selected_pack_ids(draft_game_id):
    ensure_chaos_draft_game_schema()

    parsed_draft_game_id = normalize_optional_int(draft_game_id)

    if parsed_draft_game_id is None:
        return set()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT DISTINCT tracked_pack_id
        FROM tracked_chaos_pack_openings
        WHERE draft_game_id = ?
          AND tracked_pack_id IS NOT NULL
        """,
        (parsed_draft_game_id,),
    )

    rows = cursor.fetchall()
    conn.close()

    return {
        int(row["tracked_pack_id"])
        for row in rows
        if row["tracked_pack_id"] is not None
    }

def get_chaos_draft_game_display_label(draft_game):
    if not draft_game:
        return "Draft #1"

    draft_game_id = normalize_optional_int(draft_game.get("draft_game_id"))
    campaign_id = normalize_optional_int(draft_game.get("campaign_id"))
    started_at_utc = (draft_game.get("started_at_utc") or "").strip()

    if draft_game_id is None or not started_at_utc:
        return "Draft #1"

    draft_date_utc = started_at_utc[:10]

    conn = get_db_connection()
    cursor = conn.cursor()

    if campaign_id is None:
        cursor.execute(
            """
            SELECT COUNT(*) AS draft_number
            FROM chaos_draft_games
            WHERE campaign_id IS NULL
              AND SUBSTR(started_at_utc, 1, 10) = ?
              AND (
                    started_at_utc < ?
                    OR (started_at_utc = ? AND draft_game_id <= ?)
              )
            """,
            (
                draft_date_utc,
                started_at_utc,
                started_at_utc,
                draft_game_id,
            ),
        )
    else:
        cursor.execute(
            """
            SELECT COUNT(*) AS draft_number
            FROM chaos_draft_games
            WHERE campaign_id = ?
              AND SUBSTR(started_at_utc, 1, 10) = ?
              AND (
                    started_at_utc < ?
                    OR (started_at_utc = ? AND draft_game_id <= ?)
              )
            """,
            (
                campaign_id,
                draft_date_utc,
                started_at_utc,
                started_at_utc,
                draft_game_id,
            ),
        )

    row = cursor.fetchone()
    conn.close()

    draft_number = int(row["draft_number"] or 1) if row else 1

    if draft_number < 1:
        draft_number = 1

    return f"Draft #{draft_number}"

def record_campaign_pack_opening(tracked_pack_id, opened_by_player_id=None, opening_context="campaign_mode", campaign_id=None, draft_game_id=None):
    ensure_chaos_draft_game_schema()

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO tracked_chaos_pack_openings (
            tracked_pack_id,
            opened_at_utc,
            opened_by_player_id,
            opening_context,
            campaign_id,
            draft_game_id
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            int(tracked_pack_id),
            now_utc,
            opened_by_player_id,
            opening_context,
            int(campaign_id) if campaign_id is not None else None,
            normalize_optional_int(draft_game_id),
        ),
    )

    cursor.execute(
        """
        UPDATE tracked_chaos_packs
        SET opened_count = COALESCE(opened_count, 0) + 1,
            last_opened_at_utc = ?
        WHERE tracked_pack_id = ?
        """,
        (
            now_utc,
            int(tracked_pack_id),
        ),
    )

    conn.commit()
    conn.close()

def delete_all_campaign_history():
    ensure_chaos_draft_game_schema()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM tracked_chaos_pack_openings")
    deleted_openings = cursor.rowcount

    cursor.execute("DELETE FROM chaos_draft_games")
    deleted_drafts = cursor.rowcount

    cursor.execute(
        """
        UPDATE tracked_chaos_packs
        SET opened_count = 0,
            last_opened_at_utc = NULL
        """
    )

    conn.commit()
    conn.close()

    clear_chaos_session_state("selected_chaos_draft_game_id")
    clear_chaos_session_state("pending_campaign_pack_opening_recorded")

    return {
        "deleted_openings": int(deleted_openings or 0),
        "deleted_drafts": int(deleted_drafts or 0),
    }

def delete_selected_campaign_history(opening_ids):
    ensure_chaos_draft_game_schema()

    parsed_opening_ids = []

    for raw_opening_id in opening_ids or []:
        try:
            parsed_opening_id = int(raw_opening_id)
        except (TypeError, ValueError):
            continue

        if parsed_opening_id > 0 and parsed_opening_id not in parsed_opening_ids:
            parsed_opening_ids.append(parsed_opening_id)

    if not parsed_opening_ids:
        return 0

    placeholders = ",".join(["?"] * len(parsed_opening_ids))

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        f"""
        SELECT DISTINCT tracked_pack_id
        FROM tracked_chaos_pack_openings
        WHERE opening_id IN ({placeholders})
          AND tracked_pack_id IS NOT NULL
        """,
        parsed_opening_ids,
    )

    affected_pack_ids = [
        int(row["tracked_pack_id"])
        for row in cursor.fetchall()
        if row["tracked_pack_id"] is not None
    ]

    cursor.execute(
        f"""
        DELETE FROM tracked_chaos_pack_openings
        WHERE opening_id IN ({placeholders})
        """,
        parsed_opening_ids,
    )

    deleted_count = cursor.rowcount

    for tracked_pack_id in affected_pack_ids:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS opened_count,
                MAX(opened_at_utc) AS last_opened_at_utc
            FROM tracked_chaos_pack_openings
            WHERE tracked_pack_id = ?
            """,
            (tracked_pack_id,),
        )

        summary_row = cursor.fetchone()
        opened_count = int(summary_row["opened_count"] or 0) if summary_row else 0
        last_opened_at_utc = summary_row["last_opened_at_utc"] if summary_row else None

        cursor.execute(
            """
            UPDATE tracked_chaos_packs
            SET opened_count = ?,
                last_opened_at_utc = ?
            WHERE tracked_pack_id = ?
            """,
            (
                opened_count,
                last_opened_at_utc,
                tracked_pack_id,
            ),
        )

    conn.commit()
    conn.close()

    return int(deleted_count or 0)

def get_campaign_history_filter_options(static_folder, campaign_id=None, tracked_pack_id=None):
    ensure_chaos_campaign_schema()
    ensure_campaign_player_schema()
    ensure_chaos_draft_game_schema()

    parsed_campaign_id = normalize_optional_int(campaign_id)
    parsed_tracked_pack_id = normalize_optional_int(tracked_pack_id)

    conn = get_db_connection()
    cursor = conn.cursor()

    campaign_options = [{
        "campaign_id": "",
        "campaign_name": "No Campaign",
    }]

    cursor.execute(
        """
        SELECT
            campaign_id,
            campaign_name
        FROM chaos_campaigns
        WHERE is_active = 1
        ORDER BY campaign_name COLLATE NOCASE ASC
        """
    )

    for row in cursor.fetchall():
        campaign_options.append({
            "campaign_id": int(row["campaign_id"]),
            "campaign_name": row["campaign_name"] or "",
        })

    if parsed_campaign_id is None:
        player_where = "WHERE campaign_id IS NULL"
        player_params = []
        draft_where = "WHERE campaign_id IS NULL"
        draft_params = []
        pack_where = "WHERE campaign_id IS NULL"
        pack_params = []
    else:
        player_where = "WHERE campaign_id = ?"
        player_params = [parsed_campaign_id]
        draft_where = "WHERE campaign_id = ?"
        draft_params = [parsed_campaign_id]
        pack_where = "WHERE campaign_id = ?"
        pack_params = [parsed_campaign_id]

    cursor.execute(
        f"""
        SELECT
            player_id,
            player_name
        FROM chaos_players
        {player_where}
        ORDER BY player_name COLLATE NOCASE ASC
        """,
        player_params,
    )

    player_options = []
    for row in cursor.fetchall():
        player_options.append({
            "player_id": int(row["player_id"]),
            "player_name": row["player_name"] or "",
        })

    cursor.execute(
        f"""
        SELECT
            draft_game_id,
            campaign_id,
            draft_name,
            started_at_utc,
            completed_at_utc,
            packs_per_player,
            is_active
        FROM chaos_draft_games
        {draft_where}
        ORDER BY started_at_utc DESC, draft_game_id DESC
        """,
        draft_params,
    )

    draft_options = []
    for row in cursor.fetchall():
        draft_game = {
            "draft_game_id": int(row["draft_game_id"]),
            "campaign_id": int(row["campaign_id"]) if row["campaign_id"] is not None else None,
            "draft_name": row["draft_name"] or "",
            "started_at_utc": row["started_at_utc"] or "",
            "completed_at_utc": row["completed_at_utc"] or "",
            "packs_per_player": int(row["packs_per_player"] or 3),
            "is_active": int(row["is_active"] or 0) == 1,
        }

        draft_options.append({
            "draft_game_id": draft_game["draft_game_id"],
            "draft_label": get_chaos_draft_game_display_label(draft_game),
            "started_at_utc": draft_game["started_at_utc"],
        })

    if parsed_tracked_pack_id is not None:
        pack_where = "WHERE tracked_pack_id = ?"
        pack_params = [parsed_tracked_pack_id]

    cursor.execute(
        f"""
        SELECT
            tracked_pack_id,
            pack_tracking_code,
            set_code,
            booster_name,
            pack_display_name
        FROM tracked_chaos_packs
        {pack_where}
        ORDER BY added_at_utc DESC, tracked_pack_id DESC
        """,
        pack_params,
    )

    pack_options = []
    for row in cursor.fetchall():
        pack_options.append({
            "tracked_pack_id": int(row["tracked_pack_id"]),
            "pack_tracking_code": (row["pack_tracking_code"] or "").strip().upper(),
            "pack_display_name": row["pack_display_name"] or "",
            "set_code": (row["set_code"] or "").strip().upper(),
            "booster_name": (row["booster_name"] or "").strip().lower(),
            "image_src": get_campaign_pack_art_image_src(row["set_code"], row["booster_name"], static_folder),
        })

    conn.close()

    return {
        "campaign_options": campaign_options,
        "player_options": player_options,
        "draft_options": draft_options,
        "pack_options": pack_options,
    }

def get_campaign_history_rows(static_folder, campaign_id=None, draft_game_id=None, player_id=None, tracked_pack_id=None, page=1, per_page=50):
    ensure_chaos_campaign_schema()
    ensure_campaign_player_schema()
    ensure_chaos_draft_game_schema()

    parsed_campaign_id = normalize_optional_int(campaign_id)
    parsed_draft_game_id = normalize_optional_int(draft_game_id)
    parsed_player_id = normalize_optional_int(player_id)
    parsed_tracked_pack_id = normalize_optional_int(tracked_pack_id)

    try:
        parsed_page = int(page)
    except (TypeError, ValueError):
        parsed_page = 1

    if parsed_page < 1:
        parsed_page = 1

    try:
        parsed_per_page = int(per_page)
    except (TypeError, ValueError):
        parsed_per_page = 50

    if parsed_per_page < 1:
        parsed_per_page = 50

    if parsed_per_page > 50:
        parsed_per_page = 50

    where_conditions = []
    params = []

    if parsed_campaign_id is None:
        where_conditions.append("tco.campaign_id IS NULL")
    else:
        where_conditions.append("tco.campaign_id = ?")
        params.append(parsed_campaign_id)

    if parsed_draft_game_id is not None:
        where_conditions.append("tco.draft_game_id = ?")
        params.append(parsed_draft_game_id)

    if parsed_player_id is not None:
        where_conditions.append("tco.opened_by_player_id = ?")
        params.append(parsed_player_id)

    if parsed_tracked_pack_id is not None:
        where_conditions.append("tco.tracked_pack_id = ?")
        params.append(parsed_tracked_pack_id)

    where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        f"""
        SELECT COUNT(*) AS total_count
        FROM tracked_chaos_pack_openings tco
        INNER JOIN tracked_chaos_packs tcp
            ON tcp.tracked_pack_id = tco.tracked_pack_id
        {where_clause}
        """,
        params,
    )

    total_row = cursor.fetchone()
    total_count = int(total_row["total_count"] or 0) if total_row else 0
    total_pages = max(1, (total_count + parsed_per_page - 1) // parsed_per_page)

    if parsed_page > total_pages:
        parsed_page = total_pages

    offset_rows = (parsed_page - 1) * parsed_per_page

    cursor.execute(
        f"""
        SELECT
            tco.opening_id,
            tco.tracked_pack_id,
            tco.opened_at_utc,
            tco.opened_by_player_id,
            tco.opening_context,
            tco.campaign_id,
            tco.draft_game_id,

            tcp.pack_tracking_code,
            tcp.set_code,
            tcp.booster_name,
            tcp.booster_index,
            tcp.pack_display_name,
            tcp.total_cards,
            tcp.bonus_pack_opened,

            cp.player_name,
            cc.campaign_name,

            cdg.started_at_utc AS draft_started_at_utc,
            cdg.completed_at_utc AS draft_completed_at_utc,
            cdg.packs_per_player,
            cdg.is_active AS draft_is_active
        FROM tracked_chaos_pack_openings tco
        INNER JOIN tracked_chaos_packs tcp
            ON tcp.tracked_pack_id = tco.tracked_pack_id
        LEFT JOIN chaos_players cp
            ON cp.player_id = tco.opened_by_player_id
        LEFT JOIN chaos_campaigns cc
            ON cc.campaign_id = tco.campaign_id
        LEFT JOIN chaos_draft_games cdg
            ON cdg.draft_game_id = tco.draft_game_id
        {where_clause}
        ORDER BY
            COALESCE(cdg.started_at_utc, tco.opened_at_utc) DESC,
            tco.draft_game_id DESC,
            tco.opened_at_utc DESC,
            tco.opening_id DESC
        LIMIT ?
        OFFSET ?
        """,
        params + [parsed_per_page, offset_rows],
    )

    rows = cursor.fetchall()
    conn.close()

    history_rows = []

    for row in rows:
        draft_game = None

        if row["draft_game_id"] is not None:
            draft_game = {
                "draft_game_id": int(row["draft_game_id"]),
                "campaign_id": int(row["campaign_id"]) if row["campaign_id"] is not None else None,
                "draft_name": "",
                "started_at_utc": row["draft_started_at_utc"] or "",
                "completed_at_utc": row["draft_completed_at_utc"] or "",
                "packs_per_player": int(row["packs_per_player"] or 3),
                "is_active": int(row["draft_is_active"] or 0) == 1,
            }

        draft_label = get_chaos_draft_game_display_label(draft_game) if draft_game else "Unknown Draft"
        set_code = (row["set_code"] or "").strip().upper()
        booster_name = (row["booster_name"] or "").strip().lower()

        history_rows.append({
            "opening_id": int(row["opening_id"]),
            "tracked_pack_id": int(row["tracked_pack_id"]),
            "opened_at_utc": row["opened_at_utc"] or "",
            "opened_by_player_id": int(row["opened_by_player_id"]) if row["opened_by_player_id"] is not None else None,
            "opening_context": row["opening_context"] or "",
            "campaign_id": int(row["campaign_id"]) if row["campaign_id"] is not None else None,
            "campaign_name": row["campaign_name"] or "No Campaign",
            "draft_game_id": int(row["draft_game_id"]) if row["draft_game_id"] is not None else None,
            "draft_label": draft_label,
            "draft_started_at_utc": row["draft_started_at_utc"] or "",
            "player_name": row["player_name"] or "No Player",
            "pack_tracking_code": (row["pack_tracking_code"] or "").strip().upper(),
            "set_code": set_code,
            "booster_name": booster_name,
            "booster_index": int(row["booster_index"] or 0),
            "pack_display_name": row["pack_display_name"] or "",
            "total_cards": int(row["total_cards"] or 0),
            "bonus_pack_opened": int(row["bonus_pack_opened"] or 0) == 1,
            "image_src": get_campaign_pack_art_image_src(set_code, booster_name, static_folder),
        })

    grouped_drafts = []
    grouped_lookup = {}

    for history_row in history_rows:
        group_key = history_row["draft_game_id"] if history_row["draft_game_id"] is not None else f"opening-{history_row['opening_id']}"

        if group_key not in grouped_lookup:
            grouped_lookup[group_key] = {
                "draft_game_id": history_row["draft_game_id"],
                "draft_label": history_row["draft_label"],
                "campaign_name": history_row["campaign_name"],
                "draft_started_at_utc": history_row["draft_started_at_utc"] or history_row["opened_at_utc"],
                "rows": [],
            }
            grouped_drafts.append(grouped_lookup[group_key])

        grouped_lookup[group_key]["rows"].append(history_row)

    return {
        "rows": history_rows,
        "grouped_drafts": grouped_drafts,
        "pagination": {
            "page": parsed_page,
            "per_page": parsed_per_page,
            "total_count": total_count,
            "total_pages": total_pages,
            "has_previous": parsed_page > 1,
            "has_next": parsed_page < total_pages,
            "previous_page": parsed_page - 1 if parsed_page > 1 else 1,
            "next_page": parsed_page + 1 if parsed_page < total_pages else total_pages,
        },
    }

def normalize_tracked_pack_id_list(raw_pack_ids):
    pack_ids = []

    for raw_pack_id in raw_pack_ids or []:
        try:
            parsed_id = int(raw_pack_id)
        except (TypeError, ValueError):
            continue

        if parsed_id > 0 and parsed_id not in pack_ids:
            pack_ids.append(parsed_id)

    return pack_ids

def get_tracked_pack_management_rows(static_folder, search_text="", campaign_id=None):
    ensure_tracked_pack_campaign_schema()

    search_value = (search_text or "").strip().lower()
    campaign_key = get_campaign_membership_key(campaign_id)

    conn = get_db_connection()
    cursor = conn.cursor()

    params = [campaign_key]
    where_conditions = ["tpc.campaign_key = ?"]

    if search_value:
        where_conditions.append(
            """
            (
                LOWER(tcp.pack_tracking_code) LIKE ?
                OR LOWER(tcp.set_code) LIKE ?
                OR LOWER(tcp.booster_name) LIKE ?
                OR LOWER(tcp.pack_display_name) LIKE ?
                OR EXISTS (
                    SELECT 1
                    FROM tracked_chaos_pack_cards tcpc
                    WHERE tcpc.tracked_pack_id = tcp.tracked_pack_id
                      AND LOWER(tcpc.card_name) LIKE ?
                )
            )
            """
        )

        like_value = f"%{search_value}%"
        params.extend([like_value, like_value, like_value, like_value, like_value])

    where_clause = "WHERE " + " AND ".join(where_conditions)

    cursor.execute(
        f"""
        SELECT
            tcp.tracked_pack_id,
            tcp.pack_tracking_code,
            tcp.set_code,
            tcp.booster_name,
            tcp.booster_index,
            tcp.pack_display_name,
            tcp.total_cards,
            tcp.bonus_pack_opened,
            tcp.added_at_utc,
            tcp.last_opened_at_utc,
            tcp.opened_count,
            tpc.campaign_id,
            COALESCE(tpc.campaign_enabled, 1) AS campaign_enabled
        FROM tracked_chaos_pack_campaigns tpc
        INNER JOIN tracked_chaos_packs tcp
            ON tcp.tracked_pack_id = tpc.tracked_pack_id
        {where_clause}
        ORDER BY tpc.added_at_utc DESC, tcp.tracked_pack_id DESC
        """,
        params,
    )

    rows = cursor.fetchall()
    conn.close()

    packs = []

    for row in rows:
        set_code = (row["set_code"] or "").strip().upper()
        booster_name = (row["booster_name"] or "").strip().lower()
        art_info = get_chaos_pack_art_info(set_code, booster_name, static_folder)

        packs.append({
            "tracked_pack_id": int(row["tracked_pack_id"]),
            "pack_tracking_code": (row["pack_tracking_code"] or "").strip().upper(),
            "set_code": set_code,
            "booster_name": booster_name,
            "booster_index": int(row["booster_index"] or 0),
            "pack_display_name": (row["pack_display_name"] or "").strip(),
            "total_cards": int(row["total_cards"] or 0),
            "bonus_pack_opened": bool(row["bonus_pack_opened"]),
            "added_at_utc": row["added_at_utc"] or "",
            "last_opened_at_utc": row["last_opened_at_utc"] or "",
            "opened_count": int(row["opened_count"] or 0),
            "campaign_id": int(row["campaign_id"]) if row["campaign_id"] is not None else None,
            "campaign_enabled": int(row["campaign_enabled"] or 0) == 1,
            "image_src": url_for("static", filename=art_info["image_path"]),
        })

    return packs

def set_tracked_packs_campaign_enabled(tracked_pack_ids, campaign_enabled, campaign_id=None):
    ensure_tracked_pack_campaign_schema()

    pack_ids = normalize_tracked_pack_id_list(tracked_pack_ids)

    if not pack_ids:
        return 0

    campaign_key = get_campaign_membership_key(campaign_id)
    placeholders = ",".join(["?"] * len(pack_ids))

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        f"""
        UPDATE tracked_chaos_pack_campaigns
        SET campaign_enabled = ?
        WHERE campaign_key = ?
          AND tracked_pack_id IN ({placeholders})
        """,
        [1 if campaign_enabled else 0, campaign_key] + pack_ids,
    )

    updated_count = cursor.rowcount

    conn.commit()
    conn.close()

    return updated_count

def delete_tracked_packs(tracked_pack_ids, campaign_id=None):
    ensure_tracked_pack_campaign_schema()

    pack_ids = normalize_tracked_pack_id_list(tracked_pack_ids)

    if not pack_ids:
        return 0

    campaign_key = get_campaign_membership_key(campaign_id)
    placeholders = ",".join(["?"] * len(pack_ids))

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        f"""
        DELETE FROM tracked_chaos_pack_campaigns
        WHERE campaign_key = ?
          AND tracked_pack_id IN ({placeholders})
        """,
        [campaign_key] + pack_ids,
    )

    deleted_count = cursor.rowcount

    conn.commit()
    conn.close()

    return deleted_count

def get_tracked_pack_state_by_id(tracked_pack_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            tracked_pack_id,
            pack_tracking_code,
            set_code,
            booster_name,
            booster_index,
            pack_display_name,
            total_cards,
            bonus_pack_opened,
            added_at_utc,
            last_opened_at_utc,
            opened_count,
            COALESCE(campaign_enabled, 1) AS campaign_enabled
        FROM tracked_chaos_packs
        WHERE tracked_pack_id = ?
        """,
        (int(tracked_pack_id),),
    )

    pack_row = cursor.fetchone()

    if not pack_row:
        conn.close()
        return None

    cursor.execute(
        """
        SELECT
            card_order,
            card_uuid,
            card_name,
            set_code,
            booster_name,
            booster_index,
            sheet_name,
            sheet_is_foil,
            sheet_has_balance_colors,
            sheet_total_weight,
            rarity,
            type_line,
            image_url,
            scryfall_id,
            collector_number
        FROM tracked_chaos_pack_cards
        WHERE tracked_pack_id = ?
        ORDER BY card_order ASC
        """,
        (int(tracked_pack_id),),
    )

    card_rows = cursor.fetchall()
    conn.close()

    cards = []

    for card_row in card_rows:
        cards.append({
            "card_uuid": card_row["card_uuid"],
            "card_name": card_row["card_name"],
            "set_code": card_row["set_code"],
            "booster_name": card_row["booster_name"],
            "booster_index": int(card_row["booster_index"] or 0),
            "sheet_name": card_row["sheet_name"],
            "sheet_is_foil": int(card_row["sheet_is_foil"] or 0),
            "sheet_has_balance_colors": int(card_row["sheet_has_balance_colors"] or 0),
            "sheet_total_weight": float(card_row["sheet_total_weight"] or 0),
            "rarity": card_row["rarity"],
            "type_line": card_row["type_line"],
            "image_url": card_row["image_url"],
            "scryfall_id": card_row["scryfall_id"],
            "collector_number": card_row["collector_number"],
        })

    return {
        "tracked_pack_id": int(pack_row["tracked_pack_id"]),
        "pack_tracking_code": (pack_row["pack_tracking_code"] or "").strip().upper(),
        "set_code": (pack_row["set_code"] or "").strip().upper(),
        "booster_name": (pack_row["booster_name"] or "").strip().lower(),
        "booster_index": int(pack_row["booster_index"] or 0),
        "pack_display_name": (pack_row["pack_display_name"] or "").strip(),
        "display_name": (pack_row["pack_display_name"] or "").strip(),
        "total_cards": int(pack_row["total_cards"] or len(cards)),
        "bonus_pack_opened": bool(pack_row["bonus_pack_opened"]),
        "added_at_utc": pack_row["added_at_utc"] or "",
        "last_opened_at_utc": pack_row["last_opened_at_utc"] or "",
        "opened_count": int(pack_row["opened_count"] or 0),
        "campaign_enabled": int(pack_row["campaign_enabled"] or 0) == 1,
        "cards": cards,
    }

def build_tracked_packs_combined_pdf(
    tracked_pack_ids,
    build_chaos_pack_pdf_fn,
    write_debug_log_fn=None,
):
    pack_ids = normalize_tracked_pack_id_list(tracked_pack_ids)

    if not pack_ids:
        raise ValueError("No packs were selected.")

    merger = PdfWriter()
    appended_count = 0

    for tracked_pack_id in pack_ids:
        pack_state = get_tracked_pack_state_by_id(tracked_pack_id)

        if not pack_state:
            continue

        cards = pack_state.get("cards") or []
        if not cards:
            continue

        pack_pdf_buffer = build_chaos_pack_pdf_fn(
            cards,
            pack_state["pack_display_name"],
            set_code=pack_state["set_code"],
            booster_name=pack_state["booster_name"],
            pack_tracking_code=pack_state["pack_tracking_code"],
        )

        merger.append(pack_pdf_buffer)
        appended_count += 1

        if write_debug_log_fn:
            write_debug_log_fn(
                f"MANAGE PACKS PRINT | tracked_pack_id={tracked_pack_id} | "
                f"tracking_code={pack_state['pack_tracking_code']} | cards={len(cards)}"
            )

    if appended_count == 0:
        merger.close()
        raise ValueError("No selected packs could be printed.")

    output_buffer = BytesIO()
    merger.write(output_buffer)
    merger.close()
    output_buffer.seek(0)

    return {
        "buffer": output_buffer,
        "pack_count": appended_count,
    }

def build_campaign_chaos_spin_result(static_folder, write_debug_log_fn=None, campaign_id=None, draft_game_id=None):
    excluded_pack_ids = get_draft_game_selected_pack_ids(draft_game_id)

    campaign_packs = get_tracked_chaos_packs_for_campaign_spin(
        static_folder,
        campaign_id=campaign_id,
        excluded_tracked_pack_ids=excluded_pack_ids,
    )

    if not campaign_packs:
        return None

    shuffled_packs = list(campaign_packs)
    random.shuffle(shuffled_packs)

    display_packs = shuffled_packs[:15]

    if not display_packs:
        return None

    winning_stop_index = random.randint(0, len(display_packs) - 1)
    winning_pack = display_packs[winning_stop_index]

    cards = get_tracked_chaos_pack_cards(winning_pack["tracked_pack_id"])

    if not cards:
        return None

    opened_pack_state = {
        "tracked_pack_id": int(winning_pack["tracked_pack_id"]),
        "set_code": winning_pack["set_code"],
        "booster_name": winning_pack["booster_name"],
        "booster_index": int(winning_pack["booster_index"] or 0),
        "display_name": winning_pack["display_name"],
        "pack_tracking_code": winning_pack["pack_tracking_code"],
        "bonus_pack_opened": bool(winning_pack.get("bonus_pack_opened")),
        "total_cards": len(cards),
        "cards": cards,
        "campaign_mode": True,
        "campaign_id": int(campaign_id) if campaign_id is not None else None,
        "draft_game_id": normalize_optional_int(draft_game_id),
    }

    set_chaos_session_state("pending_opened_pack", opened_pack_state)
    set_chaos_session_state("pending_campaign_pack_opening_recorded", {
        "tracked_pack_id": int(winning_pack["tracked_pack_id"]),
        "recorded": False,
    })

    spin_result = {
        "display_packs": display_packs,
        "winning_pack": {
            "tracked_pack_id": int(winning_pack["tracked_pack_id"]),
            "pack_tracking_code": winning_pack["pack_tracking_code"],
            "set_code": winning_pack["set_code"],
            "booster_name": winning_pack["booster_name"],
            "display_name": winning_pack["display_name"],
            "image_src": winning_pack["image_src"],
            "variant_count": 1,
            "total_variant_weight": 1,
            "opened_count": int(winning_pack.get("opened_count") or 0),
            "campaign_id": int(winning_pack["campaign_id"]) if winning_pack.get("campaign_id") is not None else None,
        },
        "chosen_variant": {
            "tracked_pack_id": int(winning_pack["tracked_pack_id"]),
            "set_code": winning_pack["set_code"],
            "booster_name": winning_pack["booster_name"],
            "booster_index": int(winning_pack["booster_index"] or 0),
            "booster_weight": 1,
            "campaign_id": int(campaign_id) if campaign_id is not None else None,
        },
        "winning_stop_index": winning_stop_index,
        "opened_pack_ready": True,
        "opened_pack_total_cards": len(cards),
        "bonus_pack_opened": bool(winning_pack.get("bonus_pack_opened")),
        "campaign_mode": True,
        "draft_game_id": normalize_optional_int(draft_game_id),
    }

    set_chaos_session_state("pending_spin_result", spin_result)

    if write_debug_log_fn:
        write_debug_log_fn(
            f"CAMPAIGN CHAOS SPIN | tracked_pack_id={winning_pack['tracked_pack_id']} | "
            f"tracking_code={winning_pack['pack_tracking_code']} | cards={len(cards)}"
        )

    return spin_result

def build_chaos_pack_export_text(opened_pack, export_format):
    if not opened_pack:
        raise ValueError("No opened Chaos Draft pack is available.")

    normalized_export_format = (export_format or "").strip().lower()
    if normalized_export_format not in {"archidekt", "moxfield"}:
        raise ValueError("Invalid Chaos Draft export format.")

    cards = opened_pack.get("cards") or []
    if not cards:
        raise ValueError("Opened Chaos Draft pack did not contain any cards.")

    quantity_by_name = {}
    ordered_names = []

    for card in cards:
        card_name = (card.get("card_name") or "").strip()
        if not card_name:
            continue

        if card_name not in quantity_by_name:
            quantity_by_name[card_name] = 0
            ordered_names.append(card_name)

        quantity_by_name[card_name] += 1

    lines = []
    for card_name in ordered_names:
        lines.append(f"{quantity_by_name[card_name]} {card_name}")

    export_text = "\n".join(lines).strip()

    if not export_text:
        raise ValueError("Could not build Chaos Draft export text.")

    return export_text

def build_pending_chaos_pack_pdf(
    build_chaos_pack_pdf_fn,
    write_debug_log_fn,
    safe_filename_fn,
):
    opened_pack = get_chaos_session_state("pending_opened_pack", default_value=None)

    if not opened_pack:
        return {
            "ok": False,
            "message": "No opened Chaos Draft pack is available."
        }

    set_code = (opened_pack.get("set_code") or "").strip().upper()
    booster_name = (opened_pack.get("booster_name") or "").strip().lower()
    display_name = (opened_pack.get("display_name") or "").strip()
    cards = opened_pack.get("cards") or []

    if not set_code or not booster_name or not display_name:
        return {
            "ok": False,
            "message": "Opened Chaos Draft pack data was incomplete."
        }

    if not cards:
        return {
            "ok": False,
            "message": "Opened Chaos Draft pack did not contain any cards."
        }

    pack_tracking_code = (opened_pack.get("pack_tracking_code") or "").strip().upper()

    pdf_buffer = build_chaos_pack_pdf_fn(
        cards,
        display_name,
        set_code=set_code,
        booster_name=booster_name,
        pack_tracking_code=pack_tracking_code,
    )

    filename_safe = safe_filename_fn(f"{set_code}_{booster_name}".lower())
    pdf_bytes = pdf_buffer.getvalue()

    set_chaos_session_state(
        "pending_opened_pack_pdf",
        {
            "filename": f"{filename_safe}.pdf",
            "pdf_base64": pdf_bytes.hex(),
        },
    )

    return {
        "ok": True,
        "filename": f"{filename_safe}.pdf",
        "download_url": url_for("chaos_draft_open_file"),
    }

def build_preprint_chaos_draft_pdf(
    player_count,
    packs_per_player,
    static_folder,
    build_chaos_pack_pdf_fn,
    write_debug_log_fn,
):
    try:
        parsed_player_count = int(player_count)
    except (TypeError, ValueError):
        parsed_player_count = 4

    try:
        parsed_packs_per_player = int(packs_per_player)
    except (TypeError, ValueError):
        parsed_packs_per_player = 3

    if parsed_player_count < 2:
        parsed_player_count = 2
    if parsed_player_count > 12:
        parsed_player_count = 12

    if parsed_packs_per_player < 1:
        parsed_packs_per_player = 1
    if parsed_packs_per_player > 9:
        parsed_packs_per_player = 9

    merger = PdfWriter()
    generated_pack_count = 0

    for player_number in range(1, parsed_player_count + 1):
        for pack_number in range(1, parsed_packs_per_player + 1):
            chosen_result = choose_random_eligible_chaos_pack_variant(static_folder)

            if not chosen_result:
                raise ValueError(
                    f"Not enough eligible Chaos Draft packs remained to finish generation. "
                    f"Generated {generated_pack_count} packs before stopping."
                )

            chosen_pack = chosen_result["pack"]
            chosen_variant = chosen_result["variant"]

            pack_display_name = chosen_pack["display_name"]

            pack_pdf_buffer = build_chaos_pack_pdf_from_variant(
                chosen_variant["set_code"],
                chosen_variant["booster_name"],
                chosen_variant["booster_index"],
                pack_display_name,
                build_chaos_pack_pdf_fn,
                write_debug_log_fn,
            )

            merger.append(pack_pdf_buffer)
            generated_pack_count += 1

            write_debug_log_fn(
                f"PREPRINT CHAOS DRAFT | player={player_number} | pack={pack_number} | "
                f"set={chosen_variant['set_code']} | booster={chosen_variant['booster_name']} | "
                f"booster_index={chosen_variant['booster_index']}"
            )

    output_buffer = BytesIO()
    merger.write(output_buffer)
    merger.close()
    output_buffer.seek(0)

    filename = (
        f"preprint_chaos_draft_"
        f"{parsed_player_count}p_"
        f"{parsed_packs_per_player}x_"
        f"{generated_pack_count}_packs.pdf"
    )

    return {
        "buffer": output_buffer,
        "filename": filename,
        "player_count": parsed_player_count,
        "packs_per_player": parsed_packs_per_player,
        "generated_pack_count": generated_pack_count,
    }