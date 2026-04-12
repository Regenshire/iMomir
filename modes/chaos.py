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


def build_chaos_spin_result(static_folder):
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
    }

    set_chaos_session_state("pending_spin_result", spin_result)

    return spin_result


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

    return build_chaos_pack_pdf_fn(
        cards,
        pack_display_name,
        set_code=set_code,
        booster_name=booster_name,
    )


def build_pending_chaos_pack_pdf(
    build_chaos_pack_pdf_fn,
    write_debug_log_fn,
    safe_filename_fn,
):
    spin_result = get_pending_chaos_spin_result()

    if not spin_result:
        return {
            "ok": False,
            "message": "No pending Chaos Draft spin found."
        }

    chosen_variant = spin_result.get("chosen_variant") or {}

    set_code = chosen_variant.get("set_code")
    booster_name = chosen_variant.get("booster_name")
    booster_index = chosen_variant.get("booster_index")

    if not set_code or booster_index is None:
        return {
            "ok": False,
            "message": "Invalid Chaos Draft selection."
        }

    open_result = open_chaos_pack_with_bonus_rule(
        set_code,
        booster_name,
        booster_index,
        write_debug_log_fn,
    )

    cards = open_result["cards"]

    if not cards:
        return {
            "ok": False,
            "message": "Chaos Draft pack opened but no cards were generated."
        }

    cards = sort_opened_chaos_pack_cards(cards, booster_name, write_debug_log_fn)

    record_chaos_pack_history(
        set_code,
        booster_name,
        booster_index,
        spin_result["winning_pack"]["display_name"],
    )

    pdf_buffer = build_chaos_pack_pdf_fn(
        cards,
        spin_result["winning_pack"]["display_name"],
        set_code=set_code,
        booster_name=booster_name,
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