import os
import random
from datetime import datetime, timezone

from settings import PRIMARY_TYPE_KEYS
from db.database import get_config, get_db_connection, get_selected_set_codes


def cleanup_card_history():
    cutoff_utc = datetime.now(timezone.utc).timestamp() - 86400
    cutoff_text = datetime.fromtimestamp(cutoff_utc, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        DELETE FROM card_history
        WHERE drawn_at_utc < ?
        """,
        (cutoff_text,),
    )

    conn.commit()
    conn.close()


def clear_card_history():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM card_history")

    conn.commit()
    conn.close()


def record_card_history(card_key):
    if not card_key:
        return

    cleanup_card_history()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO card_history (
            card_key,
            drawn_at_utc
        )
        VALUES (?, ?)
        """,
        (
            card_key,
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        ),
    )

    conn.commit()
    conn.close()


def get_recent_history_count():
    cleanup_card_history()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT COUNT(*) AS history_count
        FROM card_history
        """
    )

    row = cursor.fetchone()
    conn.close()

    return int(row["history_count"] or 0)


def get_draws_since_last_land():
    cleanup_card_history()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            ch.card_key,
            COALESCE(c.is_land, 0) AS is_land
        FROM card_history ch
        LEFT JOIN cards c
            ON c.card_key = ch.card_key
        ORDER BY ch.history_id DESC
        LIMIT 10
        """
    )

    rows = cursor.fetchall()
    conn.close()

    draws_since_last_land = 0

    for row in rows:
        if int(row["is_land"] or 0) == 1:
            return draws_since_last_land

        draws_since_last_land += 1

    return draws_since_last_land


def get_draws_since_non_land():
    cleanup_card_history()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            ch.card_key,
            COALESCE(c.is_land, 0) AS is_land
        FROM card_history ch
        LEFT JOIN cards c
            ON c.card_key = ch.card_key
        ORDER BY ch.history_id DESC
        LIMIT 10
        """
    )

    rows = cursor.fetchall()
    conn.close()

    draws_since_non_land = 0

    for row in rows:
        if int(row["is_land"] or 0) == 0:
            return draws_since_non_land

        draws_since_non_land += 1

    return draws_since_non_land


def append_common_draw_filters(conditions, params, config, selected_set_codes):
    conditions.append("disable_card = 0")

    if config.get("allow_unsets") == "0":
        conditions.append("is_unset = 0")

    if config.get("allow_arena") == "0":
        conditions.append("has_paper_printing = 1")

    if config.get("allow_repeats") == "0":
        conditions.append(
            """
            (
                LOWER(name) IN (
                    'plains',
                    'snow-covered plains',
                    'island',
                    'snow-covered island',
                    'swamp',
                    'snow-covered swamp',
                    'mountain',
                    'snow-covered mountain',
                    'forest',
                    'snow-covered forest',
                    'wastes',
                    'snow-covered wastes'
                )
                OR card_key NOT IN (
                    SELECT card_key
                    FROM card_history
                )
            )
            """
        )

    if config.get("all_sets_enabled") == "0" and selected_set_codes:
        set_conditions = []
        for code in selected_set_codes:
            set_conditions.append("printings_json LIKE ?")
            params.append(f'%"{code}"%')

        if set_conditions:
            conditions.append("(" + " OR ".join(set_conditions) + ")")


def fetch_random_card_by_conditions(conditions, params):
    where_clause = " AND ".join(conditions) if conditions else "1=1"

    conn = get_db_connection()
    cursor = conn.cursor()

    query = f"""
        SELECT *
        FROM cards
        WHERE {where_clause}
        ORDER BY RANDOM()
        LIMIT 1
    """

    cursor.execute(query, params)
    row = cursor.fetchone()
    conn.close()

    return row


def get_enabled_primary_type_options(config):
    enabled_primary_types = []

    for key, label in PRIMARY_TYPE_KEYS:
        if config.get(key) == "1":
            enabled_primary_types.append({
                "value": key.replace("type_", ""),
                "label": label,
                "column": key.replace("type_", "is_"),
            })

    if not enabled_primary_types:
        for key, label in PRIMARY_TYPE_KEYS:
            enabled_primary_types.append({
                "value": key.replace("type_", ""),
                "label": label,
                "column": key.replace("type_", "is_"),
            })

    return enabled_primary_types


def draw_tower_named_land(config, selected_set_codes, preferred_name, fallback_name=None):
    for chosen_name in [preferred_name, fallback_name]:
        if not chosen_name:
            continue

        conditions = []
        params = []

        append_common_draw_filters(conditions, params, config, selected_set_codes)
        conditions.append("is_land = 1")
        conditions.append("LOWER(name) = ?")
        params.append(chosen_name.strip().lower())

        row = fetch_random_card_by_conditions(conditions, params)
        if row:
            return row

    return None


def draw_tower_other_land(config, selected_set_codes):
    conditions = []
    params = []

    append_common_draw_filters(conditions, params, config, selected_set_codes)
    conditions.append("is_land = 1")
    conditions.append(
        """
        LOWER(name) NOT IN (
            'plains',
            'snow-covered plains',
            'island',
            'snow-covered island',
            'swamp',
            'snow-covered swamp',
            'mountain',
            'snow-covered mountain',
            'forest',
            'snow-covered forest',
            'wastes',
            'snow-covered wastes'
        )
        """
    )

    return fetch_random_card_by_conditions(conditions, params)


def draw_tower_any_land(config, selected_set_codes):
    conditions = []
    params = []

    append_common_draw_filters(conditions, params, config, selected_set_codes)
    conditions.append("is_land = 1")

    return fetch_random_card_by_conditions(conditions, params)


def draw_tower_land_card(config, selected_set_codes):
    land_roll = random.random()

    if land_roll < 0.14:
        use_snow = random.random() < 0.25
        return draw_tower_named_land(
            config,
            selected_set_codes,
            "snow-covered plains" if use_snow else "plains",
            "plains" if use_snow else "snow-covered plains",
        )

    if land_roll < 0.28:
        use_snow = random.random() < 0.25
        return draw_tower_named_land(
            config,
            selected_set_codes,
            "snow-covered island" if use_snow else "island",
            "island" if use_snow else "snow-covered island",
        )

    if land_roll < 0.42:
        use_snow = random.random() < 0.25
        return draw_tower_named_land(
            config,
            selected_set_codes,
            "snow-covered swamp" if use_snow else "swamp",
            "swamp" if use_snow else "snow-covered swamp",
        )

    if land_roll < 0.56:
        use_snow = random.random() < 0.25
        return draw_tower_named_land(
            config,
            selected_set_codes,
            "snow-covered mountain" if use_snow else "mountain",
            "mountain" if use_snow else "snow-covered mountain",
        )

    if land_roll < 0.70:
        use_snow = random.random() < 0.25
        return draw_tower_named_land(
            config,
            selected_set_codes,
            "snow-covered forest" if use_snow else "forest",
            "forest" if use_snow else "snow-covered forest",
        )

    if land_roll < 0.84:
        use_snow = random.random() < 0.25
        return draw_tower_named_land(
            config,
            selected_set_codes,
            "snow-covered wastes" if use_snow else "wastes",
            "wastes",
        )

    return draw_tower_other_land(config, selected_set_codes)


def draw_tower_nonland_card(config, selected_set_codes):
    enabled_primary_types = get_enabled_primary_type_options(config)

    conditions = []
    params = []

    append_common_draw_filters(conditions, params, config, selected_set_codes)

    nonland_type_conditions = []
    for type_option in enabled_primary_types:
        if type_option["value"] == "land":
            continue
        nonland_type_conditions.append(f"{type_option['column']} = 1")

    if nonland_type_conditions:
        conditions.append("(" + " OR ".join(nonland_type_conditions) + ")")

    conditions.append("is_land = 0")
    conditions.append("(is_creature = 0 OR (mana_cost IS NOT NULL AND TRIM(mana_cost) <> ''))")

    return fetch_random_card_by_conditions(conditions, params)


def draw_random_tower_of_power_card():
    cleanup_card_history()

    config = get_config()
    selected_set_codes = get_selected_set_codes()
    draws_since_last_land = get_draws_since_last_land()
    draws_since_last_nonland = get_draws_since_non_land()

    force_land_draw = draws_since_last_land > 4
    force_nonland_draw = draws_since_last_nonland > 2

    if (force_land_draw or random.random() < 0.42) and force_nonland_draw == False:
        land_card = draw_tower_land_card(config, selected_set_codes)
        if land_card:
            return land_card

        fallback_land_card = draw_tower_any_land(config, selected_set_codes)
        if fallback_land_card:
            return fallback_land_card

    return draw_tower_nonland_card(config, selected_set_codes)


def draw_tower_of_power_batch_cards(draw_count, ensure_card_image_cached_fn):
    cards = []

    try:
        parsed_draw_count = int(draw_count)
    except (TypeError, ValueError):
        parsed_draw_count = 7

    if parsed_draw_count < 1:
        parsed_draw_count = 1
    if parsed_draw_count > 100:
        parsed_draw_count = 100

    for _ in range(parsed_draw_count):
        card = draw_random_tower_of_power_card()

        if not card:
            break

        existing_cache_path = card["image_cache_path"] or ""
        cache_exists = False

        if existing_cache_path:
            cache_exists = os.path.exists(os.path.abspath(existing_cache_path))

        if not cache_exists:
            card = ensure_card_image_cached_fn(card)

        if not card:
            continue

        record_card_history(card["card_key"])
        cards.append(card)

    return cards