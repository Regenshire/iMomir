from settings import PRIMARY_TYPE_KEYS, SUPPLEMENTAL_TYPE_KEYS
from db.database import get_config, get_db_connection, get_selected_set_codes


def get_enabled_type_options(config):
    enabled_types = []

    for key, label in PRIMARY_TYPE_KEYS + SUPPLEMENTAL_TYPE_KEYS:
        if config.get(key) == "1":
            enabled_types.append({
                "value": key.replace("type_", ""),
                "label": label,
                "column": key.replace("type_", "is_"),
            })

    return enabled_types


def resolve_selected_result_type(config, requested_type_value):
    enabled_types = get_enabled_type_options(config)

    if not enabled_types:
        return {
            "enabled_types": [],
            "selected_value": "",
            "selected_label": "",
            "selected_column": "",
        }

    requested_type_value = (requested_type_value or "").strip().lower()

    for item in enabled_types:
        if item["value"] == requested_type_value:
            return {
                "enabled_types": enabled_types,
                "selected_value": item["value"],
                "selected_label": item["label"],
                "selected_column": item["column"],
            }

    first_item = enabled_types[0]
    return {
        "enabled_types": enabled_types,
        "selected_value": first_item["value"],
        "selected_label": first_item["label"],
        "selected_column": first_item["column"],
    }


def build_enabled_type_conditions(config, game_mode):
    if game_mode == "momir_basic":
        return ["is_creature = 1"]

    if game_mode == "momir_planeswalker":
        return ["is_creature = 1", "is_planeswalker = 1"]

    if game_mode == "planechase":
        return ["is_plane = 1"]

    if game_mode == "archenemy":
        return ["is_scheme = 1"]

    type_conditions = []

    for key, _ in PRIMARY_TYPE_KEYS + SUPPLEMENTAL_TYPE_KEYS:
        if config.get(key) == "1":
            column = key.replace("type_", "is_")
            type_conditions.append(f"{column} = 1")

    return type_conditions


def build_card_filter_query(mana_value, config, selected_set_codes, selected_type_value=None):
    conditions = []
    params = []

    conditions.append("disable_card = 0")

    conditions.append("CAST(mana_value AS INTEGER) = ?")
    params.append(int(mana_value))

    game_mode = (config.get("game_mode") or "custom").strip().lower()

    if game_mode == "momir_select":
        selected_type_info = resolve_selected_result_type(config, selected_type_value)
        if selected_type_info["selected_column"]:
            conditions.append(f"({selected_type_info['selected_column']} = 1)")
    else:
        type_conditions = build_enabled_type_conditions(config, game_mode)

        if type_conditions:
            conditions.append("(" + " OR ".join(type_conditions) + ")")

    if game_mode == "momir_legends":
        conditions.append("is_legendary = 1")
    elif config.get("allow_legendary") == "0":
        conditions.append("is_legendary = 0")

    if config.get("allow_unsets") == "0":
        conditions.append("is_unset = 0")

    if config.get("allow_arena") == "0":
        conditions.append("has_paper_printing = 1")

    conditions.append("(is_creature = 0 OR mana_cost IS NOT NULL)")

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

    if game_mode == "momir_legends":
        conditions.append("LOWER(COALESCE(rarity, '')) IN ('rare', 'mythic')")

    if game_mode == "momir_battleship":
        conditions.append("CAST(mana_value AS INTEGER) >= 5")

    if game_mode == "momir_aggro":
        conditions.append("CAST(mana_value AS INTEGER) <= 4")

    if game_mode == "momir_odds":
        conditions.append("(CAST(mana_value AS INTEGER) % 2) = 1")

    if game_mode == "momir_evens":
        conditions.append("(CAST(mana_value AS INTEGER) % 2) = 0")

    if game_mode == "momir_prime":
        conditions.append("CAST(mana_value AS INTEGER) IN (2, 3, 5, 6, 11, 13, 17, 19)")

    if config.get("all_sets_enabled") == "0" and selected_set_codes:
        set_conditions = []
        for code in selected_set_codes:
            set_conditions.append("printings_json LIKE ?")
            params.append(f'%"{code}"%')

        if set_conditions:
            conditions.append("(" + " OR ".join(set_conditions) + ")")

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    return where_clause, params


def draw_random_card(mana_value, selected_type_value=None):
    config = get_config()
    selected_set_codes = get_selected_set_codes()

    where_clause, params = build_card_filter_query(
        mana_value,
        config,
        selected_set_codes,
        selected_type_value=selected_type_value,
    )

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