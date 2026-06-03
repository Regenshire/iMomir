import json
from datetime import datetime, timezone

from db.database import get_db_connection


DECK_SOURCE_TYPE_DRAFT_TEST = "draft_test"
DECK_STATUS_ACTIVE = "active"
DECK_STATUS_ARCHIVED = "archived"

DECK_BUILDER_BASIC_LANDS = [
    "Plains",
    "Island",
    "Swamp",
    "Mountain",
    "Forest",
    "Wastes",
]


def deck_utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def normalize_deck_optional_int(value):
    try:
        return int(value) if value is not None and value != "" else None
    except (TypeError, ValueError):
        return None


def normalize_deck_source_type(value):
    clean_value = str(value or "").strip().lower()

    if clean_value in {
        DECK_SOURCE_TYPE_DRAFT_TEST,
        "manual",
        "deckbuilder",
        "custom",
    }:
        return clean_value

    return "manual"


def normalize_deck_zone(value):
    clean_value = str(value or "deck").strip().lower()

    if clean_value in {"deck", "sideboard"}:
        return clean_value

    return "deck"


def ensure_deck_schema():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS decks (
            deck_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_type TEXT NOT NULL DEFAULT 'manual',
            source_id INTEGER,
            campaign_id INTEGER,
            player_id INTEGER,
            deck_name TEXT NOT NULL,
            deck_format TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            default_view_mode TEXT NOT NULL DEFAULT 'grid',
            default_sort_mode TEXT NOT NULL DEFAULT 'rarity-desc',
            notes TEXT,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS deck_cards (
            deck_card_id INTEGER PRIMARY KEY AUTOINCREMENT,
            deck_id INTEGER NOT NULL,
            card_uuid TEXT NOT NULL,
            card_name TEXT NOT NULL,
            deck_zone TEXT NOT NULL DEFAULT 'deck',
            quantity INTEGER NOT NULL DEFAULT 1,
            source_type TEXT,
            source_id INTEGER,
            source_item_id INTEGER,
            is_basic_land INTEGER NOT NULL DEFAULT 0,
            stack_column TEXT,
            stack_order INTEGER,
            display_order INTEGER NOT NULL DEFAULT 0,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT,
            FOREIGN KEY (deck_id) REFERENCES decks (deck_id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS deck_builder_layouts (
            deck_builder_layout_id INTEGER PRIMARY KEY AUTOINCREMENT,
            deck_id INTEGER NOT NULL,
            layout_key TEXT NOT NULL DEFAULT 'default',
            view_mode TEXT NOT NULL DEFAULT 'grid',
            sort_mode TEXT NOT NULL DEFAULT 'rarity-desc',
            sideboard_flex REAL NOT NULL DEFAULT 0.40,
            deck_flex REAL NOT NULL DEFAULT 0.60,
            layout_json TEXT,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT,
            UNIQUE(deck_id, layout_key),
            FOREIGN KEY (deck_id) REFERENCES decks (deck_id)
        )
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_decks_source
        ON decks (source_type, source_id)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_decks_campaign_player
        ON decks (campaign_id, player_id, status)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_deck_cards_deck_zone
        ON deck_cards (deck_id, deck_zone, display_order)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_deck_cards_source
        ON deck_cards (source_type, source_id, source_item_id)
        """
    )

    conn.commit()
    conn.close()

def normalize_deck_basic_land_name(value):
    clean_value = str(value or "").strip()

    for land_name in DECK_BUILDER_BASIC_LANDS:
        if clean_value.lower() == land_name.lower():
            return land_name

    return ""


def get_deck_by_id(deck_id):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)

    if parsed_deck_id is None:
        return None

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM decks
        WHERE deck_id = ?
        """,
        (parsed_deck_id,),
    )

    row = cursor.fetchone()
    conn.close()

    return row


def archive_deck(deck_id):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)

    if parsed_deck_id is None:
        return {
            "ok": False,
            "message": "Invalid deck ID.",
        }

    now_utc = deck_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM decks
        WHERE deck_id = ?
          AND status = ?
        """,
        (
            parsed_deck_id,
            DECK_STATUS_ACTIVE,
        ),
    )

    deck_row = cursor.fetchone()

    if not deck_row:
        conn.close()
        return {
            "ok": False,
            "message": "Deck was not found or is already deleted.",
        }

    cursor.execute(
        """
        UPDATE decks
        SET status = ?,
            updated_at_utc = ?
        WHERE deck_id = ?
        """,
        (
            DECK_STATUS_ARCHIVED,
            now_utc,
            parsed_deck_id,
        ),
    )

    conn.commit()
    conn.close()

    return {
        "ok": True,
        "message": "Deck deleted.",
        "deck_id": parsed_deck_id,
        "source_type": deck_row["source_type"] or "",
        "source_id": deck_row["source_id"],
    }


def get_deckbuilder_basic_land_card_row(cursor, land_name):
    clean_land_name = normalize_deck_basic_land_name(land_name)

    if not clean_land_name:
        return None

    cursor.execute(
        """
        SELECT
            card_uuid,
            card_name,
            set_code,
            collector_number,
            rarity,
            type_line,
            mana_value,
            image_url
        FROM chaos_cards
        WHERE LOWER(card_name) = LOWER(?)
          AND LOWER(type_line) LIKE '%basic land%'
        ORDER BY
            CASE
                WHEN LOWER(set_code) IN ('fdn', 'dmu', 'neo', 'znr') THEN 0
                ELSE 1
            END,
            set_code DESC,
            collector_number ASC
        LIMIT 1
        """,
        (clean_land_name,),
    )

    return cursor.fetchone()


def get_basic_land_counts_for_deck(deck_id):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)

    counts = {
        land_name: 0
        for land_name in DECK_BUILDER_BASIC_LANDS
    }

    if parsed_deck_id is None:
        return counts

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            card_name,
            SUM(quantity) AS land_count
        FROM deck_cards
        WHERE deck_id = ?
          AND deck_zone = 'deck'
          AND is_basic_land = 1
        GROUP BY card_name
        """,
        (parsed_deck_id,),
    )

    rows = cursor.fetchall()
    conn.close()

    for row in rows:
        land_name = normalize_deck_basic_land_name(row["card_name"])

        if land_name:
            counts[land_name] = int(row["land_count"] or 0)

    return counts


def get_saved_basic_land_cards_for_deck(deck_id):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)

    if parsed_deck_id is None:
        return []

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            dc.deck_card_id,
            dc.card_uuid,
            dc.card_name,
            dc.deck_zone,
            dc.quantity,
            dc.is_basic_land,
            cc.set_code,
            cc.collector_number,
            cc.rarity,
            cc.type_line,
            cc.mana_value,
            cc.mana_cost,
            cc.colors_json,
            cc.color_identity_json,
            cc.image_url
        FROM deck_cards dc
        LEFT JOIN chaos_cards cc
            ON cc.card_uuid = dc.card_uuid
        WHERE dc.deck_id = ?
          AND dc.deck_zone = 'deck'
          AND dc.is_basic_land = 1
        ORDER BY
            dc.card_name COLLATE NOCASE ASC,
            dc.deck_card_id ASC
        """,
        (parsed_deck_id,),
    )

    rows = cursor.fetchall()
    conn.close()

    cards = []

    for row in rows:
        quantity = int(row["quantity"] or 1)

        if quantity < 1:
            continue

        for copy_index in range(quantity):
            cards.append({
                "draft_test_pick_id": f"deckcard_{row['deck_card_id']}_{copy_index + 1}",
                "card_uuid": row["card_uuid"] or "",
                "card_name": row["card_name"] or "",
                "deck_zone": "deck",
                "pick_number": 0,
                "pack_number": 0,
                "pick_reason": "Basic land",
                "is_basic_land": 1,
                "set_code": row["set_code"] or "",
                "collector_number": row["collector_number"] or "",
                "rarity": row["rarity"] or "common",
                "type_line": row["type_line"] or "Basic Land",
                "mana_value": row["mana_value"] if row["mana_value"] is not None else 0,
                "mana_cost": row["mana_cost"] or "",
                "colors_json": row["colors_json"] or "[]",
                "color_identity_json": row["color_identity_json"] or "[]",
                "image_url": row["image_url"] or "",
            })

    return cards


def add_basic_land_to_deck(deck_id, land_name):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)
    clean_land_name = normalize_deck_basic_land_name(land_name)

    if parsed_deck_id is None:
        return {
            "ok": False,
            "message": "Invalid deck ID.",
        }

    if not clean_land_name:
        return {
            "ok": False,
            "message": "Invalid basic land.",
        }

    now_utc = deck_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM decks
        WHERE deck_id = ?
          AND status = 'active'
        """,
        (parsed_deck_id,),
    )
    deck_row = cursor.fetchone()

    if not deck_row:
        conn.close()
        return {
            "ok": False,
            "message": "Deck was not found.",
        }

    land_card_row = get_deckbuilder_basic_land_card_row(
        cursor=cursor,
        land_name=clean_land_name,
    )

    if not land_card_row:
        conn.close()
        return {
            "ok": False,
            "message": f"{clean_land_name} was not found in the card database.",
        }

    cursor.execute(
        """
        SELECT *
        FROM deck_cards
        WHERE deck_id = ?
          AND deck_zone = 'deck'
          AND is_basic_land = 1
          AND LOWER(card_name) = LOWER(?)
        LIMIT 1
        """,
        (
            parsed_deck_id,
            clean_land_name,
        ),
    )
    existing_row = cursor.fetchone()

    if existing_row:
        cursor.execute(
            """
            UPDATE deck_cards
            SET quantity = quantity + 1,
                updated_at_utc = ?
            WHERE deck_card_id = ?
            """,
            (
                now_utc,
                int(existing_row["deck_card_id"]),
            ),
        )
    else:
        cursor.execute(
            """
            INSERT INTO deck_cards (
                deck_id,
                card_uuid,
                card_name,
                deck_zone,
                quantity,
                source_type,
                source_id,
                source_item_id,
                is_basic_land,
                stack_column,
                stack_order,
                display_order,
                created_at_utc,
                updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                parsed_deck_id,
                land_card_row["card_uuid"] or "",
                clean_land_name,
                "deck",
                1,
                "basic_land",
                parsed_deck_id,
                None,
                1,
                "land",
                None,
                0,
                now_utc,
                now_utc,
            ),
        )

    cursor.execute(
        """
        UPDATE decks
        SET updated_at_utc = ?
        WHERE deck_id = ?
        """,
        (
            now_utc,
            parsed_deck_id,
        ),
    )

    conn.commit()
    conn.close()

    return {
        "ok": True,
        "message": f"Added {clean_land_name}.",
        "land_name": clean_land_name,
        "action": "add",
        "deck_id": parsed_deck_id,
        "card_uuid": land_card_row["card_uuid"] or "",
        "existing_row_updated": bool(existing_row),
    }


def remove_basic_land_from_deck(deck_id, land_name):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)
    clean_land_name = normalize_deck_basic_land_name(land_name)

    if parsed_deck_id is None:
        return {
            "ok": False,
            "message": "Invalid deck ID.",
        }

    if not clean_land_name:
        return {
            "ok": False,
            "message": "Invalid basic land.",
        }

    now_utc = deck_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM deck_cards
        WHERE deck_id = ?
          AND deck_zone = 'deck'
          AND is_basic_land = 1
          AND LOWER(card_name) = LOWER(?)
        LIMIT 1
        """,
        (
            parsed_deck_id,
            clean_land_name,
        ),
    )
    existing_row = cursor.fetchone()

    if not existing_row:
        conn.close()
        return {
            "ok": False,
            "message": f"No {clean_land_name} is currently in the deck.",
        }

    existing_quantity = int(existing_row["quantity"] or 0)

    if existing_quantity > 1:
        cursor.execute(
            """
            UPDATE deck_cards
            SET quantity = quantity - 1,
                updated_at_utc = ?
            WHERE deck_card_id = ?
            """,
            (
                now_utc,
                int(existing_row["deck_card_id"]),
            ),
        )
    else:
        cursor.execute(
            """
            DELETE FROM deck_cards
            WHERE deck_card_id = ?
            """,
            (int(existing_row["deck_card_id"]),),
        )

    cursor.execute(
        """
        UPDATE decks
        SET updated_at_utc = ?
        WHERE deck_id = ?
        """,
        (
            now_utc,
            parsed_deck_id,
        ),
    )

    conn.commit()
    conn.close()

    return {
        "ok": True,
        "message": f"Removed {clean_land_name}.",
        "land_name": clean_land_name,
        "action": "remove",
        "deck_id": parsed_deck_id,
        "previous_quantity": existing_quantity,
    }


def is_draft_basic_land_pick_row(row):
    try:
        return str(row["pick_reason"] or "").strip().lower() == "basic land"
    except Exception:
        return False

def normalize_deck_view_mode(value):
    clean_value = str(value or "grid").strip().lower()

    if clean_value in {"list", "grid", "stack"}:
        return clean_value

    return "grid"


def normalize_deck_sort_mode(value):
    clean_value = str(value or "rarity-desc").strip().lower()

    if clean_value in {
        "name-asc",
        "name-desc",
        "rarity-desc",
        "rarity-asc",
        "mv-asc",
        "mv-desc",
    }:
        return clean_value

    return "rarity-desc"


def normalize_deck_card_size(value):
    try:
        parsed_value = int(value)
    except (TypeError, ValueError):
        parsed_value = 100

    if parsed_value < 70:
        parsed_value = 70

    if parsed_value > 150:
        parsed_value = 150

    return parsed_value


def normalize_deck_flex_value(value, fallback):
    try:
        parsed_value = float(value)
    except (TypeError, ValueError):
        parsed_value = fallback

    if parsed_value < 0.10:
        parsed_value = 0.10

    if parsed_value > 0.90:
        parsed_value = 0.90

    return parsed_value


def get_deck_builder_layout(deck_id, layout_key="default"):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)

    if parsed_deck_id is None:
        return None

    clean_layout_key = str(layout_key or "default").strip() or "default"

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM deck_builder_layouts
        WHERE deck_id = ?
          AND layout_key = ?
        """,
        (
            parsed_deck_id,
            clean_layout_key,
        ),
    )

    row = cursor.fetchone()
    conn.close()

    return row


def get_deck_builder_layout_json(layout_row):
    if not layout_row:
        return {}

    try:
        parsed_json = json.loads(layout_row["layout_json"] or "{}")

        if isinstance(parsed_json, dict):
            return parsed_json
    except Exception:
        pass

    return {}


def update_deck_settings(
    deck_id,
    deck_name,
    view_mode="grid",
    sort_mode="rarity-desc",
    card_size=100,
    sideboard_flex=0.40,
    deck_flex=0.60,
):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)

    if parsed_deck_id is None:
        return {
            "ok": False,
            "message": "Invalid deck ID.",
        }

    clean_deck_name = str(deck_name or "").strip()

    if not clean_deck_name:
        return {
            "ok": False,
            "message": "Deck Name is required.",
        }

    clean_view_mode = normalize_deck_view_mode(view_mode)
    clean_sort_mode = normalize_deck_sort_mode(sort_mode)
    clean_card_size = normalize_deck_card_size(card_size)
    clean_sideboard_flex = normalize_deck_flex_value(sideboard_flex, 0.40)
    clean_deck_flex = normalize_deck_flex_value(deck_flex, 0.60)

    flex_total = clean_sideboard_flex + clean_deck_flex

    if flex_total <= 0:
        clean_sideboard_flex = 0.40
        clean_deck_flex = 0.60
    else:
        clean_sideboard_flex = clean_sideboard_flex / flex_total
        clean_deck_flex = clean_deck_flex / flex_total

    now_utc = deck_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM decks
        WHERE deck_id = ?
          AND status = ?
        """,
        (
            parsed_deck_id,
            DECK_STATUS_ACTIVE,
        ),
    )

    deck_row = cursor.fetchone()

    if not deck_row:
        conn.close()
        return {
            "ok": False,
            "message": "Deck was not found.",
        }

    cursor.execute(
        """
        UPDATE decks
        SET deck_name = ?,
            default_view_mode = ?,
            default_sort_mode = ?,
            updated_at_utc = ?
        WHERE deck_id = ?
        """,
        (
            clean_deck_name,
            clean_view_mode,
            clean_sort_mode,
            now_utc,
            parsed_deck_id,
        ),
    )

    layout_json = {
        "card_size": clean_card_size,
    }

    cursor.execute(
        """
        INSERT INTO deck_builder_layouts (
            deck_id,
            layout_key,
            view_mode,
            sort_mode,
            sideboard_flex,
            deck_flex,
            layout_json,
            created_at_utc,
            updated_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(deck_id, layout_key) DO UPDATE SET
            view_mode = excluded.view_mode,
            sort_mode = excluded.sort_mode,
            sideboard_flex = excluded.sideboard_flex,
            deck_flex = excluded.deck_flex,
            layout_json = excluded.layout_json,
            updated_at_utc = excluded.updated_at_utc
        """,
        (
            parsed_deck_id,
            "default",
            clean_view_mode,
            clean_sort_mode,
            clean_sideboard_flex,
            clean_deck_flex,
            json.dumps(layout_json),
            now_utc,
            now_utc,
        ),
    )

    conn.commit()
    conn.close()

    return {
        "ok": True,
        "message": "Deck saved.",
        "deck_id": parsed_deck_id,
        "deck_name": clean_deck_name,
        "view_mode": clean_view_mode,
        "sort_mode": clean_sort_mode,
        "card_size": clean_card_size,
        "sideboard_flex": clean_sideboard_flex,
        "deck_flex": clean_deck_flex,
    }

def get_loadable_deck_rows(search_text="", limit=100):
    ensure_deck_schema()

    clean_search_text = str(search_text or "").strip().lower()

    try:
        parsed_limit = int(limit or 100)
    except (TypeError, ValueError):
        parsed_limit = 100

    if parsed_limit < 1:
        parsed_limit = 1

    if parsed_limit > 250:
        parsed_limit = 250

    where_clauses = [
        "status = ?",
        "source_type = ?",
    ]
    params = [
        DECK_STATUS_ACTIVE,
        DECK_SOURCE_TYPE_DRAFT_TEST,
    ]

    if clean_search_text:
        where_clauses.append(
            """
            (
                LOWER(deck_name) LIKE ?
                OR LOWER(COALESCE(deck_format, '')) LIKE ?
                OR CAST(COALESCE(source_id, '') AS TEXT) LIKE ?
            )
            """
        )
        like_value = f"%{clean_search_text}%"
        params.extend([
            like_value,
            like_value,
            like_value,
        ])

    sql = f"""
        SELECT *
        FROM decks
        WHERE {" AND ".join(where_clauses)}
        ORDER BY
            COALESCE(updated_at_utc, created_at_utc) DESC,
            deck_id DESC
        LIMIT ?
    """

    params.append(parsed_limit)

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(sql, params)

    rows = cursor.fetchall()
    conn.close()

    return rows

def get_deck_by_source(source_type, source_id):
    ensure_deck_schema()

    clean_source_type = normalize_deck_source_type(source_type)
    parsed_source_id = normalize_deck_optional_int(source_id)

    if parsed_source_id is None:
        return None

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM decks
        WHERE source_type = ?
          AND source_id = ?
          AND status = ?
        ORDER BY deck_id DESC
        LIMIT 1
        """,
        (
            clean_source_type,
            parsed_source_id,
            DECK_STATUS_ACTIVE,
        ),
    )

    row = cursor.fetchone()
    conn.close()

    return row


def get_or_create_deck_for_source(
    source_type,
    source_id,
    deck_name,
    campaign_id=None,
    player_id=None,
    deck_format="Limited",
):
    ensure_deck_schema()

    clean_source_type = normalize_deck_source_type(source_type)
    parsed_source_id = normalize_deck_optional_int(source_id)
    parsed_campaign_id = normalize_deck_optional_int(campaign_id)
    parsed_player_id = normalize_deck_optional_int(player_id)
    clean_deck_name = str(deck_name or "").strip() or "Untitled Deck"
    clean_deck_format = str(deck_format or "").strip() or "Limited"

    existing_deck = get_deck_by_source(
        clean_source_type,
        parsed_source_id,
    )

    if existing_deck:
        return {
            "ok": True,
            "deck_id": int(existing_deck["deck_id"]),
            "created": False,
            "deck": existing_deck,
        }

    now_utc = deck_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO decks (
            source_type,
            source_id,
            campaign_id,
            player_id,
            deck_name,
            deck_format,
            status,
            default_view_mode,
            default_sort_mode,
            notes,
            created_at_utc,
            updated_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            clean_source_type,
            parsed_source_id,
            parsed_campaign_id,
            parsed_player_id,
            clean_deck_name,
            clean_deck_format,
            DECK_STATUS_ACTIVE,
            "grid",
            "rarity-desc",
            "",
            now_utc,
            now_utc,
        ),
    )

    deck_id = int(cursor.lastrowid)

    cursor.execute(
        """
        INSERT INTO deck_builder_layouts (
            deck_id,
            layout_key,
            view_mode,
            sort_mode,
            sideboard_flex,
            deck_flex,
            layout_json,
            created_at_utc,
            updated_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            deck_id,
            "default",
            "grid",
            "rarity-desc",
            0.40,
            0.60,
            "{}",
            now_utc,
            now_utc,
        ),
    )

    conn.commit()
    conn.close()

    return {
        "ok": True,
        "deck_id": deck_id,
        "created": True,
        "deck": get_deck_by_source(clean_source_type, parsed_source_id),
    }


def get_or_create_deck_for_draft_test(draft_state):
    ensure_deck_schema()

    session_row = (draft_state or {}).get("session")
    players = (draft_state or {}).get("players") or []

    if not session_row:
        return {
            "ok": False,
            "message": "Draft session was not found.",
        }

    human_player = None

    for player in players:
        try:
            if int(player["is_human"] or 0) == 1:
                human_player = player
                break
        except Exception:
            pass

    draft_test_id = int(session_row["draft_test_id"])
    campaign_id = session_row["campaign_id"]
    player_id = human_player["campaign_player_id"] if human_player and "campaign_player_id" in human_player.keys() else None

    campaign_name = session_row["campaign_name"] if "campaign_name" in session_row.keys() else ""
    deck_name = f"{campaign_name or 'Draft'} - Test Draft {draft_test_id}"

    return get_or_create_deck_for_source(
        source_type=DECK_SOURCE_TYPE_DRAFT_TEST,
        source_id=draft_test_id,
        campaign_id=campaign_id,
        player_id=player_id,
        deck_name=deck_name,
        deck_format="Limited",
    )

def get_deckbuilder_basic_land_cards():
    ensure_deck_schema()

    conn = get_db_connection()
    cursor = conn.cursor()

    basic_land_cards = []

    for land_name in DECK_BUILDER_BASIC_LANDS:
        cursor.execute(
            """
            SELECT
                card_uuid,
                card_name,
                set_code,
                collector_number,
                rarity,
                type_line,
                mana_value,
                image_url
            FROM chaos_cards
            WHERE LOWER(card_name) = LOWER(?)
              AND LOWER(type_line) LIKE '%basic land%'
            ORDER BY
                CASE
                    WHEN LOWER(set_code) IN ('fdn', 'dmu', 'neo', 'znr') THEN 0
                    ELSE 1
                END,
                set_code DESC,
                collector_number ASC
            LIMIT 1
            """,
            (land_name,),
        )

        row = cursor.fetchone()

        if row:
            basic_land_cards.append({
                "card_uuid": row["card_uuid"] or "",
                "card_name": row["card_name"] or land_name,
                "set_code": row["set_code"] or "",
                "collector_number": row["collector_number"] or "",
                "rarity": row["rarity"] or "common",
                "type_line": row["type_line"] or "Basic Land",
                "mana_value": row["mana_value"] if row["mana_value"] is not None else 0,
                "image_url": row["image_url"] or "",
            })
        else:
            basic_land_cards.append({
                "card_uuid": "",
                "card_name": land_name,
                "set_code": "",
                "collector_number": "",
                "rarity": "common",
                "type_line": "Basic Land",
                "mana_value": 0,
                "image_url": "",
            })

    conn.close()

    return basic_land_cards


def build_deckbuilder_context_for_draft_test(draft_state, preferred_deck_id=None):
    ensure_deck_schema()

    session_row = draft_state.get("session")

    if not session_row:
        return {
            "ok": False,
            "message": "Draft session was not found.",
        }

    preferred_deck = get_deck_by_id(preferred_deck_id)

    if (
        preferred_deck
        and (preferred_deck["source_type"] or "") == DECK_SOURCE_TYPE_DRAFT_TEST
        and int(preferred_deck["source_id"] or 0) == int(session_row["draft_test_id"])
    ):
        deck_result = {
            "ok": True,
            "deck_id": int(preferred_deck["deck_id"]),
            "created": False,
            "deck": preferred_deck,
        }
    else:
        deck_result = get_or_create_deck_for_draft_test(draft_state)

    if not deck_result.get("ok"):
        return {
            "ok": False,
            "message": deck_result.get("message") or "Could not create deckbuilder context.",
        }

    players = draft_state.get("players") or []
    players = draft_state.get("players") or []

    human_player = None

    for player in players:
        try:
            if int(player["is_human"] or 0) == 1:
                human_player = player
                break
        except Exception:
            pass

    campaign_name = session_row["campaign_name"] if "campaign_name" in session_row.keys() else "No Campaign"

    deck_row = deck_result.get("deck")
    layout_row = get_deck_builder_layout(deck_result["deck_id"])
    layout_json = get_deck_builder_layout_json(layout_row)

    default_view_mode = normalize_deck_view_mode(
        layout_row["view_mode"] if layout_row else (deck_row["default_view_mode"] if deck_row else "grid")
    )
    default_sort_mode = normalize_deck_sort_mode(
        layout_row["sort_mode"] if layout_row else (deck_row["default_sort_mode"] if deck_row else "rarity-desc")
    )
    default_card_size = normalize_deck_card_size(layout_json.get("card_size", 100))
    sideboard_flex = normalize_deck_flex_value(layout_row["sideboard_flex"] if layout_row else 0.40, 0.40)
    deck_flex = normalize_deck_flex_value(layout_row["deck_flex"] if layout_row else 0.60, 0.60)

    return {
        "ok": True,
        "deck_id": deck_result["deck_id"],
        "source_type": DECK_SOURCE_TYPE_DRAFT_TEST,
        "source_id": int(session_row["draft_test_id"]),
        "title": f"{campaign_name or 'No Campaign'} - Deck Builder",
        "subtitle": (
            f"Draft complete • {session_row['pod_size']} players "
            f"• {session_row['packs_per_player']} pack(s) each"
        ),
        "mode_label": "Draft Test",
        "deck_name": deck_result["deck"]["deck_name"] if deck_result.get("deck") else "",
        "deck_format": deck_result["deck"]["deck_format"] if deck_result.get("deck") else "Limited",
        "human_player": human_player,
        "deck_cards": [
            card
            for card in (draft_state.get("human_deck_cards") or [])
            if not is_draft_basic_land_pick_row(card)
        ] + get_saved_basic_land_cards_for_deck(deck_result["deck_id"]),
        "sideboard_cards": [
            card
            for card in (draft_state.get("human_sideboard_cards") or [])
            if not is_draft_basic_land_pick_row(card)
        ],
        "basic_land_names": draft_state.get("basic_land_names") or DECK_BUILDER_BASIC_LANDS,
        "basic_land_cards": get_deckbuilder_basic_land_cards(),
        "basic_land_counts": get_basic_land_counts_for_deck(deck_result["deck_id"]),
        "default_view_mode": default_view_mode,
        "default_sort_mode": default_sort_mode,
        "default_card_size": default_card_size,
        "sideboard_flex": sideboard_flex,
        "deck_flex": deck_flex,
    }