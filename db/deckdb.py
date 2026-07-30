import json
from datetime import datetime, timezone

from db.database import get_db_connection


DECK_SOURCE_TYPE_DRAFT_TEST = "draft_test"
DECK_SOURCE_TYPE_STANDALONE = "standalone"
DECK_STATUS_ACTIVE = "active"
DECK_STATUS_ARCHIVED = "archived"

DECK_ROLE_MAIN = "main"
DECK_ROLE_COMMANDER = "commander"
DECK_ROLE_PARTNER = "partner"

DECK_ROLE_OPTIONS = {
    DECK_ROLE_MAIN,
    DECK_ROLE_COMMANDER,
    DECK_ROLE_PARTNER,
}

DECK_FORMAT_LIMITED = "Limited"
DECK_FORMAT_STANDARD = "Standard"
DECK_FORMAT_COMMANDER = "Commander"
DECK_FORMAT_MODERN = "Modern"
DECK_FORMAT_PIONEER = "Pioneer"
DECK_FORMAT_ETERNAL = "Eternal"
DECK_FORMAT_PAUPER = "Pauper"

DECK_FORMAT_OPTIONS = [
    DECK_FORMAT_LIMITED,
    DECK_FORMAT_STANDARD,
    DECK_FORMAT_COMMANDER,
    DECK_FORMAT_MODERN,
    DECK_FORMAT_PIONEER,
    DECK_FORMAT_ETERNAL,
    DECK_FORMAT_PAUPER,
]

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
        DECK_SOURCE_TYPE_STANDALONE,
        "manual",
        "deckbuilder",
        "custom",
    }:
        return clean_value

    return DECK_SOURCE_TYPE_STANDALONE


def normalize_deck_format(value, fallback=DECK_FORMAT_STANDARD):
    clean_value = str(value or "").strip().lower()

    if clean_value in {"limited", "draft"}:
        return DECK_FORMAT_LIMITED

    if clean_value == "standard":
        return DECK_FORMAT_STANDARD

    if clean_value in {"commander", "edh"}:
        return DECK_FORMAT_COMMANDER

    if clean_value == "modern":
        return DECK_FORMAT_MODERN

    if clean_value == "pioneer":
        return DECK_FORMAT_PIONEER

    if clean_value in {"eternal", "legacy", "vintage"}:
        return DECK_FORMAT_ETERNAL

    if clean_value == "pauper":
        return DECK_FORMAT_PAUPER

    return fallback if fallback in DECK_FORMAT_OPTIONS else DECK_FORMAT_STANDARD


def normalize_deck_zone(value):
    clean_value = str(value or "deck").strip().lower()

    if clean_value in {"deck", "sideboard", "removed"}:
        return clean_value

    return "deck"

def normalize_deck_role(value):
    clean_value = str(value or DECK_ROLE_MAIN).strip().lower()

    if clean_value in DECK_ROLE_OPTIONS:
        return clean_value

    return DECK_ROLE_MAIN


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
            sheet_is_foil INTEGER NOT NULL DEFAULT 0,
            deck_role TEXT NOT NULL DEFAULT 'main',
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
        CREATE TABLE IF NOT EXISTS deck_basic_land_printings (
            deck_basic_land_printing_id INTEGER PRIMARY KEY AUTOINCREMENT,
            deck_id INTEGER NOT NULL,
            land_name TEXT NOT NULL,
            card_uuid TEXT NOT NULL,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT,
            UNIQUE(deck_id, land_name),
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

    cursor.execute(
        """
        PRAGMA table_info(deck_cards)
        """
    )

    deck_card_columns = {
        row["name"]
        for row in cursor.fetchall()
    }

    if "sheet_is_foil" not in deck_card_columns:
        cursor.execute(
            """
            ALTER TABLE deck_cards
            ADD COLUMN sheet_is_foil INTEGER NOT NULL DEFAULT 0
            """
        )

    if "deck_role" not in deck_card_columns:
        cursor.execute(
            """
            ALTER TABLE deck_cards
            ADD COLUMN deck_role TEXT NOT NULL DEFAULT 'main'
            """
        )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_deck_basic_land_printings_deck
        ON deck_basic_land_printings (deck_id, land_name)
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


def get_deckbuilder_basic_land_card_row(cursor, land_name, deck_id=None):
    clean_land_name = normalize_deck_basic_land_name(land_name)
    parsed_deck_id = normalize_deck_optional_int(deck_id)

    if not clean_land_name:
        return None

    if parsed_deck_id is not None:
        cursor.execute(
            """
            SELECT
                cc.card_uuid,
                cc.card_name,
                cc.set_code,
                cc.collector_number,
                cc.rarity,
                cc.type_line,
                cc.mana_value,
                cc.image_url
            FROM deck_basic_land_printings dblp
            INNER JOIN chaos_cards cc
                ON cc.card_uuid = dblp.card_uuid
            WHERE dblp.deck_id = ?
              AND LOWER(dblp.land_name) = LOWER(?)
            LIMIT 1
            """,
            (
                parsed_deck_id,
                clean_land_name,
            ),
        )

        override_row = cursor.fetchone()

        if override_row:
            return override_row

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

def deckbuilder_row_get(row, key, default=None):
    if row is None:
        return default

    try:
        if hasattr(row, "keys") and key in row.keys():
            return row[key]
    except Exception:
        pass

    try:
        if isinstance(row, dict):
            return row.get(key, default)
    except Exception:
        pass

    return default


def import_draft_cards_into_deck(deck_id, draft_cards, default_zone):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)
    clean_default_zone = normalize_deck_zone(default_zone)

    if parsed_deck_id is None:
        return {
            "ok": False,
            "message": "Invalid deck ID.",
        }

    if clean_default_zone not in {"deck", "sideboard"}:
        clean_default_zone = "deck"

    now_utc = deck_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    imported_count = 0

    for card in draft_cards or []:
        draft_test_pick_id = normalize_deck_optional_int(deckbuilder_row_get(card, "draft_test_pick_id"))

        if draft_test_pick_id is None:
            continue

        card_uuid = str(deckbuilder_row_get(card, "card_uuid", "") or "").strip()
        card_name = str(deckbuilder_row_get(card, "card_name", "") or "").strip()

        if not card_uuid or not card_name:
            continue

        cursor.execute(
            """
            SELECT deck_card_id
            FROM deck_cards
            WHERE deck_id = ?
              AND source_type = ?
              AND source_item_id = ?
            LIMIT 1
            """,
            (
                parsed_deck_id,
                "draft_pick",
                draft_test_pick_id,
            ),
        )

        existing_row = cursor.fetchone()

        if existing_row:
            continue

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
                sheet_is_foil,
                deck_role,
                stack_column,
                stack_order,
                display_order,
                created_at_utc,
                updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                parsed_deck_id,
                card_uuid,
                card_name,
                clean_default_zone,
                1,
                "draft_pick",
                normalize_deck_optional_int(deckbuilder_row_get(card, "draft_test_id")),
                draft_test_pick_id,
                0,
                0,
                DECK_ROLE_MAIN,
                None,
                None,
                0,
                now_utc,
                now_utc,
            ),
        )

        imported_count += 1

    if imported_count > 0:
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
        "message": f"Imported {imported_count} drafted card(s).",
        "imported_count": imported_count,
    }


def import_draft_state_into_deck(deck_id, draft_state):
    deck_cards = [
        card
        for card in ((draft_state or {}).get("human_deck_cards") or [])
        if not is_draft_basic_land_pick_row(card)
    ]

    sideboard_cards = [
        card
        for card in ((draft_state or {}).get("human_sideboard_cards") or [])
        if not is_draft_basic_land_pick_row(card)
    ]

    deck_result = import_draft_cards_into_deck(
        deck_id=deck_id,
        draft_cards=deck_cards,
        default_zone="deck",
    )

    if not deck_result.get("ok"):
        return deck_result

    sideboard_result = import_draft_cards_into_deck(
        deck_id=deck_id,
        draft_cards=sideboard_cards,
        default_zone="sideboard",
    )

    if not sideboard_result.get("ok"):
        return sideboard_result

    return {
        "ok": True,
        "message": "Draft cards imported into Deck Builder.",
        "deck_imported_count": deck_result.get("imported_count", 0),
        "sideboard_imported_count": sideboard_result.get("imported_count", 0),
    }


def get_saved_deckbuilder_cards_for_deck(deck_id, deck_zone=None, include_basic_lands=False):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)

    if parsed_deck_id is None:
        return []

    clean_deck_zone = normalize_deck_zone(deck_zone) if deck_zone else ""

    where_clauses = [
        "dc.deck_id = ?",
    ]
    params = [parsed_deck_id]

    if clean_deck_zone:
        where_clauses.append("dc.deck_zone = ?")
        params.append(clean_deck_zone)
    else:
        where_clauses.append("dc.deck_zone IN ('deck', 'sideboard')")

    if not include_basic_lands:
        where_clauses.append("dc.is_basic_land = 0")

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        f"""
        SELECT
            dc.deck_card_id,
            dc.card_uuid,
            dc.card_name,
            dc.deck_zone,
            dc.quantity,
            dc.source_type,
            dc.source_id,
            dc.source_item_id,
            dc.is_basic_land,
            dc.sheet_is_foil,
            dc.deck_role,
            dc.stack_column,
            dc.stack_order,
            dc.display_order,
            cc.set_code,
            cc.collector_number,
            cc.rarity,
            cc.type_line,
            cc.mana_value,
            cc.mana_cost,
            cc.colors_json,
            cc.color_identity_json,
            cc.image_url,
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM alternate_sources alt
                    WHERE alt.card_uuid = dc.card_uuid
                      AND alt.is_enabled = 1
                    LIMIT 1
                ) THEN 1
                ELSE 0
            END AS has_alternate_image,
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM alternate_sources alt
                    WHERE alt.card_uuid = dc.card_uuid
                      AND alt.is_enabled = 1
                      AND alt.remove_bleed = 1
                    LIMIT 1
                ) THEN 1
                ELSE 0
            END AS alternate_image_remove_bleed
        FROM deck_cards dc
        LEFT JOIN chaos_cards cc
            ON cc.card_uuid = dc.card_uuid
        WHERE {" AND ".join(where_clauses)}
        ORDER BY
            dc.deck_zone ASC,
            CASE COALESCE(dc.deck_role, 'main')
                WHEN 'commander' THEN 0
                WHEN 'partner' THEN 1
                ELSE 2
            END ASC,
            dc.display_order ASC,
            dc.deck_card_id ASC
        """,
        params,
    )

    rows = cursor.fetchall()
    conn.close()

    cards = []

    for row in rows:
        quantity = int(row["quantity"] or 1)

        if quantity < 1:
            continue

        for copy_index in range(quantity):
            is_basic_land = int(row["is_basic_land"] or 0) == 1

            cards.append({
                "source_kind": "basic_land" if is_basic_land else "deck_card",
                "deck_card_id": row["deck_card_id"],
                "draft_test_pick_id": f"deckcard_{row['deck_card_id']}_{copy_index + 1}",
                "card_uuid": row["card_uuid"] or "",
                "card_name": row["card_name"] or "",
                "deck_zone": row["deck_zone"] or "deck",
                "pick_number": 0,
                "pack_number": 0,
                "pick_reason": "Basic land" if is_basic_land else (row["source_type"] or "Deck Builder"),
                "is_basic_land": 1 if is_basic_land else 0,
                "sheet_is_foil": int(row["sheet_is_foil"] or 0),
                "deck_role": normalize_deck_role(row["deck_role"] if "deck_role" in row.keys() else DECK_ROLE_MAIN),
                "stack_column": row["stack_column"] or "",
                "stack_order": row["stack_order"],
                "display_order": row["display_order"],
                "set_code": row["set_code"] or "",
                "collector_number": row["collector_number"] or "",
                "rarity": row["rarity"] or ("common" if is_basic_land else ""),
                "type_line": row["type_line"] or ("Basic Land" if is_basic_land else ""),
                "mana_value": row["mana_value"] if row["mana_value"] is not None else 0,
                "mana_cost": row["mana_cost"] or "",
                "colors_json": row["colors_json"] or "[]",
                "color_identity_json": row["color_identity_json"] or "[]",
                "image_url": row["image_url"] or "",
                "has_alternate_image": int(row["has_alternate_image"] or 0),
                "alternate_image_remove_bleed": int(row["alternate_image_remove_bleed"] or 0),
            })

    return cards

def add_card_to_deckbuilder_sideboard(deck_id, card_uuid):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)
    clean_card_uuid = str(card_uuid or "").strip()

    if parsed_deck_id is None:
        return {
            "ok": False,
            "message": "Invalid deck ID.",
        }

    if not clean_card_uuid:
        return {
            "ok": False,
            "message": "Card UUID is required.",
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
            "message": "Deck was not found.",
        }

    cursor.execute(
        """
        SELECT
            card_uuid,
            card_name
        FROM chaos_cards
        WHERE card_uuid = ?
        """,
        (clean_card_uuid,),
    )

    card_row = cursor.fetchone()

    if not card_row:
        conn.close()
        return {
            "ok": False,
            "message": "Card was not found.",
        }

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
            sheet_is_foil,
            deck_role,
            stack_column,
            stack_order,
            display_order,
            created_at_utc,
            updated_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            parsed_deck_id,
            card_row["card_uuid"],
            card_row["card_name"],
            "sideboard",
            1,
            "manual_add",
            parsed_deck_id,
            None,
            0,
            0,
            DECK_ROLE_MAIN,
            None,
            None,
            0,
            now_utc,
            now_utc,
        ),
    )

    deck_card_id = int(cursor.lastrowid)

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
        "message": "Card added to sideboard.",
        "deck_id": parsed_deck_id,
        "deck_card_id": deck_card_id,
        "card_uuid": clean_card_uuid,
        "card_name": card_row["card_name"],
        "deck_zone": "sideboard",
    }


def duplicate_deckbuilder_card(deck_id, deck_card_id):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)
    parsed_deck_card_id = normalize_deck_optional_int(deck_card_id)

    if parsed_deck_id is None or parsed_deck_card_id is None:
        return {
            "ok": False,
            "message": "Invalid deck card.",
        }

    now_utc = deck_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM deck_cards
        WHERE deck_id = ?
          AND deck_card_id = ?
        """,
        (
            parsed_deck_id,
            parsed_deck_card_id,
        ),
    )

    source_row = cursor.fetchone()

    if not source_row:
        conn.close()
        return {
            "ok": False,
            "message": "Deck card was not found.",
        }

    if int(source_row["is_basic_land"] or 0) == 1:
        conn.close()
        return add_basic_land_to_deck(
            deck_id=parsed_deck_id,
            land_name=source_row["card_name"],
        )

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
            sheet_is_foil,
            deck_role,
            stack_column,
            stack_order,
            display_order,
            created_at_utc,
            updated_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            parsed_deck_id,
            source_row["card_uuid"] or "",
            source_row["card_name"] or "",
            source_row["deck_zone"] or "deck",
            1,
            "duplicate",
            parsed_deck_id,
            parsed_deck_card_id,
            0,
            int(source_row["sheet_is_foil"] or 0),
            DECK_ROLE_MAIN,
            source_row["stack_column"],
            source_row["stack_order"],
            source_row["display_order"] or 0,
            now_utc,
            now_utc,
        ),
    )

    new_deck_card_id = int(cursor.lastrowid)

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
        "message": "Card duplicated.",
        "deck_id": parsed_deck_id,
        "deck_card_id": new_deck_card_id,
    }


def move_deckbuilder_card(deck_id, deck_card_id, deck_zone):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)
    parsed_deck_card_id = normalize_deck_optional_int(deck_card_id)
    clean_deck_zone = normalize_deck_zone(deck_zone)

    if parsed_deck_id is None or parsed_deck_card_id is None:
        return {
            "ok": False,
            "message": "Invalid deck card.",
        }

    if clean_deck_zone not in {"deck", "sideboard"}:
        return {
            "ok": False,
            "message": "Invalid deck zone.",
        }

    now_utc = deck_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM deck_cards
        WHERE deck_id = ?
          AND deck_card_id = ?
        """,
        (
            parsed_deck_id,
            parsed_deck_card_id,
        ),
    )

    deck_card_row = cursor.fetchone()

    if not deck_card_row:
        conn.close()
        return {
            "ok": False,
            "message": "Deck card was not found.",
        }

    if int(deck_card_row["is_basic_land"] or 0) == 1 and clean_deck_zone == "sideboard":
        conn.close()
        return remove_basic_land_from_deck(
            deck_id=parsed_deck_id,
            land_name=deck_card_row["card_name"],
        )

    if clean_deck_zone == "sideboard":
        cursor.execute(
            """
            UPDATE deck_cards
            SET deck_zone = ?,
                deck_role = ?,
                stack_column = NULL,
                stack_order = NULL,
                updated_at_utc = ?
            WHERE deck_id = ?
              AND deck_card_id = ?
            """,
            (
                clean_deck_zone,
                DECK_ROLE_MAIN,
                now_utc,
                parsed_deck_id,
                parsed_deck_card_id,
            ),
        )
    else:
        cursor.execute(
            """
            UPDATE deck_cards
            SET deck_zone = ?,
                updated_at_utc = ?
            WHERE deck_id = ?
              AND deck_card_id = ?
            """,
            (
                clean_deck_zone,
                now_utc,
                parsed_deck_id,
                parsed_deck_card_id,
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
        "message": "Card moved.",
        "deck_id": parsed_deck_id,
        "deck_card_id": parsed_deck_card_id,
        "deck_zone": clean_deck_zone,
    }

def normalize_deck_stack_column(value):
    clean_value = str(value or "").strip().lower()

    if clean_value in {"0", "1", "2", "3", "4", "5", "6", "land"}:
        return clean_value

    return ""


def update_deckbuilder_stack_layout(deck_id, deck_card_id, target_zone, stack_column, ordered_deck_card_ids):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)
    parsed_deck_card_id = normalize_deck_optional_int(deck_card_id)
    clean_target_zone = normalize_deck_zone(target_zone)
    clean_stack_column = normalize_deck_stack_column(stack_column)
    parsed_ordered_ids = []

    for raw_card_id in ordered_deck_card_ids or []:
        parsed_card_id = normalize_deck_optional_int(raw_card_id)

        if parsed_card_id is not None and parsed_card_id not in parsed_ordered_ids:
            parsed_ordered_ids.append(parsed_card_id)

    if parsed_deck_id is None or parsed_deck_card_id is None:
        return {
            "ok": False,
            "message": "Invalid deck card.",
        }

    if clean_target_zone not in {"deck", "sideboard"}:
        return {
            "ok": False,
            "message": "Invalid deck zone.",
        }

    if clean_target_zone == "deck" and not clean_stack_column:
        return {
            "ok": False,
            "message": "Invalid stack column.",
        }

    now_utc = deck_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM deck_cards
        WHERE deck_id = ?
          AND deck_card_id = ?
        """,
        (
            parsed_deck_id,
            parsed_deck_card_id,
        ),
    )

    deck_card_row = cursor.fetchone()

    if not deck_card_row:
        conn.close()
        return {
            "ok": False,
            "message": "Deck card was not found.",
        }

    if int(deck_card_row["is_basic_land"] or 0) == 1:
        conn.close()
        return {
            "ok": False,
            "message": "Basic lands are managed by quantity and cannot be manually reordered.",
        }

    if clean_target_zone == "sideboard":
        cursor.execute(
            """
            UPDATE deck_cards
            SET deck_zone = 'sideboard',
                stack_column = NULL,
                stack_order = NULL,
                updated_at_utc = ?
            WHERE deck_id = ?
              AND deck_card_id = ?
            """,
            (
                now_utc,
                parsed_deck_id,
                parsed_deck_card_id,
            ),
        )
    else:
        cursor.execute(
            """
            UPDATE deck_cards
            SET deck_zone = 'deck',
                stack_column = ?,
                updated_at_utc = ?
            WHERE deck_id = ?
              AND deck_card_id = ?
            """,
            (
                clean_stack_column,
                now_utc,
                parsed_deck_id,
                parsed_deck_card_id,
            ),
        )

    display_order = 0

    for ordered_card_id in parsed_ordered_ids:
        cursor.execute(
            """
            SELECT deck_card_id
            FROM deck_cards
            WHERE deck_id = ?
              AND deck_card_id = ?
              AND is_basic_land = 0
            LIMIT 1
            """,
            (
                parsed_deck_id,
                ordered_card_id,
            ),
        )

        if not cursor.fetchone():
            continue

        cursor.execute(
            """
            UPDATE deck_cards
            SET deck_zone = 'deck',
                display_order = ?,
                stack_order = ?,
                updated_at_utc = ?
            WHERE deck_id = ?
              AND deck_card_id = ?
            """,
            (
                display_order,
                display_order,
                now_utc,
                parsed_deck_id,
                ordered_card_id,
            ),
        )

        display_order += 10

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
        "message": "Stack layout updated.",
        "deck_id": parsed_deck_id,
        "deck_card_id": parsed_deck_card_id,
        "deck_zone": clean_target_zone,
        "stack_column": clean_stack_column,
    }

def update_deckbuilder_card_printing(deck_id, deck_card_id, new_card_uuid):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)
    parsed_deck_card_id = normalize_deck_optional_int(deck_card_id)
    clean_new_card_uuid = str(new_card_uuid or "").strip()

    if parsed_deck_id is None or parsed_deck_card_id is None:
        return {
            "ok": False,
            "message": "Invalid deck card.",
        }

    if not clean_new_card_uuid:
        return {
            "ok": False,
            "message": "New card UUID is required.",
        }

    now_utc = deck_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM deck_cards
        WHERE deck_id = ?
          AND deck_card_id = ?
        """,
        (
            parsed_deck_id,
            parsed_deck_card_id,
        ),
    )

    deck_card_row = cursor.fetchone()

    if not deck_card_row:
        conn.close()
        return {
            "ok": False,
            "message": "Deck card was not found.",
        }

    cursor.execute(
        """
        SELECT
            card_uuid,
            card_name,
            type_line
        FROM chaos_cards
        WHERE card_uuid = ?
        """,
        (clean_new_card_uuid,),
    )

    new_card_row = cursor.fetchone()

    if not new_card_row:
        conn.close()
        return {
            "ok": False,
            "message": "Selected printing was not found.",
        }

    original_name = (deck_card_row["card_name"] or "").strip()
    new_name = (new_card_row["card_name"] or "").strip()

    if original_name.lower() != new_name.lower():
        conn.close()
        return {
            "ok": False,
            "message": "Selected printing does not match the current card name.",
        }

    cursor.execute(
        """
        UPDATE deck_cards
        SET card_uuid = ?,
            card_name = ?,
            updated_at_utc = ?
        WHERE deck_id = ?
          AND deck_card_id = ?
        """,
        (
            new_card_row["card_uuid"],
            new_card_row["card_name"],
            now_utc,
            parsed_deck_id,
            parsed_deck_card_id,
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
        "message": "Printing changed.",
        "deck_id": parsed_deck_id,
        "deck_card_id": parsed_deck_card_id,
        "card_uuid": clean_new_card_uuid,
    }


def update_deckbuilder_basic_land_printing(deck_id, land_name, new_card_uuid):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)
    clean_land_name = normalize_deck_basic_land_name(land_name)
    clean_new_card_uuid = str(new_card_uuid or "").strip()

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

    if not clean_new_card_uuid:
        return {
            "ok": False,
            "message": "New card UUID is required.",
        }

    now_utc = deck_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            card_uuid,
            card_name,
            type_line
        FROM chaos_cards
        WHERE card_uuid = ?
        """,
        (clean_new_card_uuid,),
    )

    new_card_row = cursor.fetchone()

    if not new_card_row:
        conn.close()
        return {
            "ok": False,
            "message": "Selected Basic Land printing was not found.",
        }

    if (new_card_row["card_name"] or "").strip().lower() != clean_land_name.lower():
        conn.close()
        return {
            "ok": False,
            "message": "Selected printing does not match the selected Basic Land.",
        }

    if "basic land" not in (new_card_row["type_line"] or "").strip().lower():
        conn.close()
        return {
            "ok": False,
            "message": "Selected printing is not a Basic Land.",
        }

    cursor.execute(
        """
        INSERT INTO deck_basic_land_printings (
            deck_id,
            land_name,
            card_uuid,
            created_at_utc,
            updated_at_utc
        )
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(deck_id, land_name) DO UPDATE SET
            card_uuid = excluded.card_uuid,
            updated_at_utc = excluded.updated_at_utc
        """,
        (
            parsed_deck_id,
            clean_land_name,
            clean_new_card_uuid,
            now_utc,
            now_utc,
        ),
    )

    cursor.execute(
        """
        UPDATE deck_cards
        SET card_uuid = ?,
            updated_at_utc = ?
        WHERE deck_id = ?
          AND is_basic_land = 1
          AND LOWER(card_name) = LOWER(?)
        """,
        (
            clean_new_card_uuid,
            now_utc,
            parsed_deck_id,
            clean_land_name,
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
        "message": f"{clean_land_name} printing changed.",
        "deck_id": parsed_deck_id,
        "land_name": clean_land_name,
        "card_uuid": clean_new_card_uuid,
        "action": "change_basic_land_printing",
    }

def update_deckbuilder_card_role(deck_id, deck_card_id, deck_role):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)
    parsed_deck_card_id = normalize_deck_optional_int(deck_card_id)
    clean_deck_role = normalize_deck_role(deck_role)

    if parsed_deck_id is None or parsed_deck_card_id is None:
        return {
            "ok": False,
            "message": "Invalid deck card.",
        }

    now_utc = deck_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM deck_cards
        WHERE deck_id = ?
          AND deck_card_id = ?
          AND is_basic_land = 0
        """,
        (
            parsed_deck_id,
            parsed_deck_card_id,
        ),
    )

    deck_card_row = cursor.fetchone()

    if not deck_card_row:
        conn.close()
        return {
            "ok": False,
            "message": "Deck card was not found.",
        }

    if clean_deck_role in {DECK_ROLE_COMMANDER, DECK_ROLE_PARTNER}:
        cursor.execute(
            """
            UPDATE deck_cards
            SET deck_role = ?,
                updated_at_utc = ?
            WHERE deck_id = ?
              AND deck_role = ?
              AND deck_card_id <> ?
            """,
            (
                DECK_ROLE_MAIN,
                now_utc,
                parsed_deck_id,
                clean_deck_role,
                parsed_deck_card_id,
            ),
        )

        cursor.execute(
            """
            UPDATE deck_cards
            SET deck_zone = 'deck',
                deck_role = ?,
                stack_column = ?,
                stack_order = NULL,
                updated_at_utc = ?
            WHERE deck_id = ?
              AND deck_card_id = ?
            """,
            (
                clean_deck_role,
                clean_deck_role,
                now_utc,
                parsed_deck_id,
                parsed_deck_card_id,
            ),
        )
    else:
        cursor.execute(
            """
            UPDATE deck_cards
            SET deck_role = ?,
                stack_column = NULL,
                stack_order = NULL,
                updated_at_utc = ?
            WHERE deck_id = ?
              AND deck_card_id = ?
            """,
            (
                DECK_ROLE_MAIN,
                now_utc,
                parsed_deck_id,
                parsed_deck_card_id,
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
        "message": "Deck role updated.",
        "deck_id": parsed_deck_id,
        "deck_card_id": parsed_deck_card_id,
        "deck_role": clean_deck_role,
        "action": "set_deck_role",
    }

def update_deckbuilder_card_foil(deck_id, deck_card_id, is_foil):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)
    parsed_deck_card_id = normalize_deck_optional_int(deck_card_id)
    parsed_foil = 1 if is_foil else 0

    if parsed_deck_id is None or parsed_deck_card_id is None:
        return {
            "ok": False,
            "message": "Invalid deck card.",
        }

    now_utc = deck_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE deck_cards
        SET sheet_is_foil = ?,
            updated_at_utc = ?
        WHERE deck_id = ?
          AND deck_card_id = ?
          AND is_basic_land = 0
        """,
        (
            parsed_foil,
            now_utc,
            parsed_deck_id,
            parsed_deck_card_id,
        ),
    )

    updated = cursor.rowcount or 0

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
        "message": "Foil status updated.",
        "deck_id": parsed_deck_id,
        "deck_card_id": parsed_deck_card_id,
        "sheet_is_foil": parsed_foil,
        "updated": int(updated),
        "action": "set_foil" if parsed_foil else "remove_foil",
    }


def bulk_update_deckbuilder_card_foil(deck_id, deck_card_ids, is_foil):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)
    parsed_card_ids = []

    for raw_card_id in deck_card_ids or []:
        parsed_card_id = normalize_deck_optional_int(raw_card_id)

        if parsed_card_id is not None and parsed_card_id not in parsed_card_ids:
            parsed_card_ids.append(parsed_card_id)

    if parsed_deck_id is None:
        return {
            "ok": False,
            "message": "Invalid deck ID.",
        }

    if not parsed_card_ids:
        return {
            "ok": False,
            "message": "No deck cards were selected.",
        }

    parsed_foil = 1 if is_foil else 0
    placeholders = ",".join("?" for _ in parsed_card_ids)
    now_utc = deck_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        f"""
        UPDATE deck_cards
        SET sheet_is_foil = ?,
            updated_at_utc = ?
        WHERE deck_id = ?
          AND is_basic_land = 0
          AND deck_card_id IN ({placeholders})
        """,
        [parsed_foil, now_utc, parsed_deck_id] + parsed_card_ids,
    )

    updated = cursor.rowcount or 0

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
        "message": "Set foil on selected card(s)." if parsed_foil else "Removed foil from selected card(s).",
        "deck_id": parsed_deck_id,
        "sheet_is_foil": parsed_foil,
        "updated": int(updated),
        "affected_count": int(updated),
        "action": "set_foil" if parsed_foil else "remove_foil",
    }


def bulk_deckbuilder_card_action(deck_id, deck_card_ids, action, target_zone=""):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)
    clean_action = str(action or "").strip().lower()
    clean_target_zone = normalize_deck_zone(target_zone)
    parsed_card_ids = []

    for raw_card_id in deck_card_ids or []:
        parsed_card_id = normalize_deck_optional_int(raw_card_id)

        if parsed_card_id is not None:
            parsed_card_ids.append(parsed_card_id)

    if parsed_deck_id is None:
        return {
            "ok": False,
            "message": "Invalid deck ID.",
        }

    if clean_action not in {"move", "remove", "set_foil", "remove_foil"}:
        return {
            "ok": False,
            "message": "Unsupported bulk Deck Builder action.",
        }

    if clean_action == "move" and clean_target_zone not in {"deck", "sideboard"}:
        return {
            "ok": False,
            "message": "Invalid deck zone.",
        }

    if not parsed_card_ids:
        return {
            "ok": False,
            "message": "No deck cards were selected.",
        }
    
    if clean_action in {"set_foil", "remove_foil"}:
        return bulk_update_deckbuilder_card_foil(
            parsed_deck_id,
            parsed_card_ids,
            is_foil=(clean_action == "set_foil"),
        )

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

    unique_card_ids = []
    card_id_counts = {}

    for parsed_card_id in parsed_card_ids:
        card_id_counts[parsed_card_id] = card_id_counts.get(parsed_card_id, 0) + 1

        if parsed_card_id not in unique_card_ids:
            unique_card_ids.append(parsed_card_id)

    placeholders = ",".join("?" for _ in unique_card_ids)

    cursor.execute(
        f"""
        SELECT *
        FROM deck_cards
        WHERE deck_id = ?
          AND deck_card_id IN ({placeholders})
        """,
        [parsed_deck_id] + unique_card_ids,
    )

    selected_rows = cursor.fetchall()

    if not selected_rows:
        conn.close()
        return {
            "ok": False,
            "message": "Selected deck cards were not found.",
        }

    normal_card_ids = []
    draft_pick_card_ids = []
    basic_land_updates = []

    for row in selected_rows:
        deck_card_id = int(row["deck_card_id"])
        selected_count = max(1, int(card_id_counts.get(deck_card_id, 1)))
        is_basic_land = int(row["is_basic_land"] or 0) == 1
        source_type = (row["source_type"] or "").strip().lower()

        if is_basic_land:
            basic_land_updates.append({
                "deck_card_id": deck_card_id,
                "quantity": max(1, int(row["quantity"] or 1)),
                "selected_count": selected_count,
            })
        elif source_type == "draft_pick" and clean_action == "remove":
            draft_pick_card_ids.append(deck_card_id)
        else:
            normal_card_ids.append(deck_card_id)

    moved_count = 0
    removed_count = 0
    basic_land_removed_count = 0

    if clean_action == "move":
        if normal_card_ids:
            normal_placeholders = ",".join("?" for _ in normal_card_ids)

            if clean_target_zone == "sideboard":
                cursor.execute(
                    f"""
                    UPDATE deck_cards
                    SET deck_zone = ?,
                        deck_role = ?,
                        stack_column = NULL,
                        stack_order = NULL,
                        updated_at_utc = ?
                    WHERE deck_id = ?
                      AND deck_card_id IN ({normal_placeholders})
                    """,
                    [clean_target_zone, DECK_ROLE_MAIN, now_utc, parsed_deck_id] + normal_card_ids,
                )
            else:
                cursor.execute(
                    f"""
                    UPDATE deck_cards
                    SET deck_zone = ?,
                        updated_at_utc = ?
                    WHERE deck_id = ?
                      AND deck_card_id IN ({normal_placeholders})
                    """,
                    [clean_target_zone, now_utc, parsed_deck_id] + normal_card_ids,
                )

            moved_count += cursor.rowcount or 0

        # Moving a basic land from Deck to Sideboard means remove that many basic land copies.
        if clean_target_zone == "sideboard":
            for basic_update in basic_land_updates:
                remove_count = min(
                    basic_update["quantity"],
                    basic_update["selected_count"],
                )

                if remove_count >= basic_update["quantity"]:
                    cursor.execute(
                        """
                        DELETE FROM deck_cards
                        WHERE deck_id = ?
                          AND deck_card_id = ?
                        """,
                        (
                            parsed_deck_id,
                            basic_update["deck_card_id"],
                        ),
                    )
                else:
                    cursor.execute(
                        """
                        UPDATE deck_cards
                        SET quantity = quantity - ?,
                            updated_at_utc = ?
                        WHERE deck_id = ?
                          AND deck_card_id = ?
                        """,
                        (
                            remove_count,
                            now_utc,
                            parsed_deck_id,
                            basic_update["deck_card_id"],
                        ),
                    )

                basic_land_removed_count += remove_count
                moved_count += remove_count

    elif clean_action == "remove":
        if normal_card_ids:
            normal_placeholders = ",".join("?" for _ in normal_card_ids)

            cursor.execute(
                f"""
                DELETE FROM deck_cards
                WHERE deck_id = ?
                  AND deck_card_id IN ({normal_placeholders})
                """,
                [parsed_deck_id] + normal_card_ids,
            )

            removed_count += cursor.rowcount or 0

        if draft_pick_card_ids:
            draft_placeholders = ",".join("?" for _ in draft_pick_card_ids)

            cursor.execute(
                f"""
                UPDATE deck_cards
                SET deck_zone = 'removed',
                    stack_column = NULL,
                    stack_order = NULL,
                    updated_at_utc = ?
                WHERE deck_id = ?
                  AND deck_card_id IN ({draft_placeholders})
                """,
                [now_utc, parsed_deck_id] + draft_pick_card_ids,
            )

            removed_count += cursor.rowcount or 0

        for basic_update in basic_land_updates:
            remove_count = min(
                basic_update["quantity"],
                basic_update["selected_count"],
            )

            if remove_count >= basic_update["quantity"]:
                cursor.execute(
                    """
                    DELETE FROM deck_cards
                    WHERE deck_id = ?
                      AND deck_card_id = ?
                    """,
                    (
                        parsed_deck_id,
                        basic_update["deck_card_id"],
                    ),
                )
            else:
                cursor.execute(
                    """
                    UPDATE deck_cards
                    SET quantity = quantity - ?,
                        updated_at_utc = ?
                    WHERE deck_id = ?
                      AND deck_card_id = ?
                    """,
                    (
                        remove_count,
                        now_utc,
                        parsed_deck_id,
                        basic_update["deck_card_id"],
                    ),
                )

            basic_land_removed_count += remove_count
            removed_count += remove_count

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

    affected_count = moved_count if clean_action == "move" else removed_count

    return {
        "ok": True,
        "message": (
            f"Moved {affected_count} card(s)."
            if clean_action == "move"
            else f"Removed {affected_count} card(s)."
        ),
        "deck_id": parsed_deck_id,
        "action": clean_action,
        "deck_zone": clean_target_zone,
        "affected_count": affected_count,
        "basic_land_removed_count": basic_land_removed_count,
    }


def remove_deckbuilder_card(deck_id, deck_card_id):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)
    parsed_deck_card_id = normalize_deck_optional_int(deck_card_id)

    if parsed_deck_id is None or parsed_deck_card_id is None:
        return {
            "ok": False,
            "message": "Invalid deck card.",
        }

    now_utc = deck_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM deck_cards
        WHERE deck_id = ?
          AND deck_card_id = ?
        """,
        (
            parsed_deck_id,
            parsed_deck_card_id,
        ),
    )

    deck_card_row = cursor.fetchone()

    if not deck_card_row:
        conn.close()
        return {
            "ok": False,
            "message": "Deck card was not found.",
        }

    if int(deck_card_row["is_basic_land"] or 0) == 1:
        conn.close()
        return remove_basic_land_from_deck(
            deck_id=parsed_deck_id,
            land_name=deck_card_row["card_name"],
        )

    if (deck_card_row["source_type"] or "").strip().lower() == "draft_pick":
        cursor.execute(
            """
            UPDATE deck_cards
            SET deck_zone = ?,
                updated_at_utc = ?
            WHERE deck_id = ?
              AND deck_card_id = ?
            """,
            (
                "removed",
                now_utc,
                parsed_deck_id,
                parsed_deck_card_id,
            ),
        )
    else:
        cursor.execute(
            """
            DELETE FROM deck_cards
            WHERE deck_id = ?
              AND deck_card_id = ?
            """,
            (
                parsed_deck_id,
                parsed_deck_card_id,
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
        "message": "Card removed.",
        "deck_id": parsed_deck_id,
        "deck_card_id": parsed_deck_card_id,
    }

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
            dc.stack_column,
            dc.stack_order,
            dc.display_order,
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
                "source_kind": "basic_land",
                "deck_card_id": row["deck_card_id"],
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
        deck_id=parsed_deck_id,
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
                sheet_is_foil,
                deck_role,
                stack_column,
                stack_order,
                display_order,
                created_at_utc,
                updated_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                0,
                DECK_ROLE_MAIN,
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
    deck_format=DECK_FORMAT_STANDARD,
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

    clean_deck_format = normalize_deck_format(deck_format)
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
            deck_format = ?,
            default_view_mode = ?,
            default_sort_mode = ?,
            updated_at_utc = ?
        WHERE deck_id = ?
        """,
        (
            clean_deck_name,
            clean_deck_format,
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
        "deck_format": clean_deck_format,
        "view_mode": clean_view_mode,
        "sort_mode": clean_sort_mode,
        "card_size": clean_card_size,
        "sideboard_flex": clean_sideboard_flex,
        "deck_flex": clean_deck_flex,
    }

def get_deck_management_rows(
    search_text="",
    deck_format="",
    sort_option="created_desc",
    page=1,
    page_size=20,
):
    ensure_deck_schema()

    clean_search_text = str(search_text or "").strip().lower()
    clean_deck_format = str(deck_format or "").strip()

    try:
        parsed_page = int(page or 1)
    except (TypeError, ValueError):
        parsed_page = 1

    if parsed_page < 1:
        parsed_page = 1

    try:
        parsed_page_size = int(page_size or 20)
    except (TypeError, ValueError):
        parsed_page_size = 20

    if parsed_page_size < 10:
        parsed_page_size = 10

    if parsed_page_size > 100:
        parsed_page_size = 100

    clean_sort_option = str(sort_option or "created_desc").strip().lower()

    sort_sql_options = {
        "created_desc": """
            d.created_at_utc DESC,
            d.deck_id DESC
        """,
        "created_asc": """
            d.created_at_utc ASC,
            d.deck_id ASC
        """,
        "name_asc": """
            d.deck_name COLLATE NOCASE ASC,
            d.deck_id ASC
        """,
        "name_desc": """
            d.deck_name COLLATE NOCASE DESC,
            d.deck_id DESC
        """,
        "format_asc": """
            COALESCE(d.deck_format, '') COLLATE NOCASE ASC,
            d.deck_name COLLATE NOCASE ASC,
            d.deck_id ASC
        """,
        "format_desc": """
            COALESCE(d.deck_format, '') COLLATE NOCASE DESC,
            d.deck_name COLLATE NOCASE ASC,
            d.deck_id ASC
        """,
    }

    order_by_sql = sort_sql_options.get(
        clean_sort_option,
        sort_sql_options["created_desc"],
    )

    where_clauses = [
        "d.status = ?",
    ]

    params = [
        DECK_STATUS_ACTIVE,
    ]

    if clean_search_text:
        where_clauses.append(
            """
            (
                LOWER(d.deck_name) LIKE ?
                OR LOWER(COALESCE(d.deck_format, '')) LIKE ?
                OR LOWER(COALESCE(d.source_type, '')) LIKE ?
                OR CAST(COALESCE(d.source_id, '') AS TEXT) LIKE ?
            )
            """
        )

        like_value = f"%{clean_search_text}%"

        params.extend([
            like_value,
            like_value,
            like_value,
            like_value,
        ])

    if clean_deck_format:
        where_clauses.append(
            "LOWER(COALESCE(d.deck_format, '')) = LOWER(?)"
        )

        params.append(clean_deck_format)

    where_sql = " AND ".join(where_clauses)

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        f"""
        SELECT COUNT(*) AS total_count
        FROM decks d
        WHERE {where_sql}
        """,
        params,
    )

    count_row = cursor.fetchone()
    total_count = int(count_row["total_count"] or 0) if count_row else 0

    total_pages = max(
        1,
        (total_count + parsed_page_size - 1) // parsed_page_size,
    )

    if parsed_page > total_pages:
        parsed_page = total_pages

    offset = (parsed_page - 1) * parsed_page_size

    query_params = list(params)
    query_params.extend([
        parsed_page_size,
        offset,
    ])

    cursor.execute(
        f"""
        SELECT
            d.*,

            (
                SELECT cc.image_url
                FROM deck_cards preview_dc
                LEFT JOIN chaos_cards cc
                    ON cc.card_uuid = preview_dc.card_uuid
                WHERE preview_dc.deck_id = d.deck_id
                  AND preview_dc.deck_zone = 'deck'
                  AND COALESCE(cc.image_url, '') <> ''
                ORDER BY
                    CASE LOWER(COALESCE(preview_dc.deck_role, 'main'))
                        WHEN 'commander' THEN 0
                        WHEN 'partner' THEN 1
                        ELSE 2
                    END ASC,
                    preview_dc.display_order ASC,
                    preview_dc.deck_card_id ASC
                LIMIT 1
            ) AS preview_image_url,

            (
                SELECT COALESCE(SUM(card_count_dc.quantity), 0)
                FROM deck_cards card_count_dc
                WHERE card_count_dc.deck_id = d.deck_id
                  AND card_count_dc.deck_zone IN ('deck', 'sideboard')
            ) AS total_card_count

        FROM decks d
        WHERE {where_sql}
        ORDER BY {order_by_sql}
        LIMIT ?
        OFFSET ?
        """,
        query_params,
    )

    rows = cursor.fetchall()
    conn.close()

    return {
        "rows": rows,
        "total_count": total_count,
        "page": parsed_page,
        "page_size": parsed_page_size,
        "total_pages": total_pages,
        "has_previous": parsed_page > 1,
        "has_next": parsed_page < total_pages,
        "previous_page": parsed_page - 1 if parsed_page > 1 else None,
        "next_page": parsed_page + 1 if parsed_page < total_pages else None,
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
    ]
    params = [
        DECK_STATUS_ACTIVE,
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

def create_standalone_deck(deck_name="Untitled Deck", deck_format=DECK_FORMAT_STANDARD):
    ensure_deck_schema()

    clean_deck_name = str(deck_name or "").strip() or "Untitled Deck"
    clean_deck_format = normalize_deck_format(deck_format, fallback=DECK_FORMAT_STANDARD)
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
            DECK_SOURCE_TYPE_STANDALONE,
            None,
            None,
            None,
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
        "deck": get_deck_by_id(deck_id),
    }


def duplicate_deck(deck_id):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)

    if parsed_deck_id is None:
        return {
            "ok": False,
            "message": "Invalid deck ID.",
        }

    source_deck = get_deck_by_id(parsed_deck_id)

    if not source_deck or (source_deck["status"] or "") != DECK_STATUS_ACTIVE:
        return {
            "ok": False,
            "message": "Deck was not found.",
        }

    now_utc = deck_utc_now()
    source_name = (source_deck["deck_name"] or "Untitled Deck").strip()
    copy_name = f"{source_name} - Copy"

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
            DECK_SOURCE_TYPE_STANDALONE,
            None,
            source_deck["campaign_id"],
            source_deck["player_id"],
            copy_name,
            normalize_deck_format(source_deck["deck_format"], fallback=DECK_FORMAT_STANDARD),
            DECK_STATUS_ACTIVE,
            source_deck["default_view_mode"] or "grid",
            source_deck["default_sort_mode"] or "rarity-desc",
            source_deck["notes"] or "",
            now_utc,
            now_utc,
        ),
    )

    new_deck_id = int(cursor.lastrowid)

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
            sheet_is_foil,
            deck_role,
            stack_column,
            stack_order,
            display_order,
            created_at_utc,
            updated_at_utc
        )
        SELECT
            ?,
            card_uuid,
            card_name,
            deck_zone,
            quantity,
            'deck_copy',
            ?,
            deck_card_id,
            is_basic_land,
            sheet_is_foil,
            COALESCE(deck_role, 'main'),
            stack_column,
            stack_order,
            display_order,
            ?,
            ?
        FROM deck_cards
        WHERE deck_id = ?
          AND deck_zone IN ('deck', 'sideboard')
        """,
        (
            new_deck_id,
            parsed_deck_id,
            now_utc,
            now_utc,
            parsed_deck_id,
        ),
    )

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
        SELECT
            ?,
            layout_key,
            view_mode,
            sort_mode,
            sideboard_flex,
            deck_flex,
            layout_json,
            ?,
            ?
        FROM deck_builder_layouts
        WHERE deck_id = ?
        """,
        (
            new_deck_id,
            now_utc,
            now_utc,
            parsed_deck_id,
        ),
    )

    cursor.execute(
        """
        INSERT INTO deck_basic_land_printings (
            deck_id,
            land_name,
            card_uuid,
            created_at_utc,
            updated_at_utc
        )
        SELECT
            ?,
            land_name,
            card_uuid,
            ?,
            ?
        FROM deck_basic_land_printings
        WHERE deck_id = ?
        """,
        (
            new_deck_id,
            now_utc,
            now_utc,
            parsed_deck_id,
        ),
    )

    conn.commit()
    conn.close()

    return {
        "ok": True,
        "message": "Deck duplicated.",
        "deck_id": new_deck_id,
        "deck_name": copy_name,
    }


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

def get_deckbuilder_basic_land_cards(deck_id=None):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)

    conn = get_db_connection()
    cursor = conn.cursor()

    basic_land_cards = []

    for land_name in DECK_BUILDER_BASIC_LANDS:
        row = get_deckbuilder_basic_land_card_row(
            cursor=cursor,
            land_name=land_name,
            deck_id=parsed_deck_id,
        )

        if row:
            basic_land_cards.append(row)

    conn.close()

    return basic_land_cards

def build_deckbuilder_context_for_deck(deck_id):
    ensure_deck_schema()

    parsed_deck_id = normalize_deck_optional_int(deck_id)

    if parsed_deck_id is None:
        return {
            "ok": False,
            "message": "Invalid deck ID.",
        }

    deck_row = get_deck_by_id(parsed_deck_id)

    if not deck_row or (deck_row["status"] or "") != DECK_STATUS_ACTIVE:
        return {
            "ok": False,
            "message": "Deck was not found.",
        }

    layout_row = get_deck_builder_layout(parsed_deck_id)
    layout_json = get_deck_builder_layout_json(layout_row)

    default_view_mode = normalize_deck_view_mode(
        layout_row["view_mode"] if layout_row else (deck_row["default_view_mode"] or "grid")
    )
    default_sort_mode = normalize_deck_sort_mode(
        layout_row["sort_mode"] if layout_row else (deck_row["default_sort_mode"] or "rarity-desc")
    )
    default_card_size = normalize_deck_card_size(layout_json.get("card_size", 100))
    sideboard_flex = normalize_deck_flex_value(layout_row["sideboard_flex"] if layout_row else 0.40, 0.40)
    deck_flex = normalize_deck_flex_value(layout_row["deck_flex"] if layout_row else 0.60, 0.60)

    source_type = (deck_row["source_type"] or DECK_SOURCE_TYPE_STANDALONE).strip().lower()
    deck_name = deck_row["deck_name"] or "Untitled Deck"

    if source_type == DECK_SOURCE_TYPE_DRAFT_TEST:
        mode_label = "Draft Test"
        title = f"{deck_name} - Deck Builder"
        subtitle = "Saved draft deck."
    else:
        mode_label = "Standalone Deck"
        title = "Deck Builder"
        subtitle = "Standalone saved deck."

    return {
        "ok": True,
        "deck_id": parsed_deck_id,
        "source_type": source_type,
        "source_id": deck_row["source_id"],
        "title": title,
        "subtitle": subtitle,
        "mode_label": mode_label,
        "deck_name": deck_name,
        "deck_format": normalize_deck_format(deck_row["deck_format"], fallback=DECK_FORMAT_STANDARD),
        "human_player": None,
        "deck_cards": get_saved_deckbuilder_cards_for_deck(
            parsed_deck_id,
            deck_zone="deck",
            include_basic_lands=True,
        ),
        "sideboard_cards": get_saved_deckbuilder_cards_for_deck(
            parsed_deck_id,
            deck_zone="sideboard",
            include_basic_lands=False,
        ),
        "basic_land_names": DECK_BUILDER_BASIC_LANDS,
        "basic_land_cards": get_deckbuilder_basic_land_cards(parsed_deck_id),
        "basic_land_counts": get_basic_land_counts_for_deck(parsed_deck_id),
        "default_view_mode": default_view_mode,
        "default_sort_mode": default_sort_mode,
        "default_card_size": default_card_size,
        "sideboard_flex": sideboard_flex,
        "deck_flex": deck_flex,
    }


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

    import_result = import_draft_state_into_deck(
        deck_id=deck_result["deck_id"],
        draft_state=draft_state,
    )

    if not import_result.get("ok"):
        return {
            "ok": False,
            "message": import_result.get("message") or "Could not import drafted cards into Deck Builder.",
        }

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
        "deck_format": normalize_deck_format(
            deck_result["deck"]["deck_format"] if deck_result.get("deck") else DECK_FORMAT_LIMITED,
            fallback=DECK_FORMAT_LIMITED,
        ),
        "human_player": human_player,
        "deck_cards": get_saved_deckbuilder_cards_for_deck(
            deck_result["deck_id"],
            deck_zone="deck",
            include_basic_lands=True,
        ),
        "sideboard_cards": get_saved_deckbuilder_cards_for_deck(
            deck_result["deck_id"],
            deck_zone="sideboard",
            include_basic_lands=False,
        ),
        "basic_land_names": draft_state.get("basic_land_names") or DECK_BUILDER_BASIC_LANDS,
        "basic_land_cards": get_deckbuilder_basic_land_cards(deck_result["deck_id"]),
        "basic_land_counts": get_basic_land_counts_for_deck(deck_result["deck_id"]),
        "default_view_mode": default_view_mode,
        "default_sort_mode": default_sort_mode,
        "default_card_size": default_card_size,
        "sideboard_flex": sideboard_flex,
        "deck_flex": deck_flex,
    }