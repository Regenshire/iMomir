import os
import sqlite3

from paths import DATABASE_PATH
from settings import DEFAULT_CONFIG


def get_db_connection():
    db_parent = os.path.dirname(DATABASE_PATH)
    if db_parent and not os.path.exists(db_parent):
        os.makedirs(db_parent, exist_ok=True)

    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_column_exists(cursor, table_name, column_name, column_definition):
    cursor.execute(f"PRAGMA table_info({table_name})")
    existing_columns = {row[1] for row in cursor.fetchall()}

    if column_name not in existing_columns:
        cursor.execute(
            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}"
        )

def table_exists_with_cursor(cursor, table_name):
    cursor.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name = ?
        """,
        (table_name,),
    )

    return cursor.fetchone() is not None

def initialize_database():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS app_config (
            config_key TEXT PRIMARY KEY,
            config_value TEXT NOT NULL
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sets (
            set_code TEXT PRIMARY KEY,
            set_name TEXT NOT NULL,
            release_date TEXT,
            set_block TEXT,
            set_type TEXT
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS selected_sets (
            set_code TEXT PRIMARY KEY,
            FOREIGN KEY (set_code) REFERENCES sets (set_code)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS cards (
            card_key TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            face_name TEXT,
            mana_value REAL,
            mana_cost TEXT,
            type_line TEXT,
            layout TEXT,
            first_printing TEXT,
            printings_json TEXT,
            scryfall_id TEXT,
            image_url TEXT,
            is_legendary INTEGER NOT NULL DEFAULT 0,
            is_unset INTEGER NOT NULL DEFAULT 0,
            is_arena INTEGER NOT NULL DEFAULT 0,
            has_paper_printing INTEGER NOT NULL DEFAULT 0,
            is_creature INTEGER NOT NULL DEFAULT 0,
            is_artifact INTEGER NOT NULL DEFAULT 0,
            is_enchantment INTEGER NOT NULL DEFAULT 0,
            is_instant INTEGER NOT NULL DEFAULT 0,
            is_land INTEGER NOT NULL DEFAULT 0,
            is_sorcery INTEGER NOT NULL DEFAULT 0,
            is_planeswalker INTEGER NOT NULL DEFAULT 0,
            is_battle INTEGER NOT NULL DEFAULT 0,
            is_conspiracy INTEGER NOT NULL DEFAULT 0,
            is_dungeon INTEGER NOT NULL DEFAULT 0,
            is_emblem INTEGER NOT NULL DEFAULT 0,
            is_phenomenon INTEGER NOT NULL DEFAULT 0,
            is_plane INTEGER NOT NULL DEFAULT 0,
            is_scheme INTEGER NOT NULL DEFAULT 0,
            is_vanguard INTEGER NOT NULL DEFAULT 0
        )
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_cards_mana_value ON cards (mana_value)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_cards_first_printing ON cards (first_printing)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_cards_is_creature ON cards (is_creature)
        """
    )

    ensure_column_exists(cursor, "cards", "image_cache_path", "TEXT")
    ensure_column_exists(cursor, "cards", "image_source_url", "TEXT")
    ensure_column_exists(cursor, "cards", "image_cached_at", "TEXT")
    ensure_column_exists(cursor, "cards", "disable_card", "INTEGER NOT NULL DEFAULT 0")
    ensure_column_exists(cursor, "cards", "has_paper_printing", "INTEGER NOT NULL DEFAULT 0")
    ensure_column_exists(cursor, "cards", "rarity", "TEXT")
    ensure_column_exists(cursor, "sets", "set_block", "TEXT")
    ensure_column_exists(cursor, "sets", "set_type", "TEXT")
    ensure_column_exists(cursor, "sets", "local_icon_svg_path", "TEXT")
    if table_exists_with_cursor(cursor, "chaos_cards"):
        ensure_column_exists(cursor, "chaos_cards", "faces_json", "TEXT")
        ensure_column_exists(cursor, "chaos_cards", "face_count", "INTEGER NOT NULL DEFAULT 0")
        ensure_column_exists(cursor, "chaos_cards", "is_dual_faced", "INTEGER NOT NULL DEFAULT 0")
        ensure_column_exists(cursor, "chaos_cards", "front_image_url", "TEXT")
        ensure_column_exists(cursor, "chaos_cards", "back_image_url", "TEXT")
        ensure_column_exists(cursor, "chaos_cards", "front_face_name", "TEXT")
        ensure_column_exists(cursor, "chaos_cards", "back_face_name", "TEXT")
        ensure_column_exists(cursor, "chaos_cards", "colors_json", "TEXT")
        ensure_column_exists(cursor, "chaos_cards", "color_identity_json", "TEXT")
        ensure_column_exists(cursor, "chaos_cards", "edhrec_rank", "INTEGER")
        ensure_column_exists(cursor, "chaos_cards", "edhrec_saltiness", "REAL")
    if table_exists_with_cursor(cursor, "tracked_chaos_packs"):
        ensure_column_exists(cursor, "tracked_chaos_packs", "campaign_enabled", "INTEGER NOT NULL DEFAULT 1")

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_cards_disable_card ON cards (disable_card)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_cards_has_paper_printing ON cards (has_paper_printing)
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS scryfall_default_cards (
            scryfall_id TEXT PRIMARY KEY,
            oracle_id TEXT,
            card_name TEXT,
            set_code TEXT,
            collector_number TEXT,
            released_at TEXT,
            image_url TEXT,
            normal_image_url TEXT,
            large_image_url TEXT,
            rarity TEXT,
            games TEXT,
            lang TEXT
        )
        """
    )

    ensure_column_exists(cursor, "scryfall_default_cards", "games", "TEXT")
    ensure_column_exists(cursor, "scryfall_default_cards", "rarity", "TEXT")
    ensure_column_exists(cursor, "scryfall_default_cards", "lang", "TEXT")

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_scryfall_default_cards_oracle_set
        ON scryfall_default_cards (oracle_id, set_code)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_scryfall_default_cards_name_set
        ON scryfall_default_cards (card_name, set_code)
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS import_metadata (
            metadata_key TEXT PRIMARY KEY,
            metadata_value TEXT NOT NULL
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS chaos_cards (
            card_uuid TEXT PRIMARY KEY,
            set_code TEXT NOT NULL,
            card_name TEXT NOT NULL,
            face_name TEXT,
            mana_value REAL,
            mana_cost TEXT,
            rarity TEXT,
            type_line TEXT,
            layout TEXT,
            collector_number TEXT,
            scryfall_id TEXT,
            scryfall_illustration_id TEXT,
            image_url TEXT,
            image_cache_path TEXT,
            is_booster INTEGER NOT NULL DEFAULT 1,
            faces_json TEXT,
            face_count INTEGER NOT NULL DEFAULT 0,
            is_dual_faced INTEGER NOT NULL DEFAULT 0,
            front_image_url TEXT,
            back_image_url TEXT,
            front_face_name TEXT,
            back_face_name TEXT,
            frame_version TEXT,
            border_color TEXT,
            colors_json TEXT,
            color_identity_json TEXT,
            edhrec_rank INTEGER,
            edhrec_saltiness REAL
        )
        """
    )

    ensure_column_exists(cursor, "chaos_cards", "frame_version", "TEXT")
    ensure_column_exists(cursor, "chaos_cards", "border_color", "TEXT")
    ensure_column_exists(cursor, "chaos_cards", "colors_json", "TEXT")
    ensure_column_exists(cursor, "chaos_cards", "color_identity_json", "TEXT")
    ensure_column_exists(cursor, "chaos_cards", "edhrec_rank", "INTEGER")
    ensure_column_exists(cursor, "chaos_cards", "edhrec_saltiness", "REAL")

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_chaos_cards_set_code
        ON chaos_cards (set_code)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_chaos_cards_name
        ON chaos_cards (card_name)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_chaos_cards_edhrec_rank
        ON chaos_cards (edhrec_rank)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_chaos_cards_edhrec_saltiness
        ON chaos_cards (edhrec_saltiness)
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS chaos_pack_art (
            set_code TEXT NOT NULL,
            booster_name TEXT NOT NULL,
            display_name TEXT,
            image_path TEXT,
            is_fallback INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (set_code, booster_name)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS chaos_booster_variants (
            set_code TEXT NOT NULL,
            booster_name TEXT NOT NULL,
            booster_index INTEGER NOT NULL,
            booster_weight REAL NOT NULL DEFAULT 1,
            PRIMARY KEY (set_code, booster_name, booster_index)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS chaos_booster_variant_contents (
            set_code TEXT NOT NULL,
            booster_name TEXT NOT NULL,
            booster_index INTEGER NOT NULL,
            sheet_name TEXT NOT NULL,
            sheet_picks INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (set_code, booster_name, booster_index, sheet_name)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS chaos_booster_sheets (
            set_code TEXT NOT NULL,
            booster_name TEXT NOT NULL,
            sheet_name TEXT NOT NULL,
            sheet_is_foil INTEGER NOT NULL DEFAULT 0,
            sheet_has_balance_colors INTEGER NOT NULL DEFAULT 0,
            sheet_total_weight REAL NOT NULL DEFAULT 0,
            PRIMARY KEY (set_code, booster_name, sheet_name)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS chaos_booster_sheet_cards (
            set_code TEXT NOT NULL,
            booster_name TEXT NOT NULL,
            sheet_name TEXT NOT NULL,
            card_uuid TEXT NOT NULL,
            card_weight REAL NOT NULL DEFAULT 1,
            PRIMARY KEY (set_code, booster_name, sheet_name, card_uuid)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS chaos_pack_history (
            history_id INTEGER PRIMARY KEY AUTOINCREMENT,
            set_code TEXT NOT NULL,
            booster_name TEXT NOT NULL,
            booster_index INTEGER NOT NULL,
            pack_display_name TEXT NOT NULL,
            opened_at_utc TEXT NOT NULL
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS tracked_chaos_packs (
            tracked_pack_id INTEGER PRIMARY KEY AUTOINCREMENT,
            pack_tracking_code TEXT NOT NULL UNIQUE,
            set_code TEXT NOT NULL,
            booster_name TEXT NOT NULL,
            booster_index INTEGER NOT NULL,
            pack_display_name TEXT NOT NULL,
            total_cards INTEGER NOT NULL DEFAULT 0,
            bonus_pack_opened INTEGER NOT NULL DEFAULT 0,
            added_at_utc TEXT NOT NULL,
            last_opened_at_utc TEXT,
            opened_count INTEGER NOT NULL DEFAULT 0,
            campaign_enabled INTEGER NOT NULL DEFAULT 1,
            source_json TEXT
        )
        """
    )
    ensure_column_exists(cursor, "tracked_chaos_packs", "campaign_enabled", "INTEGER NOT NULL DEFAULT 1")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS tracked_chaos_pack_cards (
            tracked_pack_card_id INTEGER PRIMARY KEY AUTOINCREMENT,
            tracked_pack_id INTEGER NOT NULL,
            card_order INTEGER NOT NULL,
            card_uuid TEXT NOT NULL,
            card_name TEXT NOT NULL,
            set_code TEXT,
            booster_name TEXT,
            booster_index INTEGER,
            sheet_name TEXT,
            sheet_is_foil INTEGER NOT NULL DEFAULT 0,
            sheet_has_balance_colors INTEGER NOT NULL DEFAULT 0,
            sheet_total_weight REAL NOT NULL DEFAULT 0,
            rarity TEXT,
            type_line TEXT,
            image_url TEXT,
            scryfall_id TEXT,
            collector_number TEXT,
            FOREIGN KEY (tracked_pack_id) REFERENCES tracked_chaos_packs (tracked_pack_id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS tracked_chaos_pack_openings (
            opening_id INTEGER PRIMARY KEY AUTOINCREMENT,
            tracked_pack_id INTEGER NOT NULL,
            opened_at_utc TEXT NOT NULL,
            opened_by_player_id INTEGER,
            opening_context TEXT,
            FOREIGN KEY (tracked_pack_id) REFERENCES tracked_chaos_packs (tracked_pack_id)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS chaos_players (
            player_id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_name TEXT NOT NULL UNIQUE,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at_utc TEXT NOT NULL
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS chaos_session_state (
            state_key TEXT PRIMARY KEY,
            state_value TEXT NOT NULL
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS alternate_sources (
            alternate_source_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_name TEXT NOT NULL,
            source_type TEXT NOT NULL,
            card_uuid TEXT,
            set_code TEXT,
            collector_number TEXT,
            scryfall_id TEXT,
            card_name TEXT,
            face_kind TEXT NOT NULL DEFAULT 'single',
            external_image_url TEXT,
            local_image_path TEXT,
            fullbleed_image_path TEXT,
            remove_bleed INTEGER NOT NULL DEFAULT 0,
            bleed_size_mm REAL,
            export_frame_template TEXT,
            is_enabled INTEGER NOT NULL DEFAULT 1,
            priority INTEGER NOT NULL DEFAULT 100,
            notes TEXT,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT
        )
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_alternate_sources_card_uuid
        ON alternate_sources (card_uuid, face_kind, is_enabled, priority)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_alternate_sources_set_number
        ON alternate_sources (set_code, collector_number, face_kind, is_enabled, priority)
        """
    )

    ensure_column_exists(cursor, "alternate_sources", "fullbleed_image_path", "TEXT")
    ensure_column_exists(cursor, "alternate_sources", "remove_bleed", "INTEGER NOT NULL DEFAULT 0")
    ensure_column_exists(cursor, "alternate_sources", "bleed_size_mm", "REAL")
    ensure_column_exists(cursor, "alternate_sources", "export_frame_template", "TEXT")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS card_prices (
            card_uuid TEXT PRIMARY KEY,
            tcgplayer_normal_price REAL,
            tcgplayer_foil_price REAL,
            tcgplayer_etched_price REAL,
            currency TEXT,
            price_date TEXT,
            updated_at_utc TEXT
        )
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_card_prices_uuid
        ON card_prices (card_uuid)
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS custom_draft_sets (
            set_code TEXT PRIMARY KEY,
            special_category_1_name TEXT,
            special_category_2_name TEXT,
            special_category_3_name TEXT,
            icon_svg_path TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS custom_draft_set_cards (
            custom_set_card_id INTEGER PRIMARY KEY AUTOINCREMENT,
            set_code TEXT NOT NULL,
            card_uuid TEXT NOT NULL,
            special_category_index INTEGER NOT NULL DEFAULT 0,
            sort_name TEXT,
            added_at_utc TEXT NOT NULL,
            UNIQUE(set_code, card_uuid),
            FOREIGN KEY (set_code) REFERENCES custom_draft_sets (set_code),
            FOREIGN KEY (card_uuid) REFERENCES chaos_cards (card_uuid)
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS custom_draft_pack_slots (
            custom_pack_slot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            set_code TEXT NOT NULL,
            booster_name TEXT NOT NULL,
            slot_number INTEGER NOT NULL,
            color_rule TEXT NOT NULL,
            rarity_rule TEXT NOT NULL,
            special_category_rule TEXT NOT NULL DEFAULT 'none',
            foil_rule TEXT NOT NULL DEFAULT 'no',
            UNIQUE(set_code, booster_name, slot_number),
            FOREIGN KEY (set_code) REFERENCES custom_draft_sets (set_code)
        )
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_custom_draft_set_cards_set
        ON custom_draft_set_cards (set_code, sort_name)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_custom_draft_pack_slots_set_booster
        ON custom_draft_pack_slots (set_code, booster_name, slot_number)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_chaos_pack_history_set_booster
        ON chaos_pack_history (set_code, booster_name, booster_index)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tracked_chaos_packs_tracking_code
        ON tracked_chaos_packs (pack_tracking_code)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tracked_chaos_packs_set_booster
        ON tracked_chaos_packs (set_code, booster_name, booster_index)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tracked_chaos_pack_cards_pack
        ON tracked_chaos_pack_cards (tracked_pack_id, card_order)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_tracked_chaos_pack_openings_pack
        ON tracked_chaos_pack_openings (tracked_pack_id, opened_at_utc)
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS card_history (
            history_id INTEGER PRIMARY KEY AUTOINCREMENT,
            card_key TEXT NOT NULL,
            drawn_at_utc TEXT NOT NULL
        )
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_card_history_drawn_at_utc
        ON card_history (drawn_at_utc)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_card_history_card_key
        ON card_history (card_key)
        """
    )

    for key, value in DEFAULT_CONFIG.items():
        cursor.execute(
            """
            INSERT OR IGNORE INTO app_config (config_key, config_value)
            VALUES (?, ?)
            """,
            (key, value),
        )

    conn.commit()
    conn.close()

    restore_custom_draft_sets_to_sets_table()


def get_config():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT config_key, config_value
        FROM app_config
        """
    )

    rows = cursor.fetchall()
    conn.close()

    config = DEFAULT_CONFIG.copy()

    for row in rows:
        config[row["config_key"]] = row["config_value"]

    return config


def set_config_value(config_key, config_value):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO app_config (config_key, config_value)
        VALUES (?, ?)
        ON CONFLICT(config_key) DO UPDATE SET config_value = excluded.config_value
        """,
        (config_key, config_value),
    )

    conn.commit()
    conn.close()


def update_config_values(updated_config):
    conn = get_db_connection()
    cursor = conn.cursor()

    for key, value in updated_config.items():
        cursor.execute(
            """
            INSERT INTO app_config (config_key, config_value)
            VALUES (?, ?)
            ON CONFLICT(config_key) DO UPDATE SET config_value = excluded.config_value
            """,
            (key, value),
        )

    conn.commit()
    conn.close()


def get_selected_set_codes():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT set_code
        FROM selected_sets
        ORDER BY set_code
        """
    )

    rows = cursor.fetchall()
    conn.close()

    return {row["set_code"] for row in rows}


def replace_selected_sets(selected_set_codes):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM selected_sets")

    for set_code in selected_set_codes:
        cursor.execute(
            """
            INSERT INTO selected_sets (set_code)
            VALUES (?)
            """,
            (set_code,),
        )

    conn.commit()
    conn.close()


def set_import_metadata(key, value):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO import_metadata (metadata_key, metadata_value)
        VALUES (?, ?)
        ON CONFLICT(metadata_key) DO UPDATE SET metadata_value = excluded.metadata_value
        """,
        (key, str(value)),
    )

    conn.commit()
    conn.close()


def get_import_metadata():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT metadata_key, metadata_value
        FROM import_metadata
        """
    )

    rows = cursor.fetchall()
    conn.close()

    metadata = {}
    for row in rows:
        metadata[row["metadata_key"]] = row["metadata_value"]

    return metadata


def is_card_database_ready():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT COUNT(*) AS ready_count
        FROM cards
        WHERE disable_card = 0
          AND is_creature = 1
          AND has_paper_printing = 1
        """
    )

    row = cursor.fetchone()
    conn.close()

    return int(row["ready_count"] or 0) > 0


def get_all_sets():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT set_code, set_name, release_date, set_block, set_type
        FROM sets
        ORDER BY release_date DESC, set_name COLLATE NOCASE ASC
        """
    )

    rows = cursor.fetchall()
    conn.close()
    return rows


def get_card_by_key(card_key):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM cards
        WHERE card_key = ?
        """,
        (card_key,),
    )
    row = cursor.fetchone()
    conn.close()

    return row

def normalize_custom_draft_set_code(raw_set_code):
    clean_code = str(raw_set_code or "").strip().upper()

    if not clean_code:
        return ""

    clean_code = clean_code.replace(" ", "")

    if not clean_code.endswith("^"):
        clean_code = f"{clean_code}^"

    return clean_code

def restore_custom_draft_sets_to_sets_table():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO sets (
            set_code,
            set_name,
            release_date,
            set_block,
            set_type,
            local_icon_svg_path
        )
        SELECT
            cds.set_code,
            cds.set_code,
            COALESCE(SUBSTR(cds.created_at_utc, 1, 10), '2026-01-01'),
            'Custom',
            'custom',
            cds.icon_svg_path
        FROM custom_draft_sets cds
        LEFT JOIN sets s ON s.set_code = cds.set_code
        WHERE s.set_code IS NULL
        """
    )

    conn.commit()
    conn.close()

def get_custom_draft_sets():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            cds.set_code,
            COALESCE(s.set_name, cds.set_code) AS set_name,
            COALESCE(s.release_date, SUBSTR(cds.created_at_utc, 1, 10)) AS release_date,
            COALESCE(s.set_type, 'custom') AS set_type,
            COALESCE(s.local_icon_svg_path, cds.icon_svg_path) AS local_icon_svg_path,
            cds.special_category_1_name,
            cds.special_category_2_name,
            cds.special_category_3_name,
            cds.icon_svg_path,
            cds.is_active,
            cds.created_at_utc,
            cds.updated_at_utc,
            (
                SELECT COUNT(*)
                FROM custom_draft_set_cards cdsc
                WHERE cdsc.set_code = cds.set_code
            ) AS card_count
        FROM custom_draft_sets cds
        LEFT JOIN sets s ON s.set_code = cds.set_code
        ORDER BY
            COALESCE(s.release_date, SUBSTR(cds.created_at_utc, 1, 10)) DESC,
            COALESCE(s.set_name, cds.set_code) COLLATE NOCASE ASC
        """
    )

    rows = cursor.fetchall()
    conn.close()
    return rows


def get_custom_draft_set(set_code):
    clean_set_code = normalize_custom_draft_set_code(set_code)

    if not clean_set_code:
        return None

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            cds.set_code,
            COALESCE(s.set_name, cds.set_code) AS set_name,
            COALESCE(s.release_date, SUBSTR(cds.created_at_utc, 1, 10)) AS release_date,
            COALESCE(s.set_type, 'custom') AS set_type,
            COALESCE(s.local_icon_svg_path, cds.icon_svg_path) AS local_icon_svg_path,
            cds.special_category_1_name,
            cds.special_category_2_name,
            cds.special_category_3_name,
            cds.icon_svg_path,
            cds.is_active,
            cds.created_at_utc,
            cds.updated_at_utc,
            (
                SELECT COUNT(*)
                FROM custom_draft_set_cards cdsc
                WHERE cdsc.set_code = cds.set_code
            ) AS card_count
        FROM custom_draft_sets cds
        LEFT JOIN sets s ON s.set_code = cds.set_code
        WHERE cds.set_code = ?
        """,
        (clean_set_code,),
    )

    row = cursor.fetchone()
    conn.close()
    return row


def get_default_custom_draft_pack_slots():
    slots = []

    def add_slots(booster_name, start_slot, end_slot, color_rule, rarity_rule, special_category_rule, foil_rule):
        for slot_number in range(start_slot, end_slot + 1):
            slots.append({
                "booster_name": booster_name,
                "slot_number": slot_number,
                "color_rule": color_rule,
                "rarity_rule": rarity_rule,
                "special_category_rule": special_category_rule,
                "foil_rule": foil_rule,
            })

    # Mystery Booster Layout / Slots
    add_slots("mystery", 1, 2, "white", "common_uncommon", "none", "no")
    add_slots("mystery", 3, 4, "blue", "common_uncommon", "none", "no")
    add_slots("mystery", 5, 6, "black", "common_uncommon", "none", "no")
    add_slots("mystery", 7, 8, "red", "common_uncommon", "none", "no")
    add_slots("mystery", 9, 10, "green", "common_uncommon", "none", "no")
    add_slots("mystery", 11, 11, "colorless_multi_land", "any", "none", "no")
    add_slots("mystery", 12, 12, "any", "rare_mythic_equal", "none", "no")
    add_slots("mystery", 13, 13, "any", "any", "category_1", "no")
    add_slots("mystery", 14, 14, "any", "any", "category_2", "low")
    add_slots("mystery", 15, 15, "any", "any", "category_3", "no")

    # Play Booster Layout / Slots
    add_slots("play", 1, 6, "any", "common", "none", "no")
    add_slots("play", 7, 9, "any", "uncommon", "none", "no")
    add_slots("play", 10, 10, "any", "common_uncommon", "category_1", "no")
    add_slots("play", 11, 11, "any", "uncommon_rare_low", "none", "no")
    add_slots("play", 12, 12, "any", "rare_mythic", "category_1", "no")
    add_slots("play", 13, 13, "any", "rare_mythic", "none", "no")
    add_slots("play", 14, 14, "any", "any_low", "none", "yes")
    add_slots("play", 15, 15, "any", "basic_land", "none", "low")

    # Collector Booster Layout / Slots
    add_slots("collector", 1, 4, "any", "common", "none", "yes")
    add_slots("collector", 5, 7, "any", "uncommon", "none", "yes")
    add_slots("collector", 8, 9, "any", "uncommon", "category_1", "yes")
    add_slots("collector", 10, 10, "any", "basic_land", "none", "yes")
    add_slots("collector", 11, 11, "any", "rare_mythic", "none", "yes")
    add_slots("collector", 12, 12, "any", "rare_mythic", "category_2", "no")
    add_slots("collector", 13, 13, "any", "rare_mythic", "category_1", "low")
    add_slots("collector", 14, 14, "any", "rare_mythic", "category_3", "yes")
    add_slots("collector", 15, 15, "any", "basic_land", "none", "low")

    return slots


def seed_default_custom_draft_pack_slots(cursor, set_code):
    for slot in get_default_custom_draft_pack_slots():
        cursor.execute(
            """
            INSERT OR IGNORE INTO custom_draft_pack_slots (
                set_code,
                booster_name,
                slot_number,
                color_rule,
                rarity_rule,
                special_category_rule,
                foil_rule
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                set_code,
                slot["booster_name"],
                slot["slot_number"],
                slot["color_rule"],
                slot["rarity_rule"],
                slot["special_category_rule"],
                slot["foil_rule"],
            ),
        )


def upsert_custom_draft_set(
    set_name,
    set_code,
    release_year,
    special_category_1_name="",
    special_category_2_name="",
    special_category_3_name="",
    icon_svg_path="",
    is_active=True,
):
    clean_set_name = str(set_name or "").strip()
    clean_set_code = normalize_custom_draft_set_code(set_code)

    if not clean_set_name:
        raise ValueError("Set Name is required.")

    if not clean_set_code:
        raise ValueError("Set Code is required.")

    try:
        parsed_year = int(str(release_year or "").strip())
    except ValueError:
        parsed_year = 2026

    if parsed_year < 1993:
        parsed_year = 1993

    if parsed_year > 2100:
        parsed_year = 2100

    release_date = f"{parsed_year}-01-01"
    now_utc = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    conn = get_db_connection()
    cursor = conn.cursor()

    existing_custom_set = get_custom_draft_set(clean_set_code)

    cursor.execute(
        """
        INSERT INTO sets (
            set_code,
            set_name,
            release_date,
            set_block,
            set_type,
            local_icon_svg_path
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(set_code) DO UPDATE SET
            set_name = excluded.set_name,
            release_date = excluded.release_date,
            set_type = excluded.set_type,
            local_icon_svg_path = COALESCE(NULLIF(excluded.local_icon_svg_path, ''), sets.local_icon_svg_path)
        """,
        (
            clean_set_code,
            clean_set_name,
            release_date,
            "Custom",
            "custom",
            icon_svg_path or "",
        ),
    )

    cursor.execute(
        """
        INSERT INTO custom_draft_sets (
            set_code,
            special_category_1_name,
            special_category_2_name,
            special_category_3_name,
            icon_svg_path,
            is_active,
            created_at_utc,
            updated_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(set_code) DO UPDATE SET
            special_category_1_name = excluded.special_category_1_name,
            special_category_2_name = excluded.special_category_2_name,
            special_category_3_name = excluded.special_category_3_name,
            icon_svg_path = COALESCE(NULLIF(excluded.icon_svg_path, ''), custom_draft_sets.icon_svg_path),
            is_active = excluded.is_active,
            updated_at_utc = excluded.updated_at_utc
        """,
        (
            clean_set_code,
            str(special_category_1_name or "").strip(),
            str(special_category_2_name or "").strip(),
            str(special_category_3_name or "").strip(),
            icon_svg_path or "",
            1 if is_active else 0,
            existing_custom_set["created_at_utc"] if existing_custom_set else now_utc,
            now_utc,
        ),
    )

    seed_default_custom_draft_pack_slots(cursor, clean_set_code)

    conn.commit()
    conn.close()

    return clean_set_code


def get_custom_draft_pack_slots(set_code):
    clean_set_code = normalize_custom_draft_set_code(set_code)

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM custom_draft_pack_slots
        WHERE set_code = ?
        ORDER BY
            CASE booster_name
                WHEN 'mystery' THEN 1
                WHEN 'play' THEN 2
                WHEN 'collector' THEN 3
                ELSE 9
            END,
            slot_number ASC
        """,
        (clean_set_code,),
    )

    rows = cursor.fetchall()
    conn.close()
    return rows

def get_custom_draft_pack_slot_options():
    return {
        "booster_options": [
            {
                "value": "mystery",
                "label": "Mystery Booster",
            },
            {
                "value": "play",
                "label": "Play Booster",
            },
            {
                "value": "collector",
                "label": "Collector Booster",
            },
        ],
        "color_options": [
            {
                "value": "any",
                "label": "Any",
            },
            {
                "value": "white",
                "label": "White",
            },
            {
                "value": "blue",
                "label": "Blue",
            },
            {
                "value": "black",
                "label": "Black",
            },
            {
                "value": "red",
                "label": "Red",
            },
            {
                "value": "green",
                "label": "Green",
            },
            {
                "value": "colorless",
                "label": "Colorless",
            },
            {
                "value": "multi",
                "label": "Multi",
            },
            {
                "value": "land",
                "label": "Land",
            },
            {
                "value": "colorless_multi",
                "label": "Colorless/Multi",
            },
            {
                "value": "colorless_multi_land",
                "label": "Colorless/Multi/Land",
            },
        ],
        "rarity_options": [
            {
                "value": "any",
                "label": "Any",
            },
            {
                "value": "any_low",
                "label": "Any Low",
            },
            {
                "value": "common",
                "label": "Common",
            },
            {
                "value": "uncommon",
                "label": "Uncommon",
            },
            {
                "value": "rare",
                "label": "Rare",
            },
            {
                "value": "mythic",
                "label": "Mythic",
            },
            {
                "value": "common_uncommon",
                "label": "Common / Uncommon",
            },
            {
                "value": "uncommon_rare_low",
                "label": "Uncommon / Rare Low",
            },
            {
                "value": "rare_mythic",
                "label": "Rare / Mythic",
            },
            {
                "value": "rare_mythic_equal",
                "label": "Rare / Mythic Equal",
            },
            {
                "value": "basic_land",
                "label": "Basic Land",
            },
        ],
        "special_category_options": [
            {
                "value": "none",
                "label": "None",
            },
            {
                "value": "category_1",
                "label": "Special Slot Category 1",
            },
            {
                "value": "category_2",
                "label": "Special Slot Category 2",
            },
            {
                "value": "category_3",
                "label": "Special Slot Category 3",
            },
        ],
        "foil_options": [
            {
                "value": "no",
                "label": "No",
            },
            {
                "value": "yes",
                "label": "Yes",
            },
            {
                "value": "common",
                "label": "Common",
            },
            {
                "value": "low",
                "label": "Low",
            },
        ],
    }


def get_custom_draft_pack_slots_for_booster(set_code, booster_name):
    clean_set_code = normalize_custom_draft_set_code(set_code)
    clean_booster_name = str(booster_name or "").strip().lower()

    if clean_booster_name not in {"mystery", "play", "collector"}:
        return []

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM custom_draft_pack_slots
        WHERE set_code = ?
          AND booster_name = ?
        ORDER BY slot_number ASC
        """,
        (
            clean_set_code,
            clean_booster_name,
        ),
    )

    rows = cursor.fetchall()
    conn.close()
    return rows


def update_custom_draft_pack_layout(set_code, booster_name, slot_updates):
    clean_set_code = normalize_custom_draft_set_code(set_code)
    clean_booster_name = str(booster_name or "").strip().lower()

    if clean_booster_name not in {"mystery", "play", "collector"}:
        raise ValueError("Invalid custom draft booster layout.")

    option_data = get_custom_draft_pack_slot_options()

    valid_color_values = {option["value"] for option in option_data["color_options"]}
    valid_rarity_values = {option["value"] for option in option_data["rarity_options"]}
    valid_special_category_values = {option["value"] for option in option_data["special_category_options"]}
    valid_foil_values = {option["value"] for option in option_data["foil_options"]}

    conn = get_db_connection()
    cursor = conn.cursor()

    for slot_update in slot_updates or []:
        try:
            slot_number = int(slot_update.get("slot_number"))
        except (TypeError, ValueError):
            continue

        if slot_number < 1 or slot_number > 15:
            continue

        color_rule = str(slot_update.get("color_rule") or "any").strip().lower()
        rarity_rule = str(slot_update.get("rarity_rule") or "any").strip().lower()
        special_category_rule = str(slot_update.get("special_category_rule") or "none").strip().lower()
        foil_rule = str(slot_update.get("foil_rule") or "no").strip().lower()

        if color_rule not in valid_color_values:
            color_rule = "any"

        if rarity_rule not in valid_rarity_values:
            rarity_rule = "any"

        if special_category_rule not in valid_special_category_values:
            special_category_rule = "none"

        if foil_rule not in valid_foil_values:
            foil_rule = "no"

        cursor.execute(
            """
            INSERT INTO custom_draft_pack_slots (
                set_code,
                booster_name,
                slot_number,
                color_rule,
                rarity_rule,
                special_category_rule,
                foil_rule
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(set_code, booster_name, slot_number) DO UPDATE SET
                color_rule = excluded.color_rule,
                rarity_rule = excluded.rarity_rule,
                special_category_rule = excluded.special_category_rule,
                foil_rule = excluded.foil_rule
            """,
            (
                clean_set_code,
                clean_booster_name,
                slot_number,
                color_rule,
                rarity_rule,
                special_category_rule,
                foil_rule,
            ),
        )

    conn.commit()
    conn.close()

    return {
        "ok": True,
        "message": "Pack layout saved.",
    }

def get_custom_draft_set_pack_card_pool(set_code):
    clean_set_code = normalize_custom_draft_set_code(set_code)

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            cdsc.custom_set_card_id,
            cdsc.set_code AS custom_set_code,
            cdsc.card_uuid,
            cdsc.special_category_index,

            cc.card_name,
            cc.set_code AS card_set_code,
            cc.collector_number,
            cc.rarity,
            cc.type_line,
            cc.mana_cost,
            cc.mana_value,
            cc.colors_json,
            cc.color_identity_json,
            cc.image_url,
            cc.scryfall_id,
            cc.is_dual_faced
        FROM custom_draft_set_cards cdsc
        INNER JOIN chaos_cards cc ON cc.card_uuid = cdsc.card_uuid
        WHERE cdsc.set_code = ?
        ORDER BY
            cc.card_name COLLATE NOCASE ASC,
            cc.set_code ASC,
            CAST(cc.collector_number AS INTEGER) ASC,
            cc.collector_number ASC
        """,
        (clean_set_code,),
    )

    rows = cursor.fetchall()
    conn.close()
    return rows


def parse_custom_draft_json_list(raw_value):
    try:
        parsed_value = __import__("json").loads(raw_value or "[]")
        if isinstance(parsed_value, list):
            return [
                str(item or "").strip().upper()
                for item in parsed_value
                if str(item or "").strip()
            ]
    except Exception:
        pass

    return []


def get_custom_draft_card_color_identity(card_row):
    colors = parse_custom_draft_json_list(card_row["color_identity_json"] if "color_identity_json" in card_row.keys() else "[]")

    if colors:
        return colors

    mana_cost = (card_row["mana_cost"] if "mana_cost" in card_row.keys() else "") or ""
    fallback_colors = []

    for symbol in ["W", "U", "B", "R", "G"]:
        if "{" + symbol + "}" in mana_cost or symbol in mana_cost:
            fallback_colors.append(symbol)

    return fallback_colors


def custom_draft_card_matches_color_rule(card_row, color_rule):
    clean_color_rule = str(color_rule or "any").strip().lower()

    if clean_color_rule == "any":
        return True

    colors = get_custom_draft_card_color_identity(card_row)
    type_line = ((card_row["type_line"] if "type_line" in card_row.keys() else "") or "").lower()

    is_land = "land" in type_line
    is_colorless = len(colors) == 0
    is_multi = len(colors) >= 2

    if clean_color_rule == "white":
        return colors == ["W"]

    if clean_color_rule == "blue":
        return colors == ["U"]

    if clean_color_rule == "black":
        return colors == ["B"]

    if clean_color_rule == "red":
        return colors == ["R"]

    if clean_color_rule == "green":
        return colors == ["G"]

    if clean_color_rule == "colorless":
        return is_colorless and not is_land

    if clean_color_rule == "multi":
        return is_multi

    if clean_color_rule == "land":
        return is_land

    if clean_color_rule == "colorless_multi":
        return is_colorless or is_multi

    if clean_color_rule == "colorless_multi_land":
        return is_colorless or is_multi or is_land

    return True


def custom_draft_card_matches_exact_rarity(card_row, rarity_value):
    card_rarity = ((card_row["rarity"] if "rarity" in card_row.keys() else "") or "").strip().lower()
    return card_rarity == str(rarity_value or "").strip().lower()


def custom_draft_card_matches_basic_land(card_row):
    type_line = ((card_row["type_line"] if "type_line" in card_row.keys() else "") or "").strip().lower()
    return "basic" in type_line and "land" in type_line

def custom_draft_basic_land_allowed_for_slot(card_row, rarity_rule, special_category_rule):
    if not custom_draft_card_matches_basic_land(card_row):
        return True

    clean_rarity_rule = str(rarity_rule or "any").strip().lower()
    clean_special_category_rule = str(special_category_rule or "none").strip().lower()

    if clean_rarity_rule == "basic_land":
        return True

    if clean_special_category_rule != "none" and clean_rarity_rule == "any":
        return True

    return False

def get_custom_draft_rarity_bucket_for_rule(rarity_rule):
    clean_rarity_rule = str(rarity_rule or "any").strip().lower()

    if clean_rarity_rule == "common":
        return ["common"]

    if clean_rarity_rule == "uncommon":
        return ["uncommon"]

    if clean_rarity_rule == "rare":
        return ["rare"]

    if clean_rarity_rule == "mythic":
        return ["mythic"]

    if clean_rarity_rule == "common_uncommon":
        return ["common", "uncommon"]

    if clean_rarity_rule in {"rare_mythic", "rare_mythic_equal"}:
        return ["rare", "mythic"]

    if clean_rarity_rule == "uncommon_rare_low":
        return ["uncommon", "rare", "mythic"]

    if clean_rarity_rule == "any_low":
        return ["common", "uncommon", "rare", "mythic"]

    return []


def custom_draft_card_matches_rarity_rule(card_row, rarity_rule):
    clean_rarity_rule = str(rarity_rule or "any").strip().lower()

    if clean_rarity_rule == "any":
        return True

    if clean_rarity_rule == "basic_land":
        return custom_draft_card_matches_basic_land(card_row)

    rarity_bucket = get_custom_draft_rarity_bucket_for_rule(clean_rarity_rule)

    if not rarity_bucket:
        return True

    card_rarity = ((card_row["rarity"] if "rarity" in card_row.keys() else "") or "").strip().lower()
    return card_rarity in rarity_bucket


def custom_draft_card_matches_special_category_rule(card_row, special_category_rule):
    clean_special_category_rule = str(special_category_rule or "none").strip().lower()

    try:
        category_index = int(card_row["special_category_index"] or 0)
    except (TypeError, ValueError):
        category_index = 0

    if clean_special_category_rule == "none":
        return category_index == 0

    if clean_special_category_rule == "category_1":
        return category_index == 1

    if clean_special_category_rule == "category_2":
        return category_index == 2

    if clean_special_category_rule == "category_3":
        return category_index == 3

    return category_index == 0

def get_custom_draft_card_special_category_index(card_row):
    try:
        return int(card_row["special_category_index"] or 0)
    except (TypeError, ValueError):
        return 0


def get_custom_draft_rule_special_category_index(special_category_rule):
    clean_special_category_rule = str(special_category_rule or "none").strip().lower()

    if clean_special_category_rule == "category_1":
        return 1

    if clean_special_category_rule == "category_2":
        return 2

    if clean_special_category_rule == "category_3":
        return 3

    return 0


def filter_custom_draft_candidates_for_exact_special_category(candidate_cards, special_category_rule):
    required_category_index = get_custom_draft_rule_special_category_index(special_category_rule)

    return [
        card_row
        for card_row in candidate_cards or []
        if get_custom_draft_card_special_category_index(card_row) == required_category_index
    ]

def filter_custom_draft_pack_candidates(card_pool, slot_rule):
    filtered_cards = []

    color_rule = slot_rule["color_rule"] if "color_rule" in slot_rule.keys() else "any"
    rarity_rule = slot_rule["rarity_rule"] if "rarity_rule" in slot_rule.keys() else "any"
    special_category_rule = slot_rule["special_category_rule"] if "special_category_rule" in slot_rule.keys() else "none"

    for card_row in card_pool:
        if not custom_draft_card_matches_special_category_rule(card_row, special_category_rule):
            continue

        if not custom_draft_basic_land_allowed_for_slot(card_row, rarity_rule, special_category_rule):
            continue

        if not custom_draft_card_matches_color_rule(card_row, color_rule):
            continue

        if not custom_draft_card_matches_rarity_rule(card_row, rarity_rule):
            continue

        filtered_cards.append(card_row)

    return filtered_cards

def get_custom_draft_pack_candidates_with_fallback(card_pool, slot_rule):
    fallback_attempts = [
        {
            "label": "exact",
            "color_rule": slot_rule["color_rule"] if "color_rule" in slot_rule.keys() else "any",
            "rarity_rule": slot_rule["rarity_rule"] if "rarity_rule" in slot_rule.keys() else "any",
            "special_category_rule": slot_rule["special_category_rule"] if "special_category_rule" in slot_rule.keys() else "none",
        },
        {
            "label": "without_special_category",
            "color_rule": slot_rule["color_rule"] if "color_rule" in slot_rule.keys() else "any",
            "rarity_rule": slot_rule["rarity_rule"] if "rarity_rule" in slot_rule.keys() else "any",
            "special_category_rule": "none",
        },
        {
            "label": "without_special_category_or_color",
            "color_rule": "any",
            "rarity_rule": slot_rule["rarity_rule"] if "rarity_rule" in slot_rule.keys() else "any",
            "special_category_rule": "none",
        },
        {
            "label": "without_special_category_color_or_rarity",
            "color_rule": "any",
            "rarity_rule": "any",
            "special_category_rule": "none",
        },
    ]

    for fallback_rule in fallback_attempts:
        candidates = filter_custom_draft_pack_candidates(
            card_pool,
            fallback_rule,
        )

        if candidates:
            return {
                "candidates": candidates,
                "fallback_label": fallback_rule["label"],
                "effective_rule": fallback_rule,
            }

    return {
        "candidates": [],
        "fallback_label": "none",
        "effective_rule": None,
    }

def choose_custom_draft_weighted_rarity_target(rarity_rule):
    clean_rarity_rule = str(rarity_rule or "any").strip().lower()
    random_module = __import__("random")

    roll_value = random_module.random()

    if clean_rarity_rule == "any_low":
        if roll_value < 0.875:
            return "common"

        if roll_value < 0.9688:
            return "uncommon"

        return "rare_mythic"

    if clean_rarity_rule == "uncommon_rare_low":
        if roll_value < 0.875:
            return "uncommon"

        return "rare_mythic"

    if clean_rarity_rule == "rare_mythic":
        if roll_value < 0.875:
            return "rare"

        return "mythic"

    if clean_rarity_rule == "rare_mythic_equal":
        if roll_value < 0.5:
            return "rare"

        return "mythic"

    return ""


def apply_custom_draft_weighted_rarity_choice(candidate_cards, rarity_rule):
    target_rarity = choose_custom_draft_weighted_rarity_target(rarity_rule)

    if not target_rarity:
        return candidate_cards

    if target_rarity == "rare_mythic":
        preferred_cards = [
            card_row
            for card_row in candidate_cards
            if custom_draft_card_matches_exact_rarity(card_row, "rare")
               or custom_draft_card_matches_exact_rarity(card_row, "mythic")
        ]
    else:
        preferred_cards = [
            card_row
            for card_row in candidate_cards
            if custom_draft_card_matches_exact_rarity(card_row, target_rarity)
        ]

    return preferred_cards or candidate_cards


def resolve_custom_draft_foil_flag(foil_rule):
    clean_foil_rule = str(foil_rule or "no").strip().lower()
    random_module = __import__("random")

    if clean_foil_rule == "yes":
        return 1

    if clean_foil_rule == "common":
        return 1 if random_module.random() < 0.5 else 0

    if clean_foil_rule == "low":
        return 1 if random_module.random() < 0.125 else 0

    return 0


def serialize_custom_draft_pack_card(card_row, slot_rule, card_order, pack_set_code, booster_name, booster_index=0):
    sheet_is_foil = resolve_custom_draft_foil_flag(slot_rule["foil_rule"] if "foil_rule" in slot_rule.keys() else "no")

    return {
        "card_order": int(card_order),
        "card_uuid": card_row["card_uuid"],
        "card_name": card_row["card_name"],
        "set_code": (pack_set_code or "").strip().upper(),
        "booster_name": (booster_name or "").strip().lower(),
        "booster_index": int(booster_index or 0),
        "sheet_name": f"custom_slot_{int(card_order)}",
        "sheet_is_foil": sheet_is_foil,
        "sheet_has_balance_colors": 0,
        "sheet_total_weight": 0,
        "rarity": card_row["rarity"] or "",
        "type_line": card_row["type_line"] or "",
        "image_url": card_row["image_url"] or "",
        "scryfall_id": card_row["scryfall_id"] or "",
        "collector_number": card_row["collector_number"] or "",
        "source_card_set_code": card_row["card_set_code"] or "",
        "finish_type": "Foil" if sheet_is_foil else "Regular",
        "special_badges": ["Foil"] if sheet_is_foil else [],
    }


def generate_custom_draft_set_pack_cards(set_code, booster_name):
    clean_set_code = normalize_custom_draft_set_code(set_code)
    clean_booster_name = str(booster_name or "").strip().lower()

    if clean_booster_name not in {"mystery", "play", "collector"}:
        raise ValueError("Invalid custom draft booster type.")

    card_pool = get_custom_draft_set_pack_card_pool(clean_set_code)

    if not card_pool:
        raise ValueError("This custom draft set does not have any cards yet.")

    pack_slots = get_custom_draft_pack_slots_for_booster(clean_set_code, clean_booster_name)

    if not pack_slots:
        raise ValueError("No pack layout slots were found for this booster type.")

    random_module = __import__("random")
    generated_cards = []
    used_card_uuids = set()

    for slot in pack_slots:
        fallback_result = get_custom_draft_pack_candidates_with_fallback(
            card_pool,
            slot,
        )

        candidates = fallback_result["candidates"]
        effective_rule = fallback_result["effective_rule"]

        if not candidates:
            raise ValueError(f"No valid cards were found for slot {slot['slot_number']}.")

        effective_special_category_rule = (
            effective_rule["special_category_rule"]
            if effective_rule
            else slot["special_category_rule"]
        )

        candidates = apply_custom_draft_weighted_rarity_choice(
            candidates,
            effective_rule["rarity_rule"] if effective_rule else slot["rarity_rule"],
        )

        candidates = filter_custom_draft_candidates_for_exact_special_category(
            candidates,
            effective_special_category_rule,
        )

        candidates = [
            card_row
            for card_row in candidates
            if custom_draft_basic_land_allowed_for_slot(
                card_row,
                effective_rule["rarity_rule"] if effective_rule else slot["rarity_rule"],
                effective_special_category_rule,
            )
        ]

        if not candidates:
            raise ValueError(
                f"No valid cards remained for slot {slot['slot_number']} after enforcing special slot and basic land placement."
            )

        unused_candidates = [
            card_row
            for card_row in candidates
            if card_row["card_uuid"] not in used_card_uuids
        ]

        final_candidates = unused_candidates if unused_candidates else candidates
        chosen_card = random_module.choice(final_candidates)

        used_card_uuids.add(chosen_card["card_uuid"])

        generated_card = serialize_custom_draft_pack_card(
            chosen_card,
            slot,
            len(generated_cards) + 1,
            clean_set_code,
            clean_booster_name,
            0,
        )

        generated_card["custom_slot_number"] = int(slot["slot_number"])
        generated_card["custom_slot_special_category_rule"] = slot["special_category_rule"]
        generated_card["custom_effective_special_category_rule"] = effective_special_category_rule
        generated_card["custom_card_special_category_index"] = get_custom_draft_card_special_category_index(chosen_card)
        generated_card["custom_card_is_basic_land"] = custom_draft_card_matches_basic_land(chosen_card)

        fallback_label = fallback_result["fallback_label"]

        if fallback_label and fallback_label != "exact":
            generated_card["special_badges"] = list(generated_card.get("special_badges") or [])
            generated_card["special_badges"].append("Fallback")

            generated_card["fallback_rule"] = fallback_label
            generated_card["fallback_original_color_rule"] = slot["color_rule"]
            generated_card["fallback_original_rarity_rule"] = slot["rarity_rule"]
            generated_card["fallback_original_special_category_rule"] = slot["special_category_rule"]

            if effective_rule:
                generated_card["fallback_effective_color_rule"] = effective_rule["color_rule"]
                generated_card["fallback_effective_rarity_rule"] = effective_rule["rarity_rule"]
                generated_card["fallback_effective_special_category_rule"] = effective_rule["special_category_rule"]

        generated_cards.append(generated_card)

    return generated_cards

def get_custom_draft_set_card_rows(set_code, search_text=""):
    clean_set_code = normalize_custom_draft_set_code(set_code)
    clean_search_text = str(search_text or "").strip().lower()

    conn = get_db_connection()
    cursor = conn.cursor()

    sql = """
        SELECT
            cdsc.custom_set_card_id,
            cdsc.set_code AS custom_set_code,
            cdsc.card_uuid,
            cdsc.special_category_index,
            cdsc.sort_name,
            cdsc.added_at_utc,

            cc.card_name,
            cc.set_code AS card_set_code,
            cc.collector_number,
            cc.rarity,
            cc.type_line,
            cc.mana_cost,
            cc.mana_value,
            cc.colors_json,
            cc.color_identity_json,
            cc.edhrec_rank,
            cc.edhrec_saltiness,
            COALESCE(
                cp.tcgplayer_normal_price,
                cp.tcgplayer_foil_price,
                cp.tcgplayer_etched_price
            ) AS sort_price,
            cc.is_dual_faced
        FROM custom_draft_set_cards cdsc
        INNER JOIN chaos_cards cc ON cc.card_uuid = cdsc.card_uuid
        LEFT JOIN card_prices cp ON cp.card_uuid = cc.card_uuid
        WHERE cdsc.set_code = ?
    """

    params = [clean_set_code]

    if clean_search_text:
        sql += """
          AND (
                LOWER(cc.card_name) LIKE ?
             OR LOWER(cc.set_code) LIKE ?
             OR LOWER(COALESCE(cc.collector_number, '')) LIKE ?
             OR LOWER(COALESCE(cc.rarity, '')) LIKE ?
             OR LOWER(COALESCE(cc.type_line, '')) LIKE ?
          )
        """

        like_value = f"%{clean_search_text}%"
        params.extend([like_value, like_value, like_value, like_value, like_value])

    sql += """
        ORDER BY
            cc.card_name COLLATE NOCASE ASC,
            cc.set_code ASC,
            CAST(cc.collector_number AS INTEGER) ASC,
            cc.collector_number ASC
    """

    cursor.execute(sql, params)

    rows = cursor.fetchall()
    conn.close()
    return rows

def search_chaos_cards_for_custom_draft_set(
    set_code,
    search_text="",
    limit=999,
    rarity_filter="",
    color_identity_filter="",
    mana_operator="",
    mana_value=None,
    type_filter="",
    set_code_filter="",
    year_start=None,
    year_end=None,
    sort_option="name_asc",
):
    clean_set_code = normalize_custom_draft_set_code(set_code)
    clean_search_text = str(search_text or "").strip().lower()
    clean_rarity_filter = str(rarity_filter or "").strip().lower()
    clean_color_identity_filter = str(color_identity_filter or "").strip().lower()
    clean_mana_operator = str(mana_operator or "").strip()
    clean_type_filter = str(type_filter or "").strip().lower()
    clean_set_code_filter = str(set_code_filter or "").strip().upper()
    clean_sort_option = str(sort_option or "name_asc").strip().lower()

    try:
        parsed_limit = int(limit)
    except (TypeError, ValueError):
        parsed_limit = 999

    if parsed_limit < 1:
        parsed_limit = 1

    if parsed_limit > 999:
        parsed_limit = 999

    parsed_mana_value = None
    if mana_value not in {None, ""}:
        try:
            parsed_mana_value = float(mana_value)
        except (TypeError, ValueError):
            parsed_mana_value = None

    parsed_year_start = None
    if year_start not in {None, ""}:
        try:
            parsed_year_start = int(year_start)
        except (TypeError, ValueError):
            parsed_year_start = None

    parsed_year_end = None
    if year_end not in {None, ""}:
        try:
            parsed_year_end = int(year_end)
        except (TypeError, ValueError):
            parsed_year_end = None

    has_any_filter = any([
        clean_search_text,
        clean_rarity_filter,
        clean_color_identity_filter,
        clean_mana_operator and parsed_mana_value is not None,
        clean_type_filter,
        clean_set_code_filter,
        parsed_year_start is not None,
        parsed_year_end is not None,
    ])

    if not has_any_filter:
        return []

    conn = get_db_connection()
    cursor = conn.cursor()

    where_clauses = []
    params = [clean_set_code]

    if clean_search_text:
        like_value = f"%{clean_search_text}%"

        where_clauses.append(
            """
            (
                    LOWER(cc.card_name) LIKE ?
                 OR LOWER(cc.set_code) LIKE ?
                 OR LOWER(COALESCE(cc.collector_number, '')) LIKE ?
                 OR LOWER(COALESCE(cc.rarity, '')) LIKE ?
                 OR LOWER(COALESCE(cc.type_line, '')) LIKE ?
            )
            """
        )
        params.extend([like_value, like_value, like_value, like_value, like_value])

    if clean_rarity_filter:
        where_clauses.append("LOWER(COALESCE(cc.rarity, '')) = ?")
        params.append(clean_rarity_filter)

    color_json_sql = "UPPER(COALESCE(cc.color_identity_json, '[]'))"
    type_line_sql = "LOWER(COALESCE(cc.type_line, ''))"

    if clean_color_identity_filter == "colorless":
        where_clauses.append(f"{color_json_sql} = '[]'")
        where_clauses.append(f"{type_line_sql} NOT LIKE '%land%'")

    elif clean_color_identity_filter == "land":
        where_clauses.append(f"{type_line_sql} LIKE '%land%'")

    elif clean_color_identity_filter in {"w", "u", "b", "r", "g"}:
        color_symbol = clean_color_identity_filter.upper()

        where_clauses.append(f"{color_json_sql} LIKE ?")
        params.append(f'%"{color_symbol}"%')

    elif clean_color_identity_filter == "multi":
        where_clauses.append(
            f"""
            (
                (CASE WHEN {color_json_sql} LIKE '%"W"%' THEN 1 ELSE 0 END) +
                (CASE WHEN {color_json_sql} LIKE '%"U"%' THEN 1 ELSE 0 END) +
                (CASE WHEN {color_json_sql} LIKE '%"B"%' THEN 1 ELSE 0 END) +
                (CASE WHEN {color_json_sql} LIKE '%"R"%' THEN 1 ELSE 0 END) +
                (CASE WHEN {color_json_sql} LIKE '%"G"%' THEN 1 ELSE 0 END)
            ) >= 2
            """
        )

    elif clean_color_identity_filter == "colorless_multi":
        where_clauses.append(
            f"""
            (
                {color_json_sql} = '[]'
                OR (
                    (CASE WHEN {color_json_sql} LIKE '%"W"%' THEN 1 ELSE 0 END) +
                    (CASE WHEN {color_json_sql} LIKE '%"U"%' THEN 1 ELSE 0 END) +
                    (CASE WHEN {color_json_sql} LIKE '%"B"%' THEN 1 ELSE 0 END) +
                    (CASE WHEN {color_json_sql} LIKE '%"R"%' THEN 1 ELSE 0 END) +
                    (CASE WHEN {color_json_sql} LIKE '%"G"%' THEN 1 ELSE 0 END)
                ) >= 2
            )
            """
        )

    elif clean_color_identity_filter == "colorless_multi_land":
        where_clauses.append(
            f"""
            (
                {color_json_sql} = '[]'
                OR {type_line_sql} LIKE '%land%'
                OR (
                    (CASE WHEN {color_json_sql} LIKE '%"W"%' THEN 1 ELSE 0 END) +
                    (CASE WHEN {color_json_sql} LIKE '%"U"%' THEN 1 ELSE 0 END) +
                    (CASE WHEN {color_json_sql} LIKE '%"B"%' THEN 1 ELSE 0 END) +
                    (CASE WHEN {color_json_sql} LIKE '%"R"%' THEN 1 ELSE 0 END) +
                    (CASE WHEN {color_json_sql} LIKE '%"G"%' THEN 1 ELSE 0 END)
                ) >= 2
            )
            """
        )

    if clean_mana_operator and parsed_mana_value is not None:
        if clean_mana_operator == "=":
            where_clauses.append("COALESCE(cc.mana_value, -1) = ?")
            params.append(parsed_mana_value)
        elif clean_mana_operator == "<=":
            where_clauses.append("COALESCE(cc.mana_value, -1) <= ?")
            params.append(parsed_mana_value)
        elif clean_mana_operator == ">=":
            where_clauses.append("COALESCE(cc.mana_value, -1) >= ?")
            params.append(parsed_mana_value)
        elif clean_mana_operator == "<":
            where_clauses.append("COALESCE(cc.mana_value, -1) < ?")
            params.append(parsed_mana_value)
        elif clean_mana_operator == ">":
            where_clauses.append("COALESCE(cc.mana_value, -1) > ?")
            params.append(parsed_mana_value)

    if clean_type_filter:
        where_clauses.append("LOWER(COALESCE(cc.type_line, '')) LIKE ?")
        params.append(f"%{clean_type_filter}%")

    if clean_set_code_filter:
        where_clauses.append("UPPER(COALESCE(cc.set_code, '')) = ?")
        params.append(clean_set_code_filter)

    if parsed_year_start is not None:
        where_clauses.append("CAST(SUBSTR(COALESCE(s.release_date, ''), 1, 4) AS INTEGER) >= ?")
        params.append(parsed_year_start)

    if parsed_year_end is not None:
        where_clauses.append("CAST(SUBSTR(COALESCE(s.release_date, ''), 1, 4) AS INTEGER) <= ?")
        params.append(parsed_year_end)

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    order_exact_params = []
    search_rank_sql = ""

    if clean_search_text:
        order_exact_params = [
            clean_search_text,
            f"{clean_search_text}%",
        ]
        search_rank_sql = """
            CASE
                WHEN LOWER(cc.card_name) = ? THEN 0
                WHEN LOWER(cc.card_name) LIKE ? THEN 1
                ELSE 2
            END,
        """

    sort_sql_map = {
        "name_asc": "cc.card_name COLLATE NOCASE ASC, cc.set_code ASC, CAST(cc.collector_number AS INTEGER) ASC, cc.collector_number ASC",
        "name_desc": "cc.card_name COLLATE NOCASE DESC, cc.set_code ASC, CAST(cc.collector_number AS INTEGER) ASC, cc.collector_number ASC",
        "set_asc": "cc.set_code ASC, CAST(cc.collector_number AS INTEGER) ASC, cc.collector_number ASC, cc.card_name COLLATE NOCASE ASC",
        "set_desc": "cc.set_code DESC, CAST(cc.collector_number AS INTEGER) ASC, cc.collector_number ASC, cc.card_name COLLATE NOCASE ASC",
        "year_newest": "CAST(SUBSTR(COALESCE(s.release_date, ''), 1, 4) AS INTEGER) DESC, cc.card_name COLLATE NOCASE ASC",
        "year_oldest": "CAST(SUBSTR(COALESCE(s.release_date, ''), 1, 4) AS INTEGER) ASC, cc.card_name COLLATE NOCASE ASC",
        "rarity_low_high": """
            CASE LOWER(COALESCE(cc.rarity, ''))
                WHEN 'common' THEN 1
                WHEN 'uncommon' THEN 2
                WHEN 'rare' THEN 3
                WHEN 'mythic' THEN 4
                ELSE 9
            END ASC,
            cc.card_name COLLATE NOCASE ASC
        """,
        "rarity_high_low": """
            CASE LOWER(COALESCE(cc.rarity, ''))
                WHEN 'mythic' THEN 1
                WHEN 'rare' THEN 2
                WHEN 'uncommon' THEN 3
                WHEN 'common' THEN 4
                ELSE 9
            END ASC,
            cc.card_name COLLATE NOCASE ASC
        """,
        "mv_low_high": "COALESCE(cc.mana_value, 999) ASC, cc.card_name COLLATE NOCASE ASC",
        "mv_high_low": "COALESCE(cc.mana_value, -1) DESC, cc.card_name COLLATE NOCASE ASC",
        "edhrec_rank_best": "CASE WHEN cc.edhrec_rank IS NULL THEN 1 ELSE 0 END ASC, cc.edhrec_rank ASC, cc.card_name COLLATE NOCASE ASC",
        "edhrec_rank_worst": "CASE WHEN cc.edhrec_rank IS NULL THEN 1 ELSE 0 END ASC, cc.edhrec_rank DESC, cc.card_name COLLATE NOCASE ASC",
        "edhrec_salt_high": "CASE WHEN cc.edhrec_saltiness IS NULL THEN 1 ELSE 0 END ASC, cc.edhrec_saltiness DESC, cc.card_name COLLATE NOCASE ASC",
        "edhrec_salt_low": "CASE WHEN cc.edhrec_saltiness IS NULL THEN 1 ELSE 0 END ASC, cc.edhrec_saltiness ASC, cc.card_name COLLATE NOCASE ASC",
        "price_high": """
            CASE
                WHEN COALESCE(cp.tcgplayer_normal_price, cp.tcgplayer_foil_price, cp.tcgplayer_etched_price) IS NULL THEN 1
                ELSE 0
            END ASC,
            COALESCE(cp.tcgplayer_normal_price, cp.tcgplayer_foil_price, cp.tcgplayer_etched_price) DESC,
            cc.card_name COLLATE NOCASE ASC
        """,
        "price_low": """
            CASE
                WHEN COALESCE(cp.tcgplayer_normal_price, cp.tcgplayer_foil_price, cp.tcgplayer_etched_price) IS NULL THEN 1
                ELSE 0
            END ASC,
            COALESCE(cp.tcgplayer_normal_price, cp.tcgplayer_foil_price, cp.tcgplayer_etched_price) ASC,
            cc.card_name COLLATE NOCASE ASC
        """,
    }

    sort_sql = sort_sql_map.get(clean_sort_option, sort_sql_map["name_asc"])

    cursor.execute(
        f"""
        SELECT
            cc.card_uuid,
            cc.card_name,
            cc.set_code,
            s.release_date,
            cc.collector_number,
            cc.rarity,
            cc.type_line,
            cc.mana_cost,
            cc.mana_value,
            cc.colors_json,
            cc.color_identity_json,
            cc.edhrec_rank,
            cc.edhrec_saltiness,
            COALESCE(
                cp.tcgplayer_normal_price,
                cp.tcgplayer_foil_price,
                cp.tcgplayer_etched_price
            ) AS sort_price,
            cc.is_dual_faced,
            CASE
                WHEN cdsc.custom_set_card_id IS NULL THEN 0
                ELSE 1
            END AS already_in_set
        FROM chaos_cards cc
        LEFT JOIN sets s
            ON s.set_code = cc.set_code
        LEFT JOIN card_prices cp
            ON cp.card_uuid = cc.card_uuid
        LEFT JOIN custom_draft_set_cards cdsc
            ON cdsc.card_uuid = cc.card_uuid
           AND cdsc.set_code = ?
        {where_sql}
        ORDER BY
            {search_rank_sql}
            already_in_set ASC,
            {sort_sql}
        LIMIT ?
        """,
        params + order_exact_params + [parsed_limit],
    )

    rows = cursor.fetchall()
    conn.close()
    return rows

def add_card_to_custom_draft_set(set_code, card_uuid):
    clean_set_code = normalize_custom_draft_set_code(set_code)
    clean_card_uuid = str(card_uuid or "").strip()

    if not clean_set_code:
        raise ValueError("Custom set code is required.")

    if not clean_card_uuid:
        raise ValueError("Card UUID is required.")

    now_utc = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT card_name
        FROM chaos_cards
        WHERE card_uuid = ?
        """,
        (clean_card_uuid,),
    )

    card_row = cursor.fetchone()

    if not card_row:
        conn.close()
        raise ValueError("Card UUID was not found in chaos_cards.")

    cursor.execute(
        """
        INSERT OR IGNORE INTO custom_draft_set_cards (
            set_code,
            card_uuid,
            special_category_index,
            sort_name,
            added_at_utc
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            clean_set_code,
            clean_card_uuid,
            0,
            card_row["card_name"] or "",
            now_utc,
        ),
    )

    inserted = cursor.rowcount > 0

    conn.commit()
    conn.close()

    return {
        "ok": True,
        "inserted": inserted,
        "message": "Card added." if inserted else "Card is already in this custom set.",
    }


def update_custom_draft_set_card_category(custom_set_card_id, special_category_index):
    try:
        parsed_card_id = int(custom_set_card_id)
    except (TypeError, ValueError):
        raise ValueError("Invalid custom set card id.")

    try:
        parsed_category = int(special_category_index)
    except (TypeError, ValueError):
        parsed_category = 0

    if parsed_category not in {0, 1, 2, 3}:
        parsed_category = 0

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE custom_draft_set_cards
        SET special_category_index = ?
        WHERE custom_set_card_id = ?
        """,
        (
            parsed_category,
            parsed_card_id,
        ),
    )

    updated = cursor.rowcount

    conn.commit()
    conn.close()

    return {
        "ok": True,
        "updated": updated,
    }


def delete_custom_draft_set_card(custom_set_card_id):
    try:
        parsed_card_id = int(custom_set_card_id)
    except (TypeError, ValueError):
        raise ValueError("Invalid custom set card id.")

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        DELETE FROM custom_draft_set_cards
        WHERE custom_set_card_id = ?
        """,
        (parsed_card_id,),
    )

    deleted = cursor.rowcount

    conn.commit()
    conn.close()

    return {
        "ok": True,
        "deleted": deleted,
    }