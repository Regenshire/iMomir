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
    ensure_column_exists(cursor, "chaos_cards", "faces_json", "TEXT")
    ensure_column_exists(cursor, "chaos_cards", "face_count", "INTEGER NOT NULL DEFAULT 0")
    ensure_column_exists(cursor, "chaos_cards", "is_dual_faced", "INTEGER NOT NULL DEFAULT 0")
    ensure_column_exists(cursor, "chaos_cards", "front_image_url", "TEXT")
    ensure_column_exists(cursor, "chaos_cards", "back_image_url", "TEXT")
    ensure_column_exists(cursor, "chaos_cards", "front_face_name", "TEXT")
    ensure_column_exists(cursor, "chaos_cards", "back_face_name", "TEXT")

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
            back_face_name TEXT
        )
        """
    )

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
        CREATE TABLE IF NOT EXISTS chaos_session_state (
            state_key TEXT PRIMARY KEY,
            state_value TEXT NOT NULL
        )
        """
    )

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
        CREATE INDEX IF NOT EXISTS idx_chaos_pack_history_set_booster
        ON chaos_pack_history (set_code, booster_name, booster_index)
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