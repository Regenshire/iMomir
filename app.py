import json
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone

import requests
from flask import Flask, Response, flash, jsonify, redirect, render_template, request, send_file, url_for

app = Flask(__name__)
app.secret_key = "imomir-dev-key"

DATABASE_PATH = "cards.db"
DATA_DOWNLOAD_DIR = os.path.join("data", "downloads")
ATOMIC_CARDS_PATH = os.path.join(DATA_DOWNLOAD_DIR, "AtomicCards.json")
MTGJSON_ATOMIC_URL = "https://mtgjson.com/api/v5/AtomicCards.json"

SCRYFALL_BULK_DATA_URL = "https://api.scryfall.com/bulk-data"

SCRYFALL_DOWNLOAD_DIR = os.path.join("data", "scryfall")
IMAGE_CACHE_DIR = os.path.join("data", "image_cache")
SCRYFALL_DEFAULT_CARDS_PATH = os.path.join(SCRYFALL_DOWNLOAD_DIR, "default-cards.json")

DEFAULT_CONFIG = {
    "type_creature": "1",
    "type_artifact": "0",
    "type_enchantment": "0",
    "type_instant": "0",
    "type_land": "0",
    "type_sorcery": "0",
    "type_planeswalker": "0",
    "type_battle": "0",
    "type_conspiracy": "0",
    "type_dungeon": "0",
    "type_emblem": "0",
    "type_phenomenon": "0",
    "type_plane": "0",
    "type_scheme": "0",
    "type_vanguard": "0",
    "allow_legendary": "1",
    "allow_unsets": "0",
    "allow_arena": "0",
    "all_sets_enabled": "1",
}

PRIMARY_TYPE_KEYS = [
    ("type_creature", "Creature"),
    ("type_artifact", "Artifact"),
    ("type_enchantment", "Enchantment"),
    ("type_instant", "Instant"),
    ("type_land", "Land"),
    ("type_sorcery", "Sorcery"),
    ("type_planeswalker", "Planeswalker"),
    ("type_battle", "Battle"),
]

SUPPLEMENTAL_TYPE_KEYS = [
    ("type_conspiracy", "Conspiracy"),
    ("type_dungeon", "Dungeon"),
    ("type_emblem", "Emblem"),
    ("type_phenomenon", "Phenomenon"),
    ("type_plane", "Plane"),
    ("type_scheme", "Scheme"),
    ("type_vanguard", "Vanguard"),
]

OTHER_FILTER_KEYS = [
    ("allow_legendary", "Allow Legendary"),
    ("allow_unsets", "Allow Un-sets"),
    ("allow_arena", "Allow Arena"),
]

SAMPLE_SETS = [
    ("LEA", "Limited Edition Alpha", "1993-08-05", "core"),
    ("2ED", "Unlimited Edition", "1993-12-01", "core"),
    ("ARN", "Arabian Nights", "1993-12-17", "expansion"),
    ("ICE", "Ice Age", "1995-06-03", "expansion"),
    ("TMP", "Tempest", "1997-10-14", "expansion"),
    ("INV", "Invasion", "2000-10-02", "expansion"),
    ("8ED", "Eighth Edition", "2003-07-28", "core"),
    ("ZEN", "Zendikar", "2009-10-02", "expansion"),
    ("RTR", "Return to Ravnica", "2012-10-05", "expansion"),
    ("KHM", "Kaldheim", "2021-02-05", "expansion"),
    ("WOE", "Wilds of Eldraine", "2023-09-08", "expansion"),
    ("FIN", "Final Fantasy", "2025-06-13", "expansion"),
]

TYPE_FLAG_MAP = {
    "Creature": "is_creature",
    "Artifact": "is_artifact",
    "Enchantment": "is_enchantment",
    "Instant": "is_instant",
    "Land": "is_land",
    "Sorcery": "is_sorcery",
    "Planeswalker": "is_planeswalker",
    "Battle": "is_battle",
    "Conspiracy": "is_conspiracy",
    "Dungeon": "is_dungeon",
    "Emblem": "is_emblem",
    "Phenomenon": "is_phenomenon",
    "Plane": "is_plane",
    "Scheme": "is_scheme",
    "Vanguard": "is_vanguard",
}

refresh_status = {
    "is_running": False,
    "stage": "Idle",
    "message": "No refresh has been run yet.",
    "started_at": None,
    "finished_at": None,
    "cards_processed": 0,
    "cards_imported": 0,
    "total_cards": 0,
    "sets_represented": 0,
    "source_last_updated": "",
    "error": "",
}
refresh_lock = threading.Lock()

image_download_status = {
    "is_running": False,
    "stage": "Idle",
    "message": "No image download has been run yet.",
    "started_at": None,
    "finished_at": None,
    "cards_processed": 0,
    "cards_downloaded": 0,
    "cards_disabled": 0,
    "total_cards": 0,
    "error": "",
}
image_download_lock = threading.Lock()


def set_refresh_status(**kwargs):
    with refresh_lock:
        refresh_status.update(kwargs)


def get_refresh_status_copy():
    with refresh_lock:
        return dict(refresh_status)

def set_image_download_status(**kwargs):
    with image_download_lock:
        image_download_status.update(kwargs)


def get_image_download_status_copy():
    with image_download_lock:
        return dict(image_download_status)

def get_db_connection():
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
    ensure_column_exists(cursor, "scryfall_default_cards", "games", "TEXT")

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
            games TEXT
        )
        """
    )

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

    seed_sample_sets()


def seed_sample_sets():
    conn = get_db_connection()
    cursor = conn.cursor()

    for set_code, set_name, release_date, set_type in SAMPLE_SETS:
        cursor.execute(
            """
            INSERT OR IGNORE INTO sets (set_code, set_name, release_date, set_type)
            VALUES (?, ?, ?, ?)
            """,
            (set_code, set_name, release_date, set_type),
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


def update_config_from_form(form_data):
    updated_config = {}

    for key in DEFAULT_CONFIG.keys():
        if key == "all_sets_enabled":
            continue
        updated_config[key] = "1" if form_data.get(key) == "on" else "0"

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


def get_all_sets():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT set_code, set_name, release_date, set_type
        FROM sets
        ORDER BY set_name COLLATE NOCASE ASC
        """
    )

    rows = cursor.fetchall()
    conn.close()
    return rows


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

def build_card_filter_query(mana_value, config, selected_set_codes):
    conditions = []
    params = []

    conditions.append("disable_card = 0")

    # Mana value (exact match for now)
    conditions.append("CAST(mana_value AS INTEGER) = ?")
    params.append(int(mana_value))

    # Card types
    type_conditions = []

    for key, _ in PRIMARY_TYPE_KEYS + SUPPLEMENTAL_TYPE_KEYS:
        if config.get(key) == "1":
            column = key.replace("type_", "is_")
            type_conditions.append(f"{column} = 1")

    if type_conditions:
        conditions.append("(" + " OR ".join(type_conditions) + ")")

    # Legendary filter
    if config.get("allow_legendary") == "0":
        conditions.append("is_legendary = 0")

    # Un-set filter
    if config.get("allow_unsets") == "0":
        conditions.append("is_unset = 0")

    # Arena filter
    if config.get("allow_arena") == "0":
        conditions.append("has_paper_printing = 1")

    # Set filtering
    if config.get("all_sets_enabled") == "0" and selected_set_codes:
        set_conditions = []
        for code in selected_set_codes:
            set_conditions.append("printings_json LIKE ?")
            params.append(f'%"{code}"%')

        if set_conditions:
            conditions.append("(" + " OR ".join(set_conditions) + ")")

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    return where_clause, params

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

def draw_random_card(mana_value):
    config = get_config()
    selected_set_codes = get_selected_set_codes()

    where_clause, params = build_card_filter_query(
        mana_value, config, selected_set_codes
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

def build_image_candidate_filter_query(config, selected_set_codes, force_redownload=False):
    conditions = []
    params = []

    conditions.append("disable_card = 0")

    type_conditions = []

    for key, _ in PRIMARY_TYPE_KEYS + SUPPLEMENTAL_TYPE_KEYS:
        if config.get(key) == "1":
            column = key.replace("type_", "is_")
            type_conditions.append(f"{column} = 1")

    if type_conditions:
        conditions.append("(" + " OR ".join(type_conditions) + ")")

    if config.get("allow_legendary") == "0":
        conditions.append("is_legendary = 0")

    if config.get("allow_unsets") == "0":
        conditions.append("is_unset = 0")

    if config.get("allow_arena") == "0":
        conditions.append("has_paper_printing = 1")

    if config.get("all_sets_enabled") == "0" and selected_set_codes:
        set_conditions = []
        for code in selected_set_codes:
            set_conditions.append("printings_json LIKE ?")
            params.append(f'%"{code}"%')

        if set_conditions:
            conditions.append("(" + " OR ".join(set_conditions) + ")")

    if not force_redownload:
            conditions.append("(image_cache_path IS NULL OR image_cached_at IS NULL)")

    where_clause = " AND ".join(conditions) if conditions else "1=1"
    return where_clause, params

def update_selected_sets_from_form(form_data):
    all_sets_enabled = "1" if form_data.get("all_sets_enabled") == "on" else "0"

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO app_config (config_key, config_value)
        VALUES (?, ?)
        ON CONFLICT(config_key) DO UPDATE SET config_value = excluded.config_value
        """,
        ("all_sets_enabled", all_sets_enabled),
    )

    cursor.execute("DELETE FROM selected_sets")

    if all_sets_enabled == "0":
        selected_set_codes = form_data.getlist("selected_sets")

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


def ensure_download_directories():
    os.makedirs(DATA_DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(SCRYFALL_DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(IMAGE_CACHE_DIR, exist_ok=True)

def parse_remote_last_modified(raw_value):
    raw_value = (raw_value or "").strip()
    if not raw_value:
        return None

    for fmt in (
        "%a, %d %b %Y %H:%M:%S GMT",
        "%Y-%m-%d %H:%M:%S UTC",
        "%Y-%b-%d %H:%M",
    ):
        try:
            return datetime.strptime(raw_value, fmt)
        except ValueError:
            pass

    return None


def get_remote_atomic_cards_timestamp_text():
    response = requests.head(MTGJSON_ATOMIC_URL, timeout=60, allow_redirects=True)
    response.raise_for_status()

    last_modified = response.headers.get("Last-Modified")
    if not last_modified:
        raise ValueError("Remote AtomicCards.json did not return a Last-Modified header.")

    return last_modified


def get_local_atomic_cards_timestamp_text():
    if not os.path.exists(ATOMIC_CARDS_PATH):
        return None

    metadata = get_import_metadata()
    saved_value = metadata.get("source_last_updated")
    if saved_value:
        return saved_value

    file_mtime = datetime.utcfromtimestamp(os.path.getmtime(ATOMIC_CARDS_PATH))
    return file_mtime.strftime("%Y-%m-%d %H:%M:%S UTC")


def should_download_atomic_cards(force_download=False):
    if force_download:
        return True, "Forced refresh requested."

    if not os.path.exists(ATOMIC_CARDS_PATH):
        return True, "Local AtomicCards.json not found."

    remote_timestamp_text = get_remote_atomic_cards_timestamp_text()
    local_timestamp_text = get_local_atomic_cards_timestamp_text()

    remote_dt = parse_remote_last_modified(remote_timestamp_text)
    local_dt = parse_remote_last_modified(local_timestamp_text)

    if remote_dt is None:
        return True, "Could not parse remote AtomicCards.json timestamp."

    if local_dt is None:
        return True, "Could not parse local AtomicCards.json timestamp."

    if remote_dt > local_dt:
        return True, "Remote AtomicCards.json is newer than the local file."

    return False, "Local AtomicCards.json is already up to date."

def download_atomic_cards_json(force_download=False):
    ensure_download_directories()

    should_download, reason = should_download_atomic_cards(force_download=force_download)

    if not should_download:
        set_refresh_status(
            stage="Download Check",
            message=f"Skipped download. {reason}",
        )
        return {
            "downloaded": False,
            "reason": reason,
            "remote_timestamp": get_remote_atomic_cards_timestamp_text(),
        }

    set_refresh_status(
        stage="Downloading",
        message=f"Downloading AtomicCards.json from MTGJSON... {reason}",
    )

    response = requests.get(MTGJSON_ATOMIC_URL, timeout=180)
    response.raise_for_status()

    with open(ATOMIC_CARDS_PATH, "wb") as file_handle:
        file_handle.write(response.content)

    remote_timestamp_text = get_remote_atomic_cards_timestamp_text()

    set_import_metadata("source_url", MTGJSON_ATOMIC_URL)
    set_import_metadata("source_last_updated", remote_timestamp_text)

    return {
        "downloaded": True,
        "reason": reason,
        "remote_timestamp": remote_timestamp_text,
    }


def safe_list(value):
    if isinstance(value, list):
        return value
    return []


def safe_dict(value):
    if isinstance(value, dict):
        return value
    return {}

def normalize_card_lookup_name(card_name):
    value = (card_name or "").strip()

    if not value:
        return ""

    # Handle A // B cases
    if " // " in value:
        value = value.split(" // ", 1)[0].strip()

    # Normalize punctuation (keep it simple)
    value = value.replace(",", "")
    value = value.replace("'", "")
    value = value.replace("’", "")
    value = value.replace("-", "")
    value = value.replace("  ", " ")
    

    return value.strip().lower()

def build_scryfall_image_url(scryfall_id):
    if not scryfall_id or len(scryfall_id) < 2:
        return None

    return f"https://cards.scryfall.io/normal/front/{scryfall_id[0]}/{scryfall_id[1]}/{scryfall_id}.jpg"

def safe_filename(value):
    allowed = []
    for ch in (value or ""):
        if ch.isalnum() or ch in ("-", "_", "."):
            allowed.append(ch)
        else:
            allowed.append("_")
    return "".join(allowed).strip("_") or "card"


def get_scryfall_bulk_default_cards_download_uri():
    headers = {
        "User-Agent": "iMomir/1.0",
        "Accept": "application/json;q=0.9,*/*;q=0.8",
    }

    response = requests.get(
        SCRYFALL_BULK_DATA_URL,
        headers=headers,
        timeout=60,
    )
    response.raise_for_status()

    payload = response.json()
    bulk_items = safe_list(payload.get("data"))

    for item in bulk_items:
        if item.get("type") == "default_cards":
            return {
                "download_uri": item.get("download_uri"),
                "updated_at": item.get("updated_at"),
            }

    raise ValueError("Could not find Scryfall default_cards bulk entry.")


def download_scryfall_default_cards_json(force_download=False):
    ensure_download_directories()

    bulk_info = get_scryfall_bulk_default_cards_download_uri()
    remote_updated_at = bulk_info["updated_at"]
    local_metadata = get_import_metadata()
    local_updated_at = local_metadata.get("scryfall_default_cards_updated_at")

    if (
        not force_download
        and os.path.exists(SCRYFALL_DEFAULT_CARDS_PATH)
        and local_updated_at
        and local_updated_at == remote_updated_at
    ):
        return {
            "downloaded": False,
            "updated_at": remote_updated_at,
            "message": "Local Scryfall default-cards file is already current.",
        }

    headers = {
        "User-Agent": "iMomir/1.0",
        "Accept": "application/json;q=0.9,*/*;q=0.8",
    }

    response = requests.get(
        bulk_info["download_uri"],
        headers=headers,
        timeout=300,
    )
    response.raise_for_status()

    with open(SCRYFALL_DEFAULT_CARDS_PATH, "wb") as file_handle:
        file_handle.write(response.content)

    set_import_metadata("scryfall_default_cards_updated_at", remote_updated_at)
    set_import_metadata("scryfall_default_cards_path", SCRYFALL_DEFAULT_CARDS_PATH)

    return {
        "downloaded": True,
        "updated_at": remote_updated_at,
        "message": "Downloaded Scryfall default-cards bulk file.",
    }


def import_scryfall_default_cards_into_database():
    if not os.path.exists(SCRYFALL_DEFAULT_CARDS_PATH):
        raise FileNotFoundError("Scryfall default-cards bulk file was not found.")

    with open(SCRYFALL_DEFAULT_CARDS_PATH, "r", encoding="utf-8") as file_handle:
        raw_json = json.load(file_handle)

    if not isinstance(raw_json, list):
        raise ValueError("Scryfall default-cards JSON was not a list.")

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM scryfall_default_cards")

    inserted_count = 0

    for card_obj in raw_json:
        if not isinstance(card_obj, dict):
            continue

        # Skip multi-face cards (we only want clean single-face printings)
        if card_obj.get("card_faces"):
            continue

        image_uris = safe_dict(card_obj.get("image_uris"))
        games = json.dumps(card_obj.get("games", []))
        if not image_uris:
            continue

        scryfall_id = card_obj.get("id")
        oracle_id = card_obj.get("oracle_id")
        card_name = card_obj.get("name")
        set_code = card_obj.get("set")
        collector_number = card_obj.get("collector_number")
        released_at = card_obj.get("released_at")

        normal_image_url = image_uris.get("normal")
        large_image_url = image_uris.get("large")
        image_url = normal_image_url or large_image_url

        if not scryfall_id or not card_name or not set_code or not image_url:
            continue

        cursor.execute(
            """
            INSERT INTO scryfall_default_cards (
                scryfall_id,
                oracle_id,
                card_name,
                set_code,
                collector_number,
                released_at,
                image_url,
                normal_image_url,
                large_image_url,
                games
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                scryfall_id,
                oracle_id,
                card_name,
                set_code.lower(),
                collector_number,
                released_at,
                image_url,
                normal_image_url,
                large_image_url,
                games,
            ),
        )

        inserted_count += 1

        if inserted_count % 2000 == 0:
            conn.commit()

    conn.commit()
    conn.close()

    refresh_cards_has_paper_printing()

    set_import_metadata("scryfall_default_cards_rows", inserted_count)

    return inserted_count

def refresh_cards_has_paper_printing():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE cards
        SET has_paper_printing = 0
        """
    )

    cursor.execute(
        """
        UPDATE cards
        SET has_paper_printing = 1
        WHERE EXISTS (
            SELECT 1
            FROM scryfall_default_cards
            WHERE scryfall_default_cards.oracle_id = cards.scryfall_id
              AND (scryfall_default_cards.normal_image_url IS NOT NULL OR scryfall_default_cards.large_image_url IS NOT NULL)
              AND scryfall_default_cards.games LIKE '%paper%'
        )
        """
    )

    conn.commit()
    conn.close()

def find_best_local_scryfall_image_match(card_row, selected_set_codes):
    conn = get_db_connection()
    cursor = conn.cursor()

    oracle_id = (card_row["scryfall_id"] or "").strip()
    raw_card_name = (card_row["name"] or "").strip()
    normalized_card_name = normalize_card_lookup_name(raw_card_name)

    # 1. Try oracle_id match first.
    # Note: in the current schema, cards.scryfall_id may actually contain
    # a Scryfall Oracle ID fallback, so this can still work for many cards.
    if oracle_id:
        cursor.execute(
            """
            SELECT *
            FROM scryfall_default_cards
            WHERE oracle_id = ?
              AND (normal_image_url IS NOT NULL OR large_image_url IS NOT NULL)
              AND (games LIKE '%paper%' OR games IS NULL)
            ORDER BY released_at ASC
            LIMIT 1
            """,
            (oracle_id,),
        )
        row = cursor.fetchone()
        if row:
            conn.close()
            return row

    # 2. Fallback: try normalized/base name first.
    if normalized_card_name:
        cursor.execute(
            """
            SELECT *
            FROM scryfall_default_cards
            WHERE LOWER(REPLACE(REPLACE(REPLACE(card_name, ',', ''), '''', ''), '’', '')) = ?
              AND (normal_image_url IS NOT NULL OR large_image_url IS NOT NULL)
              AND (games LIKE '%paper%' OR games IS NULL)
            ORDER BY released_at ASC
            LIMIT 1
            """,
            (normalized_card_name,),
        )
        row = cursor.fetchone()
        if row:
            conn.close()
            return row

    # 3. Final fallback: try the original raw name.
    # This keeps compatibility for cards whose stored name is already correct.
    if raw_card_name and raw_card_name != normalized_card_name:
        cursor.execute(
            """
            SELECT *
            FROM scryfall_default_cards
            WHERE card_name = ?
              AND (normal_image_url IS NOT NULL OR large_image_url IS NOT NULL)
              AND (games LIKE '%paper%' OR games IS NULL)
            ORDER BY released_at ASC
            LIMIT 1
            """,
            (raw_card_name,),
        )
        row = cursor.fetchone()
        if row:
            conn.close()
            return row

    conn.close()
    return None


def download_and_cache_card_image(card_row, scryfall_match_row):
    ensure_download_directories()

    image_url = (
        scryfall_match_row["normal_image_url"]
        or scryfall_match_row["large_image_url"]
        or scryfall_match_row["image_url"]
    )
    if not image_url:
        return None

    headers = {
        "User-Agent": "iMomir/1.0",
        "Accept": "*/*",
    }

    response = requests.get(image_url, headers=headers, timeout=120)
    response.raise_for_status()

    set_code = (scryfall_match_row["set_code"] or "set").lower()
    file_ext = ".jpg"
    content_type = response.headers.get("Content-Type", "").lower()

    if "png" in content_type:
        file_ext = ".png"
    elif "jpeg" in content_type or "jpg" in content_type:
        file_ext = ".jpg"
    elif image_url.lower().endswith(".png"):
        file_ext = ".png"

    filename = f"{safe_filename(card_row['card_key'])}_{safe_filename(set_code)}{file_ext}"
    abs_path = os.path.join(IMAGE_CACHE_DIR, filename)
    rel_path = os.path.join("data", "image_cache", filename)

    with open(abs_path, "wb") as file_handle:
        file_handle.write(response.content)

    return {
        "absolute_path": abs_path,
        "relative_path": rel_path.replace("\\", "/"),
        "image_url": image_url,
    }

def get_primary_atomic_version(atomic_versions):
    if not isinstance(atomic_versions, list):
        return None

    for item in atomic_versions:
        if isinstance(item, dict):
            return item

    return None

def build_type_flags(card_types):
    flags = {
        "is_creature": 0,
        "is_artifact": 0,
        "is_enchantment": 0,
        "is_instant": 0,
        "is_land": 0,
        "is_sorcery": 0,
        "is_planeswalker": 0,
        "is_battle": 0,
        "is_conspiracy": 0,
        "is_dungeon": 0,
        "is_emblem": 0,
        "is_phenomenon": 0,
        "is_plane": 0,
        "is_scheme": 0,
        "is_vanguard": 0,
    }

    for card_type in safe_list(card_types):
        flag_name = TYPE_FLAG_MAP.get(card_type)
        if flag_name:
            flags[flag_name] = 1

    return flags


def import_atomic_cards_into_database():
    if not os.path.exists(ATOMIC_CARDS_PATH):
        raise FileNotFoundError("AtomicCards.json was not found after download.")

    set_refresh_status(stage="Parsing", message="Reading AtomicCards.json...")

    with open(ATOMIC_CARDS_PATH, "r", encoding="utf-8") as file_handle:
        raw_json = json.load(file_handle)

    atomic_cards = raw_json.get("data", {})
    if not isinstance(atomic_cards, dict):
        raise ValueError("AtomicCards.json did not contain a valid 'data' object.")

    total_cards = len(atomic_cards)

    set_refresh_status(
        stage="Importing",
        message="Importing card records into SQLite...",
        total_cards=total_cards,
        cards_processed=0,
        cards_imported=0,
        sets_represented=0,
    )

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM cards")

    imported_count = 0
    represented_sets = set()

    for index, (card_key, atomic_versions) in enumerate(atomic_cards.items(), start=1):
        atomic_card = get_primary_atomic_version(atomic_versions)
        if not atomic_card:
            continue

        identifiers = safe_dict(atomic_card.get("identifiers"))

        scryfall_id = identifiers.get("scryfallId")
        scryfall_oracle_id = identifiers.get("scryfallOracleId")

        types = safe_list(atomic_card.get("types"))
        supertypes = safe_list(atomic_card.get("supertypes"))
        printings = safe_list(atomic_card.get("printings"))

        type_flags = build_type_flags(types)

        first_printing = atomic_card.get("firstPrinting")
        if first_printing:
            represented_sets.add(first_printing)

        for set_code in printings:
            if isinstance(set_code, str) and set_code:
                represented_sets.add(set_code)

        is_legendary = 1 if "Legendary" in supertypes else 0
        is_unset = 1 if atomic_card.get("isFunny") is True else 0
        is_arena = 1 if "A" in printings else 0

        image_url = build_scryfall_image_url(scryfall_id)

        cursor.execute(
            """
            INSERT INTO cards (
                card_key,
                name,
                face_name,
                mana_value,
                mana_cost,
                type_line,
                layout,
                first_printing,
                printings_json,
                scryfall_id,
                image_url,
                is_legendary,
                is_unset,
                is_arena,
                has_paper_printing,
                is_creature,
                is_artifact,
                is_enchantment,
                is_instant,
                is_land,
                is_sorcery,
                is_planeswalker,
                is_battle,
                is_conspiracy,
                is_dungeon,
                is_emblem,
                is_phenomenon,
                is_plane,
                is_scheme,
                is_vanguard
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                card_key,
                atomic_card.get("name") or card_key,
                atomic_card.get("faceName"),
                atomic_card.get("manaValue"),
                atomic_card.get("manaCost"),
                atomic_card.get("type") or "",
                atomic_card.get("layout"),
                first_printing,
                json.dumps(printings),
                scryfall_id or scryfall_oracle_id or "",
                image_url,
                is_legendary,
                is_unset,
                is_arena,
                0,
                type_flags["is_creature"],
                type_flags["is_artifact"],
                type_flags["is_enchantment"],
                type_flags["is_instant"],
                type_flags["is_land"],
                type_flags["is_sorcery"],
                type_flags["is_planeswalker"],
                type_flags["is_battle"],
                type_flags["is_conspiracy"],
                type_flags["is_dungeon"],
                type_flags["is_emblem"],
                type_flags["is_phenomenon"],
                type_flags["is_plane"],
                type_flags["is_scheme"],
                type_flags["is_vanguard"],
            ),
        )

        imported_count += 1

        if index % 500 == 0 or index == total_cards:
            conn.commit()
            set_refresh_status(
                stage="Importing",
                message=f"Imported {imported_count} of {total_cards} atomic cards...",
                cards_processed=index,
                cards_imported=imported_count,
                sets_represented=len(represented_sets),
            )

    conn.commit()
    conn.close()

    refresh_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    set_import_metadata("last_refresh_utc", refresh_time)
    set_import_metadata("source_url", MTGJSON_ATOMIC_URL)
    set_import_metadata("cards_imported", imported_count)
    set_import_metadata("sets_represented", len(represented_sets))

    return {
        "cards_imported": imported_count,
        "sets_represented": len(represented_sets),
        "last_refresh_utc": refresh_time,
    }

def ensure_card_image_cached(card_row):
    if not card_row:
        return None

    existing_cache_path = card_row["image_cache_path"] or ""
    if existing_cache_path:
        abs_path = os.path.abspath(existing_cache_path)
        if os.path.exists(abs_path):
            return card_row

    selected_set_codes = get_selected_set_codes()

    match_row = find_best_local_scryfall_image_match(
        card_row=card_row,
        selected_set_codes=selected_set_codes,
    )

    cached_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        if match_row:
            download_result = download_and_cache_card_image(card_row, match_row)

            if download_result:
                cursor.execute(
                    """
                    UPDATE cards
                    SET image_cache_path = ?,
                        image_source_url = ?,
                        image_url = ?,
                        image_cached_at = ?,
                        disable_card = 0
                    WHERE card_key = ?
                    """,
                    (
                        download_result["relative_path"],
                        download_result["image_url"],
                        download_result["image_url"],
                        cached_at,
                        card_row["card_key"],
                    ),
                )
                conn.commit()
                return get_card_by_key(card_row["card_key"])

        cursor.execute(
            """
            UPDATE cards
            SET image_cached_at = COALESCE(image_cached_at, ?)
            WHERE card_key = ?
            """,
            (
                cached_at,
                card_row["card_key"],
            ),
        )
        conn.commit()

        return get_card_by_key(card_row["card_key"])
    finally:
        conn.close()

def run_image_download_job(force_redownload=False):
    conn = None

    try:
        set_image_download_status(
            is_running=True,
            stage="Preparing",
            message="Preparing Scryfall bulk image index...",
            started_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            finished_at=None,
            cards_processed=0,
            cards_downloaded=0,
            cards_disabled=0,
            total_cards=0,
            error="",
        )

        bulk_result = download_scryfall_default_cards_json(force_download=False)

        set_image_download_status(
            stage="Indexing",
            message=bulk_result["message"],
        )

        import_scryfall_default_cards_into_database()

        config = get_config()
        selected_set_codes = get_selected_set_codes()

        where_clause, params = build_image_candidate_filter_query(
            config=config,
            selected_set_codes=selected_set_codes,
            force_redownload=force_redownload,
        )

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            f"""
            SELECT card_key, name, first_printing, printings_json, scryfall_id
            FROM cards
            WHERE {where_clause}
            ORDER BY name COLLATE NOCASE ASC
            """,
            params,
        )
        candidate_rows = cursor.fetchall()

        total_cards = len(candidate_rows)

        set_image_download_status(
            stage="Downloading",
            message=f"Found {total_cards} cards to process.",
            total_cards=total_cards,
        )

        downloaded_count = 0
        disabled_count = 0

        for index, card_row in enumerate(candidate_rows, start=1):
            set_image_download_status(
                stage="Downloading",
                message=f"Processing {card_row['name']} ({index} of {total_cards})...",
                cards_processed=index - 1,
                cards_downloaded=downloaded_count,
                cards_disabled=disabled_count,
                total_cards=total_cards,
            )

            match_row = find_best_local_scryfall_image_match(
                card_row=card_row,
                selected_set_codes=selected_set_codes,
            )

            cached_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

            if match_row:
                download_result = download_and_cache_card_image(card_row, match_row)

                if download_result:
                    cursor.execute(
                        """
                        UPDATE cards
                        SET image_cache_path = ?,
                            image_source_url = ?,
                            image_url = ?,
                            image_cached_at = ?,
                            disable_card = 0
                        WHERE card_key = ?
                        """,
                        (
                            download_result["relative_path"],
                            download_result["image_url"],
                            download_result["image_url"],
                            cached_at,
                            card_row["card_key"],
                        ),
                    )
                    downloaded_count += 1
                else:
                    cursor.execute(
                        """
                        UPDATE cards
                        SET image_cached_at = ?
                        WHERE card_key = ?
                        """,
                        (
                            cached_at,
                            card_row["card_key"],
                        ),
                    )
                    disabled_count += 1
            else:
                cursor.execute(
                    """
                    UPDATE cards
                    SET image_cached_at = ?
                    WHERE card_key = ?
                    """,
                    (
                        cached_at,
                        card_row["card_key"],
                    ),
                )
                disabled_count += 1

            if index % 20 == 0 or index == total_cards:
                conn.commit()

            set_image_download_status(
                stage="Downloading",
                message=f"Processed {index} of {total_cards} cards.",
                cards_processed=index,
                cards_downloaded=downloaded_count,
                cards_disabled=disabled_count,
                total_cards=total_cards,
            )

        finished_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        set_image_download_status(
            is_running=False,
            stage="Complete",
            message=f"Image download complete. Downloaded {downloaded_count} images. Disabled {disabled_count} cards.",
            finished_at=finished_at,
            cards_processed=total_cards,
            cards_downloaded=downloaded_count,
            cards_disabled=disabled_count,
            total_cards=total_cards,
        )

    except Exception as exc:
        set_image_download_status(
            is_running=False,
            stage="Failed",
            message="Card image download failed.",
            finished_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            error=str(exc),
        )
    finally:
        if conn is not None:
            conn.commit()
            conn.close()

def run_refresh_job(force_download=False):
    try:
        set_refresh_status(
            is_running=True,
            stage="Starting",
            message="Starting refresh...",
            started_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            finished_at=None,
            cards_processed=0,
            cards_imported=0,
            total_cards=0,
            sets_represented=0,
            error="",
        )

        download_result = download_atomic_cards_json(force_download=force_download)

        set_refresh_status(
            stage="Preparing Import",
            message=f"{download_result['reason']} Beginning import from local AtomicCards.json..."
        )

        summary = import_atomic_cards_into_database()

        scryfall_bulk_result = download_scryfall_default_cards_json(force_download=False)

        set_refresh_status(
            stage="Indexing Paper Printings",
            message=scryfall_bulk_result["message"],
            cards_processed=summary["cards_imported"],
            cards_imported=summary["cards_imported"],
            sets_represented=summary["sets_represented"],
        )

        import_scryfall_default_cards_into_database()

        if download_result.get("remote_timestamp"):
            set_import_metadata("source_last_updated", download_result["remote_timestamp"])

        set_refresh_status(
            is_running=False,
            stage="Complete",
            message=f"Refresh complete. Imported {summary['cards_imported']} atomic cards across {summary['sets_represented']} sets.",
            finished_at=summary["last_refresh_utc"],
            cards_processed=summary["cards_imported"],
            cards_imported=summary["cards_imported"],
            sets_represented=summary["sets_represented"],
            source_last_updated=download_result.get("remote_timestamp", ""),
        )
    except Exception as exc:
        set_refresh_status(
            is_running=False,
            stage="Failed",
            message="Refresh failed.",
            finished_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            error=str(exc),
        )


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/result")
def result():
    mana_value = request.args.get("mana_value", "").strip()

    if not mana_value.isdigit():
        return render_template("result.html", mana_value=mana_value, card=None)

    card = draw_random_card(int(mana_value))

    if card:
        existing_cache_path = card["image_cache_path"] or ""
        cache_exists = False

        if existing_cache_path:
            cache_exists = os.path.exists(os.path.abspath(existing_cache_path))

        if not cache_exists:
            card = ensure_card_image_cached(card)

    return render_template(
        "result.html",
        mana_value=mana_value,
        card=card,
    )


@app.route("/config", methods=["GET", "POST"])
def config():
    if request.method == "POST":
        update_config_from_form(request.form)
        flash("Configuration saved.")
        return redirect(url_for("config"))

    config_values = get_config()
    import_metadata = get_import_metadata()
    current_refresh_status = get_refresh_status_copy()

    return render_template(
        "config.html",
        config=config_values,
        primary_type_keys=PRIMARY_TYPE_KEYS,
        supplemental_type_keys=SUPPLEMENTAL_TYPE_KEYS,
        other_filter_keys=OTHER_FILTER_KEYS,
        import_metadata=import_metadata,
        refresh_status=current_refresh_status,
    )


@app.route("/refresh-cards/start", methods=["POST"])
def refresh_cards_start():
    current_status = get_refresh_status_copy()
    if current_status["is_running"]:
        return jsonify({"ok": False, "message": "Refresh is already running."}), 409

    payload = request.get_json(silent=True) or {}
    force_download = bool(payload.get("force_download", False))

    worker = threading.Thread(target=run_refresh_job, kwargs={"force_download": force_download}, daemon=True)
    worker.start()

    return jsonify({"ok": True})


@app.route("/refresh-cards/status", methods=["GET"])
def refresh_cards_status():
    return jsonify(get_refresh_status_copy())

@app.route("/download-card-images/start", methods=["POST"])
def download_card_images_start():
    current_status = get_image_download_status_copy()
    if current_status["is_running"]:
        return jsonify({"ok": False, "message": "Card image download is already running."}), 409

    payload = request.get_json(silent=True) or {}
    force_redownload = bool(payload.get("force_redownload", False))

    worker = threading.Thread(
        target=run_image_download_job,
        kwargs={"force_redownload": force_redownload},
        daemon=True,
    )
    worker.start()

    return jsonify({"ok": True})


@app.route("/download-card-images/status", methods=["GET"])
def download_card_images_status():
    return jsonify(get_image_download_status_copy())

@app.route("/card-image/<card_key>", methods=["GET"])
def card_image(card_key):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT image_cache_path, image_url
        FROM cards
        WHERE card_key = ?
        """,
        (card_key,),
    )
    row = cursor.fetchone()
    conn.close()

    if not row:
        return ("Not found", 404)

    if row["image_cache_path"]:
        abs_path = os.path.abspath(row["image_cache_path"])
        if os.path.exists(abs_path):
            return send_file(abs_path)

    if row["image_url"]:
        return redirect(row["image_url"])

    return ("Not found", 404)

@app.route("/sets", methods=["GET", "POST"])
def sets():
    if request.method == "POST":
        update_selected_sets_from_form(request.form)
        flash("Magic set selection saved.")
        return redirect(url_for("sets"))

    config_values = get_config()
    all_sets = get_all_sets()
    selected_set_codes = get_selected_set_codes()

    return render_template(
        "sets.html",
        config=config_values,
        all_sets=all_sets,
        selected_set_codes=selected_set_codes,
    )

if __name__ == "__main__":
    initialize_database()
    app.run(debug=True)