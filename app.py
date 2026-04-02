import csv
import gzip
import json
import os
import random
import socket
import sqlite3
import sys
import threading
import time
from datetime import datetime, timezone

import requests
from PIL import Image, ImageEnhance, ImageOps, ImageFilter, ImageChops
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader, simpleSplit
from reportlab.pdfgen import canvas
from io import BytesIO
from flask import Flask, Response, flash, jsonify, redirect, render_template, request, send_file, url_for



def get_bundle_base_dir():
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return sys._MEIPASS
    return os.path.dirname(os.path.abspath(__file__))

def get_runtime_base_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))

BUNDLE_BASE_DIR = get_bundle_base_dir()
RUNTIME_BASE_DIR = get_runtime_base_dir()

app = Flask(
    __name__,
    template_folder=os.path.join(BUNDLE_BASE_DIR, "templates"),
    static_folder=os.path.join(BUNDLE_BASE_DIR, "static"),
)
app.secret_key = "imomir-dev-key"

@app.context_processor
def inject_global_template_state():
    config = get_config()
    current_game_mode = (config.get("game_mode") or "custom").strip().lower()

    return {
        "nav_current_game_mode": current_game_mode,
    }

DATABASE_PATH = os.path.join(RUNTIME_BASE_DIR, "cards.db")
DATA_ROOT_DIR = os.path.join(RUNTIME_BASE_DIR, "data")
DATA_DOWNLOAD_DIR = os.path.join(DATA_ROOT_DIR, "downloads")
ATOMIC_CARDS_PATH = os.path.join(DATA_DOWNLOAD_DIR, "AtomicCards.json")
SET_LIST_PATH = os.path.join(DATA_DOWNLOAD_DIR, "SetList.json")
ALL_PRINTINGS_PATH = os.path.join(DATA_DOWNLOAD_DIR, "AllPrintings.json")
ALL_PRINTINGS_GZ_PATH = os.path.join(DATA_DOWNLOAD_DIR, "AllPrintings.json.gz")

MTGJSON_ATOMIC_URL = "https://mtgjson.com/api/v5/AtomicCards.json"
MTGJSON_SET_LIST_URL = "https://mtgjson.com/api/v5/SetList.json"
MTGJSON_ALL_PRINTINGS_URL = "https://mtgjson.com/api/v5/AllPrintings.json.gz"
MTGJSON_CSV_BASE_URL = "https://mtgjson.com/api/v5/csv"

MTGJSON_SET_BOOSTER_CONTENTS_URL = f"{MTGJSON_CSV_BASE_URL}/setBoosterContents.csv"
MTGJSON_SET_BOOSTER_CONTENT_WEIGHTS_URL = f"{MTGJSON_CSV_BASE_URL}/setBoosterContentWeights.csv"
MTGJSON_SET_BOOSTER_SHEET_CARDS_URL = f"{MTGJSON_CSV_BASE_URL}/setBoosterSheetCards.csv"
MTGJSON_SET_BOOSTER_SHEETS_URL = f"{MTGJSON_CSV_BASE_URL}/setBoosterSheets.csv"

SCRYFALL_BULK_DATA_URL = "https://api.scryfall.com/bulk-data"

SCRYFALL_DOWNLOAD_DIR = os.path.join(DATA_ROOT_DIR, "scryfall")
IMAGE_CACHE_DIR = os.path.join(DATA_ROOT_DIR, "image_cache")
SCRYFALL_DEFAULT_CARDS_PATH = os.path.join(SCRYFALL_DOWNLOAD_DIR, "default-cards.json")
SET_BOOSTER_CONTENTS_CSV_PATH = os.path.join(DATA_DOWNLOAD_DIR, "setBoosterContents.csv")
SET_BOOSTER_CONTENT_WEIGHTS_CSV_PATH = os.path.join(DATA_DOWNLOAD_DIR, "setBoosterContentWeights.csv")
SET_BOOSTER_SHEET_CARDS_CSV_PATH = os.path.join(DATA_DOWNLOAD_DIR, "setBoosterSheetCards.csv")
SET_BOOSTER_SHEETS_CSV_PATH = os.path.join(DATA_DOWNLOAD_DIR, "setBoosterSheets.csv")

CARD_SEARCH_DEFAULT_TITLE = "Avatar - Momir Vig, Simic Visionary"
CARD_SEARCH_DEFAULT_VARIANTS = {
    "dark": {
        "label": "Dark Token",
        "filename": "img/MomirVig_Token_1.jpg",
    },
    "light": {
        "label": "Light Token",
        "filename": "img/MomirVig_Token_3.jpg",
    },
    "retro": {
        "label": "Retro Token",
        "filename": "img/MomirVig_Token_2.jpg",
    },
    "mtgo": {
        "label": "MTGO Token",
        "filename": "img/MomirVig_Token_4.jpg",
    },
}

CARD_SEARCH_DEFAULT_VARIANT = "dark"

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
    "game_mode": "custom",
    "allow_repeats": "1",
    "print_template": "dk-1234",
    "print_color_mode": "grayscale",
    "use_pdf_print": "1",
    "pdf_width_mm": "57.5",
    "pdf_height_mm": "85.25",
    "pdf_crop_border": "1",
    "momir_default_token_variant": "dark",
    "open_print_in_new_tab": "1",
    "tower_pdf_draw_count": "7",
}

REPEAT_MODE_OPTIONS = [
    ("1", "Repeat"),
    ("0", "No Repeats"),
]

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

PRINT_TEMPLATE_OPTIONS = [
    ("dk-1234", "DK-1234"),
    ("standard", "Standard"),
]

PRINT_COLOR_MODE_OPTIONS = [
    ("grayscale", "Grayscale"),
    ("color", "Full Color"),
    ("monochrome", "Monochrome"),
    ("optimal", "Optimal Print"),
]

GAME_MODE_OPTIONS = [
    {
        "value": "custom",
        "label": "Custom",
        "description": "The Momir Vig Avatar allows each player to start with <strong>24 life</strong>, and grants the following ability: <br><br> &#10006; <i>discard a card: Create a token that’s a copy of a <strong>creature</strong> card with converted mana cost X chosen at random. Activate this ability only any time you could cast a sorcery and only once per turn.</i> <br><br> This mode allows you to choose from all available Card Filters.",
        "image_filename": "img/token_mode_custom.jpg",
    },
    {
        "value": "momir_basic",
        "label": "Momir Basic",
        "description": "The Momir Vig Avatar allows each player to start with <strong>24 life</strong>, and grants the following ability: <br><br> &#10006; <i>discard a card: Create a token that’s a copy of a <strong>creature</strong> card with converted mana cost X chosen at random. Activate this ability only any time you could cast a sorcery and only once per turn.</i> <br><br> This is the standard mode of the Momir varient.",
        "image_filename": "img/token_mode_momir_basic.jpg",
    },
        {
        "value": "momir_select",
        "label": "Momir Select",
        "description": "The Momir Vig Avatar allows each player to start with <strong>24 life</strong>, and grants the following ability: <br><br> &#10006; <i>discard a card: Create a token that’s a copy of a card with converted mana cost X from the <strong>selected card type</strong>. Activate this ability only any time you could cast a sorcery and only once per turn.</i> <br><br> This mode adds a card type selector to the draw screen and only pulls from the chosen enabled type.",
        "image_filename": "img/token_mode_momir_select.jpg",
    },
    {
        "value": "momir_planeswalker",
        "label": "Momir Planeswalker",
        "description": "The Momir Vig Avatar allows each player to start with <strong>24 life</strong>, and grants the following ability: <br><br> &#10006; <i>discard a card: Create a token that’s a copy of a <strong>Creature or Planeswalker</strong> card with converted mana cost X chosen at random. Activate this ability only any time you could cast a sorcery and only once per turn.</i> <br><br> This mode includes both Creatures and Plainswalkers as token types.",
        "image_filename": "img/token_mode_momir_planeswalker.jpg",
    },
    {
        "value": "momir_legends",
        "label": "Momir Legends",
        "description": "The Momir Vig Avatar allows each player to start with <strong>24 life</strong>, and grants the following ability: <br><br> &#10006; <i>discard a card: Create a token that’s a copy of a <strong>Rare or Mythic Legendary Creature</strong> card with converted mana cost X chosen at random. Activate this ability only any time you could cast a sorcery and only once per turn.</i> <br><br> This mode can only grab Creatures that are Rare or Mythic rarity.",
        "image_filename": "img/token_mode_momir_legends.jpg",
    },
    {
        "value": "momir_battleship",
        "label": "Momir Battleship",
        "description": "The Momir Vig Avatar allows each player to start with <strong>24 life</strong>, and grants the following ability: <br><br> &#10006; <i>discard a card: Create a token that’s a copy of a <strong>creature</strong> card with converted mana cost X that is <strong>5 or greater</strong>, chosen at random. Activate this ability only any time you could cast a sorcery and only once per turn.</i> <br><br> This mode only allows cards with a cost of 5 or more to be copied.",
        "image_filename": "img/token_mode_momir_battleship.jpg",
    },
    {
        "value": "momir_aggro",
        "label": "Momir Aggro",
        "description": "The Momir Vig Avatar allows each player to start with <strong>24 life</strong>, and grants the following ability: <br><br> &#10006; <i>discard a card: Create a token that’s a copy of a <strong>creature</strong> card with converted mana cost X that is <strong>4 or less</strong>, chosen at random. Activate this ability only any time you could cast a sorcery and only once per turn.</i> <br><br> This mode only allows cards with a cost of 4 or less to be copied.",
        "image_filename": "img/token_mode_momir_aggro.jpg",
    },
    {
        "value": "momir_odds",
        "label": "Momir Odds",
        "description": "The Momir Vig Avatar allows each player to start with <strong>24 life</strong>, and grants the following ability: <br><br> &#10006; <i>discard a card: Create a token that’s a copy of a <strong>creature</strong> card with converted mana cost X that is <strong>an odd value</strong>, chosen at random. Activate this ability only any time you could cast a sorcery and only once per turn.</i> <br><br> This mode only allows cards with an odd value mana cost to be copied.",
        "image_filename": "img/token_mode_momir_odds.jpg",
    },
    {
        "value": "momir_evens",
        "label": "Momir Evens",
        "description": "The Momir Vig Avatar allows each player to start with <strong>24 life</strong>, and grants the following ability: <br><br> &#10006; <i>discard a card: Create a token that’s a copy of a <strong>creature</strong> card with converted mana cost X that is <strong>an even value</strong>, chosen at random. Activate this ability only any time you could cast a sorcery and only once per turn.</i> <br><br> This mode only allows cards with an even value mana cost to be copied.",
        "image_filename": "img/token_mode_momir_evens.jpg",
    },
    {
        "value": "momir_prime",
        "label": "Momir Prime",
        "description": "The Momir Vig Avatar allows each player to start with <strong>24 life</strong>, and grants the following ability: <br><br> &#10006; <i>discard a card: Create a token that’s a copy of a <strong>creature</strong> card with a converted mana cost of X that is a <strong>Prime Number</strong>, chosen at random. Activate this ability only any time you could cast a sorcery and only once per turn.</i> <br><br> This mode only allows cards with a mana cost that is a Prime Number to be copied.",
        "image_filename": "img/token_mode_momir_prime.jpg",
    },
    {
        "value": "tower_of_power",
        "label": "Tower of Power",
        "description": "Tower of Power is a  mode that simulates drawing from a deck of any card for the selected sets. Click <strong>Draw</strong> to draw a random card from the selected pool using <strong>Sets</strong> and <strong>Primary Card Types</strong>, plus basic and non-basic lands.",
        "image_filename": "img/token_mode_tower_of_power.jpg",
    },
    {
        "value": "chaos_draft",
        "label": "Chaos Draft",
        "description": "Chaos Draft selects a random booster pack from the currently enabled sets. One of the funnest ways to play Magic the Gathering.",
        "image_filename": "img/token_mode_tower_of_power.jpg",
    },
    {
        "value": "planechase",
        "label": "Planechase",
        "description": "The Planechase format uses a shared planar deck. Players sometimes play planes cards that affect the battlefield. You can use this mode to generate Planes by clicking on the 0.",
        "image_filename": "img/token_mode_planechase.jpg",
    },
    {
        "value": "archenemy",
        "label": "Archenemy",
        "description": "You can generate Schemes for Archenemy using this mode.  It is recommended that you turn off Repeats for this mode.",
        "image_filename": "img/token_mode_archenemy.jpg",
    },
]

MOMIR_DEFAULT_TOKEN_VARIANT_OPTIONS = [
    ("dark", "Dark Token"),
    ("light", "Light Token"),
    ("retro", "Retro Token"),
    ("mtgo", "MTGO Token"),
]

# Chaos Draft - Allowed Booster Types (EDIT THIS LIST AS NEEDED)
ALLOWED_CHAOS_BOOSTER_TYPES = {
    "draft",
    "collector",
    "set",
    "play",
    "jumpstart",
    "jumpstart-v2",
    "premium",
    "vip",
    "six",
    "collector-special",
}

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
            is_booster INTEGER NOT NULL DEFAULT 1
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


def update_config_from_form(form_data):
    updated_config = {}

    checkbox_keys = {
        "type_creature",
        "type_artifact",
        "type_enchantment",
        "type_instant",
        "type_land",
        "type_sorcery",
        "type_planeswalker",
        "type_battle",
        "type_conspiracy",
        "type_dungeon",
        "type_emblem",
        "type_phenomenon",
        "type_plane",
        "type_scheme",
        "type_vanguard",
        "allow_legendary",
        "allow_unsets",
        "allow_arena",
        "use_pdf_print",
        "pdf_crop_border",
        "open_print_in_new_tab",
    }

    select_defaults = {
        "game_mode": "custom",
        "allow_repeats": "1",
        "print_template": "dk-1234",
        "print_color_mode": "grayscale",
    }

    for key in checkbox_keys:
        updated_config[key] = "1" if form_data.get(key) == "on" else "0"

    submitted_game_mode = (form_data.get("game_mode") or "").strip().lower()
    if submitted_game_mode not in {
        "custom",
        "momir_basic",
        "momir_select",
        "momir_planeswalker",
        "momir_legends",
        "momir_battleship",
        "momir_aggro",
        "momir_odds",
        "momir_evens",
        "momir_prime",
        "tower_of_power",
        "chaos_draft",
        "planechase",
        "archenemy",
    }:
        submitted_game_mode = select_defaults["game_mode"]
    updated_config["game_mode"] = submitted_game_mode

    submitted_allow_repeats = (form_data.get("allow_repeats") or "").strip()
    if submitted_allow_repeats not in {"0", "1"}:
        submitted_allow_repeats = select_defaults["allow_repeats"]
    updated_config["allow_repeats"] = submitted_allow_repeats

    submitted_template = (form_data.get("print_template") or "").strip().lower()
    if submitted_template not in {"dk-1234", "standard"}:
        submitted_template = select_defaults["print_template"]
    updated_config["print_template"] = submitted_template

    submitted_color_mode = (form_data.get("print_color_mode") or "").strip().lower()
    if submitted_color_mode not in {"grayscale", "color", "monochrome", "optimal"}:
        submitted_color_mode = select_defaults["print_color_mode"]
    updated_config["print_color_mode"] = submitted_color_mode

    submitted_momir_variant = (form_data.get("momir_default_token_variant") or "").strip().lower()
    if submitted_momir_variant not in {"dark", "light", "retro", "mtgo"}:
        submitted_momir_variant = "dark"
    updated_config["momir_default_token_variant"] = submitted_momir_variant

    submitted_pdf_width_mm = (form_data.get("pdf_width_mm") or "").strip()
    try:
        parsed_pdf_width_mm = float(submitted_pdf_width_mm)
        if parsed_pdf_width_mm <= 0:
            raise ValueError()
    except ValueError:
        parsed_pdf_width_mm = 57.5
    updated_config["pdf_width_mm"] = str(parsed_pdf_width_mm)

    submitted_pdf_height_mm = (form_data.get("pdf_height_mm") or "").strip()
    try:
        parsed_pdf_height_mm = float(submitted_pdf_height_mm)
        if parsed_pdf_height_mm <= 0:
            raise ValueError()
    except ValueError:
        parsed_pdf_height_mm = 85.25
    updated_config["pdf_height_mm"] = str(parsed_pdf_height_mm)

    if submitted_game_mode == "tower_of_power":
        any_primary_selected = any(updated_config.get(key) == "1" for key, _ in PRIMARY_TYPE_KEYS)
        if not any_primary_selected:
            for key, _ in PRIMARY_TYPE_KEYS:
                updated_config[key] = "1"

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

def parse_bool_csv(value):
    return str(value or "").strip().upper() == "TRUE"


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

    # normalize common MTGJSON naming patterns
    value = value.replace(" booster", "")
    value = value.replace(" boosters", "")

    return value

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

    booster_label_map = {
        "draft": "Draft Booster",
        "set": "Set Booster",
        "play": "Play Booster",
        "collector": "Collector Booster",
        "jumpstart": "Jumpstart Booster",
        "jumpstart-v2": "Jumpstart Booster",
        "premium": "Premium Booster",
        "vip": "VIP Booster",
        "six": "Six Card Booster",
        "collector-special": "Collector Special Booster",
    }

    display_booster_name = booster_label_map.get(
        clean_booster_name,
        " ".join(word.capitalize() for word in clean_booster_name.split()) if clean_booster_name else "Booster Pack"
    )

    return f"{set_name} - {display_booster_name}"


def get_chaos_pack_art_relpath(set_code, booster_name):
    normalized_set_code = (set_code or "").strip().lower()
    normalized_booster_key = normalize_chaos_booster_key(booster_name)

    direct_relpath = f"img/pack_art/{normalized_set_code}/{normalized_booster_key}.png"
    direct_abspath = os.path.join(app.static_folder, direct_relpath.replace("/", os.sep))

    if os.path.exists(direct_abspath):
        return direct_relpath

    default_relpath = f"img/pack_art/{normalized_set_code}/default.png"
    default_abspath = os.path.join(app.static_folder, default_relpath.replace("/", os.sep))

    if os.path.exists(default_abspath):
        return default_relpath

    return "img/pack_art/_fallback/booster_default.png"


def get_chaos_pack_art_info(set_code, booster_name):
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
    default_image_path = get_chaos_pack_art_relpath(set_code, booster_name)

    if row:
        display_name = (row["display_name"] or "").strip() or default_display_name
        image_path = (row["image_path"] or "").strip() or default_image_path

        static_abs_path = os.path.join(app.static_folder, image_path.replace("/", os.sep))
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

def import_chaos_cards_from_all_printings():
    if not os.path.exists(ALL_PRINTINGS_PATH):
        raise FileNotFoundError("AllPrintings.json was not found after download.")

    set_refresh_status(
        stage="Importing Chaos Draft Cards",
        message="Reading AllPrintings.json for Chaos Draft card printings...",
    )

    with open(ALL_PRINTINGS_PATH, "r", encoding="utf-8") as file_handle:
        raw_json = json.load(file_handle)

    all_sets = raw_json.get("data", {})

    if not raw_json or "data" not in raw_json:
        raise ValueError("AllPrintings.json did not contain a top-level 'data' object.")

    if not isinstance(all_sets, dict) or not all_sets:
        raise ValueError("AllPrintings.json did not contain a valid non-empty 'data' object.")

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM chaos_cards")

    imported_count = 0

    for set_code, set_obj in all_sets.items():
        if not isinstance(set_obj, dict):
            continue

        set_code_clean = (set_code or "").strip().upper()
        cards = set_obj.get("cards", [])

        if not isinstance(cards, list):
            continue

        for card_obj in cards:
            if not isinstance(card_obj, dict):
                continue

            card_uuid = (card_obj.get("uuid") or "").strip()
            card_name = (card_obj.get("name") or "").strip()

            if not card_uuid or not card_name:
                continue

            identifiers = card_obj.get("identifiers") or {}
            if not isinstance(identifiers, dict):
                identifiers = {}

            scryfall_id = (identifiers.get("scryfallId") or "").strip()
            scryfall_illustration_id = (identifiers.get("scryfallIllustrationId") or "").strip()

            image_url = build_scryfall_image_url(scryfall_id) if scryfall_id else None

            is_booster = 1
            if card_obj.get("isPromo") is True:
                is_booster = 0

            cursor.execute(
                """
                INSERT INTO chaos_cards (
                    card_uuid,
                    set_code,
                    card_name,
                    face_name,
                    mana_value,
                    mana_cost,
                    rarity,
                    type_line,
                    layout,
                    collector_number,
                    scryfall_id,
                    scryfall_illustration_id,
                    image_url,
                    image_cache_path,
                    is_booster
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    card_uuid,
                    set_code_clean,
                    card_name,
                    card_obj.get("faceName"),
                    card_obj.get("manaValue"),
                    card_obj.get("manaCost"),
                    (card_obj.get("rarity") or "").strip().lower(),
                    card_obj.get("type") or "",
                    (card_obj.get("layout") or "").strip().lower(),
                    card_obj.get("number"),
                    scryfall_id,
                    scryfall_illustration_id,
                    image_url,
                    None,
                    is_booster,
                ),
            )

            imported_count += 1

            if imported_count % 5000 == 0:
                conn.commit()
                set_refresh_status(
                    stage="Importing Chaos Draft Cards",
                    message=f"Imported {imported_count} Chaos Draft printings...",
                )

    conn.commit()
    conn.close()

    set_import_metadata("chaos_cards_imported", imported_count)

    return imported_count

def import_chaos_booster_data():
    required_files = [
        SET_BOOSTER_CONTENTS_CSV_PATH,
        SET_BOOSTER_CONTENT_WEIGHTS_CSV_PATH,
        SET_BOOSTER_SHEET_CARDS_CSV_PATH,
        SET_BOOSTER_SHEETS_CSV_PATH,
    ]

    for required_file in required_files:
        if not os.path.exists(required_file):
            raise FileNotFoundError(f"Chaos Draft booster file not found: {required_file}")

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM chaos_booster_variants")
    cursor.execute("DELETE FROM chaos_booster_variant_contents")
    cursor.execute("DELETE FROM chaos_booster_sheets")
    cursor.execute("DELETE FROM chaos_booster_sheet_cards")

    with open(SET_BOOSTER_CONTENT_WEIGHTS_CSV_PATH, "r", encoding="utf-8-sig", newline="") as file_handle:
        reader = csv.DictReader(file_handle)
        for row in reader:
            set_code = (row.get("setCode") or "").strip().upper()
            booster_name = (row.get("boosterName") or "").strip().lower()

            if not set_code or not booster_name:
                continue

            try:
                booster_index = int((row.get("boosterIndex") or "0").strip())
            except ValueError:
                booster_index = 0

            try:
                booster_weight = float((row.get("boosterWeight") or "1").strip())
            except ValueError:
                booster_weight = 1.0

            cursor.execute(
                """
                INSERT INTO chaos_booster_variants (
                    set_code,
                    booster_name,
                    booster_index,
                    booster_weight
                )
                VALUES (?, ?, ?, ?)
                """,
                (
                    set_code,
                    booster_name,
                    booster_index,
                    booster_weight,
                ),
            )

    with open(SET_BOOSTER_CONTENTS_CSV_PATH, "r", encoding="utf-8-sig", newline="") as file_handle:
        reader = csv.DictReader(file_handle)
        for row in reader:
            set_code = (row.get("setCode") or "").strip().upper()
            booster_name = (row.get("boosterName") or "").strip().lower()
            sheet_name = (row.get("sheetName") or "").strip()

            if not set_code or not booster_name or not sheet_name:
                continue

            try:
                booster_index = int((row.get("boosterIndex") or "0").strip())
            except ValueError:
                booster_index = 0

            try:
                sheet_picks = int((row.get("sheetPicks") or "1").strip())
            except ValueError:
                sheet_picks = 1

            cursor.execute(
                """
                INSERT INTO chaos_booster_variant_contents (
                    set_code,
                    booster_name,
                    booster_index,
                    sheet_name,
                    sheet_picks
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    set_code,
                    booster_name,
                    booster_index,
                    sheet_name,
                    sheet_picks,
                ),
            )

    with open(SET_BOOSTER_SHEETS_CSV_PATH, "r", encoding="utf-8-sig", newline="") as file_handle:
        reader = csv.DictReader(file_handle)
        for row in reader:
            set_code = (row.get("setCode") or "").strip().upper()
            booster_name = (row.get("boosterName") or "").strip().lower()
            sheet_name = (row.get("sheetName") or "").strip()

            if not set_code or not booster_name or not sheet_name:
                continue

            try:
                sheet_total_weight = float((row.get("sheetTotalWeight") or "0").strip())
            except ValueError:
                sheet_total_weight = 0.0

            sheet_is_foil = 1 if parse_bool_csv(row.get("sheetIsFoil")) else 0
            sheet_has_balance_colors = 1 if parse_bool_csv(row.get("sheetHasBalanceColors")) else 0

            cursor.execute(
                """
                INSERT INTO chaos_booster_sheets (
                    set_code,
                    booster_name,
                    sheet_name,
                    sheet_is_foil,
                    sheet_has_balance_colors,
                    sheet_total_weight
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    set_code,
                    booster_name,
                    sheet_name,
                    sheet_is_foil,
                    sheet_has_balance_colors,
                    sheet_total_weight,
                ),
            )

    with open(SET_BOOSTER_SHEET_CARDS_CSV_PATH, "r", encoding="utf-8-sig", newline="") as file_handle:
        reader = csv.DictReader(file_handle)
        for row in reader:
            set_code = (row.get("setCode") or "").strip().upper()
            booster_name = (row.get("boosterName") or "").strip().lower()
            sheet_name = (row.get("sheetName") or "").strip()
            card_uuid = (row.get("cardUuid") or "").strip()

            if not set_code or not booster_name or not sheet_name or not card_uuid:
                continue

            try:
                card_weight = float((row.get("cardWeight") or "1").strip())
            except ValueError:
                card_weight = 1.0

            cursor.execute(
                """
                INSERT INTO chaos_booster_sheet_cards (
                    set_code,
                    booster_name,
                    sheet_name,
                    card_uuid,
                    card_weight
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    set_code,
                    booster_name,
                    sheet_name,
                    card_uuid,
                    card_weight,
                ),
            )

    conn.commit()
    conn.close()

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

def get_eligible_chaos_packs_for_spin():
    packs = get_eligible_chaos_packs()
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


def open_chaos_pack_once(set_code, booster_name, booster_index):
    variant_contents = get_chaos_booster_variant_contents(set_code, booster_name, booster_index)
    opened_cards = []

    for content_row in variant_contents:
        sheet_name = content_row["sheet_name"]
        sheet_picks = int(content_row["sheet_picks"] or 1)

        sheet_cards = get_chaos_sheet_cards(set_code, booster_name, sheet_name)
        if not sheet_cards:
            continue

        available_cards = list(sheet_cards)

        for _ in range(sheet_picks):
            chosen_card = choose_weighted_row(available_cards, "card_weight")
            if not chosen_card:
                break

            opened_cards.append({
                "set_code": (set_code or "").strip().upper(),
                "booster_name": (booster_name or "").strip().lower(),
                "booster_index": int(booster_index),
                "sheet_name": sheet_name,
                "card_uuid": chosen_card["card_uuid"],
                "card_name": chosen_card["card_name"],
                "rarity": chosen_card["rarity"],
                "type_line": chosen_card["type_line"],
                "image_url": chosen_card["image_url"],
                "scryfall_id": chosen_card["scryfall_id"],
                "collector_number": chosen_card["collector_number"],
            })

    return opened_cards


def open_chaos_pack_with_bonus_rule(set_code, booster_name, booster_index):
    first_pack_cards = open_chaos_pack_once(set_code, booster_name, booster_index)
    all_cards = list(first_pack_cards)

    bonus_pack_opened = False

    if len(first_pack_cards) < 12:
        bonus_pack_opened = True
        second_pack_cards = open_chaos_pack_once(set_code, booster_name, booster_index)
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

def build_chaos_spin_result():
    eligible_packs = get_eligible_chaos_packs_for_spin()

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

def get_eligible_chaos_packs():
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

    for row in rows:
        booster_name_raw = row["booster_name"]
        booster_type = normalize_booster_type_for_filter(booster_name_raw)

        if booster_type not in ALLOWED_CHAOS_BOOSTER_TYPES:
            continue

        art_info = get_chaos_pack_art_info(row["set_code"], booster_name_raw)

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

    if (force_land_draw or random.random() < 0.42) and force_nonland_draw == False :
        land_card = draw_tower_land_card(config, selected_set_codes)
        if land_card:
            return land_card

        fallback_land_card = draw_tower_any_land(config, selected_set_codes)
        if fallback_land_card:
            return fallback_land_card

    return draw_tower_nonland_card(config, selected_set_codes)

def draw_tower_of_power_batch_cards(draw_count):
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
            card = ensure_card_image_cached(card)

        if not card:
            continue

        record_card_history(card["card_key"])
        cards.append(card)

    return cards

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

    # Mana value (exact match for now)
    conditions.append("CAST(mana_value AS INTEGER) = ?")
    params.append(int(mana_value))

    game_mode = (config.get("game_mode") or "custom").strip().lower()

    # Card types
    if game_mode == "momir_select":
        selected_type_info = resolve_selected_result_type(config, selected_type_value)
        if selected_type_info["selected_column"]:
            conditions.append(f"({selected_type_info['selected_column']} = 1)")
    else:
        type_conditions = build_enabled_type_conditions(config, game_mode)

        if type_conditions:
            conditions.append("(" + " OR ".join(type_conditions) + ")")

    # Legendary filter
    if game_mode == "momir_legends":
        conditions.append("is_legendary = 1")
    elif config.get("allow_legendary") == "0":
        conditions.append("is_legendary = 0")

    # Un-set filter
    if config.get("allow_unsets") == "0":
        conditions.append("is_unset = 0")

    # Arena filter
    if config.get("allow_arena") == "0":
        conditions.append("has_paper_printing = 1")

    # Exclude creature cards that have a mana value but no actual mana cost.
    # This applies only to creature results, not to supplemental-only modes like
    # Planechase or Archenemy.
    conditions.append("(is_creature = 0 OR mana_cost IS NOT NULL)")

    # Repeat filter
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

    # Game mode filters
    # Game mode filters
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

def resolve_pdf_print_settings():
    config = get_config()

    use_pdf_print = (config.get("use_pdf_print") or "1").strip() == "1"
    crop_border = (config.get("pdf_crop_border") or "1").strip() == "1"

    try:
        pdf_width_mm = float((config.get("pdf_width_mm") or "57.5").strip())
        if pdf_width_mm <= 0:
            raise ValueError()
    except ValueError:
        pdf_width_mm = 57.5

    try:
        pdf_height_mm = float((config.get("pdf_height_mm") or "85.25").strip())
        if pdf_height_mm <= 0:
            raise ValueError()
    except ValueError:
        pdf_height_mm = 85.25

    return {
        "use_pdf_print": use_pdf_print,
        "pdf_width_mm": pdf_width_mm,
        "pdf_height_mm": pdf_height_mm,
        "pdf_crop_border": crop_border,
    }

def resolve_tower_pdf_draw_count():
    config = get_config()

    raw_value = (config.get("tower_pdf_draw_count") or "7").strip()

    try:
        draw_count = int(raw_value)
    except ValueError:
        draw_count = 7

    if draw_count < 1:
        draw_count = 1
    if draw_count > 100:
        draw_count = 100

    return draw_count

def save_tower_pdf_draw_count(draw_count):
    try:
        parsed_value = int(draw_count)
    except (TypeError, ValueError):
        parsed_value = 7

    if parsed_value < 1:
        parsed_value = 1
    if parsed_value > 100:
        parsed_value = 100

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO app_config (config_key, config_value)
        VALUES (?, ?)
        ON CONFLICT(config_key) DO UPDATE SET config_value = excluded.config_value
        """,
        ("tower_pdf_draw_count", str(parsed_value)),
    )

    conn.commit()
    conn.close()

    return parsed_value

def resolve_print_settings():
    config = get_config()

    print_template = (request.args.get("template") or config.get("print_template") or "dk-1234").strip().lower()
    if print_template not in {"dk-1234", "standard"}:
        print_template = "dk-1234"

    if print_template == "standard":
        print_width = "2.5in"
        print_height = "3.5in"
    else:
        print_width = "63mm"
        print_height = "86mm"

    print_mode = (request.args.get("mode") or config.get("print_color_mode") or "grayscale").strip().lower()
    if print_mode not in {"grayscale", "color", "monochrome", "optimal"}:
        print_mode = "grayscale"

    open_in_new_tab = (config.get("open_print_in_new_tab") or "1").strip() == "1"

    return {
        "print_template": print_template,
        "print_width": print_width,
        "print_height": print_height,
        "print_mode": print_mode,
        "open_in_new_tab": open_in_new_tab,
    }

def resolve_default_momir_variant():
    config = get_config()
    variant = (config.get("momir_default_token_variant") or CARD_SEARCH_DEFAULT_VARIANT).strip().lower()

    if variant not in CARD_SEARCH_DEFAULT_VARIANTS:
        variant = CARD_SEARCH_DEFAULT_VARIANT

    return {
        "key": variant,
        "label": CARD_SEARCH_DEFAULT_VARIANTS[variant]["label"],
        "filename": CARD_SEARCH_DEFAULT_VARIANTS[variant]["filename"],
    }

def get_game_mode_option_map():
    return {item["value"]: item for item in GAME_MODE_OPTIONS}

def get_card_print_href(card_key):
    pdf_settings = resolve_pdf_print_settings()

    if pdf_settings["use_pdf_print"]:
        return url_for("print_card_pdf", card_key=card_key)

    return url_for("print_card", card_key=card_key)

def get_default_token_print_href():
    pdf_settings = resolve_pdf_print_settings()

    if pdf_settings["use_pdf_print"]:
        return url_for("print_custom_default_momir_vig_pdf")

    return url_for("print_custom_default_momir_vig")

def get_game_mode_print_href(mode_value):
    pdf_settings = resolve_pdf_print_settings()

    if pdf_settings["use_pdf_print"]:
        return url_for("print_custom_game_mode_pdf", mode_value=mode_value)

    return url_for("print_custom_game_mode", mode_value=mode_value)

def get_preferred_local_ip():
    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        probe.connect(("8.8.8.8", 80))
        local_ip = probe.getsockname()[0]
        probe.close()

        if local_ip and not local_ip.startswith("127."):
            return local_ip
    except Exception:
        pass

    try:
        hostname_ip = socket.gethostbyname(socket.gethostname())
        if hostname_ip and not hostname_ip.startswith("127."):
            return hostname_ip
    except Exception:
        pass

    return "127.0.0.1"

def build_access_url():
    forwarded_proto = request.headers.get("X-Forwarded-Proto", "").strip()
    if forwarded_proto in {"http", "https"}:
        scheme = forwarded_proto
    else:
        scheme = "https" if request.is_secure else "http"

    server_port = (request.environ.get("SERVER_PORT") or "5000").strip()
    host_header = (request.headers.get("Host") or "").strip().lower()

    if host_header:
        host_only = host_header.split(":", 1)[0].strip()

        if host_only not in {"127.0.0.1", "localhost", "0.0.0.0"}:
            return f"{scheme}://{host_header}"

    local_ip = get_preferred_local_ip()
    return f"{scheme}://{local_ip}:{server_port}"


def build_qr_code_image_url(target_url):
    encoded_target = requests.utils.quote(target_url, safe="")
    return f"https://api.qrserver.com/v1/create-qr-code/?size=320x320&margin=12&data={encoded_target}"

def resolve_game_mode_token_image(mode_value):
    mode_map = get_game_mode_option_map()
    mode_item = mode_map.get((mode_value or "").strip().lower())

    default_variant = resolve_default_momir_variant()
    default_filename = default_variant["filename"]

    if not mode_item:
        return {
            "filename": default_filename,
            "label": "Custom",
        }

    configured_filename = mode_item.get("image_filename") or ""
    static_path = os.path.join(app.static_folder, configured_filename.replace("/", os.sep))

    if configured_filename and os.path.exists(static_path):
        return {
            "filename": configured_filename,
            "label": mode_item["label"],
        }

    return {
        "filename": default_filename,
        "label": mode_item["label"],
    }

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

def search_cards_for_print(search_text, search_scope="any", limit=75):
    normalized_query = (search_text or "").strip().lower()
    if not normalized_query:
        return []

    token_terms = [
        "token",
        "emblem",
        "vanguard",
        "dungeon",
        "bounty",
        "conspiracy",
        "phenomenon",
        "plane",
        "scheme",
        "hero",
    ]

    conn = get_db_connection()
    cursor = conn.cursor()

    sql = """
        SELECT
            card_key,
            name,
            type_line,
            image_cache_path,
            disable_card
        FROM cards
        WHERE LOWER(name) LIKE ?
          AND (disable_card IS NULL OR disable_card = 0)
    """
    params = [f"%{normalized_query}%"]

    if search_scope == "token":
        token_clauses = []
        for term in token_terms:
            token_clauses.append("LOWER(type_line) LIKE ?")
            params.append(f"%{term}%")
        sql += " AND (" + " OR ".join(token_clauses) + ")"

    sql += """
        ORDER BY
            CASE
                WHEN LOWER(name) = ? THEN 0
                WHEN LOWER(name) LIKE ? THEN 1
                ELSE 2
            END,
            name COLLATE NOCASE ASC
        LIMIT ?
    """
    params.extend([normalized_query, f"{normalized_query}%", limit])

    cursor.execute(sql, params)
    rows = cursor.fetchall()
    conn.close()

    return rows

def draw_random_card(mana_value, selected_type_value=None):
    cleanup_card_history()

    config = get_config()
    selected_set_codes = get_selected_set_codes()

    where_clause, params = build_card_filter_query(
        mana_value, config, selected_set_codes, selected_type_value=selected_type_value
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

def get_persisted_image_summary():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT COUNT(*) AS total_processed
        FROM cards
        WHERE image_cached_at IS NOT NULL
        """
    )
    processed_row = cursor.fetchone()

    cursor.execute(
        """
        SELECT COUNT(*) AS total_downloaded
        FROM cards
        WHERE image_cache_path IS NOT NULL
          AND TRIM(image_cache_path) <> ''
        """
    )
    downloaded_row = cursor.fetchone()

    cursor.execute(
        """
        SELECT COUNT(*) AS total_disabled
        FROM cards
        WHERE disable_card = 1
        """
    )
    disabled_row = cursor.fetchone()

    cursor.execute(
        """
        SELECT MAX(image_cached_at) AS finished_at
        FROM cards
        WHERE image_cached_at IS NOT NULL
        """
    )
    finished_row = cursor.fetchone()

    conn.close()

    return {
        "cards_processed": int(processed_row["total_processed"] or 0),
        "cards_downloaded": int(downloaded_row["total_downloaded"] or 0),
        "cards_disabled": int(disabled_row["total_disabled"] or 0),
        "finished_at": finished_row["finished_at"] or "Never",
    }

def build_config_page_refresh_status(import_metadata):
    current_refresh_status = get_refresh_status_copy()

    if current_refresh_status.get("is_running"):
        return current_refresh_status

    cards_imported = import_metadata.get("cards_imported", "0")
    sets_represented = import_metadata.get("sets_represented", "0")
    finished_at = import_metadata.get("last_refresh_utc")
    source_last_updated = import_metadata.get("source_last_updated", "")

    if finished_at and is_card_database_ready():
        return {
            **current_refresh_status,
            "stage": "Idle - Setup Complete",
            "message": f"Last loaded database contains {cards_imported} cards across {sets_represented} sets.",
            "cards_processed": int(cards_imported or 0),
            "cards_imported": int(cards_imported or 0),
            "sets_represented": int(sets_represented or 0),
            "finished_at": finished_at,
            "source_last_updated": source_last_updated,
            "error": "",
        }

    return current_refresh_status

def build_config_page_image_status():
    current_image_status = get_image_download_status_copy()

    if current_image_status.get("is_running"):
        return current_image_status

    persisted_summary = get_persisted_image_summary()

    if persisted_summary["cards_processed"] > 0 or persisted_summary["cards_downloaded"] > 0:
        return {
            **current_image_status,
            "stage": "Idle",
            "message": (
                f"Cached image data loaded from database. "
                f"Processed {persisted_summary['cards_processed']} cards, "
                f"downloaded {persisted_summary['cards_downloaded']} images."
            ),
            "cards_processed": persisted_summary["cards_processed"],
            "cards_downloaded": persisted_summary["cards_downloaded"],
            "cards_disabled": persisted_summary["cards_disabled"],
            "finished_at": persisted_summary["finished_at"],
            "error": "",
        }

    return current_image_status

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
        stage="Downloading - DO NOT LEAVE PAGE",
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

def download_set_list_json(force_download=False):
    ensure_download_directories()

    set_refresh_status(
        stage="Downloading Set List",
        message="Downloading SetList.json from MTGJSON...",
    )

    headers = {
        "User-Agent": "iMomir/1.0",
        "Accept": "application/json;q=0.9,*/*;q=0.8",
    }

    response = requests.get(
        MTGJSON_SET_LIST_URL,
        headers=headers,
        timeout=600,
    )
    response.raise_for_status()

    with open(SET_LIST_PATH, "wb") as file_handle:
        file_handle.write(response.content)

    return {
        "downloaded": True,
        "message": "Downloaded SetList.json successfully.",
    }

def download_all_printings_json(force_download=False):
    ensure_download_directories()

    if (
        not force_download
        and os.path.exists(ALL_PRINTINGS_PATH)
        and os.path.getsize(ALL_PRINTINGS_PATH) > 0
    ):
        return {
            "downloaded": False,
            "message": "Local AllPrintings.json already exists.",
        }

    set_refresh_status(
        stage="Downloading All Printings",
        message="Downloading AllPrintings.json.gz from MTGJSON...",
    )

    headers = {
        "User-Agent": "iMomir/1.0",
        "Accept": "application/gzip,application/octet-stream;q=0.9,*/*;q=0.8",
    }

    print("=== DOWNLOADING ALLPRINTINGS FROM ===", MTGJSON_ALL_PRINTINGS_URL)
    print("=== WRITING GZ TO ===", ALL_PRINTINGS_GZ_PATH)

    with requests.get(
        MTGJSON_ALL_PRINTINGS_URL,
        headers=headers,
        timeout=1200,
        stream=True,
    ) as response:
        response.raise_for_status()
        print("=== ALLPRINTINGS HTTP STATUS ===", response.status_code)
        print("=== ALLPRINTINGS CONTENT-TYPE ===", response.headers.get("Content-Type"))
        print("=== ALLPRINTINGS CONTENT-LENGTH ===", response.headers.get("Content-Length"))

        with open(ALL_PRINTINGS_GZ_PATH, "wb") as file_handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file_handle.write(chunk)

    print("=== GZ EXISTS AFTER DOWNLOAD ===", os.path.exists(ALL_PRINTINGS_GZ_PATH))
    if os.path.exists(ALL_PRINTINGS_GZ_PATH):
        print("=== GZ SIZE ===", os.path.getsize(ALL_PRINTINGS_GZ_PATH))

    set_refresh_status(
        stage="Extracting All Printings",
        message="Extracting AllPrintings.json from AllPrintings.json.gz...",
    )

    print("=== EXTRACTING GZ TO ===", ALL_PRINTINGS_PATH)

    with gzip.open(ALL_PRINTINGS_GZ_PATH, "rb") as compressed_file:
        with open(ALL_PRINTINGS_PATH, "wb") as output_file:
            while True:
                chunk = compressed_file.read(1024 * 1024)
                if not chunk:
                    break
                output_file.write(chunk)

    print("=== JSON EXISTS AFTER EXTRACT ===", os.path.exists(ALL_PRINTINGS_PATH))
    if os.path.exists(ALL_PRINTINGS_PATH):
        print("=== JSON SIZE ===", os.path.getsize(ALL_PRINTINGS_PATH))

    return {
        "downloaded": True,
        "message": "Downloaded and extracted AllPrintings.json successfully.",
    }

def download_chaos_booster_csvs(force_download=False):
    ensure_download_directories()

    files_to_download = [
        (MTGJSON_SET_BOOSTER_CONTENTS_URL, SET_BOOSTER_CONTENTS_CSV_PATH),
        (MTGJSON_SET_BOOSTER_CONTENT_WEIGHTS_URL, SET_BOOSTER_CONTENT_WEIGHTS_CSV_PATH),
        (MTGJSON_SET_BOOSTER_SHEET_CARDS_URL, SET_BOOSTER_SHEET_CARDS_CSV_PATH),
        (MTGJSON_SET_BOOSTER_SHEETS_URL, SET_BOOSTER_SHEETS_CSV_PATH),
    ]

    headers = {
        "User-Agent": "iMomir/1.0",
        "Accept": "text/csv,application/octet-stream;q=0.9,*/*;q=0.8",
    }

    for url, file_path in files_to_download:
        if not force_download and os.path.exists(file_path):
            continue

        set_refresh_status(
            stage="Downloading Chaos Draft Data",
            message=f"Downloading {os.path.basename(file_path)}...",
        )

        response = requests.get(url, headers=headers, timeout=180)
        response.raise_for_status()

        with open(file_path, "wb") as file_handle:
            file_handle.write(response.content)

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

def build_pdf_image_reader(image_path, print_mode):
    with Image.open(image_path) as source_image:
        image = source_image.convert("RGB")

        if print_mode == "grayscale":
            image = ImageOps.grayscale(image)
            image = ImageEnhance.Contrast(image).enhance(1.08)
            image = ImageEnhance.Brightness(image).enhance(1.02)
            image = image.convert("RGB")

        elif print_mode == "monochrome":
            image = ImageOps.grayscale(image)
            image = ImageEnhance.Contrast(image).enhance(2.35)
            image = ImageEnhance.Brightness(image).enhance(1.05)
            image = image.point(lambda p: 255 if p >= 160 else 0, mode="1")
            image = image.convert("RGB")

        elif print_mode == "optimal":
            # Slight global contrast boost
            image = ImageEnhance.Contrast(image).enhance(1.25)

            # Slight brightness lift
            image = ImageEnhance.Brightness(image).enhance(1.07)

            # Push light tones toward white without killing color
            def highlight_boost(p):
                if p > 200:
                    return min(255, int(p + (255 - p) * 0.7))  # push highlights toward white
                return p

            image = image.point(highlight_boost)

            # Slight midtone contrast curve
            def contrast_curve(p):
                return int((p - 128) * 1.1 + 128)

            image = image.point(contrast_curve)
        
        

        image_buffer = BytesIO()
        image.save(image_buffer, format="PNG")
        image_buffer.seek(0)

        return ImageReader(image_buffer)

def split_chaos_pack_display_name_for_title(pack_display_name):
    raw_value = (pack_display_name or "").strip()

    if not raw_value:
        return ("Booster Pack", "")

    if " - " in raw_value:
        left_part, right_part = raw_value.split(" - ", 1)
        set_name = (left_part or "").strip()
        booster_name = (right_part or "").strip()

        if booster_name:
            booster_name = " ".join(word.capitalize() for word in booster_name.split())

        return (set_name or "Booster Pack", booster_name)

    return (raw_value, "")

def build_chaos_pack_pdf(cards, pack_display_name):
    pdf_settings = resolve_pdf_print_settings()
    width_mm = pdf_settings["pdf_width_mm"]
    height_mm = pdf_settings["pdf_height_mm"]
    crop_border = pdf_settings["pdf_crop_border"]

    print_settings = resolve_print_settings()

    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=(width_mm * mm, height_mm * mm))

    # Title card page
    page_width_pts = width_mm * mm
    page_height_pts = height_mm * mm

    title_set_name, title_booster_name = split_chaos_pack_display_name_for_title(pack_display_name)

    side_padding_pts = 9 * mm
    usable_width_pts = page_width_pts - (side_padding_pts * 2)

    c.setFillColorRGB(1, 1, 1)
    c.rect(0, 0, page_width_pts, page_height_pts, fill=1, stroke=0)

    c.setFillColorRGB(0, 0, 0)

    current_y = (page_height_pts / 2) + 8

    # --- DRAW TITLE (SET NAME) ---
    if title_set_name:
        font_name = "Helvetica-Bold"
        font_size = 16
        line_spacing = 16  # tighter than before

        wrapped_lines = simpleSplit(title_set_name, font_name, font_size, usable_width_pts)

        for line in wrapped_lines:
            text_width = c.stringWidth(line, font_name, font_size)
            x_position = (page_width_pts - text_width) / 2

            c.setFont(font_name, font_size)
            c.drawString(x_position, current_y, line)

            current_y -= line_spacing

    # --- SMALL GAP BETWEEN TITLE AND SUBTITLE (reduced) ---
    current_y -= 4  # was larger before → now tighter

    # --- DRAW SUBTITLE (BOOSTER TYPE) ---
    if title_booster_name:
        font_name = "Helvetica-Bold"
        font_size = 13
        line_spacing = 14

        wrapped_lines = simpleSplit(title_booster_name, font_name, font_size, usable_width_pts)

        for line in wrapped_lines:
            text_width = c.stringWidth(line, font_name, font_size)
            x_position = (page_width_pts - text_width) / 2

            c.setFont(font_name, font_size)
            c.drawString(x_position, current_y, line)

            current_y -= line_spacing
    c.showPage()

    draw_x_mm = 0.0
    draw_y_mm = 0.0
    draw_width_mm = width_mm
    draw_height_mm = height_mm

    if crop_border:
        crop_left_right_mm = width_mm * 0.05
        crop_top_bottom_mm = height_mm * 0.034

        draw_x_mm = -crop_left_right_mm
        draw_y_mm = -crop_top_bottom_mm
        draw_width_mm = width_mm + (crop_left_right_mm * 2)
        draw_height_mm = height_mm + (crop_top_bottom_mm * 2)

    pages_rendered = 0

    for card in cards:
        card_uuid = card.get("card_uuid")

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT *
            FROM chaos_cards
            WHERE card_uuid = ?
            """,
            (card_uuid,),
        )

        card_row = cursor.fetchone()
        conn.close()

        if not card_row:
            continue

        existing_cache_path = card_row["image_cache_path"] or ""
        cache_exists = False

        if existing_cache_path:
            abs_path = os.path.abspath(existing_cache_path)
            cache_exists = os.path.exists(abs_path)

        if cache_exists:
            absolute_image_path = os.path.abspath(existing_cache_path)

            pdf_image_reader = build_pdf_image_reader(
                absolute_image_path,
                print_settings["print_mode"]
            )
        else:
            image_url = (card_row["image_url"] or "").strip()
            if not image_url:
                continue

            try:
                response = requests.get(image_url, timeout=60)
                response.raise_for_status()

                image_buffer = BytesIO(response.content)
                pdf_image_reader = ImageReader(image_buffer)
            except Exception:
                continue

        c.drawImage(
            pdf_image_reader,
            draw_x_mm * mm,
            draw_y_mm * mm,
            width=draw_width_mm * mm,
            height=draw_height_mm * mm,
            preserveAspectRatio=False,
            mask="auto",
        )
        c.showPage()
        pages_rendered += 1

    if pages_rendered == 0:
        raise ValueError("No Chaos Draft card images could be rendered into the PDF.")

    c.save()
    buffer.seek(0)

    return buffer

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

    seen_scryfall_ids = set()
    duplicate_scryfall_ids = 0

    for card_obj in raw_json:
        if not isinstance(card_obj, dict):
            continue

        # Skip multi-face cards (we only want clean single-face printings)
        if card_obj.get("card_faces"):
            continue

        image_uris = safe_dict(card_obj.get("image_uris"))
        games = json.dumps(card_obj.get("games", []))
        rarity = (card_obj.get("rarity") or "").strip().lower()
        lang = (card_obj.get("lang") or "").strip().lower()

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

        if scryfall_id in seen_scryfall_ids:
            duplicate_scryfall_ids += 1
        else:
            seen_scryfall_ids.add(scryfall_id)

        cursor.execute(
            """
            INSERT OR REPLACE INTO scryfall_default_cards (
                scryfall_id,
                oracle_id,
                card_name,
                set_code,
                collector_number,
                released_at,
                image_url,
                normal_image_url,
                large_image_url,
                rarity,
                games,
                lang
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                rarity,
                games,
                lang,
            ),
        )

        inserted_count += 1

        if inserted_count % 2000 == 0:
            conn.commit()

    conn.commit()
    conn.close()

    refresh_cards_has_paper_printing()
    refresh_cards_rarity_from_scryfall()

    print("=== SCRYFALL IMPORT COMPLETE ===")
    print("Inserted rows:", inserted_count)
    print("Duplicate scryfall_ids seen during import:", duplicate_scryfall_ids)

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

def refresh_cards_rarity_from_scryfall():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE cards
        SET rarity = ''
        """
    )

    cursor.execute(
        """
        UPDATE cards
        SET rarity = COALESCE((
            SELECT
                CASE
                    WHEN SUM(CASE WHEN LOWER(COALESCE(sdc.rarity, '')) IN ('mythic', 'mythic rare') THEN 1 ELSE 0 END) > 0 THEN 'mythic'
                    WHEN SUM(CASE WHEN LOWER(COALESCE(sdc.rarity, '')) = 'rare' THEN 1 ELSE 0 END) > 0 THEN 'rare'
                    WHEN SUM(CASE WHEN LOWER(COALESCE(sdc.rarity, '')) = 'uncommon' THEN 1 ELSE 0 END) > 0 THEN 'uncommon'
                    WHEN SUM(CASE WHEN LOWER(COALESCE(sdc.rarity, '')) = 'common' THEN 1 ELSE 0 END) > 0 THEN 'common'
                    ELSE ''
                END
            FROM scryfall_default_cards sdc
            WHERE sdc.oracle_id = cards.scryfall_id
        ), '')
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
              AND LOWER(COALESCE(lang, '')) = 'en'
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
              AND LOWER(COALESCE(lang, '')) = 'en'
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
              AND LOWER(COALESCE(lang, '')) = 'en'
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

def import_set_list_into_database():
    if not os.path.exists(SET_LIST_PATH):
        raise FileNotFoundError("SetList.json was not found after download.")

    set_refresh_status(stage="Parsing Set List", message="Reading SetList.json...")

    with open(SET_LIST_PATH, "r", encoding="utf-8") as file_handle:
        raw_json = json.load(file_handle)

    set_list = raw_json.get("data", [])
    if not isinstance(set_list, list):
        raise ValueError("SetList.json did not contain a valid 'data' list.")

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM sets")

    inserted_count = 0

    for set_obj in set_list:
        if not isinstance(set_obj, dict):
            continue

        set_code = (set_obj.get("code") or "").strip().upper()
        set_name = (set_obj.get("name") or "").strip()
        release_date = (set_obj.get("releaseDate") or "").strip()
        set_block = (set_obj.get("block") or "").strip()
        set_type = (set_obj.get("type") or "").strip()

        if not set_code or not set_name:
            continue

        cursor.execute(
            """
            INSERT INTO sets (
                set_code,
                set_name,
                release_date,
                set_block,
                set_type
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                set_code,
                set_name,
                release_date or None,
                set_block or None,
                set_type or None,
            ),
        )

        inserted_count += 1

        if inserted_count % 500 == 0:
            conn.commit()

    conn.commit()
    conn.close()

    set_import_metadata("sets_imported", inserted_count)

    return inserted_count


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
        layout = (atomic_card.get("layout") or "").strip().lower()

        types = safe_list(atomic_card.get("types"))
        supertypes = safe_list(atomic_card.get("supertypes"))
        printings = safe_list(atomic_card.get("printings"))

        if layout == "reversible_card":
            continue

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
        rarity = ""

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
                rarity,
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                card_key,
                atomic_card.get("name") or card_key,
                atomic_card.get("faceName"),
                atomic_card.get("manaValue"),
                atomic_card.get("manaCost"),
                atomic_card.get("type") or "",
                layout,
                first_printing,
                json.dumps(printings),
                scryfall_oracle_id or "",
                image_url,
                rarity,
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
                stage="Importing - DO NOT LEAVE PAGE",
                message=f"Imported {imported_count} of {total_cards} atomic cards...",
                cards_processed=index,
                cards_imported=imported_count,
                sets_represented=len(represented_sets),
            )

    conn.commit()
    conn.close()

    refresh_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

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
            stage="Downloading Set List",
            message="Downloading SetList.json from MTGJSON...",
        )
        download_set_list_json(force_download=True)

        set_refresh_status(
            stage="Importing Sets - DO NOT LEAVE PAGE",
            message="Importing set metadata into SQLite...",
        )
        imported_sets = import_set_list_into_database()

        set_refresh_status(
            stage="Importing Cards - DO NOT LEAVE PAGE",
            message=f"{download_result['reason']} Beginning import from local AtomicCards.json...",
            sets_represented=imported_sets,
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

        print("=== CHAOS DRAFT: BEGIN ALLPRINTINGS DOWNLOAD ===")

        set_refresh_status(
            stage="Downloading Chaos Draft Card Data",
            message="Downloading AllPrintings.json for Chaos Draft...",
            cards_processed=summary["cards_imported"],
            cards_imported=summary["cards_imported"],
            sets_represented=summary["sets_represented"],
        )

        all_printings_result = download_all_printings_json(force_download=True)
        print("=== CHAOS DRAFT: DOWNLOAD RESULT ===", all_printings_result)
        print("=== CHAOS DRAFT: ALL_PRINTINGS_PATH EXISTS ===", os.path.exists(ALL_PRINTINGS_PATH))
        if os.path.exists(ALL_PRINTINGS_PATH):
            print("=== CHAOS DRAFT: ALL_PRINTINGS_PATH SIZE ===", os.path.getsize(ALL_PRINTINGS_PATH))

        set_refresh_status(
            stage="Importing Chaos Draft Cards",
            message="Importing AllPrintings.json into Chaos Draft card tables...",
            cards_processed=summary["cards_imported"],
            cards_imported=summary["cards_imported"],
            sets_represented=summary["sets_represented"],
        )

        chaos_cards_imported = import_chaos_cards_from_all_printings()
        print("=== CHAOS DRAFT: IMPORTED CHAOS CARDS ===", chaos_cards_imported)

        set_refresh_status(
            stage="Importing Chaos Draft Booster Data",
            message="Importing MTGJSON Chaos Draft booster CSV data...",
            cards_processed=summary["cards_imported"],
            cards_imported=summary["cards_imported"],
            sets_represented=summary["sets_represented"],
        )

        download_chaos_booster_csvs(force_download=True)
        import_chaos_booster_data()

        refresh_finished_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        set_import_metadata("last_refresh_utc", refresh_finished_at)
        set_import_metadata("source_url", MTGJSON_ATOMIC_URL)
        set_import_metadata("cards_imported", summary["cards_imported"])
        set_import_metadata("sets_represented", summary["sets_represented"])
        set_import_metadata("chaos_cards_imported", chaos_cards_imported)
        set_import_metadata("chaos_booster_data_imported_at", refresh_finished_at)

        if download_result.get("remote_timestamp"):
            set_import_metadata("source_last_updated", download_result["remote_timestamp"])

        set_refresh_status(
            is_running=False,
            stage="Complete",
            message=(
                f"Refresh complete. Imported {summary['cards_imported']} atomic cards across "
                f"{summary['sets_represented']} sets. Paper-printing index and Chaos Draft booster data are ready."
            ),
            finished_at=refresh_finished_at,
            cards_processed=summary["cards_imported"],
            cards_imported=summary["cards_imported"],
            sets_represented=summary["sets_represented"],
            source_last_updated=download_result.get("remote_timestamp", ""),
        )
    except Exception as exc:
        print("=== REFRESH FAILED ===", repr(exc))
        set_refresh_status(
            is_running=False,
            stage="Failed",
            message="Refresh failed.",
            finished_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            error=str(exc),
        )


@app.route("/")
def index():
    config = get_config()
    selected_type_info = resolve_selected_result_type(
        config,
        request.args.get("selected_type", ""),
    )

    pdf_settings = resolve_pdf_print_settings()

    return render_template(
        "index.html",
        card_database_ready=is_card_database_ready(),
        current_game_mode=(config.get("game_mode") or "custom").strip().lower(),
        enabled_type_options=selected_type_info["enabled_types"],
        selected_type_value=selected_type_info["selected_value"],
        use_pdf_print=pdf_settings["use_pdf_print"],
        tower_pdf_draw_count=resolve_tower_pdf_draw_count(),
    )

@app.route("/result")
def result():
    mana_value = request.args.get("mana_value", "").strip()
    selected_type_value = (request.args.get("selected_type") or "").strip().lower()
    card_database_ready = is_card_database_ready()
    config = get_config()
    current_game_mode = (config.get("game_mode") or "custom").strip().lower()
    selected_type_info = resolve_selected_result_type(config, selected_type_value)

    if current_game_mode == "chaos_draft":
        clear_chaos_session_state("pending_spin_result")

        return render_template(
            "chaos_draft.html",
            card_database_ready=card_database_ready,
            current_game_mode=current_game_mode,
        )

    if current_game_mode == "tower_of_power":
        card = draw_random_tower_of_power_card() if card_database_ready else None

        if card:
            existing_cache_path = card["image_cache_path"] or ""
            cache_exists = False

            if existing_cache_path:
                cache_exists = os.path.exists(os.path.abspath(existing_cache_path))

            if not cache_exists:
                card = ensure_card_image_cached(card)

            if card:
                record_card_history(card["card_key"])

        return render_template(
            "result.html",
            mana_value="",
            card=card,
            card_database_ready=card_database_ready,
            current_game_mode=current_game_mode,
            enabled_type_options=[],
            selected_type_value="",
            card_print_href=get_card_print_href(card["card_key"]) if card else "",
            open_print_in_new_tab=resolve_print_settings()["open_in_new_tab"],
        )

    if not mana_value.isdigit():
        return render_template(
            "result.html",
            mana_value=mana_value,
            card=None,
            card_database_ready=card_database_ready,
            current_game_mode=current_game_mode,
            enabled_type_options=selected_type_info["enabled_types"],
            selected_type_value=selected_type_info["selected_value"],
            card_print_href="",
            open_print_in_new_tab=resolve_print_settings()["open_in_new_tab"],
        )

    draw_type_value = selected_type_info["selected_value"] if current_game_mode == "momir_select" else None
    card = draw_random_card(int(mana_value), selected_type_value=draw_type_value)

    if card:
        existing_cache_path = card["image_cache_path"] or ""
        cache_exists = False

        if existing_cache_path:
            cache_exists = os.path.exists(os.path.abspath(existing_cache_path))

        if not cache_exists:
            card = ensure_card_image_cached(card)

        if card:
            record_card_history(card["card_key"])

    return render_template(
        "result.html",
        mana_value=mana_value,
        card=card,
        card_database_ready=card_database_ready,
        current_game_mode=current_game_mode,
        enabled_type_options=selected_type_info["enabled_types"],
        selected_type_value=selected_type_info["selected_value"],
        card_print_href=get_card_print_href(card["card_key"]) if card else "",
        open_print_in_new_tab=resolve_print_settings()["open_in_new_tab"],
    )

@app.route("/print/<card_key>")
def print_card(card_key):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM cards WHERE card_key = ?", (card_key,))
    card = cursor.fetchone()

    conn.close()

    if not card:
        return "Card not found", 404

    print_settings = resolve_print_settings()

    return render_template(
        "print.html",
        card=card,
        image_src=url_for("card_image", card_key=card["card_key"]),
        print_mode=print_settings["print_mode"],
        print_template=print_settings["print_template"],
        print_width=print_settings["print_width"],
        print_height=print_settings["print_height"],
    )

@app.route("/print-pdf/<card_key>")
def print_card_pdf(card_key):
    card = get_card_by_key(card_key)

    if not card:
        return "Card not found", 404

    existing_cache_path = card["image_cache_path"] or ""
    if not existing_cache_path or not os.path.exists(existing_cache_path):
        card = ensure_card_image_cached(card)

    image_path = card["image_cache_path"]
    if not image_path or not os.path.exists(image_path):
        return "Image not available", 404

    pdf_settings = resolve_pdf_print_settings()
    width_mm = pdf_settings["pdf_width_mm"]
    height_mm = pdf_settings["pdf_height_mm"]
    crop_border = pdf_settings["pdf_crop_border"]

    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=(width_mm * mm, height_mm * mm))

    draw_x_mm = 0.0
    draw_y_mm = 0.0
    draw_width_mm = width_mm
    draw_height_mm = height_mm

    if crop_border:
        crop_left_right_mm = width_mm * 0.05
        crop_top_bottom_mm = height_mm * 0.034

        draw_x_mm = -crop_left_right_mm
        draw_y_mm = -crop_top_bottom_mm
        draw_width_mm = width_mm + (crop_left_right_mm * 2)
        draw_height_mm = height_mm + (crop_top_bottom_mm * 2)

    print_settings = resolve_print_settings()
    pdf_image_reader = build_pdf_image_reader(image_path, print_settings["print_mode"])

    c.drawImage(
        pdf_image_reader,
        draw_x_mm * mm,
        draw_y_mm * mm,
        width=draw_width_mm * mm,
        height=draw_height_mm * mm,
        preserveAspectRatio=False,
        mask="auto",
    )

    c.showPage()
    c.save()

    buffer.seek(0)

    pdf_filename_base = (card["scryfall_id"] or "").strip()
    if not pdf_filename_base:
        pdf_filename_base = safe_filename(card["card_key"])

    return Response(
        buffer,
        mimetype="application/pdf",
        headers={
            "Content-Disposition": f"inline; filename={pdf_filename_base}.pdf"
        }
    )

@app.route("/print-pdf/tower-of-power-batch")
def print_tower_of_power_batch_pdf():
    if not is_card_database_ready():
        return "Card database not ready", 400

    pdf_settings = resolve_pdf_print_settings()
    if not pdf_settings["use_pdf_print"]:
        return "PDF printing is disabled", 400

    requested_draw_count = request.args.get("draw_count", "").strip()
    draw_count = save_tower_pdf_draw_count(requested_draw_count)

    cards = draw_tower_of_power_batch_cards(draw_count)
    if not cards:
        return "No matching cards found", 404

    width_mm = pdf_settings["pdf_width_mm"]
    height_mm = pdf_settings["pdf_height_mm"]
    crop_border = pdf_settings["pdf_crop_border"]

    draw_x_mm = 0.0
    draw_y_mm = 0.0
    draw_width_mm = width_mm
    draw_height_mm = height_mm

    if crop_border:
        crop_left_right_mm = width_mm * 0.05
        crop_top_bottom_mm = height_mm * 0.034

        draw_x_mm = -crop_left_right_mm
        draw_y_mm = -crop_top_bottom_mm
        draw_width_mm = width_mm + (crop_left_right_mm * 2)
        draw_height_mm = height_mm + (crop_top_bottom_mm * 2)

    print_settings = resolve_print_settings()

    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=(width_mm * mm, height_mm * mm))

    for card in cards:
        image_path = card["image_cache_path"] or ""
        absolute_image_path = os.path.abspath(image_path) if image_path else ""

        if not absolute_image_path or not os.path.exists(absolute_image_path):
            continue

        pdf_image_reader = build_pdf_image_reader(absolute_image_path, print_settings["print_mode"])

        c.drawImage(
            pdf_image_reader,
            draw_x_mm * mm,
            draw_y_mm * mm,
            width=draw_width_mm * mm,
            height=draw_height_mm * mm,
            preserveAspectRatio=False,
            mask="auto",
        )
        c.showPage()

    c.save()
    buffer.seek(0)

    return Response(
        buffer,
        mimetype="application/pdf",
        headers={
            "Content-Disposition": f"inline; filename=tower_of_power_{len(cards)}_cards.pdf"
        }
    )

@app.route("/print-custom/default-momir-vig")
def print_custom_default_momir_vig():
    print_settings = resolve_print_settings()
    selected_variant = resolve_default_momir_variant()

    return render_template(
        "print.html",
        card={
            "name": CARD_SEARCH_DEFAULT_TITLE,
            "type_line": f"Avatar • {selected_variant['label']}",
        },
        image_src=url_for("static", filename=selected_variant["filename"]),
        print_mode=print_settings["print_mode"],
        print_template=print_settings["print_template"],
        print_width=print_settings["print_width"],
        print_height=print_settings["print_height"],
    )

@app.route("/print-pdf-custom/default-momir-vig")
def print_custom_default_momir_vig_pdf():
    selected_variant = resolve_default_momir_variant()
    image_path = os.path.join(app.static_folder, selected_variant["filename"].replace("/", os.sep))

    if not os.path.exists(image_path):
        return "Image not available", 404

    pdf_settings = resolve_pdf_print_settings()
    width_mm = pdf_settings["pdf_width_mm"]
    height_mm = pdf_settings["pdf_height_mm"]
    crop_border = pdf_settings["pdf_crop_border"]

    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=(width_mm * mm, height_mm * mm))

    draw_x_mm = 0.0
    draw_y_mm = 0.0
    draw_width_mm = width_mm
    draw_height_mm = height_mm

    if crop_border:
        crop_left_right_mm = width_mm * 0.05
        crop_top_bottom_mm = height_mm * 0.034

        draw_x_mm = -crop_left_right_mm
        draw_y_mm = -crop_top_bottom_mm
        draw_width_mm = width_mm + (crop_left_right_mm * 2)
        draw_height_mm = height_mm + (crop_top_bottom_mm * 2)

    print_settings = resolve_print_settings()
    pdf_image_reader = build_pdf_image_reader(image_path, print_settings["print_mode"])

    c.drawImage(
        pdf_image_reader,
        draw_x_mm * mm,
        draw_y_mm * mm,
        width=draw_width_mm * mm,
        height=draw_height_mm * mm,
        preserveAspectRatio=False,
        mask="auto",
    )

    c.showPage()
    c.save()

    buffer.seek(0)

    return Response(
        buffer,
        mimetype="application/pdf",
        headers={
            "Content-Disposition": "inline; filename=default_momir_vig.pdf"
        }
    )

@app.route("/print-custom/game-mode/<mode_value>")
def print_custom_game_mode(mode_value):
    print_settings = resolve_print_settings()
    mode_map = get_game_mode_option_map()
    mode_item = mode_map.get((mode_value or "").strip().lower())

    if not mode_item:
        return "Game mode token not found", 404

    token_image = resolve_game_mode_token_image(mode_value)

    return render_template(
        "print.html",
        card={
            "name": mode_item["label"],
            "type_line": "Avatar • Game Mode Token",
        },
        image_src=url_for("static", filename=token_image["filename"]),
        print_mode=print_settings["print_mode"],
        print_template=print_settings["print_template"],
        print_width=print_settings["print_width"],
        print_height=print_settings["print_height"],
    )

@app.route("/print-pdf-custom/game-mode/<mode_value>")
def print_custom_game_mode_pdf(mode_value):
    mode_map = get_game_mode_option_map()
    mode_item = mode_map.get((mode_value or "").strip().lower())

    if not mode_item:
        return "Game mode token not found", 404

    token_image = resolve_game_mode_token_image(mode_value)
    image_path = os.path.join(app.static_folder, token_image["filename"].replace("/", os.sep))

    if not os.path.exists(image_path):
        return "Image not available", 404

    pdf_settings = resolve_pdf_print_settings()
    width_mm = pdf_settings["pdf_width_mm"]
    height_mm = pdf_settings["pdf_height_mm"]
    crop_border = pdf_settings["pdf_crop_border"]

    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=(width_mm * mm, height_mm * mm))

    draw_x_mm = 0.0
    draw_y_mm = 0.0
    draw_width_mm = width_mm
    draw_height_mm = height_mm

    if crop_border:
        crop_left_right_mm = width_mm * 0.05
        crop_top_bottom_mm = height_mm * 0.034

        draw_x_mm = -crop_left_right_mm
        draw_y_mm = -crop_top_bottom_mm
        draw_width_mm = width_mm + (crop_left_right_mm * 2)
        draw_height_mm = height_mm + (crop_top_bottom_mm * 2)

    print_settings = resolve_print_settings()
    pdf_image_reader = build_pdf_image_reader(image_path, print_settings["print_mode"])

    c.drawImage(
        pdf_image_reader,
        draw_x_mm * mm,
        draw_y_mm * mm,
        width=draw_width_mm * mm,
        height=draw_height_mm * mm,
        preserveAspectRatio=False,
        mask="auto",
    )

    c.showPage()
    c.save()

    buffer.seek(0)

    safe_mode_value = (mode_value or "game_mode").strip().replace("/", "_")

    return Response(
        buffer,
        mimetype="application/pdf",
        headers={
            "Content-Disposition": f"inline; filename={safe_mode_value}.pdf"
        }
    )

@app.route("/card-search")
def card_search():
    search_query = (request.args.get("q") or "").strip()
    search_scope = (request.args.get("scope") or "token").strip().lower()
    if search_scope not in {"token", "any"}:
        search_scope = "token"

    selected_card_key = (request.args.get("selected") or "").strip()
    selected_variant = resolve_default_momir_variant()

    results = []
    featured_card = None

    if search_query:
        results = search_cards_for_print(search_query, search_scope=search_scope, limit=75)

        if selected_card_key:
            for row in results:
                if row["card_key"] == selected_card_key:
                    featured_card = row
                    break

        if featured_card is None and results:
            featured_card = results[0]
    else:
        featured_card = {
            "card_key": None,
            "name": CARD_SEARCH_DEFAULT_TITLE,
            "type_line": f"Avatar • {selected_variant['label']}",
            "image_src": url_for("static", filename=selected_variant["filename"]),
        }

    featured_card_print_href = ""

    if featured_card:
        featured_card_key = featured_card["card_key"] if featured_card["card_key"] else None

        if featured_card_key:
            featured_card_print_href = get_card_print_href(featured_card_key)
        else:
            featured_card_print_href = get_default_token_print_href()

    return render_template(
        "card_search.html",
        search_query=search_query,
        search_scope=search_scope,
        results=results,
        featured_card=featured_card,
        featured_card_print_href=featured_card_print_href,
    )

@app.route("/config", methods=["GET", "POST"])
def config():
    if request.method == "POST":
        update_config_from_form(request.form)
        flash("Configuration saved.")
        return redirect(url_for("config"))

    config_values = get_config()
    import_metadata = get_import_metadata()
    current_refresh_status = build_config_page_refresh_status(import_metadata)
    current_image_status = build_config_page_image_status()

    resolved_game_mode_cards = []
    for item in GAME_MODE_OPTIONS:
        token_image = resolve_game_mode_token_image(item["value"])
        resolved_game_mode_cards.append({
            **item,
            "image_src": url_for("static", filename=token_image["filename"]),
            "print_href": get_game_mode_print_href(item["value"]),
        })

    source_file_present = bool(
        import_metadata.get("source_last_updated")
        or import_metadata.get("cards_imported")
        or os.path.exists(ATOMIC_CARDS_PATH)
    )

    section_defaults = {
        "card_database": "0" if source_file_present else "1",
        "qr_code_print": "1",
        "print_defaults": "0",
        "card_repeats": "0",
        "game_modes": "1",
        "primary_types": "0",
        "supplemental_types": "0",
        "other_filters": "0",
    }

    access_url = build_access_url()

    return render_template(
        "config.html",
        config=config_values,
        primary_type_keys=PRIMARY_TYPE_KEYS,
        supplemental_type_keys=SUPPLEMENTAL_TYPE_KEYS,
        other_filter_keys=OTHER_FILTER_KEYS,
        game_mode_options=resolved_game_mode_cards,
        repeat_mode_options=REPEAT_MODE_OPTIONS,
        print_template_options=PRINT_TEMPLATE_OPTIONS,
        print_color_mode_options=PRINT_COLOR_MODE_OPTIONS,
        momir_default_token_variant_options=MOMIR_DEFAULT_TOKEN_VARIANT_OPTIONS,
        import_metadata=import_metadata,
        refresh_status=current_refresh_status,
        image_download_status=current_image_status,
        section_defaults=section_defaults,
        history_count=get_recent_history_count(),
        qr_access_url=access_url,
        qr_image_url=build_qr_code_image_url(access_url),
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
    try:
        import_metadata = get_import_metadata()
        return jsonify(build_config_page_refresh_status(import_metadata))
    except Exception as exc:
        return jsonify({
            "is_running": False,
            "stage": "Failed",
            "message": "Refresh status failed.",
            "error": str(exc),
        }), 500

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
    return jsonify(build_config_page_image_status())

@app.route("/history/clear", methods=["POST"])
def clear_history():
    clear_card_history()
    return jsonify({
        "ok": True,
        "history_count": 0,
        "message": "Card history cleared."
    })

@app.route("/card-image/<card_key>", methods=["GET"])
def card_image(card_key):
    card = get_card_by_key(card_key)

    if not card:
        return ("Not found", 404)

    existing_cache_path = card["image_cache_path"] or ""
    if existing_cache_path:
        abs_path = os.path.abspath(existing_cache_path)
        if os.path.exists(abs_path):
            return send_file(abs_path)

    card = ensure_card_image_cached(card)

    if card:
        refreshed_cache_path = card["image_cache_path"] or ""
        if refreshed_cache_path:
            abs_path = os.path.abspath(refreshed_cache_path)
            if os.path.exists(abs_path):
                return send_file(abs_path)

        if card["image_url"]:
            return redirect(card["image_url"])

    return ("Not found", 404)

@app.route("/chaos-draft/packs", methods=["GET"])
def chaos_draft_packs():
    packs = get_eligible_chaos_packs()

    return jsonify({
        "ok": True,
        "pack_count": len(packs),
        "packs": packs,
    })


@app.route("/chaos-draft/open-test", methods=["GET"])
def chaos_draft_open_test():
    eligible_packs = get_eligible_chaos_packs_for_spin()

    if not eligible_packs:
        return jsonify({
            "ok": False,
            "message": "No eligible Chaos Draft packs were found.",
        }), 404

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
        return jsonify({
            "ok": False,
            "message": "No eligible Chaos Draft pack variants were found.",
        }), 404

    open_result = open_chaos_pack_with_bonus_rule(
        chosen_variant["set_code"],
        chosen_variant["booster_name"],
        chosen_variant["booster_index"],
    )

    return jsonify({
        "ok": True,
        "pack": {
            "set_code": chosen_pack["set_code"],
            "booster_name": chosen_pack["booster_name"],
            "display_name": chosen_pack["display_name"],
            "image_src": chosen_pack["image_src"],
            "variant_count": chosen_pack.get("variant_count", 0),
            "total_variant_weight": chosen_pack.get("total_variant_weight", 0),
        },
        "chosen_variant": {
            "booster_index": chosen_variant["booster_index"],
            "booster_weight": chosen_variant["booster_weight"],
        },
        "bonus_pack_opened": open_result["bonus_pack_opened"],
        "total_cards": open_result["total_cards"],
        "cards": open_result["cards"],
    })

@app.route("/chaos-draft/spin", methods=["POST"])
def chaos_draft_spin():
    spin_result = build_chaos_spin_result()

    if not spin_result:
        return jsonify({
            "ok": False,
            "message": "No eligible Chaos Draft packs were found.",
        }), 404

    return jsonify({
        "ok": True,
        "spin_result": spin_result,
    })

@app.route("/chaos-draft/open", methods=["POST"])
def chaos_draft_open():
    spin_result = get_pending_chaos_spin_result()

    if not spin_result:
        return jsonify({
            "ok": False,
            "message": "No pending Chaos Draft spin found."
        }), 400

    chosen_variant = spin_result.get("chosen_variant") or {}

    set_code = chosen_variant.get("set_code")
    booster_name = chosen_variant.get("booster_name")
    booster_index = chosen_variant.get("booster_index")

    if not set_code or booster_index is None:
        return jsonify({
            "ok": False,
            "message": "Invalid Chaos Draft selection."
        }), 400

    open_result = open_chaos_pack_with_bonus_rule(
        set_code,
        booster_name,
        booster_index,
    )

    cards = open_result["cards"]

    if not cards:
        return jsonify({
            "ok": False,
            "message": "Chaos Draft pack opened but no cards were generated."
        }), 500

    # Record history (only once per selection)
    record_chaos_pack_history(
        set_code,
        booster_name,
        booster_index,
        spin_result["winning_pack"]["display_name"],
    )

    pdf_buffer = build_chaos_pack_pdf(
        cards,
        spin_result["winning_pack"]["display_name"],
    )

    filename_safe = safe_filename(f"{set_code}_{booster_name}".lower())

    pdf_buffer.seek(0)

    return Response(
        pdf_buffer.read(),
        mimetype="application/pdf",
        headers={
            "Content-Disposition": f"inline; filename={filename_safe}.pdf",
            "Cache-Control": "no-store"
        }
    )

@app.route("/chaos-draft/next", methods=["POST"])
def chaos_draft_next():
    clear_chaos_session_state("pending_spin_result")

    return jsonify({
        "ok": True,
        "message": "Chaos Draft pack cleared.",
    })

@app.route("/sets", methods=["GET", "POST"])
def sets():
    if request.method == "POST":
        update_selected_sets_from_form(request.form)
        flash("Magic set selection saved.")
        return redirect(url_for("sets"))

    config_values = get_config()
    all_sets = get_all_sets()
    selected_set_codes = get_selected_set_codes()

    current_year = datetime.now().year

    return render_template(
        "sets.html",
        config=config_values,
        all_sets=all_sets,
        selected_set_codes=selected_set_codes,
        current_year=current_year,
    )

if __name__ == "__main__":
    initialize_database()
    app.run(host="0.0.0.0", port=5000, debug=True)