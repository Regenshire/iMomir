import csv
import gzip
import hashlib
import json
import os
import random
import re
import shutil
import socket
import threading
import time
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from PIL import Image, ImageEnhance, ImageOps, ImageFilter, ImageChops, ImageDraw, ImageFont
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader, simpleSplit
from reportlab.pdfgen import canvas
from io import BytesIO, StringIO
from pypdf import PdfWriter
from flask import Flask, Response, flash, g, jsonify, redirect, render_template, request, send_file, url_for, has_request_context

from paths import (
    ALL_PRINTINGS_GZ_PATH,
    ALL_PRINTINGS_PATH,
    ALTERNATE_SOURCE_DIR,
    ATOMIC_CARDS_PATH,
    BUNDLE_BASE_DIR,
    CHAOS_IMAGE_CACHE_DIR,
    CHAOS_TEMP_CACHE_DIR,
    CUSTOM_SET_ICON_DIR,
    CAMPAIGN_PLAYER_PORTRAIT_DIR,
    EXPORT_ROOT_DIR,
    DATA_DOWNLOAD_DIR,
    IMAGE_CACHE_DIR,
    LOG_PATH,
    RUNTIME_BASE_DIR,
    SCRYFALL_DEFAULT_CARDS_PATH,
    SCRYFALL_DOWNLOAD_DIR,
    SET_BOOSTER_CONTENTS_CSV_PATH,
    SET_BOOSTER_CONTENT_WEIGHTS_CSV_PATH,
    SET_BOOSTER_SHEET_CARDS_CSV_PATH,
    SET_BOOSTER_SHEETS_CSV_PATH,
    SET_LIST_PATH,
    get_pack_art_dir,
    get_static_dir,
    get_template_dir,
)

from settings import (
    ALLOWED_CHAOS_BOOSTER_TYPES,
    APP_SECRET_KEY,
    CARD_SEARCH_DEFAULT_TITLE,
    CARD_SEARCH_DEFAULT_VARIANT,
    CARD_SEARCH_DEFAULT_VARIANTS,
    CHAOS_DUPLICATE_CONTROL_ENABLED,
    CHAOS_DUPLICATE_CONTROL_TYPES,
    CHAOS_DUPLICATE_LOG_ALL_DETECTIONS,
    CHAOS_DUPLICATE_MAX_REROLLS,
    CHAOS_DUPLICATE_REROLL_CHANCE,
    CHAOS_PACK_TYPE_OPTIONS,
    CHAOS_DRAFT_EXPORT_FORMAT_OPTIONS,
    GAME_MODE_OPTIONS,
    MOMIR_DEFAULT_TOKEN_VARIANT_OPTIONS,
    MTGJSON_ALL_PRINTINGS_URL,
    MTGJSON_ATOMIC_URL,
    MTGJSON_CSV_BASE_URL,
    MTGJSON_SET_BOOSTER_CONTENTS_URL,
    MTGJSON_SET_BOOSTER_CONTENT_WEIGHTS_URL,
    MTGJSON_SET_BOOSTER_SHEET_CARDS_URL,
    MTGJSON_SET_BOOSTER_SHEETS_URL,
    MTGJSON_SET_LIST_URL,
    OTHER_FILTER_KEYS,
    PRIMARY_TYPE_KEYS,
    PRINT_COLOR_MODE_OPTIONS,
    PRINT_TEMPLATE_METADATA,
    PRINT_TEMPLATE_OPTIONS,
    REPEAT_MODE_OPTIONS,
    SCRYFALL_IMAGE_QUALITY_OPTIONS,
    SCRYFALL_BULK_DATA_URL,
    SILHOUETTE_CORNER_RADIUS_MM,
    SILHOUETTE_EDGE_BORDER_PIXELS,
    SILHOUETTE_FILL_UNUSED_SLOTS_WITH_WHITE,
    SILHOUETTE_LETTER_CARD_HEIGHT_MM,
    SILHOUETTE_LETTER_CARD_WIDTH_MM,
    SILHOUETTE_LETTER_COLUMNS,
    SILHOUETTE_LETTER_ROWS,
    SILHOUETTE_LETTER_START_X_MM,
    SILHOUETTE_LETTER_START_Y_MM,
    SILHOUETTE_RENDER_TARGET_HEIGHT_PX,
    SILHOUETTE_RENDER_TARGET_WIDTH_PX,
    SUPPLEMENTAL_TYPE_KEYS,
    TYPE_FLAG_MAP,
)

from db.pricing import (
    download_all_prices_today_json,
    enrich_pack_cards_with_prices,
    import_all_prices_today_into_database,
)

from image_export_templates import (
    get_card_export_template_options,
    resolve_card_export_template_config,
)

from pack_label_templates import get_pack_label_template

from db.exports import (
    EXPORT_KIND_CAMPAIGN,
    EXPORT_KIND_FULL,
    EXPORT_KIND_PACKS,
    auto_clear_export_root,
    clear_all_history_data,
    clear_all_packs_data,
    clear_export_root,
    export_campaign_archive,
    export_default_campaign_archive,
    export_full_archive,
    export_packs_archive,
    import_archive_from_file_object,
)

from db.database import (
    ensure_column_exists,
    get_all_sets,
    add_card_to_custom_draft_set,
    bulk_add_most_recent_cards_to_custom_draft_set,
    bulk_delete_custom_draft_set_cards,
    bulk_update_custom_draft_set_card_category,
    delete_custom_draft_set_card,
    generate_custom_draft_set_pack_cards,
    get_custom_draft_pack_slot_options,
    get_custom_draft_pack_slots,
    get_custom_draft_pack_slots_for_booster,
    get_custom_draft_set,
    get_custom_draft_set_card_rows,
    get_custom_draft_sets,
    get_card_by_key,
    get_config,
    get_db_connection,
    get_import_metadata,
    get_selected_set_codes,
    initialize_database,
    is_card_database_ready,
    replace_selected_sets,
    set_config_value,
    set_import_metadata,
    update_config_values,
    normalize_custom_draft_set_code,
    search_chaos_cards_for_custom_draft_import_list,
    search_chaos_cards_for_custom_draft_set,
    update_custom_draft_pack_layout,
    update_custom_draft_set_card_category,
    update_custom_draft_set_card_printing,
    upsert_custom_draft_set,
)

from modes.momir import (
    build_card_filter_query,
    build_enabled_type_conditions,
    draw_random_card,
    get_enabled_type_options,
    resolve_selected_result_type,
)

from modes.tower import (
    append_common_draw_filters,
    cleanup_card_history,
    clear_card_history,
    draw_random_tower_of_power_card,
    draw_tower_of_power_batch_cards,
    fetch_random_card_by_conditions,
    get_draws_since_last_land,
    get_draws_since_non_land,
    get_enabled_primary_type_options,
    get_recent_history_count,
    record_card_history,
)

from modes.chaos import (
    build_chaos_pack_export_text,
    build_chaos_pack_pdf_from_variant,
    build_pack_tracking_code,
    build_chaos_pack_types_config_value,
    build_chaos_spin_result,
    build_tracked_packs_combined_pdf,
    build_campaign_chaos_spin_result,
    build_default_chaos_pack_display_name,
    build_pending_chaos_pack_pdf,
    build_preprint_chaos_draft_pdf,
    clear_chaos_pack_history,
    clear_chaos_session_state,
    create_random_pack_preview_for_manage_packs,
    create_specific_pack_preview_for_manage_packs,
    create_campaign_player,
    create_chaos_campaign,
    create_chaos_draft_game,
    create_custom_pack_preview_for_manage_packs,
    delete_tracked_packs,
    get_custom_pack_populate_options_for_set,
    populate_custom_pack_decklist_from_booster,
    get_campaign_players,
    get_selected_or_create_chaos_draft_game,
    get_chaos_draft_game_display_label,
    get_campaign_player_import_options,
    get_campaign_pack_import_options,
    get_importable_campaign_pack_rows,
    import_tracked_packs_from_campaign,
    get_campaign_history_filter_options,
    get_campaign_history_rows,
    get_active_chaos_draft_game,
    delete_all_campaign_history,
    delete_selected_campaign_history,
    get_chaos_campaigns,
    get_chaos_campaign_by_id,
    get_campaign_player_by_id,
    get_selected_campaign_player_id,
    import_campaign_players_from_campaign,
    get_selected_chaos_campaign_id,
    get_chaos_opened_pack_keys,
    get_chaos_pack_art_info,
    get_chaos_pack_art_relpath,
    get_chaos_pack_type_label_map,
    get_chaos_pack_variants,
    get_chaos_session_state,
    get_eligible_chaos_packs,
    get_eligible_chaos_packs_for_spin,
    get_pending_chaos_spin_result,
    get_selected_chaos_pack_types,
    get_tracked_pack_management_rows,
    get_tracked_pack_state_by_id,
    normalize_booster_type_for_filter,
    normalize_chaos_booster_key,
    normalize_chaos_pack_display_name,
    normalize_tracked_pack_id_list,
    open_chaos_pack_with_bonus_rule,
    parse_chaos_pack_types_config,
    record_chaos_pack_history,
    record_campaign_pack_opening,
    set_selected_campaign_player_id,
    set_selected_chaos_campaign_id,
    save_opened_chaos_pack_to_tracking_db,
    update_campaign_player,
    update_chaos_campaign,
    delete_campaign_player,
    delete_chaos_campaign,
    set_chaos_session_state,
    search_manage_pack_options,
    set_tracked_packs_campaign_enabled,
    sort_opened_chaos_pack_cards,
)

app = Flask(
    __name__,
    template_folder=get_template_dir(),
    static_folder=get_static_dir(),
)
app.secret_key = APP_SECRET_KEY

def get_request_config():
    if not hasattr(g, "_config_cache"):
        g._config_cache = get_config()
    return g._config_cache

def get_auto_clear_exports_config_value():
    try:
        config = get_request_config() if has_request_context() else get_config()
    except Exception:
        config = {}

    value = (config.get("auto_clear_exports") or "7").strip().lower()

    if value not in {"off", "1", "7", "30"}:
        value = "7"

    return value

def _build_pdf_print_settings(config):
    use_pdf_print = (config.get("use_pdf_print") or "1").strip() == "1"
    crop_border = (config.get("pdf_crop_border") or "1").strip() == "1"
    print_front_back_label = (config.get("print_front_back_label") or "1").strip() == "1"
    print_pack_tracking_code = (config.get("print_pack_tracking_code") or "0").strip() == "1"

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
        "print_front_back_label": print_front_back_label,
        "print_pack_tracking_code": print_pack_tracking_code,
    }

def get_request_pdf_print_settings():
    if not hasattr(g, "_pdf_print_settings_cache"):
        g._pdf_print_settings_cache = _build_pdf_print_settings(get_request_config())
    return g._pdf_print_settings_cache


def _build_pdf_template_layout(config):
    print_template = (config.get("print_template") or "dk-1234").strip().lower()

    if print_template not in {
        "dk-1234",
        "standard",
        "borderless-3p5x5-two-card",
        "silhouette-letter-horizontal-8",
        "perf-63x94",
        "perf-69x94",
        "landscape-3p5x5-centered",
        "portrait-3p5x5-top-aligned",
    }:
        print_template = "dk-1234"

    template_layout = resolve_print_template_layout(print_template)

    return {
        "print_template": template_layout["print_template"],
        "page_width_mm": template_layout["page_width_mm"],
        "page_height_mm": template_layout["page_height_mm"],
        "draw_x_mm": template_layout["sheet_offset_x_mm"],
        "draw_y_mm": template_layout["sheet_offset_y_mm"],
        "draw_width_mm": template_layout["sheet_width_mm"],
        "draw_height_mm": template_layout["sheet_height_mm"],
        "uses_fixed_inner_margin": template_layout["uses_fixed_inner_margin"],
        "is_multi_card_layout": template_layout.get("is_multi_card_layout", False),
        "is_silhouette_layout": template_layout.get("is_silhouette_layout", False),
    }


def get_request_pdf_template_layout():
    if not hasattr(g, "_pdf_template_layout_cache"):
        g._pdf_template_layout_cache = _build_pdf_template_layout(get_request_config())
    return g._pdf_template_layout_cache


def _build_print_settings(config):
    print_template = (request.args.get("template") or config.get("print_template") or "dk-1234").strip().lower()
    if print_template not in {
        "dk-1234",
        "standard",
        "borderless-3p5x5-two-card",
        "silhouette-letter-horizontal-8",
        "perf-63x94",
        "perf-69x94",
        "landscape-3p5x5-centered",
        "portrait-3p5x5-top-aligned",
    }:
        print_template = "dk-1234"

    template_layout = resolve_print_template_layout(print_template)

    print_mode = (request.args.get("mode") or config.get("print_color_mode") or "grayscale").strip().lower()
    if print_mode not in {"grayscale", "color", "monochrome", "optimal"}:
        print_mode = "grayscale"

    open_in_new_tab = (config.get("open_print_in_new_tab") or "1").strip() == "1"

    return {
        "print_template": template_layout["print_template"],
        "print_width": template_layout["page_width_css"],
        "print_height": template_layout["page_height_css"],
        "sheet_width": template_layout["sheet_width_css"],
        "sheet_height": template_layout["sheet_height_css"],
        "sheet_offset_x": template_layout["sheet_offset_x_css"],
        "sheet_offset_y": template_layout["sheet_offset_y_css"],
        "print_mode": print_mode,
        "open_in_new_tab": open_in_new_tab,
        "is_multi_card_layout": template_layout.get("is_multi_card_layout", False),
    }


def get_request_print_settings():
    if not hasattr(g, "_print_settings_cache"):
        g._print_settings_cache = _build_print_settings(get_request_config())
    return g._print_settings_cache


def _build_default_momir_variant(config):
    variant = (config.get("momir_default_token_variant") or CARD_SEARCH_DEFAULT_VARIANT).strip().lower()

    if variant not in CARD_SEARCH_DEFAULT_VARIANTS:
        variant = CARD_SEARCH_DEFAULT_VARIANT

    return {
        "key": variant,
        "label": CARD_SEARCH_DEFAULT_VARIANTS[variant]["label"],
        "filename": CARD_SEARCH_DEFAULT_VARIANTS[variant]["filename"],
    }


def get_request_default_momir_variant():
    if not hasattr(g, "_default_momir_variant_cache"):
        g._default_momir_variant_cache = _build_default_momir_variant(get_request_config())
    return g._default_momir_variant_cache

def parse_utc_metadata_datetime(raw_value):
    raw_value = (raw_value or "").strip()

    if not raw_value:
        return None

    for date_format in (
        "%Y-%m-%d %H:%M:%S UTC",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(raw_value, date_format).replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    return None


def get_reminder_frequency_days(config):
    reminder_frequency = (config.get("card_database_reminder_frequency") or "monthly").strip().lower()

    reminder_frequency_days = {
        "weekly": 7,
        "monthly": 30,
        "quarterly": 90,
        "yearly": 365,
    }

    return reminder_frequency_days.get(reminder_frequency)


def build_global_reminder_state(config=None, import_metadata=None):
    if config is None:
        config = get_request_config()

    if import_metadata is None:
        import_metadata = get_import_metadata()

    reminder_items = []

    reminder_frequency = (config.get("card_database_reminder_frequency") or "monthly").strip().lower()
    if reminder_frequency not in {"weekly", "monthly", "quarterly", "yearly", "never"}:
        reminder_frequency = "monthly"

    card_database_ready = is_card_database_ready()
    last_refresh_text = import_metadata.get("last_refresh_utc", "")
    last_refresh_datetime = parse_utc_metadata_datetime(last_refresh_text)

    if not card_database_ready:
        reminder_items.append({
            "key": "card_database_missing",
            "severity": "warning",
            "title": "Card Database Setup Needed",
            "message": "Download the card database before using iMomir normally.",
            "target_section": "card_database",
        })
    elif reminder_frequency != "never":
        reminder_days = get_reminder_frequency_days(config)

        if reminder_days and last_refresh_datetime:
            age_days = (datetime.now(timezone.utc) - last_refresh_datetime).days

            if age_days >= reminder_days:
                reminder_items.append({
                    "key": "card_database_due",
                    "severity": "info",
                    "title": "Card Database Update Due",
                    "message": f"Your card database was last refreshed {age_days} day(s) ago.",
                    "target_section": "card_database",
                })
        elif not last_refresh_datetime:
            reminder_items.append({
                "key": "card_database_unknown",
                "severity": "warning",
                "title": "Card Database Refresh Unknown",
                "message": "The app could not determine when the card database was last refreshed.",
                "target_section": "card_database",
            })

    return {
        "count": len(reminder_items),
        "items": reminder_items,
        "has_reminders": len(reminder_items) > 0,
        "card_database_reminder_frequency": reminder_frequency,
    }


@app.context_processor
def inject_global_template_state():
    config = get_request_config()
    current_game_mode = (config.get("game_mode") or "custom").strip().lower()
    global_reminder_state = build_global_reminder_state(config=config)

    return {
        "nav_current_game_mode": current_game_mode,
        "global_reminder_state": global_reminder_state,
    }

PACK_ART_DIR = get_pack_art_dir(app.static_folder)

SCRYFALL_SETS_API_URL = "https://api.scryfall.com/sets"
SET_ICON_RELATIVE_DIR = "img/set_icons"

refresh_status = {
    "is_running": False,
    "stage": "Idle",
    "message": "No refresh has been run yet.",
    "detail_lines": [],
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

app.config["DEBUG_LOG_ENABLED"] = False

def set_runtime_debug_log_enabled_from_config():
    try:
        config = get_config()
        app.config["DEBUG_LOG_ENABLED"] = (config.get("debug_log") or "0").strip() == "1"
    except Exception:
        app.config["DEBUG_LOG_ENABLED"] = False

def write_debug_log(message):
    if not app.config.get("DEBUG_LOG_ENABLED", False):
        return

    try:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        with open(LOG_PATH, "a", encoding="utf-8") as log_file:
            log_file.write(f"[{timestamp}] {message}\n")
    except Exception:
        pass

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

    try:
        stage = kwargs.get("stage", refresh_status.get("stage", ""))
        message = kwargs.get("message", refresh_status.get("message", ""))
        error = kwargs.get("error", "")
        write_debug_log(f"REFRESH STATUS | stage={stage} | message={message} | error={error}")
    except Exception:
        pass

def clear_refresh_detail_lines():
    with refresh_lock:
        refresh_status["detail_lines"] = []


def append_refresh_detail_line(detail_text):
    detail_text = str(detail_text or "").strip()
    if not detail_text:
        return

    with refresh_lock:
        detail_lines = list(refresh_status.get("detail_lines", []))
        detail_lines.append(detail_text)

        # Keep the list from growing forever during long refresh jobs.
        if len(detail_lines) > 150:
            detail_lines = detail_lines[-150:]

        refresh_status["detail_lines"] = detail_lines

def get_refresh_status_copy():
    with refresh_lock:
        return {
            **refresh_status,
            "detail_lines": list(refresh_status.get("detail_lines", [])),
        }

def set_image_download_status(**kwargs):
    with image_download_lock:
        image_download_status.update(kwargs)

    try:
        stage = kwargs.get("stage", image_download_status.get("stage", ""))
        message = kwargs.get("message", image_download_status.get("message", ""))
        error = kwargs.get("error", "")
        write_debug_log(f"IMAGE STATUS | stage={stage} | message={message} | error={error}")
    except Exception:
        pass

def get_image_download_status_copy():
    with image_download_lock:
        return dict(image_download_status)

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
        "print_front_back_label",
        "print_pack_tracking_code",
        "print_pack_labels",
        "enable_track_packs",
        "enable_chaos_card_image_export",
        "export_add_bleed",
        "chaos_replace_basic_lands",
        "use_pack_image_for_title",
        "open_print_in_new_tab",
        "sound_enabled",
        "debug_log",
        "display_pack_prices",
    }

    select_defaults = {
        "game_mode": "custom",
        "allow_repeats": "1",
        "print_template": "dk-1234",
        "print_color_mode": "grayscale",
        "chaos_draft_export_format": "none",
        "chaos_scryfall_image_quality": "png",
        "auto_clear_exports": "7",
        "card_database_reminder_frequency": "monthly",
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
        "chaos_draft_campaign",
        "preprint_chaos_draft",
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
    if submitted_template not in {"dk-1234", "standard", "borderless-3p5x5-two-card", "silhouette-letter-horizontal-8", "perf-63x94", "perf-69x94", "landscape-3p5x5-centered", "portrait-3p5x5-top-aligned"}:
        submitted_template = select_defaults["print_template"]
    updated_config["print_template"] = submitted_template

    submitted_color_mode = (form_data.get("print_color_mode") or "").strip().lower()
    if submitted_color_mode not in {"grayscale", "color", "monochrome", "optimal"}:
        submitted_color_mode = select_defaults["print_color_mode"]
    updated_config["print_color_mode"] = submitted_color_mode

    submitted_pack_price_source = (form_data.get("pack_price_source") or "").strip().lower()
    if submitted_pack_price_source not in {"tcgplayer-retail"}:
        submitted_pack_price_source = "tcgplayer-retail"
    updated_config["pack_price_source"] = submitted_pack_price_source

    submitted_momir_variant = (form_data.get("momir_default_token_variant") or "").strip().lower()
    if submitted_momir_variant not in {"dark", "light", "retro", "mtgo"}:
        submitted_momir_variant = "dark"
    updated_config["momir_default_token_variant"] = submitted_momir_variant

    submitted_chaos_export_format = (form_data.get("chaos_draft_export_format") or "").strip().lower()
    if submitted_chaos_export_format not in {"none", "archidekt", "moxfield"}:
        submitted_chaos_export_format = select_defaults["chaos_draft_export_format"]
    updated_config["chaos_draft_export_format"] = submitted_chaos_export_format

    submitted_scryfall_image_quality = (form_data.get("chaos_scryfall_image_quality") or "").strip().lower()
    if submitted_scryfall_image_quality not in {"normal", "large", "png"}:
        submitted_scryfall_image_quality = select_defaults["chaos_scryfall_image_quality"]
    updated_config["chaos_scryfall_image_quality"] = submitted_scryfall_image_quality

    submitted_auto_clear_exports = (form_data.get("auto_clear_exports") or "").strip().lower()
    if submitted_auto_clear_exports not in {"off", "1", "7", "30"}:
        submitted_auto_clear_exports = select_defaults["auto_clear_exports"]
    updated_config["auto_clear_exports"] = submitted_auto_clear_exports

    submitted_card_database_reminder_frequency = (form_data.get("card_database_reminder_frequency") or "").strip().lower()
    if submitted_card_database_reminder_frequency not in {"weekly", "monthly", "quarterly", "yearly", "never"}:
        submitted_card_database_reminder_frequency = select_defaults["card_database_reminder_frequency"]
    updated_config["card_database_reminder_frequency"] = submitted_card_database_reminder_frequency

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

    submitted_print_bleed_size_mm = (form_data.get("print_bleed_size_mm") or "").strip()
    try:
        parsed_print_bleed_size_mm = float(submitted_print_bleed_size_mm)
        if parsed_print_bleed_size_mm < 0:
            raise ValueError()
    except ValueError:
        parsed_print_bleed_size_mm = 3.0

    if parsed_print_bleed_size_mm > 10:
        parsed_print_bleed_size_mm = 10.0

    updated_config["print_bleed_size_mm"] = str(parsed_print_bleed_size_mm)

    if submitted_game_mode == "tower_of_power":
        any_primary_selected = any(updated_config.get(key) == "1" for key, _ in PRIMARY_TYPE_KEYS)
        if not any_primary_selected:
            for key, _ in PRIMARY_TYPE_KEYS:
                updated_config[key] = "1"

    update_config_values(updated_config)
    set_runtime_debug_log_enabled_from_config()

def parse_bool_csv(value):
    return str(value or "").strip().upper() == "TRUE"

def ensure_chaos_pack_art_set_folders():
    os.makedirs(PACK_ART_DIR, exist_ok=True)

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT set_code
        FROM sets
        WHERE set_code IS NOT NULL
          AND TRIM(set_code) <> ''
        ORDER BY set_code
        """
    )

    rows = cursor.fetchall()
    conn.close()

    created_count = 0

    for row in rows:
        set_code = (row["set_code"] or "").strip().lower()
        if not set_code:
            continue

        set_folder_path = os.path.join(PACK_ART_DIR, set_code)

        if not os.path.exists(set_folder_path):
            os.makedirs(set_folder_path, exist_ok=True)
            created_count += 1
            write_debug_log(f"PACK ART FOLDER CREATED | set_code={set_code} | path={set_folder_path}")

    write_debug_log(f"PACK ART FOLDER ENSURE COMPLETE | created_count={created_count}")

    return created_count

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

    uuid_lookup = {}

    for set_code, set_payload in all_sets.items():
        if not isinstance(set_payload, dict):
            continue

        cards = safe_list(set_payload.get("cards"))
        for card_obj in cards:
            if not isinstance(card_obj, dict):
                continue

            card_uuid = (card_obj.get("uuid") or "").strip()
            if card_uuid:
                uuid_lookup[card_uuid] = card_obj

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

            layout = (card_obj.get("layout") or "").strip().lower()
            side = (card_obj.get("side") or "").strip().lower()
            frame_version = (card_obj.get("frameVersion") or "").strip().lower()
            border_color = (card_obj.get("borderColor") or "").strip().lower()
            colors_json = json.dumps(safe_list(card_obj.get("colors")))
            color_identity_json = json.dumps(safe_list(card_obj.get("colorIdentity")))
            edhrec_rank = safe_int_or_none(card_obj.get("edhrecRank"))
            edhrec_saltiness = safe_float_or_none(card_obj.get("edhrecSaltiness"))

            if is_dual_faced_layout(layout) and side == "b":
                continue

            face_payloads = extract_chaos_card_faces(card_obj, uuid_lookup)
            face_count = len(face_payloads)
            is_dual_faced = 1 if (is_dual_faced_layout(layout) and side == "a" and face_count >= 2) else 0

            front_image_url = face_payloads[0]["image_url"] if face_count >= 1 else None
            back_image_url = face_payloads[1]["image_url"] if face_count >= 2 else None
            front_face_name = face_payloads[0]["name"] if face_count >= 1 else None
            back_face_name = face_payloads[1]["name"] if face_count >= 2 else None

            if is_dual_faced or is_dual_faced_layout(layout):
                write_debug_log(
                    f"CHAOS CARD IMPORT | card_name={card_name} | layout={layout} | side={side} | "
                    f"face_count={face_count} | is_dual_faced={is_dual_faced} | "
                    f"front_face={front_face_name} | back_face={back_face_name} | "
                    f"front_image={'yes' if front_image_url else 'no'} | back_image={'yes' if back_image_url else 'no'}"
                )

            image_url = front_image_url or (build_scryfall_image_url(scryfall_id) if scryfall_id else None)

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
                    is_booster,
                    faces_json,
                    face_count,
                    is_dual_faced,
                    front_image_url,
                    back_image_url,
                    front_face_name,
                    back_face_name,
                    frame_version,
                    border_color,
                    colors_json,
                    color_identity_json,
                    edhrec_rank,
                    edhrec_saltiness
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    layout,
                    card_obj.get("number"),
                    scryfall_id,
                    scryfall_illustration_id,
                    image_url,
                    None,
                    is_booster,
                    json.dumps(face_payloads),
                    face_count,
                    is_dual_faced,
                    front_image_url,
                    back_image_url,
                    front_face_name,
                    back_face_name,
                    frame_version,
                    border_color,
                    colors_json,
                    color_identity_json,
                    edhrec_rank,
                    edhrec_saltiness,
                ),
            )

            imported_count += 1

            if imported_count % 5000 == 0:
                conn.commit()
                write_debug_log(f"CHAOS CARD IMPORT | progress imported={imported_count}")
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

def resolve_print_template_layout(print_template):
    normalized_template = (print_template or "").strip().lower()

    if normalized_template == "standard":
        return {
            "print_template": "standard",

            "page_width_css": "2.5in",
            "page_height_css": "3.5in",
            "page_width_mm": 63.5,
            "page_height_mm": 88.9,

            "sheet_width_css": "2.5in",
            "sheet_height_css": "3.5in",
            "sheet_width_mm": 63.5,
            "sheet_height_mm": 88.9,

            "sheet_offset_x_css": "0mm",
            "sheet_offset_y_css": "0mm",
            "sheet_offset_x_mm": 0.0,
            "sheet_offset_y_mm": 0.0,

            "uses_fixed_inner_margin": False,
        }
    
    if normalized_template == "borderless-3p5x5-two-card":
        return {
            "print_template": "borderless-3p5x5-two-card",

            # Final sheet/page size: 3.5 x 5 portrait
            "page_width_css": "3.5in",
            "page_height_css": "5in",
            "page_width_mm": 88.9,
            "page_height_mm": 127.0,

            # Legacy single-sheet values are kept for compatibility,
            # but this template will use explicit card slots instead.
            "sheet_width_css": "3.5in",
            "sheet_height_css": "5in",
            "sheet_width_mm": 88.9,
            "sheet_height_mm": 127.0,

            "sheet_offset_x_css": "0in",
            "sheet_offset_y_css": "0in",
            "sheet_offset_x_mm": 0.0,
            "sheet_offset_y_mm": 0.0,

            "uses_fixed_inner_margin": True,

            # New flag so both HTML and PDF know this is a 2-card composed layout.
            "is_multi_card_layout": True,
        }
    
    if normalized_template == "silhouette-letter-horizontal-8":
        return {
            "print_template": "silhouette-letter-horizontal-8",

            "page_width_css": "11in",
            "page_height_css": "8.5in",
            "page_width_mm": 279.4,
            "page_height_mm": 215.9,

            "sheet_width_css": "11in",
            "sheet_height_css": "8.5in",
            "sheet_width_mm": 279.4,
            "sheet_height_mm": 215.9,

            "sheet_offset_x_css": "0mm",
            "sheet_offset_y_css": "0mm",
            "sheet_offset_x_mm": 0.0,
            "sheet_offset_y_mm": 0.0,

            "uses_fixed_inner_margin": True,
            "is_multi_card_layout": True,
            "is_silhouette_layout": True,
        }
    
    if normalized_template == "portrait-3p5x5-top-aligned":
        return {
            "print_template": "portrait-3p5x5-top-aligned",

            "page_width_css": "3.5in",
            "page_height_css": "5in",
            "page_width_mm": 88.9,
            "page_height_mm": 127.0,

            "sheet_width_css": "2.5in",
            "sheet_height_css": "3.5in",
            "sheet_width_mm": 63.5,
            "sheet_height_mm": 88.9,

            "sheet_offset_x_css": "0.5in",
            "sheet_offset_y_css": "0mm",
            "sheet_offset_x_mm": 12.7,
            "sheet_offset_y_mm": 38.1,

            "uses_fixed_inner_margin": True,
        }

    if normalized_template == "landscape-3p5x5-centered":
        return {
            "print_template": "landscape-3p5x5-centered",

            "page_width_css": "5in",
            "page_height_css": "3.5in",
            "page_width_mm": 127.0,
            "page_height_mm": 88.9,

            "sheet_width_css": "2.5in",
            "sheet_height_css": "3.5in",
            "sheet_width_mm": 63.5,
            "sheet_height_mm": 88.9,

            "sheet_offset_x_css": "1.25in",
            "sheet_offset_y_css": "0in",
            "sheet_offset_x_mm": 31.75,
            "sheet_offset_y_mm": 0.0,

            "uses_fixed_inner_margin": True,
        }

    if normalized_template == "perf-63x94":
        return {
            "print_template": "perf-63x94",

            "page_width_css": "69mm",
            "page_height_css": "94mm",
            "page_width_mm": 63.0,
            "page_height_mm": 94.0,

            "sheet_width_css": "63mm",
            "sheet_height_css": "88mm",
            "sheet_width_mm": 63.0,
            "sheet_height_mm": 88.0,

            "sheet_offset_x_css": "0mm",
            "sheet_offset_y_css": "3mm",
            "sheet_offset_x_mm": 0.0,
            "sheet_offset_y_mm": 3.0,

            "uses_fixed_inner_margin": True,
        }

    if normalized_template == "perf-69x94":
        return {
            "print_template": "perf-69x94",

            "page_width_css": "69mm",
            "page_height_css": "94mm",
            "page_width_mm": 69.0,
            "page_height_mm": 94.0,

            "sheet_width_css": "63mm",
            "sheet_height_css": "88mm",
            "sheet_width_mm": 63.0,
            "sheet_height_mm": 88.0,

            "sheet_offset_x_css": "3mm",
            "sheet_offset_y_css": "3mm",
            "sheet_offset_x_mm": 3.0,
            "sheet_offset_y_mm": 3.0,

            "uses_fixed_inner_margin": True,
        }

    return {
        "print_template": "dk-1234",

        "page_width_css": "63mm",
        "page_height_css": "86mm",
        "page_width_mm": 63.0,
        "page_height_mm": 86.0,

        "sheet_width_css": "63mm",
        "sheet_height_css": "86mm",
        "sheet_width_mm": 63.0,
        "sheet_height_mm": 86.0,

        "sheet_offset_x_css": "0mm",
        "sheet_offset_y_css": "0mm",
        "sheet_offset_x_mm": 0.0,
        "sheet_offset_y_mm": 0.0,

        "uses_fixed_inner_margin": False,
    }

def resolve_pdf_print_settings():
    return get_request_pdf_print_settings()

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

    set_config_value("tower_pdf_draw_count", str(parsed_value))

    return parsed_value

def is_silhouette_template(print_template):
    normalized_template = (print_template or "").strip().lower()
    return normalized_template in {
        "silhouette-letter-horizontal-8",
    }

def resolve_pdf_template_layout():
    return get_request_pdf_template_layout()

def get_active_print_template_metadata():
    config = get_request_config()
    selected_template_value = (config.get("print_template") or "dk-1234").strip().lower()

    if selected_template_value not in PRINT_TEMPLATE_METADATA:
        return {
            "template_value": selected_template_value,
            "download_links": [],
        }

    template_metadata = PRINT_TEMPLATE_METADATA[selected_template_value]

    return {
        "template_value": selected_template_value,
        "download_links": list(template_metadata.get("download_links", [])),
    }

def resolve_print_settings():
    return get_request_print_settings()

def resolve_default_momir_variant():
    return get_request_default_momir_variant()

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
    selected_pack_types = form_data.getlist("chaos_pack_types")
    chaos_pack_types_value = build_chaos_pack_types_config_value(selected_pack_types)

    set_config_value("all_sets_enabled", all_sets_enabled)
    set_config_value("chaos_pack_types", chaos_pack_types_value)

    if all_sets_enabled == "0":
        selected_set_codes = form_data.getlist("selected_sets")
        replace_selected_sets(selected_set_codes)
    else:
        replace_selected_sets([])

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
    os.makedirs(CHAOS_IMAGE_CACHE_DIR, exist_ok=True)
    os.makedirs(CHAOS_TEMP_CACHE_DIR, exist_ok=True)
    os.makedirs(CAMPAIGN_PLAYER_PORTRAIT_DIR, exist_ok=True)
    os.makedirs(ALTERNATE_SOURCE_DIR, exist_ok=True)
    os.makedirs(CUSTOM_SET_ICON_DIR, exist_ok=True)
    os.makedirs(os.path.join(app.static_folder, SET_ICON_RELATIVE_DIR.replace("/", os.sep)), exist_ok=True)

def save_custom_set_icon_file(uploaded_file, set_code):
    if not uploaded_file:
        return ""

    original_filename = (uploaded_file.filename or "").strip()
    if not original_filename:
        return ""

    safe_ext = os.path.splitext(original_filename)[1].strip().lower()
    if safe_ext != ".svg":
        raise ValueError("Set icon upload must be an SVG file.")

    ensure_download_directories()

    clean_set_code = normalize_custom_draft_set_code(set_code)
    safe_set_code = safe_filename(clean_set_code)
    output_filename = f"custom_set_icon_{safe_set_code}.svg"
    output_path = os.path.join(CUSTOM_SET_ICON_DIR, output_filename)

    uploaded_file.save(output_path)

    relative_path = os.path.relpath(output_path, app.static_folder).replace("\\", "/")

    # The file is outside static in normal development, so store a runtime-relative path instead.
    # get_pack_label_set_icon_path() joins this with app.static_folder, so we also copy a static-safe version.
    static_icon_dir = os.path.join(app.static_folder, "img", "set_icons")
    os.makedirs(static_icon_dir, exist_ok=True)

    static_output_filename = f"{safe_set_code}.svg"
    static_output_path = os.path.join(static_icon_dir, static_output_filename)

    shutil.copyfile(output_path, static_output_path)

    return f"img/set_icons/{static_output_filename}"

def save_campaign_player_portrait_file(uploaded_file, player_id):
    if not uploaded_file:
        return ""

    original_filename = (uploaded_file.filename or "").strip()
    if not original_filename:
        return ""

    ensure_download_directories()

    safe_ext = os.path.splitext(original_filename)[1].strip().lower()
    if safe_ext not in {".png", ".jpg", ".jpeg", ".webp"}:
        safe_ext = ".png"

    output_filename = f"player_{int(player_id)}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}.png"
    output_path = os.path.join(CAMPAIGN_PLAYER_PORTRAIT_DIR, output_filename)

    with Image.open(uploaded_file.stream) as source_image:
        image = source_image.convert("RGB")

        square_size = min(image.size)
        left = int((image.width - square_size) / 2)
        top = int((image.height - square_size) / 2)
        right = left + square_size
        bottom = top + square_size

        image = image.crop((left, top, right, bottom))
        image = image.resize((512, 512), Image.LANCZOS)
        image.save(output_path, format="PNG")

    return output_filename

def format_download_size(byte_count):
    try:
        byte_count = int(byte_count or 0)
    except (TypeError, ValueError):
        byte_count = 0

    if byte_count >= 1024 * 1024 * 1024:
        return f"{byte_count / (1024 * 1024 * 1024):.2f} GB"

    if byte_count >= 1024 * 1024:
        return f"{byte_count / (1024 * 1024):.2f} MB"

    if byte_count >= 1024:
        return f"{byte_count / 1024:.2f} KB"

    return f"{byte_count} bytes"


def remove_file_if_exists(file_path):
    try:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
    except Exception:
        pass


def download_file_with_retries(
    url,
    destination_path,
    headers=None,
    label="Download",
    force_download=True,
    expected_min_bytes=1,
    attempts=3,
    connect_timeout=30,
    read_timeout=60,
    chunk_size=1024 * 1024,
):
    ensure_download_directories()

    destination_path = os.path.abspath(destination_path)
    temp_path = f"{destination_path}.part"

    if (
        not force_download
        and os.path.exists(destination_path)
        and os.path.getsize(destination_path) >= int(expected_min_bytes or 1)
    ):
        existing_size = os.path.getsize(destination_path)
        append_refresh_detail_line(
            f"=== {label}: USING EXISTING FILE === {destination_path} ({format_download_size(existing_size)})"
        )

        return {
            "downloaded": False,
            "path": destination_path,
            "size": existing_size,
            "message": "Existing file is valid.",
        }

    remove_file_if_exists(temp_path)

    headers = headers or {}

    last_error = None

    for attempt_number in range(1, int(attempts or 1) + 1):
        downloaded_bytes = 0
        content_length = None
        last_log_at = time.monotonic()

        try:
            append_refresh_detail_line(
                f"=== {label}: DOWNLOAD ATTEMPT {attempt_number}/{attempts} ==="
            )
            append_refresh_detail_line(f"=== {label}: URL === {url}")
            append_refresh_detail_line(f"=== {label}: TEMP PATH === {temp_path}")

            set_refresh_status(
                stage=f"Downloading {label}",
                message=f"Starting {label} download attempt {attempt_number}/{attempts}...",
            )

            with requests.get(
                url,
                headers=headers,
                timeout=(connect_timeout, read_timeout),
                stream=True,
            ) as response:
                response.raise_for_status()

                raw_content_length = response.headers.get("Content-Length")
                try:
                    content_length = int(raw_content_length) if raw_content_length else None
                except (TypeError, ValueError):
                    content_length = None

                append_refresh_detail_line(f"=== {label}: HTTP STATUS === {response.status_code}")
                append_refresh_detail_line(f"=== {label}: CONTENT-TYPE === {response.headers.get('Content-Type')}")
                append_refresh_detail_line(f"=== {label}: CONTENT-LENGTH === {raw_content_length or 'Unknown'}")

                if content_length is not None and content_length < int(expected_min_bytes or 1):
                    raise ValueError(
                        f"{label} returned a suspiciously small Content-Length: {content_length} bytes."
                    )

                with open(temp_path, "wb") as file_handle:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if not chunk:
                            continue

                        file_handle.write(chunk)
                        downloaded_bytes += len(chunk)

                        now = time.monotonic()
                        if now - last_log_at >= 5:
                            if content_length:
                                message = (
                                    f"{label}: downloaded {format_download_size(downloaded_bytes)} "
                                    f"of {format_download_size(content_length)}"
                                )
                            else:
                                message = f"{label}: downloaded {format_download_size(downloaded_bytes)}"

                            append_refresh_detail_line(f"=== {message} ===")
                            set_refresh_status(
                                stage=f"Downloading {label}",
                                message=message,
                            )

                            try:
                                file_handle.flush()
                            except Exception:
                                pass

                            last_log_at = now

                if downloaded_bytes < int(expected_min_bytes or 1):
                    raise ValueError(
                        f"{label} downloaded only {downloaded_bytes} bytes; expected at least {expected_min_bytes} bytes."
                    )

                if content_length is not None and downloaded_bytes != content_length:
                    raise ValueError(
                        f"{label} download size mismatch. Expected {content_length} bytes, got {downloaded_bytes} bytes."
                    )

                os.replace(temp_path, destination_path)

                append_refresh_detail_line(
                    f"=== {label}: DOWNLOAD COMPLETE === {format_download_size(downloaded_bytes)}"
                )

                set_refresh_status(
                    stage=f"Downloading {label}",
                    message=f"{label} download complete: {format_download_size(downloaded_bytes)}.",
                )

                return {
                    "downloaded": True,
                    "path": destination_path,
                    "size": downloaded_bytes,
                    "message": "Download complete.",
                }

        except Exception as exc:
            last_error = exc
            remove_file_if_exists(temp_path)

            append_refresh_detail_line(
                f"=== {label}: DOWNLOAD ATTEMPT {attempt_number}/{attempts} FAILED === {str(exc)}"
            )

            set_refresh_status(
                stage=f"Downloading {label}",
                message=f"{label} download attempt {attempt_number}/{attempts} failed.",
                error=str(exc) if attempt_number >= int(attempts or 1) else "",
            )

            if attempt_number < int(attempts or 1):
                sleep_seconds = min(10, attempt_number * 3)
                append_refresh_detail_line(
                    f"=== {label}: RETRYING IN {sleep_seconds} SECOND(S) ==="
                )
                time.sleep(sleep_seconds)

    raise RuntimeError(f"{label} failed after {attempts} attempt(s): {last_error}")


def extract_gzip_file_with_progress(
    gzip_path,
    destination_path,
    label="Gzip Extract",
    expected_min_bytes=1,
    chunk_size=1024 * 1024,
):
    gzip_path = os.path.abspath(gzip_path)
    destination_path = os.path.abspath(destination_path)
    temp_path = f"{destination_path}.part"

    remove_file_if_exists(temp_path)

    if not os.path.exists(gzip_path):
        raise FileNotFoundError(f"{label} source file was not found: {gzip_path}")

    gzip_size = os.path.getsize(gzip_path)
    if gzip_size <= 0:
        raise ValueError(f"{label} source file is empty: {gzip_path}")

    append_refresh_detail_line(
        f"=== {label}: EXTRACT BEGIN === source={gzip_path} size={format_download_size(gzip_size)}"
    )
    append_refresh_detail_line(f"=== {label}: TEMP PATH === {temp_path}")

    set_refresh_status(
        stage=f"Extracting {label}",
        message=f"Extracting {label}...",
    )

    extracted_bytes = 0
    last_log_at = time.monotonic()

    try:
        with gzip.open(gzip_path, "rb") as compressed_file:
            with open(temp_path, "wb") as output_file:
                while True:
                    chunk = compressed_file.read(chunk_size)
                    if not chunk:
                        break

                    output_file.write(chunk)
                    extracted_bytes += len(chunk)

                    now = time.monotonic()
                    if now - last_log_at >= 5:
                        message = f"{label}: extracted {format_download_size(extracted_bytes)}"
                        append_refresh_detail_line(f"=== {message} ===")
                        set_refresh_status(
                            stage=f"Extracting {label}",
                            message=message,
                        )

                        try:
                            output_file.flush()
                        except Exception:
                            pass

                        last_log_at = now

        if extracted_bytes < int(expected_min_bytes or 1):
            raise ValueError(
                f"{label} extracted only {extracted_bytes} bytes; expected at least {expected_min_bytes} bytes."
            )

        os.replace(temp_path, destination_path)

        append_refresh_detail_line(
            f"=== {label}: EXTRACT COMPLETE === {format_download_size(extracted_bytes)}"
        )

        set_refresh_status(
            stage=f"Extracting {label}",
            message=f"{label} extract complete: {format_download_size(extracted_bytes)}.",
        )

        return {
            "extracted": True,
            "path": destination_path,
            "size": extracted_bytes,
        }

    except Exception:
        remove_file_if_exists(temp_path)
        raise

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

    headers = {
        "User-Agent": "iMomir/1.0",
        "Accept": "application/json;q=0.9,*/*;q=0.8",
    }

    download_file_with_retries(
        MTGJSON_ATOMIC_URL,
        ATOMIC_CARDS_PATH,
        headers=headers,
        label="AtomicCards.json",
        force_download=True,
        expected_min_bytes=50 * 1024 * 1024,
        attempts=3,
        connect_timeout=30,
        read_timeout=60,
        chunk_size=1024 * 1024,
    )

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

    headers = {
        "User-Agent": "iMomir/1.0",
        "Accept": "application/json;q=0.9,*/*;q=0.8",
    }

    result = download_file_with_retries(
        MTGJSON_SET_LIST_URL,
        SET_LIST_PATH,
        headers=headers,
        label="SetList.json",
        force_download=force_download,
        expected_min_bytes=100 * 1024,
        attempts=3,
        connect_timeout=30,
        read_timeout=45,
        chunk_size=512 * 1024,
    )

    return {
        "downloaded": result["downloaded"],
        "message": "Downloaded SetList.json successfully." if result["downloaded"] else result["message"],
    }

def download_all_printings_json(force_download=False):
    ensure_download_directories()

    existing_json_is_valid = (
        os.path.exists(ALL_PRINTINGS_PATH)
        and os.path.getsize(ALL_PRINTINGS_PATH) > 100 * 1024 * 1024
    )

    if not force_download and existing_json_is_valid:
        existing_size = os.path.getsize(ALL_PRINTINGS_PATH)
        return {
            "downloaded": False,
            "message": f"Local AllPrintings.json already exists and appears valid ({format_download_size(existing_size)}).",
        }

    set_refresh_status(
        stage="Downloading All Printings",
        message="Downloading AllPrintings.json.gz from MTGJSON...",
        error="",
    )

    headers = {
        "User-Agent": "iMomir/1.0",
        "Accept": "application/gzip,application/octet-stream;q=0.9,*/*;q=0.8",
    }

    append_refresh_detail_line("=== CHAOS DRAFT: BEGIN ALLPRINTINGS DOWNLOAD ===")
    append_refresh_detail_line(f"=== DOWNLOADING ALLPRINTINGS FROM === {MTGJSON_ALL_PRINTINGS_URL}")
    append_refresh_detail_line(f"=== WRITING GZ TO === {ALL_PRINTINGS_GZ_PATH}")

    try:
        download_result = download_file_with_retries(
            MTGJSON_ALL_PRINTINGS_URL,
            ALL_PRINTINGS_GZ_PATH,
            headers=headers,
            label="AllPrintings.json.gz",
            force_download=True,
            expected_min_bytes=50 * 1024 * 1024,
            attempts=4,
            connect_timeout=30,
            read_timeout=60,
            chunk_size=1024 * 1024,
        )
    except Exception as exc:
        append_refresh_detail_line(f"=== ALLPRINTINGS DOWNLOAD FAILED === {str(exc)}")

        if existing_json_is_valid:
            existing_size = os.path.getsize(ALL_PRINTINGS_PATH)
            append_refresh_detail_line(
                f"=== ALLPRINTINGS FALLBACK === Using existing AllPrintings.json ({format_download_size(existing_size)})"
            )

            return {
                "downloaded": False,
                "fallback_used": True,
                "message": f"AllPrintings download failed, but existing AllPrintings.json appears valid ({format_download_size(existing_size)}).",
            }

        raise

    gz_size = os.path.getsize(ALL_PRINTINGS_GZ_PATH) if os.path.exists(ALL_PRINTINGS_GZ_PATH) else 0
    append_refresh_detail_line(f"=== ALLPRINTINGS GZ SIZE === {format_download_size(gz_size)}")

    extract_result = extract_gzip_file_with_progress(
        ALL_PRINTINGS_GZ_PATH,
        ALL_PRINTINGS_PATH,
        label="AllPrintings.json",
        expected_min_bytes=100 * 1024 * 1024,
        chunk_size=1024 * 1024,
    )

    json_size = os.path.getsize(ALL_PRINTINGS_PATH) if os.path.exists(ALL_PRINTINGS_PATH) else 0
    append_refresh_detail_line(f"=== ALLPRINTINGS JSON SIZE === {format_download_size(json_size)}")

    return {
        "downloaded": True,
        "message": (
            "Downloaded and extracted AllPrintings.json successfully. "
            f"GZ={format_download_size(download_result['size'])}; "
            f"JSON={format_download_size(extract_result['size'])}."
        ),
    }

def download_chaos_booster_csvs(force_download=False):
    ensure_download_directories()

    files_to_download = [
        (MTGJSON_SET_BOOSTER_CONTENTS_URL, SET_BOOSTER_CONTENTS_CSV_PATH, "setBoosterContents.csv", 10 * 1024),
        (MTGJSON_SET_BOOSTER_CONTENT_WEIGHTS_URL, SET_BOOSTER_CONTENT_WEIGHTS_CSV_PATH, "setBoosterContentWeights.csv", 10 * 1024),
        (MTGJSON_SET_BOOSTER_SHEET_CARDS_URL, SET_BOOSTER_SHEET_CARDS_CSV_PATH, "setBoosterSheetCards.csv", 1 * 1024 * 1024),
        (MTGJSON_SET_BOOSTER_SHEETS_URL, SET_BOOSTER_SHEETS_CSV_PATH, "setBoosterSheets.csv", 10 * 1024),
    ]

    headers = {
        "User-Agent": "iMomir/1.0",
        "Accept": "text/csv,application/octet-stream;q=0.9,*/*;q=0.8",
    }

    for url, file_path, label, expected_min_bytes in files_to_download:
        download_file_with_retries(
            url,
            file_path,
            headers=headers,
            label=label,
            force_download=force_download,
            expected_min_bytes=expected_min_bytes,
            attempts=3,
            connect_timeout=30,
            read_timeout=45,
            chunk_size=512 * 1024,
        )

def safe_list(value):
    if isinstance(value, list):
        return value
    return []


def safe_dict(value):
    if isinstance(value, dict):
        return value
    return {}

def safe_int_or_none(value):
    if value is None or value == "":
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def safe_float_or_none(value):
    if value is None or value == "":
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None

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

def get_effective_chaos_scryfall_image_quality():
    try:
        if has_request_context():
            config = get_request_config()
        else:
            config = get_config()
    except Exception:
        config = {}

    quality_value = (config.get("chaos_scryfall_image_quality") or "png").strip().lower()
    if quality_value not in {"normal", "large", "png"}:
        quality_value = "png"

    return quality_value


def get_scryfall_quality_download_order(quality_preference):
    quality_preference = (quality_preference or "png").strip().lower()

    if quality_preference == "normal":
        return ["normal"]

    if quality_preference == "large":
        return ["large", "normal"]

    return ["png", "large", "normal"]


def build_scryfall_image_url(scryfall_id, image_quality="normal"):
    return build_scryfall_face_image_url(scryfall_id, "front", image_quality=image_quality)


def build_scryfall_face_image_url(scryfall_id, face_side, image_quality="normal"):
    if not scryfall_id or len(scryfall_id) < 2:
        return None

    normalized_side = (face_side or "").strip().lower()
    if normalized_side not in {"front", "back"}:
        normalized_side = "front"

    normalized_quality = (image_quality or "normal").strip().lower()
    if normalized_quality not in {"normal", "large", "png"}:
        normalized_quality = "normal"

    file_ext = "png" if normalized_quality == "png" else "jpg"

    return (
        f"https://cards.scryfall.io/"
        f"{normalized_quality}/{normalized_side}/{scryfall_id[0]}/{scryfall_id[1]}/{scryfall_id}.{file_ext}"
    )


def parse_scryfall_image_url(image_url):
    raw_url = (image_url or "").strip()
    if not raw_url:
        return None

    match = re.match(
        r"^https?://cards\.scryfall\.io/(?P<quality>[^/]+)/(?P<side>front|back)/[^/]+/[^/]+/(?P<scryfall_id>[A-Za-z0-9-]+)\.(?P<ext>jpg|jpeg|png)$",
        raw_url,
        re.IGNORECASE,
    )

    if not match:
        return None

    return {
        "quality": (match.group("quality") or "").strip().lower(),
        "side": (match.group("side") or "").strip().lower(),
        "scryfall_id": (match.group("scryfall_id") or "").strip(),
        "ext": (match.group("ext") or "").strip().lower(),
    }


def build_scryfall_candidate_image_urls(image_url, quality_preference=None):
    raw_url = (image_url or "").strip()
    if not raw_url:
        return []

    parsed = parse_scryfall_image_url(raw_url)
    if not parsed:
        return [raw_url]

    if not quality_preference:
        quality_preference = get_effective_chaos_scryfall_image_quality()

    candidate_urls = []
    seen_urls = set()

    for quality_value in get_scryfall_quality_download_order(quality_preference):
        candidate_url = build_scryfall_face_image_url(
            parsed["scryfall_id"],
            parsed["side"],
            image_quality=quality_value,
        )

        if candidate_url and candidate_url not in seen_urls:
            candidate_urls.append(candidate_url)
            seen_urls.add(candidate_url)

    if raw_url not in seen_urls:
        candidate_urls.append(raw_url)

    return candidate_urls

def extract_chaos_card_faces(card_obj, uuid_lookup):
    card_name = (card_obj.get("name") or "").strip()
    layout = (card_obj.get("layout") or "").strip().lower()
    side = (card_obj.get("side") or "").strip().lower()
    identifiers = safe_dict(card_obj.get("identifiers"))
    parent_scryfall_id = (identifiers.get("scryfallId") or "").strip()

    face_payloads = []

    current_uuid = (card_obj.get("uuid") or "").strip()
    other_face_ids = safe_list(card_obj.get("otherFaceIds"))

    if side == "a" and other_face_ids:
        for other_face_uuid in other_face_ids:
            other_face_uuid = (other_face_uuid or "").strip()
            if not other_face_uuid:
                continue

            other_face_obj = uuid_lookup.get(other_face_uuid)
            if not isinstance(other_face_obj, dict):
                continue

            other_side = (other_face_obj.get("side") or "").strip().lower()
            if other_side != "b":
                continue

            other_identifiers = safe_dict(other_face_obj.get("identifiers"))
            other_scryfall_id = (other_identifiers.get("scryfallId") or "").strip()

            front_face_name = (card_obj.get("faceName") or "").strip()
            back_face_name = (other_face_obj.get("faceName") or "").strip()

            if not front_face_name or not back_face_name:
                continue

            shared_scryfall_id = parent_scryfall_id or other_scryfall_id

            face_payloads = [
                {
                    "face_index": 0,
                    "side": "a",
                    "uuid": current_uuid,
                    "name": front_face_name,
                    "type_line": card_obj.get("type") or "",
                    "mana_cost": card_obj.get("manaCost"),
                    "layout": layout,
                    "scryfall_id": parent_scryfall_id,
                    "image_url": build_scryfall_face_image_url(shared_scryfall_id, "front") if shared_scryfall_id else None,
                },
                {
                    "face_index": 1,
                    "side": "b",
                    "uuid": other_face_uuid,
                    "name": back_face_name,
                    "type_line": other_face_obj.get("type") or "",
                    "mana_cost": other_face_obj.get("manaCost"),
                    "layout": layout,
                    "scryfall_id": other_scryfall_id,
                    "image_url": build_scryfall_face_image_url(shared_scryfall_id, "back") if shared_scryfall_id else None,
                },
            ]
            break

    if layout in {"transform", "modal_dfc", "battle", "double_faced_token"}:
        front_url = ""
        back_url = ""

        if len(face_payloads) >= 1:
            front_url = face_payloads[0].get("image_url") or ""

        if len(face_payloads) >= 2:
            back_url = face_payloads[1].get("image_url") or ""

        write_debug_log(
            f"CHAOS FACE EXTRACT | name={card_name} | layout={layout} | side={side} | "
            f"other_face_count={len(other_face_ids)} | extracted_faces={len(face_payloads)} | "
            f"front_url={front_url} | back_url={back_url}"
        )

    return face_payloads


def is_dual_faced_layout(layout_value):
    layout_value = (layout_value or "").strip().lower()

    return layout_value in {
        "transform",
        "modal_dfc",
        "battle",
        "double_faced_token",
    }

def safe_filename(value):
    allowed = []
    for ch in (value or ""):
        if ch.isalnum() or ch in ("-", "_", "."):
            allowed.append(ch)
        else:
            allowed.append("_")
    return "".join(allowed).strip("_") or "card"

def get_silhouette_edge_border_pixels():
    try:
        parsed_value = int(SILHOUETTE_EDGE_BORDER_PIXELS)
    except Exception:
        parsed_value = 1

    if parsed_value < 0:
        parsed_value = 0

    return parsed_value


def get_silhouette_horizontal_border_mm():
    border_pixels = get_silhouette_edge_border_pixels()
    return (border_pixels / SILHOUETTE_RENDER_TARGET_WIDTH_PX) * SILHOUETTE_LETTER_CARD_WIDTH_MM


def get_silhouette_vertical_border_mm():
    border_pixels = get_silhouette_edge_border_pixels()
    return (border_pixels / SILHOUETTE_RENDER_TARGET_HEIGHT_PX) * SILHOUETTE_LETTER_CARD_HEIGHT_MM

def build_chaos_cached_image_filename(card_uuid, page_kind, face_name, image_url):
    uuid_part = safe_filename(card_uuid or "card")
    page_part = safe_filename(page_kind or "single")
    face_part = safe_filename(face_name or "face")

    file_ext = ".jpg"
    lower_url = (image_url or "").strip().lower()

    if lower_url.endswith(".png"):
        file_ext = ".png"
    elif lower_url.endswith(".jpg") or lower_url.endswith(".jpeg"):
        file_ext = ".jpg"

    parsed = parse_scryfall_image_url(image_url)
    quality_part = parsed["quality"] if parsed else "source"

    url_hash = hashlib.md5((image_url or "").encode("utf-8")).hexdigest()[:10]

    return f"{uuid_part}_{page_part}_{face_part}_{quality_part}_{url_hash}{file_ext}"


def get_chaos_cached_image_paths(card_uuid, page_kind, face_name, image_url):
    filename = build_chaos_cached_image_filename(card_uuid, page_kind, face_name, image_url)
    abs_path = os.path.join(CHAOS_IMAGE_CACHE_DIR, filename)
    rel_path = os.path.join("data", "chaos_image_cache", filename).replace("\\", "/")

    return {
        "filename": filename,
        "absolute_path": abs_path,
        "relative_path": rel_path,
    }


def download_chaos_image_to_cache(card_uuid, page_kind, face_name, image_url):
    if not image_url:
        return None

    ensure_download_directories()

    headers = {
        "User-Agent": "iMomir/1.0",
        "Accept": "*/*",
    }

    candidate_urls = build_scryfall_candidate_image_urls(image_url)
    last_exception = None

    for candidate_url in candidate_urls:
        cache_paths = get_chaos_cached_image_paths(card_uuid, page_kind, face_name, candidate_url)
        abs_path = cache_paths["absolute_path"]

        if os.path.exists(abs_path):
            write_debug_log(
                f"CHAOS IMAGE CACHE HIT | card_uuid={card_uuid} | page_kind={page_kind} | "
                f"face_name={face_name} | file={cache_paths['filename']} | url={candidate_url}"
            )
            return cache_paths

        try:
            response = requests.get(candidate_url, headers=headers, timeout=120)
            response.raise_for_status()

            with open(abs_path, "wb") as file_handle:
                file_handle.write(response.content)

            write_debug_log(
                f"CHAOS IMAGE CACHE MISS | card_uuid={card_uuid} | page_kind={page_kind} | "
                f"face_name={face_name} | downloaded={cache_paths['filename']} | url={candidate_url}"
            )

            return cache_paths

        except requests.exceptions.RequestException as exc:
            last_exception = exc

            write_debug_log(
                f"CHAOS IMAGE DOWNLOAD ATTEMPT FAILED | card_uuid={card_uuid} | page_kind={page_kind} | "
                f"face_name={face_name} | url={candidate_url} | error={str(exc)}"
            )

            continue

    if last_exception:
        raise last_exception

    return None

def try_download_chaos_image_to_cache(card_uuid, page_kind, face_name, image_url, context_label="CHAOS IMAGE"):
    try:
        return download_chaos_image_to_cache(
            card_uuid,
            page_kind,
            face_name,
            image_url,
        )
    except requests.exceptions.HTTPError as exc:
        response = getattr(exc, "response", None)
        status_code = getattr(response, "status_code", None)

        write_debug_log(
            f"{context_label} DOWNLOAD HTTP ERROR | "
            f"card_uuid={card_uuid} | page_kind={page_kind} | face_name={face_name} | "
            f"status={status_code} | url={image_url} | error={str(exc)}"
        )

        return None
    except requests.exceptions.RequestException as exc:
        write_debug_log(
            f"{context_label} DOWNLOAD REQUEST ERROR | "
            f"card_uuid={card_uuid} | page_kind={page_kind} | face_name={face_name} | "
            f"url={image_url} | error={str(exc)}"
        )

        return None
    except Exception as exc:
        write_debug_log(
            f"{context_label} DOWNLOAD ERROR | "
            f"card_uuid={card_uuid} | page_kind={page_kind} | face_name={face_name} | "
            f"url={image_url} | error={str(exc)}"
        )

        return None

def get_two_card_borderless_slots_mm():
    # 3.5" x 5" portrait page
    # Two rotated cards, stacked vertically, no margins.
    return [
        {
            "x_mm": 0.0,
            "y_mm": 63.5,
            "width_mm": 88.9,
            "height_mm": 63.5,
            "rotation_degrees": 90,
        },
        {
            "x_mm": 0.0,
            "y_mm": 0.0,
            "width_mm": 88.9,
            "height_mm": 63.5,
            "rotation_degrees": 90,
        },
    ]

def get_silhouette_letter_horizontal_8_slots_mm():
    horizontal_border_mm = get_silhouette_horizontal_border_mm()
    vertical_border_mm = get_silhouette_vertical_border_mm()

    horizontal_step_mm = SILHOUETTE_LETTER_CARD_WIDTH_MM + (horizontal_border_mm * 2)
    vertical_step_mm = SILHOUETTE_LETTER_CARD_HEIGHT_MM + (vertical_border_mm * 2)

    slot_defs = []

    for row_index in range(SILHOUETTE_LETTER_ROWS):
        for column_index in range(SILHOUETTE_LETTER_COLUMNS):
            display_row_index = (SILHOUETTE_LETTER_ROWS - 1) - row_index

            art_x_mm = SILHOUETTE_LETTER_START_X_MM + (column_index * horizontal_step_mm)
            art_y_mm = SILHOUETTE_LETTER_START_Y_MM + (display_row_index * vertical_step_mm)

            slot_defs.append({
                "x_mm": art_x_mm - horizontal_border_mm,
                "y_mm": art_y_mm - vertical_border_mm,
                "width_mm": SILHOUETTE_LETTER_CARD_WIDTH_MM + (horizontal_border_mm * 2),
                "height_mm": SILHOUETTE_LETTER_CARD_HEIGHT_MM + (vertical_border_mm * 2),
                "rotation_degrees": 0,
            })

    write_debug_log(
        f"SILHOUETTE LETTER LAYOUT | border_pixels={get_silhouette_edge_border_pixels()} | "
        f"horizontal_border_mm={horizontal_border_mm:.6f} | vertical_border_mm={vertical_border_mm:.6f} | "
        f"horizontal_step_mm={horizontal_step_mm:.6f} | vertical_step_mm={vertical_step_mm:.6f}"
    )

    return slot_defs

def build_pdf_image_reader_from_bytes(image_bytes, print_mode):
    with Image.open(BytesIO(image_bytes)) as source_image:
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
            image = ImageEnhance.Contrast(image).enhance(1.25)
            image = ImageEnhance.Brightness(image).enhance(1.07)

            def highlight_boost(p):
                if p > 200:
                    return min(255, int(p + (255 - p) * 0.7))
                return p

            image = image.point(highlight_boost)

            def contrast_curve(p):
                return int((p - 128) * 1.1 + 128)

            image = image.point(contrast_curve)

        image_buffer = BytesIO()
        image.save(image_buffer, format="PNG")
        image_buffer.seek(0)
        return ImageReader(image_buffer)

def add_duplicated_edge_border(image, border_pixels=None):
    if border_pixels is None:
        border_pixels = get_silhouette_edge_border_pixels()

    if border_pixels <= 0:
        return image

    source_image = image.convert("RGB")
    source_width, source_height = source_image.size

    if source_width <= 0 or source_height <= 0:
        return source_image

    expanded_image = Image.new(
        "RGB",
        (source_width + (border_pixels * 2), source_height + (border_pixels * 2))
    )

    # Paste the original image in the center.
    expanded_image.paste(source_image, (border_pixels, border_pixels))

    # Duplicate top and bottom rows.
    top_row = source_image.crop((0, 0, source_width, 1)).resize((source_width, border_pixels), Image.NEAREST)
    bottom_row = source_image.crop((0, source_height - 1, source_width, source_height)).resize((source_width, border_pixels), Image.NEAREST)

    expanded_image.paste(top_row, (border_pixels, 0))
    expanded_image.paste(bottom_row, (border_pixels, border_pixels + source_height))

    # Duplicate left and right columns.
    left_column = source_image.crop((0, 0, 1, source_height)).resize((border_pixels, source_height), Image.NEAREST)
    right_column = source_image.crop((source_width - 1, 0, source_width, source_height)).resize((border_pixels, source_height), Image.NEAREST)

    expanded_image.paste(left_column, (0, border_pixels))
    expanded_image.paste(right_column, (border_pixels + source_width, border_pixels))

    # Duplicate corners.
    top_left_pixel = source_image.crop((0, 0, 1, 1)).resize((border_pixels, border_pixels), Image.NEAREST)
    top_right_pixel = source_image.crop((source_width - 1, 0, source_width, 1)).resize((border_pixels, border_pixels), Image.NEAREST)
    bottom_left_pixel = source_image.crop((0, source_height - 1, 1, source_height)).resize((border_pixels, border_pixels), Image.NEAREST)
    bottom_right_pixel = source_image.crop((source_width - 1, source_height - 1, source_width, source_height)).resize((border_pixels, border_pixels), Image.NEAREST)

    expanded_image.paste(top_left_pixel, (0, 0))
    expanded_image.paste(top_right_pixel, (border_pixels + source_width, 0))
    expanded_image.paste(bottom_left_pixel, (0, border_pixels + source_height))
    expanded_image.paste(bottom_right_pixel, (border_pixels + source_width, border_pixels + source_height))

    return expanded_image

def mm_to_px(mm_value, pixels_per_mm):
    return max(1, int(round(float(mm_value) * float(pixels_per_mm))))


def apply_rounded_corner_mask(image, radius_px, matte_rgb=(0, 0, 0)):
    if radius_px <= 0:
        return image.convert("RGB")

    source_rgb = image.convert("RGB")
    width, height = source_rgb.size

    mask = Image.new("L", (width, height), 0)
    mask_draw = ImageDraw.Draw(mask)
    mask_draw.rounded_rectangle(
        [(0, 0), (width - 1, height - 1)],
        radius=radius_px,
        fill=255,
    )

    # Composite onto a SOLID matte to eliminate any white/gray alpha fringe.
    matte_image = Image.new("RGB", (width, height), matte_rgb)
    matte_image.paste(source_rgb, (0, 0), mask)

    return matte_image


def build_white_blank_card_image(width_px, height_px, radius_px=0):
    base_image = Image.new("RGB", (width_px, height_px), (255, 255, 255))

    return base_image.convert("RGB")

def get_processed_card_image_bytes(image_path, print_mode):
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
            image = ImageEnhance.Contrast(image).enhance(1.25)
            image = ImageEnhance.Brightness(image).enhance(1.07)

            def highlight_boost(p):
                if p > 200:
                    return min(255, int(p + (255 - p) * 0.7))
                return p

            image = image.point(highlight_boost)

            def contrast_curve(p):
                return int((p - 128) * 1.1 + 128)

            image = image.point(contrast_curve)

        output_buffer = BytesIO()
        image.save(output_buffer, format="PNG")
        return output_buffer.getvalue()

def draw_processed_image_into_two_card_slot(pdf_canvas, image_path, print_mode, slot_def):
    processed_image_bytes = get_processed_card_image_bytes(image_path, print_mode)

    with Image.open(BytesIO(processed_image_bytes)) as source_image:
        rotated_image = source_image.convert("RGB").transpose(Image.Transpose.ROTATE_270)

        rotated_buffer = BytesIO()
        rotated_image.save(rotated_buffer, format="PNG")
        rotated_buffer.seek(0)

        slot_reader = ImageReader(rotated_buffer)

    pdf_canvas.drawImage(
        slot_reader,
        slot_def["x_mm"] * mm,
        slot_def["y_mm"] * mm,
        width=slot_def["width_mm"] * mm,
        height=slot_def["height_mm"] * mm,
        preserveAspectRatio=False,
        mask="auto",
    )

def draw_pdf_background_image(pdf_canvas, image_path, page_width_mm, page_height_mm):
    background_reader = ImageReader(image_path)

    pdf_canvas.drawImage(
        background_reader,
        0,
        0,
        width=page_width_mm * mm,
        height=page_height_mm * mm,
        preserveAspectRatio=False,
        mask="auto",
    )

def draw_processed_image_into_slot(
    pdf_canvas,
    image_path,
    print_mode,
    slot_def,
    add_edge_bleed_border=False,
    rounded_corner_radius_mm=0.0,
    blank_white_card=False,
):
    slot_width_mm = float(slot_def["width_mm"])
    slot_height_mm = float(slot_def["height_mm"])

    target_width_px = mm_to_px(slot_width_mm, 12)
    target_height_px = mm_to_px(slot_height_mm, 12)
    radius_px = mm_to_px(rounded_corner_radius_mm, 12) if rounded_corner_radius_mm and rounded_corner_radius_mm > 0 else 0

    if blank_white_card:
        image = build_white_blank_card_image(
            target_width_px,
            target_height_px,
            radius_px=radius_px,
        )
    else:
        processed_image_bytes = get_processed_card_image_bytes(image_path, print_mode)

        with Image.open(BytesIO(processed_image_bytes)) as source_image:
            image = source_image.convert("RGB")

            if add_edge_bleed_border:
                image = add_duplicated_edge_border(image)

            rotation_degrees = int(slot_def.get("rotation_degrees", 0) or 0)
            if rotation_degrees == 90:
                image = image.transpose(Image.Transpose.ROTATE_270)
            elif rotation_degrees == 180:
                image = image.transpose(Image.Transpose.ROTATE_180)
            elif rotation_degrees == 270:
                image = image.transpose(Image.Transpose.ROTATE_90)

            image = image.resize((target_width_px, target_height_px), Image.LANCZOS)

            if radius_px > 0:
                image = apply_rounded_corner_mask(image, radius_px, matte_rgb=(0, 0, 0))
            else:
                image = image.convert("RGB")

    slot_buffer = BytesIO()
    image.convert("RGB").save(slot_buffer, format="PNG")
    slot_buffer.seek(0)

    slot_reader = ImageReader(slot_buffer)

    pdf_canvas.drawImage(
        slot_reader,
        slot_def["x_mm"] * mm,
        slot_def["y_mm"] * mm,
        width=slot_width_mm * mm,
        height=slot_height_mm * mm,
        preserveAspectRatio=False,
        mask="auto",
    )

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

def build_card_corner_label_text(rendered_entry, pack_tracking_code=None, print_front_back_label=False):
    label_parts = []

    tracking_code = (pack_tracking_code or "").strip().upper()
    if tracking_code:
        label_parts.append(tracking_code)

    if print_front_back_label and int(rendered_entry.get("is_dual_faced") or 0) == 1:
        page_kind = (rendered_entry.get("page_kind") or "").strip().lower()

        if page_kind == "front":
            label_parts.append("FRONT")
        elif page_kind == "back":
            label_parts.append("BACK")

    return " - ".join(label_parts).strip()

def build_pdf_rendered_entry_with_template(rendered_entry, pack_tracking_code=None, print_front_back_label=False):
    card_row = rendered_entry.get("card_row")
    card_uuid = (rendered_entry.get("card_uuid") or "").strip()
    page_kind = (rendered_entry.get("page_kind") or "").strip().lower()

    if not card_row or not card_uuid:
        return rendered_entry

    label_text = build_card_corner_label_text(
        rendered_entry,
        pack_tracking_code=pack_tracking_code,
        print_front_back_label=print_front_back_label,
    )

    rendered_temp_path = get_chaos_rendered_pdf_image_temp_path(
        card_uuid,
        page_kind,
        label_text,
    )

    build_chaos_template_rendered_card_image(
        rendered_entry["temp_path"],
        rendered_temp_path,
        label_text,
        card_row,
        template_key_override=rendered_entry.get("export_frame_template") or "auto",
    )

    updated_entry = dict(rendered_entry)
    updated_entry["source_temp_path"] = rendered_entry["temp_path"]
    updated_entry["temp_path"] = rendered_temp_path
    updated_entry["is_persistent_cache_file"] = False
    updated_entry["is_template_rendered"] = True

    return updated_entry

def draw_card_corner_label(pdf_canvas, slot_x_mm, slot_y_mm, slot_width_mm, slot_height_mm, label_text):
    label_text = (label_text or "").strip().upper()
    if not label_text:
        return

    # Lower-left label box. Black background, white text.

    label_margin_left_mm = 3.3
    label_margin_bottom_mm = 0.4

    box_x_mm = float(slot_x_mm) #+ label_margin_left_mm
    box_y_mm = float(slot_y_mm) #+ label_margin_bottom_mm
    box_height_mm = 4.3

    # Wide enough for FIN.C.26D1 - FRONT, but capped so it does not dominate the card.
    # box_width_mm = min(float(slot_width_mm) * 0.62, 36.0)
    # box_width_mm = min(float(slot_width_mm) * 0.46, 36.0)
    box_width_mm = 31.7

    pdf_canvas.setFillColorRGB(18 / 255.0, 12 / 255.0, 12 / 255.0)
    pdf_canvas.rect(
        box_x_mm * mm,
        box_y_mm * mm,
        box_width_mm * mm,
        box_height_mm * mm,
        fill=1,
        stroke=0,
    )

    font_name = "Helvetica-Bold"
    font_size = 6.4

    pdf_canvas.setFillColorRGB(1, 1, 1)
    pdf_canvas.setFont(font_name, font_size)

    max_text_width_pts = (box_width_mm * mm) - (1.4 * mm)
    text_to_draw = label_text

    while text_to_draw and pdf_canvas.stringWidth(text_to_draw, font_name, font_size) > max_text_width_pts:
        text_to_draw = text_to_draw[:-1].rstrip()

    if text_to_draw != label_text and len(text_to_draw) > 1:
        text_to_draw = text_to_draw[:-1].rstrip() + "…"

    text_x_pts = (box_x_mm * mm) + (0.7 * mm) + (label_margin_left_mm * mm)
    text_y_pts = (box_y_mm * mm) + (1.35 * mm) + (label_margin_bottom_mm * mm)

    pdf_canvas.drawString(text_x_pts, text_y_pts, text_to_draw)

def build_single_image_pdf_buffer(image_path):
    pdf_settings = resolve_pdf_print_settings()
    pdf_template_layout = resolve_pdf_template_layout()
    print_settings = resolve_print_settings()

    width_mm = pdf_template_layout["page_width_mm"]
    height_mm = pdf_template_layout["page_height_mm"]
    crop_border = pdf_settings["pdf_crop_border"]

    buffer = BytesIO()
    pdf_canvas = canvas.Canvas(buffer, pagesize=(width_mm * mm, height_mm * mm))

    draw_x_mm = pdf_template_layout["draw_x_mm"]
    draw_y_mm = pdf_template_layout["draw_y_mm"]
    draw_width_mm = pdf_template_layout["draw_width_mm"]
    draw_height_mm = pdf_template_layout["draw_height_mm"]

    if crop_border and not pdf_template_layout["uses_fixed_inner_margin"]:
        crop_left_right_mm = width_mm * 0.05
        crop_top_bottom_mm = height_mm * 0.034

        draw_x_mm = -crop_left_right_mm
        draw_y_mm = -crop_top_bottom_mm
        draw_width_mm = width_mm + (crop_left_right_mm * 2)
        draw_height_mm = height_mm + (crop_top_bottom_mm * 2)

    if (
        pdf_template_layout.get("is_multi_card_layout", False)
        and pdf_template_layout["print_template"] == "borderless-3p5x5-two-card"
    ):
        slot_defs = get_two_card_borderless_slots_mm()

        for slot in slot_defs:
            draw_processed_image_into_two_card_slot(
                pdf_canvas,
                image_path,
                print_settings["print_mode"],
                slot,
            )
    else:
        pdf_image_reader = build_pdf_image_reader(image_path, print_settings["print_mode"])

        pdf_canvas.drawImage(
            pdf_image_reader,
            draw_x_mm * mm,
            draw_y_mm * mm,
            width=draw_width_mm * mm,
            height=draw_height_mm * mm,
            preserveAspectRatio=False,
            mask="auto",
        )

    pdf_canvas.showPage()
    pdf_canvas.save()
    buffer.seek(0)

    return buffer

def build_default_card_back_sheet_pdf():
    pdf_settings = resolve_pdf_print_settings()
    pdf_template_layout = resolve_pdf_template_layout()
    print_settings = resolve_print_settings()

    width_mm = pdf_template_layout["page_width_mm"]
    height_mm = pdf_template_layout["page_height_mm"]
    crop_border = pdf_settings["pdf_crop_border"]

    draw_x_mm = pdf_template_layout["draw_x_mm"]
    draw_y_mm = pdf_template_layout["draw_y_mm"]
    draw_width_mm = pdf_template_layout["draw_width_mm"]
    draw_height_mm = pdf_template_layout["draw_height_mm"]

    if crop_border and not pdf_template_layout["uses_fixed_inner_margin"]:
        crop_left_right_mm = width_mm * 0.05
        crop_top_bottom_mm = height_mm * 0.034

        draw_x_mm = -crop_left_right_mm
        draw_y_mm = -crop_top_bottom_mm
        draw_width_mm = width_mm + (crop_left_right_mm * 2)
        draw_height_mm = height_mm + (crop_top_bottom_mm * 2)

    mtg_back_path = os.path.join(app.static_folder, "img", "mtg_back.jpg")

    if not os.path.exists(mtg_back_path):
        raise FileNotFoundError(f"Default MTG back image was not found: {mtg_back_path}")

    # Eight entries gives one full Silhouette Letter sheet.
    # For single-card templates, this intentionally generates 8 card-back pages.
    rendered_back_entries = []

    for back_index in range(8):
        rendered_back_entries.append({
            "temp_path": mtg_back_path,
            "page_kind": "back",
            "card_uuid": f"default_back_{back_index + 1}",
            "card_row": None,
            "is_dual_faced": 0,
            "is_persistent_cache_file": True,
            "is_template_rendered": False,
        })

    buffer = BytesIO()
    pdf_canvas = canvas.Canvas(buffer, pagesize=(width_mm * mm, height_mm * mm))

    pages_rendered = draw_chaos_rendered_entries_into_pdf_layout(
        pdf_canvas,
        rendered_back_entries,
        pdf_template_layout,
        print_settings,
        width_mm,
        height_mm,
        draw_x_mm,
        draw_y_mm,
        draw_width_mm,
        draw_height_mm,
    )

    if pages_rendered == 0:
        raise ValueError("No default card back images could be rendered into the PDF.")

    pdf_canvas.save()
    buffer.seek(0)

    return buffer

def get_set_name_for_custom_title_sheet(set_code):
    clean_set_code = (set_code or "").strip().upper()

    if not clean_set_code:
        return ""

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT set_name
        FROM sets
        WHERE set_code = ?
        """,
        (clean_set_code,),
    )

    row = cursor.fetchone()
    conn.close()

    if row and row["set_name"]:
        return row["set_name"]

    return ""


def get_custom_title_sheet_pack_type_options():
    options = []
    seen_values = set()

    for item in CHAOS_PACK_TYPE_OPTIONS:
        raw_value = (item.get("value") or "").strip().lower()
        raw_label = (item.get("label") or "").strip()

        if not raw_value:
            continue

        label = raw_label or " ".join(word.capitalize() for word in raw_value.split("_"))

        if "booster" not in label.lower():
            label = f"{label} Booster"

        if raw_value not in seen_values:
            options.append({
                "value": raw_value,
                "label": label,
            })
            seen_values.add(raw_value)

    extra_options = [
        ("chaos_draft", "Chaos Draft Booster"),
        ("secret_lair", "Secret Lair Booster"),
        ("cube", "Cube Booster"),
        ("custom", "Custom Booster"),
    ]

    for value, label in extra_options:
        if value not in seen_values:
            options.append({
                "value": value,
                "label": label,
            })
            seen_values.add(value)

    return options


def get_custom_title_sheet_pack_type_label(pack_type_value):
    clean_value = (pack_type_value or "").strip().lower()

    for option in get_custom_title_sheet_pack_type_options():
        if option["value"] == clean_value:
            return option["label"]

    return "Custom Booster"


def get_custom_title_sheet_default_footer_text(set_code, pack_type_value):
    clean_set_code = (set_code or "").strip().upper() or "CUSTOM"
    clean_pack_type = (pack_type_value or "").strip().lower()

    type_code_lookup = {
        "collector": "C",
        "draft": "D",
        "play": "P",
        "set": "S",
        "jumpstart": "J",
        "chaos_draft": "CD",
        "secret_lair": "SL",
        "cube": "CB",
        "custom": "CU",
    }

    type_code = type_code_lookup.get(clean_pack_type, "O")
    return f"{clean_set_code}.{type_code}.TITLE"


def build_custom_title_sheet_pdf(set_code, pack_name, pack_type_value, custom_text):
    clean_set_code = (set_code or "").strip().upper()
    clean_pack_name = (pack_name or "").strip()
    clean_custom_text = (custom_text or "").strip()

    if not clean_set_code:
        raise ValueError("Set Code is required.")

    if not clean_pack_name:
        clean_pack_name = get_set_name_for_custom_title_sheet(clean_set_code) or clean_set_code

    pack_type_label = get_custom_title_sheet_pack_type_label(pack_type_value)
    footer_text = clean_custom_text

    pack_display_name = normalize_chaos_pack_display_name(
        f"{clean_pack_name} - {pack_type_label} ({clean_set_code})"
    )

    pdf_settings = resolve_pdf_print_settings()
    pdf_template_layout = resolve_pdf_template_layout()
    print_settings = resolve_print_settings()

    width_mm = pdf_template_layout["page_width_mm"]
    height_mm = pdf_template_layout["page_height_mm"]
    crop_border = pdf_settings["pdf_crop_border"]

    draw_x_mm = pdf_template_layout["draw_x_mm"]
    draw_y_mm = pdf_template_layout["draw_y_mm"]
    draw_width_mm = pdf_template_layout["draw_width_mm"]
    draw_height_mm = pdf_template_layout["draw_height_mm"]

    if crop_border and not pdf_template_layout["uses_fixed_inner_margin"]:
        crop_left_right_mm = width_mm * 0.05
        crop_top_bottom_mm = height_mm * 0.034

        draw_x_mm = -crop_left_right_mm
        draw_y_mm = -crop_top_bottom_mm
        draw_width_mm = width_mm + (crop_left_right_mm * 2)
        draw_height_mm = height_mm + (crop_top_bottom_mm * 2)

    rendered_title_entries = []

    try:
        for title_index in range(8):
            title_card_bytes = build_chaos_pack_title_card_image_bytes(
                pack_display_name,
                set_code=clean_set_code,
                booster_name=pack_type_label,
                pack_tracking_code=footer_text,
                icon_fallback_set_code="P16",
            )

            title_temp_filename = (
                f"custom_title_sheet_{safe_filename(clean_set_code)}_"
                f"{safe_filename(pack_type_value)}_{title_index + 1}.png"
            )
            title_temp_path = get_chaos_temp_file_path(title_temp_filename)

            with open(title_temp_path, "wb") as title_file:
                title_file.write(title_card_bytes)

            rendered_title_entries.append({
                "temp_path": title_temp_path,
                "page_kind": "title",
                "card_uuid": "",
                "card_row": None,
                "is_dual_faced": 0,
                "is_persistent_cache_file": False,
                "is_template_rendered": False,
            })

        buffer = BytesIO()
        pdf_canvas = canvas.Canvas(buffer, pagesize=(width_mm * mm, height_mm * mm))

        pages_rendered = draw_chaos_rendered_entries_into_pdf_layout(
            pdf_canvas,
            rendered_title_entries,
            pdf_template_layout,
            print_settings,
            width_mm,
            height_mm,
            draw_x_mm,
            draw_y_mm,
            draw_width_mm,
            draw_height_mm,
        )

        if pages_rendered == 0:
            raise ValueError("No custom title sheet images could be rendered into the PDF.")

        pdf_canvas.save()
        buffer.seek(0)

        return buffer

    finally:
        for rendered_entry in rendered_title_entries:
            try:
                temp_path = rendered_entry.get("temp_path")
                if temp_path and os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                pass

def build_inline_pdf_response(pdf_buffer, filename):
    safe_name = (filename or "document.pdf").strip()
    if not safe_name.lower().endswith(".pdf"):
        safe_name = f"{safe_name}.pdf"

    return Response(
        pdf_buffer,
        mimetype="application/pdf",
        headers={
            "Content-Disposition": f"inline; filename={safe_name}"
        }
    )

def render_print_page(card, image_src):
    print_settings = resolve_print_settings()

    return render_template(
        "print.html",
        card=card,
        image_src=image_src,
        print_mode=print_settings["print_mode"],
        print_template=print_settings["print_template"],
        print_width=print_settings["print_width"],
        print_height=print_settings["print_height"],
        sheet_width=print_settings["sheet_width"],
        sheet_height=print_settings["sheet_height"],
        sheet_offset_x=print_settings["sheet_offset_x"],
        sheet_offset_y=print_settings["sheet_offset_y"],
    )

def get_pack_label_set_icon_path(set_code, fallback_set_code=None):
    clean_set_code = (set_code or "").strip().upper()
    clean_fallback_set_code = (fallback_set_code or "").strip().upper()

    if not clean_set_code and clean_fallback_set_code:
        clean_set_code = clean_fallback_set_code
        clean_fallback_set_code = ""

    if not clean_set_code:
        return ""

    # Prefer database-cached local path if the set-icon sync has populated it.
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT local_icon_svg_path
            FROM sets
            WHERE set_code = ?
            """,
            (clean_set_code,),
        )

        row = cursor.fetchone()
        conn.close()

        if row and row["local_icon_svg_path"]:
            candidate_path = os.path.join(
                app.static_folder,
                str(row["local_icon_svg_path"]).replace("/", os.sep),
            )

            if os.path.exists(candidate_path):
                return candidate_path

    except Exception:
        pass

    fallback_path = os.path.join(
        app.static_folder,
        "img",
        "set_icons",
        f"{safe_filename(clean_set_code)}.svg",
    )

    if os.path.exists(fallback_path):
        return fallback_path

    if clean_fallback_set_code and clean_fallback_set_code != clean_set_code:
        return get_pack_label_set_icon_path(clean_fallback_set_code)

    return ""


def get_pack_label_signature_path(template_config=None):
    template_config = template_config or {}

    requested_filename = (template_config.get("signature_logo_path") or "iMomir_sig_1.png").strip()

    # Only allow a filename, not a path traversal or arbitrary filesystem path.
    requested_filename = os.path.basename(requested_filename)

    if not requested_filename:
        requested_filename = "iMomir_sig_1.png"

    requested_signature_path = os.path.join(
        app.static_folder,
        "img",
        requested_filename,
    )

    if os.path.exists(requested_signature_path):
        return requested_signature_path

    fallback_signature_path = os.path.join(
        app.static_folder,
        "img",
        "iMomir_sig_1.png",
    )

    if os.path.exists(fallback_signature_path):
        return fallback_signature_path

    return ""


def create_vertical_gradient(width, height, top_rgb, mid_rgb, bottom_rgb):
    gradient = Image.new("RGB", (width, height), top_rgb)
    draw = ImageDraw.Draw(gradient)

    top_rgb = tuple(top_rgb)
    mid_rgb = tuple(mid_rgb)
    bottom_rgb = tuple(bottom_rgb)

    for y in range(height):
        position = y / max(1, height - 1)

        if position <= 0.5:
            local_position = position / 0.5
            start_rgb = top_rgb
            end_rgb = mid_rgb
        else:
            local_position = (position - 0.5) / 0.5
            start_rgb = mid_rgb
            end_rgb = bottom_rgb

        row_rgb = tuple(
            int(start_rgb[channel] + ((end_rgb[channel] - start_rgb[channel]) * local_position))
            for channel in range(3)
        )

        draw.line((0, y, width, y), fill=row_rgb)

    return gradient


def load_title_card_font(preferred_size, bold=True):
    font_names = []

    if bold:
        font_names.extend([
            "arialbd.ttf",
            "Arial Bold.ttf",
            "DejaVuSans-Bold.ttf",
        ])
    else:
        font_names.extend([
            "arial.ttf",
            "Arial.ttf",
            "DejaVuSans.ttf",
        ])

    for font_name in font_names:
        try:
            return ImageFont.truetype(font_name, preferred_size)
        except Exception:
            pass

    return ImageFont.load_default()


def wrap_text_for_pixel_width(draw, text_value, font, max_width_px):
    words = (text_value or "").strip().split()

    if not words:
        return []

    lines = []
    current_line = ""

    for word in words:
        test_line = word if not current_line else f"{current_line} {word}"
        bbox = draw.textbbox((0, 0), test_line, font=font)
        test_width = bbox[2] - bbox[0]

        if test_width <= max_width_px or not current_line:
            current_line = test_line
        else:
            lines.append(current_line)
            current_line = word

    if current_line:
        lines.append(current_line)

    return lines


def draw_centered_text_lines(draw, lines, center_x, start_y, font, fill, line_spacing_px):
    current_y = start_y

    for line in lines:
        bbox = draw.textbbox((0, 0), line, font=font)
        line_width = bbox[2] - bbox[0]
        line_height = bbox[3] - bbox[1]
        draw.text(
            (center_x - (line_width / 2), current_y),
            line,
            fill=fill,
            font=font,
        )
        current_y += line_height + line_spacing_px

    return current_y


def render_svg_icon_to_tinted_image(svg_path, target_size_px, tint_rgb, opacity=64):
    svg_path = (svg_path or "").strip()

    if not svg_path or not os.path.exists(svg_path):
        return None

    try:
        import cairosvg
    except Exception:
        write_debug_log("PACK LABEL SVG RENDER SKIPPED | cairosvg is not installed")
        return None

    try:
        png_bytes = cairosvg.svg2png(
            url=svg_path,
            output_width=int(target_size_px),
            output_height=int(target_size_px),
        )

        with Image.open(BytesIO(png_bytes)) as icon_source:
            icon_rgba = icon_source.convert("RGBA")

        alpha = icon_rgba.getchannel("A")

        opacity = max(0, min(255, int(opacity)))
        if opacity < 255:
            alpha = alpha.point(lambda pixel: int(pixel * (opacity / 255.0)))

        tinted_icon = Image.new(
            "RGBA",
            icon_rgba.size,
            tuple(tint_rgb) + (0,),
        )
        tinted_icon.putalpha(alpha)

        return tinted_icon

    except Exception as exc:
        write_debug_log(f"PACK LABEL SVG RENDER FAILED | path={svg_path} | error={str(exc)}")
        return None


def paste_centered_rgba(base_image, overlay_image, center_x, center_y):
    if not overlay_image:
        return base_image

    overlay = overlay_image.convert("RGBA")
    left = int(center_x - (overlay.width / 2))
    top = int(center_y - (overlay.height / 2))

    base_image.paste(overlay, (left, top), overlay)

    return base_image

def apply_alpha_opacity(alpha_channel, opacity):
    opacity = max(0, min(255, int(opacity or 0)))

    if opacity >= 255:
        return alpha_channel

    return alpha_channel.point(lambda pixel: int(pixel * (opacity / 255.0)))


def build_gold_shimmer_overlay_from_alpha(alpha_channel, base_rgb, shimmer_rgb, opacity=120, blur_px=0):
    source_alpha = apply_alpha_opacity(alpha_channel, opacity)

    width, height = source_alpha.size
    shimmer_image = Image.new("RGBA", (width, height), tuple(base_rgb) + (0,))
    shimmer_pixels = shimmer_image.load()
    alpha_pixels = source_alpha.load()

    base_rgb = tuple(base_rgb)
    shimmer_rgb = tuple(shimmer_rgb)

    for y in range(height):
        diagonal_position = (y / max(1, height - 1))

        for x in range(width):
            alpha_value = alpha_pixels[x, y]
            if alpha_value <= 0:
                continue

            x_position = x / max(1, width - 1)

            # Static diagonal shimmer bands. Not animated; this is baked into the rendered image.
            band_value = ((x_position * 1.45) + (diagonal_position * 0.85)) % 1.0

            if 0.17 <= band_value <= 0.30 or 0.58 <= band_value <= 0.68:
                mix = 0.72
            elif 0.30 < band_value <= 0.38 or 0.68 < band_value <= 0.75:
                mix = 0.35
            else:
                mix = 0.0

            pixel_rgb = (
                int(base_rgb[0] + ((shimmer_rgb[0] - base_rgb[0]) * mix)),
                int(base_rgb[1] + ((shimmer_rgb[1] - base_rgb[1]) * mix)),
                int(base_rgb[2] + ((shimmer_rgb[2] - base_rgb[2]) * mix)),
            )

            shimmer_pixels[x, y] = pixel_rgb + (alpha_value,)

    blur_px = int(blur_px or 0)
    if blur_px > 0:
        shimmer_image = shimmer_image.filter(ImageFilter.GaussianBlur(radius=blur_px))

    return shimmer_image


def draw_pack_label_glow_rounded_rectangle(
    image,
    box,
    radius_px,
    glow_rgb,
    glow_opacity,
    blur_px,
    line_width_px,
    spread_px=0,
):
    glow_opacity = int(glow_opacity or 0)

    if glow_opacity <= 0:
        return image

    spread_px = max(0, int(spread_px or 0))
    blur_px = max(0, int(blur_px or 0))
    line_width_px = max(1, int(line_width_px or 1))

    left, top, right, bottom = box

    expanded_box = (
        int(left - spread_px),
        int(top - spread_px),
        int(right + spread_px),
        int(bottom + spread_px),
    )

    glow_layer = Image.new("RGBA", image.size, (0, 0, 0, 0))
    glow_draw = ImageDraw.Draw(glow_layer)

    glow_draw.rounded_rectangle(
        expanded_box,
        radius=max(0, int(radius_px + spread_px)),
        outline=tuple(glow_rgb) + (glow_opacity,),
        width=line_width_px,
    )

    if blur_px > 0:
        glow_layer = glow_layer.filter(ImageFilter.GaussianBlur(radius=blur_px))

    image.alpha_composite(glow_layer)

    return image


def draw_pack_label_icon_shimmer(
    image,
    icon_image,
    center_x,
    center_y,
    base_rgb,
    shimmer_rgb,
    shimmer_opacity,
    shimmer_blur_px,
    offset_x_px=0,
    offset_y_px=0,
):
    if not icon_image:
        return image

    shimmer_opacity = int(shimmer_opacity or 0)

    if shimmer_opacity <= 0:
        return image

    icon_rgba = icon_image.convert("RGBA")
    icon_alpha = icon_rgba.getchannel("A")

    shimmer_image = build_gold_shimmer_overlay_from_alpha(
        icon_alpha,
        base_rgb=base_rgb,
        shimmer_rgb=shimmer_rgb,
        opacity=shimmer_opacity,
        blur_px=shimmer_blur_px,
    )

    left = int(center_x - (shimmer_image.width / 2) + int(offset_x_px or 0))
    top = int(center_y - (shimmer_image.height / 2) + int(offset_y_px or 0))

    image.alpha_composite(shimmer_image, (left, top))

    return image

def draw_pack_label_decorative_border(image, width, height, template_config):
    outer_radius = int(min(width, height) * float(template_config.get("outer_corner_radius_pct") or 0.055))
    inner_inset = int(min(width, height) * float(template_config.get("inner_border_inset_pct") or 0.025))

    border_rgb = tuple(template_config.get("border_rgb") or (70, 70, 80))
    inner_border_rgb = tuple(template_config.get("inner_border_rgb") or border_rgb)

    outer_box = (4, 4, width - 5, height - 5)
    inner_box = (inner_inset, inner_inset, width - inner_inset - 1, height - inner_inset - 1)
    outer_line_width = max(4, int(width * 0.008))
    inner_line_width = max(1, int(width * 0.003))

    if bool(template_config.get("card_glow_enabled", False)):
        draw_pack_label_glow_rounded_rectangle(
            image,
            outer_box,
            outer_radius,
            glow_rgb=tuple(template_config.get("card_glow_rgb") or border_rgb),
            glow_opacity=int(template_config.get("card_glow_opacity") or 0),
            blur_px=int(template_config.get("card_glow_blur_px") or 0),
            line_width_px=int(template_config.get("card_glow_line_width_px") or outer_line_width),
            spread_px=int(template_config.get("card_glow_spread_px") or 0),
        )

    if bool(template_config.get("border_shimmer_enabled", False)):
        shimmer_rgb = tuple(template_config.get("border_shimmer_rgb") or border_rgb)
        shimmer_opacity = int(template_config.get("border_shimmer_opacity") or 0)
        shimmer_blur_px = int(template_config.get("border_shimmer_blur_px") or 0)
        shimmer_line_width_px = int(template_config.get("border_shimmer_line_width_px") or outer_line_width)
        shimmer_spread_px = int(template_config.get("border_shimmer_spread_px") or 0)

        draw_pack_label_glow_rounded_rectangle(
            image,
            outer_box,
            outer_radius,
            glow_rgb=shimmer_rgb,
            glow_opacity=shimmer_opacity,
            blur_px=shimmer_blur_px,
            line_width_px=shimmer_line_width_px,
            spread_px=shimmer_spread_px,
        )

        draw_pack_label_glow_rounded_rectangle(
            image,
            inner_box,
            max(1, outer_radius - inner_inset),
            glow_rgb=shimmer_rgb,
            glow_opacity=max(0, int(shimmer_opacity * 0.70)),
            blur_px=max(0, int(shimmer_blur_px * 0.70)),
            line_width_px=max(1, int(shimmer_line_width_px * 0.65)),
            spread_px=max(0, int(shimmer_spread_px * 0.50)),
        )

    draw = ImageDraw.Draw(image)

    draw.rounded_rectangle(
        outer_box,
        radius=outer_radius,
        outline=border_rgb,
        width=outer_line_width,
    )

    draw.rounded_rectangle(
        inner_box,
        radius=max(1, outer_radius - inner_inset),
        outline=inner_border_rgb,
        width=inner_line_width,
    )

    return image


def draw_pack_label_footer(draw, width, height, pack_tracking_code, template_config):
    clean_pack_code = (pack_tracking_code or "").strip().upper()

    if not clean_pack_code:
        return

    footer_fill_rgb = tuple(template_config.get("footer_fill_rgb") or (16, 16, 22))
    footer_outline_rgb = tuple(template_config.get("footer_outline_rgb") or (80, 80, 90))
    pack_code_rgb = tuple(template_config.get("pack_code_rgb") or (245, 245, 250))

    footer_height = int(height * 0.102)
    footer_width = int(width * 0.76)
    footer_left = int((width - footer_width) / 2)
    footer_top = int(height * 0.845)
    footer_right = footer_left + footer_width
    footer_bottom = footer_top + footer_height

    footer_radius = int(footer_height * 0.35)

    draw.rounded_rectangle(
        [(footer_left, footer_top), (footer_right, footer_bottom)],
        radius=footer_radius,
        fill=footer_fill_rgb,
        outline=footer_outline_rgb,
        width=max(1, int(width * 0.003)),
    )

    font_size = int(height * 0.035)
    font = load_title_card_font(font_size, bold=True)

    max_text_width = int(footer_width * 0.88)
    text_to_draw = clean_pack_code

    while text_to_draw:
        bbox = draw.textbbox((0, 0), text_to_draw, font=font)
        text_width = bbox[2] - bbox[0]

        if text_width <= max_text_width:
            break

        text_to_draw = text_to_draw[:-1].rstrip()

    if text_to_draw != clean_pack_code and len(text_to_draw) > 1:
        text_to_draw = text_to_draw[:-1].rstrip() + "…"

    bbox = draw.textbbox((0, 0), text_to_draw, font=font)
    text_width = bbox[2] - bbox[0]
    text_height = bbox[3] - bbox[1]

    draw.text(
        (
            (width - text_width) / 2,
            footer_top + ((footer_height - text_height) / 2) - int(height * 0.002),
        ),
        text_to_draw,
        fill=pack_code_rgb,
        font=font,
    )

def split_chaos_pack_display_name_for_title(pack_display_name):
    raw_value = (pack_display_name or "").strip()

    if not raw_value:
        return ("Booster Pack", "")

    if " - " in raw_value:
        left_part, right_part = raw_value.split(" - ", 1)
        set_name = (left_part or "").strip()
        booster_name = (right_part or "").strip()

        return (set_name or "Booster Pack", booster_name)

    return (raw_value, "")

def get_chaos_temp_file_path(filename):
    ensure_download_directories()
    safe_name = safe_filename(filename)
    return os.path.join(CHAOS_TEMP_CACHE_DIR, safe_name)

def get_chaos_rendered_pdf_image_temp_path(card_uuid, page_kind, label_text):
    label_part = safe_filename(label_text or "nolabel")
    filename = f"chaos_pdf_rendered_{safe_filename(card_uuid)}_{safe_filename(page_kind)}_{label_part}.jpg"
    return get_chaos_temp_file_path(filename)

def build_chaos_pack_image_title_card_bytes(set_code, booster_name, card_width_mm=63.5, card_height_mm=88.9):
    booster_key = normalize_chaos_booster_key(booster_name)

    set_code_variants = []
    raw_set_code = (set_code or "").strip()

    if raw_set_code:
        set_code_variants.append(raw_set_code)
        if raw_set_code.upper() not in set_code_variants:
            set_code_variants.append(raw_set_code.upper())
        if raw_set_code.lower() not in set_code_variants:
            set_code_variants.append(raw_set_code.lower())

    image_abs_path = None

    for set_code_variant in set_code_variants:
        candidate_relpath = f"img/pack_art/{set_code_variant}/{booster_key}.png"
        candidate_abs_path = os.path.join(app.static_folder, candidate_relpath.replace("/", os.sep))

        if os.path.exists(candidate_abs_path):
            image_abs_path = candidate_abs_path
            break

    if not image_abs_path:
        return None

    with Image.open(image_abs_path) as source_image:
        image = source_image.convert("RGB")

        source_width, source_height = image.size
        crop_amount_x = int(round(source_width * 0.05))

        left = crop_amount_x
        top = 0
        right = source_width - crop_amount_x
        bottom = source_height

        if right <= left:
            return None

        cropped_image = image.crop((left, top, right, bottom))

        target_width_px = 762
        target_height_px = 1067

        # COVER behavior:
        # preserve aspect ratio, scale to fill the target card shape,
        # then crop overflow instead of stretching the image.
        fitted_image = ImageOps.fit(
            cropped_image,
            (target_width_px, target_height_px),
            method=Image.LANCZOS,
            centering=(0.5, 0.5),
        )

        output_buffer = BytesIO()
        fitted_image.save(output_buffer, format="PNG")
        return output_buffer.getvalue()

def get_configured_print_pack_labels():
    try:
        if has_request_context():
            config = get_request_config()
        else:
            config = get_config()
    except Exception:
        config = {}

    return (config.get("print_pack_labels") or "0").strip() == "1"


def draw_chaos_pack_label_pdf_page(
    pdf_canvas,
    page_width_mm,
    page_height_mm,
    pack_display_name,
    set_code=None,
    booster_name=None,
    pack_tracking_code="",
):
    label_card_bytes = build_chaos_pack_title_card_image_bytes(
        pack_display_name,
        set_code=set_code,
        booster_name=booster_name,
        pack_tracking_code=pack_tracking_code,
        card_width_mm=page_width_mm,
        card_height_mm=page_height_mm,
    )

    label_reader = ImageReader(BytesIO(label_card_bytes))

    pdf_canvas.drawImage(
        label_reader,
        0,
        0,
        width=page_width_mm * mm,
        height=page_height_mm * mm,
        preserveAspectRatio=False,
        mask="auto",
    )

    pdf_canvas.showPage()


def save_chaos_pack_label_image_file(
    output_path,
    pack_display_name,
    set_code=None,
    booster_name=None,
    pack_tracking_code="",
    card_width_mm=63.0,
    card_height_mm=88.0,
):
    label_card_bytes = build_chaos_pack_title_card_image_bytes(
        pack_display_name,
        set_code=set_code,
        booster_name=booster_name,
        pack_tracking_code=pack_tracking_code,
        card_width_mm=card_width_mm,
        card_height_mm=card_height_mm,
    )

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with Image.open(BytesIO(label_card_bytes)) as label_image:
        label_image.convert("RGB").save(
            output_path,
            format="JPEG",
            quality=95,
            optimize=True,
        )

    return output_path

def build_chaos_pack_label_rendered_entry(
    pack_display_name,
    set_code=None,
    booster_name=None,
    pack_tracking_code="",
    label_suffix="label",
):
    pack_label_bytes = build_chaos_pack_title_card_image_bytes(
        pack_display_name,
        set_code=set_code,
        booster_name=booster_name,
        pack_tracking_code=pack_tracking_code,
    )

    pack_label_temp_filename = (
        f"chaos_pack_label_{safe_filename(pack_display_name)}"
        f"_{safe_filename(pack_tracking_code or label_suffix or 'label')}.png"
    )
    pack_label_temp_path = get_chaos_temp_file_path(pack_label_temp_filename)

    with open(pack_label_temp_path, "wb") as pack_label_file:
        pack_label_file.write(pack_label_bytes)

    return {
        "temp_path": pack_label_temp_path,
        "page_kind": "title",
        "card_uuid": "",
        "card_row": None,
        "is_dual_faced": 0,
        "is_persistent_cache_file": False,
        "is_template_rendered": False,
    }


def draw_chaos_rendered_entries_into_pdf_layout(
    pdf_canvas,
    rendered_image_entries,
    pdf_template_layout,
    print_settings,
    width_mm,
    height_mm,
    draw_x_mm,
    draw_y_mm,
    draw_width_mm,
    draw_height_mm,
):
    if not rendered_image_entries:
        return 0

    pages_rendered = 0

    if pdf_template_layout["print_template"] == "silhouette-letter-horizontal-8":
        background_abs_path = os.path.join(app.static_folder, "sil", "SIL_LETTER_HORIZONTAL.png")

        if not os.path.exists(background_abs_path):
            raise FileNotFoundError(f"Silhouette background not found: {background_abs_path}")

        slot_defs = get_silhouette_letter_horizontal_8_slots_mm()

        for page_start_index in range(0, len(rendered_image_entries), 8):
            page_entries = rendered_image_entries[page_start_index:page_start_index + 8]

            draw_pdf_background_image(
                pdf_canvas,
                background_abs_path,
                width_mm,
                height_mm,
            )

            for slot_index, rendered_entry in enumerate(page_entries):
                slot_def = slot_defs[slot_index]

                draw_processed_image_into_slot(
                    pdf_canvas,
                    rendered_entry["temp_path"],
                    print_settings["print_mode"],
                    slot_def,
                    add_edge_bleed_border=True,
                    rounded_corner_radius_mm=SILHOUETTE_CORNER_RADIUS_MM,
                )

            if SILHOUETTE_FILL_UNUSED_SLOTS_WITH_WHITE and len(page_entries) < len(slot_defs):
                for blank_slot_index in range(len(page_entries), len(slot_defs)):
                    draw_processed_image_into_slot(
                        pdf_canvas,
                        image_path=None,
                        print_mode=print_settings["print_mode"],
                        slot_def=slot_defs[blank_slot_index],
                        add_edge_bleed_border=False,
                        rounded_corner_radius_mm=SILHOUETTE_CORNER_RADIUS_MM,
                        blank_white_card=True,
                    )

            pdf_canvas.showPage()
            pages_rendered += 1

        return pages_rendered

    if (
        pdf_template_layout.get("is_multi_card_layout", False)
        and pdf_template_layout["print_template"] == "borderless-3p5x5-two-card"
    ):
        slot_defs = get_two_card_borderless_slots_mm()

        for page_start_index in range(0, len(rendered_image_entries), 2):
            page_entries = rendered_image_entries[page_start_index:page_start_index + 2]

            for slot_index, rendered_entry in enumerate(page_entries):
                slot_def = slot_defs[slot_index]

                draw_processed_image_into_two_card_slot(
                    pdf_canvas,
                    rendered_entry["temp_path"],
                    print_settings["print_mode"],
                    slot_def,
                )

            pdf_canvas.showPage()
            pages_rendered += 1

        return pages_rendered

    for rendered_entry in rendered_image_entries:
        pdf_image_reader = build_pdf_image_reader(
            rendered_entry["temp_path"],
            print_settings["print_mode"],
        )

        pdf_canvas.drawImage(
            pdf_image_reader,
            draw_x_mm * mm,
            draw_y_mm * mm,
            width=draw_width_mm * mm,
            height=draw_height_mm * mm,
            preserveAspectRatio=False,
            mask="auto",
        )

        pdf_canvas.showPage()
        pages_rendered += 1

    return pages_rendered

def build_chaos_pack_title_card_image_bytes(
    pack_display_name,
    set_code=None,
    booster_name=None,
    pack_tracking_code="",
    card_width_mm=63.5,
    card_height_mm=88.9,
    icon_fallback_set_code=None,
):
    normalized_pack_display_name = normalize_chaos_pack_display_name(pack_display_name)
    title_set_name, title_booster_name = split_chaos_pack_display_name_for_title(normalized_pack_display_name)

    clean_set_code = (set_code or "").strip().upper()

    if not clean_set_code:
        match = re.search(r"\(([^()]*)\)\s*$", title_booster_name or "")
        if match:
            clean_set_code = (match.group(1) or "").strip().upper()

    clean_booster_name = (booster_name or title_booster_name or "").strip()
    template_config = get_pack_label_template(clean_booster_name)

    pixels_per_mm = 12
    image_width_px = int(round(card_width_mm * pixels_per_mm))
    image_height_px = int(round(card_height_mm * pixels_per_mm))

    icon_center_y_pct = float(template_config.get("icon_center_y_pct", 0.245))
    title_band_top_pct = float(template_config.get("title_band_top_pct", 0.475))
    title_band_bottom_pct = float(template_config.get("title_band_bottom_pct", 0.592))
    title_center_anchor_pct = float(template_config.get("title_center_anchor_pct", 0.475))
    signature_center_y_pct = float(template_config.get("signature_center_y_pct", 0.725))

    title_box_min_width_pct = float(template_config.get("title_box_min_width_pct", 0.78))
    title_box_pad_x_pct = float(template_config.get("title_box_pad_x_pct", 0.055))
    title_box_pad_top_pct = float(template_config.get("title_box_pad_top_pct", 0.020))
    title_box_pad_bottom_pct = float(template_config.get("title_box_pad_bottom_pct", 0.028))
    title_box_corner_radius_pct = float(template_config.get("title_box_corner_radius_pct", 0.030))
    title_text_gap_pct = float(template_config.get("title_text_gap_pct", 0.010))

    signature_scale_pct = float(template_config.get("signature_scale_pct", 1.00))

    accent_rgb = tuple(template_config.get("accent_rgb") or (180, 180, 185))
    accent_highlight_rgb = tuple(template_config.get("accent_highlight_rgb") or accent_rgb)

    image = create_vertical_gradient(
        image_width_px,
        image_height_px,
        template_config.get("background_top_rgb") or (18, 18, 22),
        template_config.get("background_mid_rgb") or (30, 30, 36),
        template_config.get("background_bottom_rgb") or (10, 10, 14),
    ).convert("RGBA")

    draw_pack_label_decorative_border(
        image,
        image_width_px,
        image_height_px,
        template_config,
    )

    draw = ImageDraw.Draw(image)

    # Large centered set emblem / watermark.
    icon_path = get_pack_label_set_icon_path(
        clean_set_code,
        fallback_set_code=icon_fallback_set_code,
    )
    icon_size_px = int(min(image_width_px, image_height_px) * 0.50)

    set_icon_rgb = tuple(template_config.get("set_icon_rgb") or accent_rgb)

    icon_image = render_svg_icon_to_tinted_image(
        icon_path,
        icon_size_px,
        set_icon_rgb,
        opacity=int(template_config.get("set_icon_opacity") or 58),
    )

    if icon_image:
        icon_center_x = image_width_px / 2
        icon_center_y = image_height_px * icon_center_y_pct

        if bool(template_config.get("set_icon_shimmer_enabled", False)):
            draw_pack_label_icon_shimmer(
                image,
                icon_image,
                icon_center_x,
                icon_center_y,
                base_rgb=set_icon_rgb,
                shimmer_rgb=tuple(template_config.get("set_icon_shimmer_rgb") or set_icon_rgb),
                shimmer_opacity=int(template_config.get("set_icon_shimmer_opacity") or 0),
                shimmer_blur_px=int(template_config.get("set_icon_shimmer_blur_px") or 0),
                offset_x_px=int(image_width_px * float(template_config.get("set_icon_shimmer_offset_x_pct") or 0.0)),
                offset_y_px=int(image_height_px * float(template_config.get("set_icon_shimmer_offset_y_pct") or 0.0)),
            )

        paste_centered_rgba(
            image,
            icon_image,
            icon_center_x,
            icon_center_y,
        )

    draw = ImageDraw.Draw(image)

    title_rgb = tuple(template_config.get("title_rgb") or (255, 255, 255))
    subtitle_rgb = tuple(template_config.get("subtitle_rgb") or (226, 226, 234))

    title_font = load_title_card_font(int(image_height_px * 0.058), bold=True)
    subtitle_font = load_title_card_font(int(image_height_px * 0.034), bold=True)

    usable_text_width = int(image_width_px * 0.78)

    title_lines = wrap_text_for_pixel_width(
        draw,
        title_set_name or "Booster Pack",
        title_font,
        usable_text_width,
    )

    if len(title_lines) > 2:
        title_font = load_title_card_font(int(image_height_px * 0.048), bold=True)
        title_lines = wrap_text_for_pixel_width(
            draw,
            title_set_name or "Booster Pack",
            title_font,
            usable_text_width,
        )[:2]

    subtitle_text = title_booster_name or clean_booster_name or "Booster Pack"
    subtitle_lines = wrap_text_for_pixel_width(
        draw,
        subtitle_text,
        subtitle_font,
        usable_text_width,
    )

    if len(subtitle_lines) > 2:
        subtitle_font = load_title_card_font(int(image_height_px * 0.030), bold=True)
        subtitle_lines = wrap_text_for_pixel_width(
            draw,
            subtitle_text,
            subtitle_font,
            usable_text_width,
        )[:2]

    title_line_spacing_px = int(image_height_px * 0.010)
    subtitle_line_spacing_px = int(image_height_px * 0.006)
    title_text_gap_px = int(image_height_px * title_text_gap_pct)

    def measure_text_lines(lines, font, line_spacing_px):
        if not lines:
            return 0, 0

        max_width_px = 0
        total_height_px = 0

        for index, line in enumerate(lines):
            bbox = draw.textbbox((0, 0), line, font=font)
            line_width_px = bbox[2] - bbox[0]
            line_height_px = bbox[3] - bbox[1]

            max_width_px = max(max_width_px, line_width_px)
            total_height_px += line_height_px

            if index < len(lines) - 1:
                total_height_px += line_spacing_px

        return max_width_px, total_height_px

    title_max_width_px, title_height_px = measure_text_lines(
        title_lines,
        title_font,
        title_line_spacing_px,
    )

    subtitle_max_width_px, subtitle_height_px = measure_text_lines(
        subtitle_lines,
        subtitle_font,
        subtitle_line_spacing_px,
    )

    text_block_width_px = max(title_max_width_px, subtitle_max_width_px)

    text_block_height_px = title_height_px
    if title_lines and subtitle_lines:
        text_block_height_px += title_text_gap_px
    text_block_height_px += subtitle_height_px

    title_box_pad_x_px = int(image_width_px * title_box_pad_x_pct)
    title_box_pad_top_px = int(image_height_px * title_box_pad_top_pct)
    title_box_pad_bottom_px = int(image_height_px * title_box_pad_bottom_pct)

    title_box_width_px = max(
        int(image_width_px * title_box_min_width_pct),
        text_block_width_px + (title_box_pad_x_px * 2),
    )

    title_box_height_px = text_block_height_px + title_box_pad_top_px + title_box_pad_bottom_px

    title_box_left = int((image_width_px - title_box_width_px) / 2)
    title_box_top = int(image_height_px * title_band_top_pct)
    title_box_right = title_box_left + title_box_width_px
    title_box_bottom = title_box_top + title_box_height_px

    title_box_corner_radius_px = int(min(image_width_px, image_height_px) * title_box_corner_radius_pct)

    accent_layer = Image.new("RGBA", (image_width_px, image_height_px), (0, 0, 0, 0))
    accent_draw = ImageDraw.Draw(accent_layer)

    # Base title box
    accent_draw.rounded_rectangle(
        [
            (title_box_left, title_box_top),
            (title_box_right, title_box_bottom),
        ],
        radius=title_box_corner_radius_px,
        fill=accent_rgb + (52,),
        outline=accent_rgb + (120,),
        width=max(1, int(image_width_px * 0.003)),
    )

    # Subtle top shimmer / ripple highlight
    shimmer_height_px = max(6, int(title_box_height_px * 0.28))
    accent_draw.rounded_rectangle(
        [
            (title_box_left + 2, title_box_top + 2),
            (title_box_right - 2, title_box_top + shimmer_height_px),
        ],
        radius=max(1, title_box_corner_radius_px - 2),
        fill=accent_highlight_rgb + (34,),
    )

    image.alpha_composite(accent_layer)
    draw = ImageDraw.Draw(image)

    text_center_x = image_width_px / 2
    current_y = title_box_top + title_box_pad_top_px

    current_y = draw_centered_text_lines(
        draw,
        title_lines,
        text_center_x,
        current_y,
        title_font,
        title_rgb,
        title_line_spacing_px,
    )

    if title_lines and subtitle_lines:
        current_y += title_text_gap_px

    draw_centered_text_lines(
        draw,
        subtitle_lines,
        text_center_x,
        current_y,
        subtitle_font,
        subtitle_rgb,
        subtitle_line_spacing_px,
    )

    # iMomir gold signature.
    signature_path = get_pack_label_signature_path(template_config)

    if signature_path:
        try:
            with Image.open(signature_path) as signature_source:
                signature_image = signature_source.convert("RGBA")

            base_signature_width = int(
                image_width_px * float(template_config.get("signature_max_width_pct") or 0.44)
            )

            signature_scale_pct = max(0.10, min(2.00, float(signature_scale_pct or 1.00)))
            target_signature_width = max(1, int(base_signature_width * signature_scale_pct))

            new_signature_height = int(signature_image.height * (target_signature_width / signature_image.width))
            signature_image = signature_image.resize(
                (target_signature_width, new_signature_height),
                Image.LANCZOS,
            )

            signature_opacity = int(template_config.get("signature_opacity") or 255)
            signature_opacity = max(0, min(255, signature_opacity))

            if signature_opacity < 255:
                sig_alpha = signature_image.getchannel("A")
                sig_alpha = sig_alpha.point(lambda pixel: int(pixel * (signature_opacity / 255.0)))
                signature_image.putalpha(sig_alpha)

            paste_centered_rgba(
                image,
                signature_image,
                image_width_px / 2,
                image_height_px * signature_center_y_pct,
            )

        except Exception as exc:
            write_debug_log(f"PACK LABEL SIGNATURE FAILED | error={str(exc)}")

    draw = ImageDraw.Draw(image)
    draw_pack_label_footer(
        draw,
        image_width_px,
        image_height_px,
        pack_tracking_code,
        template_config,
    )

    output_buffer = BytesIO()
    image.convert("RGB").save(output_buffer, format="PNG")
    return output_buffer.getvalue()

def normalize_alternate_face_kind(face_kind):
    normalized_face_kind = (face_kind or "single").strip().lower()

    if normalized_face_kind not in {"single", "front", "back"}:
        normalized_face_kind = "single"

    return normalized_face_kind


def get_alternate_source_for_card(card_row, face_kind="single"):
    if not card_row:
        return None

    normalized_face_kind = normalize_alternate_face_kind(face_kind)
    row_keys = set(card_row.keys()) if hasattr(card_row, "keys") else set()

    card_uuid = (card_row["card_uuid"] if "card_uuid" in row_keys else "") or ""
    set_code = (card_row["set_code"] if "set_code" in row_keys else "") or ""
    collector_number = (card_row["collector_number"] if "collector_number" in row_keys else "") or ""

    conn = get_db_connection()
    cursor = conn.cursor()

    if card_uuid:
        cursor.execute(
            """
            SELECT *
            FROM alternate_sources
            WHERE is_enabled = 1
              AND card_uuid = ?
              AND face_kind IN (?, 'single')
            ORDER BY
                CASE WHEN face_kind = ? THEN 0 ELSE 1 END,
                alternate_source_id DESC
            LIMIT 1
            """,
            (
                card_uuid,
                normalized_face_kind,
                normalized_face_kind,
            ),
        )

        row = cursor.fetchone()
        if row:
            conn.close()
            return row

    if set_code and collector_number:
        cursor.execute(
            """
            SELECT *
            FROM alternate_sources
            WHERE is_enabled = 1
              AND UPPER(COALESCE(set_code, '')) = UPPER(?)
              AND LOWER(COALESCE(collector_number, '')) = LOWER(?)
              AND face_kind IN (?, 'single')
            ORDER BY
                CASE WHEN face_kind = ? THEN 0 ELSE 1 END,
                alternate_source_id DESC
            LIMIT 1
            """,
            (
                set_code,
                collector_number,
                normalized_face_kind,
                normalized_face_kind,
            ),
        )

        row = cursor.fetchone()
        if row:
            conn.close()
            return row

    conn.close()
    return None


def get_alternate_source_local_absolute_path(alternate_source_row):
    if not alternate_source_row:
        return ""

    local_image_path = (alternate_source_row["local_image_path"] or "").strip()

    if not local_image_path:
        return ""

    if os.path.isabs(local_image_path):
        return local_image_path

    return os.path.abspath(os.path.join(RUNTIME_BASE_DIR, local_image_path))


def build_alternate_source_cache_filename(alternate_source_id, image_url):
    parsed_ext = ".jpg"
    lower_url = (image_url or "").strip().lower()

    if lower_url.endswith(".png"):
        parsed_ext = ".png"
    elif lower_url.endswith(".webp"):
        parsed_ext = ".webp"
    elif lower_url.endswith(".jpeg"):
        parsed_ext = ".jpg"

    return f"alternate_source_{int(alternate_source_id)}{parsed_ext}"


def ensure_alternate_source_cached(alternate_source_row):
    if not alternate_source_row:
        return None

    local_path = get_alternate_source_local_absolute_path(alternate_source_row)

    if local_path and os.path.exists(local_path):
        return {
            "absolute_path": local_path,
            "source": "alternate_source_local",
            "alternate_source_id": int(alternate_source_row["alternate_source_id"]),
        }

    external_url = (alternate_source_row["external_image_url"] or "").strip()
    if not external_url:
        return None

    ensure_download_directories()

    alternate_source_id = int(alternate_source_row["alternate_source_id"])
    filename = build_alternate_source_cache_filename(alternate_source_id, external_url)
    output_path = os.path.join(ALTERNATE_SOURCE_DIR, filename)

    headers = {
        "User-Agent": "iMomir/1.0",
        "Accept": "image/avif,image/webp,image/png,image/jpeg,image/*,*/*;q=0.8",
    }

    response = requests.get(external_url, headers=headers, timeout=180)
    response.raise_for_status()

    with open(output_path, "wb") as output_file:
        output_file.write(response.content)

    relative_path = os.path.relpath(output_path, RUNTIME_BASE_DIR).replace("\\", "/")

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE alternate_sources
        SET local_image_path = ?,
            updated_at_utc = ?
        WHERE alternate_source_id = ?
        """,
        (
            relative_path,
            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            alternate_source_id,
        ),
    )

    conn.commit()
    conn.close()

    return {
        "absolute_path": output_path,
        "source": "alternate_source_external_cached",
        "alternate_source_id": alternate_source_id,
    }


def resolve_card_image_source_for_page(card_row, page_kind, fallback_image_url):
    normalized_page_kind = (page_kind or "single").strip().lower()

    if normalized_page_kind not in {"front", "back"}:
        normalized_page_kind = "single"

    alternate_source = get_alternate_source_for_card(
        card_row,
        face_kind=normalized_page_kind,
    )

    if alternate_source:
        try:
            cached_alternate = ensure_alternate_source_cached(alternate_source)

            if cached_alternate and os.path.exists(cached_alternate["absolute_path"]):
                write_debug_log(
                    f"ALTERNATE SOURCE USED | alternate_source_id={cached_alternate['alternate_source_id']} | "
                    f"card_uuid={card_row['card_uuid']} | page_kind={normalized_page_kind} | path={cached_alternate['absolute_path']}"
                )

                fullbleed_absolute_path = get_alternate_source_fullbleed_absolute_path(alternate_source)

                return {
                    "source_type": "alternate_source",
                    "absolute_path": cached_alternate["absolute_path"],
                    "fullbleed_absolute_path": fullbleed_absolute_path if fullbleed_absolute_path and os.path.exists(fullbleed_absolute_path) else "",
                    "image_url": "",
                    "alternate_source_id": cached_alternate["alternate_source_id"],
                    "export_frame_template": (
                        alternate_source["export_frame_template"] or "auto"
                        if "export_frame_template" in alternate_source.keys()
                        else "auto"
                    ),
                }

        except Exception as exc:
            write_debug_log(
                f"ALTERNATE SOURCE FAILED | card_uuid={card_row['card_uuid']} | "
                f"page_kind={normalized_page_kind} | error={str(exc)} | falling back to Scryfall"
            )

    return {
        "source_type": "scryfall",
        "absolute_path": "",
        "fullbleed_absolute_path": "",
        "image_url": fallback_image_url,
        "alternate_source_id": None,
        "export_frame_template": "auto",
    }

def serialize_alternate_source_row(row):
    if not row:
        return None

    return {
        "alternate_source_id": int(row["alternate_source_id"]),
        "source_name": row["source_name"] or "",
        "source_type": row["source_type"] or "",
        "card_uuid": row["card_uuid"] or "",
        "set_code": row["set_code"] or "",
        "collector_number": row["collector_number"] or "",
        "scryfall_id": row["scryfall_id"] or "",
        "card_name": row["card_name"] or "",
        "face_kind": row["face_kind"] or "single",
        "external_image_url": row["external_image_url"] or "",
        "local_image_path": row["local_image_path"] or "",
        "fullbleed_image_path": row["fullbleed_image_path"] or "" if "fullbleed_image_path" in row.keys() else "",
        "remove_bleed": int(row["remove_bleed"] or 0) == 1 if "remove_bleed" in row.keys() else False,
        "bleed_size_mm": row["bleed_size_mm"] if "bleed_size_mm" in row.keys() else None,
        "export_frame_template": (
            row["export_frame_template"] or "auto"
            if "export_frame_template" in row.keys()
            else "auto"
        ),
        "is_enabled": int(row["is_enabled"] or 0) == 1,
        "priority": int(row["priority"] or 100),
        "notes": row["notes"] or "",
        "created_at_utc": row["created_at_utc"] or "",
        "updated_at_utc": row["updated_at_utc"] or "",
    }


def get_alternate_sources_for_card(card_uuid):
    clean_card_uuid = (card_uuid or "").strip()

    if not clean_card_uuid:
        return []

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM alternate_sources
        WHERE card_uuid = ?
        ORDER BY
            is_enabled DESC,
            priority ASC,
            alternate_source_id DESC
        """,
        (clean_card_uuid,),
    )

    rows = cursor.fetchall()
    conn.close()

    return [
        serialize_alternate_source_row(row)
        for row in rows
    ]


def get_alternate_source_by_id(alternate_source_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM alternate_sources
        WHERE alternate_source_id = ?
        """,
        (int(alternate_source_id),),
    )

    row = cursor.fetchone()
    conn.close()

    return row

CARD_PRINT_WIDTH_MM = 63.0
CARD_PRINT_HEIGHT_MM = 88.0


def get_configured_print_bleed_size_mm():
    try:
        if has_request_context():
            config = get_request_config()
        else:
            config = get_config()
    except Exception:
        config = {}

    try:
        bleed_size_mm = float((config.get("print_bleed_size_mm") or "3.0").strip())
    except (TypeError, ValueError):
        bleed_size_mm = 3.0

    if bleed_size_mm < 0:
        bleed_size_mm = 0.0

    if bleed_size_mm > 10:
        bleed_size_mm = 10.0

    return bleed_size_mm

def get_configured_export_add_bleed():
    try:
        if has_request_context():
            config = get_request_config()
        else:
            config = get_config()
    except Exception:
        config = {}

    return (config.get("export_add_bleed") or "0").strip() == "1"


def get_alternate_source_fullbleed_absolute_path(alternate_source_row):
    if not alternate_source_row:
        return ""

    if "fullbleed_image_path" not in alternate_source_row.keys():
        return ""

    fullbleed_image_path = (alternate_source_row["fullbleed_image_path"] or "").strip()

    if not fullbleed_image_path:
        return ""

    if os.path.isabs(fullbleed_image_path):
        return fullbleed_image_path

    return os.path.abspath(os.path.join(RUNTIME_BASE_DIR, fullbleed_image_path))

def get_image_save_format_from_extension(file_ext):
    normalized_ext = (file_ext or "").strip().lower()

    if normalized_ext == ".png":
        return "PNG"

    if normalized_ext == ".webp":
        return "WEBP"

    return "JPEG"


def sample_bleed_border_rgb(image):
    source_image = image.convert("RGB")
    width, height = source_image.size

    sample_regions = [
        (0.015, 0.015, 0.080, 0.080),
        (0.920, 0.015, 0.985, 0.080),
        (0.015, 0.920, 0.080, 0.985),
        (0.920, 0.920, 0.985, 0.985),
        (0.350, 0.955, 0.650, 0.990),
    ]

    sampled_pixels = []

    for x1, y1, x2, y2 in sample_regions:
        left = max(0, min(width - 1, int(width * x1)))
        top = max(0, min(height - 1, int(height * y1)))
        right = max(left + 1, min(width, int(width * x2)))
        bottom = max(top + 1, min(height, int(height * y2)))

        region = source_image.crop((left, top, right, bottom))
        pixels = list(region.getdata())

        if pixels:
            step = max(1, len(pixels) // 500)
            sampled_pixels.extend(pixels[::step])

    if not sampled_pixels:
        return (18, 12, 12)

    reds = sorted(pixel[0] for pixel in sampled_pixels)
    greens = sorted(pixel[1] for pixel in sampled_pixels)
    blues = sorted(pixel[2] for pixel in sampled_pixels)
    middle = len(sampled_pixels) // 2

    return (
        reds[middle],
        greens[middle],
        blues[middle],
    )


def add_card_bleed(
    image,
    bleed_size_mm=None,
    card_width_mm=CARD_PRINT_WIDTH_MM,
    card_height_mm=CARD_PRINT_HEIGHT_MM,
    bleed_rgb=None,
):
    source_image = image.convert("RGB")

    try:
        bleed_mm = float(bleed_size_mm if bleed_size_mm is not None else get_configured_print_bleed_size_mm())
    except (TypeError, ValueError):
        bleed_mm = 3.0

    if bleed_mm <= 0:
        return source_image

    source_width, source_height = source_image.size

    extra_x = int(round(source_width * (bleed_mm / float(card_width_mm))))
    extra_y = int(round(source_height * (bleed_mm / float(card_height_mm))))

    if extra_x <= 0 and extra_y <= 0:
        return source_image

    if bleed_rgb is None:
        bleed_rgb = sample_bleed_border_rgb(source_image)

    bleed_rgb = tuple(bleed_rgb or (18, 12, 12))

    output_image = Image.new(
        "RGB",
        (
            source_width + (extra_x * 2),
            source_height + (extra_y * 2),
        ),
        bleed_rgb,
    )

    output_image.paste(source_image, (extra_x, extra_y))

    return output_image

def crop_image_to_card_aspect_ratio(image, card_width_mm=CARD_PRINT_WIDTH_MM, card_height_mm=CARD_PRINT_HEIGHT_MM):
    source_image = image.convert("RGB")

    source_width, source_height = source_image.size

    if source_width <= 0 or source_height <= 0:
        return source_image

    target_ratio = float(card_width_mm) / float(card_height_mm)
    source_ratio = float(source_width) / float(source_height)

    # Small tolerance prevents unnecessary 1-pixel crop noise.
    if abs(source_ratio - target_ratio) <= 0.001:
        return source_image

    if source_ratio > target_ratio:
        # Image is too wide. Crop left/right.
        target_width = int(round(source_height * target_ratio))

        if target_width <= 0 or target_width >= source_width:
            return source_image

        left = int((source_width - target_width) / 2)
        right = left + target_width

        return source_image.crop((left, 0, right, source_height))

    # Image is too tall. Crop top/bottom.
    target_height = int(round(source_width / target_ratio))

    if target_height <= 0 or target_height >= source_height:
        return source_image

    top = int((source_height - target_height) / 2)
    bottom = top + target_height

    return source_image.crop((0, top, source_width, bottom))

def remove_card_bleed(image, bleed_size_mm=None, card_width_mm=CARD_PRINT_WIDTH_MM, card_height_mm=CARD_PRINT_HEIGHT_MM):
    source_image = image.convert("RGB")

    try:
        bleed_mm = float(bleed_size_mm if bleed_size_mm is not None else get_configured_print_bleed_size_mm())
    except (TypeError, ValueError):
        bleed_mm = 3.0

    if bleed_mm <= 0:
        return crop_image_to_card_aspect_ratio(
            source_image,
            card_width_mm=card_width_mm,
            card_height_mm=card_height_mm,
        )

    source_width, source_height = source_image.size

    full_width_mm = float(card_width_mm) + (bleed_mm * 2.0)
    full_height_mm = float(card_height_mm) + (bleed_mm * 2.0)

    crop_x = int(round(source_width * (bleed_mm / full_width_mm)))
    crop_y = int(round(source_height * (bleed_mm / full_height_mm)))

    left = crop_x
    top = crop_y
    right = source_width - crop_x
    bottom = source_height - crop_y

    if right <= left or bottom <= top:
        return crop_image_to_card_aspect_ratio(
            source_image,
            card_width_mm=card_width_mm,
            card_height_mm=card_height_mm,
        )

    bleed_removed_image = source_image.crop((left, top, right, bottom))

    return crop_image_to_card_aspect_ratio(
        bleed_removed_image,
        card_width_mm=card_width_mm,
        card_height_mm=card_height_mm,
    )

def save_alternate_source_upload_file(uploaded_file, card_uuid, face_kind, remove_bleed=False, bleed_size_mm=None):
    if not uploaded_file or not uploaded_file.filename:
        return {
            "local_image_path": "",
            "fullbleed_image_path": "",
            "remove_bleed": False,
            "bleed_size_mm": None,
        }

    original_filename = (uploaded_file.filename or "").strip()
    file_ext = os.path.splitext(original_filename)[1].strip().lower()

    if file_ext not in {".png", ".jpg", ".jpeg", ".webp"}:
        raise ValueError("Alternate image upload must be a PNG, JPG, JPEG, or WEBP file.")

    ensure_download_directories()

    fullbleed_dir = os.path.join(ALTERNATE_SOURCE_DIR, "fullbleed")
    os.makedirs(fullbleed_dir, exist_ok=True)

    safe_uuid = safe_filename(card_uuid or "card")
    safe_face = safe_filename(face_kind or "single")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")

    output_filename = f"alternate_{safe_uuid}_{safe_face}_{timestamp}{file_ext}"
    output_path = os.path.join(ALTERNATE_SOURCE_DIR, output_filename)

    fullbleed_relative_path = ""

    if remove_bleed:
        fullbleed_filename = f"fullbleed_{safe_uuid}_{safe_face}_{timestamp}{file_ext}"
        fullbleed_output_path = os.path.join(fullbleed_dir, fullbleed_filename)

        uploaded_file.save(fullbleed_output_path)

        with Image.open(fullbleed_output_path) as source_image:
            processed_image = remove_card_bleed(
                source_image,
                bleed_size_mm=bleed_size_mm,
            )

            save_format = get_image_save_format_from_extension(file_ext)

            if save_format == "JPEG":
                processed_image = processed_image.convert("RGB")
                processed_image.save(output_path, format=save_format, quality=95, optimize=True)
            else:
                processed_image.save(output_path, format=save_format)

        fullbleed_relative_path = os.path.relpath(fullbleed_output_path, RUNTIME_BASE_DIR).replace("\\", "/")

    else:
        uploaded_file.save(output_path)

        # Validate the uploaded image can be opened before accepting it.
        with Image.open(output_path) as test_image:
            test_image.verify()

    return {
        "local_image_path": os.path.relpath(output_path, RUNTIME_BASE_DIR).replace("\\", "/"),
        "fullbleed_image_path": fullbleed_relative_path,
        "remove_bleed": bool(remove_bleed),
        "bleed_size_mm": float(bleed_size_mm) if bleed_size_mm is not None else None,
    }


def create_alternate_source_for_card(
    card_uuid,
    source_name,
    source_type,
    face_kind,
    external_image_url="",
    local_image_path="",
    fullbleed_image_path="",
    remove_bleed=False,
    bleed_size_mm=None,
    export_frame_template="auto",
    priority=100,
    notes="",
):
    card_row = get_chaos_card_by_uuid(card_uuid)

    if not card_row:
        raise ValueError("Card UUID was not found in chaos_cards.")

    clean_source_name = (source_name or "").strip()
    if not clean_source_name:
        clean_source_name = "Manual Alternate Source"

    clean_source_type = (source_type or "").strip().lower()
    if clean_source_type not in {"external_url", "uploaded_file", "local_file"}:
        clean_source_type = "external_url"

    clean_face_kind = normalize_alternate_face_kind(face_kind)

    clean_export_frame_template = (export_frame_template or "auto").strip().lower()
    valid_export_frame_templates = {
        option["value"]
        for option in get_card_export_template_options()
    }

    if clean_export_frame_template not in valid_export_frame_templates:
        clean_export_frame_template = "auto"

    try:
        parsed_priority = int(priority)
    except (TypeError, ValueError):
        parsed_priority = 100

    clean_external_url = (external_image_url or "").strip()
    clean_local_path = (local_image_path or "").strip()

    if clean_source_type == "external_url" and not clean_external_url:
        raise ValueError("External URL is required for external_url alternate sources.")

    if clean_source_type in {"uploaded_file", "local_file"} and not clean_local_path:
        raise ValueError("A local image path is required for uploaded_file/local_file alternate sources.")

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO alternate_sources (
            source_name,
            source_type,
            card_uuid,
            set_code,
            collector_number,
            scryfall_id,
            card_name,
            face_kind,
            external_image_url,
            local_image_path,
            fullbleed_image_path,
            remove_bleed,
            bleed_size_mm,
            export_frame_template,
            is_enabled,
            priority,
            notes,
            created_at_utc,
            updated_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            clean_source_name,
            clean_source_type,
            card_uuid,
            card_row["set_code"],
            card_row["collector_number"],
            card_row["scryfall_id"],
            card_row["card_name"],
            clean_face_kind,
            clean_external_url,
            clean_local_path,
            (fullbleed_image_path or "").strip(),
            1 if remove_bleed else 0,
            float(bleed_size_mm) if bleed_size_mm is not None else None,
            clean_export_frame_template,
            1,
            parsed_priority,
            (notes or "").strip(),
            now_utc,
            now_utc,
        ),
    )

    alternate_source_id = cursor.lastrowid

    conn.commit()
    conn.close()

    return int(alternate_source_id)

def get_chaos_card_by_uuid(card_uuid):
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

    row = cursor.fetchone()
    conn.close()
    return row


def parse_faces_json(raw_value):
    if not raw_value:
        return []

    try:
        parsed = json.loads(raw_value)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


def build_chaos_print_pages_for_card(card_row):
    if not card_row:
        return []

    write_debug_log(
        f"CHAOS PDF EXPAND | card_name={card_row['card_name']} | "
        f"layout={card_row['layout']} | is_dual_faced={card_row['is_dual_faced']} | "
        f"face_count={card_row['face_count']}"
    )

    pages = []

    card_uuid = (card_row["card_uuid"] or "").strip()

    front_image_url = (card_row["front_image_url"] or "").strip()
    back_image_url = (card_row["back_image_url"] or "").strip()
    image_url = (card_row["image_url"] or "").strip()

    front_face_name = (card_row["front_face_name"] or card_row["card_name"] or "").strip()
    back_face_name = (card_row["back_face_name"] or "").strip()

    is_dual_faced = int(card_row["is_dual_faced"] or 0) == 1

    if is_dual_faced and front_image_url and back_image_url:
        write_debug_log(
            f"CHAOS PDF EXPAND | dual-face direct urls | card_name={card_row['card_name']} | "
            f"front_face={front_face_name} | back_face={back_face_name}"
        )

        pages.append({
            "page_kind": "front",
            "image_url": front_image_url,
            "face_name": front_face_name,
            "card_name": card_row["card_name"],
            "card_uuid": card_uuid,
        })
        pages.append({
            "page_kind": "back",
            "image_url": back_image_url,
            "face_name": back_face_name or f"{front_face_name} (Back)",
            "card_name": card_row["card_name"],
            "card_uuid": card_uuid,
        })
        return pages

    faces = parse_faces_json(card_row["faces_json"])
    if int(card_row["face_count"] or 0) >= 2 and len(faces) >= 2:
        first_face_url = (faces[0].get("image_url") or "").strip()
        second_face_url = (faces[1].get("image_url") or "").strip()

        if first_face_url and second_face_url:
            write_debug_log(
                f"CHAOS PDF EXPAND | dual-face faces_json fallback | card_name={card_row['card_name']} | "
                f"front_face={faces[0].get('name')} | back_face={faces[1].get('name')}"
            )

            pages.append({
                "page_kind": "front",
                "image_url": first_face_url,
                "face_name": faces[0].get("name") or card_row["card_name"],
                "card_name": card_row["card_name"],
                "card_uuid": card_uuid,
            })
            pages.append({
                "page_kind": "back",
                "image_url": second_face_url,
                "face_name": faces[1].get("name") or f"{card_row['card_name']} (Back)",
                "card_name": card_row["card_name"],
                "card_uuid": card_uuid,
            })
            return pages

    if image_url:
        write_debug_log(
            f"CHAOS PDF EXPAND | single-page fallback | card_name={card_row['card_name']} | image_url_present=yes"
        )

        pages.append({
            "page_kind": "single",
            "image_url": image_url,
            "face_name": front_face_name or card_row["card_name"],
            "card_name": card_row["card_name"],
            "card_uuid": card_uuid,
        })

    return pages

def build_chaos_pack_pdf(
    cards,
    pack_display_name,
    set_code=None,
    booster_name=None,
    pack_tracking_code=None,
    include_pack_labels=True,
    title_card_only=False,
    pack_label_states=None,
):
    pdf_settings = resolve_pdf_print_settings()
    pdf_template_layout = resolve_pdf_template_layout()
    crop_border = pdf_settings["pdf_crop_border"]

    width_mm = pdf_template_layout["page_width_mm"]
    height_mm = pdf_template_layout["page_height_mm"]

    print_settings = resolve_print_settings()
    is_silhouette_layout = pdf_template_layout.get("is_silhouette_layout", False)

    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=(width_mm * mm, height_mm * mm))

    draw_x_mm = pdf_template_layout["draw_x_mm"]
    draw_y_mm = pdf_template_layout["draw_y_mm"]
    draw_width_mm = pdf_template_layout["draw_width_mm"]
    draw_height_mm = pdf_template_layout["draw_height_mm"]

    if crop_border and not pdf_template_layout["uses_fixed_inner_margin"]:
        crop_left_right_mm = width_mm * 0.05
        crop_top_bottom_mm = height_mm * 0.034

        draw_x_mm = -crop_left_right_mm
        draw_y_mm = -crop_top_bottom_mm
        draw_width_mm = width_mm + (crop_left_right_mm * 2)
        draw_height_mm = height_mm + (crop_top_bottom_mm * 2)

    rendered_image_entries = []
    pack_label_rendered_entries = []

    try:
        # Used by combined-pack print jobs to build a PDF made only of pack labels.
        # This must use the normal PDF layout system, and it must support multiple labels.
        if title_card_only:
            label_states = list(pack_label_states or [])

            if not label_states:
                label_states.append({
                    "pack_display_name": pack_display_name,
                    "set_code": set_code,
                    "booster_name": booster_name,
                    "pack_tracking_code": pack_tracking_code,
                })

            for label_index, label_state in enumerate(label_states, start=1):
                pack_label_rendered_entries.append(
                    build_chaos_pack_label_rendered_entry(
                        label_state.get("pack_display_name") or "Pack Label",
                        set_code=label_state.get("set_code"),
                        booster_name=label_state.get("booster_name"),
                        pack_tracking_code=label_state.get("pack_tracking_code"),
                        label_suffix=f"label_{label_index}",
                    )
                )

            pages_rendered = draw_chaos_rendered_entries_into_pdf_layout(
                c,
                pack_label_rendered_entries,
                pdf_template_layout,
                print_settings,
                width_mm,
                height_mm,
                draw_x_mm,
                draw_y_mm,
                draw_width_mm,
                draw_height_mm,
            )

            if pages_rendered == 0:
                raise ValueError("No Chaos Draft pack label images could be rendered into the PDF.")

            c.save()
            buffer.seek(0)
            return buffer

        # Normal single-card/page title behavior for non-Silhouette layouts.
        # Silhouette layouts already include the initial title card as a normal slot entry below.
        if not is_silhouette_layout:
            title_card_bytes = build_chaos_pack_title_card_image_bytes(
                pack_display_name,
                set_code=set_code,
                booster_name=booster_name,
                pack_tracking_code=pack_tracking_code,
                card_width_mm=width_mm,
                card_height_mm=height_mm,
            )

            title_reader = ImageReader(BytesIO(title_card_bytes))

            c.drawImage(
                title_reader,
                0,
                0,
                width=width_mm * mm,
                height=height_mm * mm,
                preserveAspectRatio=False,
                mask="auto",
            )

            c.showPage()

        # Initial title card as a normal card slot for Silhouette layouts.
        if pdf_template_layout["print_template"] == "silhouette-letter-horizontal-8":
            try:
                config = get_request_config()
                use_pack_image_for_title = (config.get("use_pack_image_for_title") or "0").strip() == "1"

                title_card_bytes = None

                if use_pack_image_for_title and set_code and booster_name:
                    title_card_bytes = build_chaos_pack_image_title_card_bytes(set_code, booster_name)

                if not title_card_bytes:
                    title_card_bytes = build_chaos_pack_title_card_image_bytes(
                        pack_display_name,
                        set_code=set_code,
                        booster_name=booster_name,
                        pack_tracking_code=pack_tracking_code,
                    )

                title_temp_filename = f"chaos_title_{safe_filename(pack_display_name)}.png"
                title_temp_path = get_chaos_temp_file_path(title_temp_filename)

                with open(title_temp_path, "wb") as title_file:
                    title_file.write(title_card_bytes)

                rendered_image_entries.append({
                    "temp_path": title_temp_path,
                    "page_kind": "title",
                    "card_uuid": "",
                    "card_row": None,
                    "is_dual_faced": 0,
                    "is_persistent_cache_file": False,
                    "is_template_rendered": False,
                })
            except Exception as exc:
                write_debug_log(f"CHAOS TITLE CARD ERROR | pack={pack_display_name} | error={str(exc)}")

        # Normal card image entries.
        for card in cards:
            card_uuid = card.get("card_uuid")
            card_row = get_chaos_card_by_uuid(card_uuid)

            if not card_row:
                continue

            page_entries = build_chaos_print_pages_for_card(card_row)
            if not page_entries:
                continue

            for page_entry in page_entries:
                page_image_url = (page_entry.get("image_url") or "").strip()
                page_kind = (page_entry.get("page_kind") or "").strip().lower()

                image_source = resolve_card_image_source_for_page(
                    card_row,
                    page_kind,
                    page_image_url,
                )

                write_debug_log(
                    f"CHAOS PDF RENDER | card_name={page_entry.get('card_name')} | "
                    f"page_kind={page_entry.get('page_kind')} | face_name={page_entry.get('face_name')} | "
                    f"source_type={image_source.get('source_type')} | has_url={'yes' if page_image_url else 'no'}"
                )

                try:
                    if image_source.get("source_type") == "alternate_source":
                        cached_result = {
                            "absolute_path": image_source["absolute_path"],
                        }
                    else:
                        if not page_image_url:
                            continue

                        cached_result = download_chaos_image_to_cache(
                            page_entry.get("card_uuid"),
                            page_entry.get("page_kind"),
                            page_entry.get("face_name"),
                            page_image_url,
                        )

                    if not cached_result:
                        raise ValueError("No cached result returned for chaos image download.")

                    rendered_image_entries.append({
                        "temp_path": cached_result["absolute_path"],
                        "page_kind": (page_entry.get("page_kind") or "").strip().lower(),
                        "card_uuid": (page_entry.get("card_uuid") or card_uuid or "").strip(),
                        "card_row": card_row,
                        "is_dual_faced": int(card_row["is_dual_faced"] or 0),
                        "is_persistent_cache_file": True,
                        "is_template_rendered": False,
                        "export_frame_template": image_source.get("export_frame_template") or "auto",
                    })

                except Exception as exc:
                    write_debug_log(
                        f"CHAOS PDF RENDER ERROR | card_name={page_entry.get('card_name')} | "
                        f"page_kind={page_entry.get('page_kind')} | error={str(exc)}"
                    )
                    continue

        template_rendered_entries = []

        for rendered_entry in rendered_image_entries:
            if rendered_entry.get("page_kind") == "title":
                template_rendered_entries.append(rendered_entry)
                continue

            template_rendered_entries.append(
                build_pdf_rendered_entry_with_template(
                    rendered_entry,
                    pack_tracking_code=pack_tracking_code if pdf_settings.get("print_pack_tracking_code") else "",
                    print_front_back_label=pdf_settings.get("print_front_back_label"),
                )
            )

        rendered_image_entries = template_rendered_entries

        # IMPORTANT:
        # Pack labels are NOT appended to rendered_image_entries.
        # They are rendered as their own trailing "pack" using the exact same layout system.
        if include_pack_labels and get_configured_print_pack_labels():
            try:
                pack_label_rendered_entries.append(
                    build_chaos_pack_label_rendered_entry(
                        pack_display_name,
                        set_code=set_code,
                        booster_name=booster_name,
                        pack_tracking_code=pack_tracking_code,
                        label_suffix="label",
                    )
                )
            except Exception as exc:
                write_debug_log(
                    f"CHAOS PACK LABEL PDF ERROR | pack={pack_display_name} | error={str(exc)}"
                )

        pages_rendered = draw_chaos_rendered_entries_into_pdf_layout(
            c,
            rendered_image_entries,
            pdf_template_layout,
            print_settings,
            width_mm,
            height_mm,
            draw_x_mm,
            draw_y_mm,
            draw_width_mm,
            draw_height_mm,
        )

        if pages_rendered == 0:
            raise ValueError("No Chaos Draft card images could be rendered into the PDF.")

        # Render pack labels after the normal cards, as separate trailing layout pages.
        # This gives Silhouette its own background/sheet and prevents mixed card+label pages.
        if pack_label_rendered_entries:
            draw_chaos_rendered_entries_into_pdf_layout(
                c,
                pack_label_rendered_entries,
                pdf_template_layout,
                print_settings,
                width_mm,
                height_mm,
                draw_x_mm,
                draw_y_mm,
                draw_width_mm,
                draw_height_mm,
            )

        c.save()
        buffer.seek(0)

        return buffer

    finally:
        for rendered_entry in rendered_image_entries + pack_label_rendered_entries:
            try:
                if rendered_entry.get("is_persistent_cache_file", False):
                    continue

                temp_path = rendered_entry.get("temp_path")
                if temp_path and os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                pass

    if not is_silhouette_layout:
        # Title card page.
        title_card_bytes = build_chaos_pack_title_card_image_bytes(
            pack_display_name,
            set_code=set_code,
            booster_name=booster_name,
            pack_tracking_code=pack_tracking_code,
            card_width_mm=width_mm,
            card_height_mm=height_mm,
        )

        title_reader = ImageReader(BytesIO(title_card_bytes))

        c.drawImage(
            title_reader,
            0,
            0,
            width=width_mm * mm,
            height=height_mm * mm,
            preserveAspectRatio=False,
            mask="auto",
        )

        c.showPage()

    draw_x_mm = pdf_template_layout["draw_x_mm"]
    draw_y_mm = pdf_template_layout["draw_y_mm"]
    draw_width_mm = pdf_template_layout["draw_width_mm"]
    draw_height_mm = pdf_template_layout["draw_height_mm"]

    if crop_border and not pdf_template_layout["uses_fixed_inner_margin"]:
        crop_left_right_mm = width_mm * 0.05
        crop_top_bottom_mm = height_mm * 0.034

        draw_x_mm = -crop_left_right_mm
        draw_y_mm = -crop_top_bottom_mm
        draw_width_mm = width_mm + (crop_left_right_mm * 2)
        draw_height_mm = height_mm + (crop_top_bottom_mm * 2)

    rendered_image_entries = []

    if pdf_template_layout["print_template"] == "silhouette-letter-horizontal-8":
        try:
            config = get_request_config()
            use_pack_image_for_title = (config.get("use_pack_image_for_title") or "0").strip() == "1"

            title_card_bytes = None

            if use_pack_image_for_title and set_code and booster_name:
                title_card_bytes = build_chaos_pack_image_title_card_bytes(set_code, booster_name)

            if not title_card_bytes:
                title_card_bytes = build_chaos_pack_title_card_image_bytes(
                    pack_display_name,
                    set_code=set_code,
                    booster_name=booster_name,
                    pack_tracking_code=pack_tracking_code,
                )

            title_temp_filename = f"chaos_title_{safe_filename(pack_display_name)}.png"
            title_temp_path = get_chaos_temp_file_path(title_temp_filename)

            with open(title_temp_path, "wb") as title_file:
                title_file.write(title_card_bytes)

            rendered_image_entries.append({
                "temp_path": title_temp_path,
                "page_kind": "title",
                "card_uuid": "",
                "card_row": None,
                "is_dual_faced": 0,
                "is_persistent_cache_file": False,
                "is_template_rendered": False,
            })
        except Exception as exc:
            write_debug_log(f"CHAOS TITLE CARD ERROR | pack={pack_display_name} | error={str(exc)}")

    for card in cards:
        card_uuid = card.get("card_uuid")
        card_row = get_chaos_card_by_uuid(card_uuid)

        if not card_row:
            continue

        page_entries = build_chaos_print_pages_for_card(card_row)
        if not page_entries:
            continue

        for page_entry in page_entries:
            page_image_url = (page_entry.get("image_url") or "").strip()
            page_kind = (page_entry.get("page_kind") or "").strip().lower()

            image_source = resolve_card_image_source_for_page(
                card_row,
                page_kind,
                page_image_url,
            )

            write_debug_log(
                f"CHAOS PDF RENDER | card_name={page_entry.get('card_name')} | "
                f"page_kind={page_entry.get('page_kind')} | face_name={page_entry.get('face_name')} | "
                f"source_type={image_source.get('source_type')} | has_url={'yes' if page_image_url else 'no'}"
            )

            try:
                if image_source.get("source_type") == "alternate_source":
                    cached_result = {
                        "absolute_path": image_source["absolute_path"],
                    }
                else:
                    if not page_image_url:
                        continue

                    cached_result = download_chaos_image_to_cache(
                        page_entry.get("card_uuid"),
                        page_entry.get("page_kind"),
                        page_entry.get("face_name"),
                        page_image_url,
                    )

                if not cached_result:
                    raise ValueError("No cached result returned for chaos image download.")

                rendered_image_entries.append({
                    "temp_path": cached_result["absolute_path"],
                    "page_kind": (page_entry.get("page_kind") or "").strip().lower(),
                    "card_uuid": (page_entry.get("card_uuid") or card_uuid or "").strip(),
                    "card_row": card_row,
                    "is_dual_faced": int(card_row["is_dual_faced"] or 0),
                    "is_persistent_cache_file": True,
                    "is_template_rendered": False,
                    "export_frame_template": image_source.get("export_frame_template") or "auto",
                })

            except Exception as exc:
                write_debug_log(
                    f"CHAOS PDF RENDER ERROR | card_name={page_entry.get('card_name')} | "
                    f"page_kind={page_entry.get('page_kind')} | error={str(exc)}"
                )
                continue

    pages_rendered = 0

    template_rendered_entries = []

    for rendered_entry in rendered_image_entries:
        if rendered_entry.get("page_kind") == "title":
            template_rendered_entries.append(rendered_entry)
            continue

        template_rendered_entries.append(
            build_pdf_rendered_entry_with_template(
                rendered_entry,
                pack_tracking_code=pack_tracking_code if pdf_settings.get("print_pack_tracking_code") else "",
                print_front_back_label=pdf_settings.get("print_front_back_label"),
            )
        )

    rendered_image_entries = template_rendered_entries

    if include_pack_labels and get_configured_print_pack_labels():
        try:
            pack_label_bytes = build_chaos_pack_title_card_image_bytes(
                pack_display_name,
                set_code=set_code,
                booster_name=booster_name,
                pack_tracking_code=pack_tracking_code,
            )

            pack_label_temp_filename = (
                f"chaos_pack_label_{safe_filename(pack_display_name)}"
                f"_{safe_filename(pack_tracking_code or 'label')}.png"
            )
            pack_label_temp_path = get_chaos_temp_file_path(pack_label_temp_filename)

            with open(pack_label_temp_path, "wb") as pack_label_file:
                pack_label_file.write(pack_label_bytes)

            rendered_image_entries.append({
                "temp_path": pack_label_temp_path,
                "page_kind": "title",
                "card_uuid": "",
                "card_row": None,
                "is_dual_faced": 0,
                "is_persistent_cache_file": False,
                "is_template_rendered": False,
            })

        except Exception as exc:
            write_debug_log(
                f"CHAOS PACK LABEL PDF ERROR | pack={pack_display_name} | error={str(exc)}"
            )

    try:
        if pdf_template_layout["print_template"] == "silhouette-letter-horizontal-8":
            background_abs_path = os.path.join(app.static_folder, "sil", "SIL_LETTER_HORIZONTAL.png")

            if not os.path.exists(background_abs_path):
                raise FileNotFoundError(f"Silhouette background not found: {background_abs_path}")

            slot_defs = get_silhouette_letter_horizontal_8_slots_mm()

            for page_start_index in range(0, len(rendered_image_entries), 8):
                page_entries = rendered_image_entries[page_start_index:page_start_index + 8]

                draw_pdf_background_image(
                    c,
                    background_abs_path,
                    width_mm,
                    height_mm,
                )

                for slot_index, rendered_entry in enumerate(page_entries):
                    slot_def = slot_defs[slot_index]

                    draw_processed_image_into_slot(
                        c,
                        rendered_entry["temp_path"],
                        print_settings["print_mode"],
                        slot_def,
                        add_edge_bleed_border=True,
                        rounded_corner_radius_mm=SILHOUETTE_CORNER_RADIUS_MM,
                    )

                if SILHOUETTE_FILL_UNUSED_SLOTS_WITH_WHITE and len(page_entries) < len(slot_defs):
                    for blank_slot_index in range(len(page_entries), len(slot_defs)):
                        draw_processed_image_into_slot(
                            c,
                            image_path=None,
                            print_mode=print_settings["print_mode"],
                            slot_def=slot_defs[blank_slot_index],
                            add_edge_bleed_border=False,
                            rounded_corner_radius_mm=SILHOUETTE_CORNER_RADIUS_MM,
                            blank_white_card=True,
                        )

                c.showPage()
                pages_rendered += 1

        elif pdf_template_layout.get("is_multi_card_layout", False) and pdf_template_layout["print_template"] == "borderless-3p5x5-two-card":
            slot_defs = get_two_card_borderless_slots_mm()

            for page_start_index in range(0, len(rendered_image_entries), 2):
                page_entries = rendered_image_entries[page_start_index:page_start_index + 2]

                for slot_index, rendered_entry in enumerate(page_entries):
                    slot_def = slot_defs[slot_index]

                    draw_processed_image_into_two_card_slot(
                        c,
                        rendered_entry["temp_path"],
                        print_settings["print_mode"],
                        slot_def,
                    )

                c.showPage()
                pages_rendered += 1
        else:
            for rendered_entry in rendered_image_entries:
                pdf_image_reader = build_pdf_image_reader(
                    rendered_entry["temp_path"],
                    print_settings["print_mode"],
                )

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

    finally:
        for rendered_entry in rendered_image_entries:
            try:
                if rendered_entry.get("is_persistent_cache_file", False):
                    continue

                temp_path = rendered_entry.get("temp_path")
                if temp_path and os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                pass

    if pages_rendered == 0:
        raise ValueError("No Chaos Draft card images could be rendered into the PDF.")

    c.save()
    buffer.seek(0)

    return buffer

def get_next_image_export_folder():
    auto_clear_export_root(get_auto_clear_exports_config_value())
    os.makedirs(EXPORT_ROOT_DIR, exist_ok=True)

    date_stamp = datetime.now().strftime("%Y%m%d")
    export_index = 1

    while True:
        folder_name = f"Image Export {date_stamp}_{export_index}"
        export_folder = os.path.join(EXPORT_ROOT_DIR, folder_name)

        if not os.path.exists(export_folder):
            return {
                "folder_name": folder_name,
                "folder_path": export_folder,
            }

        export_index += 1


def make_image_export_id_safe(value):
    clean_value = (value or "").strip()

    if not clean_value:
        return "CARD"

    clean_value = re.sub(r"[^A-Za-z0-9]+", "_", clean_value)
    clean_value = re.sub(r"_+", "_", clean_value)
    clean_value = clean_value.strip("_")

    return clean_value or "CARD"


def make_image_export_filename_safe(value):
    clean_value = make_image_export_id_safe(value)

    if len(clean_value) > 120:
        clean_value = clean_value[:120].rstrip("_")

    return clean_value or "CARD"

def sample_image_region_rgb(image, x1, y1, x2, y2):
    width, height = image.size

    left = max(0, min(width - 1, int(width * x1)))
    top = max(0, min(height - 1, int(height * y1)))
    right = max(left + 1, min(width, int(width * x2)))
    bottom = max(top + 1, min(height, int(height * y2)))

    region = image.crop((left, top, right, bottom)).convert("RGB")
    pixels = list(region.getdata())

    dark_pixels = [
        pixel for pixel in pixels
        if ((pixel[0] + pixel[1] + pixel[2]) / 3.0) <= 90
    ]

    source_pixels = dark_pixels if dark_pixels else pixels

    if not source_pixels:
        return None

    reds = sorted(pixel[0] for pixel in source_pixels)
    greens = sorted(pixel[1] for pixel in source_pixels)
    blues = sorted(pixel[2] for pixel in source_pixels)

    middle = len(source_pixels) // 2

    return (
        reds[middle],
        greens[middle],
        blues[middle],
    )


def sample_template_regions_rgb(image, sample_regions, fallback_rgb):
    sampled_colors = []

    for region in sample_regions or []:
        sampled_rgb = sample_image_region_rgb(
            image,
            region["x1"],
            region["y1"],
            region["x2"],
            region["y2"],
        )

        if sampled_rgb:
            sampled_colors.append(sampled_rgb)

    if sampled_colors:
        reds = sorted(rgb[0] for rgb in sampled_colors)
        greens = sorted(rgb[1] for rgb in sampled_colors)
        blues = sorted(rgb[2] for rgb in sampled_colors)
        middle = len(sampled_colors) // 2

        return (
            reds[middle],
            greens[middle],
            blues[middle],
        )

    return tuple(fallback_rgb or (18, 12, 12))


def sample_export_border_rgb(image, template_config, sample_type="card_matte", fallback_rgb=None):
    template_fallback_rgb = tuple(template_config.get("fallback_rgb") or (18, 12, 12))
    effective_fallback_rgb = tuple(fallback_rgb or template_fallback_rgb)

    if sample_type == "overlay_fill":
        sample_regions = (
            template_config.get("overlay_fill_sample_regions")
            or template_config.get("border_sample_regions")
            or []
        )
    elif sample_type == "bleed_fill":
        sample_regions = (
            template_config.get("bleed_fill_sample_regions")
            or template_config.get("card_matte_sample_regions")
            or template_config.get("border_sample_regions")
            or []
        )
    else:
        sample_regions = (
            template_config.get("card_matte_sample_regions")
            or template_config.get("border_sample_regions")
            or []
        )

    return sample_template_regions_rgb(
        image=image,
        sample_regions=sample_regions,
        fallback_rgb=effective_fallback_rgb,
    )

def draw_selective_rounded_rectangle(draw, box, radius, corners, fill):
    left, top, right, bottom = box
    radius = int(max(0, radius or 0))

    if radius <= 0:
        draw.rectangle(box, fill=fill)
        return

    width = max(1, right - left)
    height = max(1, bottom - top)
    radius = min(radius, width // 2, height // 2)

    top_left = bool((corners or {}).get("top_left"))
    top_right = bool((corners or {}).get("top_right"))
    bottom_right = bool((corners or {}).get("bottom_right"))
    bottom_left = bool((corners or {}).get("bottom_left"))

    # Center rectangles.
    draw.rectangle((left + radius, top, right - radius, bottom), fill=fill)
    draw.rectangle((left, top + radius, right, bottom - radius), fill=fill)

    # Corners.
    if top_left:
        draw.pieslice((left, top, left + radius * 2, top + radius * 2), 180, 270, fill=fill)
    else:
        draw.rectangle((left, top, left + radius, top + radius), fill=fill)

    if top_right:
        draw.pieslice((right - radius * 2, top, right, top + radius * 2), 270, 360, fill=fill)
    else:
        draw.rectangle((right - radius, top, right, top + radius), fill=fill)

    if bottom_right:
        draw.pieslice((right - radius * 2, bottom - radius * 2, right, bottom), 0, 90, fill=fill)
    else:
        draw.rectangle((right - radius, bottom - radius, right, bottom), fill=fill)

    if bottom_left:
        draw.pieslice((left, bottom - radius * 2, left + radius * 2, bottom), 90, 180, fill=fill)
    else:
        draw.rectangle((left, bottom - radius, left + radius, bottom), fill=fill)

def draw_overlay_mask_shape(mask_draw, box, radius, corners, fill=255):
    left, top, right, bottom = box
    radius = int(max(0, radius or 0))

    if radius <= 0:
        mask_draw.rectangle(box, fill=fill)
        return

    width = max(1, right - left)
    height = max(1, bottom - top)
    radius = min(radius, width // 2, height // 2)

    top_left = bool((corners or {}).get("top_left"))
    top_right = bool((corners or {}).get("top_right"))
    bottom_right = bool((corners or {}).get("bottom_right"))
    bottom_left = bool((corners or {}).get("bottom_left"))

    mask_draw.rectangle((left + radius, top, right - radius, bottom), fill=fill)
    mask_draw.rectangle((left, top + radius, right, bottom - radius), fill=fill)

    if top_left:
        mask_draw.pieslice((left, top, left + radius * 2, top + radius * 2), 180, 270, fill=fill)
    else:
        mask_draw.rectangle((left, top, left + radius, top + radius), fill=fill)

    if top_right:
        mask_draw.pieslice((right - radius * 2, top, right, top + radius * 2), 270, 360, fill=fill)
    else:
        mask_draw.rectangle((right - radius, top, right, top + radius), fill=fill)

    if bottom_right:
        mask_draw.pieslice((right - radius * 2, bottom - radius * 2, right, bottom), 0, 90, fill=fill)
    else:
        mask_draw.rectangle((right - radius, bottom - radius, right, bottom), fill=fill)

    if bottom_left:
        mask_draw.pieslice((left, bottom - radius * 2, left + radius * 2, bottom), 90, 180, fill=fill)
    else:
        mask_draw.rectangle((left, bottom - radius, left + radius, bottom), fill=fill)

def apply_overlay_with_cutouts(image, box, radius_px, corners, fill_rgb, cutouts, image_width, image_height):
    mask = Image.new("L", (image_width, image_height), 0)
    mask_draw = ImageDraw.Draw(mask)

    draw_overlay_mask_shape(
        mask_draw,
        box,
        radius_px,
        corners,
        fill=255,
    )

    for cutout in cutouts or []:
        shape = (cutout.get("shape") or "").strip().lower()

        if shape == "circle":
            cx = int(image_width * float(cutout.get("cx", 0.0)))
            cy = int(image_height * float(cutout.get("cy", 0.0)))
            radius = int(min(image_width, image_height) * float(cutout.get("r", 0.0)))

            if radius > 0:
                mask_draw.ellipse(
                    (cx - radius, cy - radius, cx + radius, cy + radius),
                    fill=0,
                )

        elif shape == "ellipse":
            cx = int(image_width * float(cutout.get("cx", 0.0)))
            cy = int(image_height * float(cutout.get("cy", 0.0)))
            rx = int(image_width * float(cutout.get("rx", 0.0)))
            ry = int(image_height * float(cutout.get("ry", 0.0)))

            if rx > 0 and ry > 0:
                mask_draw.ellipse(
                    (cx - rx, cy - ry, cx + rx, cy + ry),
                    fill=0,
                )

        elif shape == "rect":
            x1 = int(image_width * float(cutout.get("x1", 0.0)))
            y1 = int(image_height * float(cutout.get("y1", 0.0)))
            x2 = int(image_width * float(cutout.get("x2", 0.0)))
            y2 = int(image_height * float(cutout.get("y2", 0.0)))

            mask_draw.rectangle((x1, y1, x2, y2), fill=0)

    overlay_layer = Image.new("RGB", (image_width, image_height), fill_rgb)
    image.paste(overlay_layer, mask=mask)

    return image

def get_readable_overlay_text_rgb(background_rgb, template_config=None):
    template_config = template_config or {}

    # Optional hard override for unusual templates.
    forced_text_rgb = template_config.get("text_fill_rgb_override")
    if forced_text_rgb:
        return tuple(forced_text_rgb)

    try:
        red, green, blue = background_rgb
    except Exception:
        return (255, 255, 255)

    # WCAG-style relative luminance.
    # Higher value = lighter background.
    def linearize_color_channel(channel_value):
        channel = float(channel_value) / 255.0

        if channel <= 0.03928:
            return channel / 12.92

        return ((channel + 0.055) / 1.055) ** 2.4

    red_lum = linearize_color_channel(red)
    green_lum = linearize_color_channel(green)
    blue_lum = linearize_color_channel(blue)

    luminance = (0.2126 * red_lum) + (0.7152 * green_lum) + (0.0722 * blue_lum)

    # Light background: use black text.
    # Dark background: use white text.
    if luminance >= 0.48:
        return (0, 0, 0)

    return (255, 255, 255)

def draw_image_export_corner_label(image, label_text, template_config, matte_rgb, overlay_fill_rgb=None):
    label_text = (label_text or "").strip().upper()
    source_image = image.convert("RGB")

    if not label_text:
        return source_image

    draw = ImageDraw.Draw(source_image)
    image_width, image_height = source_image.size

    overlay_box = template_config.get("overlay_box") or {}
    text_box = template_config.get("text_box") or {}

    overlay_left = int(image_width * float(overlay_box.get("x1", 0.000)))
    overlay_top = int(image_height * float(overlay_box.get("y1", 0.958)))
    overlay_right = int(image_width * float(overlay_box.get("x2", 0.445)))
    overlay_bottom = int(image_height * float(overlay_box.get("y2", 1.000)))

    overlay_radius_pct = float(template_config.get("overlay_corner_radius_pct") or 0.0)
    overlay_radius_px = int(min(image_width, image_height) * overlay_radius_pct)

    overlay_fill_rgb = tuple(
        template_config.get("overlay_fill_rgb_override")
        or overlay_fill_rgb
        or matte_rgb
        or template_config.get("fallback_rgb")
        or (18, 12, 12)
    )

    text_fill_rgb = get_readable_overlay_text_rgb(
        overlay_fill_rgb,
        template_config=template_config,
    )

    source_image = apply_overlay_with_cutouts(
        image=source_image,
        box=(overlay_left, overlay_top, overlay_right, overlay_bottom),
        radius_px=overlay_radius_px,
        corners=template_config.get("overlay_round_corners") or {},
        fill_rgb=overlay_fill_rgb,
        cutouts=template_config.get("overlay_cutouts") or [],
        image_width=image_width,
        image_height=image_height,
    )

    draw = ImageDraw.Draw(source_image)

    text_left = int(image_width * float(text_box.get("x1", 0.025)))
    text_top = int(image_height * float(text_box.get("y1", 0.962)))
    text_right = int(image_width * float(text_box.get("x2", 0.430)))
    text_bottom = int(image_height * float(text_box.get("y2", 0.995)))

    max_text_width = max(20, text_right - text_left)
    max_text_height = max(14, text_bottom - text_top)

    font_size = max(12, int(max_text_height * 0.82))
    font = None
    text_width = 0
    text_height = 0

    while font_size >= 10:
        try:
            font = ImageFont.truetype("arialbd.ttf", font_size)
        except Exception:
            font = ImageFont.load_default()

        try:
            text_bbox = draw.textbbox((0, 0), label_text, font=font)
            text_width = text_bbox[2] - text_bbox[0]
            text_height = text_bbox[3] - text_bbox[1]
        except Exception:
            text_width = len(label_text) * 8
            text_height = 14

        if text_width <= max_text_width and text_height <= max_text_height:
            break

        font_size -= 1

    text_align = (template_config.get("text_align") or "left").strip().lower()

    if text_align == "center":
        text_x = text_left + max(0, (max_text_width - text_width) // 2)
    elif text_align == "right":
        text_x = text_right - text_width
    else:
        text_x = text_left

    text_y = text_top + max(0, (max_text_height - text_height) // 2)

    draw.text(
        (text_x, text_y),
        label_text,
        fill=text_fill_rgb,
        font=font,
    )

    return source_image

def build_export_card_image(
    source_image_path,
    output_image_path,
    label_text,
    card_row,
    template_key_override=None,
    add_export_bleed=False,
    use_bleed_template=False,
):
    with Image.open(source_image_path) as source_image:
        image = source_image.convert("RGB")

        # If we are GENERATING bleed, clean up the original card corners first.
        # Do not do this for real full-bleed sources, because those already include
        # their own bleed image and should not be treated like a normal 63x88 card.
        if add_export_bleed:
            pre_bleed_template_config = resolve_card_export_template(
                card_row,
                template_key_override=template_key_override,
                use_bleed_template=False,
            )

            pre_bleed_card_matte_rgb = sample_export_border_rgb(
                image,
                pre_bleed_template_config,
                sample_type="card_matte",
            )

            bleed_fill_rgb = sample_export_border_rgb(
                image,
                pre_bleed_template_config,
                sample_type="bleed_fill",
                fallback_rgb=pre_bleed_card_matte_rgb,
            )

            if (pre_bleed_template_config.get("card_corner_mode") or "").strip().lower() == "rounded_mask":
                pre_bleed_radius_px = max(
                    1,
                    int(
                        min(image.size)
                        * float(pre_bleed_template_config.get("card_corner_radius_pct") or 0.03)
                    ),
                )

                image = apply_rounded_corner_mask(
                    image,
                    pre_bleed_radius_px,
                    matte_rgb=pre_bleed_card_matte_rgb,
                )

            image = add_card_bleed(
                image,
                bleed_size_mm=get_configured_print_bleed_size_mm(),
                bleed_rgb=bleed_fill_rgb,
            )

        template_config = resolve_card_export_template(
            card_row,
            template_key_override=template_key_override,
            use_bleed_template=use_bleed_template,
        )

        image = add_duplicated_edge_border(image)

        card_matte_rgb = sample_export_border_rgb(
            image,
            template_config,
            sample_type="card_matte",
        )

        overlay_fill_rgb = sample_export_border_rgb(
            image,
            template_config,
            sample_type="overlay_fill",
            fallback_rgb=card_matte_rgb,
        )

        # Normal exports and real full-bleed sources still use the regular template corner cleanup.
        # Generated bleed already had the original card corners cleaned before the bleed was added,
        # so do not apply a second rounded mask to the expanded bleed canvas.
        if (
            not add_export_bleed
            and (template_config.get("card_corner_mode") or "").strip().lower() == "rounded_mask"
        ):
            radius_px = max(
                1,
                int(min(image.size) * float(template_config.get("card_corner_radius_pct") or 0.03)),
            )

            image = apply_rounded_corner_mask(
                image,
                radius_px,
                matte_rgb=card_matte_rgb,
            )

        image = draw_image_export_corner_label(
            image=image,
            label_text=label_text,
            template_config=template_config,
            matte_rgb=card_matte_rgb,
            overlay_fill_rgb=overlay_fill_rgb,
        )

        os.makedirs(os.path.dirname(output_image_path), exist_ok=True)
        image.save(output_image_path, format="JPEG", quality=95, optimize=True)

def build_chaos_template_rendered_card_image(
    source_image_path,
    output_image_path,
    label_text,
    card_row,
    template_key_override=None,
    add_export_bleed=False,
    use_bleed_template=False,
):
    """
    Shared Chaos Draft card rendering path.

    Used by:
    - Image export ZIP files
    - Chaos Draft PDFs
    - Campaign Mode PDFs
    - PRE-PRINT PDFs
    - Manage Packs / Preview print PDFs

    This applies:
    - duplicated edge bleed
    - frame-template corner cleanup
    - sampled border/matte color
    - template-based pack-code overlay placement
    """
    build_export_card_image(
        source_image_path,
        output_image_path,
        label_text,
        card_row,
        template_key_override=template_key_override,
        add_export_bleed=add_export_bleed,
        use_bleed_template=use_bleed_template,
    )

    return output_image_path

def get_tracked_pack_card_export_rows(tracked_pack_ids):
    pack_ids = normalize_tracked_pack_id_list(tracked_pack_ids)

    if not pack_ids:
        return []

    placeholders = ",".join(["?"] * len(pack_ids))

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        f"""
        SELECT
            tcp.tracked_pack_id,
            tcp.pack_tracking_code,
            tcp.set_code AS pack_set_code,
            tcp.booster_name AS pack_booster_name,
            tcp.booster_index,
            tcp.pack_display_name,

            tcpc.tracked_pack_card_id,
            tcpc.card_order,
            tcpc.card_uuid,
            tcpc.card_name,
            tcpc.set_code AS card_set_code,
            tcpc.sheet_is_foil,
            tcpc.rarity,
            tcpc.type_line,
            tcpc.scryfall_id,
            tcpc.collector_number
        FROM tracked_chaos_packs tcp
        INNER JOIN tracked_chaos_pack_cards tcpc
            ON tcpc.tracked_pack_id = tcp.tracked_pack_id
        WHERE tcp.tracked_pack_id IN ({placeholders})
        ORDER BY
            tcp.tracked_pack_id ASC,
            tcpc.card_order ASC,
            tcpc.tracked_pack_card_id ASC
        """,
        pack_ids,
    )

    rows = cursor.fetchall()
    conn.close()

    export_rows = []

    for row in rows:
        export_rows.append({
            "tracked_pack_id": int(row["tracked_pack_id"]),
            "pack_tracking_code": (row["pack_tracking_code"] or "").strip().upper(),
            "pack_set_code": (row["pack_set_code"] or "").strip().upper(),
            "pack_booster_name": (row["pack_booster_name"] or "").strip().lower(),
            "booster_index": int(row["booster_index"] or 0),
            "pack_display_name": row["pack_display_name"] or "",
            "tracked_pack_card_id": int(row["tracked_pack_card_id"]),
            "card_order": int(row["card_order"] or 0),
            "card_uuid": (row["card_uuid"] or "").strip(),
            "card_name": row["card_name"] or "",
            "card_set_code": (row["card_set_code"] or row["pack_set_code"] or "").strip().upper(),
            "sheet_is_foil": int(row["sheet_is_foil"] or 0),
            "rarity": row["rarity"] or "",
            "type_line": row["type_line"] or "",
            "scryfall_id": row["scryfall_id"] or "",
            "collector_number": row["collector_number"] or "",
        })

    return export_rows


def append_xml_text(parent_element, tag_name, text_value):
    child_element = ET.SubElement(parent_element, tag_name)
    child_element.text = str(text_value if text_value is not None else "")
    return child_element


def write_card_image_export_xml(xml_path, card_entries, foil_value):
    order_element = ET.Element("order")

    details_element = ET.SubElement(order_element, "details")
    append_xml_text(details_element, "quantity", len(card_entries))
    append_xml_text(details_element, "foil", "true" if foil_value else "false")

    cards_element = ET.SubElement(order_element, "cards")

    for card_entry in card_entries:
        card_element = ET.SubElement(cards_element, "card")

        append_xml_text(card_element, "id", card_entry["id"])
        append_xml_text(card_element, "name", card_entry["name"])
        append_xml_text(card_element, "setcode", card_entry["setcode"])
        append_xml_text(card_element, "collector_number", card_entry["collector_number"])
        append_xml_text(card_element, "front_image_path", card_entry["front_image_path"])
        append_xml_text(card_element, "back_image_path", card_entry["back_image_path"])
        append_xml_text(card_element, "pack_tracking_code", card_entry["pack_tracking_code"])

    tree = ET.ElementTree(order_element)
    ET.indent(tree, space="    ", level=0)

    os.makedirs(os.path.dirname(xml_path), exist_ok=True)
    tree.write(xml_path, encoding="utf-8", xml_declaration=True)


def zip_image_export_folder(export_folder_path, zip_path):
    export_parent = os.path.dirname(export_folder_path)

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zip_file:
        for root_path, _, file_names in os.walk(export_folder_path):
            for file_name in file_names:
                file_path = os.path.join(root_path, file_name)
                archive_name = os.path.relpath(file_path, export_parent)
                zip_file.write(file_path, archive_name)

def resolve_card_export_template(card_row, template_key_override=None, use_bleed_template=False):
    row_keys = set(card_row.keys()) if hasattr(card_row, "keys") else set()

    set_code = (card_row["set_code"] if "set_code" in row_keys else "") or ""
    frame_version = (card_row["frame_version"] if "frame_version" in row_keys else "") or ""

    release_year = None

    if set_code:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT release_date
            FROM sets
            WHERE set_code = ?
            """,
            ((set_code or "").strip().upper(),),
        )

        row = cursor.fetchone()
        conn.close()

        if row and row["release_date"]:
            try:
                release_year = int(str(row["release_date"])[:4])
            except Exception:
                release_year = None

    return resolve_card_export_template_config(
        set_code=set_code,
        frame_version=frame_version,
        release_year=release_year,
        template_key_override=template_key_override,
        use_bleed_template=use_bleed_template,
    )

def resolve_image_export_bleed_source_path(cached_result, image_source, export_add_bleed):
    source_path = (cached_result or {}).get("absolute_path") or ""

    if not export_add_bleed:
        return {
            "source_path": source_path,
            "add_export_bleed": False,
            "use_bleed_template": False,
            "used_fullbleed_source": False,
        }

    fullbleed_source_path = (image_source or {}).get("fullbleed_absolute_path") or ""

    if fullbleed_source_path and os.path.exists(fullbleed_source_path):
        return {
            "source_path": fullbleed_source_path,
            "add_export_bleed": False,
            "use_bleed_template": True,
            "used_fullbleed_source": True,
        }

    return {
        "source_path": source_path,
        "add_export_bleed": True,
        "use_bleed_template": True,
        "used_fullbleed_source": False,
    }

def build_custom_draft_set_image_export_rows(set_code):
    clean_set_code = normalize_custom_draft_set_code(set_code)
    custom_set = get_custom_draft_set(clean_set_code)

    if not custom_set:
        raise ValueError("Custom draft set was not found.")

    custom_set_cards = get_custom_draft_set_card_rows(clean_set_code)

    if not custom_set_cards:
        raise ValueError("This custom draft set does not have any cards to export.")

    set_name = custom_set["set_name"] if custom_set["set_name"] else "Custom Draft Set"
    pack_tracking_code = clean_set_code.replace("^", "CUSTOM")
    pack_display_name = f"{set_name} ({clean_set_code})"

    export_rows = []

    for card_index, card in enumerate(custom_set_cards, start=1):
        export_rows.append({
            "tracked_pack_id": 0,
            "pack_tracking_code": pack_tracking_code,
            "pack_set_code": clean_set_code,
            "pack_booster_name": "custom_set",
            "booster_index": 0,
            "pack_display_name": pack_display_name,

            "tracked_pack_card_id": int(card["custom_set_card_id"]),
            "card_order": card_index,
            "card_uuid": (card["card_uuid"] or "").strip(),
            "card_name": card["card_name"] or "",
            "card_set_code": (card["card_set_code"] or "").strip().upper(),
            "sheet_is_foil": 0,
            "rarity": card["rarity"] or "",
            "type_line": card["type_line"] or "",
            "scryfall_id": "",
            "collector_number": card["collector_number"] or "",
        })

    return export_rows

def build_chaos_card_image_export_zip(tracked_pack_ids=None, export_rows=None):
    if export_rows is None:
        export_rows = get_tracked_pack_card_export_rows(tracked_pack_ids or [])

    if not export_rows:
        raise ValueError("No selected pack cards were available for image export.")

    export_add_bleed = get_configured_export_add_bleed()
    export_info = get_next_image_export_folder()
    export_folder = export_info["folder_path"]
    export_folder_name = export_info["folder_name"]

    root_default_dir = os.path.join(export_folder, "Default")
    images_dir = os.path.join(export_folder, "Images")
    images_default_dir = os.path.join(images_dir, "Default")
    images_regular_dir = os.path.join(images_dir, "Regular")
    images_foil_dir = os.path.join(images_dir, "Foil")
    pack_labels_dir = os.path.join(export_folder, "PackLabels")

    os.makedirs(root_default_dir, exist_ok=True)
    os.makedirs(images_default_dir, exist_ok=True)
    os.makedirs(images_regular_dir, exist_ok=True)
    os.makedirs(images_foil_dir, exist_ok=True)
    os.makedirs(pack_labels_dir, exist_ok=True)

    mtg_back_source_path = os.path.join(app.static_folder, "img", "mtg_back.jpg")
    if not os.path.exists(mtg_back_source_path):
        raise FileNotFoundError(f"Default MTG back image was not found: {mtg_back_source_path}")

    default_back_filename = "default_back.jpg"
    default_back_output_path = os.path.join(images_default_dir, default_back_filename)

    if export_add_bleed:
        with Image.open(mtg_back_source_path) as mtg_back_image:
            default_back_image = add_card_bleed(
                mtg_back_image,
                bleed_size_mm=get_configured_print_bleed_size_mm(),
            )
            default_back_image.save(default_back_output_path, format="JPEG", quality=95, optimize=True)
    else:
        shutil.copyfile(mtg_back_source_path, default_back_output_path)

    # Also copy into root Default because the requested folder structure includes it.
    shutil.copyfile(
        default_back_output_path,
        os.path.join(root_default_dir, default_back_filename),
    )

    regular_xml_cards = []
    foil_xml_cards = []
    export_warnings = []

    exported_filename_counts = {}

    for export_row in export_rows:
        card_uuid = export_row["card_uuid"]
        card_row = get_chaos_card_by_uuid(card_uuid)

        if not card_row:
            write_debug_log(
                f"IMAGE EXPORT SKIP | card_uuid={card_uuid} | reason=card row not found"
            )
            continue

        page_entries = build_chaos_print_pages_for_card(card_row)
        if not page_entries:
            write_debug_log(
                f"IMAGE EXPORT SKIP | card_uuid={card_uuid} | reason=no printable page entries"
            )
            continue

        front_page_entry = None
        back_page_entry = None

        for page_entry in page_entries:
            page_kind = (page_entry.get("page_kind") or "").strip().lower()

            if page_kind in {"front", "single"} and front_page_entry is None:
                front_page_entry = page_entry

            elif page_kind == "back" and back_page_entry is None:
                back_page_entry = page_entry

        if front_page_entry is None:
            front_page_entry = page_entries[0]

        is_foil = int(export_row.get("sheet_is_foil") or 0) == 1
        finish_folder_name = "Foil" if is_foil else "Regular"
        finish_output_dir = images_foil_dir if is_foil else images_regular_dir

        friendly_card_name = make_image_export_id_safe(export_row["card_name"])
        export_id = (
            f"{export_row['pack_tracking_code']}_"
            f"{export_row['tracked_pack_card_id']}_"
            f"{friendly_card_name}"
        )

        base_filename = make_image_export_filename_safe(export_id)
        filename_key = base_filename.lower()
        exported_filename_counts[filename_key] = exported_filename_counts.get(filename_key, 0) + 1

        if exported_filename_counts[filename_key] > 1:
            base_filename = f"{base_filename}_{exported_filename_counts[filename_key]}"

        front_filename = f"{base_filename}.jpg"
        front_output_path = os.path.join(finish_output_dir, front_filename)

        front_image_source = resolve_card_image_source_for_page(
            card_row,
            front_page_entry.get("page_kind"),
            front_page_entry.get("image_url"),
        )

        if front_image_source.get("source_type") == "alternate_source":
            front_cached_result = {
                "absolute_path": front_image_source["absolute_path"],
            }
        else:
            front_cached_result = try_download_chaos_image_to_cache(
                front_page_entry.get("card_uuid"),
                front_page_entry.get("page_kind"),
                front_page_entry.get("face_name"),
                front_page_entry.get("image_url"),
                context_label="IMAGE EXPORT FRONT",
            )

        if not front_cached_result:
            warning_text = (
                f"Skipped card because front image was unavailable: "
                f"{export_row.get('card_name')} | {card_uuid}"
            )
            export_warnings.append(warning_text)
            write_debug_log(f"IMAGE EXPORT WARNING | {warning_text}")
            continue

        front_bleed_source = resolve_image_export_bleed_source_path(
            front_cached_result,
            front_image_source,
            export_add_bleed,
        )

        try:
            build_export_card_image(
                front_bleed_source["source_path"],
                front_output_path,
                export_row["pack_tracking_code"],
                card_row,
                template_key_override=front_image_source.get("export_frame_template") or "auto",
                add_export_bleed=front_bleed_source["add_export_bleed"],
                use_bleed_template=front_bleed_source["use_bleed_template"],
            )
        except Exception as exc:
            write_debug_log(
                f"IMAGE EXPORT FRONT RENDER FAILED | card_uuid={card_uuid} | "
                f"card_name={export_row.get('card_name')} | error={str(exc)} | skipping card"
            )
            continue

        front_relative_xml_path = f"\\Images\\{finish_folder_name}\\{front_filename}"

        if back_page_entry and back_page_entry.get("image_url"):
            back_filename = f"{base_filename}_back.jpg"
            back_output_path = os.path.join(finish_output_dir, back_filename)

            back_image_source = resolve_card_image_source_for_page(
                card_row,
                back_page_entry.get("page_kind"),
                back_page_entry.get("image_url"),
            )

            if back_image_source.get("source_type") == "alternate_source":
                back_cached_result = {
                    "absolute_path": back_image_source["absolute_path"],
                }
            else:
                back_cached_result = try_download_chaos_image_to_cache(
                    back_page_entry.get("card_uuid"),
                    back_page_entry.get("page_kind"),
                    back_page_entry.get("face_name"),
                    back_page_entry.get("image_url"),
                    context_label="IMAGE EXPORT BACK",
                )

            if back_cached_result:
                back_bleed_source = resolve_image_export_bleed_source_path(
                    back_cached_result,
                    back_image_source,
                    export_add_bleed,
                )

                try:
                    build_export_card_image(
                        back_bleed_source["source_path"],
                        back_output_path,
                        f"{export_row['pack_tracking_code']} - BACK",
                        card_row,
                        template_key_override=back_image_source.get("export_frame_template") or "auto",
                        add_export_bleed=back_bleed_source["add_export_bleed"],
                        use_bleed_template=back_bleed_source["use_bleed_template"],
                    )
                    back_relative_xml_path = f"\\Images\\{finish_folder_name}\\{back_filename}"
                except Exception as exc:
                    write_debug_log(
                        f"IMAGE EXPORT BACK RENDER FAILED | card_uuid={card_uuid} | "
                        f"card_name={export_row.get('card_name')} | error={str(exc)} | using default back"
                    )
                    back_relative_xml_path = f"\\Images\\Default\\{default_back_filename}"
            else:
                write_debug_log(
                    f"IMAGE EXPORT BACK FALLBACK | card_uuid={card_uuid} | "
                    f"card_name={export_row.get('card_name')} | back image unavailable; using default back"
                )
                back_relative_xml_path = f"\\Images\\Default\\{default_back_filename}"
        else:
            back_relative_xml_path = f"\\Images\\Default\\{default_back_filename}"

        xml_entry = {
            "id": export_id,
            "name": export_row["card_name"],
            "setcode": export_row["card_set_code"],
            "collector_number": export_row["collector_number"],
            "front_image_path": front_relative_xml_path,
            "back_image_path": back_relative_xml_path,
            "pack_tracking_code": export_row["pack_tracking_code"],
        }

        if is_foil:
            foil_xml_cards.append(xml_entry)
        else:
            regular_xml_cards.append(xml_entry)

        write_debug_log(
            f"IMAGE EXPORT CARD | id={export_id} | foil={is_foil} | front={front_relative_xml_path} | back={back_relative_xml_path}"
        )

    if get_configured_print_pack_labels():
        pack_label_lookup = {}

        for export_row in export_rows:
            tracked_pack_id = int(export_row["tracked_pack_id"])

            if tracked_pack_id in pack_label_lookup:
                continue

            pack_label_lookup[tracked_pack_id] = {
                "tracked_pack_id": tracked_pack_id,
                "pack_display_name": export_row["pack_display_name"],
                "pack_tracking_code": export_row["pack_tracking_code"],
                "set_code": export_row["pack_set_code"],
                "booster_name": export_row["pack_booster_name"],
            }

        for pack_label in pack_label_lookup.values():
            safe_pack_code = safe_filename(
                pack_label["pack_tracking_code"] or f"pack_{pack_label['tracked_pack_id']}"
            )

            pack_label_filename = f"{safe_pack_code}_label.jpg"
            pack_label_output_path = os.path.join(pack_labels_dir, pack_label_filename)

            save_chaos_pack_label_image_file(
                pack_label_output_path,
                pack_label["pack_display_name"],
                set_code=pack_label["set_code"],
                booster_name=pack_label["booster_name"],
                pack_tracking_code=pack_label["pack_tracking_code"],
                card_width_mm=CARD_PRINT_WIDTH_MM,
                card_height_mm=CARD_PRINT_HEIGHT_MM,
            )

            write_debug_log(
                f"IMAGE EXPORT PACK LABEL | tracked_pack_id={pack_label['tracked_pack_id']} | "
                f"tracking_code={pack_label['pack_tracking_code']} | file=PackLabels/{pack_label_filename}"
            )

    regular_xml_path = os.path.join(export_folder, "Regular.xml")
    foil_xml_path = os.path.join(export_folder, "Foil.xml")

    write_card_image_export_xml(
        regular_xml_path,
        regular_xml_cards,
        foil_value=False,
    )

    write_card_image_export_xml(
        foil_xml_path,
        foil_xml_cards,
        foil_value=True,
    )

    zip_filename = f"{export_folder_name}.zip"
    zip_path = os.path.join(EXPORT_ROOT_DIR, zip_filename)

    zip_image_export_folder(export_folder, zip_path)

    return {
        "export_folder": export_folder,
        "zip_path": zip_path,
        "zip_filename": zip_filename,
        "regular_count": len(regular_xml_cards),
        "foil_count": len(foil_xml_cards),
        "total_count": len(regular_xml_cards) + len(foil_xml_cards),
    }

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
    
    write_debug_log("SCRYFALL IMPORT | Starting import_scryfall_default_cards_into_database()")

    with open(SCRYFALL_DEFAULT_CARDS_PATH, "r", encoding="utf-8") as file_handle:
        raw_json = json.load(file_handle)

    write_debug_log(f"SCRYFALL IMPORT | Loaded JSON file | records={len(raw_json) if isinstance(raw_json, list) else 'invalid'}")

    if not isinstance(raw_json, list):
        raise ValueError("Scryfall default-cards JSON was not a list.")

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM scryfall_default_cards")

    inserted_count = 0

    skipped_multiface_count = 0
    skipped_no_image_count = 0
    skipped_invalid_count = 0

    seen_scryfall_ids = set()
    duplicate_scryfall_ids = 0

    for card_index, card_obj in enumerate(raw_json, start=1):
        if not isinstance(card_obj, dict):
            skipped_invalid_count += 1
            continue

        # Skip multi-face cards (we only want clean single-face printings)
        if card_obj.get("card_faces"):
            skipped_multiface_count += 1
            continue

        image_uris = safe_dict(card_obj.get("image_uris"))
        games = json.dumps(card_obj.get("games", []))
        rarity = (card_obj.get("rarity") or "").strip().lower()
        lang = (card_obj.get("lang") or "").strip().lower()

        if not image_uris:
            skipped_no_image_count += 1
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
            write_debug_log(
                f"SCRYFALL IMPORT | progress inserted={inserted_count} scanned={card_index} "
                f"skipped_multiface={skipped_multiface_count} skipped_no_image={skipped_no_image_count} "
                f"skipped_invalid={skipped_invalid_count}"
            )

    conn.commit()
    conn.close()

    refresh_cards_has_paper_printing()
    refresh_cards_rarity_from_scryfall()

    print("=== SCRYFALL IMPORT COMPLETE ===")
    print("Inserted rows:", inserted_count)
    print("Duplicate scryfall_ids seen during import:", duplicate_scryfall_ids)

    append_refresh_detail_line("=== SCRYFALL IMPORT COMPLETE ===")
    append_refresh_detail_line(f"Inserted rows: {inserted_count}")
    append_refresh_detail_line(f"Duplicate scryfall_ids seen during import: {duplicate_scryfall_ids}")

    write_debug_log(
        f"SCRYFALL IMPORT COMPLETE | inserted={inserted_count} "
        f"duplicates={duplicate_scryfall_ids} "
        f"skipped_multiface={skipped_multiface_count} "
        f"skipped_no_image={skipped_no_image_count} "
        f"skipped_invalid={skipped_invalid_count}"
    )

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

def download_scryfall_set_icons():
    ensure_download_directories()

    set_icon_dir = os.path.join(app.static_folder, SET_ICON_RELATIVE_DIR.replace("/", os.sep))
    os.makedirs(set_icon_dir, exist_ok=True)

    set_refresh_status(
        stage="Downloading Set Icons",
        message="Downloading Scryfall set icon SVG references...",
    )

    headers = {
        "User-Agent": "iMomir/1.0",
        "Accept": "application/json;q=0.9,*/*;q=0.8",
    }

    response = requests.get(
        SCRYFALL_SETS_API_URL,
        headers=headers,
        timeout=60,
    )
    response.raise_for_status()

    payload = response.json()
    set_items = safe_list(payload.get("data"))

    if not set_items:
        raise ValueError("Scryfall sets response did not contain any set data.")

    conn = get_db_connection()
    cursor = conn.cursor()

    ensure_column_exists(cursor, "sets", "scryfall_icon_svg_uri", "TEXT")
    ensure_column_exists(cursor, "sets", "local_icon_svg_path", "TEXT")

    downloaded_count = 0
    skipped_count = 0
    updated_count = 0

    svg_headers = {
        "User-Agent": "iMomir/1.0",
        "Accept": "image/svg+xml,*/*;q=0.8",
    }

    for set_item in set_items:
        set_code = (set_item.get("code") or "").strip().upper()
        icon_svg_uri = (set_item.get("icon_svg_uri") or "").strip()

        if not set_code or not icon_svg_uri:
            skipped_count += 1
            continue

        local_filename = f"{safe_filename(set_code)}.svg"
        local_abs_path = os.path.join(set_icon_dir, local_filename)
        local_rel_path = f"{SET_ICON_RELATIVE_DIR}/{local_filename}"

        try:
            should_download = True

            if os.path.exists(local_abs_path) and os.path.getsize(local_abs_path) > 0:
                should_download = False

            if should_download:
                icon_response = requests.get(
                    icon_svg_uri,
                    headers=svg_headers,
                    timeout=60,
                )
                icon_response.raise_for_status()

                icon_text = icon_response.text or ""

                if "<svg" not in icon_text.lower():
                    raise ValueError(f"Scryfall icon response for {set_code} did not look like SVG.")

                with open(local_abs_path, "w", encoding="utf-8") as icon_file:
                    icon_file.write(icon_text)

                downloaded_count += 1
            else:
                skipped_count += 1

            cursor.execute(
                """
                UPDATE sets
                SET scryfall_icon_svg_uri = ?,
                    local_icon_svg_path = ?
                WHERE set_code = ?
                """,
                (
                    icon_svg_uri,
                    local_rel_path,
                    set_code,
                ),
            )

            if cursor.rowcount > 0:
                updated_count += 1

        except Exception as exc:
            skipped_count += 1
            write_debug_log(
                f"SCRYFALL SET ICON DOWNLOAD FAILED | set={set_code} | url={icon_svg_uri} | error={str(exc)}"
            )
            continue

    conn.commit()
    conn.close()

    append_refresh_detail_line(
        f"=== SCRYFALL SET ICONS COMPLETE === downloaded={downloaded_count}, skipped={skipped_count}, updated_sets={updated_count}"
    )

    set_refresh_status(
        stage="Downloading Set Icons",
        message=f"Set icon sync complete. Downloaded {downloaded_count}; updated {updated_count} set rows.",
    )

    return {
        "downloaded_count": downloaded_count,
        "skipped_count": skipped_count,
        "updated_count": updated_count,
    }

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

    cursor.execute("DELETE FROM sets WHERE COALESCE(set_type, '') <> 'custom'")

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

def ensure_card_has_local_image(card_row):
    if not card_row:
        return None

    existing_cache_path = (card_row["image_cache_path"] or "").strip()
    if existing_cache_path:
        absolute_path = os.path.abspath(existing_cache_path)
        if os.path.exists(absolute_path):
            return card_row

    refreshed_card = ensure_card_image_cached(card_row)
    if not refreshed_card:
        return None

    refreshed_cache_path = (refreshed_card["image_cache_path"] or "").strip()
    if refreshed_cache_path:
        absolute_path = os.path.abspath(refreshed_cache_path)
        if os.path.exists(absolute_path):
            return refreshed_card

    return refreshed_card

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
            detail_lines=[],
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
            stage="Downloading Set Icons",
            message="Downloading Scryfall set icons...",
        )
        download_scryfall_set_icons()

        set_refresh_status(
            stage="Importing Cards - DO NOT LEAVE PAGE",
            message=f"{download_result['reason']} Beginning import from local AtomicCards.json...",
            sets_represented=imported_sets,
        )

        set_refresh_status(
            stage="Preparing Pack Art Folders",
            message="Creating Chaos Draft pack art set folders...",
            sets_represented=imported_sets,
        )

        created_pack_art_folders = ensure_chaos_pack_art_set_folders()

        summary = import_atomic_cards_into_database()

        scryfall_bulk_result = download_scryfall_default_cards_json(force_download=False)

        set_refresh_status(
            stage="Indexing Paper Printings",
            message=scryfall_bulk_result["message"],
            cards_processed=summary["cards_imported"],
            cards_imported=summary["cards_imported"],
            sets_represented=summary["sets_represented"],
        )

        scryfall_rows_imported = import_scryfall_default_cards_into_database()

        print("=== CHAOS DRAFT: BEGIN ALLPRINTINGS DOWNLOAD ===")

        set_refresh_status(
            stage="Downloading Chaos Draft Card Data",
            message="Downloading AllPrintings.json for Chaos Draft...",
            cards_processed=summary["cards_imported"],
            cards_imported=summary["cards_imported"],
            sets_represented=summary["sets_represented"],
        )

        all_printings_result = download_all_printings_json(force_download=True)
        append_refresh_detail_line(f"=== ALLPRINTINGS RESULT MESSAGE === {all_printings_result.get('message', '')}")
        print("=== CHAOS DRAFT: DOWNLOAD RESULT ===", all_printings_result)
        print("=== CHAOS DRAFT: ALL_PRINTINGS_PATH EXISTS ===", os.path.exists(ALL_PRINTINGS_PATH))
        if os.path.exists(ALL_PRINTINGS_PATH):
            print("=== CHAOS DRAFT: ALL_PRINTINGS_PATH SIZE ===", os.path.getsize(ALL_PRINTINGS_PATH))

        append_refresh_detail_line(f"=== CHAOS DRAFT: DOWNLOAD RESULT === {all_printings_result}")
        append_refresh_detail_line(f"=== CHAOS DRAFT: ALL_PRINTINGS_PATH EXISTS === {os.path.exists(ALL_PRINTINGS_PATH)}")
        if os.path.exists(ALL_PRINTINGS_PATH):
            append_refresh_detail_line(f"=== CHAOS DRAFT: ALL_PRINTINGS_PATH SIZE === {os.path.getsize(ALL_PRINTINGS_PATH)}")

        set_refresh_status(
            stage="Importing Chaos Draft Cards",
            message="Importing AllPrintings.json into Chaos Draft card tables...",
            cards_processed=summary["cards_imported"],
            cards_imported=summary["cards_imported"],
            sets_represented=summary["sets_represented"],
        )

        chaos_cards_imported = import_chaos_cards_from_all_printings()
        print("=== CHAOS DRAFT: IMPORTED CHAOS CARDS ===", chaos_cards_imported)
        append_refresh_detail_line(f"=== CHAOS DRAFT: IMPORTED CHAOS CARDS === {chaos_cards_imported}")

        set_refresh_status(
            stage="Importing Chaos Draft Booster Data",
            message="Importing MTGJSON Chaos Draft booster CSV data...",
            cards_processed=summary["cards_imported"],
            cards_imported=summary["cards_imported"],
            sets_represented=summary["sets_represented"],
        )

        download_chaos_booster_csvs(force_download=True)
        import_chaos_booster_data()

        set_refresh_status(
            stage="Downloading Price Data",
            message="Downloading MTGJSON AllPricesToday data...",
            cards_processed=summary["cards_imported"],
            cards_imported=summary["cards_imported"],
            sets_represented=summary["sets_represented"],
        )

        download_all_prices_today_json()

        set_refresh_status(
            stage="Importing Price Data",
            message="Importing card prices into SQLite...",
            cards_processed=summary["cards_imported"],
            cards_imported=summary["cards_imported"],
            sets_represented=summary["sets_represented"],
        )

        imported_price_rows = import_all_prices_today_into_database()
        set_import_metadata("card_prices_imported", imported_price_rows)

        refresh_finished_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        set_import_metadata("last_refresh_utc", refresh_finished_at)
        set_import_metadata("source_url", MTGJSON_ATOMIC_URL)
        set_import_metadata("cards_imported", summary["cards_imported"])
        set_import_metadata("sets_represented", summary["sets_represented"])
        set_import_metadata("chaos_cards_imported", chaos_cards_imported)
        set_import_metadata("chaos_booster_data_imported_at", refresh_finished_at)
        set_import_metadata("chaos_pack_art_folders_created", created_pack_art_folders)

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
    config = get_request_config()
    current_game_mode = (config.get("game_mode") or "custom").strip().lower()

    if current_game_mode in {
        "chaos_draft",
        "chaos_draft_campaign",
        "preprint_chaos_draft",
    }:
        return redirect(url_for("result"))

    selected_type_info = resolve_selected_result_type(
        config,
        request.args.get("selected_type", ""),
    )

    pdf_settings = resolve_pdf_print_settings()

    return render_template(
        "index.html",
        card_database_ready=is_card_database_ready(),
        current_game_mode=current_game_mode,
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
    config = get_request_config()
    print_settings = resolve_print_settings()
    current_game_mode = (config.get("game_mode") or "custom").strip().lower()
    selected_type_info = resolve_selected_result_type(config, selected_type_value)

    if current_game_mode == "chaos_draft":
        clear_chaos_session_state("pending_spin_result")

        active_template_metadata = get_active_print_template_metadata()

        return render_template(
            "chaos_draft.html",
            card_database_ready=card_database_ready,
            current_game_mode=current_game_mode,
            open_print_in_new_tab=print_settings["open_in_new_tab"],
            sound_enabled=(config.get("sound_enabled") or "1").strip() == "1",
            chaos_draft_export_format=(config.get("chaos_draft_export_format") or "none").strip().lower(),
            enable_track_packs=(config.get("enable_track_packs") or "0").strip() == "1",
            template_download_links=active_template_metadata["download_links"],
        )

    if current_game_mode == "chaos_draft_campaign":
        clear_chaos_session_state("pending_spin_result")
        clear_chaos_session_state("pending_campaign_pack_opening_recorded")

        active_template_metadata = get_active_print_template_metadata()

        selected_chaos_campaign_id = get_selected_chaos_campaign_id()
        campaign_players = get_campaign_players(
            include_disabled=False,
            campaign_id=selected_chaos_campaign_id,
        )
        selected_campaign_player_id = get_selected_campaign_player_id(
            campaign_id=selected_chaos_campaign_id,
        )
        chaos_campaigns = get_chaos_campaigns(include_disabled=False)
        active_draft_game = get_selected_or_create_chaos_draft_game(
            campaign_id=selected_chaos_campaign_id,
        )
        active_draft_game_label = get_chaos_draft_game_display_label(active_draft_game)

        return render_template(
            "campaign_chaos_draft.html",
            card_database_ready=card_database_ready,
            current_game_mode=current_game_mode,
            open_print_in_new_tab=print_settings["open_in_new_tab"],
            sound_enabled=(config.get("sound_enabled") or "1").strip() == "1",
            chaos_draft_export_format=(config.get("chaos_draft_export_format") or "none").strip().lower(),
            template_download_links=active_template_metadata["download_links"],
            campaign_players=campaign_players,
            selected_campaign_player_id=selected_campaign_player_id,
            chaos_campaigns=chaos_campaigns,
            selected_chaos_campaign_id=selected_chaos_campaign_id,
            active_draft_game=active_draft_game,
            active_draft_game_label=active_draft_game_label,
        )

    if current_game_mode == "preprint_chaos_draft":
        active_template_metadata = get_active_print_template_metadata()

        return render_template(
            "preprint_chaos_draft.html",
            card_database_ready=card_database_ready,
            current_game_mode=current_game_mode,
            open_print_in_new_tab=print_settings["open_in_new_tab"],
            default_player_count=4,
            default_packs_per_player=3,
            template_download_links=active_template_metadata["download_links"],
        )

    if current_game_mode == "tower_of_power":
        card = draw_random_tower_of_power_card() if card_database_ready else None

        if card:
            card = ensure_card_has_local_image(card)

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
            open_print_in_new_tab=print_settings["open_in_new_tab"],
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
            open_print_in_new_tab=print_settings["open_in_new_tab"],
        )

    draw_type_value = selected_type_info["selected_value"] if current_game_mode == "momir_select" else None
    card = draw_random_card(int(mana_value), selected_type_value=draw_type_value)

    if card:
        card = ensure_card_has_local_image(card)

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
        open_print_in_new_tab=print_settings["open_in_new_tab"],
    )

@app.route("/print/<card_key>")
def print_card(card_key):
    card = get_card_by_key(card_key)

    if not card:
        return "Card not found", 404

    return render_print_page(
        card=card,
        image_src=url_for("card_image", card_key=card["card_key"]),
    )

@app.route("/print-pdf/<card_key>")
def print_card_pdf(card_key):
    card = get_card_by_key(card_key)

    if not card:
        return "Card not found", 404

    card = ensure_card_has_local_image(card)

    image_path = (card["image_cache_path"] or "").strip() if card else ""
    if not image_path:
        return "Image not available", 404

    absolute_image_path = os.path.abspath(image_path)
    if not os.path.exists(absolute_image_path):
        return "Image not available", 404

    pdf_filename_base = (card["scryfall_id"] or "").strip()
    if not pdf_filename_base:
        pdf_filename_base = safe_filename(card["card_key"])

    pdf_buffer = build_single_image_pdf_buffer(absolute_image_path)
    return build_inline_pdf_response(pdf_buffer, f"{pdf_filename_base}.pdf")

@app.route("/print-pdf/tower-of-power-batch")
def print_tower_of_power_batch_pdf():
    if not is_card_database_ready():
        return "Card database not ready", 400

    pdf_settings = resolve_pdf_print_settings()
    if not pdf_settings["use_pdf_print"]:
        return "PDF printing is disabled", 400

    requested_draw_count = request.args.get("draw_count", "").strip()
    draw_count = save_tower_pdf_draw_count(requested_draw_count)

    cards = draw_tower_of_power_batch_cards(draw_count, ensure_card_image_cached)
    if not cards:
        return "No matching cards found", 404

    pdf_template_layout = resolve_pdf_template_layout()
    print_settings = resolve_print_settings()

    width_mm = pdf_template_layout["page_width_mm"]
    height_mm = pdf_template_layout["page_height_mm"]
    crop_border = pdf_settings["pdf_crop_border"]

    draw_x_mm = pdf_template_layout["draw_x_mm"]
    draw_y_mm = pdf_template_layout["draw_y_mm"]
    draw_width_mm = pdf_template_layout["draw_width_mm"]
    draw_height_mm = pdf_template_layout["draw_height_mm"]

    if crop_border and not pdf_template_layout["uses_fixed_inner_margin"]:
        crop_left_right_mm = width_mm * 0.05
        crop_top_bottom_mm = height_mm * 0.034

        draw_x_mm = -crop_left_right_mm
        draw_y_mm = -crop_top_bottom_mm
        draw_width_mm = width_mm + (crop_left_right_mm * 2)
        draw_height_mm = height_mm + (crop_top_bottom_mm * 2)

    valid_card_paths = []
    for card in cards:
        image_path = card["image_cache_path"] or ""
        absolute_image_path = os.path.abspath(image_path) if image_path else ""

        if absolute_image_path and os.path.exists(absolute_image_path):
            valid_card_paths.append(absolute_image_path)

    if not valid_card_paths:
        return "No matching card images found", 404

    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=(width_mm * mm, height_mm * mm))

    if pdf_template_layout.get("is_multi_card_layout", False) and pdf_template_layout["print_template"] == "borderless-3p5x5-two-card":
        slot_defs = get_two_card_borderless_slots_mm()

        for page_start_index in range(0, len(valid_card_paths), 2):
            page_card_paths = valid_card_paths[page_start_index:page_start_index + 2]

            for slot_index, card_path in enumerate(page_card_paths):
                draw_processed_image_into_two_card_slot(
                    c,
                    card_path,
                    print_settings["print_mode"],
                    slot_defs[slot_index],
                )

            c.showPage()
    else:
        for card_path in valid_card_paths:
            pdf_image_reader = build_pdf_image_reader(card_path, print_settings["print_mode"])

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
            "Content-Disposition": f"inline; filename=tower_of_power_{len(valid_card_paths)}_cards.pdf"
        }
    )

@app.route("/print-custom/default-momir-vig")
def print_custom_default_momir_vig():
    selected_variant = resolve_default_momir_variant()

    return render_print_page(
        card={
            "name": CARD_SEARCH_DEFAULT_TITLE,
            "type_line": f"Avatar • {selected_variant['label']}",
        },
        image_src=url_for("static", filename=selected_variant["filename"]),
    )

@app.route("/print-pdf-custom/default-momir-vig")
def print_custom_default_momir_vig_pdf():
    selected_variant = resolve_default_momir_variant()
    image_path = os.path.join(app.static_folder, selected_variant["filename"].replace("/", os.sep))

    if not os.path.exists(image_path):
        return "Image not available", 404

    pdf_buffer = build_single_image_pdf_buffer(image_path)
    return build_inline_pdf_response(pdf_buffer, "default_momir_vig.pdf")

@app.route("/print-custom/game-mode/<mode_value>")
def print_custom_game_mode(mode_value):
    mode_map = get_game_mode_option_map()
    mode_item = mode_map.get((mode_value or "").strip().lower())

    if not mode_item:
        return "Game mode token not found", 404

    token_image = resolve_game_mode_token_image(mode_value)

    return render_print_page(
        card={
            "name": mode_item["label"],
            "type_line": "Avatar • Game Mode Token",
        },
        image_src=url_for("static", filename=token_image["filename"]),
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

    safe_mode_value = (mode_value or "game_mode").strip().replace("/", "_")
    pdf_buffer = build_single_image_pdf_buffer(image_path)
    return build_inline_pdf_response(pdf_buffer, f"{safe_mode_value}.pdf")

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

    config_values = get_request_config()
    import_metadata = get_import_metadata()
    current_refresh_status = build_config_page_refresh_status(import_metadata)
    current_image_status = build_config_page_image_status()

    resolved_game_mode_cards = []
    hidden_config_game_modes = {
        # This mode is still valid internally, but it is no longer shown as a selectable mode.
        "preprint_chaos_draft",
    }

    for item in GAME_MODE_OPTIONS:
        mode_value = (item.get("value") or "").strip().lower()

        if mode_value in hidden_config_game_modes:
            continue

        token_image = resolve_game_mode_token_image(mode_value)
        resolved_game_mode_cards.append({
            **item,
            "image_src": url_for("static", filename=token_image["filename"]),
            "print_href": get_game_mode_print_href(mode_value),
        })

    game_mode_group_definitions = [
        {
            "key": "momir",
            "label": "Momir",
            "values": [
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
            ],
        },
        {
            "key": "chaos",
            "label": "Chaos Draft",
            "values": [
                "chaos_draft_campaign",
                "chaos_draft",
            ],
        },
        {
            "key": "other",
            "label": "Other",
            "values": [
                "planechase",
                "archenemy",
            ],
        },
    ]

    game_mode_lookup = {
        (mode.get("value") or "").strip().lower(): mode
        for mode in resolved_game_mode_cards
    }

    grouped_game_modes = []

    for group_definition in game_mode_group_definitions:
        group_modes = []

        for mode_value in group_definition["values"]:
            mode = game_mode_lookup.get(mode_value)
            if mode:
                group_modes.append(mode)

        if group_modes:
            grouped_game_modes.append({
                "key": group_definition["key"],
                "label": group_definition["label"],
                "modes": group_modes,
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
        grouped_game_modes=grouped_game_modes,
        repeat_mode_options=REPEAT_MODE_OPTIONS,
        print_template_options=PRINT_TEMPLATE_OPTIONS,
        print_color_mode_options=PRINT_COLOR_MODE_OPTIONS,
        chaos_draft_export_format_options=CHAOS_DRAFT_EXPORT_FORMAT_OPTIONS,
        scryfall_image_quality_options=SCRYFALL_IMAGE_QUALITY_OPTIONS,
        momir_default_token_variant_options=MOMIR_DEFAULT_TOKEN_VARIANT_OPTIONS,
        import_metadata=import_metadata,
        refresh_status=current_refresh_status,
        image_download_status=current_image_status,
        section_defaults=section_defaults,
        history_count=get_recent_history_count(),
        qr_access_url=access_url,
        qr_image_url=build_qr_code_image_url(access_url),
    )

@app.route("/maintenance/clear-exports", methods=["POST"])
def maintenance_clear_exports():
    result = clear_export_root()

    flash(f"Cleared Export folder. Removed {result['removed_count']} item(s).")
    return redirect(url_for("config"))

@app.route("/maintenance/backup-imomir", methods=["POST"])
def maintenance_backup_imomir():
    try:
        backup_result = export_full_archive(
            auto_clear_exports_value=get_auto_clear_exports_config_value(),
        )
    except Exception as exc:
        return str(exc), 400

    return send_file(
        backup_result["zip_path"],
        mimetype="application/zip",
        as_attachment=True,
        download_name=backup_result["zip_filename"],
        max_age=0,
    )


@app.route("/maintenance/import-imomir", methods=["POST"])
def maintenance_import_imomir():
    backup_file = request.files.get("backup_file")

    try:
        import_result = import_archive_from_file_object(
            backup_file,
            EXPORT_KIND_FULL,
        )
    except Exception as exc:
        flash(f"iMomir import failed: {str(exc)}")
        return redirect(url_for("config"))

    flash(
        f"iMomir import complete. Imported {import_result['imported_rows']} row(s) "
        f"and restored {import_result['extracted_files']} file(s)."
    )

    return redirect(url_for("config"))

@app.route("/maintenance/backup-all-packs", methods=["POST"])
def maintenance_backup_all_packs():
    try:
        backup_result = export_packs_archive(
            [],
            auto_clear_exports_value=get_auto_clear_exports_config_value(),
        )
    except Exception as exc:
        return str(exc), 400

    return send_file(
        backup_result["zip_path"],
        mimetype="application/zip",
        as_attachment=True,
        download_name=backup_result["zip_filename"],
        max_age=0,
    )


@app.route("/maintenance/clear-all-history", methods=["POST"])
def maintenance_clear_all_history():
    delete_confirmation = (request.form.get("delete_confirmation") or "").strip()

    if delete_confirmation != "DELETE":
        flash("Clear All History cancelled. Type DELETE to confirm.")
        return redirect(url_for("config"))

    result = clear_all_history_data()

    flash(f"Cleared all history. Deleted {result['total_deleted']} row(s).")
    return redirect(url_for("config"))


@app.route("/maintenance/clear-all-packs", methods=["POST"])
def maintenance_clear_all_packs():
    delete_confirmation = (request.form.get("delete_confirmation") or "").strip()

    if delete_confirmation != "DELETE":
        flash("Clear All Packs cancelled. Type DELETE to confirm.")
        return redirect(url_for("config"))

    result = clear_all_packs_data()

    flash(f"Cleared all packs. Deleted {result['total_deleted']} row(s).")
    return redirect(url_for("config"))

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

    card = ensure_card_has_local_image(card)

    if card:
        cache_path = (card["image_cache_path"] or "").strip()
        if cache_path:
            abs_path = os.path.abspath(cache_path)
            if os.path.exists(abs_path):
                return send_file(abs_path)

        if card["image_url"]:
            return redirect(card["image_url"])

    return ("Not found", 404)

@app.route("/chaos-draft/packs", methods=["GET"])
def chaos_draft_packs():
    packs = get_eligible_chaos_packs(app.static_folder)

    return jsonify({
        "ok": True,
        "pack_count": len(packs),
        "packs": packs,
    })


@app.route("/chaos-draft/open-test", methods=["GET"])
def chaos_draft_open_test():
    eligible_packs = get_eligible_chaos_packs_for_spin(app.static_folder)

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

    config = get_request_config()
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
        write_debug_log,
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

@app.route("/campaign-chaos/campaigns", methods=["GET"])
def campaign_chaos_campaigns():
    campaigns = get_chaos_campaigns(include_disabled=True)
    selected_chaos_campaign_id = get_selected_chaos_campaign_id()

    return render_template(
        "campaign_campaigns.html",
        campaigns=campaigns,
        selected_chaos_campaign_id=selected_chaos_campaign_id,
    )

@app.route("/campaign-chaos/campaigns/default/save", methods=["POST"])
def campaign_chaos_campaigns_default_save():
    set_selected_chaos_campaign_id(None)

    clear_chaos_session_state("selected_campaign_player_id")
    clear_chaos_session_state("selected_chaos_draft_game_id")
    clear_chaos_session_state("pending_spin_result")
    clear_chaos_session_state("pending_opened_pack")
    clear_chaos_session_state("pending_campaign_pack_opening_recorded")

    flash("No Campaign selected.")
    return redirect(url_for("campaign_chaos_campaigns"))


@app.route("/campaign-chaos/campaigns/default/backup", methods=["POST"])
def campaign_chaos_campaigns_default_backup():
    try:
        backup_result = export_default_campaign_archive(
            auto_clear_exports_value=get_auto_clear_exports_config_value(),
        )
    except Exception as exc:
        return str(exc), 400

    return send_file(
        backup_result["zip_path"],
        mimetype="application/zip",
        as_attachment=True,
        download_name=backup_result["zip_filename"],
        max_age=0,
    )

@app.route("/campaign-chaos/campaigns/import-backup", methods=["POST"])
def campaign_chaos_campaigns_import_backup():
    backup_file = request.files.get("backup_file")
    campaign_name_override = (request.form.get("campaign_name_override") or "").strip()

    try:
        import_result = import_archive_from_file_object(
            backup_file,
            EXPORT_KIND_CAMPAIGN,
            campaign_name_override=campaign_name_override,
        )
    except Exception as exc:
        flash(f"Campaign import failed: {str(exc)}")
        return redirect(url_for("campaign_chaos_campaigns"))

    flash(
        f"Campaign import complete. Imported {import_result['imported_rows']} row(s) "
        f"and restored {import_result['extracted_files']} file(s)."
    )

    return redirect(url_for("campaign_chaos_campaigns"))


@app.route("/campaign-chaos/campaigns/<int:campaign_id>/backup", methods=["POST"])
def campaign_chaos_campaigns_backup(campaign_id):
    try:
        backup_result = export_campaign_archive(
            campaign_id,
            auto_clear_exports_value=get_auto_clear_exports_config_value(),
        )
    except Exception as exc:
        return str(exc), 400

    return send_file(
        backup_result["zip_path"],
        mimetype="application/zip",
        as_attachment=True,
        download_name=backup_result["zip_filename"],
        max_age=0,
    )

@app.route("/campaign-chaos/campaigns/add", methods=["POST"])
def campaign_chaos_campaigns_add():
    campaign_name = (request.form.get("campaign_name") or "").strip()

    create_result = create_chaos_campaign(campaign_name)

    flash(create_result.get("message") or "Campaign added.")
    return redirect(url_for("campaign_chaos_campaigns"))


@app.route("/campaign-chaos/campaigns/<int:campaign_id>/update", methods=["POST"])
def campaign_chaos_campaigns_update(campaign_id):
    campaign_name = (request.form.get("campaign_name") or "").strip()
    is_active = request.form.get("is_active") == "on"

    update_result = update_chaos_campaign(
        campaign_id,
        campaign_name=campaign_name,
        is_active=is_active,
    )

    flash(update_result.get("message") or "Campaign updated.")
    return redirect(url_for("campaign_chaos_campaigns"))


@app.route("/campaign-chaos/campaigns/<int:campaign_id>/delete", methods=["POST"])
def campaign_chaos_campaigns_delete(campaign_id):
    delete_confirmation = (request.form.get("delete_confirmation") or "").strip()

    if delete_confirmation != "DELETE":
        flash("Delete cancelled. To delete a campaign, type DELETE in the confirmation prompt.")
        return redirect(url_for("campaign_chaos_campaigns"))

    delete_result = delete_chaos_campaign(campaign_id)

    flash(delete_result.get("message") or "Campaign deleted.")
    return redirect(url_for("campaign_chaos_campaigns"))


@app.route("/campaign-chaos/select-campaign", methods=["POST"])
def campaign_chaos_select_campaign():
    payload = request.get_json(silent=True) or {}
    campaign_id = payload.get("campaign_id")

    result = set_selected_chaos_campaign_id(campaign_id)

    if not result.get("ok"):
        return jsonify(result), 400

    clear_chaos_session_state("selected_campaign_player_id")
    clear_chaos_session_state("selected_chaos_draft_game_id")
    clear_chaos_session_state("pending_spin_result")
    clear_chaos_session_state("pending_opened_pack")
    clear_chaos_session_state("pending_campaign_pack_opening_recorded")
    return jsonify(result)

@app.route("/campaign-chaos/players", methods=["GET"])
def campaign_chaos_players():
    selected_chaos_campaign_id = get_selected_chaos_campaign_id()
    selected_chaos_campaign = get_chaos_campaign_by_id(selected_chaos_campaign_id) if selected_chaos_campaign_id else None

    players = get_campaign_players(
        include_disabled=True,
        campaign_id=selected_chaos_campaign_id,
    )
    selected_campaign_player_id = get_selected_campaign_player_id(
        campaign_id=selected_chaos_campaign_id,
    )

    import_campaign_options = get_campaign_player_import_options(
        current_campaign_id=selected_chaos_campaign_id,
    )

    return render_template(
        "campaign_players.html",
        players=players,
        selected_campaign_player_id=selected_campaign_player_id,
        selected_chaos_campaign=selected_chaos_campaign,
        import_campaign_options=import_campaign_options,
    )

@app.route("/campaign-chaos/player-portrait/<filename>", methods=["GET"])
def campaign_chaos_player_portrait(filename):
    safe_name = safe_filename(filename)
    portrait_path = os.path.join(CAMPAIGN_PLAYER_PORTRAIT_DIR, safe_name)

    if not os.path.exists(portrait_path):
        return ("Not found", 404)

    return send_file(portrait_path)

@app.route("/campaign-chaos/players/import", methods=["POST"])
def campaign_chaos_players_import():
    source_campaign_id = request.form.get("source_campaign_id")

    if source_campaign_id == "__none__":
        source_campaign_id = None

    target_campaign_id = get_selected_chaos_campaign_id()

    import_result = import_campaign_players_from_campaign(
        source_campaign_id=source_campaign_id,
        target_campaign_id=target_campaign_id,
    )

    flash(import_result.get("message") or "Player import complete.")
    return redirect(url_for("campaign_chaos_players"))

@app.route("/campaign-chaos/players/add", methods=["POST"])
def campaign_chaos_players_add():
    player_name = (request.form.get("player_name") or "").strip()

    create_result = create_campaign_player(
        player_name,
        campaign_id=get_selected_chaos_campaign_id(),
    )

    if not create_result.get("ok"):
        flash(create_result.get("message") or "Could not add player.")
        return redirect(url_for("campaign_chaos_players"))

    player_id = create_result.get("player_id")
    portrait_file = request.files.get("portrait_file")

    if portrait_file and portrait_file.filename:
        try:
            portrait_filename = save_campaign_player_portrait_file(portrait_file, player_id)
            if portrait_filename:
                update_campaign_player(player_id, portrait_image_path=portrait_filename)
        except Exception as exc:
            flash(f"Player added, but portrait upload failed: {str(exc)}")
            return redirect(url_for("campaign_chaos_players"))

    flash(create_result.get("message") or "Player added.")
    return redirect(url_for("campaign_chaos_players"))


@app.route("/campaign-chaos/players/<int:player_id>/update", methods=["POST"])
def campaign_chaos_players_update(player_id):
    player_name = (request.form.get("player_name") or "").strip()
    is_active = request.form.get("is_active") == "on"
    portrait_file = request.files.get("portrait_file")

    portrait_filename = None

    if portrait_file and portrait_file.filename:
        try:
            portrait_filename = save_campaign_player_portrait_file(portrait_file, player_id)
        except Exception as exc:
            flash(f"Portrait upload failed: {str(exc)}")
            return redirect(url_for("campaign_chaos_players"))

    update_result = update_campaign_player(
        player_id,
        player_name=player_name,
        portrait_image_path=portrait_filename if portrait_filename else None,
        is_active=is_active,
    )

    flash(update_result.get("message") or "Player updated.")
    return redirect(url_for("campaign_chaos_players"))


@app.route("/campaign-chaos/players/<int:player_id>/delete", methods=["POST"])
def campaign_chaos_players_delete(player_id):
    delete_confirmation = (request.form.get("delete_confirmation") or "").strip()

    if delete_confirmation != "DELETE":
        flash("Delete cancelled. To delete a player, type DELETE in the confirmation prompt.")
        return redirect(url_for("campaign_chaos_players"))

    delete_result = delete_campaign_player(player_id)

    flash(delete_result.get("message") or "Player deleted.")
    return redirect(url_for("campaign_chaos_players"))


@app.route("/campaign-chaos/select-player", methods=["POST"])
def campaign_chaos_select_player():
    payload = request.get_json(silent=True) or {}
    player_id = payload.get("player_id")

    result = set_selected_campaign_player_id(
        player_id,
        campaign_id=get_selected_chaos_campaign_id(),
    )

    if not result.get("ok"):
        return jsonify(result), 400

    return jsonify(result)

def build_manage_pack_summary_rows(packs):
    summary_lookup = {}

    for pack in packs or []:
        if not pack.get("campaign_enabled"):
            continue

        group_key = (
            (pack.get("pack_display_name") or "").strip(),
            (pack.get("set_code") or "").strip().upper(),
            (pack.get("booster_name") or "").strip().lower(),
        )

        if group_key not in summary_lookup:
            summary_lookup[group_key] = {
                "pack_display_name": (pack.get("pack_display_name") or "").strip(),
                "set_code": (pack.get("set_code") or "").strip().upper(),
                "booster_name": (pack.get("booster_name") or "").strip().lower(),
                "quantity": 0,
                "pack_ids": [],
                "first_added_at_utc": pack.get("added_at_utc") or "",
                "latest_added_at_utc": pack.get("added_at_utc") or "",
                "release_date": "",
            }

        summary_row = summary_lookup[group_key]
        summary_row["quantity"] += 1
        summary_row["pack_ids"].append(int(pack["tracked_pack_id"]))

        added_at_utc = pack.get("added_at_utc") or ""
        if added_at_utc and (not summary_row["first_added_at_utc"] or added_at_utc < summary_row["first_added_at_utc"]):
            summary_row["first_added_at_utc"] = added_at_utc

        if added_at_utc and (not summary_row["latest_added_at_utc"] or added_at_utc > summary_row["latest_added_at_utc"]):
            summary_row["latest_added_at_utc"] = added_at_utc

    summary_rows = list(summary_lookup.values())

    if summary_rows:
        set_codes = sorted({
            row["set_code"]
            for row in summary_rows
            if row.get("set_code")
        })

        if set_codes:
            placeholders = ",".join(["?"] * len(set_codes))

            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute(
                f"""
                SELECT
                    set_code,
                    release_date
                FROM sets
                WHERE set_code IN ({placeholders})
                """,
                set_codes,
            )

            release_lookup = {
                (row["set_code"] or "").strip().upper(): row["release_date"] or ""
                for row in cursor.fetchall()
            }

            conn.close()

            for summary_row in summary_rows:
                summary_row["release_date"] = release_lookup.get(summary_row["set_code"], "")

    summary_rows.sort(
        key=lambda row: (
            -int(row["quantity"] or 0),
            (row["pack_display_name"] or "").lower(),
            row["set_code"],
        )
    )

    return {
        "total_enabled_packs": sum(int(row["quantity"] or 0) for row in summary_rows),
        "rows": summary_rows,
    }

@app.route("/campaign-chaos/packs", methods=["GET"])
def campaign_chaos_packs():
    search_text = (request.args.get("q") or "").strip()
    selected_chaos_campaign_id = get_selected_chaos_campaign_id()
    selected_chaos_campaign = get_chaos_campaign_by_id(selected_chaos_campaign_id) if selected_chaos_campaign_id else None

    pack_import_campaign_options = get_campaign_pack_import_options(
        current_campaign_id=selected_chaos_campaign_id,
    )

    packs = get_tracked_pack_management_rows(
        app.static_folder,
        search_text=search_text,
        campaign_id=selected_chaos_campaign_id,
    )

    pack_summary = build_manage_pack_summary_rows(packs)

    return render_template(
        "campaign_manage_packs.html",
        packs=packs,
        search_text=search_text,
        selected_chaos_campaign=selected_chaos_campaign,
        selected_chaos_campaign_id=selected_chaos_campaign_id,
        pack_import_campaign_options=pack_import_campaign_options,
        pack_summary=pack_summary,
        enable_chaos_card_image_export=(get_request_config().get("enable_chaos_card_image_export") or "0").strip() == "1",
        custom_title_pack_type_options=get_custom_title_sheet_pack_type_options(),
    )

@app.route("/campaign-chaos/history", methods=["GET"])
def campaign_chaos_history():
    selected_chaos_campaign_id = get_selected_chaos_campaign_id()

    raw_campaign_id = request.args.get("campaign_id")
    raw_draft_game_id = request.args.get("draft_game_id")
    raw_player_id = request.args.get("player_id")
    raw_tracked_pack_id = request.args.get("tracked_pack_id")
    raw_page = request.args.get("page", "1")

    if raw_campaign_id is None:
        selected_filter_campaign_id = selected_chaos_campaign_id
    else:
        selected_filter_campaign_id = raw_campaign_id.strip() or None

    selected_filter_tracked_pack_id = raw_tracked_pack_id.strip() if raw_tracked_pack_id else None

    if raw_draft_game_id is None and not selected_filter_tracked_pack_id:
        active_draft_game = get_active_chaos_draft_game(
            campaign_id=selected_filter_campaign_id,
        )
        selected_filter_draft_game_id = str(active_draft_game["draft_game_id"]) if active_draft_game else None
    else:
        selected_filter_draft_game_id = raw_draft_game_id.strip() if raw_draft_game_id else None

    selected_filter_player_id = raw_player_id.strip() if raw_player_id else None

    filter_options = get_campaign_history_filter_options(
        app.static_folder,
        campaign_id=selected_filter_campaign_id,
        tracked_pack_id=selected_filter_tracked_pack_id,
    )

    history_result = get_campaign_history_rows(
        app.static_folder,
        campaign_id=selected_filter_campaign_id,
        draft_game_id=selected_filter_draft_game_id,
        player_id=selected_filter_player_id,
        tracked_pack_id=selected_filter_tracked_pack_id,
        page=raw_page,
        per_page=50,
    )

    selected_campaign = get_chaos_campaign_by_id(selected_filter_campaign_id) if selected_filter_campaign_id else None

    tracked_pack_context = None
    if selected_filter_tracked_pack_id:
        tracked_pack_context = get_tracked_pack_state_by_id(selected_filter_tracked_pack_id)

    return render_template(
        "campaign_history.html",
        selected_campaign=selected_campaign,
        selected_campaign_id=int(selected_filter_campaign_id) if selected_filter_campaign_id else None,
        selected_draft_game_id=int(selected_filter_draft_game_id) if selected_filter_draft_game_id else None,
        selected_player_id=int(selected_filter_player_id) if selected_filter_player_id else None,
        selected_tracked_pack_id=int(selected_filter_tracked_pack_id) if selected_filter_tracked_pack_id else None,
        tracked_pack_context=tracked_pack_context,
        campaign_options=filter_options["campaign_options"],
        draft_options=filter_options["draft_options"],
        player_options=filter_options["player_options"],
        pack_options=filter_options["pack_options"],
        history_rows=history_result["rows"],
        grouped_drafts=history_result["grouped_drafts"],
        pagination=history_result["pagination"],
    )

@app.route("/campaign-chaos/history/action", methods=["POST"])
def campaign_chaos_history_action():
    action = (request.form.get("bulk_action") or "").strip().lower()
    selected_pack_ids = normalize_tracked_pack_id_list(request.form.getlist("pack_ids"))
    selected_opening_ids = request.form.getlist("opening_ids")

    if action == "print" and not selected_pack_ids:
        flash("No packs were selected.")
        return redirect(url_for("campaign_chaos_history"))

    if action == "delete" and not selected_opening_ids:
        flash("No history rows were selected.")
        return redirect(url_for("campaign_chaos_history"))

    if action == "print":
        try:
            print_result = build_tracked_packs_combined_pdf(
                selected_pack_ids,
                build_chaos_pack_pdf,
                write_debug_log,
            )
        except Exception as exc:
            return str(exc), 400

        return Response(
            print_result["buffer"].getvalue(),
            mimetype="application/pdf",
            headers={
                "Content-Disposition": f'inline; filename="campaign_history_selected_packs_{print_result["pack_count"]}.pdf"',
                "Cache-Control": "no-store",
            },
        )
    
    if action == "delete":
        delete_confirmation = (request.form.get("delete_confirmation") or "").strip()

        if delete_confirmation != "DELETE":
            flash("Delete cancelled. To delete selected history rows, type DELETE in the confirmation prompt.")
            return redirect(url_for("campaign_chaos_history"))

        deleted_count = delete_selected_campaign_history(selected_opening_ids)
        flash(f"Deleted {deleted_count} history row(s).")
        return redirect(url_for("campaign_chaos_history"))
    
    flash("Unknown history action.")
    return redirect(url_for("campaign_chaos_history"))


@app.route("/campaign-chaos/history/delete-all", methods=["POST"])
def campaign_chaos_history_delete_all():
    delete_confirmation = (request.form.get("delete_confirmation") or "").strip()

    if delete_confirmation != "DELETE":
        flash("Delete cancelled. To delete all Campaign History, type DELETE in the confirmation prompt.")
        return redirect(url_for("campaign_chaos_history"))

    result = delete_all_campaign_history()

    flash(
        f"Deleted Campaign History. Removed {result['deleted_openings']} pack selection(s) and {result['deleted_drafts']} draft record(s)."
    )

    return redirect(url_for("campaign_chaos_history"))

@app.route("/campaign-chaos/packs/<int:tracked_pack_id>", methods=["GET"])
def campaign_chaos_pack_detail(tracked_pack_id):
    pack = get_tracked_pack_state_by_id(tracked_pack_id)

    if not pack:
        return "Tracked pack not found.", 404

    config = get_request_config()
    display_pack_prices = (config.get("display_pack_prices") or "1").strip() == "1"
    pack_price_source = (config.get("pack_price_source") or "tcgplayer-retail").strip().lower()

    cards = enrich_pack_cards_with_prices(
        pack.get("cards") or [],
        display_prices=display_pack_prices,
        price_source=pack_price_source,
    )

    return render_template(
        "campaign_pack_detail.html",
        pack=pack,
        cards=cards,
        display_pack_prices=display_pack_prices,
        pack_price_source=pack_price_source,
    )

@app.route("/campaign-chaos/packs/backup", methods=["POST"])
def campaign_chaos_packs_backup():
    selected_pack_ids = normalize_tracked_pack_id_list(request.form.getlist("pack_ids"))

    try:
        backup_result = export_packs_archive(
            selected_pack_ids,
            auto_clear_exports_value=get_auto_clear_exports_config_value(),
        )
    except Exception as exc:
        return str(exc), 400

    return send_file(
        backup_result["zip_path"],
        mimetype="application/zip",
        as_attachment=True,
        download_name=backup_result["zip_filename"],
        max_age=0,
    )


@app.route("/campaign-chaos/packs/import-backup", methods=["POST"])
def campaign_chaos_packs_import_backup():
    backup_file = request.files.get("backup_file")

    try:
        import_result = import_archive_from_file_object(
            backup_file,
            EXPORT_KIND_PACKS,
        )
    except Exception as exc:
        flash(f"Pack import failed: {str(exc)}")
        return redirect(url_for("campaign_chaos_packs"))

    flash(
        f"Pack import complete. Imported {import_result['imported_rows']} row(s) "
        f"and restored {import_result['extracted_files']} file(s)."
    )

    return redirect(url_for("campaign_chaos_packs"))

@app.route("/campaign-chaos/packs/print-default-back-sheet", methods=["GET"])
def campaign_chaos_print_default_back_sheet():
    try:
        pdf_buffer = build_default_card_back_sheet_pdf()
    except Exception as exc:
        return str(exc), 400

    return Response(
        pdf_buffer.getvalue(),
        mimetype="application/pdf",
        headers={
            "Content-Disposition": 'inline; filename="default_card_back_sheet.pdf"',
            "Cache-Control": "no-store",
        },
    )

@app.route("/campaign-chaos/packs/custom-title-set-name", methods=["GET"])
def campaign_chaos_custom_title_set_name():
    set_code = (request.args.get("set_code") or "").strip().upper()

    return jsonify({
        "ok": True,
        "set_code": set_code,
        "set_name": get_set_name_for_custom_title_sheet(set_code),
    })


@app.route("/campaign-chaos/packs/print-custom-title-sheet", methods=["GET"])
def campaign_chaos_print_custom_title_sheet():
    set_code = (request.args.get("set_code") or "").strip().upper()
    pack_name = (request.args.get("pack_name") or "").strip()
    pack_type = (request.args.get("pack_type") or "").strip().lower()
    custom_text = (request.args.get("custom_text") or "").strip()

    try:
        pdf_buffer = build_custom_title_sheet_pdf(
            set_code=set_code,
            pack_name=pack_name,
            pack_type_value=pack_type,
            custom_text=custom_text,
        )
    except Exception as exc:
        return str(exc), 400

    filename_base = safe_filename(
        f"custom_title_sheet_{set_code or 'custom'}_{pack_type or 'custom'}"
    )

    return Response(
        pdf_buffer.getvalue(),
        mimetype="application/pdf",
        headers={
            "Content-Disposition": f'inline; filename="{filename_base}.pdf"',
            "Cache-Control": "no-store",
        },
    )

@app.route("/campaign-chaos/packs/<int:tracked_pack_id>/print", methods=["GET"])
def campaign_chaos_pack_print(tracked_pack_id):
    try:
        print_result = build_tracked_packs_combined_pdf(
            [tracked_pack_id],
            build_chaos_pack_pdf,
            write_debug_log,
        )
    except Exception as exc:
        return str(exc), 400

    return Response(
        print_result["buffer"].getvalue(),
        mimetype="application/pdf",
        headers={
            "Content-Disposition": f'inline; filename="campaign_pack_{tracked_pack_id}.pdf"',
            "Cache-Control": "no-store",
        },
    )

@app.route("/campaign-chaos/packs/<int:tracked_pack_id>/export", methods=["POST"])
def campaign_chaos_pack_export(tracked_pack_id):
    pack = get_tracked_pack_state_by_id(tracked_pack_id)

    if not pack:
        return jsonify({
            "ok": False,
            "message": "Tracked pack was not found.",
        }), 404

    payload = request.get_json(silent=True) or {}
    export_format = (payload.get("export_format") or request.form.get("export_format") or "").strip().lower()

    if export_format not in {"archidekt", "moxfield"}:
        return jsonify({
            "ok": False,
            "message": "Invalid export format.",
        }), 400

    try:
        export_text = build_chaos_pack_export_text(pack, export_format)
    except Exception as exc:
        return jsonify({
            "ok": False,
            "message": str(exc),
        }), 400

    filename_base = safe_filename(
        f"{pack.get('pack_tracking_code') or 'saved_pack'}_{export_format}".lower()
    )

    return jsonify({
        "ok": True,
        "export_format": export_format,
        "filename": f"{filename_base}.txt",
        "export_text": export_text,
    })

@app.route("/campaign-chaos/packs/import-campaign-packs", methods=["GET"])
def campaign_chaos_packs_import_campaign_packs():
    source_campaign_id = (request.args.get("source_campaign_id") or "").strip()
    selected_chaos_campaign_id = get_selected_chaos_campaign_id()

    if not source_campaign_id:
        return jsonify({
            "ok": False,
            "message": "Source campaign is required.",
            "packs": [],
        }), 400

    packs = get_importable_campaign_pack_rows(
        app.static_folder,
        source_campaign_id=source_campaign_id,
        target_campaign_id=selected_chaos_campaign_id,
    )

    return jsonify({
        "ok": True,
        "packs": packs,
    })


@app.route("/campaign-chaos/packs/import-campaign-packs", methods=["POST"])
def campaign_chaos_packs_import_campaign_packs_post():
    selected_chaos_campaign_id = get_selected_chaos_campaign_id()
    payload = request.get_json(silent=True) or {}

    selected_pack_ids = normalize_tracked_pack_id_list(payload.get("pack_ids") or [])

    if not selected_pack_ids:
        return jsonify({
            "ok": False,
            "message": "No packs were selected.",
            "imported_count": 0,
            "skipped_count": 0,
        }), 400

    result = import_tracked_packs_from_campaign(
        selected_pack_ids,
        target_campaign_id=selected_chaos_campaign_id,
    )

    if not result.get("ok"):
        return jsonify(result), 400

    return jsonify(result)

@app.route("/campaign-chaos/packs/search-options", methods=["GET"])
def campaign_chaos_packs_search_options():
    search_text = (request.args.get("q") or "").strip()

    results = search_manage_pack_options(
        app.static_folder,
        search_text,
        limit=30,
    )

    return jsonify({
        "ok": True,
        "results": results,
    })

@app.route("/campaign-chaos/packs/add-random", methods=["POST"])
def campaign_chaos_packs_add_random():
    result = create_random_pack_preview_for_manage_packs(
        app.static_folder,
        write_debug_log,
    )

    if not result.get("ok"):
        return jsonify(result), 400

    return jsonify({
        **result,
        "view_url": url_for("campaign_chaos_pack_preview_view"),
        "print_url": url_for("campaign_chaos_pack_preview_print"),
        "save_url": url_for("campaign_chaos_pack_preview_save"),
    })

@app.route("/campaign-chaos/packs/add-specific-random", methods=["POST"])
def campaign_chaos_packs_add_specific_random():
    payload = request.get_json(silent=True) or {}

    set_code = (payload.get("set_code") or "").strip()
    booster_name = (payload.get("booster_name") or "").strip()

    result = create_specific_pack_preview_for_manage_packs(
        set_code,
        booster_name,
        app.static_folder,
        write_debug_log,
    )

    if not result.get("ok"):
        return jsonify(result), 400

    return jsonify({
        **result,
        "view_url": url_for("campaign_chaos_pack_preview_view"),
        "print_url": url_for("campaign_chaos_pack_preview_print"),
        "save_url": url_for("campaign_chaos_pack_preview_save"),
    })

@app.route("/campaign-chaos/packs/custom-populate-options", methods=["GET"])
def campaign_chaos_packs_custom_populate_options():
    set_code = (request.args.get("set_code") or "").strip()

    options = get_custom_pack_populate_options_for_set(set_code)

    return jsonify({
        "ok": True,
        "options": options,
    })

@app.route("/campaign-chaos/packs/custom-populate", methods=["POST"])
def campaign_chaos_packs_custom_populate():
    payload = request.get_json(silent=True) or {}

    set_code = (payload.get("set_code") or "").strip()
    booster_name = (payload.get("booster_name") or "").strip()
    existing_decklist_text = payload.get("existing_decklist_text") or ""

    result = populate_custom_pack_decklist_from_booster(
        set_code,
        booster_name,
        existing_decklist_text,
        write_debug_log,
    )

    if not result.get("ok"):
        return jsonify(result), 400

    return jsonify(result)

@app.route("/campaign-chaos/packs/add-custom-preview", methods=["POST"])
def campaign_chaos_packs_add_custom_preview():
    payload = request.get_json(silent=True) or {}

    set_code = (payload.get("set_code") or "").strip()
    pack_name = (payload.get("pack_name") or "").strip()
    decklist_text = payload.get("decklist_text") or ""

    result = create_custom_pack_preview_for_manage_packs(
        set_code,
        pack_name,
        decklist_text,
        write_debug_log,
    )

    if not result.get("ok"):
        return jsonify(result), 400

    return jsonify({
        **result,
        "view_url": url_for("campaign_chaos_pack_preview_view"),
        "print_url": url_for("campaign_chaos_pack_preview_print"),
        "save_url": url_for("campaign_chaos_pack_preview_save"),
    })

@app.route("/campaign-chaos/packs/preview/view", methods=["GET"])
def campaign_chaos_pack_preview_view():
    preview_pack = get_chaos_session_state("pending_manage_pack_preview", default_value=None)

    if not preview_pack:
        return "No generated pack preview is available.", 404

    config = get_request_config()
    display_pack_prices = (config.get("display_pack_prices") or "1").strip() == "1"
    pack_price_source = (config.get("pack_price_source") or "tcgplayer-retail").strip().lower()

    cards = enrich_pack_cards_with_prices(
        preview_pack.get("cards") or [],
        display_prices=display_pack_prices,
        price_source=pack_price_source,
    )

    return render_template(
        "campaign_pack_detail.html",
        pack={
            "tracked_pack_id": 0,
            "pack_tracking_code": preview_pack.get("pack_tracking_code") or "",
            "pack_display_name": preview_pack.get("pack_display_name") or preview_pack.get("display_name") or "",
            "total_cards": int(preview_pack.get("total_cards") or 0),
            "opened_count": 0,
            "campaign_enabled": True,
            "is_preview": True,
        },
        cards=cards,
        display_pack_prices=display_pack_prices,
        pack_price_source=pack_price_source,
    )


@app.route("/campaign-chaos/packs/preview/print", methods=["GET"])
def campaign_chaos_pack_preview_print():
    preview_pack = get_chaos_session_state("pending_manage_pack_preview", default_value=None)

    if not preview_pack:
        return "No generated pack preview is available.", 404

    try:
        pdf_buffer = build_chaos_pack_pdf(
            preview_pack.get("cards") or [],
            preview_pack.get("pack_display_name") or preview_pack.get("display_name") or "Generated Pack",
            set_code=preview_pack.get("set_code"),
            booster_name=preview_pack.get("booster_name"),
            pack_tracking_code=preview_pack.get("pack_tracking_code"),
        )
    except Exception as exc:
        return str(exc), 400

    return Response(
        pdf_buffer.getvalue(),
        mimetype="application/pdf",
        headers={
            "Content-Disposition": 'inline; filename="campaign_pack_preview.pdf"',
            "Cache-Control": "no-store",
        },
    )

@app.route("/campaign-chaos/packs/preview/save", methods=["POST"])
def campaign_chaos_pack_preview_save():
    preview_pack = get_chaos_session_state("pending_manage_pack_preview", default_value=None)

    if not preview_pack:
        return jsonify({
            "ok": False,
            "message": "No generated pack preview is available to save.",
        }), 400

    result = save_opened_chaos_pack_to_tracking_db(
        preview_pack,
        campaign_id=get_selected_chaos_campaign_id(),
    )

    if not result.get("ok"):
        return jsonify(result), 400

    clear_chaos_session_state("pending_manage_pack_preview")

    return jsonify(result)

@app.route("/campaign-chaos/packs/action", methods=["POST"])
def campaign_chaos_packs_action():
    action = (request.form.get("bulk_action") or "").strip().lower()
    selected_pack_ids = normalize_tracked_pack_id_list(request.form.getlist("pack_ids"))
    selected_chaos_campaign_id = get_selected_chaos_campaign_id()

    if not selected_pack_ids:
        flash("No packs were selected.")
        return redirect(url_for("campaign_chaos_packs"))

    if action == "print":
        try:
            print_result = build_tracked_packs_combined_pdf(
                selected_pack_ids,
                build_chaos_pack_pdf,
                write_debug_log,
            )
        except Exception as exc:
            return str(exc), 400

        return Response(
            print_result["buffer"].getvalue(),
            mimetype="application/pdf",
            headers={
                "Content-Disposition": f'inline; filename="campaign_saved_packs_{print_result["pack_count"]}.pdf"',
                "Cache-Control": "no-store",
            },
        )

    if action == "export_zip":
        config = get_request_config()

        if (config.get("enable_chaos_card_image_export") or "0").strip() != "1":
            flash("Chaos Draft Card Image Export is disabled in Settings.")
            return redirect(url_for("campaign_chaos_packs"))

        try:
            export_result = build_chaos_card_image_export_zip(selected_pack_ids)
        except Exception as exc:
            write_debug_log(f"IMAGE EXPORT ERROR | error={str(exc)}")
            return str(exc), 400

        return send_file(
            export_result["zip_path"],
            mimetype="application/zip",
            as_attachment=True,
            download_name=export_result["zip_filename"],
            max_age=0,
        )

    if action == "disable":
        updated_count = set_tracked_packs_campaign_enabled(
            selected_pack_ids,
            False,
            campaign_id=selected_chaos_campaign_id,
        )
        flash(f"Disabled {updated_count} pack(s) from Campaign Mode.")
        return redirect(url_for("campaign_chaos_packs"))

    if action == "enable":
        updated_count = set_tracked_packs_campaign_enabled(
            selected_pack_ids,
            True,
            campaign_id=selected_chaos_campaign_id,
        )
        flash(f"Enabled {updated_count} pack(s) for Campaign Mode.")
        return redirect(url_for("campaign_chaos_packs"))

    if action == "delete":
        delete_confirmation = (request.form.get("delete_confirmation") or "").strip()

        if delete_confirmation != "DELETE":
            flash("Delete cancelled. To delete packs, type DELETE in the confirmation prompt.")
            return redirect(url_for("campaign_chaos_packs"))

        deleted_count = delete_tracked_packs(
            selected_pack_ids,
            campaign_id=selected_chaos_campaign_id,
        )
        flash(f"Removed {deleted_count} pack(s) from this campaign.")
        return redirect(url_for("campaign_chaos_packs"))

    flash("Unknown pack action.")
    return redirect(url_for("campaign_chaos_packs"))

@app.route("/campaign-chaos/draft/new", methods=["POST"])
def campaign_chaos_new_draft():
    selected_chaos_campaign_id = get_selected_chaos_campaign_id()

    result = create_chaos_draft_game(
        campaign_id=selected_chaos_campaign_id,
        packs_per_player=3,
    )

    clear_chaos_session_state("pending_spin_result")
    clear_chaos_session_state("pending_opened_pack")
    clear_chaos_session_state("pending_opened_pack_pdf")
    clear_chaos_session_state("pending_opened_pack_export")
    clear_chaos_session_state("pending_campaign_pack_opening_recorded")

    return jsonify(result)

@app.route("/campaign-chaos/spin", methods=["POST"])
def campaign_chaos_spin():
    selected_chaos_campaign_id = get_selected_chaos_campaign_id()
    active_draft_game = get_selected_or_create_chaos_draft_game(
        campaign_id=selected_chaos_campaign_id,
    )
    active_draft_game_id = active_draft_game["draft_game_id"] if active_draft_game else None

    spin_result = build_campaign_chaos_spin_result(
        app.static_folder,
        write_debug_log,
        campaign_id=selected_chaos_campaign_id,
        draft_game_id=active_draft_game_id,
    )

    if not spin_result:
        return jsonify({
            "ok": False,
            "message": "No saved packs were found in the Pack Tracking Database.",
        }), 404

    chosen_variant = spin_result.get("chosen_variant") or {}
    tracked_pack_id = chosen_variant.get("tracked_pack_id")

    if tracked_pack_id:
        record_campaign_pack_opening(
            tracked_pack_id,
            opened_by_player_id=get_selected_campaign_player_id(campaign_id=selected_chaos_campaign_id),
            opening_context="campaign_mode_spin_selected",
            campaign_id=selected_chaos_campaign_id,
            draft_game_id=active_draft_game_id,
        )

        set_chaos_session_state(
            "pending_campaign_pack_opening_recorded",
            {
                "tracked_pack_id": int(tracked_pack_id),
                "recorded": True,
                "recorded_at": "spin",
            },
        )

    return jsonify({
        "ok": True,
        "spin_result": spin_result,
    })


@app.route("/campaign-chaos/open", methods=["POST"])
def campaign_chaos_open():
    opened_pack = get_chaos_session_state("pending_opened_pack", default_value=None)
    if not opened_pack:
        return jsonify({
            "ok": False,
            "message": "No Campaign Mode pack is ready to open."
        }), 400

    tracked_pack_id = opened_pack.get("tracked_pack_id")
    if tracked_pack_id:
        opening_state = get_chaos_session_state(
            "pending_campaign_pack_opening_recorded",
            default_value=None,
        )

        already_recorded = bool(opening_state and opening_state.get("recorded"))

        if not already_recorded:
            selected_chaos_campaign_id = get_selected_chaos_campaign_id()

            record_campaign_pack_opening(
                tracked_pack_id,
                opened_by_player_id=get_selected_campaign_player_id(campaign_id=selected_chaos_campaign_id),
                opening_context="campaign_mode_pdf_open",
                campaign_id=selected_chaos_campaign_id,
                draft_game_id=opened_pack.get("draft_game_id"),
            )

            set_chaos_session_state(
                "pending_campaign_pack_opening_recorded",
                {
                    "tracked_pack_id": int(tracked_pack_id),
                    "recorded": True,
                },
            )

    result = build_pending_chaos_pack_pdf(
        build_chaos_pack_pdf,
        write_debug_log,
        safe_filename,
    )

    if not result.get("ok"):
        return jsonify(result), 400

    return jsonify(result)

@app.route("/chaos-draft/spin", methods=["POST"])
def chaos_draft_spin():
    spin_result = build_chaos_spin_result(
        app.static_folder,
        write_debug_log,
    )

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
    result = build_pending_chaos_pack_pdf(
        build_chaos_pack_pdf,
        write_debug_log,
        safe_filename,
    )

    if not result.get("ok"):
        return jsonify(result), 400

    return jsonify(result)

@app.route("/chaos-draft/save-pack", methods=["POST"])
def chaos_draft_save_pack():
    config = get_request_config()
    if (config.get("enable_track_packs") or "0").strip() != "1":
        return jsonify({
            "ok": False,
            "message": "Pack Tracking Database is disabled in Config."
        }), 400

    result = save_opened_chaos_pack_to_tracking_db()

    if not result.get("ok"):
        return jsonify(result), 400

    return jsonify(result)

@app.route("/chaos/cards/<card_uuid>/alternate-sources", methods=["GET"])
@app.route("/campaign-chaos/cards/<card_uuid>/alternate-sources", methods=["GET"])
def campaign_chaos_card_alternate_sources(card_uuid):
    card_row = get_chaos_card_by_uuid(card_uuid)

    if not card_row:
        return jsonify({
            "ok": False,
            "message": "Card UUID was not found.",
            "alternate_sources": [],
        }), 404

    alternate_sources = get_alternate_sources_for_card(card_uuid)

    active_source = None
    for source in alternate_sources:
        if source["is_enabled"]:
            active_source = source
            break

    return jsonify({
        "ok": True,
        "card": {
            "card_uuid": card_row["card_uuid"],
            "card_name": card_row["card_name"],
            "set_code": card_row["set_code"],
            "collector_number": card_row["collector_number"],
            "scryfall_id": card_row["scryfall_id"],
            "is_dual_faced": int(card_row["is_dual_faced"] or 0) == 1,
            "face_count": int(card_row["face_count"] or 0),
            "front_face_name": card_row["front_face_name"] or card_row["card_name"],
            "back_face_name": card_row["back_face_name"] or "",
        },
        "frame_template_options": get_card_export_template_options(),
        "active_source": active_source,
        "alternate_sources": alternate_sources,
    })


@app.route("/chaos/cards/<card_uuid>/alternate-sources/add", methods=["POST"])
@app.route("/campaign-chaos/cards/<card_uuid>/alternate-sources/add", methods=["POST"])
def campaign_chaos_card_alternate_sources_add(card_uuid):
    card_row = get_chaos_card_by_uuid(card_uuid)

    if not card_row:
        return jsonify({
            "ok": False,
            "message": "Card UUID was not found.",
        }), 404

    source_name = (request.form.get("source_name") or "").strip()
    source_type = (request.form.get("source_type") or "external_url").strip().lower()
    face_kind = normalize_alternate_face_kind(request.form.get("face_kind") or "single")
    external_image_url = (request.form.get("external_image_url") or "").strip()
    local_image_path = (request.form.get("local_image_path") or "").strip()
    priority = "100"
    notes = (request.form.get("notes") or "").strip()
    remove_bleed = request.form.get("remove_bleed") == "on"
    bleed_size_mm = get_configured_print_bleed_size_mm()
    export_frame_template = (request.form.get("export_frame_template") or "auto").strip().lower()

    fullbleed_image_path = ""

    uploaded_file = request.files.get("alternate_image_file")

    if not source_name:
        if uploaded_file and uploaded_file.filename:
            source_name = "Upload File"
        elif source_type == "external_url" and external_image_url:
            try:
                parsed_domain = re.sub(
                    r"^www\.",
                    "",
                    urlparse(external_image_url).hostname or "",
                    flags=re.IGNORECASE,
                )
                source_name = parsed_domain or "External URL"
            except Exception:
                source_name = "External URL"
        elif source_type == "local_file":
            source_name = "Local File"
        else:
            source_name = "Manual Alternate Image"

    try:
        if uploaded_file and uploaded_file.filename:
            source_type = "uploaded_file"
            upload_result = save_alternate_source_upload_file(
                uploaded_file,
                card_uuid=card_uuid,
                face_kind=face_kind,
                remove_bleed=remove_bleed,
                bleed_size_mm=bleed_size_mm,
            )

            local_image_path = upload_result["local_image_path"]
            fullbleed_image_path = upload_result["fullbleed_image_path"]
            remove_bleed = upload_result["remove_bleed"]
            bleed_size_mm = upload_result["bleed_size_mm"]

        alternate_source_id = create_alternate_source_for_card(
            card_uuid=card_uuid,
            source_name=source_name,
            source_type=source_type,
            face_kind=face_kind,
            external_image_url=external_image_url,
            local_image_path=local_image_path,
            fullbleed_image_path=fullbleed_image_path,
            remove_bleed=remove_bleed,
            bleed_size_mm=bleed_size_mm,
            export_frame_template=export_frame_template,
            priority=priority,
            notes=notes,
        )

        return jsonify({
            "ok": True,
            "message": "Alternate image source added.",
            "alternate_source_id": alternate_source_id,
            "alternate_sources": get_alternate_sources_for_card(card_uuid),
        })

    except Exception as exc:
        return jsonify({
            "ok": False,
            "message": str(exc),
        }), 400

@app.route("/chaos/alternate-sources/<int:alternate_source_id>/frame-template", methods=["POST"])
@app.route("/campaign-chaos/alternate-sources/<int:alternate_source_id>/frame-template", methods=["POST"])
def campaign_chaos_alternate_source_frame_template_update(alternate_source_id):
    source_row = get_alternate_source_by_id(alternate_source_id)

    if not source_row:
        return jsonify({
            "ok": False,
            "message": "Alternate source was not found.",
        }), 404

    payload = request.get_json(silent=True) or {}
    export_frame_template = (payload.get("export_frame_template") or "auto").strip().lower()

    valid_export_frame_templates = {
        option["value"]
        for option in get_card_export_template_options()
    }

    if export_frame_template not in valid_export_frame_templates:
        export_frame_template = "auto"

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE alternate_sources
        SET export_frame_template = ?,
            updated_at_utc = ?
        WHERE alternate_source_id = ?
        """,
        (
            export_frame_template,
            now_utc,
            int(alternate_source_id),
        ),
    )

    conn.commit()
    conn.close()

    return jsonify({
        "ok": True,
        "message": "Card frame template updated.",
        "alternate_sources": get_alternate_sources_for_card(source_row["card_uuid"]),
    })

@app.route("/chaos/alternate-sources/<int:alternate_source_id>/toggle", methods=["POST"])
@app.route("/campaign-chaos/alternate-sources/<int:alternate_source_id>/toggle", methods=["POST"])
def campaign_chaos_alternate_source_toggle(alternate_source_id):
    source_row = get_alternate_source_by_id(alternate_source_id)

    if not source_row:
        return jsonify({
            "ok": False,
            "message": "Alternate source was not found.",
        }), 404

    payload = request.get_json(silent=True) or {}
    enabled = bool(payload.get("enabled", False))

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE alternate_sources
        SET is_enabled = ?,
            updated_at_utc = ?
        WHERE alternate_source_id = ?
        """,
        (
            1 if enabled else 0,
            now_utc,
            int(alternate_source_id),
        ),
    )

    conn.commit()
    conn.close()

    return jsonify({
        "ok": True,
        "message": "Alternate source updated.",
        "alternate_sources": get_alternate_sources_for_card(source_row["card_uuid"]),
    })

@app.route("/chaos/alternate-sources/<int:alternate_source_id>/delete", methods=["POST"])
@app.route("/campaign-chaos/alternate-sources/<int:alternate_source_id>/delete", methods=["POST"])
def campaign_chaos_alternate_source_delete(alternate_source_id):
    source_row = get_alternate_source_by_id(alternate_source_id)

    if not source_row:
        return jsonify({
            "ok": False,
            "message": "Alternate source was not found.",
        }), 404

    card_uuid = source_row["card_uuid"]
    local_path = get_alternate_source_local_absolute_path(source_row)

    fullbleed_path = ""
    if "fullbleed_image_path" in source_row.keys():
        fullbleed_relative_path = (source_row["fullbleed_image_path"] or "").strip()
        if fullbleed_relative_path:
            fullbleed_path = os.path.abspath(os.path.join(RUNTIME_BASE_DIR, fullbleed_relative_path))

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        DELETE FROM alternate_sources
        WHERE alternate_source_id = ?
        """,
        (int(alternate_source_id),),
    )

    conn.commit()
    conn.close()

    # Only remove files inside the managed alternate source folder.
    try:
        alternate_root = os.path.abspath(ALTERNATE_SOURCE_DIR)

        for candidate_path in [local_path, fullbleed_path]:
            if not candidate_path:
                continue

            local_abs = os.path.abspath(candidate_path)
            if local_abs.startswith(alternate_root) and os.path.exists(local_abs):
                os.remove(local_abs)
    except Exception:
        pass

    return jsonify({
        "ok": True,
        "message": "Alternate source deleted.",
        "alternate_sources": get_alternate_sources_for_card(card_uuid),
    })

@app.route("/debug/alternate-source/add", methods=["POST"])
def debug_alternate_source_add():
    card_uuid = (request.form.get("card_uuid") or "").strip()
    face_kind = normalize_alternate_face_kind(request.form.get("face_kind") or "single")
    source_name = (request.form.get("source_name") or "Manual Alternate Source").strip()
    source_type = (request.form.get("source_type") or "external_url").strip().lower()
    external_image_url = (request.form.get("external_image_url") or "").strip()
    local_image_path = (request.form.get("local_image_path") or "").strip()
    priority = request.form.get("priority") or "100"

    if not card_uuid:
        return "card_uuid is required", 400

    card_row = get_chaos_card_by_uuid(card_uuid)
    if not card_row:
        return "card_uuid was not found in chaos_cards", 404

    try:
        parsed_priority = int(priority)
    except (TypeError, ValueError):
        parsed_priority = 100

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO alternate_sources (
            source_name,
            source_type,
            card_uuid,
            set_code,
            collector_number,
            scryfall_id,
            card_name,
            face_kind,
            external_image_url,
            local_image_path,
            is_enabled,
            priority,
            notes,
            created_at_utc,
            updated_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source_name,
            source_type,
            card_uuid,
            card_row["set_code"],
            card_row["collector_number"],
            card_row["scryfall_id"],
            card_row["card_name"],
            face_kind,
            external_image_url,
            local_image_path,
            1,
            parsed_priority,
            "Debug route insert",
            now_utc,
            now_utc,
        ),
    )

    alternate_source_id = cursor.lastrowid

    conn.commit()
    conn.close()

    return jsonify({
        "ok": True,
        "alternate_source_id": int(alternate_source_id),
    })

@app.route("/chaos-card-image/<card_uuid>", methods=["GET"])
def chaos_card_image(card_uuid):
    card_row = get_chaos_card_by_uuid(card_uuid)

    if not card_row:
        return ("Not found", 404)

    page_entries = build_chaos_print_pages_for_card(card_row)
    if not page_entries:
        return ("Not found", 404)

    first_page = page_entries[0]
    image_url = (first_page.get("image_url") or "").strip()
    page_kind = (first_page.get("page_kind") or "").strip().lower()

    image_source = resolve_card_image_source_for_page(
        card_row,
        page_kind,
        image_url,
    )

    if image_source.get("source_type") == "alternate_source":
        alternate_path = os.path.abspath(image_source["absolute_path"])
        if os.path.exists(alternate_path):
            return send_file(alternate_path)

    if not image_url:
        return ("Not found", 404)

    cached_result = download_chaos_image_to_cache(
        first_page.get("card_uuid"),
        first_page.get("page_kind"),
        first_page.get("face_name"),
        image_url,
    )

    if not cached_result:
        return ("Not found", 404)

    absolute_path = os.path.abspath(cached_result["absolute_path"])
    if os.path.exists(absolute_path):
        return send_file(absolute_path)

    return redirect(image_url)

@app.route("/chaos-draft/open-file", methods=["GET"])
def chaos_draft_open_file():
    pdf_state = get_chaos_session_state("pending_opened_pack_pdf", default_value=None)

    if not pdf_state:
        return "No opened Chaos Draft PDF is available.", 404

    pdf_hex = (pdf_state.get("pdf_base64") or "").strip()
    filename = (pdf_state.get("filename") or "chaos_draft_pack.pdf").strip()

    if not pdf_hex:
        return "Chaos Draft PDF data was empty.", 404

    try:
        pdf_bytes = bytes.fromhex(pdf_hex)
    except Exception:
        return "Chaos Draft PDF data was invalid.", 500

    return Response(
        pdf_bytes,
        mimetype="application/pdf",
        headers={
            "Content-Disposition": f"inline; filename={filename}",
            "Cache-Control": "no-store"
        }
    )

@app.route("/chaos-draft/view-data", methods=["GET"])
def chaos_draft_view_data():
    opened_pack = get_chaos_session_state("pending_opened_pack", default_value=None)

    if not opened_pack:
        return jsonify({
            "ok": False,
            "message": "No opened Chaos Draft pack is available."
        }), 404

    config = get_request_config()
    display_pack_prices = (config.get("display_pack_prices") or "1").strip() == "1"
    pack_price_source = (config.get("pack_price_source") or "tcgplayer-retail").strip().lower()

    cards = enrich_pack_cards_with_prices(
        opened_pack.get("cards") or [],
        display_prices=display_pack_prices,
        price_source=pack_price_source,
    )

    serialized_cards = []
    for card in cards:
        serialized_cards.append({
            "card_uuid": card.get("card_uuid"),
            "card_name": card.get("card_name"),
            "image_src": url_for("chaos_card_image", card_uuid=card.get("card_uuid")),
            "finish_type": card.get("finish_type"),
            "special_badges": card.get("special_badges") or [],
            "price": card.get("price_info", {}).get("price"),
            "currency": card.get("price_info", {}).get("currency") or "USD",
        })

    return jsonify({
        "ok": True,
        "pack_display_name": opened_pack.get("display_name") or "",
        "pack_total_cards": int(opened_pack.get("total_cards") or 0),
        "bonus_pack_opened": bool(opened_pack.get("bonus_pack_opened")),
        "display_pack_prices": display_pack_prices,
        "cards": serialized_cards,
    })

@app.route("/chaos-draft/view", methods=["GET"])
def chaos_draft_view():
    opened_pack = get_chaos_session_state("pending_opened_pack", default_value=None)

    if not opened_pack:
        return "No opened Chaos Draft pack is available.", 404

    config = get_request_config()
    display_pack_prices = (config.get("display_pack_prices") or "1").strip() == "1"
    pack_price_source = (config.get("pack_price_source") or "tcgplayer-retail").strip().lower()

    cards = enrich_pack_cards_with_prices(
        opened_pack.get("cards") or [],
        display_prices=display_pack_prices,
        price_source=pack_price_source,
    )

    return render_template(
        "chaos_pack_view.html",
        pack_display_name=opened_pack.get("display_name") or "",
        pack_total_cards=int(opened_pack.get("total_cards") or 0),
        bonus_pack_opened=bool(opened_pack.get("bonus_pack_opened")),
        cards=cards,
        display_pack_prices=display_pack_prices,
        pack_price_source=pack_price_source,
    )

@app.route("/chaos-draft/export", methods=["POST"])
def chaos_draft_export():
    config = get_request_config()
    export_format = (config.get("chaos_draft_export_format") or "none").strip().lower()

    if export_format not in {"archidekt", "moxfield"}:
        return jsonify({
            "ok": False,
            "message": "Chaos Draft export is disabled."
        }), 400

    opened_pack = get_chaos_session_state("pending_opened_pack", default_value=None)

    if not opened_pack:
        return jsonify({
            "ok": False,
            "message": "No opened Chaos Draft pack is available."
        }), 400

    try:
        export_text = build_chaos_pack_export_text(opened_pack, export_format)
    except Exception as exc:
        return jsonify({
            "ok": False,
            "message": str(exc),
        }), 400

    filename_base = safe_filename(
        f"{opened_pack['set_code']}_{opened_pack['booster_name']}_{export_format}".lower()
    )

    set_chaos_session_state(
        "pending_opened_pack_export",
        {
            "filename": f"{filename_base}.txt",
            "export_text": export_text,
            "export_format": export_format,
        },
    )

    return jsonify({
        "ok": True,
        "export_format": export_format,
        "filename": f"{filename_base}.txt",
        "export_text": export_text,
        "download_url": url_for("chaos_draft_export_file"),
    })


@app.route("/chaos-draft/export-file", methods=["GET"])
def chaos_draft_export_file():
    export_state = get_chaos_session_state("pending_opened_pack_export", default_value=None)

    if not export_state:
        return "No Chaos Draft export is available.", 404

    export_text = (export_state.get("export_text") or "").strip()
    filename = (export_state.get("filename") or "chaos_draft_export.txt").strip()

    if not export_text:
        return "Chaos Draft export data was empty.", 404

    return Response(
        export_text,
        mimetype="text/plain; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-store"
        }
    )

@app.route("/chaos-draft/preprint-pdf", methods=["GET"])
def chaos_draft_preprint_pdf():
    if not is_card_database_ready():
        return "Card database not ready", 400

    pdf_settings = resolve_pdf_print_settings()
    if not pdf_settings["use_pdf_print"]:
        return "PDF printing is disabled", 400

    player_count = (request.args.get("player_count") or "4").strip()
    packs_per_player = (request.args.get("packs_per_player") or "3").strip()

    try:
        preprint_result = build_preprint_chaos_draft_pdf(
            player_count,
            packs_per_player,
            app.static_folder,
            build_chaos_pack_pdf,
            write_debug_log,
        )
    except Exception as exc:
        write_debug_log(f"PREPRINT CHAOS DRAFT ERROR | error={str(exc)}")
        return str(exc), 400

    return Response(
        preprint_result["buffer"].getvalue(),
        mimetype="application/pdf",
        headers={
            "Content-Disposition": f'inline; filename="{preprint_result["filename"]}"',
            "Cache-Control": "no-store"
        }
    )

@app.route("/chaos-draft/next", methods=["POST"])
def chaos_draft_next():
    clear_chaos_session_state("pending_spin_result")
    clear_chaos_session_state("pending_opened_pack")
    clear_chaos_session_state("pending_opened_pack_pdf")
    clear_chaos_session_state("pending_opened_pack_export")

    return jsonify({
        "ok": True,
        "message": "Chaos Draft pack cleared.",
    })

@app.route("/debug/chaos-card")
def debug_chaos_card():
    card_name = (request.args.get("name") or "").strip()

    if not card_name:
        return jsonify({
            "ok": False,
            "message": "Missing ?name=Card Name"
        }), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            card_uuid,
            set_code,
            card_name,
            layout,
            face_count,
            is_dual_faced,
            front_face_name,
            back_face_name,
            front_image_url,
            back_image_url,
            image_url,
            faces_json
        FROM chaos_cards
        WHERE LOWER(card_name) = LOWER(?)
        ORDER BY set_code, collector_number
        """,
        (card_name,),
    )

    rows = cursor.fetchall()
    conn.close()

    results = []
    for row in rows:
        results.append({
            "card_uuid": row["card_uuid"],
            "set_code": row["set_code"],
            "card_name": row["card_name"],
            "layout": row["layout"],
            "face_count": row["face_count"],
            "is_dual_faced": row["is_dual_faced"],
            "front_face_name": row["front_face_name"],
            "back_face_name": row["back_face_name"],
            "front_image_url": row["front_image_url"],
            "back_image_url": row["back_image_url"],
            "image_url": row["image_url"],
            "faces_json": row["faces_json"],
        })

    return jsonify({
        "ok": True,
        "count": len(results),
        "results": results,
    })

@app.route("/custom-draft-sets/add", methods=["POST"])
def custom_draft_sets_add():
    set_name = (request.form.get("set_name") or "").strip()
    set_code = normalize_custom_draft_set_code(request.form.get("set_code"))
    release_year = (request.form.get("release_year") or "").strip()

    try:
        icon_svg_path = ""
        uploaded_icon = request.files.get("set_icon_file")
        if uploaded_icon and uploaded_icon.filename:
            icon_svg_path = save_custom_set_icon_file(uploaded_icon, set_code)

        saved_set_code = upsert_custom_draft_set(
            set_name=set_name,
            set_code=set_code,
            release_year=release_year,
            special_category_1_name=request.form.get("special_category_1_name") or "",
            special_category_2_name=request.form.get("special_category_2_name") or "",
            special_category_3_name=request.form.get("special_category_3_name") or "",
            icon_svg_path=icon_svg_path,
            is_active=True,
        )

        flash(f"Custom draft set {saved_set_code} created.")
        return redirect(url_for("custom_draft_set_manage", set_code=saved_set_code))

    except Exception as exc:
        flash(str(exc))
        return redirect(url_for("sets"))

@app.route("/custom-draft-sets/<path:set_code>/export-zip", methods=["POST"])
def custom_draft_set_export_zip(set_code):
    clean_set_code = normalize_custom_draft_set_code(set_code)

    config = get_request_config()

    if (config.get("enable_chaos_card_image_export") or "0").strip() != "1":
        flash("Chaos Draft Card Image Export is disabled in Settings.")
        return redirect(url_for("custom_draft_set_manage", set_code=clean_set_code))

    try:
        export_rows = build_custom_draft_set_image_export_rows(clean_set_code)
        export_result = build_chaos_card_image_export_zip(export_rows=export_rows)
    except Exception as exc:
        write_debug_log(f"CUSTOM SET IMAGE EXPORT ERROR | set_code={clean_set_code} | error={str(exc)}")
        return str(exc), 400

    custom_set = get_custom_draft_set(clean_set_code)
    set_name = custom_set["set_name"] if custom_set and custom_set["set_name"] else clean_set_code
    download_filename = f"{safe_filename(set_name)}_{safe_filename(clean_set_code)}_image_export.zip"

    return send_file(
        export_result["zip_path"],
        mimetype="application/zip",
        as_attachment=True,
        download_name=download_filename,
        max_age=0,
    )

@app.route("/custom-draft-sets/<path:set_code>", methods=["GET", "POST"])
def custom_draft_set_manage(set_code):
    clean_set_code = normalize_custom_draft_set_code(set_code)

    if request.method == "POST":
        try:
            icon_svg_path = ""
            uploaded_icon = request.files.get("set_icon_file")
            if uploaded_icon and uploaded_icon.filename:
                icon_svg_path = save_custom_set_icon_file(uploaded_icon, clean_set_code)

            saved_set_code = upsert_custom_draft_set(
                set_name=request.form.get("set_name") or "",
                set_code=clean_set_code,
                release_year=request.form.get("release_year") or "",
                special_category_1_name=request.form.get("special_category_1_name") or "",
                special_category_2_name=request.form.get("special_category_2_name") or "",
                special_category_3_name=request.form.get("special_category_3_name") or "",
                icon_svg_path=icon_svg_path,
                is_active=request.form.get("is_active") == "on",
            )

            flash("Custom draft set saved.")
            return redirect(url_for("custom_draft_set_manage", set_code=saved_set_code))

        except Exception as exc:
            flash(str(exc))
            return redirect(url_for("custom_draft_set_manage", set_code=clean_set_code))

    custom_set = get_custom_draft_set(clean_set_code)

    if not custom_set:
        flash("Custom draft set was not found.")
        return redirect(url_for("sets"))

    pack_slots = get_custom_draft_pack_slots(clean_set_code)
    custom_set_cards = get_custom_draft_set_card_rows(clean_set_code)

    special_category_options = [
        {
            "value": 0,
            "label": "None",
        },
        {
            "value": 1,
            "label": custom_set["special_category_1_name"] or "Special Slot Category 1",
        },
        {
            "value": 2,
            "label": custom_set["special_category_2_name"] or "Special Slot Category 2",
        },
        {
            "value": 3,
            "label": custom_set["special_category_3_name"] or "Special Slot Category 3",
        },
    ]

    return render_template(
        "custom_draft_set.html",
        custom_set=custom_set,
        pack_slots=pack_slots,
        custom_set_cards=custom_set_cards,
        special_category_options=special_category_options,
    )

def get_custom_draft_booster_label(booster_name):
    clean_booster_name = str(booster_name or "").strip().lower()

    if clean_booster_name == "mystery":
        return "Mystery Booster"

    if clean_booster_name == "play":
        return "Play Booster"

    if clean_booster_name == "collector":
        return "Collector Booster"

    return "Custom Booster"

def build_custom_draft_set_pack_display_name(custom_set, booster_name):
    booster_label = get_custom_draft_booster_label(booster_name)
    set_name = custom_set["set_name"] if custom_set and custom_set["set_name"] else "Custom Draft Set"
    set_code = custom_set["set_code"] if custom_set and custom_set["set_code"] else ""

    if set_code:
        return normalize_chaos_pack_display_name(f"{set_name} - {booster_label} ({set_code})")

    return normalize_chaos_pack_display_name(f"{set_name} - {booster_label}")

def normalize_custom_draft_booster_name(booster_name):
    clean_booster_name = str(booster_name or "").strip().lower()

    if clean_booster_name not in {"mystery", "play", "collector"}:
        return ""

    return clean_booster_name

@app.route("/custom-draft-sets/<path:set_code>/layouts/<booster_name>", methods=["GET", "POST"])
def custom_draft_set_layout_edit(set_code, booster_name):
    clean_set_code = normalize_custom_draft_set_code(set_code)
    clean_booster_name = normalize_custom_draft_booster_name(booster_name)

    if not clean_booster_name:
        flash("Invalid custom draft pack layout.")
        return redirect(url_for("custom_draft_set_manage", set_code=clean_set_code))

    custom_set = get_custom_draft_set(clean_set_code)

    if not custom_set:
        flash("Custom draft set was not found.")
        return redirect(url_for("sets"))

    if request.method == "POST":
        slot_updates = []

        for slot_number in range(1, 16):
            slot_updates.append({
                "slot_number": slot_number,
                "color_rule": request.form.get(f"slot_{slot_number}_color_rule") or "any",
                "rarity_rule": request.form.get(f"slot_{slot_number}_rarity_rule") or "any",
                "special_category_rule": request.form.get(f"slot_{slot_number}_special_category_rule") or "none",
                "foil_rule": request.form.get(f"slot_{slot_number}_foil_rule") or "no",
            })

        try:
            update_custom_draft_pack_layout(
                clean_set_code,
                clean_booster_name,
                slot_updates,
            )
            flash("Pack layout saved.")
        except Exception as exc:
            flash(str(exc))

        return redirect(url_for(
            "custom_draft_set_layout_edit",
            set_code=clean_set_code,
            booster_name=clean_booster_name,
        ))

    pack_slots = get_custom_draft_pack_slots_for_booster(
        clean_set_code,
        clean_booster_name,
    )

    slot_options = get_custom_draft_pack_slot_options()

    special_category_labels = {
        "none": "None",
        "category_1": custom_set["special_category_1_name"] or "Special Slot Category 1",
        "category_2": custom_set["special_category_2_name"] or "Special Slot Category 2",
        "category_3": custom_set["special_category_3_name"] or "Special Slot Category 3",
    }

    return render_template(
        "custom_draft_pack_layout.html",
        custom_set=custom_set,
        booster_name=clean_booster_name,
        booster_label=get_custom_draft_booster_label(clean_booster_name),
        pack_slots=pack_slots,
        slot_options=slot_options,
        special_category_labels=special_category_labels,
    )

@app.route("/custom-draft-sets/<path:set_code>/generate/<booster_name>", methods=["POST"])
def custom_draft_set_generate_pack(set_code, booster_name):
    clean_set_code = normalize_custom_draft_set_code(set_code)
    clean_booster_name = normalize_custom_draft_booster_name(booster_name)

    if not clean_booster_name:
        return jsonify({
            "ok": False,
            "message": "Invalid custom draft pack type.",
        }), 400

    custom_set = get_custom_draft_set(clean_set_code)

    if not custom_set:
        return jsonify({
            "ok": False,
            "message": "Custom draft set was not found.",
        }), 404

    try:
        generated_cards = generate_custom_draft_set_pack_cards(
            clean_set_code,
            clean_booster_name,
        )

        pack_display_name = build_custom_draft_set_pack_display_name(
            custom_set,
            clean_booster_name,
        )

        preview_pack = {
            "ok": True,
            "source": "custom_draft_set",
            "set_code": clean_set_code,
            "booster_name": clean_booster_name,
            "booster_index": 0,
            "display_name": pack_display_name,
            "pack_display_name": pack_display_name,
            "pack_tracking_code": build_pack_tracking_code(
                clean_set_code,
                clean_booster_name,
                0,
            ),
            "total_cards": len(generated_cards),
            "bonus_pack_opened": False,
            "cards": generated_cards,
            "source_json": {
                "source": "custom_draft_set",
                "set_code": clean_set_code,
                "booster_name": clean_booster_name,
            },
        }

        set_chaos_session_state(
            "pending_manage_pack_preview",
            preview_pack,
        )

        return jsonify({
            "ok": True,
            "message": "Custom draft set pack generated.",
            "pack_display_name": pack_display_name,
            "total_cards": len(generated_cards),
            "view_url": url_for("campaign_chaos_pack_preview_view"),
            "print_url": url_for("campaign_chaos_pack_preview_print"),
            "save_url": url_for("campaign_chaos_pack_preview_save"),
        })

    except Exception as exc:
        return jsonify({
            "ok": False,
            "message": str(exc),
        }), 400

def parse_custom_draft_bulk_import_quantity(raw_value):
    try:
        parsed_quantity = int(float(str(raw_value or "1").strip()))
    except (TypeError, ValueError):
        parsed_quantity = 1

    if parsed_quantity < 1:
        parsed_quantity = 1

    if parsed_quantity > 999:
        parsed_quantity = 999

    return parsed_quantity


def parse_custom_draft_bulk_import_csv(raw_text):
    clean_text = str(raw_text or "").strip()

    if not clean_text:
        return []

    first_line = ""
    for raw_line in clean_text.splitlines():
        if raw_line.strip():
            first_line = raw_line.strip()
            break

    if "," not in first_line:
        return []

    try:
        reader = csv.DictReader(StringIO(clean_text))
    except Exception:
        return []

    if not reader.fieldnames:
        return []

    normalized_field_lookup = {
        str(field_name or "").strip().lower(): field_name
        for field_name in reader.fieldnames
    }

    name_field = (
        normalized_field_lookup.get("name")
        or normalized_field_lookup.get("card name")
        or normalized_field_lookup.get("card")
    )

    if not name_field:
        return []

    quantity_field = (
        normalized_field_lookup.get("quantity")
        or normalized_field_lookup.get("count")
        or normalized_field_lookup.get("qty")
        or normalized_field_lookup.get("amount")
    )

    set_field = (
        normalized_field_lookup.get("set code")
        or normalized_field_lookup.get("set")
        or normalized_field_lookup.get("edition")
        or normalized_field_lookup.get("edition code")
    )

    collector_field = (
        normalized_field_lookup.get("collector number")
        or normalized_field_lookup.get("collector #")
        or normalized_field_lookup.get("number")
        or normalized_field_lookup.get("collector_number")
    )

    parsed_items = []

    for row in reader:
        card_name = str(row.get(name_field) or "").strip()

        if not card_name:
            continue

        quantity = parse_custom_draft_bulk_import_quantity(
            row.get(quantity_field) if quantity_field else 1
        )

        parsed_items.append({
            "card_name": card_name,
            "quantity": quantity,
            "set_code": str(row.get(set_field) or "").strip().upper() if set_field else "",
            "collector_number": str(row.get(collector_field) or "").strip() if collector_field else "",
        })

    return parsed_items


def parse_custom_draft_bulk_import_decklist(raw_text):
    parsed_items = []
    ignored_section_headers = {
        "commander",
        "mainboard",
        "main deck",
        "sideboard",
        "maybeboard",
        "considering",
        "companions",
        "companion",
        "deck",
        "lands",
        "creatures",
        "instants",
        "sorceries",
        "artifacts",
        "enchantments",
        "planeswalkers",
        "battles",
        "tokens",
    }

    for raw_line in str(raw_text or "").splitlines():
        line = str(raw_line or "").strip()

        if not line:
            continue

        if line.startswith("#") or line.startswith("//"):
            continue

        clean_header = re.sub(r"\s*\(\d+\)\s*$", "", line).strip().lower()
        if clean_header in ignored_section_headers:
            continue

        if line.lower().startswith("sideboard:"):
            line = line.split(":", 1)[1].strip()

        if line.lower().startswith("sb:"):
            line = line[3:].strip()

        line = re.sub(r"\s+\*\w+\*$", "", line).strip()

        quantity = 1
        quantity_match = re.match(r"^(\d+)\s*x?\s+(.+)$", line, flags=re.IGNORECASE)

        if quantity_match:
            quantity = parse_custom_draft_bulk_import_quantity(quantity_match.group(1))
            line = quantity_match.group(2).strip()

        requested_set_code = ""
        requested_collector_number = ""

        specific_printing_match = re.match(
            r"^(.*?)\s+\(([A-Za-z0-9]{2,10})\)\s+([A-Za-z0-9\-]+)\s*$",
            line,
        )

        if specific_printing_match:
            line = specific_printing_match.group(1).strip()
            requested_set_code = specific_printing_match.group(2).strip().upper()
            requested_collector_number = specific_printing_match.group(3).strip()

        else:
            set_only_match = re.match(
                r"^(.*?)\s+\(([A-Za-z0-9]{2,10})\)\s*$",
                line,
            )

            if set_only_match:
                line = set_only_match.group(1).strip()
                requested_set_code = set_only_match.group(2).strip().upper()

            bracket_set_match = re.match(
                r"^(.*?)\s+\[([A-Za-z0-9]{2,10})\]\s*$",
                line,
            )

            if bracket_set_match:
                line = bracket_set_match.group(1).strip()
                requested_set_code = bracket_set_match.group(2).strip().upper()

        card_name = line.strip()

        if not card_name:
            continue

        parsed_items.append({
            "card_name": card_name,
            "quantity": quantity,
            "set_code": requested_set_code,
            "collector_number": requested_collector_number,
        })

    return parsed_items

def parse_custom_draft_bulk_import_text(raw_text):
    csv_items = parse_custom_draft_bulk_import_csv(raw_text)

    if csv_items:
        return csv_items

    return parse_custom_draft_bulk_import_decklist(raw_text)


def serialize_custom_draft_card_search_result(row):
    return {
        "card_uuid": row["card_uuid"],
        "card_name": row["card_name"],
        "set_code": row["set_code"],
        "release_date": row["release_date"] or "",
        "release_year": (row["release_date"] or "")[:4],
        "collector_number": row["collector_number"] or "",
        "rarity": row["rarity"] or "",
        "type_line": row["type_line"] or "",
        "mana_cost": row["mana_cost"] or "",
        "mana_value": row["mana_value"],
        "colors_json": row["colors_json"] or "[]",
        "color_identity_json": row["color_identity_json"] or "[]",
        "edhrec_rank": row["edhrec_rank"],
        "edhrec_saltiness": row["edhrec_saltiness"],
        "sort_price": row["sort_price"],
        "already_in_set": int(row["already_in_set"] or 0) == 1,
        "image_src": url_for("chaos_card_image", card_uuid=row["card_uuid"]),
    }

@app.route("/custom-draft-sets/<path:set_code>/cards/import-list", methods=["POST"])
def custom_draft_set_cards_import_list(set_code):
    clean_set_code = normalize_custom_draft_set_code(set_code)

    import_text_parts = []

    form_import_text = request.form.get("import_text") or ""
    if form_import_text.strip():
        import_text_parts.append(form_import_text)

    uploaded_file = request.files.get("import_file")
    if uploaded_file and uploaded_file.filename:
        file_bytes = uploaded_file.read()

        try:
            file_text = file_bytes.decode("utf-8-sig")
        except UnicodeDecodeError:
            file_text = file_bytes.decode("latin-1", errors="replace")

        if file_text.strip():
            import_text_parts.append(file_text)

    combined_import_text = "\n".join(import_text_parts).strip()

    if not combined_import_text:
        return jsonify({
            "ok": False,
            "message": "Paste a Moxfield/Archidekt list or upload a list file.",
        }), 400

    parsed_items = parse_custom_draft_bulk_import_text(combined_import_text)

    if not parsed_items:
        return jsonify({
            "ok": False,
            "message": "No card names could be parsed from the imported list.",
        }), 400

    rows = search_chaos_cards_for_custom_draft_import_list(
        clean_set_code,
        parsed_items,
        limit=9999,
    )

    results = [
        serialize_custom_draft_card_search_result(row)
        for row in rows
    ]

    matched_names = {
        str(row["card_name"] or "").strip().lower()
        for row in rows
    }

    unmatched_items = []
    for item in parsed_items:
        item_name = str(item.get("card_name") or "").strip()
        if item_name and item_name.lower() not in matched_names:
            unmatched_items.append(item_name)

    unmatched_unique = []
    seen_unmatched = set()

    for item_name in unmatched_items:
        item_key = item_name.lower()
        if item_key in seen_unmatched:
            continue

        seen_unmatched.add(item_key)
        unmatched_unique.append(item_name)

    return jsonify({
        "ok": True,
        "message": "Imported list loaded into search results.",
        "parsed_count": len(parsed_items),
        "result_count": len(results),
        "unmatched_count": len(unmatched_unique),
        "unmatched": unmatched_unique[:50],
        "results": results,
    })

@app.route("/custom-draft-sets/<path:set_code>/cards/import-list/add-most-recent", methods=["POST"])
def custom_draft_set_cards_import_list_add_most_recent(set_code):
    clean_set_code = normalize_custom_draft_set_code(set_code)

    import_text_parts = []

    form_import_text = request.form.get("import_text") or ""
    if form_import_text.strip():
        import_text_parts.append(form_import_text)

    uploaded_file = request.files.get("import_file")
    if uploaded_file and uploaded_file.filename:
        file_bytes = uploaded_file.read()

        try:
            file_text = file_bytes.decode("utf-8-sig")
        except UnicodeDecodeError:
            file_text = file_bytes.decode("latin-1", errors="replace")

        if file_text.strip():
            import_text_parts.append(file_text)

    combined_import_text = "\n".join(import_text_parts).strip()

    if not combined_import_text:
        return jsonify({
            "ok": False,
            "message": "Paste a Moxfield/Archidekt list or upload a list file.",
        }), 400

    parsed_items = parse_custom_draft_bulk_import_text(combined_import_text)

    if not parsed_items:
        return jsonify({
            "ok": False,
            "message": "No card names could be parsed from the imported list.",
        }), 400

    try:
        result = bulk_add_most_recent_cards_to_custom_draft_set(
            clean_set_code,
            parsed_items,
        )

        result["message"] = (
            f"Imported {result['added_count']} card(s). "
            f"Skipped {result['skipped_count']} already-added or duplicate card(s). "
            f"Unresolved {result['unresolved_count']} card name(s)."
        )

        return jsonify(result)

    except Exception as exc:
        return jsonify({
            "ok": False,
            "message": str(exc),
        }), 400

@app.route("/custom-draft-sets/<path:set_code>/cards/search", methods=["GET"])
def custom_draft_set_cards_search(set_code):
    clean_set_code = normalize_custom_draft_set_code(set_code)
    search_text = (request.args.get("q") or "").strip()

    if not get_custom_draft_set(clean_set_code):
        return jsonify({
            "ok": False,
            "message": "Custom draft set was not found.",
            "results": [],
        }), 404

    rows = search_chaos_cards_for_custom_draft_set(
        clean_set_code,
        search_text,
        limit=999,
        rarity_filter=request.args.get("rarity") or "",
        color_identity_filter=request.args.get("color_identity") or "",
        mana_operator=request.args.get("mana_operator") or "",
        mana_value=request.args.get("mana_value") or "",
        type_filter=request.args.get("type") or "",
        set_code_filter=request.args.get("set_code") or "",
        year_start=request.args.get("year_start") or "",
        year_end=request.args.get("year_end") or "",
        sort_option=request.args.get("sort") or "name_asc",
    )

    results = [
        serialize_custom_draft_card_search_result(row)
        for row in rows
    ]

    return jsonify({
        "ok": True,
        "results": results,
    })


@app.route("/custom-draft-sets/<path:set_code>/cards/add", methods=["POST"])
def custom_draft_set_cards_add(set_code):
    clean_set_code = normalize_custom_draft_set_code(set_code)

    if not get_custom_draft_set(clean_set_code):
        return jsonify({
            "ok": False,
            "message": "Custom draft set was not found.",
        }), 404

    payload = request.get_json(silent=True) or {}
    card_uuid = (payload.get("card_uuid") or "").strip()

    try:
        result = add_card_to_custom_draft_set(clean_set_code, card_uuid)
    except Exception as exc:
        return jsonify({
            "ok": False,
            "message": str(exc),
        }), 400

    return jsonify(result)

@app.route("/custom-draft-sets/<path:set_code>/cards/<int:custom_set_card_id>/printing", methods=["POST"])
def custom_draft_set_cards_update_printing(set_code, custom_set_card_id):
    clean_set_code = normalize_custom_draft_set_code(set_code)
    payload = request.get_json(silent=True) or {}
    new_card_uuid = (payload.get("card_uuid") or "").strip()

    try:
        result = update_custom_draft_set_card_printing(
            clean_set_code,
            custom_set_card_id,
            new_card_uuid,
        )

        return jsonify(result)

    except Exception as exc:
        return jsonify({
            "ok": False,
            "message": str(exc),
        }), 400

@app.route("/custom-draft-sets/<path:set_code>/cards/bulk-category", methods=["POST"])
def custom_draft_set_cards_bulk_update_category(set_code):
    clean_set_code = normalize_custom_draft_set_code(set_code)

    if not get_custom_draft_set(clean_set_code):
        return jsonify({
            "ok": False,
            "message": "Custom draft set was not found.",
        }), 404

    payload = request.get_json(silent=True) or {}
    custom_set_card_ids = payload.get("custom_set_card_ids") or []
    special_category_index = payload.get("special_category_index", 0)

    try:
        result = bulk_update_custom_draft_set_card_category(
            clean_set_code,
            custom_set_card_ids,
            special_category_index,
        )
    except Exception as exc:
        return jsonify({
            "ok": False,
            "message": str(exc),
        }), 400

    return jsonify(result)

@app.route("/custom-draft-sets/<path:set_code>/cards/bulk-delete", methods=["POST"])
def custom_draft_set_cards_bulk_delete(set_code):
    clean_set_code = normalize_custom_draft_set_code(set_code)

    if not get_custom_draft_set(clean_set_code):
        return jsonify({
            "ok": False,
            "message": "Custom draft set was not found.",
        }), 404

    payload = request.get_json(silent=True) or {}
    custom_set_card_ids = payload.get("custom_set_card_ids") or []

    try:
        result = bulk_delete_custom_draft_set_cards(
            clean_set_code,
            custom_set_card_ids,
        )
    except Exception as exc:
        return jsonify({
            "ok": False,
            "message": str(exc),
        }), 400

    return jsonify(result)

@app.route("/custom-draft-sets/<path:set_code>/cards/<int:custom_set_card_id>/category", methods=["POST"])
def custom_draft_set_cards_update_category(set_code, custom_set_card_id):
    clean_set_code = normalize_custom_draft_set_code(set_code)

    if not get_custom_draft_set(clean_set_code):
        return jsonify({
            "ok": False,
            "message": "Custom draft set was not found.",
        }), 404

    payload = request.get_json(silent=True) or {}
    special_category_index = payload.get("special_category_index", 0)

    try:
        result = update_custom_draft_set_card_category(
            custom_set_card_id,
            special_category_index,
        )
    except Exception as exc:
        return jsonify({
            "ok": False,
            "message": str(exc),
        }), 400

    return jsonify(result)


@app.route("/custom-draft-sets/<path:set_code>/cards/<int:custom_set_card_id>/delete", methods=["POST"])
def custom_draft_set_cards_delete(set_code, custom_set_card_id):
    clean_set_code = normalize_custom_draft_set_code(set_code)

    if not get_custom_draft_set(clean_set_code):
        return jsonify({
            "ok": False,
            "message": "Custom draft set was not found.",
        }), 404

    try:
        result = delete_custom_draft_set_card(custom_set_card_id)
    except Exception as exc:
        return jsonify({
            "ok": False,
            "message": str(exc),
        }), 400

    return jsonify(result)

@app.route("/sets", methods=["GET", "POST"])
def sets():
    if request.method == "POST":
        update_selected_sets_from_form(request.form)
        flash("Magic set selection saved.")
        return redirect(url_for("sets"))

    config_values = get_request_config()
    selected_chaos_pack_types = get_selected_chaos_pack_types(config_values)
    all_sets = get_all_sets()
    selected_set_codes = get_selected_set_codes()
    custom_draft_sets = get_custom_draft_sets()

    current_year = datetime.now().year

    return render_template(
        "sets.html",
        config=config_values,
        all_sets=all_sets,
        selected_set_codes=selected_set_codes,
        custom_draft_sets=custom_draft_sets,
        current_year=current_year,
        current_game_mode=(config_values.get("game_mode") or "custom").strip().lower(),
        chaos_pack_type_options=CHAOS_PACK_TYPE_OPTIONS,
        selected_chaos_pack_types=selected_chaos_pack_types,
    )

if __name__ == "__main__":
    initialize_database()
    set_runtime_debug_log_enabled_from_config()
    app.run(host="0.0.0.0", port=5000, debug=True)