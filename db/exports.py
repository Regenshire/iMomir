import os
import shutil
import time
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from io import BytesIO

from paths import (
    ALTERNATE_SOURCE_DIR,
    CAMPAIGN_PLAYER_PORTRAIT_DIR,
    EXPORT_ROOT_DIR,
    RUNTIME_BASE_DIR,
)

from db.database import get_db_connection


IMOMIR_EXPORT_VERSION = "1"

EXPORT_KIND_PACKS = "packs"
EXPORT_KIND_CAMPAIGN = "campaign"
EXPORT_KIND_FULL = "full"

MANIFEST_FILENAME = "manifest.xml"
ARCHIVE_FILE_ROOT = "files"


PACK_TABLE_NAMES = {
    "tracked_chaos_packs",
    "tracked_chaos_pack_cards",
    "tracked_chaos_pack_openings",
    "tracked_chaos_pack_campaigns",
    "alternate_sources",
}

SETTINGS_TABLE_NAMES = {
    "app_config",
    "selected_sets",
}

# These are intentionally broad because campaign tables have evolved over time.
# The filtering below also checks actual table/column existence at runtime.
CAMPAIGN_TABLE_NAME_PREFIXES = (
    "chaos_campaign",
    "campaign_",
)

CAMPAIGN_TABLE_NAME_CONTAINS = (
    "draft_game",
    "campaign",
)

PLAYER_TABLE_NAMES = {
    "chaos_players",
}

HISTORY_TABLE_NAMES = {
    "chaos_pack_history",
    "card_history",
    "tracked_chaos_pack_openings",
}

FILE_FIELD_NAMES = {
    "local_image_path",
    "fullbleed_image_path",
    "portrait_image_path",
    "image_path",
}


def utc_now_text():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def utc_timestamp_for_filename():
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def make_export_filename_safe(value):
    clean_value = str(value or "").strip()

    if not clean_value:
        return "imomir_export"

    safe_chars = []

    for char in clean_value:
        if char.isalnum() or char in ("-", "_", "."):
            safe_chars.append(char)
        else:
            safe_chars.append("_")

    safe_value = "".join(safe_chars).strip("_")

    return safe_value or "imomir_export"

def normalize_id_list(values):
    normalized_values = []

    for value in values or []:
        try:
            parsed_value = int(value)
        except (TypeError, ValueError):
            continue

        if parsed_value > 0 and parsed_value not in normalized_values:
            normalized_values.append(parsed_value)

    return normalized_values

def ensure_export_root():
    os.makedirs(EXPORT_ROOT_DIR, exist_ok=True)

def delete_from_table_if_exists(cursor, table_name):
    cursor.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name = ?
        """,
        (table_name,),
    )

    if not cursor.fetchone():
        return 0

    cursor.execute(f"SELECT COUNT(*) AS row_count FROM {table_name}")
    row = cursor.fetchone()
    row_count = int(row["row_count"] or 0)

    cursor.execute(f"DELETE FROM {table_name}")

    return row_count


def clear_all_history_data():
    conn = get_db_connection()
    cursor = conn.cursor()

    deleted_counts = {}

    try:
        for table_name in [
            "card_history",
            "chaos_pack_history",
            "tracked_chaos_pack_openings",
        ]:
            deleted_counts[table_name] = delete_from_table_if_exists(cursor, table_name)

        # Some newer campaign/history tables may exist depending on your current schema.
        for table_name in get_sqlite_table_names():
            clean_name = table_name.lower()

            if clean_name in deleted_counts:
                continue

            if "history" in clean_name or "opening" in clean_name:
                deleted_counts[table_name] = delete_from_table_if_exists(cursor, table_name)

        conn.commit()

    finally:
        conn.close()

    return {
        "deleted_counts": deleted_counts,
        "total_deleted": sum(deleted_counts.values()),
    }

def clear_all_packs_data():
    conn = get_db_connection()
    cursor = conn.cursor()

    deleted_counts = {}

    try:
        # Child/link tables first.
        for table_name in [
            "tracked_chaos_pack_campaigns",
            "tracked_chaos_pack_openings",
            "tracked_chaos_pack_cards",
            "tracked_chaos_packs",
        ]:
            deleted_counts[table_name] = delete_from_table_if_exists(cursor, table_name)

        conn.commit()

    finally:
        conn.close()

    return {
        "deleted_counts": deleted_counts,
        "total_deleted": sum(deleted_counts.values()),
    }

def clear_export_root():
    ensure_export_root()

    removed_count = 0

    for item_name in os.listdir(EXPORT_ROOT_DIR):
        item_path = os.path.join(EXPORT_ROOT_DIR, item_name)

        try:
            if os.path.isdir(item_path):
                shutil.rmtree(item_path)
                removed_count += 1
            elif os.path.isfile(item_path):
                os.remove(item_path)
                removed_count += 1
        except Exception:
            continue

    return {
        "removed_count": removed_count,
    }


def normalize_auto_clear_exports_days(value):
    clean_value = str(value or "7").strip().lower()

    if clean_value == "off":
        return None

    if clean_value not in {"1", "7", "30"}:
        clean_value = "7"

    return int(clean_value)


def auto_clear_export_root(auto_clear_exports_value):
    clear_days = normalize_auto_clear_exports_days(auto_clear_exports_value)

    if clear_days is None:
        return {
            "removed_count": 0,
            "enabled": False,
            "days": None,
        }

    ensure_export_root()

    cutoff_timestamp = time.time() - (clear_days * 24 * 60 * 60)
    removed_count = 0

    for item_name in os.listdir(EXPORT_ROOT_DIR):
        item_path = os.path.join(EXPORT_ROOT_DIR, item_name)

        try:
            item_mtime = os.path.getmtime(item_path)

            if item_mtime >= cutoff_timestamp:
                continue

            if os.path.isdir(item_path):
                shutil.rmtree(item_path)
                removed_count += 1
            elif os.path.isfile(item_path):
                os.remove(item_path)
                removed_count += 1
        except Exception:
            continue

    return {
        "removed_count": removed_count,
        "enabled": True,
        "days": clear_days,
    }

def get_sqlite_table_names():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    )

    table_names = [row["name"] for row in cursor.fetchall()]
    conn.close()

    return table_names


def table_exists(table_name):
    return table_name in set(get_sqlite_table_names())


def get_table_columns(table_name):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(f"PRAGMA table_info({table_name})")

    columns = [
        row["name"]
        for row in cursor.fetchall()
    ]

    conn.close()

    return columns


def table_has_column(table_name, column_name):
    return column_name in set(get_table_columns(table_name))


def fetch_table_rows(table_name, where_clause="", params=None):
    params = params or []

    conn = get_db_connection()
    cursor = conn.cursor()

    sql = f"SELECT * FROM {table_name}"

    if where_clause:
        sql += f" WHERE {where_clause}"

    cursor.execute(sql, params)

    rows = [
        {
            column_name: row[column_name]
            for column_name in row.keys()
        }
        for row in cursor.fetchall()
    ]

    conn.close()

    return rows


def get_all_tracked_pack_ids():
    if not table_exists("tracked_chaos_packs"):
        return []

    rows = fetch_table_rows("tracked_chaos_packs")

    return normalize_id_list([
        row.get("tracked_pack_id")
        for row in rows
    ])


def get_tracked_pack_ids_for_campaign(campaign_id):
    campaign_id = int(campaign_id)

    if not table_exists("tracked_chaos_packs"):
        return []

    if table_has_column("tracked_chaos_packs", "campaign_id"):
        rows = fetch_table_rows(
            "tracked_chaos_packs",
            where_clause="campaign_id = ?",
            params=[campaign_id],
        )

        return normalize_id_list([
            row.get("tracked_pack_id")
            for row in rows
        ])

    # If packs are linked through a join table, discover a likely table dynamically.
    candidate_pack_ids = []

    for table_name in get_sqlite_table_names():
        if not table_has_column(table_name, "campaign_id"):
            continue

        if not table_has_column(table_name, "tracked_pack_id"):
            continue

        rows = fetch_table_rows(
            table_name,
            where_clause="campaign_id = ?",
            params=[campaign_id],
        )

        candidate_pack_ids.extend(row.get("tracked_pack_id") for row in rows)

    return normalize_id_list(candidate_pack_ids)

def get_tracked_pack_ids_for_default_campaign():
    if not table_exists("tracked_chaos_packs"):
        return []

    # Newer schema: packs may have a direct campaign_id column.
    if table_has_column("tracked_chaos_packs", "campaign_id"):
        rows = fetch_table_rows(
            "tracked_chaos_packs",
            where_clause="campaign_id IS NULL",
            params=[],
        )

        return normalize_id_list([
            row.get("tracked_pack_id")
            for row in rows
        ])

    all_pack_ids = get_all_tracked_pack_ids()

    # Join-table schema: default/no-campaign packs are packs that are not linked
    # to any explicit campaign.
    linked_pack_ids = set()

    for table_name in get_sqlite_table_names():
        if not table_has_column(table_name, "campaign_id"):
            continue

        if not table_has_column(table_name, "tracked_pack_id"):
            continue

        rows = fetch_table_rows(table_name)

        linked_pack_ids.update(
            normalize_id_list([
                row.get("tracked_pack_id")
                for row in rows
            ])
        )

    if linked_pack_ids:
        return [
            pack_id
            for pack_id in all_pack_ids
            if pack_id not in linked_pack_ids
        ]

    # Older/unscoped schema: all tracked packs are effectively No Campaign packs.
    return all_pack_ids


def get_card_uuids_for_tracked_pack_ids(tracked_pack_ids):
    pack_ids = normalize_id_list(tracked_pack_ids)

    if not pack_ids:
        return []

    if not table_exists("tracked_chaos_pack_cards"):
        return []

    placeholders = ",".join(["?"] * len(pack_ids))

    rows = fetch_table_rows(
        "tracked_chaos_pack_cards",
        where_clause=f"tracked_pack_id IN ({placeholders})",
        params=pack_ids,
    )

    card_uuids = sorted({
        str(row.get("card_uuid") or "").strip()
        for row in rows
        if str(row.get("card_uuid") or "").strip()
    })

    return card_uuids


def get_existing_pack_backup_tables():
    existing_tables = set(get_sqlite_table_names())

    return sorted([
        table_name
        for table_name in PACK_TABLE_NAMES
        if table_name in existing_tables
    ])


def get_existing_settings_backup_tables():
    existing_tables = set(get_sqlite_table_names())

    return sorted([
        table_name
        for table_name in SETTINGS_TABLE_NAMES
        if table_name in existing_tables
    ])


def is_campaign_table_name(table_name):
    clean_name = (table_name or "").strip().lower()

    if clean_name in PLAYER_TABLE_NAMES:
        return True

    if any(clean_name.startswith(prefix) for prefix in CAMPAIGN_TABLE_NAME_PREFIXES):
        return True

    if any(fragment in clean_name for fragment in CAMPAIGN_TABLE_NAME_CONTAINS):
        return True

    return False


def get_existing_campaign_backup_tables():
    existing_tables = get_sqlite_table_names()

    return sorted([
        table_name
        for table_name in existing_tables
        if is_campaign_table_name(table_name)
    ])


def get_existing_full_backup_tables():
    excluded_tables = {
        # Large downloaded/generated reference tables should be refreshable,
        # not part of user backup payloads.
        "cards",
        "sets",
        "chaos_cards",
        "chaos_booster_variants",
        "chaos_booster_variant_contents",
        "chaos_booster_sheets",
        "chaos_booster_sheet_cards",
        "scryfall_default_cards",
        "card_prices",
        "import_metadata",
        "chaos_session_state",
    }

    return sorted([
        table_name
        for table_name in get_sqlite_table_names()
        if table_name not in excluded_tables
    ])


def get_rows_for_pack_table(table_name, tracked_pack_ids):
    pack_ids = normalize_id_list(tracked_pack_ids)

    if not pack_ids:
        return []

    if table_name == "alternate_sources":
        card_uuids = get_card_uuids_for_tracked_pack_ids(pack_ids)

        if not card_uuids:
            return []

        placeholders = ",".join(["?"] * len(card_uuids))

        return fetch_table_rows(
            table_name,
            where_clause=f"card_uuid IN ({placeholders})",
            params=card_uuids,
        )

    if table_has_column(table_name, "tracked_pack_id"):
        placeholders = ",".join(["?"] * len(pack_ids))

        return fetch_table_rows(
            table_name,
            where_clause=f"tracked_pack_id IN ({placeholders})",
            params=pack_ids,
        )

    return fetch_table_rows(table_name)


def get_rows_for_campaign_table(table_name, campaign_id):
    campaign_id = int(campaign_id)

    if table_has_column(table_name, "campaign_id"):
        return fetch_table_rows(
            table_name,
            where_clause="campaign_id = ?",
            params=[campaign_id],
        )

    if table_name == "chaos_players":
        # Older schema may not scope players by campaign. Include players for campaign backup.
        return fetch_table_rows(table_name)

    return fetch_table_rows(table_name)

def get_rows_for_default_campaign_table(table_name):
    if table_has_column(table_name, "campaign_id"):
        return fetch_table_rows(
            table_name,
            where_clause="campaign_id IS NULL",
            params=[],
        )

    if table_name == "chaos_players":
        # Older schema may not scope players by campaign. Include players for No Campaign backup.
        return fetch_table_rows(table_name)

    return []

def build_pack_rows_by_table(tracked_pack_ids=None):
    pack_ids = normalize_id_list(tracked_pack_ids or [])

    if not pack_ids:
        pack_ids = get_all_tracked_pack_ids()

    if not pack_ids:
        raise ValueError("No packs were available to export.")

    rows_by_table = {}

    for table_name in get_existing_pack_backup_tables():
        rows_by_table[table_name] = get_rows_for_pack_table(
            table_name,
            tracked_pack_ids=pack_ids,
        )

    return rows_by_table


def build_campaign_rows_by_table(campaign_id):
    campaign_id = int(campaign_id)

    rows_by_table = {}

    for table_name in get_existing_campaign_backup_tables():
        rows_by_table[table_name] = get_rows_for_campaign_table(
            table_name,
            campaign_id=campaign_id,
        )

    campaign_pack_ids = get_tracked_pack_ids_for_campaign(campaign_id)

    if campaign_pack_ids:
        pack_rows_by_table = build_pack_rows_by_table(campaign_pack_ids)

        for table_name, table_rows in pack_rows_by_table.items():
            existing_rows = rows_by_table.get(table_name, [])
            rows_by_table[table_name] = merge_rows_by_identity(existing_rows, table_rows)

    return rows_by_table

def build_default_campaign_rows_by_table():
    rows_by_table = {}

    for table_name in get_existing_campaign_backup_tables():
        rows_by_table[table_name] = get_rows_for_default_campaign_table(table_name)

    default_pack_ids = get_tracked_pack_ids_for_default_campaign()

    if default_pack_ids:
        pack_rows_by_table = build_pack_rows_by_table(default_pack_ids)

        for table_name, table_rows in pack_rows_by_table.items():
            existing_rows = rows_by_table.get(table_name, [])
            rows_by_table[table_name] = merge_rows_by_identity(existing_rows, table_rows)

    return rows_by_table

def build_full_rows_by_table():
    rows_by_table = {}

    for table_name in get_existing_full_backup_tables():
        rows_by_table[table_name] = fetch_table_rows(table_name)

    return rows_by_table


def get_row_identity(row):
    # Used only to merge rows inside one export payload.
    # Keep simple and stable across schema changes.
    for key_name in (
        "tracked_pack_id",
        "tracked_pack_card_id",
        "opening_id",
        "alternate_source_id",
        "campaign_id",
        "player_id",
        "draft_game_id",
        "config_key",
        "set_code",
        "history_id",
    ):
        if key_name in row:
            return (key_name, row.get(key_name))

    return tuple(sorted(row.items()))


def merge_rows_by_identity(left_rows, right_rows):
    merged_lookup = {}

    for row in list(left_rows or []) + list(right_rows or []):
        merged_lookup[get_row_identity(row)] = row

    return list(merged_lookup.values())


def add_xml_field(row_element, field_name, field_value):
    field_element = ET.SubElement(row_element, "field")
    field_element.set("name", str(field_name))

    if field_value is None:
        field_element.set("is_null", "1")
        field_element.text = ""
    else:
        field_element.set("is_null", "0")
        field_element.text = str(field_value)

    return field_element


def add_xml_table(tables_element, table_name, rows):
    table_element = ET.SubElement(tables_element, "table")
    table_element.set("name", table_name)
    table_element.set("row_count", str(len(rows or [])))

    for row in rows or []:
        row_element = ET.SubElement(table_element, "row")

        for field_name, field_value in row.items():
            add_xml_field(row_element, field_name, field_value)

    return table_element


def is_safe_relative_path(relative_path):
    clean_path = str(relative_path or "").replace("\\", "/").lstrip("/")

    if not clean_path:
        return False

    if ".." in clean_path.split("/"):
        return False

    return True


def resolve_runtime_relative_path(relative_path):
    clean_path = str(relative_path or "").replace("\\", "/").lstrip("/")

    if not is_safe_relative_path(clean_path):
        return ""

    absolute_path = os.path.abspath(os.path.join(RUNTIME_BASE_DIR, clean_path))
    runtime_base = os.path.abspath(RUNTIME_BASE_DIR)

    if not absolute_path.startswith(runtime_base):
        return ""

    return absolute_path

def normalize_export_file_relative_path(table_name, field_name, field_value):
    clean_value = str(field_value or "").strip()

    if not clean_value:
        return ""

    clean_value = clean_value.replace("\\", "/").lstrip("/")

    # chaos_players.portrait_image_path stores only the portrait filename.
    # The actual file lives in data/campaign_player_portraits.
    if table_name == "chaos_players" and field_name == "portrait_image_path":
        if "/" not in clean_value:
            return os.path.relpath(
                os.path.join(CAMPAIGN_PLAYER_PORTRAIT_DIR, clean_value),
                RUNTIME_BASE_DIR,
            ).replace("\\", "/")

    return clean_value

def collect_file_payloads(rows_by_table):
    file_payloads = {}

    for table_name, rows in rows_by_table.items():
        for row in rows or []:
            for field_name, field_value in row.items():
                if field_name not in FILE_FIELD_NAMES:
                    continue

                relative_path = normalize_export_file_relative_path(
                    table_name=table_name,
                    field_name=field_name,
                    field_value=field_value,
                )

                if not relative_path:
                    continue

                if not is_safe_relative_path(relative_path):
                    continue

                absolute_path = resolve_runtime_relative_path(relative_path)

                if not absolute_path:
                    continue

                if not os.path.exists(absolute_path) or not os.path.isfile(absolute_path):
                    continue

                clean_relative_path = relative_path.replace("\\", "/").lstrip("/")
                archive_path = f"{ARCHIVE_FILE_ROOT}/{clean_relative_path}"

                file_payloads[clean_relative_path] = {
                    "relative_path": clean_relative_path,
                    "absolute_path": absolute_path,
                    "archive_path": archive_path,
                }

    return list(file_payloads.values())


def build_export_manifest(export_kind, rows_by_table):
    root = ET.Element("imomir_export")
    root.set("version", IMOMIR_EXPORT_VERSION)
    root.set("kind", export_kind)
    root.set("created_at_utc", utc_now_text())

    tables_element = ET.SubElement(root, "tables")

    for table_name in sorted(rows_by_table.keys()):
        add_xml_table(
            tables_element,
            table_name=table_name,
            rows=rows_by_table[table_name],
        )

    files_element = ET.SubElement(root, "files")

    for file_payload in collect_file_payloads(rows_by_table):
        file_element = ET.SubElement(files_element, "file")
        file_element.set("relative_path", file_payload["relative_path"])
        file_element.set("archive_path", file_payload["archive_path"])

    return root


def write_manifest_to_bytes(root):
    buffer = BytesIO()
    tree = ET.ElementTree(root)
    ET.indent(tree, space="    ", level=0)
    tree.write(buffer, encoding="utf-8", xml_declaration=True)
    buffer.seek(0)

    return buffer.getvalue()


def create_export_archive(export_kind, rows_by_table, filename_prefix, auto_clear_exports_value=None):
    if auto_clear_exports_value is not None:
        auto_clear_export_root(auto_clear_exports_value)

    ensure_export_root()

    safe_prefix = make_export_filename_safe(filename_prefix)
    zip_filename = f"{safe_prefix}_{utc_timestamp_for_filename()}.zip"
    zip_path = os.path.join(EXPORT_ROOT_DIR, zip_filename)

    root = build_export_manifest(export_kind, rows_by_table)
    manifest_bytes = write_manifest_to_bytes(root)
    file_payloads = collect_file_payloads(rows_by_table)

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr(MANIFEST_FILENAME, manifest_bytes)

        for file_payload in file_payloads:
            zip_file.write(
                file_payload["absolute_path"],
                file_payload["archive_path"],
            )

    return {
        "zip_path": zip_path,
        "zip_filename": zip_filename,
        "export_kind": export_kind,
        "table_count": len(rows_by_table),
        "file_count": len(file_payloads),
    }


def export_packs_archive(tracked_pack_ids=None, auto_clear_exports_value=None):
    rows_by_table = build_pack_rows_by_table(tracked_pack_ids)

    return create_export_archive(
        export_kind=EXPORT_KIND_PACKS,
        rows_by_table=rows_by_table,
        filename_prefix="imomir_packs_backup",
        auto_clear_exports_value=auto_clear_exports_value,
    )


def export_campaign_archive(campaign_id, auto_clear_exports_value=None):
    rows_by_table = build_campaign_rows_by_table(campaign_id)

    if not any(rows_by_table.values()):
        raise ValueError("No campaign data was available to export.")

    return create_export_archive(
        export_kind=EXPORT_KIND_CAMPAIGN,
        rows_by_table=rows_by_table,
        filename_prefix=f"imomir_campaign_{campaign_id}_backup",
        auto_clear_exports_value=auto_clear_exports_value,
    )

def export_default_campaign_archive(auto_clear_exports_value=None):
    rows_by_table = build_default_campaign_rows_by_table()

    if not any(rows_by_table.values()):
        raise ValueError("No default campaign data was available to export.")

    return create_export_archive(
        export_kind=EXPORT_KIND_CAMPAIGN,
        rows_by_table=rows_by_table,
        filename_prefix="imomir_no_campaign_backup",
        auto_clear_exports_value=auto_clear_exports_value,
    )

def export_full_archive(auto_clear_exports_value=None):
    rows_by_table = build_full_rows_by_table()

    return create_export_archive(
        export_kind=EXPORT_KIND_FULL,
        rows_by_table=rows_by_table,
        filename_prefix="imomir_full_backup",
        auto_clear_exports_value=auto_clear_exports_value,
    )


def load_export_manifest_from_zip(zip_file):
    if MANIFEST_FILENAME not in zip_file.namelist():
        raise ValueError("Export archive does not contain manifest.xml.")

    manifest_bytes = zip_file.read(MANIFEST_FILENAME)
    root = ET.fromstring(manifest_bytes)

    if root.tag != "imomir_export":
        raise ValueError("XML manifest is not an iMomir export file.")

    version = root.get("version") or ""

    if version != IMOMIR_EXPORT_VERSION:
        raise ValueError(f"Unsupported iMomir export version: {version}")

    return root


def get_tables_from_manifest(root):
    tables_element = root.find("tables")

    if tables_element is None:
        return {}

    rows_by_table = {}

    for table_element in tables_element.findall("table"):
        table_name = (table_element.get("name") or "").strip()

        if not table_name:
            continue

        table_rows = []

        for row_element in table_element.findall("row"):
            row_data = {}

            for field_element in row_element.findall("field"):
                field_name = (field_element.get("name") or "").strip()

                if not field_name:
                    continue

                if field_element.get("is_null") == "1":
                    row_data[field_name] = None
                else:
                    row_data[field_name] = field_element.text or ""

            if row_data:
                table_rows.append(row_data)

        rows_by_table[table_name] = table_rows

    return rows_by_table

def get_campaign_name_from_rows(rows_by_table):
    for table_name, table_rows in rows_by_table.items():
        if not table_name.startswith("chaos_campaign"):
            continue

        for row_data in table_rows:
            campaign_name = (row_data.get("campaign_name") or "").strip()
            if campaign_name:
                return campaign_name

    return ""


def get_or_create_import_campaign_id(campaign_name):
    clean_campaign_name = (campaign_name or "").strip()

    if not clean_campaign_name:
        clean_campaign_name = "Imported Campaign"

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT campaign_id
        FROM chaos_campaigns
        WHERE campaign_name = ?
        """,
        (clean_campaign_name,),
    )

    row = cursor.fetchone()

    if row:
        campaign_id = int(row["campaign_id"])

        cursor.execute(
            """
            UPDATE chaos_campaigns
            SET is_active = 1
            WHERE campaign_id = ?
            """,
            (campaign_id,),
        )

        conn.commit()
        conn.close()
        return campaign_id

    cursor.execute(
        """
        INSERT INTO chaos_campaigns (
            campaign_name,
            is_active,
            created_at_utc
        )
        VALUES (?, 1, ?)
        """,
        (
            clean_campaign_name,
            utc_now_text(),
        ),
    )

    campaign_id = int(cursor.lastrowid)

    conn.commit()
    conn.close()

    return campaign_id


def remap_campaign_import_rows(rows_by_table, campaign_name_override):
    clean_campaign_name = (campaign_name_override or "").strip()

    if not clean_campaign_name:
        clean_campaign_name = get_campaign_name_from_rows(rows_by_table)

    if not clean_campaign_name:
        clean_campaign_name = "Imported Campaign"

    target_campaign_id = get_or_create_import_campaign_id(clean_campaign_name)
    target_campaign_key = str(target_campaign_id)

    updated_rows_by_table = {}

    for table_name, table_rows in rows_by_table.items():
        updated_rows = []

        for row_data in table_rows:
            updated_row = dict(row_data)

            if table_name == "chaos_campaigns":
                updated_row["campaign_id"] = target_campaign_id
                updated_row["campaign_name"] = clean_campaign_name
                updated_row["is_active"] = 1

                if "created_at_utc" in updated_row and not updated_row.get("created_at_utc"):
                    updated_row["created_at_utc"] = utc_now_text()

            elif "campaign_id" in updated_row:
                updated_row["campaign_id"] = target_campaign_id

            if table_name == "tracked_chaos_pack_campaigns":
                updated_row["campaign_id"] = target_campaign_id
                updated_row["campaign_key"] = target_campaign_key
                updated_row["campaign_enabled"] = updated_row.get("campaign_enabled") or 1

                if "added_at_utc" in updated_row and not updated_row.get("added_at_utc"):
                    updated_row["added_at_utc"] = utc_now_text()

            updated_rows.append(updated_row)

        updated_rows_by_table[table_name] = updated_rows

    if "chaos_campaigns" not in updated_rows_by_table:
        updated_rows_by_table["chaos_campaigns"] = [{
            "campaign_id": target_campaign_id,
            "campaign_name": clean_campaign_name,
            "is_active": 1,
            "created_at_utc": utc_now_text(),
        }]

    return updated_rows_by_table

def get_allowed_tables_for_import(import_scope):
    clean_scope = (import_scope or "").strip().lower()

    if clean_scope == EXPORT_KIND_PACKS:
        return set(get_existing_pack_backup_tables())

    if clean_scope == EXPORT_KIND_CAMPAIGN:
        return (
            set(get_existing_campaign_backup_tables())
            | set(get_existing_pack_backup_tables())
            | {"chaos_campaigns"}
        )

    if clean_scope == EXPORT_KIND_FULL:
        return set(get_existing_full_backup_tables())

    raise ValueError(f"Unknown import scope: {import_scope}")


def extract_files_from_manifest(zip_file, root):
    files_element = root.find("files")

    if files_element is None:
        return 0

    extracted_count = 0
    archive_names = set(zip_file.namelist())

    for file_element in files_element.findall("file"):
        relative_path = (file_element.get("relative_path") or "").strip()
        archive_path = (file_element.get("archive_path") or "").strip()

        if not relative_path or not archive_path:
            continue

        if archive_path not in archive_names:
            continue

        destination_path = resolve_runtime_relative_path(relative_path)

        if not destination_path:
            continue

        os.makedirs(os.path.dirname(destination_path), exist_ok=True)

        with zip_file.open(archive_path) as source_file:
            with open(destination_path, "wb") as output_file:
                shutil.copyfileobj(source_file, output_file)

        extracted_count += 1

    return extracted_count


def insert_or_replace_manifest_rows(rows_by_table, allowed_tables):
    allowed_tables = set(allowed_tables or [])
    existing_tables = set(get_sqlite_table_names())

    imported_rows = 0

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        for table_name, table_rows in rows_by_table.items():
            if table_name not in existing_tables:
                continue

            if allowed_tables and table_name not in allowed_tables:
                continue

            table_columns = set(get_table_columns(table_name))

            for row_data in table_rows:
                filtered_row = {
                    field_name: field_value
                    for field_name, field_value in row_data.items()
                    if field_name in table_columns
                }

                if not filtered_row:
                    continue

                columns = list(filtered_row.keys())
                column_sql = ", ".join(columns)
                placeholder_sql = ", ".join(["?"] * len(columns))

                cursor.execute(
                    f"INSERT OR REPLACE INTO {table_name} ({column_sql}) VALUES ({placeholder_sql})",
                    [filtered_row[column] for column in columns],
                )

                imported_rows += 1

        conn.commit()

    finally:
        conn.close()

    return imported_rows


def import_archive_from_path(archive_path, import_scope, campaign_name_override=""):
    if not archive_path or not os.path.exists(archive_path):
        raise ValueError("Export archive was not found.")

    with zipfile.ZipFile(archive_path, "r") as zip_file:
        root = load_export_manifest_from_zip(zip_file)
        manifest_kind = (root.get("kind") or "").strip().lower()

        # Packs screen can import pack-only exports or campaign/full exports,
        # but only the pack tables are restored when import_scope='packs'.
        allowed_tables = get_allowed_tables_for_import(import_scope)

        extracted_files = extract_files_from_manifest(zip_file, root)
        rows_by_table = get_tables_from_manifest(root)

        if import_scope == EXPORT_KIND_CAMPAIGN:
            rows_by_table = remap_campaign_import_rows(
                rows_by_table,
                campaign_name_override,
            )

        imported_rows = insert_or_replace_manifest_rows(rows_by_table, allowed_tables)

    return {
        "manifest_kind": manifest_kind,
        "import_scope": import_scope,
        "imported_rows": imported_rows,
        "extracted_files": extracted_files,
    }


def import_archive_from_file_object(file_object, import_scope, campaign_name_override=""):
    if not file_object:
        raise ValueError("No export archive file was provided.")

    archive_bytes = file_object.read()

    if not archive_bytes:
        raise ValueError("Export archive file was empty.")

    with zipfile.ZipFile(BytesIO(archive_bytes), "r") as zip_file:
        root = load_export_manifest_from_zip(zip_file)
        manifest_kind = (root.get("kind") or "").strip().lower()

        allowed_tables = get_allowed_tables_for_import(import_scope)

        extracted_files = extract_files_from_manifest(zip_file, root)
        rows_by_table = get_tables_from_manifest(root)

        if import_scope == EXPORT_KIND_CAMPAIGN:
            rows_by_table = remap_campaign_import_rows(
                rows_by_table,
                campaign_name_override,
            )

        imported_rows = insert_or_replace_manifest_rows(rows_by_table, allowed_tables)

    return {
        "manifest_kind": manifest_kind,
        "import_scope": import_scope,
        "imported_rows": imported_rows,
        "extracted_files": extracted_files,
    }