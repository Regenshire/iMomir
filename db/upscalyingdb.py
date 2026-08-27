import hashlib
import json
import os
from datetime import datetime, timedelta, timezone

from db.database import (
    ensure_column_exists,
    get_db_connection,
)
from paths import UPSCALED_SCRYFALL_DIR


UPSCALE_QUALITY_PENDING = "pending"
UPSCALE_QUALITY_ACCEPTED = "accepted"
UPSCALE_QUALITY_REJECTED = "rejected"
UPSCALE_QUALITY_SUPERSEDED = "superseded"


def upscaling_utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def ensure_upscaling_schema():
    os.makedirs(
        UPSCALED_SCRYFALL_DIR,
        exist_ok=True,
    )

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS upscaled_images (
            upscaled_image_id INTEGER PRIMARY KEY AUTOINCREMENT,

            card_uuid TEXT,
            scryfall_id TEXT,
            set_code TEXT,
            collector_number TEXT,

            face_kind TEXT NOT NULL DEFAULT 'single',

            source_image_path TEXT,
            source_sha256 TEXT,

            output_image_path TEXT NOT NULL,
            output_sha256 TEXT,

            output_width INTEGER,
            output_height INTEGER,

            has_generated_bleed INTEGER NOT NULL DEFAULT 0,
            bleed_size_mm REAL,
            fullbleed_image_path TEXT,
            fullbleed_sha256 TEXT,
            bleed_model_id TEXT,
            bleed_plugin_id TEXT,
            bleed_plugin_version TEXT,
            bleed_generation_metadata TEXT,

            plugin_id TEXT NOT NULL,
            plugin_version TEXT,
            pipeline_version TEXT,
            plugin_result_json TEXT,

            quality_status TEXT NOT NULL DEFAULT 'accepted',
            quality_score REAL,
            text_validation_score REAL,

            is_current INTEGER NOT NULL DEFAULT 1,

            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT
        )
        """
    )

    ensure_column_exists(
        cursor,
        "upscaled_images",
        "plugin_result_json",
        "TEXT",
    )

    ensure_column_exists(
        cursor,
        "upscaled_images",
        "has_generated_bleed",
        "INTEGER NOT NULL DEFAULT 0",
    )

    ensure_column_exists(
        cursor,
        "upscaled_images",
        "bleed_size_mm",
        "REAL",
    )

    ensure_column_exists(
        cursor,
        "upscaled_images",
        "fullbleed_image_path",
        "TEXT",
    )

    ensure_column_exists(
        cursor,
        "upscaled_images",
        "fullbleed_sha256",
        "TEXT",
    )

    ensure_column_exists(
        cursor,
        "upscaled_images",
        "bleed_model_id",
        "TEXT",
    )

    ensure_column_exists(
        cursor,
        "upscaled_images",
        "bleed_plugin_id",
        "TEXT",
    )

    ensure_column_exists(
        cursor,
        "upscaled_images",
        "bleed_plugin_version",
        "TEXT",
    )

    ensure_column_exists(
        cursor,
        "upscaled_images",
        "bleed_generation_metadata",
        "TEXT",
    )


    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_upscaled_images_card_uuid
        ON upscaled_images (
            card_uuid,
            face_kind,
            is_current,
            quality_status
        )
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_upscaled_images_scryfall_id
        ON upscaled_images (
            scryfall_id,
            face_kind,
            is_current,
            quality_status
        )
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_upscaled_images_set_number
        ON upscaled_images (
            set_code,
            collector_number,
            face_kind,
            is_current,
            quality_status
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS upscaling_jobs (
            upscaling_job_id INTEGER PRIMARY KEY AUTOINCREMENT,

            plugin_id TEXT NOT NULL,
            plugin_version TEXT,

            job_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued',

            total_items INTEGER NOT NULL DEFAULT 0,
            processed_items INTEGER NOT NULL DEFAULT 0,
            completed_items INTEGER NOT NULL DEFAULT 0,
            failed_items INTEGER NOT NULL DEFAULT 0,
            skipped_items INTEGER NOT NULL DEFAULT 0,
            rejected_items INTEGER NOT NULL DEFAULT 0,

            settings_json TEXT,

            created_at_utc TEXT NOT NULL,
            started_at_utc TEXT,
            completed_at_utc TEXT,

            error_message TEXT
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS upscaling_job_items (
            upscaling_job_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            upscaling_job_id INTEGER NOT NULL,

            card_uuid TEXT,
            scryfall_id TEXT,
            set_code TEXT,
            collector_number TEXT,
            face_kind TEXT NOT NULL DEFAULT 'single',

            source_sha256 TEXT,

            status TEXT NOT NULL DEFAULT 'queued',
            attempt_number INTEGER NOT NULL DEFAULT 0,

            upscaled_image_id INTEGER,

            created_at_utc TEXT NOT NULL,
            started_at_utc TEXT,
            completed_at_utc TEXT,

            processing_ms INTEGER,
            error_message TEXT,

            FOREIGN KEY (upscaling_job_id)
                REFERENCES upscaling_jobs (upscaling_job_id),

            FOREIGN KEY (upscaled_image_id)
                REFERENCES upscaled_images (upscaled_image_id)
        )
        """
    )

    conn.commit()
    conn.close()


def _row_value(row, key, default=""):
    if row is None:
        return default

    try:
        if hasattr(row, "keys") and key in row.keys():
            value = row[key]
            return default if value is None else value
    except Exception:
        pass

    if isinstance(row, dict):
        value = row.get(key, default)
        return default if value is None else value

    return default


def normalize_upscaled_face_kind(face_kind):
    value = str(
        face_kind or "single"
    ).strip().lower()

    if value not in {
        "single",
        "front",
        "back",
    }:
        value = "single"

    return value


def get_upscaled_image_absolute_path(output_image_path):
    clean_path = str(
        output_image_path or ""
    ).strip()

    if not clean_path:
        return ""

    root_path = os.path.realpath(
        UPSCALED_SCRYFALL_DIR
    )

    if os.path.isabs(clean_path):
        candidate_path = os.path.realpath(
            clean_path
        )
    else:
        candidate_path = os.path.realpath(
            os.path.join(
                UPSCALED_SCRYFALL_DIR,
                clean_path,
            )
        )

    if (
        candidate_path != root_path
        and not candidate_path.startswith(
            root_path + os.sep
        )
    ):
        return ""

    return candidate_path

def hydrate_upscaled_image_paths(
    upscaled_row,
):
    if not upscaled_row:
        return None

    result = dict(
        upscaled_row
    )

    absolute_path = (
        get_upscaled_image_absolute_path(
            result.get(
                "output_image_path",
                "",
            )
        )
    )

    if (
        not absolute_path
        or not os.path.exists(
            absolute_path
        )
    ):
        return None

    fullbleed_absolute_path = (
        get_upscaled_image_absolute_path(
            result.get(
                "fullbleed_image_path",
                "",
            )
        )
    )

    if (
        not fullbleed_absolute_path
        or not os.path.exists(
            fullbleed_absolute_path
        )
    ):
        fullbleed_absolute_path = ""

    result[
        "absolute_path"
    ] = absolute_path

    result[
        "fullbleed_absolute_path"
    ] = fullbleed_absolute_path

    return result



def get_current_upscaled_image_for_card(
    card_row,
    face_kind="single",
):
    if not card_row:
        return None

    clean_face_kind = normalize_upscaled_face_kind(
        face_kind
    )

    card_uuid = str(
        _row_value(
            card_row,
            "card_uuid",
            "",
        )
        or ""
    ).strip()

    scryfall_id = str(
        _row_value(
            card_row,
            "scryfall_id",
            "",
        )
        or ""
    ).strip()

    set_code = str(
        _row_value(
            card_row,
            "set_code",
            "",
        )
        or ""
    ).strip()

    collector_number = str(
        _row_value(
            card_row,
            "collector_number",
            "",
        )
        or ""
    ).strip()

    identity_conditions = []
    identity_parameters = []

    if card_uuid:
        identity_conditions.append(
            "card_uuid = ?"
        )
        identity_parameters.append(
            card_uuid
        )

    if scryfall_id:
        identity_conditions.append(
            "scryfall_id = ?"
        )
        identity_parameters.append(
            scryfall_id
        )

    if set_code and collector_number:
        identity_conditions.append(
            """
            (
                UPPER(COALESCE(set_code, '')) = UPPER(?)
                AND LOWER(COALESCE(collector_number, '')) = LOWER(?)
            )
            """
        )
        identity_parameters.extend(
            [
                set_code,
                collector_number,
            ]
        )

    if not identity_conditions:
        return None

    conn = get_db_connection()
    cursor = conn.cursor()

    where_identity_sql = " OR ".join(
        identity_conditions
    )

    cursor.execute(
        f"""
        SELECT *
        FROM upscaled_images
        WHERE is_current = 1
          AND quality_status = ?
          AND face_kind IN (?, 'single')
          AND (
              {where_identity_sql}
          )
        ORDER BY
            CASE
                WHEN face_kind = ? THEN 0
                ELSE 1
            END,
            upscaled_image_id DESC
        LIMIT 1
        """,
        (
            UPSCALE_QUALITY_ACCEPTED,
            clean_face_kind,
            *identity_parameters,
            clean_face_kind,
        ),
    )

    row = cursor.fetchone()
    conn.close()

    if not row:
        return None

    return hydrate_upscaled_image_paths(
        row
    )

def _build_upscaled_identity_where(
    card_row,
):
    card_uuid = str(
        _row_value(
            card_row,
            "card_uuid",
            "",
        )
        or ""
    ).strip()

    if card_uuid:
        return (
            "card_uuid = ?",
            [card_uuid],
        )

    scryfall_id = str(
        _row_value(
            card_row,
            "scryfall_id",
            "",
        )
        or ""
    ).strip()

    if scryfall_id:
        return (
            "scryfall_id = ?",
            [scryfall_id],
        )

    set_code = str(
        _row_value(
            card_row,
            "set_code",
            "",
        )
        or ""
    ).strip()

    collector_number = str(
        _row_value(
            card_row,
            "collector_number",
            "",
        )
        or ""
    ).strip()

    if set_code and collector_number:
        return (
            """
            UPPER(COALESCE(set_code, ''))
                = UPPER(?)
            AND
            LOWER(COALESCE(
                collector_number,
                ''
            )) = LOWER(?)
            """,
            [
                set_code,
                collector_number,
            ],
        )

    return "", []


def get_upscaled_image_by_id(
    upscaled_image_id,
):
    try:
        parsed_id = int(
            upscaled_image_id
        )
    except (TypeError, ValueError):
        return None

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM upscaled_images
        WHERE upscaled_image_id = ?
        LIMIT 1
        """,
        (parsed_id,),
    )

    row = cursor.fetchone()
    conn.close()

    if not row:
        return None

    return hydrate_upscaled_image_paths(
        row
    )

def calculate_upscaled_file_sha256(
    file_path,
):
    digest = hashlib.sha256()

    with open(
        file_path,
        "rb",
    ) as source_file:
        while True:
            chunk = source_file.read(
                1024 * 1024
            )

            if not chunk:
                break

            digest.update(
                chunk
            )

    return digest.hexdigest()


def set_upscaled_generated_bleed(
    upscaled_image_id,
    *,
    fullbleed_image_path,
    bleed_size_mm,
    bleed_model_id,
    bleed_plugin_id,
    bleed_plugin_version=None,
    bleed_generation_metadata=None,
):
    upscaled_image = (
        get_upscaled_image_by_id(
            upscaled_image_id
        )
    )

    if not upscaled_image:
        raise ValueError(
            "Upscaled image was not found."
        )

    absolute_fullbleed_path = (
        get_upscaled_image_absolute_path(
            fullbleed_image_path
        )
    )

    if not absolute_fullbleed_path:
        raise ValueError(
            "Generated bleed image must be "
            "inside UPSCALED_SCRYFALL_DIR."
        )

    if not os.path.isfile(
        absolute_fullbleed_path
    ):
        raise ValueError(
            "Generated bleed image file "
            "does not exist."
        )

    try:
        clean_bleed_size_mm = float(
            bleed_size_mm
        )

    except (
        TypeError,
        ValueError,
    ) as exc:
        raise ValueError(
            "Generated bleed size must "
            "be a number."
        ) from exc

    if clean_bleed_size_mm <= 0:
        raise ValueError(
            "Generated bleed size must "
            "be greater than zero."
        )

    clean_bleed_model_id = str(
        bleed_model_id
        or ""
    ).strip()

    if not clean_bleed_model_id:
        raise ValueError(
            "Generated bleed model ID "
            "is required."
        )

    clean_bleed_plugin_id = str(
        bleed_plugin_id
        or ""
    ).strip()

    if not clean_bleed_plugin_id:
        raise ValueError(
            "Generated bleed plugin ID "
            "is required."
        )

    relative_fullbleed_path = (
        os.path.relpath(
            absolute_fullbleed_path,
            UPSCALED_SCRYFALL_DIR,
        )
    )

    fullbleed_sha256 = (
        calculate_upscaled_file_sha256(
            absolute_fullbleed_path
        )
    )

    generation_metadata_json = (
        json.dumps(
            bleed_generation_metadata
            or {},
            ensure_ascii=False,
            sort_keys=True,
        )
    )

    old_fullbleed_absolute_path = (
        upscaled_image.get(
            "fullbleed_absolute_path"
        )
        or ""
    )

    now_utc = upscaling_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE upscaled_images
        SET
            has_generated_bleed = 1,
            bleed_size_mm = ?,
            fullbleed_image_path = ?,
            fullbleed_sha256 = ?,
            bleed_model_id = ?,
            bleed_plugin_id = ?,
            bleed_plugin_version = ?,
            bleed_generation_metadata = ?,
            updated_at_utc = ?
        WHERE upscaled_image_id = ?
        """,
        (
            clean_bleed_size_mm,
            relative_fullbleed_path,
            fullbleed_sha256,
            clean_bleed_model_id,
            clean_bleed_plugin_id,
            str(
                bleed_plugin_version
                or ""
            ).strip(),
            generation_metadata_json,
            now_utc,
            int(
                upscaled_image_id
            ),
        ),
    )

    if cursor.rowcount != 1:
        conn.rollback()
        conn.close()

        raise ValueError(
            "Generated bleed metadata "
            "could not be saved."
        )

    conn.commit()
    conn.close()

    if (
        old_fullbleed_absolute_path
        and os.path.realpath(
            old_fullbleed_absolute_path
        )
        != os.path.realpath(
            absolute_fullbleed_path
        )
        and os.path.exists(
            old_fullbleed_absolute_path
        )
    ):
        try:
            os.remove(
                old_fullbleed_absolute_path
            )

        except OSError:
            pass

    return get_upscaled_image_by_id(
        upscaled_image_id
    )




def register_upscaled_candidate(
    *,
    card_uuid=None,
    scryfall_id=None,
    set_code=None,
    collector_number=None,
    face_kind="single",
    source_image_path=None,
    output_image_path,
    output_width=None,
    output_height=None,
    plugin_id,
    plugin_version=None,
    pipeline_version=None,
    plugin_result=None,
):
    clean_face_kind = (
        normalize_upscaled_face_kind(
            face_kind
        )
    )

    absolute_output_path = (
        get_upscaled_image_absolute_path(
            output_image_path
        )
    )

    if not absolute_output_path:
        raise ValueError(
            "Upscaled candidate must be "
            "inside UPSCALED_SCRYFALL_DIR."
        )

    relative_output_path = os.path.relpath(
        absolute_output_path,
        UPSCALED_SCRYFALL_DIR,
    )

    now_utc = upscaling_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO upscaled_images (
            card_uuid,
            scryfall_id,
            set_code,
            collector_number,
            face_kind,
            source_image_path,
            output_image_path,
            output_width,
            output_height,
            plugin_id,
            plugin_version,
            pipeline_version,
            plugin_result_json,
            quality_status,
            is_current,
            created_at_utc
        )
        VALUES (
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, 0, ?
        )
        """,
        (
            card_uuid,
            scryfall_id,
            set_code,
            collector_number,
            clean_face_kind,
            source_image_path,
            relative_output_path,
            output_width,
            output_height,
            plugin_id,
            plugin_version,
            pipeline_version,
            json.dumps(
                plugin_result
                or {},
                ensure_ascii=False,
            ),
            UPSCALE_QUALITY_PENDING,
            now_utc,
        ),
    )

    candidate_id = (
        cursor.lastrowid
    )

    conn.commit()
    conn.close()

    return candidate_id

def delete_upscaled_image_records(
    upscaled_image_ids,
):
    clean_ids = []

    for raw_id in (
        upscaled_image_ids
        or []
    ):
        try:
            clean_id = int(
                raw_id
            )

        except (
            TypeError,
            ValueError,
        ):
            continue

        if (
            clean_id > 0
            and clean_id
            not in clean_ids
        ):
            clean_ids.append(
                clean_id
            )

    if not clean_ids:
        return {
            "deleted_count": 0,
            "deleted_files": 0,
        }

    placeholders = ",".join(
        "?"
        for _ in clean_ids
    )

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        f"""
        SELECT
            upscaled_image_id,
            output_image_path,
            fullbleed_image_path
        FROM upscaled_images
        WHERE upscaled_image_id
            IN ({placeholders})
        """,
        tuple(
            clean_ids
        ),
    )

    rows = cursor.fetchall()

    if not rows:
        conn.close()

        return {
            "deleted_count": 0,
            "deleted_files": 0,
        }

    found_ids = [
        int(
            row[
                "upscaled_image_id"
            ]
        )
        for row
        in rows
    ]

    absolute_paths = []

    for row in rows:
        for path_column in (
            "output_image_path",
            "fullbleed_image_path",
        ):
            absolute_path = (
                get_upscaled_image_absolute_path(
                    row[
                        path_column
                    ]
                )
            )

            if (
                absolute_path
                and absolute_path
                not in absolute_paths
            ):
                absolute_paths.append(
                    absolute_path
                )

    found_placeholders = ",".join(
        "?"
        for _ in found_ids
    )

    try:
        cursor.execute(
            f"""
            UPDATE upscaling_job_items
            SET upscaled_image_id = NULL
            WHERE upscaled_image_id
                IN ({found_placeholders})
            """,
            tuple(
                found_ids
            ),
        )

        cursor.execute(
            f"""
            DELETE FROM upscaled_images
            WHERE upscaled_image_id
                IN ({found_placeholders})
            """,
            tuple(
                found_ids
            ),
        )

        deleted_count = (
            cursor.rowcount
        )

        conn.commit()

    except Exception:
        conn.rollback()
        conn.close()
        raise

    conn.close()

    deleted_files = 0
    parent_directories = set()

    for absolute_path in absolute_paths:
        parent_directories.add(
            os.path.dirname(
                absolute_path
            )
        )

        if not os.path.exists(
            absolute_path
        ):
            continue

        try:
            os.remove(
                absolute_path
            )

            deleted_files += 1

        except OSError:
            pass

    for parent_directory in sorted(
        parent_directories,
        key=len,
        reverse=True,
    ):
        if not parent_directory:
            continue

        try:
            os.rmdir(
                parent_directory
            )

        except OSError:
            pass

    return {
        "deleted_count": int(
            deleted_count
        ),

        "deleted_files": int(
            deleted_files
        ),
    }

UPSCALE_MAINTENANCE_STALE_PENDING_HOURS = 24


def _parse_upscaling_utc_value(
    value,
):
    clean_value = str(
        value
        or ""
    ).strip()

    if not clean_value:
        return None

    try:
        return datetime.strptime(
            clean_value,
            "%Y-%m-%d %H:%M:%S UTC",
        ).replace(
            tzinfo=timezone.utc
        )

    except ValueError:
        return None


def _safe_file_size(
    absolute_path,
):
    try:
        return int(
            os.path.getsize(
                absolute_path
            )
        )

    except OSError:
        return 0


def _scan_upscaled_image_maintenance(
    stale_pending_hours=(
        UPSCALE_MAINTENANCE_STALE_PENDING_HOURS
    ),
):
    ensure_upscaling_schema()

    now_utc = datetime.now(
        timezone.utc
    )

    stale_cutoff = (
        now_utc
        - timedelta(
            hours=max(
                1,
                int(
                    stale_pending_hours
                    or 24
                ),
            )
        )
    )

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            upscaled_image_id,
            output_image_path,
            fullbleed_image_path,
            has_generated_bleed,
            quality_status,
            is_current,
            created_at_utc
        FROM upscaled_images
        ORDER BY upscaled_image_id ASC
        """
    )

    rows = cursor.fetchall()
    conn.close()

    referenced_paths = set()
    protected_paths = set()

    removable_rows = []
    protected_rows = []

    broken_record_ids = []
    stale_pending_ids = []
    broken_fullbleed_ids = []

    active_records = 0
    pending_records = 0

    for row in rows:
        upscaled_image_id = int(
            row[
                "upscaled_image_id"
            ]
        )

        quality_status = str(
            row[
                "quality_status"
            ]
            or ""
        ).strip().lower()

        is_current = bool(
            row[
                "is_current"
            ]
        )

        if (
            is_current
            and quality_status
            == UPSCALE_QUALITY_ACCEPTED
        ):
            active_records += 1

        if (
            quality_status
            == UPSCALE_QUALITY_PENDING
        ):
            pending_records += 1

        output_path = (
            get_upscaled_image_absolute_path(
                row[
                    "output_image_path"
                ]
            )
        )

        fullbleed_path = (
            get_upscaled_image_absolute_path(
                row[
                    "fullbleed_image_path"
                ]
            )
        )

        row_paths = []

        for absolute_path in (
            output_path,
            fullbleed_path,
        ):
            if not absolute_path:
                continue

            referenced_paths.add(
                absolute_path
            )

            row_paths.append(
                absolute_path
            )

        primary_missing = bool(
            not output_path
            or not os.path.isfile(
                output_path
            )
        )

        created_at = (
            _parse_upscaling_utc_value(
                row[
                    "created_at_utc"
                ]
            )
        )

        stale_pending = bool(
            quality_status
            == UPSCALE_QUALITY_PENDING
            and not is_current
            and created_at is not None
            and created_at
            <= stale_cutoff
        )

        removable_reason = ""

        if primary_missing:
            removable_reason = (
                "missing_output"
            )

            broken_record_ids.append(
                upscaled_image_id
            )

        elif quality_status in {
            UPSCALE_QUALITY_REJECTED,
            UPSCALE_QUALITY_SUPERSEDED,
        }:
            removable_reason = (
                quality_status
            )

        elif (
            quality_status
            == UPSCALE_QUALITY_ACCEPTED
            and not is_current
        ):
            removable_reason = (
                "inactive_accepted"
            )

        elif stale_pending:
            removable_reason = (
                "stale_pending"
            )

            stale_pending_ids.append(
                upscaled_image_id
            )

        if (
            bool(
                row[
                    "has_generated_bleed"
                ]
            )
            and (
                not fullbleed_path
                or not os.path.isfile(
                    fullbleed_path
                )
            )
        ):
            broken_fullbleed_ids.append(
                upscaled_image_id
            )

        row_info = {
            "upscaled_image_id": (
                upscaled_image_id
            ),

            "reason": (
                removable_reason
            ),

            "paths": (
                row_paths
            ),
        }

        if removable_reason:
            removable_rows.append(
                row_info
            )

        else:
            protected_rows.append(
                row_info
            )

            for absolute_path in row_paths:
                protected_paths.add(
                    absolute_path
                )

    removable_record_ids = []
    removable_record_paths = set()

    for row_info in removable_rows:
        shared_with_protected_record = any(
            absolute_path
            in protected_paths
            for absolute_path
            in row_info[
                "paths"
            ]
        )

        if shared_with_protected_record:
            continue

        removable_record_ids.append(
            row_info[
                "upscaled_image_id"
            ]
        )

        for absolute_path in row_info[
            "paths"
        ]:
            if os.path.isfile(
                absolute_path
            ):
                removable_record_paths.add(
                    absolute_path
                )

    orphan_file_paths = set()

    os.makedirs(
        UPSCALED_SCRYFALL_DIR,
        exist_ok=True,
    )

    root_path = os.path.realpath(
        UPSCALED_SCRYFALL_DIR
    )

    for (
        directory_path,
        _directory_names,
        filenames,
    ) in os.walk(
        UPSCALED_SCRYFALL_DIR
    ):
        for filename in filenames:
            absolute_path = os.path.realpath(
                os.path.join(
                    directory_path,
                    filename,
                )
            )

            if (
                absolute_path
                != root_path
                and not absolute_path.startswith(
                    root_path
                    + os.sep
                )
            ):
                continue

            if (
                absolute_path
                not in referenced_paths
            ):
                orphan_file_paths.add(
                    absolute_path
                )

    recoverable_paths = (
        removable_record_paths
        | orphan_file_paths
    )

    recoverable_bytes = sum(
        _safe_file_size(
            absolute_path
        )
        for absolute_path
        in recoverable_paths
    )

    return {
        "total_records": len(
            rows
        ),

        "active_records": (
            active_records
        ),

        "pending_records": (
            pending_records
        ),

        "unused_records": len(
            removable_record_ids
        ),

        "broken_records": len(
            broken_record_ids
        ),

        "stale_pending_records": len(
            stale_pending_ids
        ),

        "broken_fullbleed_records": len(
            broken_fullbleed_ids
        ),

        "orphan_files": len(
            orphan_file_paths
        ),

        "unused_files": len(
            recoverable_paths
        ),

        "recoverable_bytes": int(
            recoverable_bytes
        ),

        "removable_record_ids": (
            removable_record_ids
        ),

        "orphan_file_paths": sorted(
            orphan_file_paths
        ),

        "broken_fullbleed_ids": (
            broken_fullbleed_ids
        ),

        "scanned_at_utc": (
            upscaling_utc_now()
        ),
    }


def _build_upscaled_maintenance_summary(
    scan_result,
):
    return {
        "total_records": int(
            scan_result.get(
                "total_records",
                0,
            )
            or 0
        ),

        "active_records": int(
            scan_result.get(
                "active_records",
                0,
            )
            or 0
        ),

        "pending_records": int(
            scan_result.get(
                "pending_records",
                0,
            )
            or 0
        ),

        "unused_records": int(
            scan_result.get(
                "unused_records",
                0,
            )
            or 0
        ),

        "broken_records": int(
            scan_result.get(
                "broken_records",
                0,
            )
            or 0
        ),

        "stale_pending_records": int(
            scan_result.get(
                "stale_pending_records",
                0,
            )
            or 0
        ),

        "broken_fullbleed_records": int(
            scan_result.get(
                "broken_fullbleed_records",
                0,
            )
            or 0
        ),

        "orphan_files": int(
            scan_result.get(
                "orphan_files",
                0,
            )
            or 0
        ),

        "unused_files": int(
            scan_result.get(
                "unused_files",
                0,
            )
            or 0
        ),

        "recoverable_bytes": int(
            scan_result.get(
                "recoverable_bytes",
                0,
            )
            or 0
        ),

        "scanned_at_utc": (
            scan_result.get(
                "scanned_at_utc"
            )
            or ""
        ),
    }


def analyze_upscaled_image_maintenance():
    scan_result = (
        _scan_upscaled_image_maintenance()
    )

    return (
        _build_upscaled_maintenance_summary(
            scan_result
        )
    )


def cleanup_upscaled_image_maintenance():
    scan_result = (
        _scan_upscaled_image_maintenance()
    )

    before_summary = (
        _build_upscaled_maintenance_summary(
            scan_result
        )
    )

    removable_record_ids = list(
        scan_result.get(
            "removable_record_ids",
            [],
        )
        or []
    )

    broken_fullbleed_ids = [
        int(
            upscaled_image_id
        )
        for upscaled_image_id
        in (
            scan_result.get(
                "broken_fullbleed_ids",
                [],
            )
            or []
        )
        if int(
            upscaled_image_id
        )
        not in set(
            removable_record_ids
        )
    ]

    repaired_fullbleed_records = 0

    if broken_fullbleed_ids:
        placeholders = ",".join(
            "?"
            for _ in broken_fullbleed_ids
        )

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            f"""
            UPDATE upscaled_images
            SET
                has_generated_bleed = 0,
                bleed_size_mm = NULL,
                fullbleed_image_path = NULL,
                fullbleed_sha256 = NULL,
                bleed_model_id = NULL,
                bleed_plugin_id = NULL,
                bleed_plugin_version = NULL,
                bleed_generation_metadata = NULL,
                updated_at_utc = ?
            WHERE upscaled_image_id
                IN ({placeholders})
            """,
            (
                upscaling_utc_now(),
                *broken_fullbleed_ids,
            ),
        )

        repaired_fullbleed_records = (
            cursor.rowcount
        )

        conn.commit()
        conn.close()

    deleted_records = 0
    deleted_record_files = 0

    if removable_record_ids:
        delete_result = (
            delete_upscaled_image_records(
                removable_record_ids
            )
        )

        deleted_records = int(
            delete_result.get(
                "deleted_count",
                0,
            )
            or 0
        )

        deleted_record_files = int(
            delete_result.get(
                "deleted_files",
                0,
            )
            or 0
        )

    deleted_orphan_files = 0

    for absolute_path in (
        scan_result.get(
            "orphan_file_paths",
            [],
        )
        or []
    ):
        if not os.path.isfile(
            absolute_path
        ):
            continue

        try:
            os.remove(
                absolute_path
            )

            deleted_orphan_files += 1

        except OSError:
            pass

    removed_directories = 0

    for (
        directory_path,
        _directory_names,
        _filenames,
    ) in os.walk(
        UPSCALED_SCRYFALL_DIR,
        topdown=False,
    ):
        if (
            os.path.realpath(
                directory_path
            )
            == os.path.realpath(
                UPSCALED_SCRYFALL_DIR
            )
        ):
            continue

        try:
            os.rmdir(
                directory_path
            )

            removed_directories += 1

        except OSError:
            pass

    after_summary = (
        analyze_upscaled_image_maintenance()
    )

    return {
        "deleted_records": (
            deleted_records
        ),

        "deleted_files": (
            deleted_record_files
            + deleted_orphan_files
        ),

        "deleted_orphan_files": (
            deleted_orphan_files
        ),

        "repaired_fullbleed_records": (
            repaired_fullbleed_records
        ),

        "removed_directories": (
            removed_directories
        ),

        "recovered_bytes": (
            before_summary[
                "recoverable_bytes"
            ]
        ),

        "before": (
            before_summary
        ),

        "after": (
            after_summary
        ),
    }

def accept_upscaled_candidate(
    upscaled_image_id,
):
    candidate = get_upscaled_image_by_id(
        upscaled_image_id
    )

    if not candidate:
        raise ValueError(
            "Upscaled candidate was "
            "not found."
        )

    identity_where, identity_params = (
        _build_upscaled_identity_where(
            candidate
        )
    )

    if not identity_where:
        raise ValueError(
            "Upscaled candidate has no "
            "usable card identity."
        )

    now_utc = upscaling_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    obsolete_ids = []

    try:
        cursor.execute(
            f"""
            UPDATE upscaled_images
            SET
                is_current = 0,
                quality_status = ?,
                updated_at_utc = ?
            WHERE upscaled_image_id <> ?
              AND face_kind IN (?, 'single')
              AND is_current = 1
              AND quality_status = ?
              AND ({identity_where})
            """,
            (
                UPSCALE_QUALITY_SUPERSEDED,
                now_utc,
                int(
                    upscaled_image_id
                ),
                candidate[
                    "face_kind"
                ],
                UPSCALE_QUALITY_ACCEPTED,
                *identity_params,
            ),
        )

        cursor.execute(
            """
            UPDATE upscaled_images
            SET
                quality_status = ?,
                is_current = 1,
                updated_at_utc = ?
            WHERE upscaled_image_id = ?
            """,
            (
                UPSCALE_QUALITY_ACCEPTED,
                now_utc,
                int(
                    upscaled_image_id
                ),
            ),
        )

        if cursor.rowcount != 1:
            raise RuntimeError(
                "Upscale candidate "
                "could not be accepted."
            )

        cursor.execute(
            f"""
            SELECT upscaled_image_id
            FROM upscaled_images
            WHERE upscaled_image_id <> ?
              AND face_kind IN (?, 'single')
              AND is_current = 0
              AND ({identity_where})
            """,
            (
                int(
                    upscaled_image_id
                ),
                candidate[
                    "face_kind"
                ],
                *identity_params,
            ),
        )

        obsolete_ids = [
            int(
                row[
                    "upscaled_image_id"
                ]
            )
            for row
            in cursor.fetchall()
        ]

        conn.commit()

    except Exception:
        conn.rollback()
        conn.close()
        raise

    conn.close()

    if obsolete_ids:
        delete_upscaled_image_records(
            obsolete_ids
        )

    return get_upscaled_image_by_id(
        upscaled_image_id
    )


def accept_upscaled_candidates(
    upscaled_image_ids,
):
    clean_ids = []

    for raw_id in (
        upscaled_image_ids
        or []
    ):
        try:
            clean_id = int(
                raw_id
            )
        except (
            TypeError,
            ValueError,
        ):
            continue

        if (
            clean_id > 0
            and clean_id
            not in clean_ids
        ):
            clean_ids.append(
                clean_id
            )

    if not clean_ids:
        raise ValueError(
            "No Upscale candidates "
            "were supplied."
        )

    placeholders = ",".join(
        "?"
        for _ in clean_ids
    )

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            f"""
            SELECT *
            FROM upscaled_images
            WHERE upscaled_image_id
                IN ({placeholders})
            """,
            tuple(
                clean_ids
            ),
        )

        candidate_rows = (
            cursor.fetchall()
        )

        candidates_by_id = {
            int(
                row[
                    "upscaled_image_id"
                ]
            ): row
            for row
            in candidate_rows
        }

        missing_ids = [
            candidate_id
            for candidate_id
            in clean_ids
            if candidate_id
            not in candidates_by_id
        ]

        if missing_ids:
            raise ValueError(
                "One or more Upscale "
                "candidates were not found."
            )

        batch_identity = None

        for candidate_id in clean_ids:
            candidate = (
                candidates_by_id[
                    candidate_id
                ]
            )

            identity_token = (
                (
                    "card_uuid",
                    candidate[
                        "card_uuid"
                    ],
                )
                if candidate[
                    "card_uuid"
                ]
                else (
                    "scryfall_id",
                    candidate[
                        "scryfall_id"
                    ],
                )
            )

            if (
                not identity_token[1]
            ):
                raise ValueError(
                    "Upscale candidate has "
                    "no usable card identity."
                )

            if batch_identity is None:
                batch_identity = (
                    identity_token
                )

            elif (
                identity_token
                != batch_identity
            ):
                raise ValueError(
                    "Batch candidates do "
                    "not belong to the "
                    "same card."
                )

        now_utc = (
            upscaling_utc_now()
        )

        obsolete_ids = set()

        for candidate_id in clean_ids:
            candidate = (
                candidates_by_id[
                    candidate_id
                ]
            )

            (
                identity_where,
                identity_params,
            ) = (
                _build_upscaled_identity_where(
                    candidate
                )
            )

            if not identity_where:
                raise ValueError(
                    "Upscale candidate has "
                    "no usable card identity."
                )

            cursor.execute(
                f"""
                UPDATE upscaled_images
                SET
                    is_current = 0,
                    quality_status = ?,
                    updated_at_utc = ?
                WHERE upscaled_image_id <> ?
                  AND face_kind = ?
                  AND is_current = 1
                  AND quality_status = ?
                  AND ({identity_where})
                """,
                (
                    UPSCALE_QUALITY_SUPERSEDED,
                    now_utc,
                    candidate_id,
                    candidate[
                        "face_kind"
                    ],
                    UPSCALE_QUALITY_ACCEPTED,
                    *identity_params,
                ),
            )

            cursor.execute(
                """
                UPDATE upscaled_images
                SET
                    quality_status = ?,
                    is_current = 1,
                    updated_at_utc = ?
                WHERE upscaled_image_id = ?
                """,
                (
                    UPSCALE_QUALITY_ACCEPTED,
                    now_utc,
                    candidate_id,
                ),
            )

            if cursor.rowcount != 1:
                raise RuntimeError(
                    "Upscale candidate "
                    "could not be accepted."
                )

            cursor.execute(
                f"""
                SELECT upscaled_image_id
                FROM upscaled_images
                WHERE upscaled_image_id <> ?
                  AND face_kind IN (?, 'single')
                  AND is_current = 0
                  AND ({identity_where})
                """,
                (
                    candidate_id,
                    candidate[
                        "face_kind"
                    ],
                    *identity_params,
                ),
            )

            for row in cursor.fetchall():
                obsolete_ids.add(
                    int(
                        row[
                            "upscaled_image_id"
                        ]
                    )
                )

        conn.commit()

    except Exception:
        conn.rollback()
        conn.close()
        raise

    conn.close()

    protected_ids = set(
        clean_ids
    )

    obsolete_ids = {
        obsolete_id
        for obsolete_id
        in obsolete_ids
        if obsolete_id
        not in protected_ids
    }

    if obsolete_ids:
        delete_upscaled_image_records(
            sorted(
                obsolete_ids
            )
        )

    return [
        get_upscaled_image_by_id(
            candidate_id
        )
        for candidate_id
        in clean_ids
    ]


def discard_upscaled_candidate(
    upscaled_image_id,
):
    try:
        clean_id = int(
            upscaled_image_id
        )

    except (
        TypeError,
        ValueError,
    ):
        return False

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            upscaled_image_id,
            is_current
        FROM upscaled_images
        WHERE upscaled_image_id = ?
        LIMIT 1
        """,
        (
            clean_id,
        ),
    )

    row = cursor.fetchone()
    conn.close()

    if not row:
        return False

    if bool(
        row[
            "is_current"
        ]
    ):
        return False

    result = (
        delete_upscaled_image_records(
            [
                clean_id,
            ]
        )
    )

    return (
        result[
            "deleted_count"
        ] > 0
    )


def revert_current_upscaled_image_for_card(
    card_row,
    face_kind="single",
):
    clean_face_kind = (
        normalize_upscaled_face_kind(
            face_kind
        )
    )

    identity_where, identity_params = (
        _build_upscaled_identity_where(
            card_row
        )
    )

    if not identity_where:
        return 0

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        f"""
        SELECT
            upscaled_image_id,
            is_current,
            quality_status
        FROM upscaled_images
        WHERE face_kind IN (?, 'single')
          AND ({identity_where})
        ORDER BY upscaled_image_id ASC
        """,
        (
            clean_face_kind,
            *identity_params,
        ),
    )

    rows = cursor.fetchall()
    conn.close()

    reverted_count = sum(
        1
        for row
        in rows
        if (
            bool(
                row[
                    "is_current"
                ]
            )
            and row[
                "quality_status"
            ]
            == UPSCALE_QUALITY_ACCEPTED
        )
    )

    upscaled_image_ids = [
        int(
            row[
                "upscaled_image_id"
            ]
        )
        for row
        in rows
    ]

    if upscaled_image_ids:
        delete_upscaled_image_records(
            upscaled_image_ids
        )

    return reverted_count

def delete_upscaled_images_for_card(
    card_row,
):
    identity_where, identity_params = (
        _build_upscaled_identity_where(
            card_row
        )
    )

    if not identity_where:
        return {
            "deleted_count": 0,
            "deleted_files": 0,
        }

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        f"""
        SELECT upscaled_image_id
        FROM upscaled_images
        WHERE ({identity_where})
        ORDER BY upscaled_image_id ASC
        """,
        tuple(
            identity_params
        ),
    )

    upscaled_image_ids = [
        int(
            row[
                "upscaled_image_id"
            ]
        )
        for row
        in cursor.fetchall()
    ]

    conn.close()

    return delete_upscaled_image_records(
        upscaled_image_ids
    )

def register_upscaled_image(
    *,
    card_uuid=None,
    scryfall_id=None,
    set_code=None,
    collector_number=None,
    face_kind="single",
    source_image_path=None,
    source_sha256=None,
    output_image_path,
    output_sha256=None,
    output_width=None,
    output_height=None,
    plugin_id,
    plugin_version=None,
    pipeline_version=None,
    quality_status=UPSCALE_QUALITY_ACCEPTED,
    quality_score=None,
    text_validation_score=None,
):
    clean_face_kind = (
        normalize_upscaled_face_kind(
            face_kind
        )
    )

    absolute_output_path = (
        get_upscaled_image_absolute_path(
            output_image_path
        )
    )

    if not absolute_output_path:
        raise ValueError(
            "Upscaled image output must be inside "
            "UPSCALED_SCRYFALL_DIR."
        )

    relative_output_path = os.path.relpath(
        absolute_output_path,
        UPSCALED_SCRYFALL_DIR,
    )

    now_utc = upscaling_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    if card_uuid:
        cursor.execute(
            """
            UPDATE upscaled_images
            SET is_current = 0,
                quality_status = CASE
                    WHEN quality_status = 'accepted'
                    THEN 'superseded'
                    ELSE quality_status
                END,
                updated_at_utc = ?
            WHERE card_uuid = ?
              AND face_kind = ?
              AND is_current = 1
            """,
            (
                now_utc,
                card_uuid,
                clean_face_kind,
            ),
        )

    elif scryfall_id:
        cursor.execute(
            """
            UPDATE upscaled_images
            SET is_current = 0,
                quality_status = CASE
                    WHEN quality_status = 'accepted'
                    THEN 'superseded'
                    ELSE quality_status
                END,
                updated_at_utc = ?
            WHERE scryfall_id = ?
              AND face_kind = ?
              AND is_current = 1
            """,
            (
                now_utc,
                scryfall_id,
                clean_face_kind,
            ),
        )

    cursor.execute(
        """
        INSERT INTO upscaled_images (
            card_uuid,
            scryfall_id,
            set_code,
            collector_number,
            face_kind,
            source_image_path,
            source_sha256,
            output_image_path,
            output_sha256,
            output_width,
            output_height,
            plugin_id,
            plugin_version,
            pipeline_version,
            quality_status,
            quality_score,
            text_validation_score,
            is_current,
            created_at_utc
        )
        VALUES (
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, 1, ?
        )
        """,
        (
            card_uuid,
            scryfall_id,
            set_code,
            collector_number,
            clean_face_kind,
            source_image_path,
            source_sha256,
            relative_output_path,
            output_sha256,
            output_width,
            output_height,
            plugin_id,
            plugin_version,
            pipeline_version,
            quality_status,
            quality_score,
            text_validation_score,
            now_utc,
        ),
    )

    upscaled_image_id = (
        cursor.lastrowid
    )

    conn.commit()
    conn.close()

    return upscaled_image_id