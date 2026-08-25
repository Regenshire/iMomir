import json
import os
from datetime import datetime, timezone

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

    result = dict(row)

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

    result["absolute_path"] = (
        absolute_path
    )

    return result

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

    result = dict(row)

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

    result["absolute_path"] = (
        absolute_path
    )

    return result


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
            int(upscaled_image_id),
            candidate["face_kind"],
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
            int(upscaled_image_id),
        ),
    )

    conn.commit()
    conn.close()

    return get_upscaled_image_by_id(
        upscaled_image_id
    )


def discard_upscaled_candidate(
    upscaled_image_id,
):
    now_utc = upscaling_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE upscaled_images
        SET
            quality_status = ?,
            is_current = 0,
            updated_at_utc = ?
        WHERE upscaled_image_id = ?
          AND is_current = 0
        """,
        (
            UPSCALE_QUALITY_REJECTED,
            now_utc,
            int(upscaled_image_id),
        ),
    )

    changed_count = cursor.rowcount

    conn.commit()
    conn.close()

    return changed_count > 0


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

    now_utc = upscaling_utc_now()

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        f"""
        UPDATE upscaled_images
        SET
            quality_status = ?,
            is_current = 0,
            updated_at_utc = ?
        WHERE face_kind IN (?, 'single')
          AND is_current = 1
          AND quality_status = ?
          AND ({identity_where})
        """,
        (
            UPSCALE_QUALITY_SUPERSEDED,
            now_utc,
            clean_face_kind,
            UPSCALE_QUALITY_ACCEPTED,
            *identity_params,
        ),
    )

    changed_count = cursor.rowcount

    conn.commit()
    conn.close()

    return changed_count



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