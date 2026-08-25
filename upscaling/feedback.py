import hashlib
import json
import os
import threading
import uuid
from datetime import datetime, timezone

from PIL import (
    Image,
    ImageChops,
    ImageFilter,
    ImageOps,
    ImageStat,
)

from db.database import get_db_connection

from paths import (
    RUNTIME_BASE_DIR,
    UPSCALING_DEV_FEEDBACK_DIR,
    UPSCALING_DEV_FEEDBACK_LOG_PATH,
)

from settings import APP_VERSION


FEEDBACK_SCHEMA_VERSION = 1


QUALITY_RATING_LABELS = {
    -2: "Much Worse",
    -1: "Worse",
    0: "Same",
    1: "Improved",
    2: "Perfect",
}


SOURCE_CONDITION_LABELS = {
    0: "Low Quality",
    1: "Ok Quality",
    2: "Good Quality",
}


FEEDBACK_REGION_KEYS = (
    "card_title",
    "mana_cost",
    "artwork",
    "rules_text",
    "power_toughness",
    "frame",
    "bottom_text",
    "card_overall",
)


_feedback_file_lock = threading.Lock()


def feedback_utc_now():
    return datetime.now(
        timezone.utc
    ).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def _row_value(
    row,
    key,
    default=None,
):
    if row is None:
        return default

    try:
        if (
            hasattr(row, "keys")
            and key in row.keys()
        ):
            value = row[key]

            return (
                default
                if value is None
                else value
            )

    except Exception:
        pass

    if isinstance(row, dict):
        value = row.get(
            key,
            default,
        )

        return (
            default
            if value is None
            else value
        )

    return default


def _safe_json_array(
    raw_value,
):
    if isinstance(
        raw_value,
        list,
    ):
        return raw_value

    try:
        parsed_value = json.loads(
            str(
                raw_value
                or "[]"
            )
        )

    except Exception:
        return []

    if isinstance(
        parsed_value,
        list,
    ):
        return parsed_value

    return []


def _runtime_relative_path(
    path_value,
):
    clean_path = str(
        path_value
        or ""
    ).strip()

    if not clean_path:
        return ""

    absolute_path = os.path.abspath(
        clean_path
    )

    runtime_root = os.path.abspath(
        RUNTIME_BASE_DIR
    )

    try:
        common_path = os.path.commonpath(
            [
                runtime_root,
                absolute_path,
            ]
        )

    except ValueError:
        return absolute_path

    if common_path == runtime_root:
        return os.path.relpath(
            absolute_path,
            runtime_root,
        ).replace(
            "\\",
            "/",
        )

    return absolute_path


def _sha256_file(
    path_value,
):
    clean_path = str(
        path_value
        or ""
    ).strip()

    if (
        not clean_path
        or not os.path.exists(
            clean_path
        )
    ):
        return ""

    digest = hashlib.sha256()

    with open(
        clean_path,
        "rb",
    ) as file_handle:
        while True:
            chunk = file_handle.read(
                1024 * 1024
            )

            if not chunk:
                break

            digest.update(
                chunk
            )

    return digest.hexdigest()


def _analyze_image(
    path_value,
):
    clean_path = str(
        path_value
        or ""
    ).strip()

    result = {
        "path": (
            _runtime_relative_path(
                clean_path
            )
        ),
        "exists": False,
        "sha256": "",
        "file_size_bytes": 0,
        "format": "",
        "mode": "",
        "width": None,
        "height": None,
        "aspect_ratio": None,

        "luminance_mean": None,
        "contrast_stddev": None,
        "entropy": None,
        "edge_strength_mean": None,

        "black_clip_pct": None,
        "white_clip_pct": None,
    }

    if (
        not clean_path
        or not os.path.exists(
            clean_path
        )
    ):
        return result

    result["exists"] = True

    result[
        "file_size_bytes"
    ] = int(
        os.path.getsize(
            clean_path
        )
    )

    result["sha256"] = (
        _sha256_file(
            clean_path
        )
    )

    with Image.open(
        clean_path
    ) as image_file:
        result["format"] = str(
            image_file.format
            or ""
        )

        result["mode"] = str(
            image_file.mode
            or ""
        )

        image = image_file.convert(
            "RGB"
        )

        width = int(
            image.width
        )

        height = int(
            image.height
        )

        result["width"] = width
        result["height"] = height

        if height:
            result[
                "aspect_ratio"
            ] = round(
                width / height,
                6,
            )

        gray = ImageOps.grayscale(
            image
        )

        gray_stat = ImageStat.Stat(
            gray
        )

        result[
            "luminance_mean"
        ] = round(
            float(
                gray_stat.mean[0]
            ),
            4,
        )

        result[
            "contrast_stddev"
        ] = round(
            float(
                gray_stat.stddev[0]
            ),
            4,
        )

        result["entropy"] = round(
            float(
                gray.entropy()
            ),
            4,
        )

        edges = gray.filter(
            ImageFilter.FIND_EDGES
        )

        if (
            width > 4
            and height > 4
        ):
            edges = edges.crop(
                (
                    2,
                    2,
                    width - 2,
                    height - 2,
                )
            )

        edge_stat = ImageStat.Stat(
            edges
        )

        result[
            "edge_strength_mean"
        ] = round(
            float(
                edge_stat.mean[0]
            ),
            4,
        )

        histogram = (
            gray.histogram()
        )

        pixel_count = max(
            1,
            width * height,
        )

        result[
            "black_clip_pct"
        ] = round(
            (
                sum(
                    histogram[0:5]
                )
                / pixel_count
            )
            * 100.0,
            5,
        )

        result[
            "white_clip_pct"
        ] = round(
            (
                sum(
                    histogram[251:256]
                )
                / pixel_count
            )
            * 100.0,
            5,
        )

    return result


def _compare_images(
    source_path,
    candidate_path,
    source_metrics,
    candidate_metrics,
):
    result = {
        "scale_x": None,
        "scale_y": None,

        "mean_absolute_difference": None,
        "rms_difference": None,
        "pixel_similarity_pct": None,

        "edge_strength_delta": None,
        "edge_strength_ratio": None,
        "contrast_delta": None,
        "entropy_delta": None,
    }

    source_width = (
        source_metrics.get(
            "width"
        )
    )

    source_height = (
        source_metrics.get(
            "height"
        )
    )

    candidate_width = (
        candidate_metrics.get(
            "width"
        )
    )

    candidate_height = (
        candidate_metrics.get(
            "height"
        )
    )

    if (
        source_width
        and candidate_width
    ):
        result[
            "scale_x"
        ] = round(
            candidate_width
            / source_width,
            6,
        )

    if (
        source_height
        and candidate_height
    ):
        result[
            "scale_y"
        ] = round(
            candidate_height
            / source_height,
            6,
        )

    source_edge = (
        source_metrics.get(
            "edge_strength_mean"
        )
    )

    candidate_edge = (
        candidate_metrics.get(
            "edge_strength_mean"
        )
    )

    if (
        source_edge is not None
        and candidate_edge is not None
    ):
        result[
            "edge_strength_delta"
        ] = round(
            candidate_edge
            - source_edge,
            4,
        )

        if source_edge:
            result[
                "edge_strength_ratio"
            ] = round(
                candidate_edge
                / source_edge,
                6,
            )

    source_contrast = (
        source_metrics.get(
            "contrast_stddev"
        )
    )

    candidate_contrast = (
        candidate_metrics.get(
            "contrast_stddev"
        )
    )

    if (
        source_contrast is not None
        and candidate_contrast is not None
    ):
        result[
            "contrast_delta"
        ] = round(
            candidate_contrast
            - source_contrast,
            4,
        )

    source_entropy = (
        source_metrics.get(
            "entropy"
        )
    )

    candidate_entropy = (
        candidate_metrics.get(
            "entropy"
        )
    )

    if (
        source_entropy is not None
        and candidate_entropy is not None
    ):
        result[
            "entropy_delta"
        ] = round(
            candidate_entropy
            - source_entropy,
            4,
        )

    if (
        not source_metrics.get(
            "exists"
        )
        or not candidate_metrics.get(
            "exists"
        )
    ):
        return result

    try:
        with Image.open(
            source_path
        ) as source_file:
            source_image = (
                source_file.convert(
                    "RGB"
                )
            )

        with Image.open(
            candidate_path
        ) as candidate_file:
            candidate_image = (
                candidate_file.convert(
                    "RGB"
                )
            )

        candidate_image = (
            candidate_image.resize(
                source_image.size,
                Image.Resampling.LANCZOS,
            )
        )

        difference = (
            ImageChops.difference(
                source_image,
                candidate_image,
            )
        )

        difference_stat = (
            ImageStat.Stat(
                difference
            )
        )

        mean_difference = (
            sum(
                difference_stat.mean
            )
            / len(
                difference_stat.mean
            )
        )

        rms_difference = (
            sum(
                difference_stat.rms
            )
            / len(
                difference_stat.rms
            )
        )

        result[
            "mean_absolute_difference"
        ] = round(
            float(
                mean_difference
            ),
            4,
        )

        result[
            "rms_difference"
        ] = round(
            float(
                rms_difference
            ),
            4,
        )

        result[
            "pixel_similarity_pct"
        ] = round(
            max(
                0.0,
                100.0
                - (
                    mean_difference
                    / 255.0
                    * 100.0
                ),
            ),
            4,
        )

    except Exception as exc:
        result[
            "comparison_error"
        ] = (
            f"{type(exc).__name__}: "
            f"{exc}"
        )

    return result


def _normalize_feedback_payload(
    feedback_payload,
):
    if feedback_payload is None:
        return {
            "source_condition": None,
            "ratings": None,
            "notes": "",
        }

    if not isinstance(
        feedback_payload,
        dict,
    ):
        feedback_payload = {}

    try:
        source_condition = int(
            feedback_payload.get(
                "source_condition",
                1,
            )
        )

    except (
        TypeError,
        ValueError,
    ):
        source_condition = 1

    if (
        source_condition
        not in SOURCE_CONDITION_LABELS
    ):
        source_condition = 1

    raw_ratings = (
        feedback_payload.get(
            "ratings"
        )
    )

    if not isinstance(
        raw_ratings,
        dict,
    ):
        raw_ratings = {}

    ratings = {}

    for region_key in (
        FEEDBACK_REGION_KEYS
    ):
        try:
            rating_value = int(
                raw_ratings.get(
                    region_key,
                    0,
                )
            )

        except (
            TypeError,
            ValueError,
        ):
            rating_value = 0

        if (
            rating_value
            not in QUALITY_RATING_LABELS
        ):
            rating_value = 0

        ratings[
            region_key
        ] = {
            "label": (
                QUALITY_RATING_LABELS[
                    rating_value
                ]
            ),
            "value": (
                rating_value
            ),
        }

    notes = str(
        feedback_payload.get(
            "notes",
            "",
        )
        or ""
    ).strip()

    notes = notes[:8000]

    return {
        "source_condition": {
            "label": (
                SOURCE_CONDITION_LABELS[
                    source_condition
                ]
            ),
            "value": (
                source_condition
            ),
        },
        "ratings": ratings,
        "notes": notes,
    }


def _get_release_metadata(
    set_code,
):
    clean_set_code = str(
        set_code
        or ""
    ).strip().upper()

    empty_result = {
        "set_name": "",
        "set_type": "",
        "release_date": "",
        "release_year": None,
    }

    if not clean_set_code:
        return empty_result

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT
                set_name,
                set_type,
                release_date
            FROM sets
            WHERE UPPER(set_code) = ?
            LIMIT 1
            """,
            (
                clean_set_code,
            ),
        )

        row = cursor.fetchone()

    finally:
        conn.close()

    if not row:
        return empty_result

    release_date = str(
        row["release_date"]
        or ""
    ).strip()

    try:
        release_year = int(
            release_date[:4]
        )

    except (
        TypeError,
        ValueError,
    ):
        release_year = None

    return {
        "set_name": str(
            row["set_name"]
            or ""
        ),
        "set_type": str(
            row["set_type"]
            or ""
        ),
        "release_date": (
            release_date
        ),
        "release_year": (
            release_year
        ),
    }


def _build_card_metadata(
    card_row,
    candidate_row,
):
    set_code = str(
        _row_value(
            card_row,
            "set_code",
            _row_value(
                candidate_row,
                "set_code",
                "",
            ),
        )
        or ""
    ).strip().upper()

    release = (
        _get_release_metadata(
            set_code
        )
    )

    return {
        "card_uuid": str(
            _row_value(
                card_row,
                "card_uuid",
                _row_value(
                    candidate_row,
                    "card_uuid",
                    "",
                ),
            )
            or ""
        ),

        "scryfall_id": str(
            _row_value(
                card_row,
                "scryfall_id",
                _row_value(
                    candidate_row,
                    "scryfall_id",
                    "",
                ),
            )
            or ""
        ),

        "scryfall_illustration_id": str(
            _row_value(
                card_row,
                "scryfall_illustration_id",
                "",
            )
            or ""
        ),

        "card_name": str(
            _row_value(
                card_row,
                "card_name",
                "",
            )
            or ""
        ),

        "face_name": str(
            _row_value(
                card_row,
                "face_name",
                "",
            )
            or ""
        ),

        "face_kind": str(
            _row_value(
                candidate_row,
                "face_kind",
                "single",
            )
            or "single"
        ),

        "set_code": set_code,
        "set_name": release[
            "set_name"
        ],
        "set_type": release[
            "set_type"
        ],

        "collector_number": str(
            _row_value(
                card_row,
                "collector_number",
                _row_value(
                    candidate_row,
                    "collector_number",
                    "",
                ),
            )
            or ""
        ),

        "release_date": release[
            "release_date"
        ],

        "release_year": release[
            "release_year"
        ],

        "rarity": str(
            _row_value(
                card_row,
                "rarity",
                "",
            )
            or ""
        ),

        "type_line": str(
            _row_value(
                card_row,
                "type_line",
                "",
            )
            or ""
        ),

        "layout": str(
            _row_value(
                card_row,
                "layout",
                "",
            )
            or ""
        ),

        "frame_version": str(
            _row_value(
                card_row,
                "frame_version",
                "",
            )
            or ""
        ),

        "border_color": str(
            _row_value(
                card_row,
                "border_color",
                "",
            )
            or ""
        ),

        "mana_cost": str(
            _row_value(
                card_row,
                "mana_cost",
                "",
            )
            or ""
        ),

        "mana_value": (
            _row_value(
                card_row,
                "mana_value",
                None,
            )
        ),

        "is_dual_faced": bool(
            int(
                _row_value(
                    card_row,
                    "is_dual_faced",
                    0,
                )
                or 0
            )
        ),

        "face_count": int(
            _row_value(
                card_row,
                "face_count",
                0,
            )
            or 0
        ),

        "front_face_name": str(
            _row_value(
                card_row,
                "front_face_name",
                "",
            )
            or ""
        ),

        "back_face_name": str(
            _row_value(
                card_row,
                "back_face_name",
                "",
            )
            or ""
        ),

        "colors": _safe_json_array(
            _row_value(
                card_row,
                "colors_json",
                "[]",
            )
        ),

        "color_identity": _safe_json_array(
            _row_value(
                card_row,
                "color_identity_json",
                "[]",
            )
        ),
    }


def _parse_plugin_result(
    candidate_row,
):
    raw_json = str(
        _row_value(
            candidate_row,
            "plugin_result_json",
            "",
        )
        or ""
    ).strip()

    if not raw_json:
        return {}

    try:
        result = json.loads(
            raw_json
        )

    except Exception:
        return {}

    if not isinstance(
        result,
        dict,
    ):
        return {}

    result = dict(
        result
    )

    # Do not export full machine paths from
    # the plugin result into a shared dataset.
    result.pop(
        "input_path",
        None,
    )

    result.pop(
        "output_path",
        None,
    )

    return result


def get_upscaling_dev_feedback_log_status():
    exists = os.path.exists(
        UPSCALING_DEV_FEEDBACK_LOG_PATH
    )

    record_count = 0
    file_size_bytes = 0

    if exists:
        file_size_bytes = int(
            os.path.getsize(
                UPSCALING_DEV_FEEDBACK_LOG_PATH
            )
        )

        try:
            with open(
                UPSCALING_DEV_FEEDBACK_LOG_PATH,
                "r",
                encoding="utf-8",
            ) as feedback_file:
                record_count = sum(
                    1
                    for line in feedback_file
                    if line.strip()
                )

        except OSError:
            record_count = 0

    return {
        "exists": exists,
        "record_count": (
            record_count
        ),
        "file_size_bytes": (
            file_size_bytes
        ),
        "directory": (
            _runtime_relative_path(
                UPSCALING_DEV_FEEDBACK_DIR
            )
        ),
        "path": (
            _runtime_relative_path(
                UPSCALING_DEV_FEEDBACK_LOG_PATH
            )
        ),
        "absolute_path": (
            UPSCALING_DEV_FEEDBACK_LOG_PATH
        ),
    }


def append_upscaling_dev_feedback(
    *,
    decision,
    card_row,
    candidate_row,
    feedback_payload=None,
):
    if not candidate_row:
        raise ValueError(
            "Upscaling feedback requires "
            "a candidate image record."
        )

    source_path = str(
        _row_value(
            candidate_row,
            "source_image_path",
            "",
        )
        or ""
    ).strip()

    candidate_path = str(
        _row_value(
            candidate_row,
            "absolute_path",
            "",
        )
        or ""
    ).strip()

    source_metrics = (
        _analyze_image(
            source_path
        )
    )

    candidate_metrics = (
        _analyze_image(
            candidate_path
        )
    )

    normalized_feedback = (
        _normalize_feedback_payload(
            feedback_payload
        )
    )

    plugin_result = (
        _parse_plugin_result(
            candidate_row
        )
    )

    comparison_metrics = (
        _compare_images(
            source_path,
            candidate_path,
            source_metrics,
            candidate_metrics,
        )
    )

    record = {
        "feedback_schema_version": (
            FEEDBACK_SCHEMA_VERSION
        ),

        "record_id": str(
            uuid.uuid4()
        ),

        "recorded_at_utc": (
            feedback_utc_now()
        ),

        "decision": str(
            decision
            or ""
        ).strip().lower(),

        "app_version": (
            APP_VERSION
        ),

        "card": (
            _build_card_metadata(
                card_row,
                candidate_row,
            )
        ),

        "source": {
            **source_metrics,

            "condition": (
                normalized_feedback[
                    "source_condition"
                ]
            ),
        },

        "candidate": {
            **candidate_metrics,

            "upscaled_image_id": int(
                _row_value(
                    candidate_row,
                    "upscaled_image_id",
                    0,
                )
                or 0
            ),

            "quality_status_before_decision": str(
                _row_value(
                    candidate_row,
                    "quality_status",
                    "",
                )
                or ""
            ),
        },

        "pipeline": {
            "plugin_id": str(
                _row_value(
                    candidate_row,
                    "plugin_id",
                    "",
                )
                or ""
            ),

            "plugin_version": str(
                _row_value(
                    candidate_row,
                    "plugin_version",
                    "",
                )
                or ""
            ),

            "pipeline_version": str(
                _row_value(
                    candidate_row,
                    "pipeline_version",
                    "",
                )
                or ""
            ),

            "model_id": str(
                plugin_result.get(
                    "model_id"
                )
                or _row_value(
                    candidate_row,
                    "pipeline_version",
                    "",
                )
                or ""
            ),

            "model_label": str(
                plugin_result.get(
                    "model_label"
                )
                or plugin_result.get(
                    "model_id"
                )
                or ""
            ),

            "processor": str(
                plugin_result.get(
                    "processor"
                )
                or ""
            ),

            "processing_ms": (
                plugin_result.get(
                    "processing_ms"
                )
            ),

            "peak_gpu_memory_mb": (
                plugin_result.get(
                    "peak_gpu_memory_mb"
                )
            ),

            "device": str(
                plugin_result.get(
                    "device"
                )
                or ""
            ),

            "plugin_result": (
                plugin_result
            ),
        },

        "ratings": (
            normalized_feedback[
                "ratings"
            ]
        ),

        "notes": (
            normalized_feedback[
                "notes"
            ]
        ),

        "technical_metrics": {
            "source": (
                source_metrics
            ),

            "candidate": (
                candidate_metrics
            ),

            "comparison": (
                comparison_metrics
            ),

            "plugin": (
                plugin_result.get(
                    "technical_metrics"
                )
                or {}
            ),
        },
    }

    os.makedirs(
        UPSCALING_DEV_FEEDBACK_DIR,
        exist_ok=True,
    )

    serialized_record = json.dumps(
        record,
        ensure_ascii=False,
        separators=(
            ",",
            ":",
        ),
    )

    with _feedback_file_lock:
        with open(
            UPSCALING_DEV_FEEDBACK_LOG_PATH,
            "a",
            encoding="utf-8",
        ) as feedback_file:
            feedback_file.write(
                serialized_record
                + "\n"
            )

    return record