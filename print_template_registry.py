import configparser
import os
import re
import threading
from dataclasses import dataclass

from paths import (
    get_print_template_dirs,
    get_runtime_print_template_dir,
)


PRINT_TEMPLATE_EXTENSION = ".ini"
PRINT_TEMPLATE_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class PrintTemplate:
    template_id: str
    display_name: str
    description: str
    schema_version: int
    source_path: str

    enabled: bool
    featured_template: bool
    momir_support: bool
    cardprint_support: bool

    page_width_mm: float
    page_height_mm: float

    finished_card_width_mm: float
    finished_card_height_mm: float
    slot_width_mm: float
    slot_height_mm: float

    horizontal_size_adjustment_percent: float
    vertical_size_adjustment_percent: float

    layout_type: str
    columns: int
    rows: int
    slot_defs: tuple

    metadata: dict
    rendering: dict
    silhouette: dict
    duplex: dict

    @property
    def cards_per_page(self):
        return len(self.slot_defs)

    @property
    def is_multi_card_layout(self):
        return self.cards_per_page > 1

    @property
    def is_silhouette_layout(self):
        return bool(
            self.metadata.get(
                "silhouette_support",
                False,
            )
        )

    def supports_scope(self, scope):
        normalized_scope = str(
            scope or ""
        ).strip().lower()

        if normalized_scope == "momir":
            return self.momir_support

        if normalized_scope in {
            "chaos",
            "cardprint",
            "card_print",
        }:
            return self.cardprint_support

        return False

class PrintTemplateRegistry:
    def __init__(self, template_dirs=None):
        self._template_dirs = tuple(
            template_dirs
            or get_print_template_dirs()
        )
        self._templates = {}
        self._errors = []
        self._source_signature = ()
        self._lock = threading.RLock()

        self.reload()

    def _build_source_signature(self):
        signature = []
        seen_directories = set()

        for template_dir in self._template_dirs:
            if not template_dir:
                continue

            normalized_dir = os.path.normcase(
                os.path.abspath(template_dir)
            )

            if normalized_dir in seen_directories:
                continue

            seen_directories.add(normalized_dir)

            if not os.path.isdir(template_dir):
                signature.append(
                    (
                        normalized_dir,
                        "missing",
                    )
                )
                continue

            for filename in sorted(
                os.listdir(template_dir)
            ):
                if not filename.lower().endswith(
                    PRINT_TEMPLATE_EXTENSION
                ):
                    continue

                template_path = os.path.join(
                    template_dir,
                    filename,
                )

                if not os.path.isfile(
                    template_path
                ):
                    continue

                try:
                    file_stat = os.stat(
                        template_path
                    )

                except OSError:
                    continue

                signature.append(
                    (
                        os.path.normcase(
                            os.path.abspath(
                                template_path
                            )
                        ),
                        file_stat.st_mtime_ns,
                        file_stat.st_size,
                    )
                )

        return tuple(signature)

    def refresh_if_changed(self):
        current_signature = (
            self._build_source_signature()
        )

        with self._lock:
            if (
                current_signature
                == self._source_signature
            ):
                return False

        self.reload()
        return True

    def reload(self):
        templates = {}
        template_sources = {}
        errors = []

        runtime_template_dir = (
            get_runtime_print_template_dir()
        )

        os.makedirs(
            runtime_template_dir,
            exist_ok=True,
        )

        with self._lock:
            for template_dir in self._template_dirs:
                if (
                    not template_dir
                    or not os.path.isdir(template_dir)
                ):
                    continue

                normalized_template_dir = (
                    os.path.normcase(
                        os.path.abspath(
                            template_dir
                        )
                    )
                )

                for filename in sorted(
                    os.listdir(template_dir)
                ):
                    if not filename.lower().endswith(
                        PRINT_TEMPLATE_EXTENSION
                    ):
                        continue

                    template_path = os.path.join(
                        template_dir,
                        filename,
                    )

                    if not os.path.isfile(
                        template_path
                    ):
                        continue

                    try:
                        template = (
                            self._load_template_file(
                                template_path
                            )
                        )
                    except (
                        OSError,
                        ValueError,
                        configparser.Error,
                    ) as exc:
                        errors.append({
                            "path": template_path,
                            "error": str(exc),
                        })
                        continue

                    if not template.enabled:
                        continue

                    existing_source_dir = (
                        template_sources.get(
                            template.template_id
                        )
                    )

                    if (
                        existing_source_dir
                        == normalized_template_dir
                    ):
                        errors.append({
                            "path": template_path,
                            "error": (
                                "Duplicate Template_ID "
                                "in the same template "
                                "directory: "
                                f"{template.template_id}"
                            ),
                        })
                        continue

                    # A runtime template may intentionally
                    # override a bundled template while
                    # retaining the same stable Template_ID.
                    templates[
                        template.template_id
                    ] = template

                    template_sources[
                        template.template_id
                    ] = normalized_template_dir

            self._templates = templates
            self._errors = errors
            self._source_signature = (
                self._build_source_signature()
            )

        return self

    def get(self, template_id, default=None):
        normalized_id = (
            _normalize_template_id(
                template_id
            )
        )

        with self._lock:
            return self._templates.get(
                normalized_id,
                default,
            )

    def contains(self, template_id):
        return self.get(template_id) is not None

    def list_templates(self, scope=None):
        with self._lock:
            templates = list(
                self._templates.values()
            )

        if scope:
            templates = [
                template
                for template in templates
                if template.supports_scope(
                    scope
                )
            ]

        return sorted(
            templates,
            key=lambda template: (
                not template.featured_template,
                template.display_name.lower(),
                template.template_id,
            ),
        )

    def get_options(self, scope=None):
        return [
            (
                template.template_id,
                template.display_name,
            )
            for template
            in self.list_templates(
                scope=scope
            )
        ]

    def get_errors(self):
        with self._lock:
            return [
                dict(item)
                for item in self._errors
            ]

    def _load_template_file(
        self,
        template_path,
    ):
        parser = configparser.ConfigParser(
            interpolation=None,
            inline_comment_prefixes=(
                "#",
                ";",
            ),
        )

        with open(
            template_path,
            "r",
            encoding="utf-8-sig",
        ) as template_file:
            parser.read_file(
                template_file
            )

        for section_name in (
            "Template",
            "Page",
            "Card",
            "Layout",
        ):
            if not parser.has_section(
                section_name
            ):
                raise ValueError(
                    "Missing required "
                    f"[{section_name}] section."
                )

        schema_version = _get_int(
            parser,
            "Template",
            "Schema_Version",
            minimum=1,
        )

        if (
            schema_version
            != PRINT_TEMPLATE_SCHEMA_VERSION
        ):
            raise ValueError(
                "Unsupported Schema_Version "
                f"{schema_version}; expected "
                f"{PRINT_TEMPLATE_SCHEMA_VERSION}."
            )

        template_id = (
            _normalize_template_id(
                _get_required_text(
                    parser,
                    "Template",
                    "Template_ID",
                )
            )
        )

        if not template_id:
            raise ValueError(
                "Template_ID is invalid."
            )

        display_name = (
            _get_required_text(
                parser,
                "Template",
                "Display_Name",
            )
        )

        page_width_mm = _get_float(
            parser,
            "Page",
            "Width_mm",
            minimum=0.000001,
        )

        page_height_mm = _get_float(
            parser,
            "Page",
            "Height_mm",
            minimum=0.000001,
        )

        finished_card_width_mm = (
            _get_float(
                parser,
                "Card",
                "Finished_Width_mm",
                minimum=0.000001,
            )
        )

        finished_card_height_mm = (
            _get_float(
                parser,
                "Card",
                "Finished_Height_mm",
                minimum=0.000001,
            )
        )

        bleed_mm = _get_float(
            parser,
            "Card",
            "Bleed_mm",
            fallback=0.0,
            minimum=0.0,
        )

        slot_width_mm = _get_float(
            parser,
            "Card",
            "Slot_Width_mm",
            fallback=(
                finished_card_width_mm
                + (bleed_mm * 2.0)
            ),
            minimum=0.000001,
        )

        slot_height_mm = _get_float(
            parser,
            "Card",
            "Slot_Height_mm",
            fallback=(
                finished_card_height_mm
                + (bleed_mm * 2.0)
            ),
            minimum=0.000001,
        )

        horizontal_adjustment = (
            _get_float(
                parser,
                "Card",
                "Horizontal_Size_Adjustment",
                fallback=0.0,
            )
        )

        vertical_adjustment = (
            _get_float(
                parser,
                "Card",
                "Vertical_Size_Adjustment",
                fallback=0.0,
            )
        )

        _validate_size_adjustment(
            "Horizontal_Size_Adjustment",
            horizontal_adjustment,
        )

        _validate_size_adjustment(
            "Vertical_Size_Adjustment",
            vertical_adjustment,
        )

        layout_type = (
            _get_required_text(
                parser,
                "Layout",
                "Type",
            )
            .strip()
            .lower()
        )

        if layout_type == "grid":
            (
                columns,
                rows,
                slot_defs,
            ) = self._build_grid_slots(
                parser,
                page_width_mm=page_width_mm,
                page_height_mm=page_height_mm,
                finished_card_width_mm=(
                    finished_card_width_mm
                ),
                finished_card_height_mm=(
                    finished_card_height_mm
                ),
                slot_width_mm=slot_width_mm,
                slot_height_mm=slot_height_mm,
            )

        elif layout_type == "explicit":
            (
                columns,
                rows,
                slot_defs,
            ) = self._build_explicit_slots(
                parser,
                finished_card_width_mm=(
                    finished_card_width_mm
                ),
                finished_card_height_mm=(
                    finished_card_height_mm
                ),
            )

        else:
            raise ValueError(
                "[Layout] Type must be "
                "either Grid or Explicit."
            )

        return PrintTemplate(
            template_id=template_id,
            display_name=display_name,
            description=_get_text(
                parser,
                "Template",
                "Description",
                fallback="",
            ),
            schema_version=schema_version,
            source_path=os.path.abspath(
                template_path
            ),
            enabled=_get_bool(
                parser,
                "Template",
                "Enabled",
                fallback=True,
            ),
            featured_template=_get_bool(
                parser,
                "Template",
                "FeaturedTemplate",
                fallback=False,
            ),
            momir_support=_get_bool(
                parser,
                "Template",
                "Momir_Support",
                fallback=False,
            ),
            cardprint_support=_get_bool(
                parser,
                "Template",
                "CardPrint_Support",
                fallback=False,
            ),
            page_width_mm=page_width_mm,
            page_height_mm=page_height_mm,
            finished_card_width_mm=(
                finished_card_width_mm
            ),
            finished_card_height_mm=(
                finished_card_height_mm
            ),
            slot_width_mm=slot_width_mm,
            slot_height_mm=slot_height_mm,
            horizontal_size_adjustment_percent=(
                horizontal_adjustment
            ),
            vertical_size_adjustment_percent=(
                vertical_adjustment
            ),
            layout_type=layout_type,
            columns=columns,
            rows=rows,
            slot_defs=tuple(slot_defs),
            metadata=_read_metadata(
                parser
            ),
            rendering=_read_rendering_settings(
                parser
            ),
            silhouette=_read_silhouette_settings(
                parser,
                template_path,
            ),
            duplex=_read_duplex_settings(
                parser
            ),
        )

    def _build_grid_slots(
        self,
        parser,
        page_width_mm,
        page_height_mm,
        finished_card_width_mm,
        finished_card_height_mm,
        slot_width_mm,
        slot_height_mm,
    ):
        columns = _get_int(
            parser,
            "Layout",
            "Columns",
            minimum=1,
        )

        rows = _get_int(
            parser,
            "Layout",
            "Rows",
            minimum=1,
        )

        horizontal_spacing_mm = (
            _get_float(
                parser,
                "Layout",
                "Horizontal_Spacing_mm",
                fallback=max(
                    0.0,
                    (
                        slot_width_mm
                        - finished_card_width_mm
                    ),
                ),
                minimum=0.0,
            )
        )

        vertical_spacing_mm = (
            _get_float(
                parser,
                "Layout",
                "Vertical_Spacing_mm",
                fallback=max(
                    0.0,
                    (
                        slot_height_mm
                        - finished_card_height_mm
                    ),
                ),
                minimum=0.0,
            )
        )

        horizontal_step_mm = (
            _get_optional_float(
                parser,
                "Layout",
                "Horizontal_Step_mm",
            )
        )

        vertical_step_mm = (
            _get_optional_float(
                parser,
                "Layout",
                "Vertical_Step_mm",
            )
        )

        if horizontal_step_mm is None:
            horizontal_step_mm = (
                finished_card_width_mm
                + horizontal_spacing_mm
            )

        if vertical_step_mm is None:
            vertical_step_mm = (
                finished_card_height_mm
                + vertical_spacing_mm
            )

        if (
            horizontal_step_mm <= 0
            or vertical_step_mm <= 0
        ):
            raise ValueError(
                "Grid step values must be "
                "greater than zero."
            )

        group_width_mm = (
            slot_width_mm
            + (
                (columns - 1)
                * horizontal_step_mm
            )
        )

        group_height_mm = (
            slot_height_mm
            + (
                (rows - 1)
                * vertical_step_mm
            )
        )

        start_x_mm = (
            _get_optional_float(
                parser,
                "Layout",
                "Start_X_mm",
            )
        )

        start_y_mm = (
            _get_optional_float(
                parser,
                "Layout",
                "Start_Y_mm",
            )
        )

        if start_x_mm is None:
            start_x_mm = (
                _resolve_alignment_start(
                    axis="horizontal",
                    alignment=_get_text(
                        parser,
                        "Layout",
                        "Horizontal_Alignment",
                        fallback="Center",
                    ),
                    page_size_mm=page_width_mm,
                    group_size_mm=group_width_mm,
                )
            )

        if start_y_mm is None:
            start_y_mm = (
                _resolve_alignment_start(
                    axis="vertical",
                    alignment=_get_text(
                        parser,
                        "Layout",
                        "Vertical_Alignment",
                        fallback="Center",
                    ),
                    page_size_mm=page_height_mm,
                    group_size_mm=group_height_mm,
                )
            )

        rotation_degrees = (
            _normalize_rotation(
                _get_int(
                    parser,
                    "Card",
                    "Rotation_Degrees",
                    fallback=0,
                )
            )
        )

        fill_order = _get_text(
            parser,
            "Layout",
            "Fill_Order",
            fallback="Top_Left_Row_Major",
        ).strip().lower()

        if (
            fill_order
            != "top_left_row_major"
        ):
            raise ValueError(
                "Schema version 1 supports "
                "Fill_Order="
                "Top_Left_Row_Major."
            )

        slot_defs = []

        for row_index in range(rows):
            display_row_index = (
                (rows - 1)
                - row_index
            )

            for column_index in range(
                columns
            ):
                slot_defs.append({
                    "x_mm": (
                        start_x_mm
                        + (
                            column_index
                            * horizontal_step_mm
                        )
                    ),
                    "y_mm": (
                        start_y_mm
                        + (
                            display_row_index
                            * vertical_step_mm
                        )
                    ),
                    "width_mm": (
                        slot_width_mm
                    ),
                    "height_mm": (
                        slot_height_mm
                    ),
                    "finished_card_width_mm": (
                        finished_card_width_mm
                    ),
                    "finished_card_height_mm": (
                        finished_card_height_mm
                    ),
                    "rotation_degrees": (
                        rotation_degrees
                    ),
                })

        return (
            columns,
            rows,
            slot_defs,
        )

    def _build_explicit_slots(
        self,
        parser,
        finished_card_width_mm,
        finished_card_height_mm,
    ):
        slot_sections = [
            section_name
            for section_name
            in parser.sections()
            if re.fullmatch(
                r"Slot_\d+",
                section_name,
                flags=re.IGNORECASE,
            )
        ]

        slot_sections.sort(
            key=lambda section_name: int(
                section_name.split(
                    "_",
                    1,
                )[1]
            )
        )

        if not slot_sections:
            raise ValueError(
                "Explicit layouts require "
                "at least one [Slot_#] section."
            )

        slot_defs = []

        for section_name in slot_sections:
            slot_defs.append({
                "x_mm": _get_float(
                    parser,
                    section_name,
                    "X_mm",
                ),
                "y_mm": _get_float(
                    parser,
                    section_name,
                    "Y_mm",
                ),
                "width_mm": _get_float(
                    parser,
                    section_name,
                    "Width_mm",
                    minimum=0.000001,
                ),
                "height_mm": _get_float(
                    parser,
                    section_name,
                    "Height_mm",
                    minimum=0.000001,
                ),
                "finished_card_width_mm": (
                    _get_float(
                        parser,
                        section_name,
                        "Finished_Width_mm",
                        fallback=(
                            finished_card_width_mm
                        ),
                        minimum=0.000001,
                    )
                ),
                "finished_card_height_mm": (
                    _get_float(
                        parser,
                        section_name,
                        "Finished_Height_mm",
                        fallback=(
                            finished_card_height_mm
                        ),
                        minimum=0.000001,
                    )
                ),
                "rotation_degrees": (
                    _normalize_rotation(
                        _get_int(
                            parser,
                            section_name,
                            "Rotation_Degrees",
                            fallback=0,
                        )
                    )
                ),
            })

        columns = _get_int(
            parser,
            "Layout",
            "Columns",
            fallback=len(slot_defs),
            minimum=1,
        )

        rows = _get_int(
            parser,
            "Layout",
            "Rows",
            fallback=1,
            minimum=1,
        )

        return (
            columns,
            rows,
            slot_defs,
        )

_registry = None
_registry_lock = threading.Lock()


def get_print_template_registry(
    reload=False,
):
    global _registry

    with _registry_lock:
        if _registry is None:
            _registry = (
                PrintTemplateRegistry()
            )

        elif reload:
            _registry.reload()

        else:
            _registry.refresh_if_changed()

        return _registry


def _normalize_template_id(value):
    normalized = str(
        value or ""
    ).strip().lower()

    if not re.fullmatch(
        r"[a-z0-9][a-z0-9._-]*",
        normalized,
    ):
        return ""

    return normalized


def _get_required_text(
    parser,
    section,
    option,
):
    value = _get_text(
        parser,
        section,
        option,
        fallback="",
    )

    if not value:
        raise ValueError(
            f"[{section}] {option} "
            "is required."
        )

    return value


def _get_text(
    parser,
    section,
    option,
    fallback="",
):
    if not parser.has_option(
        section,
        option,
    ):
        return str(
            fallback or ""
        ).strip()

    return str(
        parser.get(
            section,
            option,
        )
        or ""
    ).strip()


def _get_bool(
    parser,
    section,
    option,
    fallback=False,
):
    if not parser.has_option(
        section,
        option,
    ):
        return bool(fallback)

    try:
        return parser.getboolean(
            section,
            option,
        )

    except ValueError as exc:
        raise ValueError(
            f"[{section}] {option} "
            "must be True or False."
        ) from exc


def _get_float(
    parser,
    section,
    option,
    fallback=None,
    minimum=None,
):
    raw_value = _get_text(
        parser,
        section,
        option,
        fallback="",
    )

    if not raw_value:
        if fallback is None:
            raise ValueError(
                f"[{section}] {option} "
                "is required."
            )

        value = float(fallback)

    else:
        try:
            value = float(raw_value)

        except ValueError as exc:
            raise ValueError(
                f"[{section}] {option} "
                "must be a number."
            ) from exc

    if (
        minimum is not None
        and value < minimum
    ):
        raise ValueError(
            f"[{section}] {option} "
            f"must be at least {minimum}."
        )

    return value


def _get_optional_float(
    parser,
    section,
    option,
):
    raw_value = _get_text(
        parser,
        section,
        option,
        fallback="",
    )

    if not raw_value:
        return None

    try:
        return float(raw_value)

    except ValueError as exc:
        raise ValueError(
            f"[{section}] {option} "
            "must be a number when supplied."
        ) from exc


def _get_int(
    parser,
    section,
    option,
    fallback=None,
    minimum=None,
):
    raw_value = _get_text(
        parser,
        section,
        option,
        fallback="",
    )

    if not raw_value:
        if fallback is None:
            raise ValueError(
                f"[{section}] {option} "
                "is required."
            )

        value = int(fallback)

    else:
        try:
            value = int(raw_value)

        except ValueError as exc:
            raise ValueError(
                f"[{section}] {option} "
                "must be an integer."
            ) from exc

    if (
        minimum is not None
        and value < minimum
    ):
        raise ValueError(
            f"[{section}] {option} "
            f"must be at least {minimum}."
        )

    return value


def _validate_size_adjustment(
    option_name,
    adjustment_percent,
):
    scale = (
        1.0
        + (
            float(adjustment_percent)
            / 100.0
        )
    )

    if scale <= 0:
        raise ValueError(
            f"{option_name} would produce "
            "a zero or negative dimension."
        )


def _resolve_alignment_start(
    axis,
    alignment,
    page_size_mm,
    group_size_mm,
):
    normalized_alignment = str(
        alignment or "center"
    ).strip().lower()

    if normalized_alignment == "center":
        return (
            page_size_mm
            - group_size_mm
        ) / 2.0

    if axis == "horizontal":
        if normalized_alignment == "left":
            return 0.0

        if normalized_alignment == "right":
            return (
                page_size_mm
                - group_size_mm
            )

    else:
        if normalized_alignment == "bottom":
            return 0.0

        if normalized_alignment == "top":
            return (
                page_size_mm
                - group_size_mm
            )

    raise ValueError(
        f"Unsupported {axis} alignment: "
        f"{alignment}"
    )


def _normalize_rotation(value):
    rotation = int(
        value or 0
    ) % 360

    if rotation not in {
        0,
        90,
        180,
        270,
    }:
        raise ValueError(
            "Rotation_Degrees must be "
            "0, 90, 180, or 270."
        )

    return rotation


def _read_metadata(parser):
    if not parser.has_section(
        "Metadata"
    ):
        return {
            "silhouette_support": False,
        }

    metadata = {
        str(key).strip().lower(): (
            str(value or "").strip()
        )
        for key, value
        in parser.items("Metadata")
    }

    metadata[
        "silhouette_support"
    ] = _get_bool(
        parser,
        "Metadata",
        "Silhouette_Support",
        fallback=False,
    )

    return metadata


def _read_rendering_settings(
    parser,
):
    if not parser.has_section(
        "Rendering"
    ):
        return {
            "add_edge_bleed_border": False,
            "fill_unused_slots_with_white": False,
            "corner_radius_mm": 0.0,
            "uses_fixed_inner_margin": False,
        }

    return {
        "add_edge_bleed_border": (
            _get_bool(
                parser,
                "Rendering",
                "Add_Edge_Bleed_Border",
                fallback=False,
            )
        ),
        "fill_unused_slots_with_white": (
            _get_bool(
                parser,
                "Rendering",
                "Fill_Unused_Slots_With_White",
                fallback=False,
            )
        ),
        "corner_radius_mm": (
            _get_float(
                parser,
                "Rendering",
                "Corner_Radius_mm",
                fallback=0.0,
                minimum=0.0,
            )
        ),
        "uses_fixed_inner_margin": (
            _get_bool(
                parser,
                "Rendering",
                "Uses_Fixed_Inner_Margin",
                fallback=False,
            )
        ),
    }


def _read_silhouette_settings(
    parser,
    template_path,
):
    if not parser.has_section(
        "Silhouette"
    ):
        return {}

    background_filename = (
        _get_text(
            parser,
            "Silhouette",
            "Registration_Background",
            fallback="",
        )
    )

    background_path = ""

    if background_filename:
        background_path = os.path.abspath(
            os.path.join(
                os.path.dirname(
                    template_path
                ),
                background_filename,
            )
        )

    return {
        "registration_background": (
            background_filename
        ),
        "registration_background_path": (
            background_path
        ),
    }


def _read_duplex_settings(parser):
    if not parser.has_section(
        "Duplex"
    ):
        return {
            "back_side_slot_order": "same",
        }

    back_side_slot_order = (
        _get_text(
            parser,
            "Duplex",
            "Back_Side_Slot_Order",
            fallback="Same",
        )
        .strip()
        .lower()
    )

    if back_side_slot_order not in {
        "same",
        "mirror_horizontal",
        "mirror_vertical",
        "reverse",
    }:
        raise ValueError(
            "[Duplex] Back_Side_Slot_Order "
            "must be Same, Mirror_Horizontal, "
            "Mirror_Vertical, or Reverse."
        )

    return {
        "back_side_slot_order": (
            back_side_slot_order
        ),
    }