# Image export overlay templates for Chaos Draft card image exports.
#
# Coordinate values are normalized percentages of the final image:
# x1/y1 = top-left, x2/y2 = bottom-right.
#
# These templates intentionally start close to the old small footer-label behavior:
# - small footer box
# - lower-left placement by default
# - no huge metadata-covering blocks
#
# Tune these values as test exports reveal frame-specific needs.

LEGACY_LABEL_BOX = {
    "x1": 0.000,
    "y1": 0.958,
    "x2": 0.445,
    "y2": 1.000,
}

LEGACY_TEXT_BOX = {
    "x1": 0.025,
    "y1": 0.962,
    "x2": 0.430,
    "y2": 0.995,
}

CARD_EXPORT_FRAME_TEMPLATES = {
    "default": {
        "template_name": "Default Legacy Small Footer",
        "inherits": None,

        "overlay_box": dict(LEGACY_LABEL_BOX),
        "text_box": dict(LEGACY_TEXT_BOX),
        "text_align": "left",
        "text_fill_rgb": (255, 255, 255),

        # Overlay-box corner rounding. These control only the pack-code box,
        # not the full card image.
        "overlay_corner_radius_pct": 0.000,
        "overlay_round_corners": {
            "top_left": False,
            "top_right": False,
            "bottom_right": False,
            "bottom_left": False,
        },

        # Full card corner handling. This helps old scans/images with pale corner artifacts.
        "card_corner_mode": "rounded_mask",
        "card_corner_radius_pct": 0.030,

        "border_sample_regions": [
            {"x1": 0.010, "y1": 0.955, "x2": 0.070, "y2": 0.990},
            {"x1": 0.010, "y1": 0.020, "x2": 0.070, "y2": 0.085},
        ],
        "fallback_rgb": (18, 12, 12),
    },

    "1993": {
        "template_name": "Original Frame",
        "inherits": "default",

        # Original frames often have centered artist/copyright data.
        # Start small and centered. Adjust this if you want it lower/wider.
        "overlay_box": {
            "x1": 0.255,
            "y1": 0.958,
            "x2": 0.745,
            "y2": 1.000,
        },
        "text_box": {
            "x1": 0.275,
            "y1": 0.954,
            "x2": 0.725,
            "y2": 0.987,
        },
        "text_align": "center",

        # Example selective rounding: top corners only.
        # Set these to False/True as you test.
        "overlay_corner_radius_pct": 0.020,
        "overlay_round_corners": {
            "top_left": True,
            "top_right": True,
            "bottom_right": False,
            "bottom_left": False,
        },

        "overlay_fill_sample_regions": [
            {"x1": 0.50, "y1": 0.980, "x2": 0.55, "y2": 0.985},
            {"x1": 0.80, "y1": 0.980, "x2": 0.94, "y2": 0.985},
        ],

        "card_matte_sample_regions": [
            {"x1": 0.500, "y1": 0.980, "x2": 0.55, "y2": 0.985},
            {"x1": 0.500, "y1": 0.980, "x2": 0.55, "y2": 0.985},
            {"x1": 0.500, "y1": 0.980, "x2": 0.55, "y2": 0.985},
            {"x1": 0.500, "y1": 0.980, "x2": 0.55, "y2": 0.985},
        ],

        "card_corner_radius_pct": 0.16,
        "fallback_rgb": (20, 17, 15),
    },

    "1997": {
        "template_name": "Original Frame",
        "inherits": "default",

        # Original frames often have centered artist/copyright data.
        # Start small and centered. Adjust this if you want it lower/wider.
        "overlay_box": {
            "x1": 0.255,
            "y1": 0.958,
            "x2": 0.745,
            "y2": 1.000,
        },
        "text_box": {
            "x1": 0.275,
            "y1": 0.954,
            "x2": 0.725,
            "y2": 0.987,
        },
        "text_align": "center",

        # Example selective rounding: top corners only.
        # Set these to False/True as you test.
        "overlay_corner_radius_pct": 0.020,
        "overlay_round_corners": {
            "top_left": True,
            "top_right": True,
            "bottom_right": False,
            "bottom_left": False,
        },

        "overlay_fill_sample_regions": [
            {"x1": 0.62, "y1": 0.945, "x2": 0.76, "y2": 0.985},
            {"x1": 0.80, "y1": 0.945, "x2": 0.94, "y2": 0.985},
        ],

        "card_matte_sample_regions": [
            {"x1": 0.010, "y1": 0.900, "x2": 0.055, "y2": 0.980},
            {"x1": 0.945, "y1": 0.900, "x2": 0.990, "y2": 0.980},
            {"x1": 0.020, "y1": 0.945, "x2": 0.180, "y2": 0.990},
            {"x1": 0.760, "y1": 0.945, "x2": 0.940, "y2": 0.990},
        ],

        "card_corner_radius_pct": 0.16,
        "fallback_rgb": (20, 17, 15),
    },

    "2003": {
        "template_name": "Modern Frame",
        "inherits": "default",

        # Keep close to old lower-left footer behavior.
        "overlay_box": {
            "x1": 0.000,
            "y1": 0.948,
            "x2": 0.665,
            "y2": 1.000,
        },
        "text_box": {
            "x1": 0.066,
            "y1": 0.950,
            "x2": 0.471,
            "y2": 0.983,
        },
        "text_align": "left",

        "overlay_corner_radius_pct": 0.020,
        "overlay_round_corners": {
            "top_left": False,
            "top_right": True,
            "bottom_right": False,
            "bottom_left": False,
        },

        "overlay_fill_sample_regions": [
            {"x1": 0.62, "y1": 0.945, "x2": 0.76, "y2": 0.985},
            {"x1": 0.80, "y1": 0.945, "x2": 0.94, "y2": 0.985},
        ],

        "card_matte_sample_regions": [
            {"x1": 0.010, "y1": 0.900, "x2": 0.055, "y2": 0.980},
            {"x1": 0.945, "y1": 0.900, "x2": 0.990, "y2": 0.980},
            {"x1": 0.020, "y1": 0.945, "x2": 0.180, "y2": 0.990},
            {"x1": 0.760, "y1": 0.945, "x2": 0.940, "y2": 0.990},
        ],

        "card_corner_radius_pct": 0.16,
        "fallback_rgb": (18, 14, 12),
    },

    "2015": {
        "template_name": "M15 Frame",
        "inherits": "default",

        #"overlay_fill_rgb_override": (255, 0, 255),

        # This is basically the old behavior. Small lower-left black footer label.
        "overlay_box": {
            "x1": 0.000,
            "y1": 0.929,
            "x2": 0.500,
            "y2": 1.000,
        },
        "text_box": {
            "x1": 0.066,
            "y1": 0.940,
            "x2": 0.471,
            "y2": 0.973,
        },
        "text_align": "left",

        "overlay_corner_radius_pct": 0.00,
        "overlay_round_corners": {
            "top_left": False,
            "top_right": False,
            "bottom_right": False,
            "bottom_left": False,
        },

        # New: carve-out support for the center holo stamp / emblem.
        "overlay_cutouts": [
            {
                "shape": "ellipse",
                "cx": 0.500,
                "cy": 0.929,
                "rx": 0.055,
                "ry": 0.021,
            },
        ],

        "overlay_fill_sample_regions": [
            {"x1": 0.62, "y1": 0.945, "x2": 0.76, "y2": 0.985},
            {"x1": 0.80, "y1": 0.945, "x2": 0.94, "y2": 0.985},
        ],

        "card_matte_sample_regions": [
            {"x1": 0.010, "y1": 0.900, "x2": 0.055, "y2": 0.980},
            {"x1": 0.945, "y1": 0.900, "x2": 0.990, "y2": 0.980},
            {"x1": 0.020, "y1": 0.945, "x2": 0.180, "y2": 0.990},
            {"x1": 0.760, "y1": 0.945, "x2": 0.940, "y2": 0.990},
        ],

        "card_corner_radius_pct": 0.060,
        "fallback_rgb": (15, 12, 12),
    },

    "future": {
        "template_name": "Future Sight Frame",
        "inherits": "default",

        # Future Sight frames have odd bottom/footer shapes.
        "overlay_box": {
            "x1": 0.255,
            "y1": 0.958,
            "x2": 0.745,
            "y2": 1.000,
        },
        "text_box": {
            "x1": 0.275,
            "y1": 0.954,
            "x2": 0.725,
            "y2": 0.987,
        },
        "text_align": "center",

        # Example selective rounding: top corners only.
        # Set these to False/True as you test.
        "overlay_corner_radius_pct": 0.020,
        "overlay_round_corners": {
            "top_left": True,
            "top_right": True,
            "bottom_right": False,
            "bottom_left": False,
        },

        "card_corner_radius_pct": 0.16,
        "fallback_rgb": (20, 17, 15),
    },
}

CARD_EXPORT_SET_TEMPLATE_OVERRIDES = {
    # Use this for specific weird/problem sets after testing.
    #
    # Example:
    # "WC03": {
    #     "inherits": "2003",
    #     "overlay_box": {"x1": 0.000, "y1": 0.956, "x2": 0.470, "y2": 1.000},
    # },
    #
    # Example for MB2 Future Sight bonus cards if you want special treatment:
    # "MB2": {
    #     "inherits": "future",
    # },
}


def deep_merge_template(base, override):
    merged = dict(base)

    for key, value in (override or {}).items():
        if (
            isinstance(value, dict)
            and isinstance(merged.get(key), dict)
        ):
            nested = dict(merged[key])
            nested.update(value)
            merged[key] = nested
        else:
            merged[key] = value

    return merged

def get_card_export_template_options():
    options = [
        {
            "value": "auto",
            "label": "Automatic - Use Card Printing Frame",
        }
    ]

    for template_key, template_config in CARD_EXPORT_FRAME_TEMPLATES.items():
        if template_key == "default":
            continue

        template_name = template_config.get("template_name") or template_key

        if template_key in {"1993", "1997"}:
            label = f"{template_name} ({template_key})"
        elif template_key == "2003":
            label = "Modern Frame (2003)"
        elif template_key == "2015":
            label = "M15 Frame (2015)"
        elif template_key == "future":
            label = "Future Sight Frame"
        else:
            label = template_name

        options.append({
            "value": template_key,
            "label": label,
        })

    return options

def get_card_export_template_by_key(template_key):
    clean_key = (template_key or "default").strip().lower()

    if clean_key not in CARD_EXPORT_FRAME_TEMPLATES:
        clean_key = "default"

    template = dict(CARD_EXPORT_FRAME_TEMPLATES[clean_key])
    inherits = template.get("inherits")

    if inherits:
        base_template = get_card_export_template_by_key(inherits)
        template = deep_merge_template(base_template, template)

    return template


def resolve_card_export_template_config(set_code, frame_version, release_year=None, template_key_override=None):
    clean_template_key_override = (template_key_override or "").strip().lower()

    if clean_template_key_override and clean_template_key_override != "auto":
        if clean_template_key_override in CARD_EXPORT_FRAME_TEMPLATES:
            return get_card_export_template_by_key(clean_template_key_override)
        
    clean_set_code = (set_code or "").strip().upper()
    clean_frame_version = (frame_version or "").strip().lower()

    if clean_set_code in CARD_EXPORT_SET_TEMPLATE_OVERRIDES:
        override = dict(CARD_EXPORT_SET_TEMPLATE_OVERRIDES[clean_set_code])
        inherits = (override.get("inherits") or clean_frame_version or "default").strip().lower()
        base_template = get_card_export_template_by_key(inherits)
        return deep_merge_template(base_template, override)

    if clean_frame_version in CARD_EXPORT_FRAME_TEMPLATES:
        return get_card_export_template_by_key(clean_frame_version)

    try:
        release_year = int(release_year) if release_year is not None else None
    except (TypeError, ValueError):
        release_year = None

    if release_year is not None:
        if release_year < 2003:
            return get_card_export_template_by_key("1993")
        if release_year < 2014:
            return get_card_export_template_by_key("2003")
        return get_card_export_template_by_key("2015")

    return get_card_export_template_by_key("default")