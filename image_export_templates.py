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

        # Used only when generating artificial bleed for ZIP image export.
        # Keep this to one or two clean border-color samples.
        "bleed_fill_sample_regions": [
            {"x1": 0.020, "y1": 0.955, "x2": 0.180, "y2": 0.990},
            {"x1": 0.760, "y1": 0.955, "x2": 0.940, "y2": 0.990},
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

        "bleed_fill_sample_regions": [
            {"x1": 0.500, "y1": 0.980, "x2": 0.550, "y2": 0.985},
            {"x1": 0.800, "y1": 0.980, "x2": 0.940, "y2": 0.985},
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

        "bleed_fill_sample_regions": [
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

        "bleed_fill_sample_regions": [
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
            {"x1": 0.49, "y1": 0.991, "x2": 0.51, "y2": 0.993},
            {"x1": 0.55, "y1": 0.994, "x2": 0.54, "y2": 0.996},
        ],

        "card_matte_sample_regions": [
            {"x1": 0.010, "y1": 0.900, "x2": 0.055, "y2": 0.980},
            {"x1": 0.945, "y1": 0.900, "x2": 0.990, "y2": 0.980},
            {"x1": 0.49, "y1": 0.955, "x2": 0.51, "y2": 0.965},
            {"x1": 0.52, "y1": 0.965, "x2": 0.54, "y2": 0.985},
        ],

        "bleed_fill_sample_regions": [
            {"x1": 0.490, "y1": 0.955, "x2": 0.510, "y2": 0.965},
            {"x1": 0.520, "y1": 0.965, "x2": 0.540, "y2": 0.985},
        ],

        "card_corner_radius_pct": 0.060,
        "fallback_rgb": (15, 12, 12),
    },

    "2015_short": {
        "template_name": "M15 Frame - Short",
        "inherits": "2015",

        #"overlay_fill_rgb_override": (255, 0, 255),

        # This is basically the old behavior. Small lower-left black footer label.
        "overlay_box": {
            "x1": 0.000,
            "y1": 0.929,
            "x2": 0.450,
            "y2": 1.000,
        },
        "text_box": {
            "x1": 0.066,
            "y1": 0.940,
            "x2": 0.471,
            "y2": 0.973,
        },
        "text_align": "left",

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

        "bleed_fill_sample_regions": [
            {"x1": 0.275, "y1": 0.955, "x2": 0.450, "y2": 0.990},
            {"x1": 0.550, "y1": 0.955, "x2": 0.725, "y2": 0.990},
        ],

        "card_corner_radius_pct": 0.16,
        "fallback_rgb": (20, 17, 15),
    },

    "classic_floating": {
        "template_name": "Classic Floating Frame",
        "inherits": "1993",

        # Based on the 1993 frame, but make the export box feel more like a
        # "floating" tag instead of something attached directly to the edge.
        "overlay_box": {
            "x1": 0.285,
            "y1": 0.904,
            "x2": 0.715,
            "y2": 0.948,
        },
        "text_box": {
            "x1": 0.305,
            "y1": 0.902,
            "x2": 0.695,
            "y2": 0.937,
        },
        "text_align": "center",

        # Fully rounded floating badge.
        "overlay_corner_radius_pct": 0.040,
        "overlay_round_corners": {
            "top_left": True,
            "top_right": True,
            "bottom_right": True,
            "bottom_left": True,
        },

        # Sample from the lower classic frame matte area so the floating label
        # picks up a natural old-frame tone.
        "overlay_fill_sample_regions": [
            {"x1": 0.500, "y1": 0.940, "x2": 0.550, "y2": 0.945},
            {"x1": 0.800, "y1": 0.940, "x2": 0.940, "y2": 0.945},
        ],

        "card_matte_sample_regions": [
            {"x1": 0.500, "y1": 0.980, "x2": 0.550, "y2": 0.985},
            {"x1": 0.500, "y1": 0.980, "x2": 0.550, "y2": 0.985},
            {"x1": 0.500, "y1": 0.980, "x2": 0.550, "y2": 0.985},
            {"x1": 0.500, "y1": 0.980, "x2": 0.550, "y2": 0.985},
        ],

        "bleed_fill_sample_regions": [
            {"x1": 0.500, "y1": 0.980, "x2": 0.550, "y2": 0.985},
            {"x1": 0.800, "y1": 0.980, "x2": 0.940, "y2": 0.985},
        ],

        "card_corner_radius_pct": 0.00,
        "fallback_rgb": (20, 17, 15),
    },

    "classic_floating_below": {
        "template_name": "Classic Floating Frame - Below",
        "inherits": "1993",

        # Based on the 1993 frame, but make the export box feel more like a
        # "floating" tag instead of something attached directly to the edge.
        "overlay_box": {
            "x1": 0.285,
            "y1": 0.944,
            "x2": 0.715,
            "y2": 0.988,
        },
        "text_box": {
            "x1": 0.220,
            "y1": 0.971,
            "x2": 0.780,
            "y2": 0.994,
        },
        "text_align": "center",

        # Hide the overlay badge entirely. This template draws text directly
        # over the card art/frame instead of placing it in a filled box.
        "overlay_box_enabled": False,

        # Explicit text styling for this template.
        "text_fill_rgb_override": (255, 255, 255),
        "text_font_family": "Plantin MT Pro",
        "text_font_size_pt": 18.0,
        "text_font_bold": False,
        "text_shadow_enabled": True,

        # Fully rounded floating badge.
        "overlay_corner_radius_pct": 0.040,
        "overlay_round_corners": {
            "top_left": True,
            "top_right": True,
            "bottom_right": True,
            "bottom_left": True,
        },

        # Sample from the lower classic frame matte area so the floating label
        # picks up a natural old-frame tone.
        "overlay_fill_sample_regions": [
            {"x1": 0.500, "y1": 0.940, "x2": 0.550, "y2": 0.945},
            {"x1": 0.800, "y1": 0.940, "x2": 0.940, "y2": 0.945},
        ],

        "card_matte_sample_regions": [
            {"x1": 0.500, "y1": 0.980, "x2": 0.550, "y2": 0.985},
            {"x1": 0.500, "y1": 0.980, "x2": 0.550, "y2": 0.985},
            {"x1": 0.500, "y1": 0.980, "x2": 0.550, "y2": 0.985},
            {"x1": 0.500, "y1": 0.980, "x2": 0.550, "y2": 0.985},
        ],

        "bleed_fill_sample_regions": [
            {"x1": 0.500, "y1": 0.980, "x2": 0.550, "y2": 0.985},
            {"x1": 0.800, "y1": 0.980, "x2": 0.940, "y2": 0.985},
        ],

        "card_corner_radius_pct": 0.00,
        "fallback_rgb": (20, 17, 15),
    },
}

CARD_WIDTH_MM = 63.0
CARD_HEIGHT_MM = 88.0
BLEED_SIZE_MM = 3.0
BLEED_WIDTH_MM = CARD_WIDTH_MM + (BLEED_SIZE_MM * 2.0)
BLEED_HEIGHT_MM = CARD_HEIGHT_MM + (BLEED_SIZE_MM * 2.0)


def convert_card_x_to_bleed_x(value):
    return (BLEED_SIZE_MM + (float(value) * CARD_WIDTH_MM)) / BLEED_WIDTH_MM


def convert_card_y_to_bleed_y(value):
    return (BLEED_SIZE_MM + (float(value) * CARD_HEIGHT_MM)) / BLEED_HEIGHT_MM


def convert_card_width_to_bleed_width(value):
    return (float(value) * CARD_WIDTH_MM) / BLEED_WIDTH_MM


def convert_card_height_to_bleed_height(value):
    return (float(value) * CARD_HEIGHT_MM) / BLEED_HEIGHT_MM


def make_bleed_box(box):
    if not isinstance(box, dict):
        return box

    return {
        "x1": convert_card_x_to_bleed_x(box.get("x1", 0.0)),
        "y1": convert_card_y_to_bleed_y(box.get("y1", 0.0)),
        "x2": convert_card_x_to_bleed_x(box.get("x2", 1.0)),
        "y2": convert_card_y_to_bleed_y(box.get("y2", 1.0)),
    }


def make_bleed_regions(regions):
    return [
        make_bleed_box(region)
        for region in (regions or [])
        if isinstance(region, dict)
    ]


def make_bleed_cutout(cutout):
    if not isinstance(cutout, dict):
        return cutout

    bleed_cutout = dict(cutout)
    shape = (bleed_cutout.get("shape") or "").strip().lower()

    if "cx" in bleed_cutout:
        bleed_cutout["cx"] = convert_card_x_to_bleed_x(bleed_cutout["cx"])

    if "cy" in bleed_cutout:
        bleed_cutout["cy"] = convert_card_y_to_bleed_y(bleed_cutout["cy"])

    if "x1" in bleed_cutout:
        bleed_cutout["x1"] = convert_card_x_to_bleed_x(bleed_cutout["x1"])

    if "x2" in bleed_cutout:
        bleed_cutout["x2"] = convert_card_x_to_bleed_x(bleed_cutout["x2"])

    if "y1" in bleed_cutout:
        bleed_cutout["y1"] = convert_card_y_to_bleed_y(bleed_cutout["y1"])

    if "y2" in bleed_cutout:
        bleed_cutout["y2"] = convert_card_y_to_bleed_y(bleed_cutout["y2"])

    if "rx" in bleed_cutout:
        bleed_cutout["rx"] = convert_card_width_to_bleed_width(bleed_cutout["rx"])

    if "ry" in bleed_cutout:
        bleed_cutout["ry"] = convert_card_height_to_bleed_height(bleed_cutout["ry"])

    if shape == "circle" and "r" in bleed_cutout:
        bleed_cutout["r"] = convert_card_width_to_bleed_width(bleed_cutout["r"])

    return bleed_cutout


def make_bleed_template(template_key, template_config):
    bleed_template = {
        "template_name": f"{template_config.get('template_name') or template_key} - Bleed",
        "inherits": template_key,
        "is_bleed_template": True,
    }

    if "overlay_box" in template_config:
        bleed_template["overlay_box"] = make_bleed_box(template_config["overlay_box"])

    if "text_box" in template_config:
        bleed_template["text_box"] = make_bleed_box(template_config["text_box"])

    if "overlay_fill_sample_regions" in template_config:
        bleed_template["overlay_fill_sample_regions"] = make_bleed_regions(
            template_config.get("overlay_fill_sample_regions") or []
        )

    if "card_matte_sample_regions" in template_config:
        bleed_template["card_matte_sample_regions"] = make_bleed_regions(
            template_config.get("card_matte_sample_regions") or []
        )

    if "border_sample_regions" in template_config:
        bleed_template["border_sample_regions"] = make_bleed_regions(
            template_config.get("border_sample_regions") or []
        )

    if "overlay_cutouts" in template_config:
        bleed_template["overlay_cutouts"] = [
            make_bleed_cutout(cutout)
            for cutout in (template_config.get("overlay_cutouts") or [])
        ]

    return bleed_template


def register_bleed_templates():
    for template_key, template_config in list(CARD_EXPORT_FRAME_TEMPLATES.items()):
        if template_key.endswith("_bleed"):
            continue

        bleed_key = f"{template_key}_bleed"

        if bleed_key in CARD_EXPORT_FRAME_TEMPLATES:
            continue

        CARD_EXPORT_FRAME_TEMPLATES[bleed_key] = make_bleed_template(
            template_key,
            template_config,
        )


register_bleed_templates()

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
        if template_key == "default" or template_key.endswith("_bleed"):
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


def resolve_card_export_template_config(set_code, frame_version, release_year=None, template_key_override=None, use_bleed_template=False):
    clean_template_key_override = (template_key_override or "").strip().lower()

    def resolve_key_with_bleed(template_key):
        clean_key = (template_key or "default").strip().lower()

        if use_bleed_template and not clean_key.endswith("_bleed"):
            bleed_key = f"{clean_key}_bleed"
            if bleed_key in CARD_EXPORT_FRAME_TEMPLATES:
                clean_key = bleed_key

        return get_card_export_template_by_key(clean_key)

    if clean_template_key_override and clean_template_key_override != "auto":
        if clean_template_key_override in CARD_EXPORT_FRAME_TEMPLATES:
            return resolve_key_with_bleed(clean_template_key_override)
        
    clean_set_code = (set_code or "").strip().upper()
    clean_frame_version = (frame_version or "").strip().lower()

    if clean_set_code in CARD_EXPORT_SET_TEMPLATE_OVERRIDES:
        override = dict(CARD_EXPORT_SET_TEMPLATE_OVERRIDES[clean_set_code])
        inherits = (override.get("inherits") or clean_frame_version or "default").strip().lower()
        base_template = get_card_export_template_by_key(inherits)
        return deep_merge_template(base_template, override)

    if clean_frame_version in CARD_EXPORT_FRAME_TEMPLATES:
        return resolve_key_with_bleed(clean_frame_version)

    try:
        release_year = int(release_year) if release_year is not None else None
    except (TypeError, ValueError):
        release_year = None

    if release_year is not None:
        if release_year < 2003:
            return resolve_key_with_bleed("1993")
        if release_year < 2014:
            return resolve_key_with_bleed("2003")
        return resolve_key_with_bleed("2015")

    return resolve_key_with_bleed("default")