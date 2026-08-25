FRAME_PROFILES = {
    "normal_1993": {
        "profile_id": (
            "normal_1993"
        ),

        "frame_versions": {
            "1993",
        },

        "layouts": {
            "normal",
        },

        "title_box": (
            0.064,
            0.048,
            0.872,
            0.070,
        ),

        "mana_cost_box": (
            0.720,
            0.048,
            0.216,
            0.070,
        ),

        "type_line_box": (
            0.064,
            0.500,
            0.872,
            0.055,
        ),

        "rules_text_box": (
            0.064,
            0.552,
            0.872,
            0.318,
        ),

        "power_toughness_exclusion": (
            0.730,
            0.812,
            0.225,
            0.105,
        ),

        "power_toughness_box": (
            0.730,
            0.812,
            0.225,
            0.105,
        ),

        "bottom_text_box": (
            0.055,
            0.895,
            0.890,
            0.070,
        ),
    },

    "normal_2015": {
        "profile_id": (
            "normal_2015"
        ),

        "frame_versions": {
            "2015",
        },

        "layouts": {
            "normal",
        },

        "title_box": (
            0.064,
            0.042,
            0.872,
            0.074,
        ),

        "mana_cost_box": (
            0.705,
            0.042,
            0.231,
            0.074,
        ),

        "type_line_box": (
            0.066,
            0.492,
            0.868,
            0.060,
        ),

        # Normalized coordinates:
        #
        # x,
        # y,
        # width,
        # height
        #
        # These are deliberately stored
        # independently of source image
        # resolution.
        "rules_text_box": (
            0.068,
            0.552,
            0.864,
            0.318,
        ),

        # Preserve this area when the
        # card is a creature so Rules
        # Text processing does not alter
        # the P/T box.
        "power_toughness_exclusion": (
            0.745,
            0.817,
            0.215,
            0.100,
        ),

        "power_toughness_box": (
            0.745,
            0.817,
            0.215,
            0.100,
        ),

        "bottom_text_box": (
            0.055,
            0.895,
            0.890,
            0.070,
        ),
    },

        "class_2015": {
        "profile_id": (
            "class_2015"
        ),

        "frame_versions": {
            "2015",
        },

        "layouts": {
            "class",
        },

        "title_box": (
            0.064,
            0.042,
            0.872,
            0.074,
        ),

        "mana_cost_box": (
            0.705,
            0.042,
            0.231,
            0.074,
        ),

        # Class cards divide most of the
        # body vertically between artwork
        # on the left and rules/level
        # sections on the right.
        #
        # v1 still requires one overall
        # Rules Text region.
        "rules_text_box": (
            0.500,
            0.108,
            0.425,
            0.733,
        ),

        # The v3 router uses these smaller
        # regions instead of processing
        # the entire Class text column as
        # one image.
        "class_rules_intro_box": (
            0.500,
            0.108,
            0.425,
            0.226,
        ),

        "class_level_2_header_box": (
            0.500,
            0.334,
            0.425,
            0.052,
        ),

        "class_level_2_text_box": (
            0.500,
            0.386,
            0.425,
            0.104,
        ),

        "class_level_3_header_box": (
            0.500,
            0.490,
            0.425,
            0.052,
        ),

        "class_level_3_text_box": (
            0.500,
            0.542,
            0.425,
            0.299,
        ),

        "type_line_box": (
            0.060,
            0.843,
            0.880,
            0.065,
        ),

        "bottom_text_box": (
            0.055,
            0.922,
            0.890,
            0.062,
        ),

        # Explicitly tell Magic Card AI v3
        # which regions this layout wants
        # routed through the targeted
        # RealESRNet model.
        "target_region_keys": [
            "title_box",
            "mana_cost_box",
            "class_rules_intro_box",
            "class_level_2_header_box",
            "class_level_2_text_box",
            "class_level_3_header_box",
            "class_level_3_text_box",
            "type_line_box",
            "bottom_text_box",
        ],
    },

}


def normalize_card_metadata(
    card,
):
    if not isinstance(
        card,
        dict,
    ):
        return {}

    return {
        "frame_version": str(
            card.get(
                "frame_version",
                "",
            )
            or ""
        ).strip().lower(),

        "layout": str(
            card.get(
                "layout",
                "",
            )
            or ""
        ).strip().lower(),

        "type_line": str(
            card.get(
                "type_line",
                "",
            )
            or ""
        ).strip(),

        "border_color": str(
            card.get(
                "border_color",
                "",
            )
            or ""
        ).strip().lower(),
    }


def resolve_frame_profile(
    card,
):
    card_metadata = (
        normalize_card_metadata(
            card
        )
    )

    frame_version = (
        card_metadata.get(
            "frame_version",
            ""
        )
    )

    layout = (
        card_metadata.get(
            "layout",
            ""
        )
    )

    for profile in (
        FRAME_PROFILES.values()
    ):
        if (
            frame_version
            not in profile[
                "frame_versions"
            ]
        ):
            continue

        if (
            layout
            not in profile[
                "layouts"
            ]
        ):
            continue

        return dict(
            profile
        )

    return None