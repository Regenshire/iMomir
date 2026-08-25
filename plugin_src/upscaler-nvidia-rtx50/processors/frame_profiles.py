FRAME_PROFILES = {
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