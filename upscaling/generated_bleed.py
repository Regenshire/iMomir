import os

from PIL import (
    Image,
    ImageDraw,
    ImageFilter,
    ImageStat,
)


CARD_WIDTH_MM = 63.0
CARD_HEIGHT_MM = 88.0

DEFAULT_BLEED_MM = 3.0

SYNTHETIC_BLEED_MODEL_ID = (
    "imomir_synthetic_frame_bleed_v1"
)


def normalize_printing_frame_name(
    value,
):
    return " ".join(
        str(
            value
            or ""
        )
        .strip()
        .lower()
        .replace(
            "_",
            " ",
        )
        .replace(
            "-",
            " ",
        )
        .split()
    )


def resolve_bleed_strategy(
    printing_frame_key,
    printing_frame_name,
):
    route_text = (
        normalize_printing_frame_name(
            (
                f"{printing_frame_key or ''} "
                f"{printing_frame_name or ''}"
            )
        )
    )

    if "borderless" in route_text:
        return {
            "profile": "borderless",
            "requires_outpaint": True,

            "edges": {
                "top": "generative",
                "right": "generative",
                "bottom": "synthetic",
                "left": "generative",
            },

            "corners": {
                "top_left": "generative",
                "top_right": "generative",
                "bottom_right": "synthetic",
                "bottom_left": "synthetic",
            },
        }

    if "extended" in route_text:
        return {
            "profile": "extended",
            "requires_outpaint": True,

            "edges": {
                "top": "synthetic",
                "right": "generative",
                "bottom": "synthetic",
                "left": "generative",
            },

            "corners": {
                "top_left": "synthetic",
                "top_right": "synthetic",
                "bottom_right": "synthetic",
                "bottom_left": "synthetic",
            },
        }

    return {
        "profile": "solid_frame",
        "requires_outpaint": False,

        "edges": {
            "top": "synthetic",
            "right": "synthetic",
            "bottom": "synthetic",
            "left": "synthetic",
        },

        "corners": {
            "top_left": "synthetic",
            "top_right": "synthetic",
            "bottom_right": "synthetic",
            "bottom_left": "synthetic",
        },
    }


def calculate_bleed_pixels(
    width,
    height,
    bleed_size_mm,
):
    bleed_size_mm = float(
        bleed_size_mm
    )

    if bleed_size_mm <= 0:
        raise ValueError(
            "bleed_size_mm must be "
            "greater than zero."
        )

    bleed_x = max(
        1,
        int(
            round(
                width
                * (
                    bleed_size_mm
                    / CARD_WIDTH_MM
                )
            )
        ),
    )

    bleed_y = max(
        1,
        int(
            round(
                height
                * (
                    bleed_size_mm
                    / CARD_HEIGHT_MM
                )
            )
        ),
    )

    return (
        bleed_x,
        bleed_y,
    )


def calculate_corner_radius_pixels(
    image,
    corner_radius_pct,
):
    try:
        clean_pct = float(
            corner_radius_pct
            or 0.03
        )

    except (
        TypeError,
        ValueError,
    ):
        clean_pct = 0.03

    # Always repair some amount of
    # corner, even on frame definitions
    # that historically disabled export
    # corner rounding.
    clean_pct = max(
        0.01,
        min(
            clean_pct,
            0.20,
        ),
    )

    return max(
        1,
        int(
            round(
                min(
                    image.size
                )
                * clean_pct
            )
        ),
    )


def clamp(
    value,
    minimum,
    maximum,
):
    return max(
        minimum,
        min(
            value,
            maximum,
        ),
    )


def median_rgb(
    image,
    box,
):
    width, height = (
        image.size
    )

    left = clamp(
        int(
            box[0]
        ),
        0,
        max(
            0,
            width - 1,
        ),
    )

    top = clamp(
        int(
            box[1]
        ),
        0,
        max(
            0,
            height - 1,
        ),
    )

    right = clamp(
        int(
            box[2]
        ),
        left + 1,
        width,
    )

    bottom = clamp(
        int(
            box[3]
        ),
        top + 1,
        height,
    )

    statistics = ImageStat.Stat(
        image.crop(
            (
                left,
                top,
                right,
                bottom,
            )
        )
    )

    median = (
        statistics.median
    )

    if len(
        median
    ) < 3:
        value = int(
            round(
                median[0]
                if median
                else 0
            )
        )

        return (
            value,
            value,
            value,
        )

    return tuple(
        int(
            round(
                channel
            )
        )
        for channel
        in median[:3]
    )


def sample_edge_reference_colors(
    image,
    corner_radius_px,
):
    width, height = (
        image.size
    )

    sample_depth = max(
        3,
        int(
            round(
                min(
                    width,
                    height,
                )
                * 0.010
            )
        ),
    )

    horizontal_inset = min(
        max(
            corner_radius_px,
            sample_depth * 2,
        ),
        max(
            0,
            (width // 2) - 2,
        ),
    )

    vertical_inset = min(
        max(
            corner_radius_px,
            sample_depth * 2,
        ),
        max(
            0,
            (height // 2) - 2,
        ),
    )

    return {
        "top": median_rgb(
            image,
            (
                horizontal_inset,
                0,
                width
                - horizontal_inset,
                sample_depth,
            ),
        ),

        "right": median_rgb(
            image,
            (
                width
                - sample_depth,
                vertical_inset,
                width,
                height
                - vertical_inset,
            ),
        ),

        "bottom": median_rgb(
            image,
            (
                horizontal_inset,
                height
                - sample_depth,
                width
                - horizontal_inset,
                height,
            ),
        ),

        "left": median_rgb(
            image,
            (
                0,
                vertical_inset,
                sample_depth,
                height
                - vertical_inset,
            ),
        ),
    }


def blend_rgb(
    color_a,
    color_b,
    weight_b,
):
    weight_b = max(
        0.0,
        min(
            float(
                weight_b
            ),
            1.0,
        ),
    )

    weight_a = (
        1.0
        - weight_b
    )

    return tuple(
        int(
            round(
                (
                    channel_a
                    * weight_a
                )
                + (
                    channel_b
                    * weight_b
                )
            )
        )
        for (
            channel_a,
            channel_b,
        )
        in zip(
            color_a,
            color_b,
        )
    )


def build_valid_card_mask(
    size,
    radius_px,
):
    width, height = size

    mask = Image.new(
        "L",
        size,
        0,
    )

    draw = ImageDraw.Draw(
        mask
    )

    draw.rounded_rectangle(
        (
            0,
            0,
            width - 1,
            height - 1,
        ),
        radius=radius_px,
        fill=255,
    )

    return mask


def repair_synthetic_corners(
    source_image,
    radius_px,
    edge_colors,
):
    repaired = (
        source_image
        .convert(
            "RGB"
        )
        .copy()
    )

    width, height = (
        repaired.size
    )

    valid_mask = (
        build_valid_card_mask(
            repaired.size,
            radius_px,
        )
    )

    corner_specs = {
        "top_left": (
            (
                0,
                0,
                radius_px,
                radius_px,
            ),
            "top",
            "left",
            False,
            False,
        ),

        "top_right": (
            (
                width
                - radius_px,
                0,
                width,
                radius_px,
            ),
            "top",
            "right",
            True,
            False,
        ),

        "bottom_right": (
            (
                width
                - radius_px,
                height
                - radius_px,
                width,
                height,
            ),
            "bottom",
            "right",
            True,
            True,
        ),

        "bottom_left": (
            (
                0,
                height
                - radius_px,
                radius_px,
                height,
            ),
            "bottom",
            "left",
            False,
            True,
        ),
    }

    repaired_pixels = (
        repaired.load()
    )

    mask_pixels = (
        valid_mask.load()
    )

    for (
        box,
        horizontal_edge,
        vertical_edge,
        reverse_x,
        reverse_y,
    ) in corner_specs.values():
        (
            left,
            top,
            right,
            bottom,
        ) = box

        box_width = max(
            1,
            right - left,
        )

        box_height = max(
            1,
            bottom - top,
        )

        for y in range(
            top,
            bottom,
        ):
            for x in range(
                left,
                right,
            ):
                # Anything inside the valid
                # rounded card surface remains
                # untouched.
                if mask_pixels[
                    x,
                    y,
                ] != 0:
                    continue

                local_x = (
                    x - left
                ) / float(
                    max(
                        1,
                        box_width - 1,
                    )
                )

                local_y = (
                    y - top
                ) / float(
                    max(
                        1,
                        box_height - 1,
                    )
                )

                if reverse_x:
                    local_x = (
                        1.0
                        - local_x
                    )

                if reverse_y:
                    local_y = (
                        1.0
                        - local_y
                    )

                vertical_weight = (
                    local_y
                    / max(
                        0.0001,
                        local_x
                        + local_y,
                    )
                )

                repaired_pixels[
                    x,
                    y,
                ] = blend_rgb(
                    edge_colors[
                        horizontal_edge
                    ],
                    edge_colors[
                        vertical_edge
                    ],
                    vertical_weight,
                )

    return repaired


def get_edge_line(
    image,
    edge_name,
):
    width, height = (
        image.size
    )

    if edge_name == "top":
        return image.crop(
            (
                0,
                0,
                width,
                1,
            )
        )

    if edge_name == "bottom":
        return image.crop(
            (
                0,
                height - 1,
                width,
                height,
            )
        )

    if edge_name == "left":
        return image.crop(
            (
                0,
                0,
                1,
                height,
            )
        )

    if edge_name == "right":
        return image.crop(
            (
                width - 1,
                0,
                width,
                height,
            )
        )

    raise ValueError(
        "Unknown edge name: "
        f"{edge_name}"
    )


def build_bleed_band(
    repaired_image,
    edge_name,
    band_size,
):
    edge_line = get_edge_line(
        repaired_image,
        edge_name,
    )

    blur_radius = max(
        1,
        int(
            round(
                min(
                    repaired_image.size
                )
                * 0.004
            )
        ),
    )

    smooth_line = (
        edge_line.filter(
            ImageFilter.GaussianBlur(
                radius=blur_radius
            )
        )
    )

    horizontal = (
        edge_name
        in {
            "top",
            "bottom",
        }
    )

    band = Image.new(
        "RGB",
        (
            (
                repaired_image.width,
                band_size,
            )
            if horizontal
            else (
                band_size,
                repaired_image.height,
            )
        ),
    )

    for index in range(
        band_size
    ):
        distance_from_card = (
            band_size
            - 1
            - index
            if edge_name
            in {
                "top",
                "left",
            }
            else index
        )

        alpha = (
            distance_from_card
            / float(
                max(
                    1,
                    band_size - 1,
                )
            )
        ) ** 0.70

        blended_line = (
            Image.blend(
                edge_line,
                smooth_line,
                alpha,
            )
        )

        if horizontal:
            band.paste(
                blended_line,
                (
                    0,
                    index,
                ),
            )

        else:
            band.paste(
                blended_line,
                (
                    index,
                    0,
                ),
            )

    return band


def fill_outer_corner(
    output_image,
    box,
    horizontal_color,
    vertical_color,
):
    fill_color = blend_rgb(
        horizontal_color,
        vertical_color,
        0.5,
    )

    ImageDraw.Draw(
        output_image
    ).rectangle(
        box,
        fill=fill_color,
    )


def render_synthetic_fullbleed(
    source_image,
    bleed_size_mm,
    corner_radius_pct,
):
    source_image = (
        source_image.convert(
            "RGB"
        )
    )

    width, height = (
        source_image.size
    )

    (
        bleed_x,
        bleed_y,
    ) = calculate_bleed_pixels(
        width,
        height,
        bleed_size_mm,
    )

    corner_radius_px = (
        calculate_corner_radius_pixels(
            source_image,
            corner_radius_pct,
        )
    )

    # Actual colors come exclusively
    # from this actual Upscaled image.
    edge_colors = (
        sample_edge_reference_colors(
            source_image,
            corner_radius_px,
        )
    )

    # Replace the cut-out/blank corners
    # before creating any exterior bleed.
    repaired_image = (
        repair_synthetic_corners(
            source_image,
            corner_radius_px,
            edge_colors,
        )
    )

    output_width = (
        width
        + (bleed_x * 2)
    )

    output_height = (
        height
        + (bleed_y * 2)
    )

    output_image = Image.new(
        "RGB",
        (
            output_width,
            output_height,
        ),
        edge_colors[
            "bottom"
        ],
    )

    output_image.paste(
        build_bleed_band(
            repaired_image,
            "top",
            bleed_y,
        ),
        (
            bleed_x,
            0,
        ),
    )

    output_image.paste(
        build_bleed_band(
            repaired_image,
            "bottom",
            bleed_y,
        ),
        (
            bleed_x,
            bleed_y + height,
        ),
    )

    output_image.paste(
        build_bleed_band(
            repaired_image,
            "left",
            bleed_x,
        ),
        (
            0,
            bleed_y,
        ),
    )

    output_image.paste(
        build_bleed_band(
            repaired_image,
            "right",
            bleed_x,
        ),
        (
            bleed_x + width,
            bleed_y,
        ),
    )

    fill_outer_corner(
        output_image,
        (
            0,
            0,
            bleed_x,
            bleed_y,
        ),
        edge_colors[
            "top"
        ],
        edge_colors[
            "left"
        ],
    )

    fill_outer_corner(
        output_image,
        (
            bleed_x + width,
            0,
            output_width,
            bleed_y,
        ),
        edge_colors[
            "top"
        ],
        edge_colors[
            "right"
        ],
    )

    fill_outer_corner(
        output_image,
        (
            bleed_x + width,
            bleed_y + height,
            output_width,
            output_height,
        ),
        edge_colors[
            "bottom"
        ],
        edge_colors[
            "right"
        ],
    )

    fill_outer_corner(
        output_image,
        (
            0,
            bleed_y + height,
            bleed_x,
            output_height,
        ),
        edge_colors[
            "bottom"
        ],
        edge_colors[
            "left"
        ],
    )

    # Paste the repaired card rather
    # than the raw source. This is what
    # permanently fixes rounded-corner
    # cutouts in the full-bleed master.
    output_image.paste(
        repaired_image,
        (
            bleed_x,
            bleed_y,
        ),
    )

    return {
        "image": output_image,

        "bleed_pixels_x": (
            bleed_x
        ),

        "bleed_pixels_y": (
            bleed_y
        ),

        "corner_radius_px": (
            corner_radius_px
        ),

        "edge_colors": {
            key: list(
                value
            )
            for (
                key,
                value,
            )
            in edge_colors.items()
        },
    }

KNOWN_ALWAYS_SYNTHETIC_FRAME_KEYS = {
    "1993",
    "1997",
    "2003",
    "future",
}


def measure_edge_complexity(
    image,
    edge_name,
    corner_radius_px,
):
    width, height = (
        image.size
    )

    depth = max(
        6,
        int(
            round(
                min(
                    width,
                    height,
                )
                * 0.018
            )
        ),
    )

    inset = max(
        corner_radius_px,
        int(
            round(
                min(
                    width,
                    height,
                )
                * 0.040
            )
        ),
    )

    if edge_name == "top":
        box = (
            inset,
            0,
            width - inset,
            depth,
        )

    elif edge_name == "bottom":
        box = (
            inset,
            height - depth,
            width - inset,
            height,
        )

    elif edge_name == "left":
        box = (
            0,
            inset,
            depth,
            height - inset,
        )

    elif edge_name == "right":
        box = (
            width - depth,
            inset,
            width,
            height - inset,
        )

    else:
        raise ValueError(
            "Unknown edge: "
            f"{edge_name}"
        )

    region = (
        image.crop(
            box
        )
        .convert(
            "RGB"
        )
    )

    statistics = (
        ImageStat.Stat(
            region
        )
    )

    color_stddev = (
        sum(
            statistics.stddev[:3]
        )
        / 3.0
    )

    edge_image = (
        region.convert(
            "L"
        )
        .filter(
            ImageFilter.FIND_EDGES
        )
    )

    texture_energy = (
        ImageStat.Stat(
            edge_image
        ).mean[0]
    )

    score = (
        color_stddev
        + (
            texture_energy
            * 0.60
        )
    )

    return {
        "score": round(
            score,
            3,
        ),

        "color_stddev": round(
            color_stddev,
            3,
        ),

        "texture_energy": round(
            texture_energy,
            3,
        ),

        "artwork_like": bool(
            score >= 32.0
        ),
    }


def refine_bleed_strategy_from_pixels(
    source_image,
    strategy,
    printing_frame_key,
    corner_radius_px,
):
    result = {
        "profile": (
            strategy[
                "profile"
            ]
        ),

        "requires_outpaint": bool(
            strategy[
                "requires_outpaint"
            ]
        ),

        "edges": dict(
            strategy[
                "edges"
            ]
        ),

        "corners": dict(
            strategy[
                "corners"
            ]
        ),
    }

    clean_frame_key = str(
        printing_frame_key
        or ""
    ).strip().lower()

    # If Card Printing Frame already
    # identified a complex frame, trust it.
    if result[
        "requires_outpaint"
    ]:
        return (
            result,
            {}
        )

    # These historical frame families
    # are known conventional outer frames.
    if clean_frame_key in (
        KNOWN_ALWAYS_SYNTHETIC_FRAME_KEYS
    ):
        return (
            result,
            {}
        )

    analysis = {
        edge_name: (
            measure_edge_complexity(
                source_image,
                edge_name,
                corner_radius_px,
            )
        )
        for edge_name in (
            "top",
            "right",
            "bottom",
            "left",
        )
    }

    artwork_edges = {
        edge_name
        for (
            edge_name,
            edge_data,
        )
        in analysis.items()
        if edge_data[
            "artwork_like"
        ]
    }

    if (
        "top" in artwork_edges
        and "left" in artwork_edges
        and "right" in artwork_edges
    ):
        result[
            "profile"
        ] = (
            "borderless_detected"
        )

        result[
            "requires_outpaint"
        ] = True

        result[
            "edges"
        ] = {
            edge_name: (
                "generative"
                if edge_name
                in artwork_edges
                else "synthetic"
            )
            for edge_name
            in (
                "top",
                "right",
                "bottom",
                "left",
            )
        }

        result[
            "corners"
        ] = {
            "top_left": "generative",
            "top_right": "generative",

            "bottom_right": (
                "generative"
                if (
                    "bottom"
                    in artwork_edges
                    and "right"
                    in artwork_edges
                )
                else "synthetic"
            ),

            "bottom_left": (
                "generative"
                if (
                    "bottom"
                    in artwork_edges
                    and "left"
                    in artwork_edges
                )
                else "synthetic"
            ),
        }

    elif (
        "left" in artwork_edges
        and "right" in artwork_edges
    ):
        result[
            "profile"
        ] = (
            "extended_detected"
        )

        result[
            "requires_outpaint"
        ] = True

        result[
            "edges"
        ] = {
            "top": "synthetic",
            "right": "generative",
            "bottom": "synthetic",
            "left": "generative",
        }

        result[
            "corners"
        ] = {
            "top_left": "synthetic",
            "top_right": "synthetic",
            "bottom_right": "synthetic",
            "bottom_left": "synthetic",
        }

    return (
        result,
        analysis,
    )


def build_generated_bleed_derivative(
    *,
    source_path,
    output_path,
    printing_frame_key,
    printing_frame_name,
    corner_radius_pct,
    bleed_size_mm=DEFAULT_BLEED_MM,
):
    source_path = os.path.abspath(
        str(
            source_path
            or ""
        ).strip()
    )

    output_path = os.path.abspath(
        str(
            output_path
            or ""
        ).strip()
    )

    if (
        not source_path
        or not os.path.isfile(
            source_path
        )
    ):
        raise FileNotFoundError(
            "Upscaled source image "
            "does not exist: "
            f"{source_path}"
        )

    if not output_path:
        raise ValueError(
            "output_path is required."
        )

    with Image.open(
        source_path
    ) as source_file:
        source_image = (
            source_file.convert(
                "RGB"
            )
        )

    corner_radius_px = (
        calculate_corner_radius_pixels(
            source_image,
            corner_radius_pct,
        )
    )

    base_strategy = (
        resolve_bleed_strategy(
            printing_frame_key,
            printing_frame_name,
        )
    )

    (
        strategy,
        edge_analysis,
    ) = (
        refine_bleed_strategy_from_pixels(
            source_image,
            base_strategy,
            printing_frame_key,
            corner_radius_px,
        )
    )

    generative_sides = [
        edge_name
        for (
            edge_name,
            edge_mode,
        )
        in strategy[
            "edges"
        ].items()
        if edge_mode
        == "generative"
    ]

    generative_corners = [
        corner_name
        for (
            corner_name,
            corner_mode,
        )
        in strategy[
            "corners"
        ].items()
        if corner_mode
        == "generative"
    ]

    if strategy[
        "requires_outpaint"
    ]:
        return {
            "ok": True,

            "printing_frame_key": (
                str(
                    printing_frame_key
                    or ""
                ).strip()
            ),

            "printing_frame": (
                str(
                    printing_frame_name
                    or ""
                ).strip()
            ),

            "profile": (
                strategy[
                    "profile"
                ]
            ),

            "requires_outpaint": True,

            "edges": dict(
                strategy[
                    "edges"
                ]
            ),

            "corners": dict(
                strategy[
                    "corners"
                ]
            ),

            "generative_sides": (
                generative_sides
            ),

            "generative_corners": (
                generative_corners
            ),

            "edge_analysis": (
                edge_analysis
            ),

            "bleed_size_mm": float(
                bleed_size_mm
            ),

            "output_path": "",

            "attached": False,

            "method": (
                "outpainting_required"
            ),
        }

    # Conventional framed cards reach
    # this point and receive the fast
    # image-sampled synthetic bleed.
    #
    # For ordinary cards this becomes
    # the final bleed.
    #
    # For artwork-edge cards FLUX will
    # overwrite only the generative mask.
    render_result = (
        render_synthetic_fullbleed(
            source_image,
            bleed_size_mm,
            corner_radius_pct,
        )
    )

    output_directory = (
        os.path.dirname(
            output_path
        )
    )

    if output_directory:
        os.makedirs(
            output_directory,
            exist_ok=True,
        )

    render_result[
        "image"
    ].save(
        output_path,
        format="PNG",
    )



    return {
        "ok": True,

        "printing_frame_key": (
            str(
                printing_frame_key
                or ""
            ).strip()
        ),

        "printing_frame": (
            str(
                printing_frame_name
                or ""
            ).strip()
        ),

        "profile": (
            strategy[
                "profile"
            ]
        ),

        "requires_outpaint": bool(
            strategy[
                "requires_outpaint"
            ]
        ),

        "edges": dict(
            strategy[
                "edges"
            ]
        ),

        "corners": dict(
            strategy[
                "corners"
            ]
        ),

        "generative_sides": (
            generative_sides
        ),

        "generative_corners": (
            generative_corners
        ),

        "edge_analysis": (
            edge_analysis
        ),

        "bleed_size_mm": float(
            bleed_size_mm
        ),

        "output_path": (
            output_path
        ),

        "attached": (
            not strategy[
                "requires_outpaint"
            ]
        ),

        "method": (
            SYNTHETIC_BLEED_MODEL_ID
        ),

        "bleed_pixels_x": (
            render_result[
                "bleed_pixels_x"
            ]
        ),

        "bleed_pixels_y": (
            render_result[
                "bleed_pixels_y"
            ]
        ),

        "corner_radius_px": (
            render_result[
                "corner_radius_px"
            ]
        ),

        "edge_colors": (
            render_result[
                "edge_colors"
            ]
        ),
    }