import os
import time

from PIL import (
    Image,
    ImageChops,
    ImageDraw,
    ImageFilter,
    ImageStat,
)


REPLACEMENT_NONE = "none"
REPLACEMENT_BACKGROUND = "background"

SUPPORTED_REPLACEMENTS = {
    REPLACEMENT_NONE,
    REPLACEMENT_BACKGROUND,
}

HOLOFOIL_SUPPORTED_FRAME_VERSIONS = {
    "2015",
}

STAMP_SEARCH_BOX = (
    0.410,
    0.885,
    0.590,
    0.958,
)

STAMP_COMPONENT_BOX = (
    0.435,
    0.890,
    0.565,
    0.955,
)

DIFFERENCE_THRESHOLD = 34
MIN_COMPONENT_PIXELS = 36
MAX_COMPONENT_COVERAGE = 0.62

CONTEXT_GAP_RATIO = 0.004
CONTEXT_WIDTH_RATIO = 0.025

RESTORATION_CONTEXT_WIDTH_RATIO = 0.012
DARK_CONTEXT_LUMINANCE_THRESHOLD = 72.0
DARK_CONTEXT_MIN_FRACTION = 0.25
DARK_CONTEXT_VERTICAL_RADIUS_RATIO = 0.002
TEXT_SAFE_BOTTOM_FRACTION = 0.20

TEXT_PRESERVE_BOTTOM_FRACTION = 0.45
TEXT_PRESERVE_LUMINANCE_THRESHOLD = 180.0
TEXT_PRESERVE_BACKGROUND_MAX_LUMINANCE = 110.0
TEXT_PRESERVE_MAX_CHANNEL_SPREAD = 24
TEXT_PRESERVE_DILATION_SIZE = 3

STAMP_SHAPE_UNKNOWN = "unknown"
STAMP_SHAPE_OVAL = "oval"
STAMP_SHAPE_TRIANGLE = "triangle"

SECURITY_STAMP_SHAPE_MAP = {
    "oval": STAMP_SHAPE_OVAL,
    "circle": STAMP_SHAPE_OVAL,
    "triangle": STAMP_SHAPE_TRIANGLE,
}

OVAL_MIDDLE_BULGE_RATIO = 1.28
TRIANGLE_BOTTOM_TAPER_RATIO = 0.70

OVAL_TARGET_WIDTH_RATIO = 0.110
OVAL_TARGET_HEIGHT_RATIO = 0.043

TRIANGLE_TARGET_WIDTH_RATIO = 0.120
TRIANGLE_TARGET_HEIGHT_RATIO = 0.064
TRIANGLE_SHOULDER_WIDTH_RATIO = 0.142
TRIANGLE_SHOULDER_HEIGHT_RATIO = 0.018

CANONICAL_TARGET_PAD_SIZE = 5

OVAL_MASK_EXPANSION_SIZE = 9
TRIANGLE_MASK_EXPANSION_SIZE = 39
TRIANGLE_SHADOW_PAD_SIZE = 9
TRIANGLE_SHADOW_UPPER_FRACTION = 0.60
MASK_FEATHER_RADIUS = 2.0


class HolofoilStampProcessor:
    def process(
        self,
        payload,
        result,
    ):
        if not isinstance(
            payload,
            dict,
        ):
            return result

        if (
            "holofoil_stamp_replacement"
            not in payload
        ):
            return result

        replacement = str(
            payload.get(
                "holofoil_stamp_replacement",
                REPLACEMENT_NONE,
            )
            or REPLACEMENT_NONE
        ).strip().lower()

        if (
            replacement
            not in SUPPORTED_REPLACEMENTS
        ):
            raise ValueError(
                "Unsupported holofoil stamp "
                "replacement mode: "
                f"{replacement}"
            )

        if (
            replacement
            == REPLACEMENT_NONE
        ):
            return result

        if not isinstance(
            result,
            dict,
        ):
            raise RuntimeError(
                "Holofoil stamp post-processing "
                "requires an Upscale result object."
            )

        card = (
            payload.get(
                "card"
            )
            or {}
        )

        if not isinstance(
            card,
            dict,
        ):
            card = {}

        frame_version = str(
            card.get(
                "frame_version",
                "",
            )
            or ""
        ).strip().lower()

        security_stamp = str(
            card.get(
                "security_stamp",
                "",
            )
            or ""
        ).strip().lower()

        if (
            frame_version
            not in HOLOFOIL_SUPPORTED_FRAME_VERSIONS
        ):
            result[
                "holofoil_stamp"
            ] = {
                "replacement": replacement,
                "detected": False,
                "background_restored": False,
                "normalized_box": None,
                "skipped": True,
                "skip_reason": (
                    "unsupported_frame_version"
                    if frame_version
                    else "missing_frame_version"
                ),
                "frame_version": frame_version,
                "processing_ms": 0.0,
            }

            return result

        output_path = (
            self._resolve_output_path(
                payload,
                result,
            )
        )

        started_at = (
            time.perf_counter()
        )

        with Image.open(
            output_path
        ) as output_file:
            output_image = (
                output_file.convert(
                    "RGB"
                )
            )

        (
            restored_image,
            normalized_box,
        ) = self._restore_background(
            output_image,
            security_stamp=security_stamp,
        )

        detected = (
            normalized_box
            is not None
        )

        if detected:
            restored_image.save(
                output_path,
                format="PNG",
            )

        result[
            "holofoil_stamp"
        ] = {
            "replacement": replacement,
            "detected": detected,
            "background_restored": detected,
            "normalized_box": (
                list(
                    normalized_box
                )
                if normalized_box
                else None
            ),
            "skipped": False,
            "skip_reason": "",
            "frame_version": frame_version,
            "security_stamp": security_stamp,
            "processing_ms": round(
                (
                    time.perf_counter()
                    - started_at
                )
                * 1000.0,
                2,
            ),
        }

        return result

    @staticmethod
    def _resolve_output_path(
        payload,
        result,
    ):
        output_path = os.path.abspath(
            str(
                result.get(
                    "output_path",
                    "",
                )
                or payload.get(
                    "output_path",
                    "",
                )
                or ""
            ).strip()
        )

        if not output_path:
            raise RuntimeError(
                "Holofoil stamp post-processing "
                "could not determine the Upscaled "
                "output path."
            )

        if not os.path.isfile(
            output_path
        ):
            raise FileNotFoundError(
                "Holofoil stamp post-processing "
                "could not find the Upscaled image: "
                f"{output_path}"
            )

        return output_path

    def _restore_background(
        self,
        image,
        security_stamp="",
    ):
        search_box = (
            self._normalized_box_to_pixels(
                image.size,
                STAMP_SEARCH_BOX,
            )
        )

        actual_patch = image.crop(
            search_box
        )

        expected_patch = (
            self._build_horizontal_background_patch(
                image,
                search_box,
            )
        )

        difference = ImageChops.difference(
            actual_patch,
            expected_patch,
        )

        (
            red_channel,
            green_channel,
            blue_channel,
        ) = difference.split()

        strongest_channel = (
            ImageChops.lighter(
                ImageChops.lighter(
                    red_channel,
                    green_channel,
                ),
                blue_channel,
            )
        )

        binary_mask = (
            strongest_channel.point(
                lambda value: (
                    255
                    if value
                    >= DIFFERENCE_THRESHOLD
                    else 0
                )
            ).filter(
                ImageFilter.MaxFilter(
                    5
                )
            )
        )

        component_box = (
            self._normalized_box_to_pixels(
                image.size,
                STAMP_COMPONENT_BOX,
            )
        )

        component_left = max(
            0,
            component_box[0]
            - search_box[0],
        )

        component_top = max(
            0,
            component_box[1]
            - search_box[1],
        )

        component_right = min(
            binary_mask.width,
            component_box[2]
            - search_box[0],
        )

        component_bottom = min(
            binary_mask.height,
            component_box[3]
            - search_box[1],
        )

        component_filter_mask = Image.new(
            "L",
            binary_mask.size,
            0,
        )

        if (
            component_right
            > component_left
            and component_bottom
            > component_top
        ):
            component_filter_mask.paste(
                binary_mask.crop(
                    (
                        component_left,
                        component_top,
                        component_right,
                        component_bottom,
                    )
                ),
                (
                    component_left,
                    component_top,
                ),
            )

        component_mask = (
            self._select_stamp_component(
                component_filter_mask,
                image.size,
                search_box,
            )
        )

        if component_mask is None:
            return (
                image,
                None,
            )

        stamp_shape = (
            self._resolve_stamp_shape(
                component_mask,
                security_stamp,
            )
        )

        canonical_target_mask = (
            self._build_canonical_stamp_target_mask(
                component_mask,
                image.size,
                stamp_shape,
            )
        )

        mask_expansion_size = (
            OVAL_MASK_EXPANSION_SIZE
        )

        if (
            stamp_shape
            == STAMP_SHAPE_TRIANGLE
        ):
            mask_expansion_size = (
                TRIANGLE_MASK_EXPANSION_SIZE
            )

        expanded_mask = (
            component_mask.filter(
                ImageFilter.MaxFilter(
                    mask_expansion_size
                )
            )
        )

        if (
            stamp_shape
            == STAMP_SHAPE_TRIANGLE
        ):
            triangle_box = (
                expanded_mask.getbbox()
            )

            if triangle_box:
                triangle_height = max(
                    1,
                    triangle_box[3]
                    - triangle_box[1],
                )

                shadow_cleanup_bottom = min(
                    expanded_mask.height,
                    triangle_box[1]
                    + max(
                        1,
                        int(
                            round(
                                triangle_height
                                * TRIANGLE_SHADOW_UPPER_FRACTION
                            )
                        ),
                    ),
                )

                shadow_cleanup_mask = (
                    expanded_mask.filter(
                        ImageFilter.MaxFilter(
                            TRIANGLE_SHADOW_PAD_SIZE
                        )
                    )
                )

                upper_shadow_mask = Image.new(
                    "L",
                    expanded_mask.size,
                    0,
                )

                upper_shadow_mask.paste(
                    shadow_cleanup_mask.crop(
                        (
                            0,
                            0,
                            expanded_mask.width,
                            shadow_cleanup_bottom,
                        )
                    ),
                    (
                        0,
                        0,
                    ),
                )

                expanded_mask = (
                    ImageChops.lighter(
                        expanded_mask,
                        upper_shadow_mask,
                    )
                )

        restoration_context_box = (
            expanded_mask.getbbox()
        )

        if not restoration_context_box:
            return (
                image,
                None,
            )

        expanded_mask = (
            ImageChops.lighter(
                expanded_mask,
                canonical_target_mask,
            )
        )

        component_box = (
            expanded_mask.getbbox()
        )

        if not component_box:
            return (
                image,
                None,
            )

        blurred_mask = (
            expanded_mask.filter(
                ImageFilter.GaussianBlur(
                    MASK_FEATHER_RADIUS
                )
            )
        )

        feathered_mask = (
            blurred_mask
        )

        if (
            stamp_shape
            == STAMP_SHAPE_TRIANGLE
        ):
            feathered_mask = (
                ImageChops.lighter(
                    expanded_mask,
                    blurred_mask,
                )
            )

        restoration_patch = (
            self._build_local_restoration_patch(
                image,
                search_box,
                restoration_context_box,
            )
        )

        if (
            stamp_shape
            == STAMP_SHAPE_TRIANGLE
        ):
            text_protection_mask = (
                self._build_footer_text_protection_mask(
                    actual_patch,
                    restoration_patch,
                    component_box,
                )
            )

            if text_protection_mask is not None:
                text_protection_mask = (
                    ImageChops.subtract(
                        text_protection_mask,
                        canonical_target_mask,
                    )
                )

                feathered_mask = (
                    ImageChops.subtract(
                        feathered_mask,
                        text_protection_mask,
                    )
                )

        restored_patch = Image.composite(
            restoration_patch,
            actual_patch,
            feathered_mask,
        )

        restored_image = image.copy()

        restored_image.paste(
            restored_patch,
            search_box[:2],
        )

        full_box = (
            search_box[0]
            + component_box[0],
            search_box[1]
            + component_box[1],
            search_box[0]
            + component_box[2],
            search_box[1]
            + component_box[3],
        )

        normalized_box = tuple(
            round(
                value,
                6,
            )
            for value
            in (
                full_box[0]
                / image.width,
                full_box[1]
                / image.height,
                full_box[2]
                / image.width,
                full_box[3]
                / image.height,
            )
        )

        return (
            restored_image,
            normalized_box,
        )

    def _build_local_restoration_patch(
        self,
        image,
        search_box,
        component_box,
    ):
        (
            search_left,
            search_top,
            search_right,
            search_bottom,
        ) = search_box

        component_left = (
            component_box[0]
        )

        component_top = (
            component_box[1]
        )

        component_right = (
            component_box[2]
        )

        component_bottom = (
            component_box[3]
        )

        component_height = max(
            1,
            component_bottom
            - component_top,
        )

        text_safe_start = max(
            component_top,
            component_bottom
            - max(
                1,
                int(
                    round(
                        component_height
                        * TEXT_SAFE_BOTTOM_FRACTION
                    )
                ),
            ),
        )

        patch_width = max(
            1,
            search_right - search_left,
        )

        patch_height = max(
            1,
            search_bottom - search_top,
        )

        full_component_left = (
            search_left
            + component_left
        )

        full_component_right = (
            search_left
            + component_right
        )

        gap_pixels = max(
            2,
            int(
                round(
                    image.width
                    * CONTEXT_GAP_RATIO
                )
            ),
        )

        context_width = max(
            8,
            int(
                round(
                    image.width
                    * RESTORATION_CONTEXT_WIDTH_RATIO
                )
            ),
        )

        left_context_right = max(
            1,
            full_component_left
            - gap_pixels,
        )

        left_context_left = max(
            0,
            left_context_right
            - context_width,
        )

        right_context_left = min(
            image.width - 1,
            full_component_right
            + gap_pixels,
        )

        right_context_right = min(
            image.width,
            right_context_left
            + context_width,
        )

        dark_vertical_radius = max(
            3,
            int(
                round(
                    image.height
                    * DARK_CONTEXT_VERTICAL_RADIUS_RATIO
                )
            ),
        )

        patch = Image.new(
            "RGB",
            (
                patch_width,
                patch_height,
            ),
        )

        patch_pixels = (
            patch.load()
        )

        component_denominator = max(
            1,
            component_right
            - component_left
            - 1,
        )

        for local_y in range(
            patch_height
        ):
            source_y = (
                search_top
                + local_y
            )

            sample_top = max(
                0,
                source_y - 1,
            )

            sample_bottom = min(
                image.height,
                source_y + 2,
            )

            left_color = self._median_color(
                image,
                (
                    left_context_left,
                    sample_top,
                    left_context_right,
                    sample_bottom,
                ),
                (
                    max(
                        0,
                        full_component_left - 1,
                    ),
                    source_y,
                ),
            )

            right_color = self._median_color(
                image,
                (
                    right_context_left,
                    sample_top,
                    right_context_right,
                    sample_bottom,
                ),
                (
                    min(
                        image.width - 1,
                        full_component_right,
                    ),
                    source_y,
                ),
            )

            left_luminance = (
                0.2126
                * left_color[0]
                + 0.7152
                * left_color[1]
                + 0.0722
                * left_color[2]
            )

            right_luminance = (
                0.2126
                * right_color[0]
                + 0.7152
                * right_color[1]
                + 0.0722
                * right_color[2]
            )

            use_text_safe_sampling = (
                local_y
                >= text_safe_start
            )

            if (
                use_text_safe_sampling
                or (
                    left_luminance
                    <= DARK_CONTEXT_LUMINANCE_THRESHOLD
                    and right_luminance
                    <= DARK_CONTEXT_LUMINANCE_THRESHOLD
                )
            ):
                stable_top = max(
                    0,
                    source_y
                    - dark_vertical_radius,
                )

                stable_bottom = min(
                    image.height,
                    source_y
                    + dark_vertical_radius
                    + 1,
                )

                left_dark_color = (
                    self._dark_background_color(
                        image,
                        (
                            left_context_left,
                            stable_top,
                            left_context_right,
                            stable_bottom,
                        ),
                    )
                )

                right_dark_color = (
                    self._dark_background_color(
                        image,
                        (
                            right_context_left,
                            stable_top,
                            right_context_right,
                            stable_bottom,
                        ),
                    )
                )

                if (
                    left_dark_color
                    is not None
                    and right_dark_color
                    is not None
                ):
                    left_color = (
                        left_dark_color
                    )

                    right_color = (
                        right_dark_color
                    )

            for local_x in range(
                patch_width
            ):
                ratio = (
                    local_x
                    - component_left
                ) / component_denominator

                ratio = max(
                    0.0,
                    min(
                        1.0,
                        ratio,
                    ),
                )

                patch_pixels[
                    local_x,
                    local_y,
                ] = tuple(
                    int(
                        round(
                            left_color[
                                channel
                            ]
                            + (
                                right_color[
                                    channel
                                ]
                                - left_color[
                                    channel
                                ]
                            )
                            * ratio
                        )
                    )
                    for channel
                    in range(3)
                )

        return patch

    def _build_horizontal_background_patch(
        self,
        image,
        search_box,
    ):
        (
            left,
            top,
            right,
            bottom,
        ) = search_box

        patch_width = max(
            1,
            right - left,
        )

        patch_height = max(
            1,
            bottom - top,
        )

        gap_pixels = max(
            2,
            int(
                round(
                    image.width
                    * CONTEXT_GAP_RATIO
                )
            ),
        )

        context_width = max(
            8,
            int(
                round(
                    image.width
                    * CONTEXT_WIDTH_RATIO
                )
            ),
        )

        left_context_right = max(
            1,
            left - gap_pixels,
        )

        left_context_left = max(
            0,
            left_context_right
            - context_width,
        )

        right_context_left = min(
            image.width - 1,
            right + gap_pixels,
        )

        right_context_right = min(
            image.width,
            right_context_left
            + context_width,
        )

        patch = Image.new(
            "RGB",
            (
                patch_width,
                patch_height,
            ),
        )

        patch_pixels = patch.load()

        denominator = max(
            1,
            patch_width - 1,
        )

        for local_y in range(
            patch_height
        ):
            source_y = (
                top
                + local_y
            )

            sample_top = max(
                0,
                source_y - 1,
            )

            sample_bottom = min(
                image.height,
                source_y + 2,
            )

            left_color = self._median_color(
                image,
                (
                    left_context_left,
                    sample_top,
                    left_context_right,
                    sample_bottom,
                ),
                (
                    max(
                        0,
                        left - 1,
                    ),
                    source_y,
                ),
            )

            right_color = self._median_color(
                image,
                (
                    right_context_left,
                    sample_top,
                    right_context_right,
                    sample_bottom,
                ),
                (
                    min(
                        image.width - 1,
                        right,
                    ),
                    source_y,
                ),
            )

            for local_x in range(
                patch_width
            ):
                ratio = (
                    local_x
                    / denominator
                )

                patch_pixels[
                    local_x,
                    local_y,
                ] = tuple(
                    int(
                        round(
                            left_color[
                                channel
                            ]
                            + (
                                right_color[
                                    channel
                                ]
                                - left_color[
                                    channel
                                ]
                            )
                            * ratio
                        )
                    )
                    for channel
                    in range(3)
                )

        return patch

    @staticmethod
    def _median_color(
        image,
        box,
        fallback_xy,
    ):
        (
            left,
            top,
            right,
            bottom,
        ) = box

        if (
            right <= left
            or bottom <= top
        ):
            return image.getpixel(
                fallback_xy
            )[:3]

        statistics = ImageStat.Stat(
            image.crop(
                box
            )
        )

        return tuple(
            int(
                round(
                    value
                )
            )
            for value
            in statistics.median[:3]
        )

    @staticmethod
    def _build_canonical_stamp_target_mask(
        component_mask,
        full_image_size,
        stamp_shape,
    ):
        component_box = (
            component_mask.getbbox()
        )

        target_mask = Image.new(
            "L",
            component_mask.size,
            0,
        )

        if not component_box:
            return target_mask

        (
            component_left,
            component_top,
            component_right,
            component_bottom,
        ) = component_box

        center_x = (
            target_mask.width
            / 2.0
        )

        center_y = (
            component_top
            + component_bottom
        ) / 2.0

        full_width = max(
            1,
            full_image_size[0],
        )

        full_height = max(
            1,
            full_image_size[1],
        )

        if (
            stamp_shape
            == STAMP_SHAPE_OVAL
        ):
            target_width = max(
                1,
                int(
                    round(
                        full_width
                        * OVAL_TARGET_WIDTH_RATIO
                    )
                ),
            )

            target_height = max(
                1,
                int(
                    round(
                        full_height
                        * OVAL_TARGET_HEIGHT_RATIO
                    )
                ),
            )

        elif (
            stamp_shape
            == STAMP_SHAPE_TRIANGLE
        ):
            target_width = max(
                1,
                int(
                    round(
                        full_width
                        * TRIANGLE_TARGET_WIDTH_RATIO
                    )
                ),
            )

            target_height = max(
                1,
                int(
                    round(
                        full_height
                        * TRIANGLE_TARGET_HEIGHT_RATIO
                    )
                ),
            )

        else:
            return component_mask.copy()

        left = max(
            0,
            int(
                round(
                    center_x
                    - target_width / 2.0
                )
            ),
        )

        top = max(
            0,
            int(
                round(
                    center_y
                    - target_height / 2.0
                )
            ),
        )

        right = min(
            target_mask.width,
            left + target_width,
        )

        bottom = min(
            target_mask.height,
            top + target_height,
        )

        if (
            right <= left
            or bottom <= top
        ):
            return target_mask

        draw = ImageDraw.Draw(
            target_mask
        )

        if (
            stamp_shape
            == STAMP_SHAPE_OVAL
        ):
            draw.ellipse(
                (
                    left,
                    top,
                    right - 1,
                    bottom - 1,
                ),
                fill=255,
            )

        else:
            shoulder_width = max(
                target_width,
                int(
                    round(
                        full_width
                        * TRIANGLE_SHOULDER_WIDTH_RATIO
                    )
                ),
            )

            shoulder_height = max(
                1,
                int(
                    round(
                        full_height
                        * TRIANGLE_SHOULDER_HEIGHT_RATIO
                    )
                ),
            )

            shoulder_left = max(
                0,
                int(
                    round(
                        center_x
                        - shoulder_width / 2.0
                    )
                ),
            )

            shoulder_right = min(
                target_mask.width,
                int(
                    round(
                        center_x
                        + shoulder_width / 2.0
                    )
                ),
            )

            shoulder_bottom = min(
                bottom - 1,
                top + shoulder_height,
            )

            draw.polygon(
                (
                    (
                        shoulder_left,
                        top,
                    ),
                    (
                        shoulder_right - 1,
                        top,
                    ),
                    (
                        right - 1,
                        shoulder_bottom,
                    ),
                    (
                        int(
                            round(
                                center_x
                            )
                        ),
                        bottom - 1,
                    ),
                    (
                        left,
                        shoulder_bottom,
                    ),
                ),
                fill=255,
            )

        return target_mask.filter(
            ImageFilter.MaxFilter(
                CANONICAL_TARGET_PAD_SIZE
            )
        )

    @staticmethod
    def _build_footer_text_protection_mask(
        actual_patch,
        restoration_patch,
        component_box,
    ):
        (
            component_left,
            component_top,
            component_right,
            component_bottom,
        ) = component_box

        component_height = max(
            1,
            component_bottom
            - component_top,
        )

        protect_start = max(
            component_top,
            component_bottom
            - max(
                1,
                int(
                    round(
                        component_height
                        * TEXT_PRESERVE_BOTTOM_FRACTION
                    )
                ),
            ),
        )

        protect_end = min(
            actual_patch.height,
            component_bottom + 2,
        )

        protect_left = max(
            0,
            component_left - 2,
        )

        protect_right = min(
            actual_patch.width,
            component_right + 2,
        )

        if (
            protect_right
            <= protect_left
            or protect_end
            <= protect_start
        ):
            return None

        actual_pixels = (
            actual_patch.load()
        )

        restoration_pixels = (
            restoration_patch.load()
        )

        protection_mask = Image.new(
            "L",
            actual_patch.size,
            0,
        )

        protection_pixels = (
            protection_mask.load()
        )

        for y in range(
            protect_start,
            protect_end,
        ):
            for x in range(
                protect_left,
                protect_right,
            ):
                actual_pixel = (
                    actual_pixels[
                        x,
                        y,
                    ]
                )

                restoration_pixel = (
                    restoration_pixels[
                        x,
                        y,
                    ]
                )

                actual_luminance = (
                    0.2126
                    * actual_pixel[0]
                    + 0.7152
                    * actual_pixel[1]
                    + 0.0722
                    * actual_pixel[2]
                )

                restoration_luminance = (
                    0.2126
                    * restoration_pixel[0]
                    + 0.7152
                    * restoration_pixel[1]
                    + 0.0722
                    * restoration_pixel[2]
                )

                channel_spread = (
                    max(
                        actual_pixel[:3]
                    )
                    - min(
                        actual_pixel[:3]
                    )
                )

                if (
                    actual_luminance
                    >= TEXT_PRESERVE_LUMINANCE_THRESHOLD
                    and restoration_luminance
                    <= TEXT_PRESERVE_BACKGROUND_MAX_LUMINANCE
                    and channel_spread
                    <= TEXT_PRESERVE_MAX_CHANNEL_SPREAD
                ):
                    protection_pixels[
                        x,
                        y,
                    ] = 255

        if not protection_mask.getbbox():
            return None

        protection_mask = (
            protection_mask.filter(
                ImageFilter.MaxFilter(
                    TEXT_PRESERVE_DILATION_SIZE
                )
            )
        )

        clip_mask = Image.new(
            "L",
            actual_patch.size,
            0,
        )

        clip_mask.paste(
            255,
            (
                protect_left,
                protect_start,
                protect_right,
                protect_end,
            ),
        )

        return ImageChops.darker(
            protection_mask,
            clip_mask,
        )

    @staticmethod
    def _dark_background_color(
        image,
        box,
    ):
        (
            left,
            top,
            right,
            bottom,
        ) = box

        if (
            right <= left
            or bottom <= top
        ):
            return None

        region = image.crop(
            box
        )

        pixels = list(
            region.getdata()
        )

        if not pixels:
            return None

        dark_pixels = []

        for pixel in pixels:
            luminance = (
                0.2126
                * pixel[0]
                + 0.7152
                * pixel[1]
                + 0.0722
                * pixel[2]
            )

            if (
                luminance
                <= DARK_CONTEXT_LUMINANCE_THRESHOLD
            ):
                dark_pixels.append(
                    pixel[:3]
                )

        minimum_dark_pixels = max(
            1,
            int(
                round(
                    len(
                        pixels
                    )
                    * DARK_CONTEXT_MIN_FRACTION
                )
            ),
        )

        if (
            len(
                dark_pixels
            )
            < minimum_dark_pixels
        ):
            return None

        median_color = []

        for channel in range(3):
            channel_values = sorted(
                pixel[
                    channel
                ]
                for pixel
                in dark_pixels
            )

            value_count = len(
                channel_values
            )

            middle_index = (
                value_count
                // 2
            )

            if (
                value_count
                % 2
            ):
                median_value = (
                    channel_values[
                        middle_index
                    ]
                )

            else:
                median_value = (
                    channel_values[
                        middle_index - 1
                    ]
                    + channel_values[
                        middle_index
                    ]
                ) / 2.0

            median_color.append(
                int(
                    round(
                        median_value
                    )
                )
            )

        return tuple(
            median_color
        )

    @staticmethod
    def _resolve_stamp_shape(
        component_mask,
        security_stamp,
    ):
        metadata_shape = (
            SECURITY_STAMP_SHAPE_MAP.get(
                str(
                    security_stamp
                    or ""
                ).strip().lower()
            )
        )

        if metadata_shape:
            return metadata_shape

        return HolofoilStampProcessor._classify_stamp_shape(
            component_mask
        )

    @staticmethod
    def _classify_stamp_shape(
        component_mask,
    ):
        component_box = (
            component_mask.getbbox()
        )

        if not component_box:
            return STAMP_SHAPE_UNKNOWN

        component_region = (
            component_mask.crop(
                component_box
            )
        )

        region_pixels = (
            component_region.load()
        )

        row_widths = []

        for y in range(
            component_region.height
        ):
            row_width = 0

            for x in range(
                component_region.width
            ):
                if (
                    region_pixels[
                        x,
                        y,
                    ]
                    > 0
                ):
                    row_width += 1

            row_widths.append(
                row_width
            )

        if not row_widths:
            return STAMP_SHAPE_UNKNOWN

        quarter_size = max(
            1,
            len(
                row_widths
            )
            // 4,
        )

        top_width = (
            sum(
                row_widths[
                    :quarter_size
                ]
            )
            / quarter_size
        )

        middle_start = (
            len(
                row_widths
            )
            // 3
        )

        middle_end = max(
            middle_start + 1,
            (
                len(
                    row_widths
                )
                * 2
            )
            // 3,
        )

        middle_rows = (
            row_widths[
                middle_start:
                middle_end
            ]
        )

        middle_width = (
            sum(
                middle_rows
            )
            / max(
                1,
                len(
                    middle_rows
                ),
            )
        )

        bottom_width = (
            sum(
                row_widths[
                    -quarter_size:
                ]
            )
            / quarter_size
        )

        if (
            top_width
            > 0
            and bottom_width
            <= (
                top_width
                * TRIANGLE_BOTTOM_TAPER_RATIO
            )
        ):
            return STAMP_SHAPE_TRIANGLE

        shoulder_width = max(
            1.0,
            top_width,
            bottom_width,
        )

        if (
            middle_width
            >= (
                shoulder_width
                * OVAL_MIDDLE_BULGE_RATIO
            )
        ):
            return STAMP_SHAPE_OVAL

        return STAMP_SHAPE_TRIANGLE

    @staticmethod
    def _select_stamp_component(
        binary_mask,
        full_image_size,
        search_box,
    ):
        (
            width,
            height,
        ) = binary_mask.size

        source_pixels = (
            binary_mask.load()
        )

        visited = bytearray(
            width
            * height
        )

        full_width = (
            full_image_size[0]
        )

        full_height = (
            full_image_size[1]
        )

        expected_center_x = (
            full_width
            * 0.5
        )

        expected_center_y = (
            full_height
            * 0.925
        )

        maximum_area = (
            width
            * height
            * MAX_COMPONENT_COVERAGE
        )

        best_component = None
        best_score = None

        for y in range(
            height
        ):
            for x in range(
                width
            ):
                index = (
                    y
                    * width
                    + x
                )

                if (
                    visited[index]
                    or source_pixels[
                        x,
                        y,
                    ] == 0
                ):
                    continue

                stack = [
                    (
                        x,
                        y,
                    )
                ]

                visited[index] = 1
                component = []

                while stack:
                    (
                        current_x,
                        current_y,
                    ) = stack.pop()

                    component.append(
                        (
                            current_x,
                            current_y,
                        )
                    )

                    for (
                        next_x,
                        next_y,
                    ) in (
                        (
                            current_x - 1,
                            current_y,
                        ),
                        (
                            current_x + 1,
                            current_y,
                        ),
                        (
                            current_x,
                            current_y - 1,
                        ),
                        (
                            current_x,
                            current_y + 1,
                        ),
                    ):
                        if not (
                            0
                            <= next_x
                            < width
                            and 0
                            <= next_y
                            < height
                        ):
                            continue

                        next_index = (
                            next_y
                            * width
                            + next_x
                        )

                        if (
                            visited[
                                next_index
                            ]
                            or source_pixels[
                                next_x,
                                next_y,
                            ] == 0
                        ):
                            continue

                        visited[
                            next_index
                        ] = 1

                        stack.append(
                            (
                                next_x,
                                next_y,
                            )
                        )

                area = len(
                    component
                )

                if (
                    area
                    < MIN_COMPONENT_PIXELS
                    or area
                    > maximum_area
                ):
                    continue

                min_x = min(
                    point[0]
                    for point
                    in component
                )

                max_x = max(
                    point[0]
                    for point
                    in component
                )

                min_y = min(
                    point[1]
                    for point
                    in component
                )

                max_y = max(
                    point[1]
                    for point
                    in component
                )

                full_left = (
                    search_box[0]
                    + min_x
                )

                full_top = (
                    search_box[1]
                    + min_y
                )

                full_right = (
                    search_box[0]
                    + max_x
                    + 1
                )

                full_bottom = (
                    search_box[1]
                    + max_y
                    + 1
                )

                component_width = (
                    full_right
                    - full_left
                )

                component_height = (
                    full_bottom
                    - full_top
                )

                normalized_width = (
                    component_width
                    / full_width
                )

                normalized_height = (
                    component_height
                    / full_height
                )

                if not (
                    0.025
                    <= normalized_width
                    <= 0.145
                ):
                    continue

                if not (
                    0.012
                    <= normalized_height
                    <= 0.070
                ):
                    continue

                component_center_x = (
                    full_left
                    + full_right
                ) / 2.0

                component_center_y = (
                    full_top
                    + full_bottom
                ) / 2.0

                normalized_center_x = (
                    component_center_x
                    / full_width
                )

                normalized_center_y = (
                    component_center_y
                    / full_height
                )

                if not (
                    0.455
                    <= normalized_center_x
                    <= 0.545
                ):
                    continue

                if not (
                    0.895
                    <= normalized_center_y
                    <= 0.953
                ):
                    continue

                horizontal_distance = abs(
                    component_center_x
                    - expected_center_x
                ) / max(
                    1.0,
                    full_width
                    * 0.05,
                )

                vertical_distance = abs(
                    component_center_y
                    - expected_center_y
                ) / max(
                    1.0,
                    full_height
                    * 0.04,
                )

                score = (
                    area
                    / (
                        1.0
                        + horizontal_distance
                        + vertical_distance
                    )
                )

                if (
                    best_score is None
                    or score
                    > best_score
                ):
                    best_score = score
                    best_component = component

        if not best_component:
            return None

        component_mask = Image.new(
            "L",
            (
                width,
                height,
            ),
            0,
        )

        component_pixels = (
            component_mask.load()
        )

        for (
            x,
            y,
        ) in best_component:
            component_pixels[
                x,
                y,
            ] = 255

        return component_mask

    @staticmethod
    def _normalized_box_to_pixels(
        image_size,
        normalized_box,
    ):
        (
            width,
            height,
        ) = image_size

        left = max(
            0,
            min(
                width - 1,
                int(
                    round(
                        normalized_box[0]
                        * width
                    )
                ),
            ),
        )

        top = max(
            0,
            min(
                height - 1,
                int(
                    round(
                        normalized_box[1]
                        * height
                    )
                ),
            ),
        )

        right = max(
            left + 1,
            min(
                width,
                int(
                    round(
                        normalized_box[2]
                        * width
                    )
                ),
            ),
        )

        bottom = max(
            top + 1,
            min(
                height,
                int(
                    round(
                        normalized_box[3]
                        * height
                    )
                ),
            ),
        )

        return (
            left,
            top,
            right,
            bottom,
        )


HOLOFOIL_STAMP_PROCESSOR = (
    HolofoilStampProcessor()
)


def apply_holofoil_stamp_postprocess(
    payload,
    result,
):
    return HOLOFOIL_STAMP_PROCESSOR.process(
        payload,
        result,
    )