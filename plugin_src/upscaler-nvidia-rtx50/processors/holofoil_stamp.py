import os
import time

from PIL import (
    Image,
    ImageChops,
    ImageFilter,
    ImageStat,
)


REPLACEMENT_NONE = "none"
REPLACEMENT_BACKGROUND = "background"

SUPPORTED_REPLACEMENTS = {
    REPLACEMENT_NONE,
    REPLACEMENT_BACKGROUND,
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

MASK_EXPANSION_SIZE = 9
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
            output_image
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

        expanded_mask = (
            component_mask.filter(
                ImageFilter.MaxFilter(
                    MASK_EXPANSION_SIZE
                )
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

        feathered_mask = (
            expanded_mask.filter(
                ImageFilter.GaussianBlur(
                    MASK_FEATHER_RADIUS
                )
            )
        )

        restored_patch = Image.composite(
            expected_patch,
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