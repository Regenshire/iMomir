import os
import time

from PIL import Image

from processors.frame_profiles import (
    resolve_frame_profile,
)

from processors.magic_card import (
    process_magic_card_ai_v1,
)

from processors.rules_text import (
    image_to_tensor,
    measure_region,
    normalized_box_to_pixels,
    tensor_to_image,
)

from processors.rules_text_ai import (
    build_feather_mask,
    load_runtime_model,
)


PROCESSOR_ID = (
    "magic_card_ai_v3"
)

TARGET_RUNTIME_MODEL_ID = (
    "realesrnet_x4plus"
)

TARGET_RUNTIME_SCALE = 4


TARGET_REGION_CONFIG = {
    "title_box": {
        "label": (
            "card_title"
        ),

        "target_weight": 0.70,

        "padding_pixels": 18,

        "feather_pixels": 12,
    },

    "mana_cost_box": {
        "label": (
            "mana_cost"
        ),

        "target_weight": 0.78,

        "padding_pixels": 16,

        "feather_pixels": 10,
    },

    "type_line_box": {
        "label": (
            "type_line"
        ),

        "target_weight": 0.68,

        "padding_pixels": 16,

        "feather_pixels": 10,
    },

    "rules_text_box": {
        "label": (
            "rules_text"
        ),

        "target_weight": 0.60,

        "padding_pixels": 22,

        "feather_pixels": 18,
    },

    "power_toughness_box": {
        "label": (
            "power_toughness"
        ),

        "target_weight": 0.72,

        "padding_pixels": 16,

        "feather_pixels": 10,
    },

    "bottom_text_box": {
        "label": (
            "bottom_text"
        ),

        "target_weight": 0.68,

        "padding_pixels": 16,

        "feather_pixels": 10,
    },

    "class_rules_intro_box": {
        "label": (
            "class_rules_intro"
        ),

        "target_weight": 0.64,

        "padding_pixels": 16,

        "feather_pixels": 10,
    },

    "class_level_2_header_box": {
        "label": (
            "class_level_2_header"
        ),

        "target_weight": 0.82,

        "padding_pixels": 14,

        "feather_pixels": 8,
    },

    "class_level_2_text_box": {
        "label": (
            "class_level_2_text"
        ),

        "target_weight": 0.66,

        "padding_pixels": 16,

        "feather_pixels": 10,
    },

    "class_level_3_header_box": {
        "label": (
            "class_level_3_header"
        ),

        "target_weight": 0.82,

        "padding_pixels": 14,

        "feather_pixels": 8,
    },

    "class_level_3_text_box": {
        "label": (
            "class_level_3_text"
        ),

        "target_weight": 0.64,

        "padding_pixels": 18,

        "feather_pixels": 10,
    },
}


def expand_pixel_box(
    pixel_box,
    width,
    height,
    padding_pixels,
):
    (
        left,
        top,
        right,
        bottom,
    ) = pixel_box

    padding_pixels = max(
        0,
        int(
            padding_pixels
        ),
    )

    return (
        max(
            0,
            left - padding_pixels,
        ),

        max(
            0,
            top - padding_pixels,
        ),

        min(
            width,
            right + padding_pixels,
        ),

        min(
            height,
            bottom + padding_pixels,
        ),
    )


def extract_inner_model_region(
    model_region,
    expanded_box,
    inner_box,
    model_scale,
):
    expanded_left = (
        expanded_box[0]
    )

    expanded_top = (
        expanded_box[1]
    )

    inner_left = int(
        round(
            (
                inner_box[0]
                - expanded_left
            )
            * model_scale
        )
    )

    inner_top = int(
        round(
            (
                inner_box[1]
                - expanded_top
            )
            * model_scale
        )
    )

    inner_right = int(
        round(
            (
                inner_box[2]
                - expanded_left
            )
            * model_scale
        )
    )

    inner_bottom = int(
        round(
            (
                inner_box[3]
                - expanded_top
            )
            * model_scale
        )
    )

    return model_region[
        :,
        :,
        inner_top:inner_bottom,
        inner_left:inner_right,
    ]


def route_target_region(
    torch,
    target_descriptor,
    source_tensor,
    v1_tensor,
    output_tensor,
    profile,
    region_key,
    region_config,
):
    normalized_box = (
        profile.get(
            region_key
        )
    )

    if not normalized_box:
        return None

    source_height = int(
        source_tensor.shape[2]
    )

    source_width = int(
        source_tensor.shape[3]
    )

    output_height = int(
        output_tensor.shape[2]
    )

    output_width = int(
        output_tensor.shape[3]
    )

    source_box = (
        normalized_box_to_pixels(
            normalized_box,
            source_width,
            source_height,
        )
    )

    output_box = (
        normalized_box_to_pixels(
            normalized_box,
            output_width,
            output_height,
        )
    )

    expanded_source_box = (
        expand_pixel_box(
            source_box,
            source_width,
            source_height,
            region_config.get(
                "padding_pixels",
                16,
            ),
        )
    )

    (
        expanded_left,
        expanded_top,
        expanded_right,
        expanded_bottom,
    ) = expanded_source_box

    source_crop = source_tensor[
        :,
        :,
        expanded_top:expanded_bottom,
        expanded_left:expanded_right,
    ]

    target_region = (
        target_descriptor(
            source_crop
        )
        .clamp(
            0.0,
            1.0,
        )
    )

    expected_target_height = (
        int(
            source_crop.shape[2]
        )
        * TARGET_RUNTIME_SCALE
    )

    expected_target_width = (
        int(
            source_crop.shape[3]
        )
        * TARGET_RUNTIME_SCALE
    )

    if (
        int(
            target_region.shape[2]
        )
        != expected_target_height
        or int(
            target_region.shape[3]
        )
        != expected_target_width
    ):
        target_region = (
            torch.nn.functional.interpolate(
                target_region,
                size=(
                    expected_target_height,
                    expected_target_width,
                ),
                mode="bicubic",
                align_corners=False,
            )
            .clamp(
                0.0,
                1.0,
            )
        )

    target_inner = (
        extract_inner_model_region(
            target_region,
            expanded_source_box,
            source_box,
            TARGET_RUNTIME_SCALE,
        )
    )

    (
        output_left,
        output_top,
        output_right,
        output_bottom,
    ) = output_box

    target_height = (
        output_bottom
        - output_top
    )

    target_width = (
        output_right
        - output_left
    )

    if (
        int(
            target_inner.shape[2]
        )
        != target_height
        or int(
            target_inner.shape[3]
        )
        != target_width
    ):
        target_inner = (
            torch.nn.functional.interpolate(
                target_inner,
                size=(
                    target_height,
                    target_width,
                ),
                mode="bicubic",
                align_corners=False,
                antialias=True,
            )
            .clamp(
                0.0,
                1.0,
            )
        )

    v1_region = v1_tensor[
        :,
        :,
        output_top:output_bottom,
        output_left:output_right,
    ]

    feather_mask = (
        build_feather_mask(
            torch,
            target_height,
            target_width,
            output_tensor.device,
            output_tensor.dtype,
            feather_pixels=int(
                region_config.get(
                    "feather_pixels",
                    10,
                )
            ),
        )
    )

    target_weight = max(
        0.0,
        min(
            float(
                region_config.get(
                    "target_weight",
                    0.65,
                )
            ),
            1.0,
        ),
    )

    blend_mask = (
        feather_mask
        * target_weight
    )

    candidate_region = (
        v1_region
        * (
            1.0
            - blend_mask
        )
        + target_inner
        * blend_mask
    ).clamp(
        0.0,
        1.0,
    )

    output_tensor[
        :,
        :,
        output_top:output_bottom,
        output_left:output_right,
    ] = candidate_region

    mean_target_v1_difference = (
        round(
            float(
                (
                    target_inner
                    - v1_region
                )
                .abs()
                .mean()
                .item()
            ),
            7,
        )
    )

    return {
        "normalized_box": list(
            normalized_box
        ),

        "source_pixel_box": list(
            source_box
        ),

        "expanded_source_pixel_box": (
            list(
                expanded_source_box
            )
        ),

        "output_pixel_box": list(
            output_box
        ),

        "target_model_id": (
            TARGET_RUNTIME_MODEL_ID
        ),

        "target_model_scale": (
            TARGET_RUNTIME_SCALE
        ),

        "target_weight": (
            target_weight
        ),

        "padding_pixels": int(
            region_config.get(
                "padding_pixels",
                16,
            )
        ),

        "feather_pixels": int(
            region_config.get(
                "feather_pixels",
                10,
            )
        ),

        "mean_target_v1_difference": (
            mean_target_v1_difference
        ),

        "v1_metrics": (
            measure_region(
                torch,
                v1_region,
            )
        ),

        "target_model_metrics": (
            measure_region(
                torch,
                target_inner,
            )
        ),

        "candidate_metrics": (
            measure_region(
                torch,
                candidate_region,
            )
        ),
    }


def process_magic_card_ai_v3(
    payload,
):
    # Start from v1 because v1 is
    # still our strongest overall
    # visual-quality baseline.
    base_result = (
        process_magic_card_ai_v1(
            payload
        )
    )

    try:
        import torch

    except Exception as exc:
        raise RuntimeError(
            "PyTorch could not be "
            "loaded: "
            f"{type(exc).__name__}: "
            f"{exc}"
        )

    input_path = os.path.abspath(
        str(
            payload.get(
                "input_path",
                "",
            )
            or ""
        ).strip()
    )

    output_path = os.path.abspath(
        str(
            payload.get(
                "output_path",
                "",
            )
            or ""
        ).strip()
    )

    card = payload.get(
        "card"
    )

    if not isinstance(
        card,
        dict,
    ):
        card = {}

    device = torch.device(
        "cuda:0"
    )

    profile = (
        resolve_frame_profile(
            card
        )
    )

    # Unsupported/showcase cards keep
    # the complete v1 result. We do not
    # reduce quality just because there
    # is no targeting profile.
    if not profile:
        base_result[
            "processor"
        ] = PROCESSOR_ID

        base_result[
            "strategy"
        ] = (
            "magic_card_ai_v1_"
            "fallback_no_target_profile"
        )

        base_result[
            "targeted_model_routing"
        ] = {
            "profile_supported": False,
            "regions": {},
        }

        return base_result

    with Image.open(
        input_path
    ) as source_file:
        source_image = (
            source_file.convert(
                "RGB"
            )
        )

    with Image.open(
        output_path
    ) as output_file:
        output_image = (
            output_file.convert(
                "RGB"
            )
        )

    source_tensor = (
        image_to_tensor(
            torch,
            source_image,
            device,
        )
    )

    output_tensor = (
        image_to_tensor(
            torch,
            output_image,
            device,
        )
    )

    # All targeted regions blend
    # against the exact same v1 output,
    # even when regions overlap.
    v1_tensor = (
        output_tensor.clone()
    )

    torch.cuda.empty_cache()

    post_started_at = (
        time.perf_counter()
    )

    with torch.inference_mode():
        (
            target_descriptor,
            target_model_path,
        ) = load_runtime_model(
            device,
            TARGET_RUNTIME_MODEL_ID,
        )

        region_results = {}

        type_line = str(
            card.get(
                "type_line",
                "",
            )
            or ""
        ).lower()

        configured_region_keys = (
            profile.get(
                "target_region_keys"
            )
        )

        if isinstance(
            configured_region_keys,
            (
                list,
                tuple,
            ),
        ):
            target_region_items = [
                (
                    region_key,
                    TARGET_REGION_CONFIG[
                        region_key
                    ],
                )
                for region_key
                in configured_region_keys
                if region_key
                in TARGET_REGION_CONFIG
            ]

        else:
            target_region_items = list(
                TARGET_REGION_CONFIG.items()
            )

        for (
            region_key,
            region_config,
        ) in target_region_items:
            if (
                region_key
                == "power_toughness_box"
                and "creature"
                not in type_line
            ):
                continue

            region_result = (
                route_target_region(
                    torch,
                    target_descriptor,
                    source_tensor,
                    v1_tensor,
                    output_tensor,
                    profile,
                    region_key,
                    region_config,
                )
            )

            if region_result:
                region_results[
                    region_config[
                        "label"
                    ]
                ] = region_result

    torch.cuda.synchronize(
        device
    )

    post_processing_ms = round(
        (
            time.perf_counter()
            - post_started_at
        )
        * 1000.0,
        2,
    )

    final_image = (
        tensor_to_image(
            torch,
            output_tensor,
        )
    )

    final_image.save(
        output_path,
        format="PNG",
    )

    base_result[
        "processor"
    ] = PROCESSOR_ID

    base_result[
        "strategy"
    ] = (
        "whole_card_realesrgan_x2_"
        "plus_targeted_realesrnet_x4"
    )

    base_result[
        "post_processing_ms"
    ] = (
        post_processing_ms
    )

    base_result[
        "processing_ms"
    ] = round(
        float(
            base_result.get(
                "processing_ms",
                0.0,
            )
            or 0.0
        )
        + post_processing_ms,
        2,
    )

    base_result[
        "frame_profile"
    ] = (
        profile.get(
            "profile_id",
            "",
        )
    )

    base_result[
        "targeted_model_routing"
    ] = {
        "profile_supported": True,

        "whole_card_model_id": (
            "realesrgan_x2plus"
        ),

        "target_model_id": (
            TARGET_RUNTIME_MODEL_ID
        ),

        "target_model_filename": (
            os.path.basename(
                target_model_path
            )
        ),

        "regions": (
            region_results
        ),
    }

    technical_metrics = (
        base_result.setdefault(
            "technical_metrics",
            {},
        )
    )

    technical_metrics[
        "targeted_model_routing"
    ] = (
        base_result[
            "targeted_model_routing"
        ]
    )

    return base_result