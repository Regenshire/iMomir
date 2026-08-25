import os
import time

from PIL import Image

from processors.frame_profiles import resolve_frame_profile
from processors.magic_card import process_magic_card_ai_v1
from processors.rules_text import (
    image_to_tensor,
    measure_region,
    normalized_box_to_pixels,
    tensor_to_image,
)
from processors.rules_text_ai import build_feather_mask


PROCESSOR_ID = "magic_card_ai_v2"

DIFFERENCE_LOW = 0.018
DIFFERENCE_HIGH = 0.085
MAX_PROTECTION = 0.92

PROTECTED_REGION_CONFIG = {
    "title_box": {
        "label": "card_title",
        "ai_weight": 0.32,
        "color_protection": False,
        "feather_pixels": 12,
    },
    "mana_cost_box": {
        "label": "mana_cost",
        "ai_weight": 0.22,
        "color_protection": True,
        "feather_pixels": 10,
    },
    "type_line_box": {
        "label": "type_line",
        "ai_weight": 0.35,
        "color_protection": False,
        "feather_pixels": 10,
    },
    "rules_text_box": {
        "label": "rules_text",
        "ai_weight": 0.52,
        "color_protection": True,
        "feather_pixels": 18,
    },
    "power_toughness_box": {
        "label": "power_toughness",
        "ai_weight": 0.28,
        "color_protection": False,
        "feather_pixels": 10,
    },
    "bottom_text_box": {
        "label": "bottom_text",
        "ai_weight": 0.25,
        "color_protection": False,
        "feather_pixels": 10,
    },
}


def build_color_protection_mask(baseline_region):
    channel_max = baseline_region.max(
        dim=1,
        keepdim=True,
    ).values

    channel_min = baseline_region.min(
        dim=1,
        keepdim=True,
    ).values

    saturation = channel_max - channel_min

    return (
        (saturation - 0.10) / 0.24
    ).clamp(
        0.0,
        1.0,
    )


def build_structure_protected_region(
    torch,
    ai_region,
    baseline_region,
    *,
    ai_weight,
    color_protection,
):
    difference = (
        ai_region
        - baseline_region
    ).abs().mean(
        dim=1,
        keepdim=True,
    )

    protection_mask = (
        (
            difference
            - DIFFERENCE_LOW
        )
        / (
            DIFFERENCE_HIGH
            - DIFFERENCE_LOW
        )
    ).clamp(
        0.0,
        1.0,
    )

    color_mask = None

    if color_protection:
        color_mask = (
            build_color_protection_mask(
                baseline_region
            )
        )

        protection_mask = (
            torch.maximum(
                protection_mask,
                color_mask * 0.95,
            )
        )

    effective_ai_weight = (
        float(ai_weight)
        * (
            1.0
            - (
                protection_mask
                * MAX_PROTECTION
            )
        )
    )

    protected_region = (
        baseline_region
        * (
            1.0
            - effective_ai_weight
        )
        + ai_region
        * effective_ai_weight
    ).clamp(
        0.0,
        1.0,
    )

    metrics = {
        "mean_ai_bicubic_difference": round(
            float(
                difference.mean().item()
            ),
            7,
        ),

        "mean_protection": round(
            float(
                protection_mask.mean().item()
            ),
            7,
        ),

        "mean_effective_ai_weight": round(
            float(
                effective_ai_weight.mean().item()
            ),
            7,
        ),
    }

    if color_mask is not None:
        metrics[
            "mean_color_protection"
        ] = round(
            float(
                color_mask.mean().item()
            ),
            7,
        )

    return (
        protected_region,
        metrics,
    )


def apply_protected_region(
    torch,
    output_tensor,
    baseline_tensor,
    profile,
    region_key,
    region_config,
):
    normalized_box = profile.get(
        region_key
    )

    if not normalized_box:
        return None

    output_height = int(
        output_tensor.shape[2]
    )

    output_width = int(
        output_tensor.shape[3]
    )

    pixel_box = (
        normalized_box_to_pixels(
            normalized_box,
            output_width,
            output_height,
        )
    )

    left, top, right, bottom = (
        pixel_box
    )

    ai_region = output_tensor[
        :,
        :,
        top:bottom,
        left:right,
    ].clone()

    baseline_region = baseline_tensor[
        :,
        :,
        top:bottom,
        left:right,
    ]

    protected_region, metrics = (
        build_structure_protected_region(
            torch,
            ai_region,
            baseline_region,
            ai_weight=(
                region_config[
                    "ai_weight"
                ]
            ),
            color_protection=bool(
                region_config.get(
                    "color_protection",
                    False,
                )
            ),
        )
    )

    feather_mask = build_feather_mask(
        torch,
        int(
            ai_region.shape[2]
        ),
        int(
            ai_region.shape[3]
        ),
        ai_region.device,
        ai_region.dtype,
        feather_pixels=int(
            region_config.get(
                "feather_pixels",
                10,
            )
        ),
    )

    candidate_region = (
        ai_region
        * (
            1.0
            - feather_mask
        )
        + protected_region
        * feather_mask
    ).clamp(
        0.0,
        1.0,
    )

    output_tensor[
        :,
        :,
        top:bottom,
        left:right,
    ] = candidate_region

    return {
        "normalized_box": list(
            normalized_box
        ),

        "output_pixel_box": list(
            pixel_box
        ),

        "configured_ai_weight": (
            region_config[
                "ai_weight"
            ]
        ),

        "color_protection": bool(
            region_config.get(
                "color_protection",
                False,
            )
        ),

        "difference_low": (
            DIFFERENCE_LOW
        ),

        "difference_high": (
            DIFFERENCE_HIGH
        ),

        "max_protection": (
            MAX_PROTECTION
        ),

        **metrics,

        "ai_metrics": measure_region(
            torch,
            ai_region,
        ),

        "bicubic_metrics": measure_region(
            torch,
            baseline_region,
        ),

        "candidate_metrics": measure_region(
            torch,
            candidate_region,
        ),
    }


def process_magic_card_ai_v2(
    payload,
):
    # Run the existing proven v1
    # pipeline first.
    base_result = (
        process_magic_card_ai_v1(
            payload
        )
    )

    try:
        import torch

    except Exception as exc:
        raise RuntimeError(
            "PyTorch could not be loaded: "
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

    source_tensor = image_to_tensor(
        torch,
        source_image,
        device,
    )

    output_tensor = image_to_tensor(
        torch,
        output_image,
        device,
    )

    baseline_tensor = (
        torch.nn.functional.interpolate(
            source_tensor,
            size=(
                int(
                    output_tensor.shape[2]
                ),
                int(
                    output_tensor.shape[3]
                ),
            ),
            mode="bicubic",
            align_corners=False,
        )
        .clamp(
            0.0,
            1.0,
        )
    )

    profile = resolve_frame_profile(
        card
    )

    post_started_at = (
        time.perf_counter()
    )

    region_results = {}

    with torch.inference_mode():
        if profile:
            type_line = str(
                card.get(
                    "type_line",
                    "",
                )
                or ""
            ).lower()

            for (
                region_key,
                region_config,
            ) in (
                PROTECTED_REGION_CONFIG.items()
            ):
                if (
                    region_key
                    == "power_toughness_box"
                    and "creature"
                    not in type_line
                ):
                    continue

                region_result = (
                    apply_protected_region(
                        torch,
                        output_tensor,
                        baseline_tensor,
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
                    ] = (
                        region_result
                    )

        else:
            # Showcase / unsupported frames:
            # keep most AI improvement but
            # reduce hallucination risk.
            output_tensor = (
                baseline_tensor
                * 0.28
                + output_tensor
                * 0.72
            ).clamp(
                0.0,
                1.0,
            )

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

    final_image = tensor_to_image(
        torch,
        output_tensor,
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
        "magic_card_ai_v1_plus_"
        "structure_protection"
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
            "profile_id"
        )
        if profile
        else ""
    )

    base_result[
        "structure_protection"
    ] = {
        "profile_supported": bool(
            profile
        ),

        "fallback_ai_weight": (
            None
            if profile
            else 0.72
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
        "structure_protection"
    ] = (
        base_result[
            "structure_protection"
        ]
    )

    return base_result