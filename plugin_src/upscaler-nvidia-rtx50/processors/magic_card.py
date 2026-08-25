import os
import time

from PIL import Image

from processors.frame_profiles import (
    resolve_frame_profile,
)

from processors.rules_text import (
    enhance_rules_region,
    image_to_tensor,
    measure_region,
    normalized_box_to_pixels,
    tensor_to_image,
)

from processors.rules_text_ai import (
    RUNTIME_MODEL_ID,
    build_feather_mask,
    load_runtime_model,
)


PROCESSOR_ID = "magic_card_ai_v1"


def process_magic_card_ai_v1(payload):
    try:
        import torch

    except Exception as exc:
        raise RuntimeError(
            "PyTorch could not be loaded: "
            f"{type(exc).__name__}: {exc}"
        )

    if not torch.cuda.is_available():
        raise RuntimeError(
            "CUDA is not available."
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

    if not input_path:
        raise ValueError(
            "input_path is required."
        )

    if not output_path:
        raise ValueError(
            "output_path is required."
        )

    if not os.path.exists(
        input_path
    ):
        raise FileNotFoundError(
            "Input image was not found: "
            f"{input_path}"
        )

    card = payload.get(
        "card"
    )

    if not isinstance(
        card,
        dict,
    ):
        card = {}

    try:
        rules_strength = float(
            payload.get(
                "rules_strength",
                0.35,
            )
            or 0.35
        )

    except (
        TypeError,
        ValueError,
    ):
        rules_strength = 0.35

    rules_strength = max(
        0.0,
        min(
            rules_strength,
            1.0,
        ),
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

    device = torch.device(
        "cuda:0"
    )

    device_name = (
        torch.cuda.get_device_name(
            device
        )
    )

    device_capability = (
        torch.cuda.get_device_capability(
            device
        )
    )

    try:
        cudnn_version = (
            torch.backends.cudnn.version()
        )

    except Exception:
        cudnn_version = None

    torch.cuda.empty_cache()

    torch.cuda.reset_peak_memory_stats(
        device
    )

    with Image.open(
        input_path
    ) as source_file:
        source_image = (
            source_file.convert(
                "RGB"
            )
        )

    input_width = int(
        source_image.width
    )

    input_height = int(
        source_image.height
    )

    input_tensor = image_to_tensor(
        torch,
        source_image,
        device,
    )

    profile = resolve_frame_profile(
        card
    )

    torch.cuda.synchronize(
        device
    )

    started_at = (
        time.perf_counter()
    )

    with torch.inference_mode():
        descriptor, model_path = (
            load_runtime_model(
                device
            )
        )

        output_tensor = descriptor(
            input_tensor
        ).clamp(
            0.0,
            1.0,
        )

        expected_height = (
            input_height * 2
        )

        expected_width = (
            input_width * 2
        )

        if (
            int(
                output_tensor.shape[2]
            )
            != expected_height
            or int(
                output_tensor.shape[3]
            )
            != expected_width
        ):
            output_tensor = (
                torch.nn.functional.interpolate(
                    output_tensor,
                    size=(
                        expected_height,
                        expected_width,
                    ),
                    mode="bicubic",
                    align_corners=False,
                )
                .clamp(
                    0.0,
                    1.0,
                )
            )

        whole_card_source_metrics = (
            measure_region(
                torch,
                input_tensor,
            )
        )

        rules_region_data = {
            "supported": False,

            "fallback_reason": (
                "No matching frame profile."
            ),
        }

        if profile:
            output_width = int(
                output_tensor.shape[3]
            )

            output_height = int(
                output_tensor.shape[2]
            )

            output_box = (
                normalized_box_to_pixels(
                    profile[
                        "rules_text_box"
                    ],
                    output_width,
                    output_height,
                )
            )

            (
                output_left,
                output_top,
                output_right,
                output_bottom,
            ) = output_box

            base_region = (
                output_tensor[
                    :,
                    :,
                    output_top:output_bottom,
                    output_left:output_right,
                ]
            )

            base_metrics = (
                measure_region(
                    torch,
                    base_region,
                )
            )

            restored_region = (
                enhance_rules_region(
                    torch,
                    base_region,
                    rules_strength,
                )
            )

            region_height = int(
                base_region.shape[2]
            )

            region_width = int(
                base_region.shape[3]
            )

            feather_mask = (
                build_feather_mask(
                    torch,
                    region_height,
                    region_width,
                    device,
                    base_region.dtype,
                    feather_pixels=24,
                )
            )

            candidate_region = (
                base_region
                * (
                    1.0
                    - feather_mask
                )
                + restored_region
                * feather_mask
            ).clamp(
                0.0,
                1.0,
            )

            type_line = str(
                card.get(
                    "type_line",
                    "",
                )
                or ""
            ).lower()

            if (
                "creature" in type_line
                and profile.get(
                    "power_toughness_exclusion"
                )
            ):
                pt_box = (
                    normalized_box_to_pixels(
                        profile[
                            "power_toughness_exclusion"
                        ],
                        output_width,
                        output_height,
                    )
                )

                pt_left = max(
                    0,
                    pt_box[0]
                    - output_left,
                )

                pt_top = max(
                    0,
                    pt_box[1]
                    - output_top,
                )

                pt_right = min(
                    candidate_region.shape[3],
                    pt_box[2]
                    - output_left,
                )

                pt_bottom = min(
                    candidate_region.shape[2],
                    pt_box[3]
                    - output_top,
                )

                if (
                    pt_right > pt_left
                    and pt_bottom > pt_top
                ):
                    candidate_region[
                        :,
                        :,
                        pt_top:pt_bottom,
                        pt_left:pt_right,
                    ] = base_region[
                        :,
                        :,
                        pt_top:pt_bottom,
                        pt_left:pt_right,
                    ]

            output_tensor[
                :,
                :,
                output_top:output_bottom,
                output_left:output_right,
            ] = candidate_region

            candidate_metrics = (
                measure_region(
                    torch,
                    candidate_region,
                )
            )

            rules_region_data = {
                "supported": True,

                "profile_id": (
                    profile[
                        "profile_id"
                    ]
                ),

                "normalized_box": list(
                    profile[
                        "rules_text_box"
                    ]
                ),

                "output_pixel_box": list(
                    output_box
                ),

                "post_restore_strength": (
                    rules_strength
                ),

                "base_metrics": (
                    base_metrics
                ),

                "candidate_metrics": (
                    candidate_metrics
                ),
            }

        whole_card_candidate_metrics = (
            measure_region(
                torch,
                output_tensor,
            )
        )

    torch.cuda.synchronize(
        device
    )

    processing_ms = round(
        (
            time.perf_counter()
            - started_at
        )
        * 1000.0,
        2,
    )

    output_image = tensor_to_image(
        torch,
        output_tensor,
    )

    output_image.save(
        output_path,
        format="PNG",
    )

    peak_memory_mb = round(
        torch.cuda.max_memory_allocated(
            device
        )
        / (
            1024
            * 1024
        ),
        2,
    )

    return {
        "ok": True,

        "processor": (
            PROCESSOR_ID
        ),

        "base_model": (
            RUNTIME_MODEL_ID
        ),

        "device": (
            device_name
        ),

        "torch_version": str(
            torch.__version__
        ),

        "cuda_runtime_version": str(
            torch.version.cuda
            or ""
        ),

        "cudnn_version": (
            cudnn_version
        ),

        "compute_capability": (
            f"{device_capability[0]}."
            f"{device_capability[1]}"
        ),

        "input_path": (
            input_path
        ),

        "output_path": (
            output_path
        ),

        "input_width": (
            input_width
        ),

        "input_height": (
            input_height
        ),

        "output_width": int(
            output_image.width
        ),

        "output_height": int(
            output_image.height
        ),

        "scale": 2.0,

        "processing_ms": (
            processing_ms
        ),

        "peak_gpu_memory_mb": (
            peak_memory_mb
        ),

        "regions": {
            "rules_text": (
                rules_region_data
            ),
        },

        "technical_metrics": {
            "whole_card": {
                "source": (
                    whole_card_source_metrics
                ),

                "candidate": (
                    whole_card_candidate_metrics
                ),
            },

            "rules_text": (
                rules_region_data
            ),
        },

        "runtime_model_filename": (
            os.path.basename(
                model_path
            )
        ),
    }