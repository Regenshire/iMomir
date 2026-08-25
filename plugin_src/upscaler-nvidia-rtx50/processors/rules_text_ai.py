import os
import time

import torch

from spandrel import (
    ImageModelDescriptor,
    ModelLoader,
)

from model_runtime import (
    ensure_model_file,
)

from processors.frame_profiles import (
    resolve_frame_profile,
)

from processors.rules_text import (
    image_to_tensor,
    measure_region,
    normalized_box_to_pixels,
    tensor_to_image,
)


PROCESSOR_ID = (
    "rules_text_realesrgan_x2_v1"
)

RUNTIME_MODEL_ID = (
    "realesrgan_x2plus"
)

_RUNTIME_MODEL_CACHE = {}


def build_feather_mask(
    torch_module,
    height,
    width,
    device,
    dtype,
    feather_pixels=18,
):
    feather_pixels = max(
        0,
        int(feather_pixels),
    )

    if feather_pixels <= 0:
        return torch_module.ones(
            (
                1,
                1,
                height,
                width,
            ),
            device=device,
            dtype=dtype,
        )

    y_positions = torch_module.arange(
        height,
        device=device,
        dtype=dtype,
    )

    x_positions = torch_module.arange(
        width,
        device=device,
        dtype=dtype,
    )

    y_distance = torch_module.minimum(
        y_positions,
        (
            height
            - 1
            - y_positions
        ),
    )

    x_distance = torch_module.minimum(
        x_positions,
        (
            width
            - 1
            - x_positions
        ),
    )

    y_weight = (
        y_distance
        / float(
            feather_pixels
        )
    ).clamp(
        0.0,
        1.0,
    )

    x_weight = (
        x_distance
        / float(
            feather_pixels
        )
    ).clamp(
        0.0,
        1.0,
    )

    mask = (
        y_weight[:, None]
        * x_weight[None, :]
    )

    return mask.reshape(
        1,
        1,
        height,
        width,
    )

def load_runtime_model(
    device,
    model_id=None,
):
    runtime_model_id = (
        str(
            model_id
            or RUNTIME_MODEL_ID
        ).strip()
    )

    cache_key = (
        runtime_model_id,
        str(device),
    )

    cached_model = (
        _RUNTIME_MODEL_CACHE.get(
            cache_key
        )
    )

    if cached_model:
        return cached_model

    model_path = ensure_model_file(
        runtime_model_id
    )

    descriptor = (
        ModelLoader()
        .load_from_file(
            model_path
        )
    )

    if not isinstance(
        descriptor,
        ImageModelDescriptor,
    ):
        raise RuntimeError(
            "Upscale model did not load "
            "as an image model: "
            f"{runtime_model_id}"
        )

    descriptor = (
        descriptor
        .to(device)
        .eval()
    )

    cached_model = (
        descriptor,
        model_path,
    )

    _RUNTIME_MODEL_CACHE[
        cache_key
    ] = cached_model

    return cached_model


def process_rules_text_realesrgan(
    payload,
):
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
        blend_strength = float(
            payload.get(
                "ai_blend_strength",
                0.88,
            )
            or 0.88
        )

    except (
        TypeError,
        ValueError,
    ):
        blend_strength = 0.88

    blend_strength = max(
        0.0,
        min(
            blend_strength,
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

    from PIL import Image

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
        base_output = (
            torch.nn.functional.interpolate(
                input_tensor,
                scale_factor=2.0,
                mode="bicubic",
                align_corners=False,
            )
            .clamp(
                0.0,
                1.0,
            )
        )

        output_tensor = (
            base_output.clone()
        )

        region_supported = (
            profile is not None
        )

        rules_region_data = {
            "supported": (
                region_supported
            ),
        }

        model_path = ""

        if profile:
            source_box = (
                normalized_box_to_pixels(
                    profile[
                        "rules_text_box"
                    ],
                    input_width,
                    input_height,
                )
            )

            output_height = int(
                base_output.shape[2]
            )

            output_width = int(
                base_output.shape[3]
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
                source_left,
                source_top,
                source_right,
                source_bottom,
            ) = source_box

            (
                output_left,
                output_top,
                output_right,
                output_bottom,
            ) = output_box

            source_region = (
                input_tensor[
                    :,
                    :,
                    source_top:source_bottom,
                    source_left:source_right,
                ]
            )

            base_region = (
                base_output[
                    :,
                    :,
                    output_top:output_bottom,
                    output_left:output_right,
                ]
            )

            source_metrics = (
                measure_region(
                    torch,
                    source_region,
                )
            )

            base_metrics = (
                measure_region(
                    torch,
                    base_region,
                )
            )

            descriptor, model_path = (
                load_runtime_model(
                    device
                )
            )

            ai_region = descriptor(
                source_region
            ).clamp(
                0.0,
                1.0,
            )

            expected_height = int(
                base_region.shape[2]
            )

            expected_width = int(
                base_region.shape[3]
            )

            if (
                int(
                    ai_region.shape[2]
                )
                != expected_height
                or int(
                    ai_region.shape[3]
                )
                != expected_width
            ):
                ai_region = (
                    torch.nn.functional.interpolate(
                        ai_region,
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

            feather_mask = (
                build_feather_mask(
                    torch,
                    expected_height,
                    expected_width,
                    device,
                    base_region.dtype,
                    feather_pixels=18,
                )
            )

            blend_mask = (
                feather_mask
                * blend_strength
            )

            candidate_region = (
                base_region
                * (
                    1.0
                    - blend_mask
                )
                + ai_region
                * blend_mask
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
                "creature"
                in type_line
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

                "source_pixel_box": list(
                    source_box
                ),

                "output_pixel_box": list(
                    output_box
                ),

                "ai_blend_strength": (
                    blend_strength
                ),

                "runtime_model_id": (
                    RUNTIME_MODEL_ID
                ),

                "source_metrics": (
                    source_metrics
                ),

                "base_metrics": (
                    base_metrics
                ),

                "candidate_metrics": (
                    candidate_metrics
                ),
            }

        else:
            rules_region_data = {
                "supported": False,

                "fallback_reason": (
                    "No matching frame "
                    "profile."
                ),

                "frame_version": str(
                    card.get(
                        "frame_version",
                        "",
                    )
                    or ""
                ),

                "layout": str(
                    card.get(
                        "layout",
                        "",
                    )
                    or ""
                ),
            }

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

        "region_supported": (
            region_supported
        ),

        "runtime_model_id": (
            RUNTIME_MODEL_ID
        ),

        "runtime_model_filename": (
            os.path.basename(
                model_path
            )
            if model_path
            else ""
        ),

        "regions": {
            "rules_text": (
                rules_region_data
            ),
        },

        "technical_metrics": {
            "rules_text": (
                rules_region_data
            ),
        },
    }