import os
import time

from PIL import Image

from processors.frame_profiles import (
    resolve_frame_profile,
)


PROCESSOR_ID = (
    "rules_text_restore_v1"
)


def normalized_box_to_pixels(
    normalized_box,
    width,
    height,
):
    x, y, box_width, box_height = (
        normalized_box
    )

    left = int(
        round(
            x * width
        )
    )

    top = int(
        round(
            y * height
        )
    )

    right = int(
        round(
            (
                x
                + box_width
            )
            * width
        )
    )

    bottom = int(
        round(
            (
                y
                + box_height
            )
            * height
        )
    )

    left = max(
        0,
        min(
            left,
            width,
        ),
    )

    top = max(
        0,
        min(
            top,
            height,
        ),
    )

    right = max(
        left + 1,
        min(
            right,
            width,
        ),
    )

    bottom = max(
        top + 1,
        min(
            bottom,
            height,
        ),
    )

    return (
        left,
        top,
        right,
        bottom,
    )


def image_to_tensor(
    torch,
    image,
    device,
):
    image = image.convert(
        "RGB"
    )

    width = int(
        image.width
    )

    height = int(
        image.height
    )

    raw_bytes = bytearray(
        image.tobytes()
    )

    tensor = torch.frombuffer(
        raw_bytes,
        dtype=torch.uint8,
    )

    tensor = tensor.reshape(
        height,
        width,
        3,
    )

    tensor = (
        tensor
        .permute(
            2,
            0,
            1,
        )
        .unsqueeze(0)
        .contiguous()
    )

    tensor = (
        tensor
        .to(
            device=device,
            dtype=torch.float32,
        )
        / 255.0
    )

    return tensor


def tensor_to_image(
    torch,
    tensor,
):
    output_tensor = (
        tensor
        .squeeze(0)
        .permute(
            1,
            2,
            0,
        )
        .mul(255.0)
        .round()
        .clamp(
            0,
            255,
        )
        .to(
            dtype=torch.uint8
        )
        .cpu()
        .contiguous()
    )

    return Image.fromarray(
        output_tensor.numpy()
    )


def get_luminance(
    tensor,
):
    return (
        tensor[:, 0:1] * 0.299
        + tensor[:, 1:2] * 0.587
        + tensor[:, 2:3] * 0.114
    )


def measure_region(
    torch,
    region,
):
    if (
        region is None
        or region.numel() == 0
    ):
        return {}

    luminance = (
        get_luminance(
            region
        )
    )

    local_mean = (
        torch.nn.functional.avg_pool2d(
            luminance,
            kernel_size=5,
            stride=1,
            padding=2,
        )
    )

    detail = (
        luminance
        - local_mean
    )

    contrast = float(
        luminance.std().item()
    )

    edge_strength = float(
        detail.abs().mean().item()
    )

    dark_detail_density = float(
        (
            detail
            < -0.025
        )
        .float()
        .mean()
        .item()
    )

    return {
        "contrast_stddev": round(
            contrast,
            7,
        ),

        "edge_strength_mean": round(
            edge_strength,
            7,
        ),

        "dark_detail_density": round(
            dark_detail_density,
            7,
        ),
    }


def enhance_rules_region(
    torch,
    region,
    strength,
):
    luminance = (
        get_luminance(
            region
        )
    )

    local_mean = (
        torch.nn.functional.avg_pool2d(
            luminance,
            kernel_size=9,
            stride=1,
            padding=4,
        )
    )

    local_detail = (
        luminance
        - local_mean
    )

    laplacian_kernel = (
        torch.tensor(
            [
                [
                    0.0,
                    -1.0,
                    0.0,
                ],
                [
                    -1.0,
                    4.0,
                    -1.0,
                ],
                [
                    0.0,
                    -1.0,
                    0.0,
                ],
            ],
            dtype=region.dtype,
            device=region.device,
        )
        .reshape(
            1,
            1,
            3,
            3,
        )
    )

    laplacian = (
        torch.nn.functional.conv2d(
            luminance,
            laplacian_kernel,
            padding=1,
        )
    )

    # Dark text strokes are usually
    # darker than their local rules-box
    # background.
    dark_stroke_mask = (
        (
            local_mean
            - luminance
        )
        * 9.0
    ).clamp(
        0.0,
        1.0,
    )

    edge_mask = (
        (
            local_detail.abs()
            * 12.0
        )
        + (
            laplacian.abs()
            * 1.75
        )
    ).clamp(
        0.0,
        1.0,
    )

    luminance_delta = (
        local_detail
        * (
            0.42
            * strength
        )
        + laplacian
        * (
            0.055
            * strength
        )
        - dark_stroke_mask
        * (
            0.018
            * strength
        )
    )

    adjusted = (
        region
        + luminance_delta.repeat(
            1,
            3,
            1,
            1,
        )
    ).clamp(
        0.0,
        1.0,
    )

    blend_mask = (
        edge_mask
        * (
            0.78
            * strength
        )
    ).clamp(
        0.0,
        0.90,
    )

    enhanced = (
        region
        * (
            1.0
            - blend_mask
        )
        + adjusted
        * blend_mask
    )

    return enhanced.clamp(
        0.0,
        1.0,
    )


def process_rules_text_restore(
    payload,
):
    try:
        import torch

    except Exception as exc:
        raise RuntimeError(
            "PyTorch could not be "
            "loaded: "
            f"{type(exc).__name__}: "
            f"{exc}"
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
            "Input image was not "
            f"found: {input_path}"
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
        scale = float(
            payload.get(
                "scale",
                2.0,
            )
            or 2.0
        )

    except (
        TypeError,
        ValueError,
    ):
        scale = 2.0

    try:
        rules_strength = float(
            payload.get(
                "rules_strength",
                0.70,
            )
            or 0.70
        )

    except (
        TypeError,
        ValueError,
    ):
        rules_strength = 0.70

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

    torch.cuda.synchronize(
        device
    )

    started_at = (
        time.perf_counter()
    )

    with torch.inference_mode():
        output_tensor = (
            torch.nn.functional.interpolate(
                input_tensor,
                scale_factor=scale,
                mode="bicubic",
                align_corners=False,
            )
        ).clamp(
            0.0,
            1.0,
        )

        profile = (
            resolve_frame_profile(
                card
            )
        )

        region_supported = (
            profile is not None
        )

        rules_region_data = {
            "supported": (
                region_supported
            ),
        }

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
                output_tensor.shape[2]
            )

            output_width = int(
                output_tensor.shape[3]
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

            source_left = (
                source_box[0]
            )

            source_top = (
                source_box[1]
            )

            source_right = (
                source_box[2]
            )

            source_bottom = (
                source_box[3]
            )

            output_left = (
                output_box[0]
            )

            output_top = (
                output_box[1]
            )

            output_right = (
                output_box[2]
            )

            output_bottom = (
                output_box[3]
            )

            source_region = (
                input_tensor[
                    :,
                    :,
                    source_top:source_bottom,
                    source_left:source_right,
                ]
            )

            output_region = (
                output_tensor[
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

            before_metrics = (
                measure_region(
                    torch,
                    output_region,
                )
            )

            enhanced_region = (
                enhance_rules_region(
                    torch,
                    output_region,
                    rules_strength,
                )
            )

            # Creatures have a P/T box
            # overlapping the lower-right
            # portion of the general text
            # area. Restore that portion
            # from the base upscale.
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
                    enhanced_region.shape[3],
                    pt_box[2]
                    - output_left,
                )

                pt_bottom = min(
                    enhanced_region.shape[2],
                    pt_box[3]
                    - output_top,
                )

                if (
                    pt_right
                    > pt_left
                    and pt_bottom
                    > pt_top
                ):
                    enhanced_region[
                        :,
                        :,
                        pt_top:pt_bottom,
                        pt_left:pt_right,
                    ] = output_region[
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
            ] = enhanced_region

            after_metrics = (
                measure_region(
                    torch,
                    enhanced_region,
                )
            )

            before_edge = (
                before_metrics.get(
                    "edge_strength_mean"
                )
            )

            after_edge = (
                after_metrics.get(
                    "edge_strength_mean"
                )
            )

            edge_gain_ratio = None

            if (
                before_edge
                and after_edge
                is not None
            ):
                edge_gain_ratio = round(
                    after_edge
                    / before_edge,
                    6,
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

                "strength": (
                    rules_strength
                ),

                "source_metrics": (
                    source_metrics
                ),

                "base_metrics": (
                    before_metrics
                ),

                "candidate_metrics": (
                    after_metrics
                ),

                "edge_gain_ratio": (
                    edge_gain_ratio
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

    output_image = (
        tensor_to_image(
            torch,
            output_tensor,
        )
    )

    output_image.save(
        output_path,
        format="PNG",
    )

    output_width = int(
        output_image.width
    )

    output_height = int(
        output_image.height
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

        "output_width": (
            output_width
        ),

        "output_height": (
            output_height
        ),

        "scale": (
            scale
        ),

        "processing_ms": (
            processing_ms
        ),

        "peak_gpu_memory_mb": (
            peak_memory_mb
        ),

        "region_supported": (
            region_supported
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