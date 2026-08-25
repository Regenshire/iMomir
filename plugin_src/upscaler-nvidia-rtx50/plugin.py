import json
import os
import platform
import sys
import time

from processors.rules_text import (
    process_rules_text_restore,
)

PLUGIN_ID = "upscaler-nvidia-rtx50"
PLUGIN_NAME = (
    "iMomir Upscaler - "
    "NVIDIA RTX 50 Series"
)
PLUGIN_VERSION = "0.8.0"
PROTOCOL_VERSION = 1


UPSCALE_MODELS = {
    "cuda_bicubic_test": {
        "model_id": (
            "cuda_bicubic_test"
        ),

        "label": (
            "CUDA Bicubic "
            "(Test Baseline)"
        ),

        "description": (
            "2x CUDA bicubic resize. "
            "Testing baseline only; "
            "not AI super-resolution."
        ),

        "scale": 2.0,
        "test_only": True,

        "targets": [
            "whole_card",
        ],
    },

    "rules_text_restore_v1": {
        "model_id": (
            "rules_text_restore_v1"
        ),

        "label": (
            "Rules Text Restore v1"
        ),

        "description": (
            "2x CUDA upscale with "
            "region-aware Rules Text "
            "restoration."
        ),

        "scale": 2.0,

        "rules_strength": 0.70,

        "test_only": False,

        "targets": [
            "rules_text",
        ],
    },

    "rules_text_realesrgan_x2_v1": {
        "model_id": (
            "rules_text_realesrgan_x2_v1"
        ),

        "label": (
            "Rules Text AI v2 "
            "(Real-ESRGAN x2)"
        ),

        "description": (
            "Neural 2x restoration of "
            "the Rules Text region using "
            "RealESRGAN_x2plus."
        ),

        "scale": 2.0,

        "ai_blend_strength": 0.88,

        "test_only": False,

        "targets": [
            "rules_text",
        ],
    },

    "magic_card_ai_v1": {
        "model_id": (
            "magic_card_ai_v1"
        ),

        "label": (
            "Magic Card AI v1 "
            "(Whole Card + Rules Text)"
        ),

        "description": (
            "Real-ESRGAN x2 across the "
            "entire card followed by a "
            "targeted Rules Text "
            "restoration pass."
        ),

        "scale": 2.0,

        "rules_strength": 0.35,

        "test_only": False,

        "targets": [
            "whole_card",
            "rules_text",
        ],
    },

    "magic_card_ai_v2": {
        "model_id": (
            "magic_card_ai_v2"
        ),

        "label": (
            "Magic Card AI v2 "
            "(Structure Protected)"
        ),

        "description": (
            "Magic Card AI v1 plus "
            "structure-aware protection "
            "for titles, mana symbols, "
            "Rules Text, P/T, and "
            "bottom text."
        ),

        "scale": 2.0,

        "rules_strength": 0.35,

        "test_only": False,

        "targets": [
            "whole_card",
            "card_title",
            "mana_cost",
            "type_line",
            "rules_text",
            "power_toughness",
            "bottom_text",
        ],
    },

    "magic_card_ai_v3": {
        "model_id": (
            "magic_card_ai_v3"
        ),

        "label": (
            "Magic Card AI v3 "
            "(Targeted Model Routing)"
        ),

        "description": (
            "Real-ESRGAN x2 for the full "
            "card with RealESRNet x4 "
            "routed through text and "
            "symbol-sensitive regions."
        ),

        "scale": 2.0,

        "rules_strength": 0.35,

        "test_only": False,

        "targets": [
            "whole_card",
            "card_title",
            "mana_cost",
            "type_line",
            "rules_text",
            "power_toughness",
            "bottom_text",
        ],
    },
}


def read_payload():
    raw_input = sys.stdin.read()

    if not raw_input.strip():
        return {}

    try:
        payload = json.loads(
            raw_input
        )
    except json.JSONDecodeError:
        raise ValueError(
            "Plugin input was not valid JSON."
        )

    if not isinstance(
        payload,
        dict,
    ):
        raise ValueError(
            "Plugin input must be a JSON object."
        )

    return payload


def write_result(result):
    print(
        json.dumps(
            result,
            ensure_ascii=False,
        )
    )


def load_torch():
    try:
        import torch

        return torch, ""

    except Exception as exc:
        return (
            None,
            (
                f"{type(exc).__name__}: "
                f"{exc}"
            ),
        )


def build_hardware_status():
    torch, torch_error = load_torch()

    result = {
        "python_version": (
            platform.python_version()
        ),
        "python_executable": (
            sys.executable
        ),
        "platform": (
            platform.platform()
        ),

        "torch_installed": (
            torch is not None
        ),
        "torch_version": "",

        "cuda_available": False,
        "cuda_runtime_version": "",
        "cudnn_version": None,

        "device_count": 0,
        "devices": [],

        "target_hardware_detected": False,
        "error": "",
    }

    if torch is None:
        result["error"] = torch_error
        return result

    result["torch_version"] = str(
        getattr(
            torch,
            "__version__",
            "",
        )
        or ""
    )

    result["cuda_runtime_version"] = str(
        getattr(
            torch.version,
            "cuda",
            "",
        )
        or ""
    )

    try:
        result["cudnn_version"] = (
            torch.backends.cudnn.version()
        )
    except Exception:
        result["cudnn_version"] = None

    try:
        cuda_available = bool(
            torch.cuda.is_available()
        )

        result["cuda_available"] = (
            cuda_available
        )

    except Exception as exc:
        result["error"] = (
            f"{type(exc).__name__}: "
            f"{exc}"
        )

        return result

    if not cuda_available:
        result["error"] = (
            "PyTorch is installed, but CUDA "
            "is not available."
        )

        return result

    try:
        device_count = int(
            torch.cuda.device_count()
        )

        result["device_count"] = (
            device_count
        )

        for device_index in range(
            device_count
        ):
            properties = (
                torch.cuda.get_device_properties(
                    device_index
                )
            )

            capability = (
                torch.cuda.get_device_capability(
                    device_index
                )
            )

            device_name = str(
                properties.name
                or ""
            )

            total_memory_bytes = int(
                properties.total_memory
                or 0
            )

            total_memory_gb = round(
                total_memory_bytes
                / (1024 ** 3),
                2,
            )

            compute_capability = (
                f"{capability[0]}."
                f"{capability[1]}"
            )

            device_info = {
                "index": device_index,
                "name": device_name,

                "compute_capability": (
                    compute_capability
                ),

                "total_memory_gb": (
                    total_memory_gb
                ),

                "multiprocessor_count": int(
                    getattr(
                        properties,
                        "multi_processor_count",
                        0,
                    )
                    or 0
                ),
            }

            result["devices"].append(
                device_info
            )

            if (
                capability[0] >= 12
                or "5090"
                in device_name.upper()
            ):
                result[
                    "target_hardware_detected"
                ] = True

    except Exception as exc:
        result["error"] = (
            f"{type(exc).__name__}: "
            f"{exc}"
        )

    return result

def handle_upscale_test(payload):
    torch, torch_error = load_torch()

    if torch is None:
        raise RuntimeError(
            "PyTorch could not be loaded: "
            f"{torch_error}"
        )

    if not torch.cuda.is_available():
        raise RuntimeError(
            "CUDA is not available."
        )

    try:
        from PIL import Image
    except Exception as exc:
        raise RuntimeError(
            "Pillow could not be loaded: "
            f"{type(exc).__name__}: {exc}"
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
            f"Input image was not found: "
            f"{input_path}"
        )

    if input_path == output_path:
        raise ValueError(
            "Input and output paths must "
            "be different."
        )

    try:
        scale = float(
            payload.get(
                "scale",
                2.0,
            )
            or 2.0
        )
    except (TypeError, ValueError):
        scale = 2.0

    if scale < 1.0 or scale > 4.0:
        raise ValueError(
            "scale must be between "
            "1.0 and 4.0."
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
            source_file
            .convert("RGB")
        )

    input_width = int(
        source_image.width
    )

    input_height = int(
        source_image.height
    )

    raw_bytes = bytearray(
        source_image.tobytes()
    )

    input_tensor = torch.frombuffer(
        raw_bytes,
        dtype=torch.uint8,
    )

    input_tensor = input_tensor.reshape(
        input_height,
        input_width,
        3,
    )

    input_tensor = (
        input_tensor
        .permute(
            2,
            0,
            1,
        )
        .unsqueeze(0)
        .contiguous()
    )

    input_tensor = (
        input_tensor
        .to(
            device=device,
            dtype=torch.float32,
        )
        / 255.0
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
        )

        output_tensor = (
            output_tensor
            .clamp(
                0.0,
                1.0,
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

    output_tensor = (
        output_tensor
        .squeeze(0)
        .permute(
            1,
            2,
            0,
        )
        .mul(255.0)
        .round()
        .to(
            dtype=torch.uint8
        )
        .cpu()
        .contiguous()
    )

    output_array = (
        output_tensor.numpy()
    )

    output_image = (
        Image.fromarray(
            output_array
        )
    )

    output_image.save(
        output_path
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
        / (1024 * 1024),
        2,
    )

    return {
        "ok": True,

        "plugin_id": PLUGIN_ID,
        "version": PLUGIN_VERSION,

        "processor": (
            "cuda_bicubic_test"
        ),

        "test_only": True,

        "device": device_name,

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

        "input_path": input_path,
        "output_path": output_path,

        "input_width": input_width,
        "input_height": input_height,

        "output_width": output_width,
        "output_height": output_height,

        "scale": scale,

        "processing_ms": (
            processing_ms
        ),

        "peak_gpu_memory_mb": (
            peak_memory_mb
        ),
    }

def handle_models(payload):
    visible_model_ids = {
        "magic_card_ai_v3",
    }

    return {
        "ok": True,
        "plugin_id": PLUGIN_ID,
        "version": PLUGIN_VERSION,

        "models": [
            dict(model)
            for model
            in UPSCALE_MODELS.values()
            if model.get(
                "model_id"
            )
            in visible_model_ids
        ],
    }


def handle_upscale(payload):
    model_id = str(
        payload.get(
            "model_id",
            "",
        )
        or ""
    ).strip()

    if not model_id:
        model_id = (
            "cuda_bicubic_test"
        )

    model = UPSCALE_MODELS.get(
        model_id
    )

    if not model:
        raise ValueError(
            "Unknown upscale model: "
            f"{model_id}"
        )

    processor_payload = dict(
        payload
    )

    processor_payload["scale"] = (
        model["scale"]
    )

    if (
        model_id
        == "cuda_bicubic_test"
    ):
        result = handle_upscale_test(
            processor_payload
        )

    elif (
        model_id
        == "rules_text_restore_v1"
    ):
        processor_payload[
            "rules_strength"
        ] = model.get(
            "rules_strength",
            0.70,
        )

        result = (
            process_rules_text_restore(
                processor_payload
            )
        )

    elif (
        model_id
        == "rules_text_realesrgan_x2_v1"
    ):
        from processors.rules_text_ai import (
            process_rules_text_realesrgan,
        )

        processor_payload[
            "ai_blend_strength"
        ] = model.get(
            "ai_blend_strength",
            0.88,
        )

        result = (
            process_rules_text_realesrgan(
                processor_payload
            )
        )

    elif (
        model_id
        == "magic_card_ai_v1"
    ):
        from processors.magic_card import (
            process_magic_card_ai_v1,
        )

        processor_payload[
            "rules_strength"
        ] = model.get(
            "rules_strength",
            0.35,
        )

        result = (
            process_magic_card_ai_v1(
                processor_payload
            )
        )

    elif (
        model_id
        == "magic_card_ai_v2"
    ):
        from processors.magic_card_v2 import (
            process_magic_card_ai_v2,
        )

        processor_payload[
            "rules_strength"
        ] = model.get(
            "rules_strength",
            0.35,
        )

        result = (
            process_magic_card_ai_v2(
                processor_payload
            )
        )

    elif (
        model_id
        == "magic_card_ai_v3"
    ):
        from processors.magic_card_v3 import (
            process_magic_card_ai_v3,
        )

        processor_payload[
            "rules_strength"
        ] = model.get(
            "rules_strength",
            0.35,
        )

        result = (
            process_magic_card_ai_v3(
                processor_payload
            )
        )

    else:
        raise ValueError(
            "No processor is registered "
            f"for model {model_id}."
        )

    result["model_id"] = (
        model_id
    )

    result["model_label"] = (
        model["label"]
    )

    result["test_only"] = bool(
        model.get(
            "test_only",
            False,
        )
    )

    result["plugin_id"] = (
        PLUGIN_ID
    )

    result["version"] = (
        PLUGIN_VERSION
    )

    return result

def handle_upscale_batch(payload):
    raw_items = payload.get(
        "items"
    )

    if not isinstance(
        raw_items,
        list,
    ) or not raw_items:
        raise ValueError(
            "upscale_batch requires "
            "at least one item."
        )

    if len(raw_items) > 8:
        raise ValueError(
            "upscale_batch supports a "
            "maximum of 8 items."
        )

    shared_payload = {
        key: value
        for key, value
        in payload.items()
        if key != "items"
    }

    results = []

    for item_index, raw_item in enumerate(
        raw_items
    ):
        if not isinstance(
            raw_item,
            dict,
        ):
            raise ValueError(
                "Each upscale_batch item "
                "must be an object."
            )

        item_payload = dict(
            shared_payload
        )

        item_payload.update(
            raw_item
        )

        batch_item_id = str(
            raw_item.get(
                "batch_item_id",
                item_index,
            )
        )

        result = handle_upscale(
            item_payload
        )

        result[
            "batch_item_id"
        ] = batch_item_id

        results.append(
            result
        )

    return {
        "ok": True,
        "plugin_id": PLUGIN_ID,
        "version": PLUGIN_VERSION,
        "item_count": len(results),
        "results": results,
    }

def handle_info(payload):
    return {
        "ok": True,
        "plugin_id": PLUGIN_ID,
        "name": PLUGIN_NAME,
        "version": PLUGIN_VERSION,
        "protocol_version": (
            PROTOCOL_VERSION
        ),
        "plugin_type": "upscaler",

        "capabilities": [
            "info",
            "health",
            "hardware",
            "models",
            "upscale",
            "upscale_batch",
            "upscale_test",
        ],
    }


def handle_hardware(payload):
    hardware = build_hardware_status()

    runtime_ready = bool(
        hardware.get(
            "torch_installed"
        )
        and hardware.get(
            "cuda_available"
        )
        and hardware.get(
            "device_count",
            0,
        )
        > 0
    )

    return {
        "ok": True,
        "plugin_id": PLUGIN_ID,
        "version": PLUGIN_VERSION,
        "runtime_ready": runtime_ready,
        "hardware": hardware,
    }


def handle_health(payload):
    hardware = build_hardware_status()

    runtime_ready = bool(
        hardware.get(
            "torch_installed"
        )
        and hardware.get(
            "cuda_available"
        )
        and hardware.get(
            "device_count",
            0,
        )
        > 0
    )

    if runtime_ready:
        status = "ready"

        message = (
            "CUDA upscaling runtime "
            "is ready."
        )

    else:
        status = "unavailable"

        message = (
            hardware.get("error")
            or (
                "CUDA upscaling runtime "
                "is not ready."
            )
        )

    return {
        "ok": True,
        "plugin_id": PLUGIN_ID,
        "version": PLUGIN_VERSION,
        "status": status,
        "runtime_ready": runtime_ready,
        "message": message,
        "hardware": hardware,
    }


COMMAND_HANDLERS = {
    "info": handle_info,
    "health": handle_health,
    "hardware": handle_hardware,
    "models": handle_models,
    "upscale": handle_upscale,
    "upscale_batch": handle_upscale_batch,
    "upscale_test": handle_upscale_test,
}


def main():
    if len(sys.argv) < 2:
        raise ValueError(
            "Plugin command is required."
        )

    command = str(
        sys.argv[1]
        or ""
    ).strip().lower()

    if not command:
        raise ValueError(
            "Plugin command is required."
        )

    handler = COMMAND_HANDLERS.get(
        command
    )

    if not handler:
        raise ValueError(
            f"Unsupported plugin command: "
            f"{command}"
        )

    payload = read_payload()

    result = handler(
        payload
    )

    write_result(
        result
    )


if __name__ == "__main__":
    try:
        main()

    except Exception as exc:
        error_result = {
            "ok": False,
            "error": str(exc),
            "error_type": (
                type(exc).__name__
            ),
        }

        print(
            json.dumps(
                error_result
            ),
            file=sys.stderr,
        )

        sys.exit(1)