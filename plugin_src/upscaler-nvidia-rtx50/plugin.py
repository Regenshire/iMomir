import json
import os
import platform
import sys
import time


PLUGIN_ID = "upscaler-nvidia-rtx50"
PLUGIN_NAME = (
    "iMomir Upscaler - "
    "NVIDIA RTX 50 Series"
)
PLUGIN_VERSION = "0.1.0"
PROTOCOL_VERSION = 1


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