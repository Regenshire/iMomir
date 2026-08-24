import json
import sys


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
        ],
    }


def handle_health(payload):
    return {
        "ok": True,
        "plugin_id": PLUGIN_ID,
        "version": PLUGIN_VERSION,
        "status": "ready",
        "message": (
            "Upscaler plugin runtime "
            "is responding."
        ),
    }


COMMAND_HANDLERS = {
    "info": handle_info,
    "health": handle_health,
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