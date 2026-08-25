import hashlib
import os
import urllib.request


PLUGIN_DIR = os.path.dirname(
    os.path.abspath(__file__)
)

MODEL_DIR = os.path.join(
    PLUGIN_DIR,
    "models",
)


MODEL_SPECS = {
    "realesrgan_x2plus": {
        "model_id": (
            "realesrgan_x2plus"
        ),

        "filename": (
            "RealESRGAN_x2plus.pth"
        ),

        "url": (
            "https://github.com/xinntao/"
            "Real-ESRGAN/releases/download/"
            "v0.2.1/RealESRGAN_x2plus.pth"
        ),

        "sha256": (
            "49fafd45f8fd7aa8d31ab2a22d14d91b"
            "536c34494a5cfe31eb5d89c2fa266abb"
        ),

        "size_bytes": 67061725,

        "scale": 2,
    },

    "realesrnet_x4plus": {
        "model_id": (
            "realesrnet_x4plus"
        ),

        "filename": (
            "RealESRNet_x4plus.pth"
        ),

        "url": (
            "https://github.com/xinntao/"
            "Real-ESRGAN/releases/download/"
            "v0.1.1/RealESRNet_x4plus.pth"
        ),

        "sha256": (
            "a820b9bde89a874d7599d545567308ce"
            "6c128fc8754a53208eda016d40aa81df"
        ),

        "scale": 4,
    },
}


def calculate_sha256(
    path_value,
):
    digest = hashlib.sha256()

    with open(
        path_value,
        "rb",
    ) as model_file:
        while True:
            chunk = model_file.read(
                1024 * 1024
            )

            if not chunk:
                break

            digest.update(
                chunk
            )

    return digest.hexdigest()


def get_model_spec(
    model_id,
):
    model_spec = MODEL_SPECS.get(
        str(
            model_id
            or ""
        ).strip()
    )

    if not model_spec:
        raise ValueError(
            "Unknown runtime model: "
            f"{model_id}"
        )

    return dict(
        model_spec
    )


def get_model_path(
    model_id,
):
    model_spec = get_model_spec(
        model_id
    )

    return os.path.join(
        MODEL_DIR,
        model_spec[
            "filename"
        ],
    )


def is_model_ready(
    model_id,
):
    model_spec = get_model_spec(
        model_id
    )

    model_path = get_model_path(
        model_id
    )

    if not os.path.exists(
        model_path
    ):
        return False

    try:
        return (
            calculate_sha256(
                model_path
            ).lower()
            == model_spec[
                "sha256"
            ].lower()
        )

    except OSError:
        return False


def ensure_model_file(
    model_id,
):
    model_spec = get_model_spec(
        model_id
    )

    model_path = get_model_path(
        model_id
    )

    if is_model_ready(
        model_id
    ):
        return model_path

    os.makedirs(
        MODEL_DIR,
        exist_ok=True,
    )

    temporary_path = (
        model_path
        + ".download"
    )

    if os.path.exists(
        temporary_path
    ):
        os.remove(
            temporary_path
        )

    request = urllib.request.Request(
        model_spec["url"],
        headers={
            "User-Agent": (
                "iMomir-Upscaler/0.3"
            ),

            "Accept": (
                "application/octet-stream"
            ),
        },
    )

    try:
        with urllib.request.urlopen(
            request,
            timeout=120,
        ) as response:
            with open(
                temporary_path,
                "wb",
            ) as output_file:
                while True:
                    chunk = response.read(
                        1024 * 1024
                    )

                    if not chunk:
                        break

                    output_file.write(
                        chunk
                    )

        actual_sha256 = (
            calculate_sha256(
                temporary_path
            )
        )

        expected_sha256 = (
            model_spec[
                "sha256"
            ]
        )

        if (
            actual_sha256.lower()
            != expected_sha256.lower()
        ):
            raise RuntimeError(
                "Downloaded model failed "
                "SHA256 validation. "
                f"Expected {expected_sha256}; "
                f"received {actual_sha256}."
            )

        os.replace(
            temporary_path,
            model_path,
        )

    except Exception:
        if os.path.exists(
            temporary_path
        ):
            os.remove(
                temporary_path
            )

        raise

    return model_path