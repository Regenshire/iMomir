import os

from PIL import Image, ImageOps

DEFAULT_CARD_BACK_FILENAME = "00 - Word Wall Proxy Back - Dark Color.jpg"
DEFAULT_CARD_BACK_KEY = f"builtin:{DEFAULT_CARD_BACK_FILENAME}"

BUILTIN_CARD_BACK_PREFIX = "builtin:"
CUSTOM_CARD_BACK_PREFIX = "custom:"

Image.init()
SUPPORTED_IMAGE_EXTENSIONS = frozenset(
    extension.lower()
    for extension in Image.registered_extensions().keys()
)


def get_builtin_card_back_dir(static_dir):
    return os.path.join(static_dir, "img", "card_backs")


def _is_supported_image_filename(filename):
    extension = os.path.splitext(str(filename or ""))[1].lower()
    return bool(extension) and extension in SUPPORTED_IMAGE_EXTENSIONS


def _safe_upload_stem(filename):
    stem = os.path.splitext(os.path.basename(str(filename or "")))[0].strip()
    safe_chars = []

    for character in stem:
        if character.isalnum() or character in {"-", "_"}:
            safe_chars.append(character)
        elif character.isspace():
            safe_chars.append("_")
        else:
            safe_chars.append("_")

    safe_stem = "".join(safe_chars).strip("_-")

    while "__" in safe_stem:
        safe_stem = safe_stem.replace("__", "_")

    return safe_stem or "custom_card_back"


def list_card_back_options(static_dir, runtime_card_back_dir):
    options = []
    builtin_dir = get_builtin_card_back_dir(static_dir)

    if os.path.isdir(builtin_dir):
        for filename in sorted(os.listdir(builtin_dir), key=str.casefold):
            absolute_path = os.path.join(builtin_dir, filename)

            if not os.path.isfile(absolute_path):
                continue

            if not _is_supported_image_filename(filename):
                continue

            options.append({
                "key": f"{BUILTIN_CARD_BACK_PREFIX}{filename}",
                "filename": filename,
                "label": os.path.splitext(filename)[0],
                "source": "builtin",
                "absolute_path": absolute_path,
            })

    if os.path.isdir(runtime_card_back_dir):
        for filename in sorted(os.listdir(runtime_card_back_dir), key=str.casefold):
            absolute_path = os.path.join(runtime_card_back_dir, filename)

            if not os.path.isfile(absolute_path):
                continue

            if not _is_supported_image_filename(filename):
                continue

            options.append({
                "key": f"{CUSTOM_CARD_BACK_PREFIX}{filename}",
                "filename": filename,
                "label": os.path.splitext(filename)[0].replace("_", " "),
                "source": "custom",
                "absolute_path": absolute_path,
            })

    return options


def _find_card_back_option(raw_key, static_dir, runtime_card_back_dir):
    clean_key = str(raw_key or "").strip()

    if not clean_key:
        return None

    options = list_card_back_options(static_dir, runtime_card_back_dir)
    option_by_key = {
        option["key"]: option
        for option in options
    }

    if clean_key in option_by_key:
        return option_by_key[clean_key]

    # Compatibility for a plain filename value if one was saved manually or
    # by an older development build before source prefixes were introduced.
    basename = os.path.basename(clean_key)

    for prefix in (BUILTIN_CARD_BACK_PREFIX, CUSTOM_CARD_BACK_PREFIX):
        legacy_key = f"{prefix}{basename}"

        if legacy_key in option_by_key:
            return option_by_key[legacy_key]

    return None


def normalize_card_back_key(
    raw_key,
    static_dir,
    runtime_card_back_dir,
    allow_empty=False,
    fallback_key=None,
):
    clean_key = str(raw_key or "").strip()

    if not clean_key and allow_empty:
        return ""

    option = _find_card_back_option(
        clean_key,
        static_dir,
        runtime_card_back_dir,
    )

    if option:
        return option["key"]

    if fallback_key:
        fallback_option = _find_card_back_option(
            fallback_key,
            static_dir,
            runtime_card_back_dir,
        )

        if fallback_option:
            return fallback_option["key"]

    default_option = _find_card_back_option(
        DEFAULT_CARD_BACK_KEY,
        static_dir,
        runtime_card_back_dir,
    )

    if default_option:
        return default_option["key"]

    options = list_card_back_options(
        static_dir,
        runtime_card_back_dir,
    )

    if options:
        return options[0]["key"]

    return ""


def resolve_card_back_option(
    raw_key,
    static_dir,
    runtime_card_back_dir,
):
    normalized_key = normalize_card_back_key(
        raw_key,
        static_dir,
        runtime_card_back_dir,
    )

    if not normalized_key:
        return None

    return _find_card_back_option(
        normalized_key,
        static_dir,
        runtime_card_back_dir,
    )


def save_custom_card_back_upload(
    file_storage,
    runtime_card_back_dir,
    max_file_size_bytes=None,
):
    if not file_storage or not getattr(file_storage, "filename", ""):
        raise ValueError("Choose an image file to upload.")

    if max_file_size_bytes is not None:
        max_file_size_bytes = max(
            0,
            int(max_file_size_bytes),
        )

        if max_file_size_bytes > 0:
            try:
                upload_stream = file_storage.stream

                upload_stream.seek(
                    0,
                    os.SEEK_END,
                )

                upload_size_bytes = (
                    upload_stream.tell()
                )

                upload_stream.seek(0)

            except (
                AttributeError,
                OSError,
                ValueError,
            ):
                upload_size_bytes = int(
                    getattr(
                        file_storage,
                        "content_length",
                        0,
                    )
                    or 0
                )

            if upload_size_bytes > max_file_size_bytes:
                max_size_mb = (
                    max_file_size_bytes
                    / (1024 * 1024)
                )

                raise ValueError(
                    f"The uploaded card back image exceeds the "
                    f"{max_size_mb:g} MB size limit."
                )

    try:
        source_image = Image.open(
            file_storage.stream
        )

        image = ImageOps.exif_transpose(
            source_image
        )

        image.load()

    except Exception as exc:
        raise ValueError(
            "The uploaded file is not a valid image."
        ) from exc

    if image.width < 1 or image.height < 1:
        raise ValueError(
            "The uploaded card back image has invalid dimensions."
        )

    if image.width * image.height > 100_000_000:
        raise ValueError(
            "The uploaded card back image is too large."
        )

    os.makedirs(
        runtime_card_back_dir,
        exist_ok=True,
    )

    safe_stem = _safe_upload_stem(
        file_storage.filename
    )

    candidate_filename = f"{safe_stem}.png"
    candidate_path = os.path.join(
        runtime_card_back_dir,
        candidate_filename,
    )

    duplicate_index = 2

    while os.path.exists(candidate_path):
        candidate_filename = (
            f"{safe_stem}_{duplicate_index}.png"
        )

        candidate_path = os.path.join(
            runtime_card_back_dir,
            candidate_filename,
        )

        duplicate_index += 1

    image.convert("RGB").save(
        candidate_path,
        format="PNG",
        optimize=True,
    )

    return (
        f"{CUSTOM_CARD_BACK_PREFIX}"
        f"{candidate_filename}"
    )

def delete_custom_card_back(
    raw_key,
    static_dir,
    runtime_card_back_dir,
):
    clean_key = str(raw_key or "").strip()

    if not clean_key.startswith(CUSTOM_CARD_BACK_PREFIX):
        raise ValueError(
            "Only custom uploaded card backs can be deleted."
        )

    custom_filename = clean_key[
        len(CUSTOM_CARD_BACK_PREFIX):
    ].strip()

    if (
        not custom_filename
        or custom_filename
        != os.path.basename(custom_filename)
    ):
        raise ValueError(
            "The custom card back filename is invalid."
        )

    option = next(
        (
            item
            for item in list_card_back_options(
                static_dir,
                runtime_card_back_dir,
            )
            if (
                item["key"] == clean_key
                and item["source"] == "custom"
            )
        ),
        None,
    )

    if not option:
        raise FileNotFoundError(
            "The custom card back was not found."
        )

    runtime_root = os.path.realpath(
        runtime_card_back_dir
    )

    target_path = os.path.realpath(
        option["absolute_path"]
    )

    try:
        common_path = os.path.commonpath(
            [
                runtime_root,
                target_path,
            ]
        )
    except ValueError as exc:
        raise ValueError(
            "The custom card back path is invalid."
        ) from exc

    if common_path != runtime_root:
        raise ValueError(
            "The custom card back path is outside "
            "the permitted upload directory."
        )

    if not os.path.isfile(target_path):
        raise FileNotFoundError(
            "The custom card back image no longer exists."
        )

    os.remove(target_path)

    return option