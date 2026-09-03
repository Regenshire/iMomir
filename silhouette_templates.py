import json
import os
import re
import threading


STUDIO3_EXTENSION = ".studio3"
SILHOUETTE_TEMPLATE_MANIFEST_FILENAME = "templates.json"

_manifest_lock = threading.Lock()


def get_silhouette_studio3_dir(static_folder):
    return os.path.join(
        os.path.abspath(static_folder),
        "sil",
        "Studio3",
    )


def get_silhouette_template_manifest_path(static_folder):
    return os.path.join(
        get_silhouette_studio3_dir(static_folder),
        SILHOUETTE_TEMPLATE_MANIFEST_FILENAME,
    )


def _validate_manifest_filename(filename):
    clean_filename = str(filename or "").strip()

    if (
        not clean_filename
        or clean_filename != os.path.basename(clean_filename)
        or os.path.splitext(clean_filename)[1].lower() != STUDIO3_EXTENSION
    ):
        return ""

    return clean_filename


def _safe_uploaded_filename(filename):
    original_filename = os.path.basename(
        str(filename or "").strip()
    )

    stem, extension = os.path.splitext(
        original_filename
    )

    if extension.lower() != STUDIO3_EXTENSION:
        raise ValueError(
            "Only Silhouette Studio .studio3 files can be uploaded."
        )

    safe_stem = re.sub(
        r"[^A-Za-z0-9._-]+",
        "_",
        stem,
    ).strip("._-")

    if not safe_stem:
        safe_stem = "Silhouette_Template"

    return (
        f"{safe_stem}"
        f"{STUDIO3_EXTENSION}"
    )


def _read_manifest_document(static_folder):
    manifest_path = (
        get_silhouette_template_manifest_path(
            static_folder
        )
    )

    if not os.path.isfile(manifest_path):
        return {
            "version": 1,
            "templates": [],
        }

    try:
        with open(
            manifest_path,
            "r",
            encoding="utf-8",
        ) as manifest_file:
            document = json.load(
                manifest_file
            )

    except (
        OSError,
        json.JSONDecodeError,
    ) as exc:
        raise ValueError(
            "The Silhouette template manifest could not be read."
        ) from exc

    if not isinstance(document, dict):
        raise ValueError(
            "The Silhouette template manifest is invalid."
        )

    templates = document.get(
        "templates"
    )

    if not isinstance(templates, list):
        templates = []

    return {
        "version": int(
            document.get("version")
            or 1
        ),
        "templates": templates,
    }


def _write_manifest_document(
    static_folder,
    document,
):
    studio3_dir = (
        get_silhouette_studio3_dir(
            static_folder
        )
    )

    os.makedirs(
        studio3_dir,
        exist_ok=True,
    )

    manifest_path = (
        get_silhouette_template_manifest_path(
            static_folder
        )
    )

    temp_manifest_path = (
        f"{manifest_path}.tmp"
    )

    with open(
        temp_manifest_path,
        "w",
        encoding="utf-8",
    ) as manifest_file:
        json.dump(
            document,
            manifest_file,
            indent=2,
            ensure_ascii=False,
        )

        manifest_file.write("\n")

    os.replace(
        temp_manifest_path,
        manifest_path,
    )


def list_silhouette_templates(
    static_folder,
):
    studio3_dir = (
        get_silhouette_studio3_dir(
            static_folder
        )
    )

    document = (
        _read_manifest_document(
            static_folder
        )
    )

    templates = []

    for raw_entry in document.get(
        "templates",
        [],
    ):
        if not isinstance(
            raw_entry,
            dict,
        ):
            continue

        filename = (
            _validate_manifest_filename(
                raw_entry.get(
                    "filename"
                )
            )
        )

        if not filename:
            continue

        absolute_path = os.path.join(
            studio3_dir,
            filename,
        )

        if not os.path.isfile(
            absolute_path
        ):
            continue

        name = str(
            raw_entry.get("name")
            or os.path.splitext(
                filename
            )[0]
        ).strip()

        description = str(
            raw_entry.get(
                "description"
            )
            or ""
        ).strip()

        print_template = str(
            raw_entry.get(
                "print_template"
            )
            or ""
        ).strip().lower()

        templates.append({
            "filename": filename,
            "name": name,
            "description": description,
            "print_template": print_template,
            "absolute_path": absolute_path,
        })

    templates.sort(
        key=lambda item: (
            item["name"].casefold(),
            item["filename"].casefold(),
        )
    )

    return templates


def get_silhouette_template_entry(
    static_folder,
    filename,
):
    clean_filename = (
        _validate_manifest_filename(
            filename
        )
    )

    if not clean_filename:
        return None

    for entry in (
        list_silhouette_templates(
            static_folder
        )
    ):
        if (
            entry["filename"]
            == clean_filename
        ):
            return entry

    return None


def save_silhouette_template_upload(
    file_storage,
    name,
    description,
    print_template,
    static_folder,
):
    if (
        not file_storage
        or not getattr(
            file_storage,
            "filename",
            "",
        )
    ):
        raise ValueError(
            "Choose a .studio3 file to upload."
        )

    clean_name = str(
        name or ""
    ).strip()

    clean_description = str(
        description or ""
    ).strip()

    clean_print_template = str(
        print_template or ""
    ).strip().lower()

    if not clean_name:
        raise ValueError(
            "Template Name is required."
        )

    if len(clean_name) > 120:
        raise ValueError(
            "Template Name must be 120 characters or fewer."
        )

    if len(clean_description) > 1000:
        raise ValueError(
            "Template Description must be 1000 characters or fewer."
        )

    if not clean_print_template:
        raise ValueError(
            "Linked Print Template is required."
        )

    studio3_dir = (
        get_silhouette_studio3_dir(
            static_folder
        )
    )

    os.makedirs(
        studio3_dir,
        exist_ok=True,
    )

    base_filename = (
        _safe_uploaded_filename(
            file_storage.filename
        )
    )

    stem = os.path.splitext(
        base_filename
    )[0]

    with _manifest_lock:
        document = (
            _read_manifest_document(
                static_folder
            )
        )

        existing_filenames = {
            str(
                item.get(
                    "filename"
                )
                or ""
            ).casefold()
            for item
            in document.get(
                "templates",
                [],
            )
            if isinstance(
                item,
                dict,
            )
        }

        candidate_filename = (
            base_filename
        )

        duplicate_index = 2

        while (
            candidate_filename.casefold()
            in existing_filenames
            or os.path.exists(
                os.path.join(
                    studio3_dir,
                    candidate_filename,
                )
            )
        ):
            candidate_filename = (
                f"{stem}_"
                f"{duplicate_index}"
                f"{STUDIO3_EXTENSION}"
            )

            duplicate_index += 1

        target_path = os.path.join(
            studio3_dir,
            candidate_filename,
        )

        new_entry = {
            "filename": (
                candidate_filename
            ),
            "name": clean_name,
            "description": (
                clean_description
            ),
            "print_template": (
                clean_print_template
            ),
        }

        try:
            file_storage.save(
                target_path
            )

            if not os.path.isfile(
                target_path
            ):
                raise OSError(
                    "Uploaded template file was not saved."
                )

            document.setdefault(
                "templates",
                [],
            ).append(
                new_entry
            )

            _write_manifest_document(
                static_folder,
                document,
            )

        except Exception:
            if os.path.isfile(
                target_path
            ):
                try:
                    os.remove(
                        target_path
                    )
                except OSError:
                    pass

            raise

    saved_entry = (
        get_silhouette_template_entry(
            static_folder,
            candidate_filename,
        )
    )

    if not saved_entry:
        raise ValueError(
            "The uploaded Silhouette template could not be registered."
        )

    return saved_entry