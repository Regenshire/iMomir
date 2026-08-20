import argparse
import os
import sys
from pathlib import Path


SET_ICON_RELATIVE_PATH = Path(
    "static",
    "img",
    "set_icons",
)


def get_project_root():
    return Path(
        __file__
    ).resolve().parent.parent


def get_set_icon_directory():
    return (
        get_project_root()
        / SET_ICON_RELATIVE_PATH
    )


def get_windows_extended_path(path):
    """
    Return a Windows extended-length path.

    This is especially important for legacy files such as CON.svg,
    PRN.svg, AUX.svg, etc. Windows normally interprets those names as
    device names even though they have an extension.

    The \\?\\ prefix tells Windows to use the literal filesystem path.
    """

    absolute_path = os.path.abspath(
        os.fspath(path)
    )

    if os.name != "nt":
        return absolute_path

    if absolute_path.startswith(
        "\\\\?\\"
    ):
        return absolute_path

    if absolute_path.startswith(
        "\\\\"
    ):
        return (
            "\\\\?\\UNC\\"
            + absolute_path[2:]
        )

    return "\\\\?\\" + absolute_path


def is_svg_filename(filename):
    return str(
        filename or ""
    ).lower().endswith(
        ".svg"
    )


def is_canonical_set_icon_filename(filename):
    return str(
        filename or ""
    ).startswith(
        "_"
    )


def get_canonical_filename(filename):
    clean_filename = str(
        filename or ""
    )

    if is_canonical_set_icon_filename(
        clean_filename
    ):
        return clean_filename

    return f"_{clean_filename}"


def rename_set_icons(
    icon_directory,
    dry_run=False,
):
    if not icon_directory.exists():
        raise FileNotFoundError(
            "Set icon directory does not exist: "
            f"{icon_directory}"
        )

    if not icon_directory.is_dir():
        raise NotADirectoryError(
            "Set icon path is not a directory: "
            f"{icon_directory}"
        )

    renamed_count = 0
    already_correct_count = 0
    skipped_count = 0
    error_count = 0

    print(
        f"Set icon directory: {icon_directory}"
    )

    if dry_run:
        print("Mode: DRY RUN")
    else:
        print("Mode: RENAME")

    print()

    with os.scandir(
        icon_directory
    ) as entries:
        entries = sorted(
            list(entries),
            key=lambda entry: (
                entry.name.casefold()
            ),
        )

    for entry in entries:
        filename = entry.name

        if not entry.is_file(
            follow_symlinks=False
        ):
            continue

        if not is_svg_filename(
            filename
        ):
            skipped_count += 1
            continue

        if is_canonical_set_icon_filename(
            filename
        ):
            already_correct_count += 1

            print(
                f"OK      {filename}"
            )

            continue

        new_filename = (
            get_canonical_filename(
                filename
            )
        )

        source_path = (
            icon_directory
            / filename
        )

        target_path = (
            icon_directory
            / new_filename
        )

        if target_path.exists():
            error_count += 1

            print(
                "ERROR   "
                f"{filename} -> {new_filename} "
                "(target already exists)"
            )

            continue

        print(
            f"RENAME  {filename} -> {new_filename}"
        )

        if dry_run:
            renamed_count += 1
            continue

        try:
            os.rename(
                get_windows_extended_path(
                    source_path
                ),
                get_windows_extended_path(
                    target_path
                ),
            )

            renamed_count += 1

        except Exception as exc:
            error_count += 1

            print(
                "ERROR   "
                f"{filename} -> {new_filename}: "
                f"{exc}"
            )

    print()
    print("=== COMPLETE ===")
    print(
        f"Renamed:         {renamed_count}"
    )
    print(
        f"Already correct: {already_correct_count}"
    )
    print(
        f"Non-SVG skipped: {skipped_count}"
    )
    print(
        f"Errors:          {error_count}"
    )

    return error_count


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Rename all iMomir set-icon SVG files "
            "to the canonical underscore-prefixed "
            "filename convention."
        )
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Show the planned renames without "
            "modifying any files."
        ),
    )

    parser.add_argument(
        "--directory",
        default="",
        help=(
            "Optional set-icons directory override. "
            "Defaults to static/img/set_icons "
            "under the iMomir project root."
        ),
    )

    arguments = parser.parse_args()

    if arguments.directory:
        icon_directory = Path(
            arguments.directory
        ).expanduser().resolve()
    else:
        icon_directory = (
            get_set_icon_directory()
        )

    error_count = rename_set_icons(
        icon_directory,
        dry_run=arguments.dry_run,
    )

    if error_count:
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(
        main()
    )