import atexit
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import zipfile

import requests

from paths import (
    BUNDLE_BASE_DIR,
    PLUGIN_DOWNLOAD_DIR,
    PLUGIN_ROOT_DIR,
)

from plugin_system.catalog import (
    PLUGIN_CATALOG,
)


PLUGIN_MANIFEST_FILENAME = (
    "imomir-plugin.json"
)

PLUGIN_INSTALL_STATUS = {}
PLUGIN_INSTALL_STATUS_LOCK = (
    threading.Lock()
)

PERSISTENT_PLUGIN_COMMANDS = {
    "upscale",
    "upscale_batch",
}

PLUGIN_WORKERS = {}
PLUGIN_WORKERS_LOCK = threading.Lock()


def ensure_plugin_directories():
    os.makedirs(
        PLUGIN_ROOT_DIR,
        exist_ok=True,
    )

    os.makedirs(
        PLUGIN_DOWNLOAD_DIR,
        exist_ok=True,
    )


def get_plugin_catalog_entry(
    plugin_id,
):
    clean_plugin_id = str(
        plugin_id or ""
    ).strip()

    return PLUGIN_CATALOG.get(
        clean_plugin_id
    )


def get_plugin_install_dir(
    plugin_id,
):
    clean_plugin_id = str(
        plugin_id or ""
    ).strip()

    if not clean_plugin_id:
        return ""

    return os.path.join(
        PLUGIN_ROOT_DIR,
        clean_plugin_id,
    )


def get_plugin_manifest_path(
    plugin_id,
):
    plugin_dir = (
        get_plugin_install_dir(
            plugin_id
        )
    )

    if not plugin_dir:
        return ""

    return os.path.join(
        plugin_dir,
        PLUGIN_MANIFEST_FILENAME,
    )


def load_plugin_manifest(
    plugin_id,
):
    manifest_path = (
        get_plugin_manifest_path(
            plugin_id
        )
    )

    if (
        not manifest_path
        or not os.path.exists(
            manifest_path
        )
    ):
        return None

    try:
        with open(
            manifest_path,
            "r",
            encoding="utf-8",
        ) as manifest_file:
            manifest = json.load(
                manifest_file
            )
    except (
        OSError,
        ValueError,
        TypeError,
    ):
        return None

    if (
        manifest.get("plugin_id")
        != plugin_id
    ):
        return None

    return manifest


def get_plugin_venv_dir(
    plugin_id,
):
    return os.path.join(
        get_plugin_install_dir(
            plugin_id
        ),
        ".venv",
    )


def get_plugin_python_path(
    plugin_id,
):
    venv_dir = get_plugin_venv_dir(
        plugin_id
    )

    if os.name == "nt":
        return os.path.join(
            venv_dir,
            "Scripts",
            "python.exe",
        )

    return os.path.join(
        venv_dir,
        "bin",
        "python",
    )

def get_plugin_development_dir(
    plugin_id,
):
    clean_plugin_id = str(
        plugin_id or ""
    ).strip()

    if not clean_plugin_id:
        return ""

    return os.path.join(
        BUNDLE_BASE_DIR,
        "plugin_src",
        clean_plugin_id,
    )


def _get_plugin_python_path_for_directory(
    plugin_dir,
):
    venv_dir = os.path.join(
        plugin_dir,
        ".venv",
    )

    if os.name == "nt":
        return os.path.join(
            venv_dir,
            "Scripts",
            "python.exe",
        )

    return os.path.join(
        venv_dir,
        "bin",
        "python",
    )


def _load_plugin_manifest_from_directory(
    plugin_id,
    plugin_dir,
):
    if not plugin_dir:
        return None

    manifest_path = os.path.join(
        plugin_dir,
        PLUGIN_MANIFEST_FILENAME,
    )

    if not os.path.exists(
        manifest_path
    ):
        return None

    try:
        with open(
            manifest_path,
            "r",
            encoding="utf-8",
        ) as manifest_file:
            manifest = json.load(
                manifest_file
            )
    except Exception:
        return None

    if (
        manifest.get("plugin_id")
        != plugin_id
    ):
        return None

    return manifest


def get_plugin_runtime_context(
    plugin_id,
):
    runtime_candidates = [
        (
            "installed",
            get_plugin_install_dir(
                plugin_id
            ),
        ),
    ]

    if not getattr(
        sys,
        "frozen",
        False,
    ):
        runtime_candidates.append(
            (
                "development",
                get_plugin_development_dir(
                    plugin_id
                ),
            )
        )

    for runtime_source, plugin_dir in runtime_candidates:
        manifest = (
            _load_plugin_manifest_from_directory(
                plugin_id,
                plugin_dir,
            )
        )

        if not manifest:
            continue

        entrypoint = str(
            manifest.get(
                "entrypoint",
                "",
            )
            or ""
        ).strip()

        if not entrypoint:
            continue

        entrypoint_path = os.path.join(
            plugin_dir,
            entrypoint,
        )

        plugin_python = (
            _get_plugin_python_path_for_directory(
                plugin_dir
            )
        )

        if (
            not os.path.exists(
                entrypoint_path
            )
            or not os.path.exists(
                plugin_python
            )
        ):
            continue

        return {
            "plugin_id": plugin_id,
            "plugin_dir": plugin_dir,
            "plugin_python": plugin_python,
            "entrypoint_path": (
                entrypoint_path
            ),
            "manifest": manifest,
            "runtime_source": (
                runtime_source
            ),
            "development": (
                runtime_source
                == "development"
            ),
        }

    return None

def get_plugin_status(
    plugin_id,
):
    catalog_entry = (
        get_plugin_catalog_entry(
            plugin_id
        )
    )

    if not catalog_entry:
        return {
            "plugin_id": plugin_id,
            "known": False,
            "installed": False,
            "ready": False,
            "status": "unknown",
            "message": (
                "Unknown plugin."
            ),
        }

    runtime_context = (
        get_plugin_runtime_context(
            plugin_id
        )
    )

    if runtime_context:
        manifest = runtime_context[
            "manifest"
        ]

        return {
            **catalog_entry,

            "known": True,
            "installed": True,
            "ready": True,
            "status": "ready",

            "development": (
                runtime_context[
                    "development"
                ]
            ),

            "runtime_source": (
                runtime_context[
                    "runtime_source"
                ]
            ),

            "version": str(
                manifest.get(
                    "version",
                    "",
                )
                or ""
            ),

            "manifest": manifest,

            "message": (
                "Development plugin is ready."
                if runtime_context[
                    "development"
                ]
                else "Plugin is ready."
            ),
        }


    manifest = load_plugin_manifest(
        plugin_id
    )

    if not manifest:
        return {
            **catalog_entry,
            "known": True,
            "installed": False,
            "ready": False,
            "status": "not_installed",
            "version": "",
            "message": (
                "Plugin is not installed."
            ),
        }

    plugin_dir = (
        get_plugin_install_dir(
            plugin_id
        )
    )

    entrypoint = str(
        manifest.get(
            "entrypoint",
            "",
        )
        or ""
    ).strip()

    entrypoint_path = os.path.join(
        plugin_dir,
        entrypoint,
    )

    plugin_python = (
        get_plugin_python_path(
            plugin_id
        )
    )

    ready = bool(
        entrypoint
        and os.path.exists(
            entrypoint_path
        )
        and os.path.exists(
            plugin_python
        )
    )

    return {
        **catalog_entry,
        "known": True,
        "installed": True,
        "ready": ready,
        "status": (
            "ready"
            if ready
            else "incomplete"
        ),
        "version": str(
            manifest.get(
                "version",
                "",
            )
            or ""
        ),
        "manifest": manifest,
        "message": (
            "Plugin is ready."
            if ready
            else (
                "Plugin files are present, "
                "but the runtime is incomplete."
            )
        ),
    }

def get_ready_plugins_by_type(
    plugin_type,
):
    clean_plugin_type = str(
        plugin_type or ""
    ).strip().lower()

    ready_plugins = []

    for plugin_id, catalog_entry in (
        PLUGIN_CATALOG.items()
    ):
        catalog_type = str(
            catalog_entry.get(
                "plugin_type",
                "",
            )
            or ""
        ).strip().lower()

        if (
            catalog_type
            != clean_plugin_type
        ):
            continue

        status = get_plugin_status(
            plugin_id
        )

        if status.get("ready"):
            ready_plugins.append(
                status
            )

    return ready_plugins




def _set_install_status(
    plugin_id,
    **values,
):
    with PLUGIN_INSTALL_STATUS_LOCK:
        current_status = dict(
            PLUGIN_INSTALL_STATUS.get(
                plugin_id,
                {},
            )
        )

        current_status.update(
            values
        )

        PLUGIN_INSTALL_STATUS[
            plugin_id
        ] = current_status


def get_plugin_install_status(
    plugin_id,
):
    with PLUGIN_INSTALL_STATUS_LOCK:
        current_status = dict(
            PLUGIN_INSTALL_STATUS.get(
                plugin_id,
                {},
            )
        )

    if not current_status:
        return {
            "plugin_id": plugin_id,
            "is_running": False,
            "stage": "Idle",
            "message": (
                "No installation is running."
            ),
            "error": "",
        }

    return current_status


def _safe_extract_zip(
    zip_path,
    destination_dir,
):
    destination_real = os.path.realpath(
        destination_dir
    )

    with zipfile.ZipFile(
        zip_path,
        "r",
    ) as archive:
        for member in archive.infolist():
            member_path = os.path.realpath(
                os.path.join(
                    destination_dir,
                    member.filename,
                )
            )

            if (
                member_path
                != destination_real
                and not member_path.startswith(
                    destination_real
                    + os.sep
                )
            ):
                raise ValueError(
                    "Plugin ZIP contains an "
                    "unsafe path."
                )

        archive.extractall(
            destination_dir
        )


def _find_manifest_root(
    extracted_dir,
):
    manifest_paths = []

    for root, dirs, files in os.walk(
        extracted_dir
    ):
        dirs[:] = [
            directory
            for directory in dirs
            if directory != ".venv"
        ]

        if (
            PLUGIN_MANIFEST_FILENAME
            in files
        ):
            manifest_paths.append(
                os.path.join(
                    root,
                    PLUGIN_MANIFEST_FILENAME,
                )
            )

    if len(manifest_paths) != 1:
        raise ValueError(
            "Plugin package must contain "
            "exactly one imomir-plugin.json."
        )

    return os.path.dirname(
        manifest_paths[0]
    )


def _validate_manifest(
    plugin_id,
    plugin_root,
):
    manifest_path = os.path.join(
        plugin_root,
        PLUGIN_MANIFEST_FILENAME,
    )

    with open(
        manifest_path,
        "r",
        encoding="utf-8",
    ) as manifest_file:
        manifest = json.load(
            manifest_file
        )

    if (
        int(
            manifest.get(
                "schema_version",
                0,
            )
            or 0
        )
        != 1
    ):
        raise ValueError(
            "Unsupported plugin manifest "
            "schema version."
        )

    if (
        manifest.get("plugin_id")
        != plugin_id
    ):
        raise ValueError(
            "Downloaded plugin ID does not "
            "match the requested plugin."
        )

    entrypoint = str(
        manifest.get(
            "entrypoint",
            "",
        )
        or ""
    ).strip()

    if not entrypoint:
        raise ValueError(
            "Plugin manifest does not "
            "define an entrypoint."
        )

    entrypoint_path = os.path.join(
        plugin_root,
        entrypoint,
    )

    if not os.path.exists(
        entrypoint_path
    ):
        raise ValueError(
            "Plugin entrypoint does not exist."
        )

    return manifest


def _command_exists(
    command,
):
    try:
        result = subprocess.run(
            command + ["--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=15,
            check=False,
        )

        return result.returncode == 0

    except (
        OSError,
        subprocess.SubprocessError,
    ):
        return False


def _resolve_python_command():
    configured_python = str(
        os.environ.get(
            "IMOMIR_PLUGIN_PYTHON",
            "",
        )
        or ""
    ).strip()

    candidates = []

    if configured_python:
        candidates.append(
            [configured_python]
        )

    if not getattr(
        sys,
        "frozen",
        False,
    ):
        candidates.append(
            [sys.executable]
        )

    if os.name == "nt":
        candidates.extend(
            [
                ["py", "-3.12"],
                ["py", "-3.11"],
                ["python"],
            ]
        )

    else:
        candidates.extend(
            [
                ["python3.12"],
                ["python3.11"],
                ["python3"],
                ["python"],
            ]
        )

    for command in candidates:
        if _command_exists(
            command
        ):
            return command

    raise RuntimeError(
        "No compatible Python installation "
        "was found for plugin installation. "
        "Set IMOMIR_PLUGIN_PYTHON to the "
        "Python executable to use."
    )


def _install_plugin_requirements(
    plugin_id,
    manifest,
):
    plugin_dir = (
        get_plugin_install_dir(
            plugin_id
        )
    )

    venv_dir = get_plugin_venv_dir(
        plugin_id
    )

    bootstrap_python = (
        _resolve_python_command()
    )

    _set_install_status(
        plugin_id,
        stage="Creating Plugin Runtime",
        message=(
            "Creating isolated Python "
            "environment."
        ),
    )

    if os.path.exists(
        venv_dir
    ):
        shutil.rmtree(
            venv_dir
        )

    subprocess.run(
        bootstrap_python
        + [
            "-m",
            "venv",
            venv_dir,
        ],
        check=True,
    )

    plugin_python = (
        get_plugin_python_path(
            plugin_id
        )
    )

    requirements_filename = str(
        manifest.get(
            "requirements_file",
            "requirements.txt",
        )
        or ""
    ).strip()

    if not requirements_filename:
        return

    requirements_path = os.path.join(
        plugin_dir,
        requirements_filename,
    )

    if not os.path.exists(
        requirements_path
    ):
        return

    _set_install_status(
        plugin_id,
        stage="Installing Requirements",
        message=(
            "Installing plugin Python "
            "requirements."
        ),
    )

    subprocess.run(
        [
            plugin_python,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "-r",
            requirements_path,
        ],
        check=True,
    )


def _download_latest_release_asset(
    plugin_id,
    catalog_entry,
):
    owner = catalog_entry[
        "github_owner"
    ]

    repo = catalog_entry[
        "github_repo"
    ]

    tag_prefix = str(
        catalog_entry.get(
            "release_tag_prefix",
            "",
        )
        or ""
    ).strip()

    asset_name = catalog_entry[
        "release_asset_name"
    ]

    releases_url = (
        "https://api.github.com/repos/"
        f"{owner}/{repo}/releases"
    )

    _set_install_status(
        plugin_id,
        stage="Checking GitHub",
        message=(
            "Checking the latest plugin "
            "release."
        ),
    )

    releases_response = requests.get(
        releases_url,
        params={
            "per_page": 100,
        },
        headers={
            "Accept": (
                "application/vnd.github+json"
            ),
            "User-Agent": (
                "iMomir-Plugin-Installer"
            ),
        },
        timeout=30,
    )

    releases_response.raise_for_status()

    releases_data = (
        releases_response.json()
    )

    if not isinstance(
        releases_data,
        list,
    ):
        raise RuntimeError(
            "GitHub returned an invalid "
            "release list."
        )

    matching_release = None

    for release in releases_data:
        if release.get("draft"):
            continue

        tag_name = str(
            release.get(
                "tag_name",
                "",
            )
            or ""
        ).strip()

        if (
            tag_prefix
            and not tag_name.startswith(
                tag_prefix
            )
        ):
            continue

        matching_release = release
        break

    if not matching_release:
        raise RuntimeError(
            "No compatible plugin release "
            "was found on GitHub."
        )

    matching_asset = None

    for asset in matching_release.get(
        "assets",
        [],
    ):
        if asset.get("name") == asset_name:
            matching_asset = asset
            break

    if not matching_asset:
        release_tag = str(
            matching_release.get(
                "tag_name",
                "",
            )
            or ""
        )

        raise RuntimeError(
            f"Plugin release {release_tag} "
            f"does not contain {asset_name}."
        )

    download_url = matching_asset.get(
        "browser_download_url"
    )

    if not download_url:
        raise RuntimeError(
            "GitHub release asset has no "
            "download URL."
        )

    destination_path = os.path.join(
        PLUGIN_DOWNLOAD_DIR,
        f"{plugin_id}.zip",
    )

    release_tag = str(
        matching_release.get(
            "tag_name",
            "",
        )
        or ""
    ).strip()

    _set_install_status(
        plugin_id,
        stage="Downloading",
        message=(
            "Downloading plugin package "
            f"{release_tag} from GitHub."
        ),
    )

    with requests.get(
        download_url,
        stream=True,
        timeout=(30, 300),
    ) as download_response:
        download_response.raise_for_status()

        with open(
            destination_path,
            "wb",
        ) as output_file:
            for chunk in (
                download_response.iter_content(
                    chunk_size=1024 * 1024
                )
            ):
                if chunk:
                    output_file.write(
                        chunk
                    )

    return destination_path


def install_plugin(
    plugin_id,
):
    ensure_plugin_directories()

    _stop_plugin_worker(
        plugin_id
    )

    catalog_entry = (
        get_plugin_catalog_entry(
            plugin_id
        )
    )

    if not catalog_entry:
        raise ValueError(
            "Unknown plugin."
        )

    package_path = (
        _download_latest_release_asset(
            plugin_id,
            catalog_entry,
        )
    )

    _set_install_status(
        plugin_id,
        stage="Extracting",
        message=(
            "Extracting plugin package."
        ),
    )

    staging_dir = tempfile.mkdtemp(
        prefix=(
            f"imomir-plugin-{plugin_id}-"
        )
    )

    plugin_dir = (
        get_plugin_install_dir(
            plugin_id
        )
    )

    backup_dir = (
        plugin_dir + ".backup"
    )

    try:
        _safe_extract_zip(
            package_path,
            staging_dir,
        )

        package_root = (
            _find_manifest_root(
                staging_dir
            )
        )

        manifest = (
            _validate_manifest(
                plugin_id,
                package_root,
            )
        )

        if os.path.exists(
            backup_dir
        ):
            shutil.rmtree(
                backup_dir
            )

        if os.path.exists(
            plugin_dir
        ):
            os.replace(
                plugin_dir,
                backup_dir,
            )

        shutil.copytree(
            package_root,
            plugin_dir,
        )

        _install_plugin_requirements(
            plugin_id,
            manifest,
        )

        if os.path.exists(
            backup_dir
        ):
            shutil.rmtree(
                backup_dir
            )

    except Exception:
        if os.path.exists(
            plugin_dir
        ):
            shutil.rmtree(
                plugin_dir
            )

        if os.path.exists(
            backup_dir
        ):
            os.replace(
                backup_dir,
                plugin_dir,
            )

        raise

    finally:
        shutil.rmtree(
            staging_dir,
            ignore_errors=True,
        )

    return get_plugin_status(
        plugin_id
    )


def _install_plugin_worker(
    plugin_id,
):
    try:
        _set_install_status(
            plugin_id,
            is_running=True,
            stage="Starting",
            message=(
                "Starting plugin installation."
            ),
            error="",
        )

        plugin_status = install_plugin(
            plugin_id
        )

        _set_install_status(
            plugin_id,
            is_running=False,
            stage="Complete",
            message=(
                "Plugin installation complete."
            ),
            error="",
            plugin_status=plugin_status,
        )

    except Exception as exc:
        _set_install_status(
            plugin_id,
            is_running=False,
            stage="Failed",
            message=(
                "Plugin installation failed."
            ),
            error=str(exc),
        )


def start_plugin_install(
    plugin_id,
):
    catalog_entry = (
        get_plugin_catalog_entry(
            plugin_id
        )
    )

    if not catalog_entry:
        raise ValueError(
            "Unknown plugin."
        )

    current_status = (
        get_plugin_install_status(
            plugin_id
        )
    )

    if current_status.get(
        "is_running"
    ):
        return current_status

    worker = threading.Thread(
        target=_install_plugin_worker,
        args=(plugin_id,),
        daemon=True,
        name=(
            "imomir-plugin-install-"
            f"{plugin_id}"
        ),
    )

    worker.start()

    return get_plugin_install_status(
        plugin_id
    )


def _stop_plugin_worker(plugin_id):
    with PLUGIN_WORKERS_LOCK:
        worker = PLUGIN_WORKERS.pop(
            plugin_id,
            None,
        )

    if not worker:
        return

    process = worker["process"]

    if process.poll() is None:
        process.terminate()

        try:
            process.wait(timeout=2)

        except subprocess.TimeoutExpired:
            process.kill()


def uninstall_plugin(
    plugin_id,
):
    if not get_plugin_catalog_entry(
        plugin_id
    ):
        raise ValueError(
            "Unknown plugin."
        )

    install_status = (
        get_plugin_install_status(
            plugin_id
        )
    )

    if install_status.get(
        "is_running"
    ):
        raise RuntimeError(
            "Plugin installation is still running."
        )

    _stop_plugin_worker(
        plugin_id
    )

    plugin_dir = (
        get_plugin_install_dir(
            plugin_id
        )
    )

    if os.path.exists(
        plugin_dir
    ):
        shutil.rmtree(
            plugin_dir
        )

    download_path = os.path.join(
        PLUGIN_DOWNLOAD_DIR,
        f"{plugin_id}.zip",
    )

    if os.path.exists(
        download_path
    ):
        os.remove(
            download_path
        )

    with PLUGIN_INSTALL_STATUS_LOCK:
        PLUGIN_INSTALL_STATUS.pop(
            plugin_id,
            None,
        )

    return get_plugin_status(
        plugin_id
    )


def _stop_all_plugin_workers():
    with PLUGIN_WORKERS_LOCK:
        plugin_ids = list(
            PLUGIN_WORKERS
        )

    for plugin_id in plugin_ids:
        _stop_plugin_worker(
            plugin_id
        )


def _get_plugin_worker(
    plugin_id,
    runtime_context,
):
    with PLUGIN_WORKERS_LOCK:
        worker = PLUGIN_WORKERS.get(
            plugin_id
        )

        if (
            worker
            and worker["process"].poll()
            is None
        ):
            return worker

        process = subprocess.Popen(
            [
                runtime_context[
                    "plugin_python"
                ],
                runtime_context[
                    "entrypoint_path"
                ],
                "worker",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            bufsize=1,
        )

        worker = {
            "process": process,
            "lock": threading.Lock(),
        }

        PLUGIN_WORKERS[plugin_id] = (
            worker
        )

        return worker


def _run_persistent_plugin_json(
    plugin_id,
    command,
    payload,
    timeout,
    runtime_context,
):
    worker = _get_plugin_worker(
        plugin_id,
        runtime_context,
    )

    with worker["lock"]:
        process = worker["process"]

        try:
            process.stdin.write(
                json.dumps({
                    "command": command,
                    "payload": payload or {},
                })
                + "\n"
            )

            process.stdin.flush()

        except (
            BrokenPipeError,
            OSError,
        ):
            _stop_plugin_worker(
                plugin_id
            )

            raise RuntimeError(
                "Plugin worker stopped "
                "unexpectedly."
            )

        response = {}

        def read_response():
            response["text"] = (
                process.stdout.readline()
            )

        reader = threading.Thread(
            target=read_response,
            daemon=True,
        )

        reader.start()
        reader.join(timeout)

        if reader.is_alive():
            _stop_plugin_worker(
                plugin_id
            )

            raise RuntimeError(
                "Plugin worker timed out."
            )

        response_text = str(
            response.get("text")
            or ""
        ).strip()

        if not response_text:
            _stop_plugin_worker(
                plugin_id
            )

            raise RuntimeError(
                "Plugin worker exited "
                "without a response."
            )

        result = json.loads(
            response_text
        )

        if not result.get("ok"):
            raise RuntimeError(
                result.get("error")
                or "Plugin worker failed."
            )

        return result.get(
            "result",
            {},
        )


atexit.register(
    _stop_all_plugin_workers
)

def run_plugin_json(
    plugin_id,
    command,
    payload=None,
    timeout=300,
):
    runtime_context = (
        get_plugin_runtime_context(
            plugin_id
        )
    )

    if not runtime_context:
        raise RuntimeError(
            "Plugin is not ready."
        )

    if command in PERSISTENT_PLUGIN_COMMANDS:
        return _run_persistent_plugin_json(
            plugin_id,
            command,
            payload,
            timeout,
            runtime_context,
        )

    process = subprocess.run(
        [
            runtime_context[
                "plugin_python"
            ],
            runtime_context[
                "entrypoint_path"
            ],
            command,
        ],
        input=json.dumps(
            payload or {}
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
        check=False,
    )

    if process.returncode != 0:
        raise RuntimeError(
            process.stderr.strip()
            or (
                "Plugin command failed "
                f"with exit code "
                f"{process.returncode}."
            )
        )

    output_text = (
        process.stdout.strip()
    )

    if not output_text:
        return {}

    return json.loads(
        output_text
    )