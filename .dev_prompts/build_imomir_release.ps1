$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "========================================"
Write-Host "       iMomir Windows Release Builder"
Write-Host "========================================"
Write-Host ""

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptRoot

$SettingsPath = Join-Path `
    $RepoRoot `
    "settings.py"

$SpecPath = Join-Path `
    $RepoRoot `
    "iMomir.spec"

$StartBatSource = Join-Path `
    $ScriptRoot `
    "Start_iMomir.bat"

$BuildAppDir = Join-Path `
    $RepoRoot `
    "build\iMomir"

$DistRoot = Join-Path `
    $RepoRoot `
    "dist"

$AppDistDir = Join-Path `
    $DistRoot `
    "iMomir"


Write-Host "[1/6] Reading release version..."

if (-not (Test-Path $SettingsPath)) {
    throw "settings.py was not found: $SettingsPath"
}

$SettingsText = Get-Content `
    $SettingsPath `
    -Raw

$VersionMatch = [regex]::Match(
    $SettingsText,
    '(?m)^\s*APP_VERSION\s*=\s*"([^"]+)"'
)

if (-not $VersionMatch.Success) {
    throw "APP_VERSION could not be found in settings.py."
}

$Version = $VersionMatch.Groups[1].Value.Trim()

if (-not $Version) {
    throw "APP_VERSION is blank."
}

$OutputZip = Join-Path `
    $DistRoot `
    "iMomir_v${Version}_Windows.zip"

Write-Host "      Version: $Version"
Write-Host "      Output:  $OutputZip"
Write-Host ""


Write-Host "[2/6] Checking release files..."

if (-not (Test-Path $SpecPath)) {
    throw "iMomir.spec was not found: $SpecPath"
}

if (-not (Test-Path $StartBatSource)) {
    throw "Start_iMomir.bat was not found: $StartBatSource"
}

$PyInstallerCommand = Get-Command `
    "pyinstaller" `
    -ErrorAction SilentlyContinue

if (-not $PyInstallerCommand) {
    throw "PyInstaller was not found in the current environment."
}

$PythonCommand = Get-Command `
    "python" `
    -ErrorAction SilentlyContinue

if (-not $PythonCommand) {
    throw "Python was not found in the current environment."
}

Write-Host "      PyInstaller: $($PyInstallerCommand.Source)"
Write-Host "      Python:      $($PythonCommand.Source)"
Write-Host "      Spec:        $SpecPath"
Write-Host "      Launcher:    $StartBatSource"
Write-Host ""


Write-Host "[3/6] Cleaning previous build output..."

if (Test-Path $BuildAppDir) {
    Write-Host "      Removing build\iMomir"

    Remove-Item `
        $BuildAppDir `
        -Recurse `
        -Force
}

if (Test-Path $AppDistDir) {
    Write-Host "      Removing dist\iMomir"

    Remove-Item `
        $AppDistDir `
        -Recurse `
        -Force
}

if (Test-Path $OutputZip) {
    Write-Host "      Removing old release ZIP"

    Remove-Item `
        $OutputZip `
        -Force
}

Write-Host "      Previous output cleared."
Write-Host ""


Write-Host "[4/6] Building iMomir with PyInstaller..."
Write-Host ""
Write-Host "      pyinstaller --noconfirm --clean iMomir.spec"
Write-Host ""
Write-Host "----------------------------------------"

Push-Location $RepoRoot

try {
    & pyinstaller `
        --noconfirm `
        --clean `
        "iMomir.spec"

    if ($LASTEXITCODE -ne 0) {
        throw "PyInstaller failed with exit code $LASTEXITCODE."
    }
}
finally {
    Pop-Location
}

Write-Host "----------------------------------------"
Write-Host ""

$AppExePath = Join-Path `
    $AppDistDir `
    "iMomir.exe"

if (-not (Test-Path $AppExePath)) {
    throw "Build completed but iMomir.exe was not found: $AppExePath"
}

Write-Host "      PyInstaller build complete."
Write-Host "      Found: $AppExePath"
Write-Host ""


Write-Host "[5/6] Adding Start_iMomir.bat..."

$StartBatDestination = Join-Path `
    $AppDistDir `
    "Start_iMomir.bat"

Copy-Item `
    $StartBatSource `
    $StartBatDestination `
    -Force

if (-not (Test-Path $StartBatDestination)) {
    throw "Start_iMomir.bat was not copied successfully."
}

Write-Host "      Copied:"
Write-Host "      $StartBatDestination"
Write-Host ""


Write-Host "[6/6] Creating release ZIP..."

$ReleaseFiles = Get-ChildItem `
    $AppDistDir `
    -Recurse `
    -File

$ReleaseBytes = (
    $ReleaseFiles |
    Measure-Object `
        -Property Length `
        -Sum
).Sum

$ReleaseSizeMB = [math]::Round(
    $ReleaseBytes / 1MB,
    2
)

Write-Host "      Files: $($ReleaseFiles.Count)"
Write-Host "      Uncompressed size: $ReleaseSizeMB MB"
Write-Host ""
Write-Host "      Compressing..."
Write-Host ""

$ZipHelperPath = Join-Path `
    $RepoRoot `
    "build\zip_imomir_release.py"

$ZipHelperCode = @'
import os
import sys
import zipfile


source_dir = os.path.abspath(sys.argv[1])
output_zip = os.path.abspath(sys.argv[2])

archive_root = os.path.basename(
    source_dir.rstrip("\\/")
)


def windows_extended_path(path):
    path = os.path.abspath(path)

    if (
        os.name == "nt"
        and not path.startswith("\\\\?\\")
    ):
        return "\\\\?\\" + path

    return path


files = []

for directory, _, filenames in os.walk(source_dir):
    for filename in filenames:
        files.append(
            os.path.join(
                directory,
                filename,
            )
        )


total_files = len(files)

print(
    f"      Preparing to compress "
    f"{total_files} files."
)

with zipfile.ZipFile(
    output_zip,
    mode="w",
    compression=zipfile.ZIP_DEFLATED,
    compresslevel=6,
    allowZip64=True,
) as archive:

    for index, file_path in enumerate(
        files,
        start=1,
    ):
        relative_path = os.path.relpath(
            file_path,
            source_dir,
        )

        archive_path = os.path.join(
            archive_root,
            relative_path,
        ).replace(
            "\\",
            "/",
        )

        archive.write(
            windows_extended_path(
                file_path
            ),
            archive_path,
        )

        if (
            index == 1
            or index % 250 == 0
            or index == total_files
        ):
            percent = (
                index
                / total_files
                * 100
            )

            print(
                f"      {index}/{total_files} "
                f"files ({percent:.0f}%)"
            )


print(
    "      ZIP compression finished."
)
'@

Set-Content `
    -Path $ZipHelperPath `
    -Value $ZipHelperCode `
    -Encoding UTF8

try {
    & $PythonCommand.Source `
        $ZipHelperPath `
        $AppDistDir `
        $OutputZip

    $ZipExitCode = $LASTEXITCODE

    if ($ZipExitCode -ne 0) {
        throw "ZIP creation failed with exit code $ZipExitCode."
    }
}
finally {
    if (Test-Path $ZipHelperPath) {
        Remove-Item `
            $ZipHelperPath `
            -Force
    }
}

if (-not (Test-Path $OutputZip)) {
    throw "Release ZIP was not created."
}

$ZipSizeMB = [math]::Round(
    (Get-Item $OutputZip).Length / 1MB,
    2
)

Write-Host "      Compression complete."
Write-Host ""
Write-Host "========================================"
Write-Host "       RELEASE BUILD COMPLETE"
Write-Host "========================================"
Write-Host ""
Write-Host "iMomir version:"
Write-Host "  $Version"
Write-Host ""
Write-Host "Release ZIP:"
Write-Host "  $OutputZip"
Write-Host ""
Write-Host "ZIP size:"
Write-Host "  $ZipSizeMB MB"
Write-Host ""
Write-Host "GitHub tag:"
Write-Host "  v$Version"
Write-Host ""
Write-Host "GitHub release title:"
Write-Host "  iMomir v$Version"
Write-Host ""