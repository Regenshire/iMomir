import os
import sys

def get_bundle_base_dir():
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return sys._MEIPASS
    return os.path.dirname(os.path.abspath(__file__))

def get_runtime_base_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))

BUNDLE_BASE_DIR = get_bundle_base_dir()
RUNTIME_BASE_DIR = get_runtime_base_dir()

DATABASE_PATH = os.path.join(RUNTIME_BASE_DIR, "cards.db")

DATA_ROOT_DIR = os.path.join(RUNTIME_BASE_DIR, "data")
DATA_DOWNLOAD_DIR = os.path.join(DATA_ROOT_DIR, "downloads")
SCRYFALL_DOWNLOAD_DIR = os.path.join(DATA_ROOT_DIR, "scryfall")
IMAGE_CACHE_DIR = os.path.join(DATA_ROOT_DIR, "image_cache")
CHAOS_IMAGE_CACHE_DIR = os.path.join(DATA_ROOT_DIR, "chaos_image_cache")
CHAOS_TEMP_CACHE_DIR = os.path.join(DATA_ROOT_DIR, "chaos_temp_cache")

ATOMIC_CARDS_PATH = os.path.join(DATA_DOWNLOAD_DIR, "AtomicCards.json")
SET_LIST_PATH = os.path.join(DATA_DOWNLOAD_DIR, "SetList.json")
ALL_PRINTINGS_PATH = os.path.join(DATA_DOWNLOAD_DIR, "AllPrintings.json")
ALL_PRINTINGS_GZ_PATH = os.path.join(DATA_DOWNLOAD_DIR, "AllPrintings.json.gz")

SCRYFALL_DEFAULT_CARDS_PATH = os.path.join(SCRYFALL_DOWNLOAD_DIR, "default-cards.json")
ALL_PRICES_TODAY_PATH = os.path.join(DATA_DOWNLOAD_DIR, "AllPricesToday.json")
ALL_PRICES_TODAY_ZIP_PATH = os.path.join(DATA_DOWNLOAD_DIR, "AllPricesToday.json.zip")

SET_BOOSTER_CONTENTS_CSV_PATH = os.path.join(DATA_DOWNLOAD_DIR, "setBoosterContents.csv")
SET_BOOSTER_CONTENT_WEIGHTS_CSV_PATH = os.path.join(DATA_DOWNLOAD_DIR, "setBoosterContentWeights.csv")
SET_BOOSTER_SHEET_CARDS_CSV_PATH = os.path.join(DATA_DOWNLOAD_DIR, "setBoosterSheetCards.csv")
SET_BOOSTER_SHEETS_CSV_PATH = os.path.join(DATA_DOWNLOAD_DIR, "setBoosterSheets.csv")

LOG_PATH = os.path.join(RUNTIME_BASE_DIR, "imomir_debug.log")

def get_template_dir():
    return os.path.join(BUNDLE_BASE_DIR, "templates")


def get_static_dir():
    return os.path.join(BUNDLE_BASE_DIR, "static")


def get_pack_art_dir(static_folder):
    return os.path.join(static_folder, "img", "pack_art")