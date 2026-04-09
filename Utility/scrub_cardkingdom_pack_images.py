import os
import re
import sqlite3
import random
import time
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


# ============================================================
# Paths
# ============================================================

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

DATABASE_PATH = PROJECT_ROOT / "cards.db"
STATIC_PACK_ART_DIR = PROJECT_ROOT / "static" / "img" / "pack_art"
TEMP_PACK_ART_DIR = PROJECT_ROOT / "utility" / "temp" / "img" / "pack_art"

# ============================================================
# Card Kingdom config
# ============================================================

CARDKINGDOM_SEARCH_URL = "https://www.cardkingdom.com/catalog/search"
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) iMomirPackArtScrubber/1.0",
    "Accept-Language": "en-US,en;q=0.9",
}

# Set the Max number of images to look for in a single run
MAX_TARGETS = 500 # Set to 0 for no limit

REQUEST_DELAY_SECONDS_MIN = 3.0
REQUEST_DELAY_SECONDS_MAX = 19.0
REQUEST_TIMEOUT_SECONDS = 15

FORBIDDEN_COOLDOWN_SECONDS = 313
MAX_CONSECUTIVE_403S = 3



# ============================================================
# Booster normalization
# ============================================================

# These are your internal booster names from chaos_booster_variants.
# The value is:
#   (output_filename, search_phrase_suffixes_in_priority_order)
BOOSTER_SEARCH_MAP = {
    "default": (
        "default",
        [
            "booster pack",
            "booster",
        ],
    ),
    "draft": (
        "draft",
        [
            "draft booster pack",
            "draft booster",
        ],
    ),
    "set": (
        "set",
        [
            "set booster pack",
            "set booster",
        ],
    ),
    "play": (
        "play",
        [
            "play booster pack",
            "play booster",
        ],
    ),
    "collector": (
        "collector",
        [
            "collector booster pack",
            "collector booster",
        ],
    ),
    "collector-special": (
        "collector-special",
        [
            "collector booster pack",
            "collector booster",
            "collector special booster pack",
        ],
    ),
    "collector-sample": (
        "collector-sample",
        [
            "collector sample pack",
            "collector booster sample pack",
        ],
    ),
    "jumpstart": (
        "jumpstart",
        [
            "jumpstart booster pack",
            "jumpstart booster",
        ],
    ),
    "jumpstart-v2": (
        "jumpstart-v2",
        [
            "jumpstart booster pack",
            "jumpstart booster",
        ],
    ),
    "premium": (
        "premium",
        [
            "premium booster pack",
            "premium booster",
        ],
    ),
    "vip": (
        "vip",
        [
            "vip booster pack",
            "vip booster",
        ],
    ),
    "core": (
        "core",
        [
            "booster pack",
            "booster",
            "core booster pack",
        ],
    ),
    "six": (
        "six",
        [
            "six card booster pack",
            "6 card booster pack",
            "booster pack",
        ],
    ),
}

VALID_OUTPUT_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}

SESSION = requests.Session()
SESSION.headers.update(REQUEST_HEADERS)

CONSECUTIVE_403_COUNT = 0

# ============================================================
# Helpers
# ============================================================

def sleep_with_jitter() -> None:
    delay_seconds = random.uniform(REQUEST_DELAY_SECONDS_MIN, REQUEST_DELAY_SECONDS_MAX)
    print(f"    WAIT:         sleeping {delay_seconds:.1f}s")
    time.sleep(delay_seconds)


def handle_403_and_maybe_abort() -> None:
    global CONSECUTIVE_403_COUNT

    CONSECUTIVE_403_COUNT += 1

    print(f"    403 DETECTED: consecutive_403s={CONSECUTIVE_403_COUNT}")
    print(f"    COOLDOWN:     sleeping {FORBIDDEN_COOLDOWN_SECONDS}s before retrying")

    time.sleep(FORBIDDEN_COOLDOWN_SECONDS)

    if CONSECUTIVE_403_COUNT >= MAX_CONSECUTIVE_403S:
        raise RuntimeError(
            f"Aborting run after {CONSECUTIVE_403_COUNT} consecutive 403 responses."
        )


def reset_403_counter() -> None:
    global CONSECUTIVE_403_COUNT
    CONSECUTIVE_403_COUNT = 0

def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DATABASE_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def safe_slug(value: str) -> str:
    value = (value or "").strip().lower()
    value = value.replace("&", "and")
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = re.sub(r"-{2,}", "-", value)
    return value.strip("-")


def normalize_text(value: str) -> str:
    value = (value or "").strip().lower()
    value = value.replace("’", "'")
    value = value.replace("—", "-")
    value = re.sub(r"\s+", " ", value)
    return value

def normalize_cardkingdom_image_url(image_url: str) -> str:
    value = (image_url or "").strip()

    if not value:
        return ""

    # Card Kingdom search results often use thumbnail images.
    # Convert:
    #   ...-thumb.jpg
    # into:
    #   ....jpg
    value = re.sub(r"-thumb(\.[a-z0-9]+)$", r"\1", value, flags=re.IGNORECASE)

    return value

def get_base_set_name(set_name: str) -> str:
    value = (set_name or "").strip()

    if not value:
        return ""

    # Remove known suffixes that are internal / derivative naming,
    # not the primary market-facing sealed product name.
    removable_suffixes = [
        " Eternal",
        " Commander",
        " Alchemy",
        " Promos",
        " Tokens",
        " Extras",
    ]

    for suffix in removable_suffixes:
        if value.endswith(suffix):
            return value[: -len(suffix)].strip()

    return value


def get_existing_output_filenames_for_set(set_code: str) -> set[str]:
    existing = set()

    source_dir = STATIC_PACK_ART_DIR / set_code.lower()
    if source_dir.exists():
        for file_path in source_dir.iterdir():
            if file_path.is_file() and file_path.suffix.lower() in VALID_OUTPUT_EXTENSIONS:
                existing.add(file_path.stem.lower())

    temp_dir = TEMP_PACK_ART_DIR / set_code.lower()
    if temp_dir.exists():
        for file_path in temp_dir.iterdir():
            if file_path.is_file() and file_path.suffix.lower() in VALID_OUTPUT_EXTENSIONS:
                existing.add(file_path.stem.lower())

    return existing


def fetch_missing_pack_targets() -> list[dict]:
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
        SELECT
            s.set_code,
            s.set_name,
            cbv.booster_name
        FROM chaos_booster_variants cbv
        INNER JOIN sets s
            ON s.set_code = cbv.set_code
        WHERE cbv.booster_name IN ('draft','play','jumpstart','jumpstart-v2','set','default','collector','premium')
        GROUP BY s.set_code, s.set_name, cbv.booster_name
        ORDER BY s.release_date DESC, s.set_name COLLATE NOCASE ASC, cbv.booster_name ASC
    """

    if MAX_TARGETS > 0:
        query += f"\nLIMIT {int(MAX_TARGETS)}"

    cursor.execute(query)

    rows = cursor.fetchall()
    conn.close()

    targets = []

    for row in rows:
        set_code = (row["set_code"] or "").strip().upper()
        set_name = (row["set_name"] or "").strip()
        booster_name = (row["booster_name"] or "").strip().lower()

        # Skip collector sample packs (Card Kingdom does not carry these)
        if booster_name == "collector-sample":
            continue

        if not set_code or not set_name or not booster_name:
            continue

        if booster_name not in BOOSTER_SEARCH_MAP:
            continue

        output_filename, _ = BOOSTER_SEARCH_MAP[booster_name]
        existing_filenames = get_existing_output_filenames_for_set(set_code)

        if output_filename.lower() in existing_filenames:
            continue

        base_set_name = get_base_set_name(set_name)

        targets.append(
            {
                "set_code": set_code,
                "set_name": set_name,
                "base_set_name": base_set_name,
                "booster_name": booster_name,
                "output_filename": output_filename,
            }
        )

    return targets


def build_search_queries(set_name: str, booster_name: str) -> list[str]:
    _, suffixes = BOOSTER_SEARCH_MAP[booster_name]

    set_name_clean = (set_name or "").strip()
    set_name_norm = normalize_text(set_name_clean)

    queries = []

    for suffix in suffixes:
        suffix_clean = (suffix or "").strip()
        final_suffix = suffix_clean

        # Special handling:
        # If the set name already contains "jumpstart", do not search:
        #   "Jumpstart 2022 jumpstart booster pack"
        # Instead search:
        #   "Jumpstart 2022 booster pack"
        if booster_name in {"jumpstart", "jumpstart-v2"} and "jumpstart" in set_name_norm:
            final_suffix = re.sub(r"\bjumpstart\b\s*", "", suffix_clean, flags=re.IGNORECASE).strip()

        if final_suffix:
            queries.append(f"{set_name_clean} {final_suffix}".strip())

    # Draft fallback:
    # Many older draft boosters are just listed as "booster pack" / "booster".
    if booster_name == "draft":
        queries.append(f"{set_name_clean} booster pack")
        queries.append(f"{set_name_clean} booster")

    # Useful fallback:
    queries.append(set_name_clean)

    # Deduplicate while preserving order
    deduped = []
    seen = set()
    for query in queries:
        norm = normalize_text(query)
        if norm not in seen:
            seen.add(norm)
            deduped.append(query)

    return deduped


def search_cardkingdom_page_html(query: str) -> str | None:
    params = {
        "filter[name]": query,
        "search": "header",
    }

    print(f"    SEARCH QUERY: {query}")

    sleep_with_jitter()

    response = SESSION.get(
        CARDKINGDOM_SEARCH_URL,
        params=params,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    print(f"    SEARCH URL:   {response.url}")
    print(f"    STATUS:       {response.status_code}")

    if response.status_code == 403:
        handle_403_and_maybe_abort()
        raise requests.HTTPError(f"403 Client Error: Forbidden for url: {response.url}", response=response)

    response.raise_for_status()
    reset_403_counter()

    return response.text


def extract_candidate_images_from_search(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    candidates = []

    # Card Kingdom search pages often contain direct image URLs to their CDN/image store.
    for img in soup.find_all("img"):
        src = (img.get("src") or "").strip()
        alt = (img.get("alt") or "").strip()

        if not src:
            continue

        if "/images/magic-the-gathering/" not in src:
            continue

        full_src = urljoin("https://www.cardkingdom.com", src)
        full_src = normalize_cardkingdom_image_url(full_src)

        candidates.append(
            {
                "image_url": full_src,
                "alt": alt,
            }
        )

    # Deduplicate by URL
    deduped = []
    seen_urls = set()
    for item in candidates:
        image_url = item["image_url"]
        if image_url not in seen_urls:
            seen_urls.add(image_url)
            deduped.append(item)

    return deduped


def score_candidate(candidate: dict, set_name: str, booster_name: str) -> int:
    score = 0

    image_url = normalize_text(candidate.get("image_url", ""))
    alt = normalize_text(candidate.get("alt", ""))

    # Add a de-slugged version of the URL so slug text can match phrase text.
    image_url_spaces = image_url.replace("-", " ")
    haystack = f"{image_url} {image_url_spaces} {alt}"

    set_name_norm = normalize_text(set_name)
    set_name_slug = safe_slug(set_name)

    booster_output_name, suffixes = BOOSTER_SEARCH_MAP[booster_name]
    booster_output_slug = safe_slug(booster_output_name)

    # Exact set name phrase match
    if set_name_norm in haystack:
        score += 40

    # Strong signal: set slug appears in URL
    if set_name_slug and set_name_slug in image_url:
        score += 50

    # Booster suffix phrase/slug matches
    for suffix in suffixes:
        suffix_norm = normalize_text(suffix)
        suffix_slug = safe_slug(suffix)

        if suffix_norm in haystack:
            score += 25

        if suffix_slug and suffix_slug in image_url:
            score += 25

    # Internal output filename keyword, e.g. jumpstart / collector / draft
    if booster_output_name in haystack:
        score += 12

    if booster_output_slug and booster_output_slug in image_url:
        score += 12

    if "booster pack" in haystack:
        score += 10

    if "magic-the-gathering" in haystack:
        score += 5

    return score


def pick_best_candidate(candidates: list[dict], set_name: str, booster_name: str) -> dict | None:
    if not candidates:
        return None

    scored = []
    for candidate in candidates:
        scored.append((score_candidate(candidate, set_name, booster_name), candidate))

    scored.sort(key=lambda item: item[0], reverse=True)

    best_score, best_candidate = scored[0]

    if best_score < 30:
        return None

    return best_candidate


def download_image(image_url: str, output_path: Path) -> None:
    print(f"    DOWNLOAD URL: {image_url}")

    sleep_with_jitter()

    response = SESSION.get(
        image_url,
        timeout=REQUEST_TIMEOUT_SECONDS,
        stream=True,
    )

    print(f"    DL STATUS:    {response.status_code}")

    if response.status_code == 403:
        handle_403_and_maybe_abort()
        raise requests.HTTPError(f"403 Client Error: Forbidden for url: {image_url}", response=response)

    response.raise_for_status()
    reset_403_counter()

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "wb") as file_handle:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file_handle.write(chunk)


def try_fetch_pack_image(set_code: str, set_name: str, booster_name: str, output_filename: str) -> tuple[bool, str]:
    queries = build_search_queries(set_name, booster_name)

    print(f"    QUERY COUNT:  {len(queries)}")

    for query in queries:
        try:
            html = search_cardkingdom_page_html(query)
            candidates = extract_candidate_images_from_search(html)

            print(f"    CANDIDATES:   {len(candidates)}")

            for candidate in candidates[:10]:
                print(f"      candidate_url={candidate.get('image_url', '')} | alt={candidate.get('alt', '')}")

            best_candidate = pick_best_candidate(candidates, set_name, booster_name)

            if not best_candidate:
                print("    BEST MATCH:   none")
                continue

            image_url = best_candidate["image_url"]

            print(f"    BEST MATCH:   {image_url}")

            output_dir = TEMP_PACK_ART_DIR / set_code.lower()
            output_path = output_dir / f"{output_filename}.jpg"

            print(f"    SAVE PATH:    {output_path}")

            download_image(image_url, output_path)

            return True, image_url

        except RuntimeError:
            raise
        except Exception as exc:
            print(f"[WARN] Search failed for {set_code} {booster_name} query='{query}': {exc}")

    return False, ""


def main() -> None:
    if not DATABASE_PATH.exists():
        raise FileNotFoundError(f"cards.db was not found: {DATABASE_PATH}")

    TEMP_PACK_ART_DIR.mkdir(parents=True, exist_ok=True)

    targets = fetch_missing_pack_targets()

    print(f"Found {len(targets)} missing pack image targets.")

    hit_count = 0
    miss_count = 0

    for index, target in enumerate(targets, start=1):
        set_code = target["set_code"]
        set_name = target["set_name"]
        base_set_name = target["base_set_name"]
        booster_name = target["booster_name"]
        output_filename = target["output_filename"]

        print("")
        print("=" * 90)
        print(
            f"[{index}/{len(targets)}] SET={set_name} ({set_code}) | "
            f"BASE_SET={base_set_name} | BOOSTER={booster_name} | OUTPUT={output_filename}.jpg"
        )
        print("=" * 90)

        try:
            found, image_url = try_fetch_pack_image(
                set_code=set_code,
                set_name=base_set_name,
                booster_name=booster_name,
                output_filename=output_filename,
            )
        except RuntimeError as exc:
            print("")
            print("ABORTING RUN")
            print(str(exc))
            break

        if found:
            hit_count += 1
            print(f"  HIT  -> {image_url}")
        else:
            miss_count += 1
            print("  MISS")

    print("")
    print("Done.")
    print(f"Hits:  {hit_count}")
    print(f"Miss:  {miss_count}")
    print(f"Output: {TEMP_PACK_ART_DIR}")


if __name__ == "__main__":
    main()