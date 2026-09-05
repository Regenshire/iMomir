from card_backs import DEFAULT_CARD_BACK_KEY

APP_VERSION = "2.0.2"
GITHUB_RELEASE_OWNER = "Regenshire"
GITHUB_RELEASE_REPO = "iMomir"
GITHUB_LATEST_RELEASE_API_URL = (
    f"https://api.github.com/repos/{GITHUB_RELEASE_OWNER}/{GITHUB_RELEASE_REPO}/releases/latest"
)
UPDATE_CHECK_INTERVAL_HOURS = 24

MTGJSON_ATOMIC_URL = "https://mtgjson.com/api/v5/AtomicCards.json"
MTGJSON_SET_LIST_URL = "https://mtgjson.com/api/v5/SetList.json"
MTGJSON_ALL_PRINTINGS_URL = "https://mtgjson.com/api/v5/AllPrintings.json.gz"
MTGJSON_CSV_BASE_URL = "https://mtgjson.com/api/v5/csv"
MTGJSON_ALL_PRICES_TODAY_URL = "https://mtgjson.com/api/v5/AllPricesToday.json.zip"

MTGJSON_SET_BOOSTER_CONTENTS_URL = f"{MTGJSON_CSV_BASE_URL}/setBoosterContents.csv"
MTGJSON_SET_BOOSTER_CONTENT_WEIGHTS_URL = f"{MTGJSON_CSV_BASE_URL}/setBoosterContentWeights.csv"
MTGJSON_SET_BOOSTER_SHEET_CARDS_URL = f"{MTGJSON_CSV_BASE_URL}/setBoosterSheetCards.csv"
MTGJSON_SET_BOOSTER_SHEETS_URL = f"{MTGJSON_CSV_BASE_URL}/setBoosterSheets.csv"

SCRYFALL_BULK_DATA_URL = "https://api.scryfall.com/bulk-data"

APP_SECRET_KEY = "imomir-dev-key"

CARD_BACK_UPLOAD_MAX_SIZE_MB = 35
CARD_BACK_UPLOAD_MAX_SIZE_BYTES = CARD_BACK_UPLOAD_MAX_SIZE_MB * 1024 * 1024

SILHOUETTE_LETTER_CARD_WIDTH_MM = 63.5
SILHOUETTE_LETTER_CARD_HEIGHT_MM = 88.9
SILHOUETTE_LETTER_START_X_MM = 13.0
SILHOUETTE_LETTER_START_Y_MM = 18.6
SILHOUETTE_LETTER_COLUMNS = 4
SILHOUETTE_LETTER_ROWS = 2
SILHOUETTE_EDGE_BORDER_PIXELS = 1
SILHOUETTE_RENDER_TARGET_WIDTH_PX = 762
SILHOUETTE_RENDER_TARGET_HEIGHT_PX = 1067
SILHOUETTE_CORNER_RADIUS_MM = 3.25
SILHOUETTE_FILL_UNUSED_SLOTS_WITH_WHITE = True

# Silhouette A4 - Vertical - 9 Card
SILHOUETTE_A4_PAGE_WIDTH_MM = 210.0
SILHOUETTE_A4_PAGE_HEIGHT_MM = 297.0

SILHOUETTE_A4_CARD_WIDTH_MM = 63.0
SILHOUETTE_A4_CARD_HEIGHT_MM = 88.0

SILHOUETTE_A4_BLEED_MM = 0.5925
SILHOUETTE_A4_CARD_SPACING_MM = 1.185

SILHOUETTE_A4_COLUMNS = 3
SILHOUETTE_A4_ROWS = 3

PDF_CUTTING_GUIDE_CARD_WIDTH_MM = 63.0
PDF_CUTTING_GUIDE_CARD_HEIGHT_MM = 88.0
PDF_CUTTING_GUIDE_SIZE_MM = 2.0
PDF_CUTTING_GUIDE_THICKNESS_MM = 0.20
PDF_CUTTING_GUIDE_COLOR_RGB = (0, 255, 0)

PDF_OUTER_SLOT_REGION_BAND_DEFAULT_SIZE_MM = 0.40
PDF_OUTER_SLOT_REGION_BAND_DEFAULT_COLOR_HEX = "#000000"
PDF_OUTER_SLOT_REGION_BAND_DEFAULT_MODE = "back_only"

CARD_SEARCH_DEFAULT_TITLE = "Avatar - Momir Vig, Simic Visionary"
CARD_SEARCH_DEFAULT_VARIANTS = {
    "dark": {
        "label": "Dark Token",
        "filename": "img/MomirVig_Token_1.jpg",
    },
    "light": {
        "label": "Light Token",
        "filename": "img/MomirVig_Token_3.jpg",
    },
    "retro": {
        "label": "Retro Token",
        "filename": "img/MomirVig_Token_2.jpg",
    },
    "mtgo": {
        "label": "MTGO Token",
        "filename": "img/MomirVig_Token_4.jpg",
    },
}
CARD_SEARCH_DEFAULT_VARIANT = "dark"

MOMIR_MODE_VALUES = {
    "custom",
    "momir_basic",
    "momir_select",
    "momir_planeswalker",
    "momir_legends",
    "momir_battleship",
    "momir_aggro",
    "momir_odds",
    "momir_evens",
    "momir_prime",
    "tower_of_power",
    "planechase",
    "archenemy",
}

CHAOS_DRAFT_MODE_VALUES = {
    "chaos_draft",
    "chaos_draft_campaign",
    "preprint_chaos_draft",
}


def resolve_momir_mode_value(config):
    saved_mode = (config.get("momir_mode") or "").strip().lower()
    if saved_mode in MOMIR_MODE_VALUES:
        return saved_mode

    legacy_mode = (config.get("game_mode") or "").strip().lower()
    if legacy_mode in MOMIR_MODE_VALUES:
        return legacy_mode

    return "momir_basic"


def resolve_chaos_draft_mode_value(config):
    saved_mode = (config.get("chaos_draft_mode") or "").strip().lower()
    if saved_mode in CHAOS_DRAFT_MODE_VALUES:
        return saved_mode

    legacy_mode = (config.get("game_mode") or "").strip().lower()
    if legacy_mode in CHAOS_DRAFT_MODE_VALUES:
        return legacy_mode

    return "chaos_draft"


SCOPED_PRINT_SETTING_KEYS = (
    "print_template",
    "print_color_mode",
    "use_pdf_print",
    "pdf_width_mm",
    "pdf_height_mm",
    "print_bleed_size_mm",
    "pdf_crop_border",
    "pdf_cutting_guides",

    # Legacy on/off key retained so existing scoped settings can be migrated.
    "pdf_outer_slot_region_band",

    "pdf_outer_slot_region_band_mode",
    "pdf_outer_slot_region_band_size_mm",
    "pdf_outer_slot_region_band_color_hex",

    "print_card_backs",
    "default_card_back",
    "print_labels_enabled",
    "print_label_tracking_code",
    "print_label_front_back",
    "print_pack_label_cards",
    "open_print_in_new_tab",
)


CHAOS_ONLY_PRINT_SETTING_KEYS = (
    "silhouette_registration_marks",
    "no_wasted_space_enabled",
    "no_wasted_space_set_rules",
    "no_wasted_space_card_types",
    "no_wasted_space_label_option",
)


def resolve_scoped_print_config(config, scope):
    resolved = dict(config or {})
    normalized_scope = (scope or "momir").strip().lower()

    if normalized_scope not in {"momir", "chaos"}:
        normalized_scope = "momir"

    prefix = f"{normalized_scope}_"

    scoped_keys = SCOPED_PRINT_SETTING_KEYS

    if normalized_scope == "chaos":
        scoped_keys += CHAOS_ONLY_PRINT_SETTING_KEYS

    for key in scoped_keys:
        scoped_value = resolved.get(f"{prefix}{key}")

        if scoped_value is None:
            continue

        if isinstance(scoped_value, str) and not scoped_value.strip():
            continue

        resolved[key] = scoped_value

    return resolved


DEFAULT_CONFIG = {
    "type_creature": "1",
    "type_artifact": "0",
    "type_enchantment": "0",
    "type_instant": "0",
    "type_land": "0",
    "type_sorcery": "0",
    "type_planeswalker": "0",
    "type_battle": "0",
    "type_conspiracy": "0",
    "type_dungeon": "0",
    "type_emblem": "0",
    "type_phenomenon": "0",
    "type_plane": "0",
    "type_scheme": "0",
    "type_vanguard": "0",
    "allow_legendary": "1",
    "allow_unsets": "0",
    "allow_arena": "0",
    "all_sets_enabled": "1",
    "game_mode": "custom",
    "momir_mode": "",
    "chaos_draft_mode": "",
    "allow_repeats": "1",
    "print_template": "dk-1234",
    "print_color_mode": "grayscale",
    "use_pdf_print": "1",
    "pdf_width_mm": "57.5",
    "pdf_height_mm": "85.25",
    "print_bleed_size_mm": "3.0",
    "pdf_crop_border": "1",
    "pdf_cutting_guides": "1",

    # Legacy safety-band checkbox value.
    "pdf_outer_slot_region_band": "0",

    # New configurable safety-band settings.
    # Blank values are resolved to Back Only / 0.40 mm / #000000.
    "pdf_outer_slot_region_band_mode": "",
    "pdf_outer_slot_region_band_size_mm": "",
    "pdf_outer_slot_region_band_color_hex": "",

    "print_card_backs": "0",
    "default_card_back": DEFAULT_CARD_BACK_KEY,

    # Mode-specific print settings. Blank values intentionally fall back to
    # the legacy shared print settings until each scope is saved once.
    "momir_print_template": "",
    "momir_print_color_mode": "",
    "momir_use_pdf_print": "",
    "momir_pdf_width_mm": "",
    "momir_pdf_height_mm": "",
    "momir_print_bleed_size_mm": "",
    "momir_pdf_crop_border": "",
    "momir_pdf_cutting_guides": "",

    # Legacy safety-band checkbox value.
    "momir_pdf_outer_slot_region_band": "",

    "momir_pdf_outer_slot_region_band_mode": "",
    "momir_pdf_outer_slot_region_band_size_mm": "",
    "momir_pdf_outer_slot_region_band_color_hex": "",

    "momir_print_card_backs": "",
    "momir_print_labels_enabled": "",
    "momir_print_label_tracking_code": "",
    "momir_print_label_front_back": "",
    "momir_print_pack_label_cards": "",
    "momir_open_print_in_new_tab": "",

    "chaos_print_template": "silhouette-letter-horizontal-8",
    "chaos_print_color_mode": "color",
    "chaos_use_pdf_print": "1",
    "chaos_pdf_width_mm": "",
    "chaos_pdf_height_mm": "",
    "chaos_print_bleed_size_mm": "",
    "chaos_pdf_crop_border": "",
    "chaos_pdf_cutting_guides": "1",
    "chaos_silhouette_registration_marks": "1",
    "chaos_no_wasted_space_enabled": "0",
    "chaos_no_wasted_space_set_rules": "current_set",
    "chaos_no_wasted_space_card_types": "any",
    "chaos_no_wasted_space_label_option": "use_label_setting",

    # Legacy safety-band checkbox value.
    "chaos_pdf_outer_slot_region_band": "",

    "chaos_pdf_outer_slot_region_band_mode": "",
    "chaos_pdf_outer_slot_region_band_size_mm": "",
    "chaos_pdf_outer_slot_region_band_color_hex": "",

    "chaos_print_card_backs": "1",
    "chaos_default_card_back": DEFAULT_CARD_BACK_KEY,
    "chaos_print_labels_enabled": "",
    "chaos_print_label_tracking_code": "",
    "chaos_print_label_front_back": "",
    "chaos_print_pack_label_cards": "",
    "chaos_open_print_in_new_tab": "",

    # Print / Export Labels
    "print_labels_enabled": "1",
    "print_label_tracking_code": "0",
    "print_label_front_back": "1",
    "print_pack_label_cards": "0",

    # Legacy label keys retained for compatibility with older saved configs.
    "print_front_back_label": "1",
    "print_pack_tracking_code": "0",
    "print_pack_labels": "0",

    "enable_track_packs": "1",
    "enable_chaos_card_image_export": "1",
    "export_add_bleed": "1",
    "export_separate_special_slots": "0",
    "use_pack_image_for_title": "0",
    "momir_default_token_variant": "dark",
    "open_print_in_new_tab": "1",
    "sound_enabled": "1",
    "debug_log": "0",
    "tower_pdf_draw_count": "7",
    "chaos_pack_types": "core,default,draft,collector,set,play,jumpstart,jumpstart-v2,premium,six,collector-special",
    "chaos_draft_export_format": "archidekt",
    "chaos_scryfall_image_quality": "png",

    "upscaling_active_plugin": "",
    "upscaling_add_button_to_card_view": "1",
    "upscaling_holofoil_stamp_enabled": "0",
    "upscaling_holofoil_stamp_replacement": "background",
    "upscaling_dev_feedback_system": "0",

    "display_pack_prices": "1",
    "check_new_releases": "1",
    "pack_price_source": "tcgplayer-retail",
}

REPEAT_MODE_OPTIONS = [
    ("1", "Repeat"),
    ("0", "No Repeats"),
]

SCRYFALL_IMAGE_QUALITY_OPTIONS = [
    ("normal", "Normal"),
    ("large", "Large → Normal"),
    ("png", "PNG Preferred → Large → Normal"),
]

PRIMARY_TYPE_KEYS = [
    ("type_creature", "Creature"),
    ("type_artifact", "Artifact"),
    ("type_enchantment", "Enchantment"),
    ("type_instant", "Instant"),
    ("type_land", "Land"),
    ("type_sorcery", "Sorcery"),
    ("type_planeswalker", "Planeswalker"),
    ("type_battle", "Battle"),
]

SUPPLEMENTAL_TYPE_KEYS = [
    ("type_conspiracy", "Conspiracy"),
    ("type_dungeon", "Dungeon"),
    ("type_emblem", "Emblem"),
    ("type_phenomenon", "Phenomenon"),
    ("type_plane", "Plane"),
    ("type_scheme", "Scheme"),
    ("type_vanguard", "Vanguard"),
]

OTHER_FILTER_KEYS = [
    ("allow_legendary", "Allow Legendary"),
    ("allow_unsets", "Allow Un-sets"),
    ("allow_arena", "Allow Arena"),
]

PRINT_TEMPLATE_OPTIONS = [
    ("dk-1234", "DK-1234"),
    ("standard", "Standard"),
    ("borderless-3p5x5-two-card", "PDF ONLY - 3.5 x 5 Borderless - 2 Card Layout"),
    ("portrait-3p5x5-top-aligned", "PDF ONLY - 3.5 x 5 Portrait Top aligned"),
    ("landscape-3p5x5-centered", "PDF ONLY - 3.5 x 5 Landscape Centered"),
    ("silhouette-letter-horizontal-8", "US Letter - Horizontal - 8 Card"),
    ("silhouette-a4-vertical-9", "A4 - Vertical - 9 Card"),
]

NO_WASTE_SET_RULE_OPTIONS = [
    ("current_set", "Current Set Only"),
    ("set_selection", "Use Set Selection"),
]

NO_WASTE_CARD_TYPE_OPTIONS = [
    ("any", "Any cards"),
    ("any_no_tokens", "Any cards (no tokens)"),
    ("tokens_only", "Tokens Only"),
    ("basic_lands_only", "Basic Lands Only"),
    ("non_basic_lands_only", "Non Basic Lands Only"),
    ("artifacts_only", "Artifacts Only"),
    ("rare_mythic_only", "Rare/Mythic Only"),
]

NO_WASTE_LABEL_OPTIONS = [
    ("use_label_setting", "Use Label Setting"),
    ("bonus_card", "Display BONUS CARD"),
    ("proxy", "Display PROXY"),
    ("surprise", "Display SURPRISE!"),
    ("rarity_hearts", "Display 🤍🤍🤍"),
    ("no_label", "No Label"),
]

CHAOS_DRAFT_EXPORT_FORMAT_OPTIONS = [
    ("none", "None"),
    ("archidekt", "Archidekt"),
    ("moxfield", "Moxfield"),
]

PACK_PRICE_SOURCE_OPTIONS = [
    ("tcgplayer-retail", "TCGPlayer Retail"),
]

PRINT_COLOR_MODE_OPTIONS = [
    ("grayscale", "Grayscale"),
    ("color", "Full Color"),
    ("monochrome", "Monochrome"),
    ("optimal", "Optimal Print"),
]

GAME_MODE_OPTIONS = [
    {
        "value": "custom",
        "label": "Custom",
        "description": "The Momir Vig Avatar allows each player to start with <strong>24 life</strong>, and grants the following ability: <br><br> &#10006; <i>discard a card: Create a token that’s a copy of a <strong>creature</strong> card with converted mana cost X chosen at random. Activate this ability only any time you could cast a sorcery and only once per turn.</i> <br><br> This mode allows you to choose from all available Card Filters.",
        "image_filename": "img/token_mode_custom.jpg",
    },
    {
        "value": "momir_basic",
        "label": "Momir Basic",
        "description": "The Momir Vig Avatar allows each player to start with <strong>24 life</strong>, and grants the following ability: <br><br> &#10006; <i>discard a card: Create a token that’s a copy of a <strong>creature</strong> card with converted mana cost X chosen at random. Activate this ability only any time you could cast a sorcery and only once per turn.</i> <br><br> This is the standard mode of the Momir varient.",
        "image_filename": "img/token_mode_momir_basic.jpg",
    },
    {
        "value": "momir_select",
        "label": "Momir Select",
        "description": "The Momir Vig Avatar allows each player to start with <strong>24 life</strong>, and grants the following ability: <br><br> &#10006; <i>discard a card: Create a token that’s a copy of a card with converted mana cost X from the <strong>selected card type</strong>. Activate this ability only any time you could cast a sorcery and only once per turn.</i> <br><br> This mode adds a card type selector to the draw screen and only pulls from the chosen enabled type.",
        "image_filename": "img/token_mode_momir_select.jpg",
    },
    {
        "value": "momir_planeswalker",
        "label": "Momir Planeswalker",
        "description": "The Momir Vig Avatar allows each player to start with <strong>24 life</strong>, and grants the following ability: <br><br> &#10006; <i>discard a card: Create a token that’s a copy of a <strong>Creature or Planeswalker</strong> card with converted mana cost X chosen at random. Activate this ability only any time you could cast a sorcery and only once per turn.</i> <br><br> This mode includes both Creatures and Plainswalkers as token types.",
        "image_filename": "img/token_mode_momir_planeswalker.jpg",
    },
    {
        "value": "momir_legends",
        "label": "Momir Legends",
        "description": "The Momir Vig Avatar allows each player to start with <strong>24 life</strong>, and grants the following ability: <br><br> &#10006; <i>discard a card: Create a token that’s a copy of a <strong>Rare or Mythic Legendary Creature</strong> card with converted mana cost X chosen at random. Activate this ability only any time you could cast a sorcery and only once per turn.</i> <br><br> This mode can only grab Creatures that are Rare or Mythic rarity.",
        "image_filename": "img/token_mode_momir_legends.jpg",
    },
    {
        "value": "momir_battleship",
        "label": "Momir Battleship",
        "description": "The Momir Vig Avatar allows each player to start with <strong>24 life</strong>, and grants the following ability: <br><br> &#10006; <i>discard a card: Create a token that’s a copy of a <strong>creature</strong> card with converted mana cost X that is <strong>5 or greater</strong>, chosen at random. Activate this ability only any time you could cast a sorcery and only once per turn.</i> <br><br> This mode only allows cards with a cost of 5 or more to be copied.",
        "image_filename": "img/token_mode_momir_battleship.jpg",
    },
    {
        "value": "momir_aggro",
        "label": "Momir Aggro",
        "description": "The Momir Vig Avatar allows each player to start with <strong>24 life</strong>, and grants the following ability: <br><br> &#10006; <i>discard a card: Create a token that’s a copy of a <strong>creature</strong> card with converted mana cost X that is <strong>4 or less</strong>, chosen at random. Activate this ability only any time you could cast a sorcery and only once per turn.</i> <br><br> This mode only allows cards with a cost of 4 or less to be copied.",
        "image_filename": "img/token_mode_momir_aggro.jpg",
    },
    {
        "value": "momir_odds",
        "label": "Momir Odds",
        "description": "The Momir Vig Avatar allows each player to start with <strong>24 life</strong>, and grants the following ability: <br><br> &#10006; <i>discard a card: Create a token that’s a copy of a <strong>creature</strong> card with converted mana cost X that is <strong>an odd value</strong>, chosen at random. Activate this ability only any time you could cast a sorcery and only once per turn.</i> <br><br> This mode only allows cards with an odd value mana cost to be copied.",
        "image_filename": "img/token_mode_momir_odds.jpg",
    },
    {
        "value": "momir_evens",
        "label": "Momir Evens",
        "description": "The Momir Vig Avatar allows each player to start with <strong>24 life</strong>, and grants the following ability: <br><br> &#10006; <i>discard a card: Create a token that’s a copy of a <strong>creature</strong> card with converted mana cost X that is <strong>an even value</strong>, chosen at random. Activate this ability only any time you could cast a sorcery and only once per turn.</i> <br><br> This mode only allows cards with an even value mana cost to be copied.",
        "image_filename": "img/token_mode_momir_evens.jpg",
    },
    {
        "value": "momir_prime",
        "label": "Momir Prime",
        "description": "The Momir Vig Avatar allows each player to start with <strong>24 life</strong>, and grants the following ability: <br><br> &#10006; <i>discard a card: Create a token that’s a copy of a <strong>creature</strong> card with a converted mana cost of X that is a <strong>Prime Number</strong>, chosen at random. Activate this ability only any time you could cast a sorcery and only once per turn.</i> <br><br> This mode only allows cards with a mana cost that is a Prime Number to be copied.",
        "image_filename": "img/token_mode_momir_prime.jpg",
    },
    {
        "value": "tower_of_power",
        "label": "Tower of Power",
        "description": "Tower of Power is a  mode that simulates drawing from a deck of any card for the selected sets. Click <strong>Draw</strong> to draw a random card from the selected pool using <strong>Sets</strong> and <strong>Primary Card Types</strong>, plus basic and non-basic lands.",
        "image_filename": "img/token_mode_tower_of_power.jpg",
    },
    {
        "value": "chaos_draft_campaign",
        "label": "Chaos Draft - Campaign Mode",
        "description": "Chaos Draft is the ultimate way to play! Campaign Mode allows you to manage and design your Chaos drafts with specific packs and roll for specific players. Add packs you own, or print packs you dont.  You can do it all in Campaign Mode.",
        "image_filename": "img/token_mode_chaos_draft.jpg",
    },
    {
        "value": "chaos_draft",
        "label": "Chaos Draft",
        "description": "Chaos Draft selects a random booster pack from the currently enabled sets. One of the funnest ways to play Magic the Gathering.",
        "image_filename": "img/token_mode_chaos_draft.jpg",
    },
    {
        "value": "preprint_chaos_draft",
        "label": "PRE-PRINT - Chaos Draft",
        "description": "Pre-generate Chaos Draft packs for your next game. Choose how many players and how many packs per player, then combine all generated packs into one printable PDF document.",
        "image_filename": "img/token_mode_chaos_draft.jpg",
    },
    {
        "value": "planechase",
        "label": "Planechase",
        "description": "The Planechase format uses a shared planar deck. Players sometimes play planes cards that affect the battlefield. You can use this mode to generate Planes by clicking on the 0.",
        "image_filename": "img/token_mode_planechase.jpg",
    },
    {
        "value": "archenemy",
        "label": "Archenemy",
        "description": "You can generate Schemes for Archenemy using this mode.  It is recommended that you turn off Repeats for this mode.",
        "image_filename": "img/token_mode_archenemy.jpg",
    },
]

MOMIR_DEFAULT_TOKEN_VARIANT_OPTIONS = [
    ("dark", "Dark Token"),
    ("light", "Light Token"),
    ("retro", "Retro Token"),
    ("mtgo", "MTGO Token"),
]

CHAOS_PACK_TYPE_OPTIONS = [
    {"value": "core", "label": "Core Booster"},
    {"value": "default", "label": "Booster"},
    {"value": "set", "label": "Set Booster"},
    {"value": "draft", "label": "Draft Booster"},
    {"value": "play", "label": "Play Booster"},
    {"value": "collector", "label": "Collector Booster"},
    {"value": "collector-special", "label": "Collector Special Booster"},
    {"value": "jumpstart", "label": "Jumpstart Booster"},
    {"value": "jumpstart-v2", "label": "Jumpstart Booster"},
    {"value": "premium", "label": "Premium Booster"},
    {"value": "vip", "label": "VIP Booster"},
    {"value": "six", "label": "Six Card Booster"},
    {"value": "collector-sample", "label": "Collector Sample Pack (2 cards)"},
    {"value": "custom", "label": "Custom"},
]

ALLOWED_CHAOS_BOOSTER_TYPES = {
    item["value"]
    for item in CHAOS_PACK_TYPE_OPTIONS
}

CHAOS_DUPLICATE_CONTROL_ENABLED = True
CHAOS_DUPLICATE_CONTROL_TYPES = {
    "play",
    "draft",
    "set",
    "collector",
}
CHAOS_DUPLICATE_REROLL_CHANCE = 0.7
CHAOS_DUPLICATE_MAX_REROLLS = 3
CHAOS_DUPLICATE_LOG_ALL_DETECTIONS = True

CHAOS_BATCH_REPEAT_REPLACEMENT_CHANCES = {
    "common": {
        1: 0.20,
        2: 0.40,
        3: 0.60,
        4: 0.80,
    },
    "uncommon": {
        1: 0.30,
        2: 0.50,
        3: 0.70,
        4: 0.90,
    },
    "rare": {
        1: 0.70,
        2: 0.80,
        3: 0.90,
        4: 1.00,
    },
    "mythic": {
        1: 0.75,
        2: 0.90,
        3: 1.00,
        4: 1.00,
    },
}

TYPE_FLAG_MAP = {
    "Creature": "is_creature",
    "Artifact": "is_artifact",
    "Enchantment": "is_enchantment",
    "Instant": "is_instant",
    "Land": "is_land",
    "Sorcery": "is_sorcery",
    "Planeswalker": "is_planeswalker",
    "Battle": "is_battle",
    "Conspiracy": "is_conspiracy",
    "Dungeon": "is_dungeon",
    "Emblem": "is_emblem",
    "Phenomenon": "is_phenomenon",
    "Plane": "is_plane",
    "Scheme": "is_scheme",
    "Vanguard": "is_vanguard",
}