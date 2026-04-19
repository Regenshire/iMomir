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
    "allow_repeats": "1",
    "print_template": "dk-1234",
    "print_color_mode": "grayscale",
    "use_pdf_print": "1",
    "pdf_width_mm": "57.5",
    "pdf_height_mm": "85.25",
    "pdf_crop_border": "1",
    "print_front_back_label": "1",
    "use_pack_image_for_title": "0",
    "momir_default_token_variant": "dark",
    "open_print_in_new_tab": "1",
    "sound_enabled": "1",
    "debug_log": "0",
    "tower_pdf_draw_count": "7",
    "chaos_pack_types": "core,default,draft,collector,set,play,jumpstart,jumpstart-v2,premium,six,collector-special",
    "chaos_draft_export_format": "none",
    "display_pack_prices": "1",
    "pack_price_source": "tcgplayer-retail",
}

REPEAT_MODE_OPTIONS = [
    ("1", "Repeat"),
    ("0", "No Repeats"),
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
    ("silhouette-letter-horizontal-8", "Silhouette Letter - Horizontal - 8 Card"),
]

PRINT_TEMPLATE_METADATA = {
    "dk-1234": {
        "download_links": [],
    },
    "standard": {
        "download_links": [],
    },
    "borderless-3p5x5-two-card": {
        "download_links": [],
    },
    "portrait-3p5x5-top-aligned": {
        "download_links": [],
    },
    "landscape-3p5x5-centered": {
        "download_links": [],
    },
    "silhouette-letter-horizontal-8": {
        "download_links": [
            {
                "label": "Download Silhouette Template",
                "filename": "sil/Silhouette_Legal_Vertical_8_Card.studio3",
            }
        ],
    },
}

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
        "value": "chaos_draft",
        "label": "Chaos Draft",
        "description": "Chaos Draft selects a random booster pack from the currently enabled sets. One of the funnest ways to play Magic the Gathering.",
        "image_filename": "img/token_mode_tower_of_power.jpg",
    },
    {
        "value": "preprint_chaos_draft",
        "label": "PRE-PRINT - Chaos Draft",
        "description": "Pre-generate Chaos Draft packs for your next game. Choose how many players and how many packs per player, then combine all generated packs into one printable PDF document.",
        "image_filename": "img/token_mode_tower_of_power.jpg",
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
CHAOS_DUPLICATE_REROLL_CHANCE = 0.5
CHAOS_DUPLICATE_MAX_REROLLS = 3
CHAOS_DUPLICATE_LOG_ALL_DETECTIONS = True

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