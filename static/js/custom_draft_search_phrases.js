(function () {
    "use strict";

    function getRandomArrayItem(items) {
        if (!items || !items.length) {
            return "";
        }

        return items[Math.floor(Math.random() * items.length)];
    }

    function normalizeValues(values) {
        if (!Array.isArray(values)) {
            return [];
        }

        return values
            .map(function (value) {
                return String(value || "").trim().toLowerCase();
            })
            .filter(Boolean);
    }

    function getColorSymbolKey(colorValues) {
        return normalizeValues(colorValues)
            .filter(function (colorValue) {
                return ["w", "u", "b", "r", "g"].indexOf(colorValue) !== -1;
            })
            .sort()
            .join("");
    }

    function getSpecialColorModes(colorValues) {
        return normalizeValues(colorValues)
            .filter(function (colorValue) {
                return ["colorless", "land", "multi_selected", "multi_has_selected", "multi_any"].indexOf(colorValue) !== -1;
            });
    }

    const colorPhrasePools = {
        "": [
            "Searching the library...",
            "Scrying the next page...",
            "Consulting the sideboard...",
            "Reading the topdeck...",
            "Shuffling through possibilities...",
            "Cutting to a better spell...",
            "Revealing hidden options...",
            "Looking three cards deep...",
            "Checking the command zone...",
            "Opening another booster..."
        ],

        "w": [
            "Mustering the host...",
            "Calling the vanguard...",
            "Searching the chapel archives...",
            "Rallying the shield line...",
            "Summoning the faithful...",
            "Finding a lawful answer...",
            "Gathering the warband...",
            "Calling the watch...",
            "Seeking a blessed blade...",
            "Reading the battle hymnal..."
        ],

        "u": [
            "Scrying through the deep...",
            "Consulting the academy...",
            "Reading the stack...",
            "Opening the spellbook...",
            "Seeking a clever answer...",
            "Charting the aether...",
            "Tracing a counterspell...",
            "Looking beyond the topdeck...",
            "Searching the archives...",
            "Calculating the line..."
        ],

        "b": [
            "Opening the crypt...",
            "Counting graveyard whispers...",
            "Calling from below...",
            "Searching the undercity...",
            "Reading the death ledger...",
            "Making a darker bargain...",
            "Raising old answers...",
            "Listening to the tomb...",
            "Seeking forbidden value...",
            "Consulting the necropolis..."
        ],

        "r": [
            "Lighting the fuse...",
            "Stoking the furnace...",
            "Searching the war drums...",
            "Calling the next explosion...",
            "Looking for lethal...",
            "Rattling the goblin crates...",
            "Finding the spark...",
            "Opening the dragon cage...",
            "Chasing lightning...",
            "Rolling for chaos..."
        ],

        "g": [
            "Hunting through the wilds...",
            "Following beast tracks...",
            "Calling the old growth...",
            "Searching the canopy...",
            "Listening to the roots...",
            "Waking the forest...",
            "Finding a bigger creature...",
            "Walking the leyline grove...",
            "Seeking primal strength...",
            "Tracking something enormous..."
        ],

        "gw": [
            "Calling the conclave...",
            "Blessing the wilds...",
            "Rallying the groveguard...",
            "Searching the sanctuary...",
            "Gathering the living host...",
            "Walking the temple garden...",
            "Summoning ordered growth...",
            "Seeking strength in numbers...",
            "Reading the covenant of leaves...",
            "Mustering the herd..."
        ],

        "gu": [
            "Studying the growth spiral...",
            "Opening the biomancer's notes...",
            "Scrying through the canopy...",
            "Following an evolving line...",
            "Testing the specimen pool...",
            "Reading the Simic ledger...",
            "Searching for adaptation...",
            "Mapping the living equation...",
            "Calling a clever beast...",
            "Growing the answer..."
        ],

        "bg": [
            "Digging through the rot farm...",
            "Calling the grave bloom...",
            "Searching the mossy crypt...",
            "Harvesting from the undergrowth...",
            "Feeding the fungus...",
            "Reading the Golgari ledger...",
            "Following death into life...",
            "Counting bones in the loam...",
            "Opening the compost heap...",
            "Seeking value in decay..."
        ],

        "gr": [
            "Calling the stampede...",
            "Rousing the clans...",
            "Tracking thunderhoof prints...",
            "Searching the riot grounds...",
            "Waking mountain beasts...",
            "Following the warpath...",
            "Hunting with fire and fang...",
            "Looking for the biggest threat...",
            "Charging the red-green zone...",
            "Summoning something unsubtle..."
        ],

        "br": [
            "Checking the blood ledger...",
            "Lighting the undercity fires...",
            "Searching the sacrifice pit...",
            "Calling the devil's bargain...",
            "Counting knives and sparks...",
            "Opening the Rakdos stage...",
            "Looking for ruthless value...",
            "Feeding the furnace...",
            "Finding the sharpest spell...",
            "Summoning trouble..."
        ],

        "rw": [
            "Sounding the charge...",
            "Opening the armory...",
            "Rallying the legion...",
            "Lighting the signal fires...",
            "Seeking righteous pressure...",
            "Marching under bright banners...",
            "Calling the battle line...",
            "Finding a combat trick...",
            "Sharpening blades at dawn...",
            "Mustering the assault..."
        ],

        "ru": [
            "Charging the experiment...",
            "Reading sparks in the lab...",
            "Searching the storm kiln...",
            "Bottling lightning...",
            "Consulting the weirds...",
            "Opening the Izzet notes...",
            "Looking for a brilliant mistake...",
            "Mixing thought and thunder...",
            "Casting before thinking...",
            "Testing unstable magic..."
        ],

        "bu": [
            "Reading forbidden files...",
            "Searching the drowned archive...",
            "Consulting hidden agents...",
            "Opening the Dimir dossier...",
            "Counting secrets...",
            "Scrying in the dark...",
            "Looking for the perfect scheme...",
            "Tracing a shadow line...",
            "Whispering through the canals...",
            "Finding a quiet answer..."
        ],

        "uw": [
            "Checking the senate records...",
            "Consulting the high archive...",
            "Balancing law and logic...",
            "Searching the sky patrol...",
            "Finding the cleanest answer...",
            "Reading the Azorius docket...",
            "Holding up permission...",
            "Calling orderly reinforcements...",
            "Surveying the battlefield...",
            "Preparing a careful line..."
        ],

        "bw": [
            "Opening the chapel crypt...",
            "Counting debts of faith...",
            "Reading the covenant...",
            "Calling solemn witnesses...",
            "Searching the reliquary...",
            "Balancing mercy and ambition...",
            "Gathering saints and sinners...",
            "Lighting black candles...",
            "Seeking devotion with teeth...",
            "Finding power in obligation..."
        ]
    };

    const specialPhrasePools = {
        "colorless": [
            "Opening the artifact vault...",
            "Sorting ancient relics...",
            "Awakening forgotten constructs...",
            "Checking the powerstones...",
            "Listening to silent machines...",
            "Dusting off old devices...",
            "Searching the colorless archive...",
            "Finding tools without masters...",
            "Reading the golem schematics...",
            "Unlocking strange metal..."
        ],

        "land": [
            "Surveying the horizon...",
            "Reading the leyline map...",
            "Searching for mana-rich ground...",
            "Opening the atlas...",
            "Walking the old roads...",
            "Charting a new domain...",
            "Following the borders...",
            "Finding a place to stand...",
            "Listening to the stones...",
            "Exploring the mana base..."
        ],

        "multi": [
            "Following braided leylines...",
            "Searching the gold cards...",
            "Balancing multiple colors...",
            "Opening the guild records...",
            "Reading the prism...",
            "Seeking many-colored power...",
            "Sorting ambitious spells...",
            "Consulting the multicolor archive...",
            "Mixing mana streams...",
            "Finding a complicated answer..."
        ]
    };

    const typePhrasePools = {
        "creature": [
            "Tracking combatants...",
            "Listening for claws...",
            "Opening the monster pen...",
            "Calling a body to the board...",
            "Searching for a threat...",
            "Finding something that attacks...",
            "Checking the creature type line...",
            "Summoning a board presence...",
            "Looking for teeth and toughness...",
            "Calling the next attacker..."
        ],

        "artifact": [
            "Opening the artifact vault...",
            "Checking the workshop...",
            "Sorting old machines...",
            "Polishing powerstones...",
            "Finding useful metal...",
            "Consulting the artificer...",
            "Unlocking a device...",
            "Searching for a relic...",
            "Waking the construct...",
            "Reading the schematic..."
        ],

        "enchantment": [
            "Tracing a lingering aura...",
            "Searching the shrine wall...",
            "Calling lasting magic...",
            "Reading the enchantment script...",
            "Finding magic that stays...",
            "Unraveling old bindings...",
            "Opening the aura ledger...",
            "Listening to the blessing...",
            "Seeking persistent value...",
            "Studying the spellwork..."
        ],

        "instant": [
            "Holding up mana...",
            "Reading the stack...",
            "Searching for a response...",
            "Looking for instant speed...",
            "Waiting for priority...",
            "Finding the trick...",
            "Checking the reaction window...",
            "Preparing the answer...",
            "Leaving mana open...",
            "Scrying for interaction..."
        ],

        "sorcery": [
            "Preparing the main phase...",
            "Opening the ritual scroll...",
            "Searching for a haymaker...",
            "Calling deliberate magic...",
            "Finding the big spell...",
            "Reading the sorcery line...",
            "Choosing a planned disaster...",
            "Gathering slow thunder...",
            "Writing the next chapter...",
            "Casting with intent..."
        ],

        "planeswalker": [
            "Calling across the Blind Eternities...",
            "Searching for a spark...",
            "Checking loyalty counters...",
            "Following a planeswalker trail...",
            "Opening the oath records...",
            "Seeking a powerful ally...",
            "Reading a planar signature...",
            "Looking for a strategist...",
            "Summoning a traveler...",
            "Calling an old friend..."
        ],

        "battle": [
            "Reading the war map...",
            "Choosing the battlefield...",
            "Preparing the invasion...",
            "Marking the siege lines...",
            "Searching contested ground...",
            "Calling banners to war...",
            "Opening the campaign map...",
            "Finding the front line...",
            "Surveying the conflict...",
            "Looking for a fight..."
        ],

        "land": [
            "Surveying the horizon...",
            "Searching the mana base...",
            "Opening the atlas...",
            "Reading the leyline map...",
            "Finding fertile ground...",
            "Walking forgotten roads...",
            "Charting wild territory...",
            "Listening to the land...",
            "Following old borders...",
            "Seeking a new domain..."
        ]
    };

    function getColorPhrasePool(colorValues) {
        const colorKey = getColorSymbolKey(colorValues);
        const specialModes = getSpecialColorModes(colorValues);

        if (!colorKey && specialModes.indexOf("colorless") !== -1) {
            return specialPhrasePools.colorless;
        }

        if (!colorKey && specialModes.indexOf("land") !== -1) {
            return specialPhrasePools.land;
        }

        if (!colorKey && (
            specialModes.indexOf("multi_any") !== -1
            || specialModes.indexOf("multi_selected") !== -1
            || specialModes.indexOf("multi_has_selected") !== -1
        )) {
            return specialPhrasePools.multi;
        }

        if (colorPhrasePools[colorKey]) {
            return colorPhrasePools[colorKey];
        }

        if (colorKey.length >= 3) {
            return specialPhrasePools.multi;
        }

        return colorPhrasePools[""];
    }

    function getTypePhrasePool(typeValue) {
        const cleanTypeValue = String(typeValue || "").trim().toLowerCase();
        return typePhrasePools[cleanTypeValue] || [];
    }

    function getLoadingPhrase(options) {
        const colorValues = options && options.colorValues ? options.colorValues : [];
        const typeValue = options && options.typeValue ? options.typeValue : "";

        const colorPhrases = getColorPhrasePool(colorValues);
        const typePhrases = getTypePhrasePool(typeValue);

        if (typePhrases.length && Math.random() < 0.55) {
            return getRandomArrayItem(typePhrases);
        }

        return getRandomArrayItem(colorPhrases) || "Searching the library...";
    }

    window.iMomirCustomDraftSearchPhrases = {
        getLoadingPhrase: getLoadingPhrase,
        colorPhrasePools: colorPhrasePools,
        typePhrasePools: typePhrasePools,
        specialPhrasePools: specialPhrasePools
    };
})();