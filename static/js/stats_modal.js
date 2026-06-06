(function () {
    function getElement(id) {
        return document.getElementById(id);
    }

    function formatPercent(count, total) {
        if (!total) {
            return "0%";
        }

        return Math.round((count / total) * 100) + "%";
    }

    function parseColorIdentity(rawValue) {
        if (Array.isArray(rawValue)) {
            return rawValue
                .map(function (colorValue) {
                    return String(colorValue || "").trim().toUpperCase();
                })
                .filter(Boolean);
        }

        try {
            const parsedValue = JSON.parse(rawValue || "[]");

            if (!Array.isArray(parsedValue)) {
                return [];
            }

            return parsedValue
                .map(function (colorValue) {
                    return String(colorValue || "").trim().toUpperCase();
                })
                .filter(Boolean);
        } catch (error) {
            return [];
        }
    }

    function getPrimaryCardType(typeLine) {
        const cleanTypeLine = String(typeLine || "").toLowerCase();

        if (cleanTypeLine.indexOf("creature") !== -1) {
            return "Creature";
        }

        if (cleanTypeLine.indexOf("instant") !== -1) {
            return "Instant";
        }

        if (cleanTypeLine.indexOf("sorcery") !== -1) {
            return "Sorcery";
        }

        if (cleanTypeLine.indexOf("artifact") !== -1) {
            return "Artifact";
        }

        if (cleanTypeLine.indexOf("enchantment") !== -1) {
            return "Enchantment";
        }

        if (cleanTypeLine.indexOf("planeswalker") !== -1) {
            return "Planeswalker";
        }

        if (cleanTypeLine.indexOf("battle") !== -1) {
            return "Battle";
        }

        if (cleanTypeLine.indexOf("land") !== -1) {
            return "Land";
        }

        return "Other";
    }

    function isLandCard(cardInfo) {
        const typeLine = String(cardInfo.typeLine || "").toLowerCase();
        const cardName = String(cardInfo.cardName || "").trim();

        return Boolean(
            cardInfo.isLand
            || cardInfo.isBasicLand
            || typeLine.indexOf("land") !== -1
            || cardName.match(/^(Plains|Island|Swamp|Mountain|Forest|Wastes)$/i)
        );
    }

    function normalizeCardInfo(rawCard) {
        rawCard = rawCard || {};

        const manaValue = Number(rawCard.manaValue);

        return {
            cardName: String(rawCard.cardName || "").trim(),
            typeLine: String(rawCard.typeLine || "").trim(),
            manaValue: Number.isFinite(manaValue) ? manaValue : null,
            colorIdentity: parseColorIdentity(rawCard.colorIdentity || rawCard.colorIdentityJson || "[]"),
            rarity: String(rawCard.rarity || "").trim().toLowerCase(),
            isLand: Boolean(rawCard.isLand),
            isBasicLand: Boolean(rawCard.isBasicLand)
        };
    }

    function renderSummary(summaryGrid, summaryItems) {
        if (!summaryGrid) {
            return;
        }

        summaryGrid.innerHTML = "";

        summaryItems.forEach(function (summaryItem) {
            const item = document.createElement("div");

            const label = document.createElement("span");
            label.textContent = summaryItem.label || "";

            const value = document.createElement("strong");
            value.textContent = String(summaryItem.value ?? "");

            item.appendChild(label);
            item.appendChild(value);
            summaryGrid.appendChild(item);
        });
    }

    function renderManaDistribution(container, buckets, maxCount) {
        if (!container) {
            return;
        }

        container.innerHTML = "";

        Object.keys(buckets).forEach(function (bucketLabel) {
            const count = buckets[bucketLabel];
            const percent = maxCount ? Math.max(4, Math.round((count / maxCount) * 100)) : 0;

            const row = document.createElement("div");
            row.className = "custom-draft-stat-bar-row";

            const label = document.createElement("span");
            label.className = "custom-draft-stat-bar-label";
            label.textContent = bucketLabel;

            const barWrap = document.createElement("div");
            barWrap.className = "custom-draft-stat-bar-wrap";

            const bar = document.createElement("div");
            bar.className = "custom-draft-stat-bar-fill";
            bar.style.width = percent + "%";

            const value = document.createElement("strong");
            value.textContent = String(count);

            barWrap.appendChild(bar);
            row.appendChild(label);
            row.appendChild(barWrap);
            row.appendChild(value);

            container.appendChild(row);
        });
    }

    function renderColorDistribution(container, colorCounts, totalCards) {
        if (!container) {
            return;
        }

        container.innerHTML = "";

        [
            ["W", "White"],
            ["U", "Blue"],
            ["B", "Black"],
            ["R", "Red"],
            ["G", "Green"],
            ["M", "Multicolor"],
            ["C", "Colorless"]
        ].forEach(function (colorInfo) {
            const colorKey = colorInfo[0];
            const colorLabel = colorInfo[1];
            const count = colorCounts[colorKey] || 0;

            const pill = document.createElement("div");
            pill.className = "custom-draft-stat-color-pill custom-draft-stat-color-" + colorKey.toLowerCase();

            const label = document.createElement("span");
            label.textContent = colorLabel;

            const value = document.createElement("strong");
            value.textContent = count + " • " + formatPercent(count, totalCards);

            pill.appendChild(label);
            pill.appendChild(value);
            container.appendChild(pill);
        });
    }

    function renderTypeDistribution(container, typeCounts, totalCards) {
        if (!container) {
            return;
        }

        container.innerHTML = "";

        [
            "Creature",
            "Instant",
            "Sorcery",
            "Artifact",
            "Enchantment",
            "Planeswalker",
            "Battle",
            "Land",
            "Other"
        ].forEach(function (typeName) {
            const count = typeCounts[typeName] || 0;

            if (count <= 0) {
                return;
            }

            const row = document.createElement("div");
            row.className = "custom-draft-stat-row";

            const label = document.createElement("span");
            label.textContent = typeName;

            const value = document.createElement("strong");
            value.textContent = count + " • " + formatPercent(count, totalCards);

            row.appendChild(label);
            row.appendChild(value);
            container.appendChild(row);
        });
    }

    function buildStats(cards, options) {
        options = options || {};

        const normalizedCards = (cards || []).map(normalizeCardInfo);
        const totalCards = normalizedCards.length;

        let creatureCount = 0;
        let landCount = 0;
        let manaTotal = 0;
        let manaCount = 0;

        const manaBuckets = {
            "0": 0,
            "1": 0,
            "2": 0,
            "3": 0,
            "4": 0,
            "5": 0,
            "6+": 0
        };

        const colorCounts = {
            "W": 0,
            "U": 0,
            "B": 0,
            "R": 0,
            "G": 0,
            "M": 0,
            "C": 0
        };

        const typeCounts = {
            "Creature": 0,
            "Instant": 0,
            "Sorcery": 0,
            "Artifact": 0,
            "Enchantment": 0,
            "Planeswalker": 0,
            "Battle": 0,
            "Land": 0,
            "Other": 0
        };

        normalizedCards.forEach(function (cardInfo) {
            const primaryType = getPrimaryCardType(cardInfo.typeLine);
            const cardIsLand = isLandCard(cardInfo);
            const colors = cardInfo.colorIdentity || [];

            typeCounts[primaryType] = (typeCounts[primaryType] || 0) + 1;

            if (cardIsLand) {
                landCount += 1;
            }

            if (String(cardInfo.typeLine || "").toLowerCase().indexOf("creature") !== -1) {
                creatureCount += 1;
            }

            if (!colors.length) {
                colorCounts.C += 1;
            } else if (colors.length >= 2) {
                colorCounts.M += 1;
            } else if (colorCounts[colors[0]] !== undefined) {
                colorCounts[colors[0]] += 1;
            }

            if (Number.isFinite(cardInfo.manaValue)) {
                if (!options.averageManaValueExcludesLands || !cardIsLand) {
                    manaTotal += cardInfo.manaValue;
                    manaCount += 1;
                }

                if (cardInfo.manaValue >= 6) {
                    manaBuckets["6+"] += 1;
                } else {
                    manaBuckets[String(Math.max(0, Math.floor(cardInfo.manaValue)))] += 1;
                }
            }
        });

        const maxManaBucket = Math.max.apply(null, Object.keys(manaBuckets).map(function (bucketKey) {
            return manaBuckets[bucketKey];
        }));

        return {
            totalCards: totalCards,
            creatureCount: creatureCount,
            nonCreatureCount: totalCards - creatureCount,
            landCount: landCount,
            nonLandCount: totalCards - landCount,
            averageManaValue: manaCount ? (manaTotal / manaCount) : 0,
            manaBuckets: manaBuckets,
            maxManaBucket: maxManaBucket,
            colorCounts: colorCounts,
            typeCounts: typeCounts
        };
    }

    function initStatsModal(config) {
        config = config || {};

        const modal = getElement("statsModal");
        const backdrop = getElement("statsBackdrop");
        const closeButton = getElement("statsCloseButton");
        const openButton = getElement(config.openButtonId || "");

        const title = getElement("statsModalTitle");
        const subtitle = getElement("statsModalSubtitle");
        const summaryGrid = getElement("statsSummaryGrid");
        const manaDistribution = getElement("statsManaDistribution");
        const colorDistribution = getElement("statsColorDistribution");
        const typeDistribution = getElement("statsTypeDistribution");

        function setVisible(isVisible) {
            if (!modal) {
                return;
            }

            modal.classList.toggle("hidden", !isVisible);
            modal.setAttribute("aria-hidden", isVisible ? "false" : "true");

            if (isVisible) {
                refresh();
            }
        }

        function getCards() {
            if (typeof config.getCards === "function") {
                return config.getCards() || [];
            }

            return [];
        }

        function getSummaryItems(stats) {
            if (typeof config.getSummaryItems === "function") {
                return config.getSummaryItems(stats) || [];
            }

            return [
                {
                    label: config.totalLabel || "Matching Cards",
                    value: stats.totalCards
                },
                {
                    label: "Creatures",
                    value: stats.creatureCount
                },
                {
                    label: "Noncreatures",
                    value: stats.nonCreatureCount
                },
                {
                    label: "Average MV",
                    value: stats.averageManaValue.toFixed(1)
                }
            ];
        }

        function refresh() {
            const stats = buildStats(getCards(), {
                averageManaValueExcludesLands: Boolean(config.averageManaValueExcludesLands)
            });

            if (title) {
                title.textContent = config.title || "Stats";
            }

            if (subtitle) {
                subtitle.textContent = config.subtitle || "Review card statistics.";
            }

            renderSummary(summaryGrid, getSummaryItems(stats));
            renderManaDistribution(manaDistribution, stats.manaBuckets, stats.maxManaBucket);
            renderColorDistribution(colorDistribution, stats.colorCounts, stats.totalCards);
            renderTypeDistribution(typeDistribution, stats.typeCounts, stats.totalCards);
        }

        if (openButton) {
            openButton.addEventListener("click", function () {
                if (typeof config.beforeOpen === "function") {
                    config.beforeOpen();
                }

                setVisible(true);
            });
        }

        if (backdrop) {
            backdrop.addEventListener("click", function () {
                setVisible(false);
            });
        }

        if (closeButton) {
            closeButton.addEventListener("click", function () {
                setVisible(false);
            });
        }

        return {
            open: function () {
                if (typeof config.beforeOpen === "function") {
                    config.beforeOpen();
                }

                setVisible(true);
            },
            close: function () {
                setVisible(false);
            },
            refresh: refresh,
            isOpen: function () {
                return Boolean(modal && !modal.classList.contains("hidden"));
            }
        };
    }

    window.iMomirStatsModal = {
        init: initStatsModal
    };
})();