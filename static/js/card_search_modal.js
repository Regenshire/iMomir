(function () {
    function getElement(id) {
        return document.getElementById(id);
    }

    function safeText(value) {
        return String(value || "").trim();
    }

    function parseJsonArray(rawValue) {
        try {
            const parsed = JSON.parse(rawValue || "[]");
            return Array.isArray(parsed) ? parsed : [];
        } catch (error) {
            return [];
        }
    }

    function normalizeColorArray(rawValue) {
        return parseJsonArray(rawValue)
            .map(function (item) {
                return String(item || "").trim().toUpperCase();
            })
            .filter(Boolean);
    }

    const colorNameMap = {
        "W": "White",
        "U": "Blue",
        "B": "Black",
        "R": "Red",
        "G": "Green"
    };

    const digitalSetCodeLookup = new Set([
        "AKR", "ANA", "ANB", "EA1", "HBG", "J21", "KLR",
        "MED", "ME2", "ME3", "ME4", "PRM", "PZ1", "PZ2",
        "SIR", "SIS", "TPR", "VMA"
    ]);

    function isDigitalSetCode(setCode) {
        const cleanSetCode = String(setCode || "").trim().toUpperCase();

        if (!cleanSetCode) {
            return false;
        }

        if (cleanSetCode.charAt(0) === "Y") {
            return true;
        }

        return digitalSetCodeLookup.has(cleanSetCode);
    }

    function getColorIdentityLabel(rawValue, typeLine) {
        const colors = normalizeColorArray(rawValue);
        const cleanTypeLine = String(typeLine || "").toLowerCase();

        if (cleanTypeLine.indexOf("land") !== -1 && colors.length === 0) {
            return "Colorless Land";
        }

        if (!colors.length) {
            return "Colorless";
        }

        return colors.map(function (symbol) {
            return colorNameMap[symbol] || symbol;
        }).join("/");
    }

    function getNumericSortValue(rawValue, nullValue) {
        const cleanValue = String(rawValue ?? "").trim();

        if (!cleanValue) {
            return nullValue;
        }

        const parsedValue = Number(cleanValue);

        if (!Number.isFinite(parsedValue)) {
            return nullValue;
        }

        return parsedValue;
    }

    function compareText(a, b) {
        return String(a || "").localeCompare(String(b || ""), undefined, {
            sensitivity: "base",
            numeric: true
        });
    }

    function getRarityRank(rarityValue, highToLow) {
        const cleanRarity = String(rarityValue || "").toLowerCase();

        const lowHighRanks = {
            "common": 1,
            "uncommon": 2,
            "rare": 3,
            "mythic": 4
        };

        const highLowRanks = {
            "mythic": 1,
            "rare": 2,
            "uncommon": 3,
            "common": 4
        };

        const rankMap = highToLow ? highLowRanks : lowHighRanks;
        return rankMap[cleanRarity] || 99;
    }

    function compareCardRows(rowA, rowB, sortValue) {
        const sortOption = sortValue || "name_asc";

        if (sortOption === "name_desc") {
            return compareText(rowB.dataset.cardName, rowA.dataset.cardName);
        }

        if (sortOption === "set_asc") {
            return compareText(rowA.dataset.setCode, rowB.dataset.setCode) || compareText(rowA.dataset.cardName, rowB.dataset.cardName);
        }

        if (sortOption === "set_desc") {
            return compareText(rowB.dataset.setCode, rowA.dataset.setCode) || compareText(rowA.dataset.cardName, rowB.dataset.cardName);
        }

        if (sortOption === "year_newest") {
            return getNumericSortValue(rowB.dataset.releaseDateSort, 0) - getNumericSortValue(rowA.dataset.releaseDateSort, 0) || compareText(rowA.dataset.cardName, rowB.dataset.cardName);
        }

        if (sortOption === "year_oldest") {
            return getNumericSortValue(rowA.dataset.releaseDateSort, 99999999) - getNumericSortValue(rowB.dataset.releaseDateSort, 99999999) || compareText(rowA.dataset.cardName, rowB.dataset.cardName);
        }

        if (sortOption === "rarity_low_high") {
            return getRarityRank(rowA.dataset.rarity, false) - getRarityRank(rowB.dataset.rarity, false) || compareText(rowA.dataset.cardName, rowB.dataset.cardName);
        }

        if (sortOption === "rarity_high_low") {
            return getRarityRank(rowA.dataset.rarity, true) - getRarityRank(rowB.dataset.rarity, true) || compareText(rowA.dataset.cardName, rowB.dataset.cardName);
        }

        if (sortOption === "mv_low_high") {
            return getNumericSortValue(rowA.dataset.manaValue, 999) - getNumericSortValue(rowB.dataset.manaValue, 999) || compareText(rowA.dataset.cardName, rowB.dataset.cardName);
        }

        if (sortOption === "mv_high_low") {
            return getNumericSortValue(rowB.dataset.manaValue, -1) - getNumericSortValue(rowA.dataset.manaValue, -1) || compareText(rowA.dataset.cardName, rowB.dataset.cardName);
        }

        if (sortOption === "edhrec_rank_best") {
            return getNumericSortValue(rowA.dataset.edhrecRank, 999999999) - getNumericSortValue(rowB.dataset.edhrecRank, 999999999) || compareText(rowA.dataset.cardName, rowB.dataset.cardName);
        }

        if (sortOption === "edhrec_rank_worst") {
            return getNumericSortValue(rowB.dataset.edhrecRank, -1) - getNumericSortValue(rowA.dataset.edhrecRank, -1) || compareText(rowA.dataset.cardName, rowB.dataset.cardName);
        }

        if (sortOption === "edhrec_salt_high") {
            return getNumericSortValue(rowB.dataset.edhrecSaltiness, -1) - getNumericSortValue(rowA.dataset.edhrecSaltiness, -1) || compareText(rowA.dataset.cardName, rowB.dataset.cardName);
        }

        if (sortOption === "edhrec_salt_low") {
            return getNumericSortValue(rowA.dataset.edhrecSaltiness, 999999999) - getNumericSortValue(rowB.dataset.edhrecSaltiness, 999999999) || compareText(rowA.dataset.cardName, rowB.dataset.cardName);
        }

        if (sortOption === "price_high") {
            return getNumericSortValue(rowB.dataset.sortPrice, -1) - getNumericSortValue(rowA.dataset.sortPrice, -1) || compareText(rowA.dataset.cardName, rowB.dataset.cardName);
        }

        if (sortOption === "price_low") {
            return getNumericSortValue(rowA.dataset.sortPrice, 999999999) - getNumericSortValue(rowB.dataset.sortPrice, 999999999) || compareText(rowA.dataset.cardName, rowB.dataset.cardName);
        }

        return compareText(rowA.dataset.cardName, rowB.dataset.cardName);
    }

    function getCardReleaseYear(card) {
        const rawYear = card && card.release_year ? String(card.release_year).trim() : "";

        if (!rawYear) {
            return 0;
        }

        const parsedYear = Number(rawYear);

        if (!Number.isFinite(parsedYear)) {
            return 0;
        }

        return parsedYear;
    }

    function getCardReleaseDateSortValue(card) {
        const rawDate = card && card.release_date ? String(card.release_date).trim() : "";

        if (!rawDate) {
            return getCardReleaseYear(card) * 10000;
        }

        const cleanDate = rawDate.replace(/[^0-9]/g, "");

        if (!cleanDate) {
            return getCardReleaseYear(card) * 10000;
        }

        const parsedDate = Number(cleanDate);

        if (!Number.isFinite(parsedDate)) {
            return getCardReleaseYear(card) * 10000;
        }

        return parsedDate;
    }

    function normalizeLookupText(rawValue) {
        return String(rawValue || "")
            .trim()
            .toLowerCase()
            .replace(/[’'`´‘ʼ]/g, "")
            .replace(/[.,]/g, "")
            .replace(/\s+/g, " ");
    }

    function buildPrintingKey(cardName, setCode, collectorNumber) {
        const cleanCardName = normalizeLookupText(cardName);
        const cleanSetCode = String(setCode || "").trim().toUpperCase();
        const cleanCollectorNumber = normalizeLookupText(collectorNumber);

        if (!cleanCardName || !cleanSetCode || !cleanCollectorNumber) {
            return "";
        }

        return cleanCardName + "|" + cleanSetCode + "|" + cleanCollectorNumber;
    }

    function buildVirtualSearchCardRow(card, cardNameOverride) {
        const cardName = cardNameOverride !== undefined
            ? cardNameOverride
            : card.card_name;

        return {
            dataset: {
                cardName: String(cardName || "").toLowerCase(),
                setCode: String(card.set_code || "").toUpperCase(),
                rarity: String(card.rarity || "").toLowerCase(),
                manaValue: card.mana_value === null || card.mana_value === undefined ? "" : String(card.mana_value),
                edhrecRank: card.edhrec_rank === null || card.edhrec_rank === undefined ? "" : String(card.edhrec_rank),
                edhrecSaltiness: card.edhrec_saltiness === null || card.edhrec_saltiness === undefined ? "" : String(card.edhrec_saltiness),
                sortPrice: card.sort_price === null || card.sort_price === undefined ? "" : String(card.sort_price),
                releaseDateSort: String(getCardReleaseDateSortValue(card))
            }
        };
    }

    function getMostRecentCardPrinting(cards) {
        const cardList = Array.isArray(cards) ? cards.slice() : [];

        cardList.sort(function (cardA, cardB) {
            return getCardReleaseDateSortValue(cardB) - getCardReleaseDateSortValue(cardA);
        });

        return cardList[0] || null;
    }

    function initCardSearchModal(config) {
        config = config || {};

        const searchInput = getElement("customDraftCardSearchInput");
        const searchButton = getElement("customDraftCardSearchButton");
        const searchStatus = getElement("customDraftCardSearchStatus");
        const searchResults = getElement("customDraftCardSearchResults");
        const searchOverlay = getElement("customDraftCardSearchOverlay");
        const closeButton = getElement("customDraftCardSearchCloseButton");

        const searchRarityFilter = getElement("customDraftSearchRarityFilter");
        const searchColorFilter = getElement("customDraftSearchColorFilter");
        const searchManaOperatorFilter = getElement("customDraftSearchManaOperatorFilter");
        const searchManaValueFilter = getElement("customDraftSearchManaValueFilter");
        const searchTypeFilter = getElement("customDraftSearchTypeFilter");
        const searchSetCodeFilter = getElement("customDraftSearchSetCodeFilter");
        const searchSortSelect = getElement("customDraftSearchSortSelect");
        const searchYearStartFilter = getElement("customDraftSearchYearStartFilter");
        const searchYearEndFilter = getElement("customDraftSearchYearEndFilter");
        const searchDigitalFilter = getElement("customDraftSearchDigitalFilter");
        const searchPageSizeSelect = getElement("customDraftSearchPageSizeSelect");
        const searchPaginationBar = getElement("customDraftCardSearchPagination");
        const searchPrevButton = getElement("customDraftSearchPrevButton");
        const searchNextButton = getElement("customDraftSearchNextButton");
        const searchPageInput = getElement("customDraftSearchPageInput");
        const searchPageTotal = getElement("customDraftSearchPageTotal");
        const searchClearFiltersButton = getElement("customDraftSearchClearFiltersButton");

        const bulkImportToggleButton = getElement("customDraftBulkImportToggleButton");
        const bulkImportPanel = getElement("customDraftBulkImportPanel");
        const bulkImportText = getElement("customDraftBulkImportText");
        const bulkImportFile = getElement("customDraftBulkImportFile");
        const bulkImportButton = getElement("customDraftBulkImportButton");
        const bulkImportAddMostRecentButton = getElement("customDraftBulkImportAddMostRecentButton");

        let activeChangePrintingState = null;
        let currentSearchPage = 1;
        let currentSearchTotalPages = 1;
        let modalZoomOverlay = null;
        let modalZoomImage = null;

        const clientAddedCardUuidLookup = new Set();
        const clientAddedPrintingKeyLookup = new Set();

        function ensureModalZoomOverlay() {
            if (modalZoomOverlay && modalZoomImage) {
                return;
            }

            modalZoomOverlay = document.createElement("div");
            modalZoomOverlay.className = "custom-draft-card-search-zoom-overlay hidden";
            modalZoomOverlay.setAttribute("aria-hidden", "true");

            modalZoomOverlay.style.position = "fixed";
            modalZoomOverlay.style.inset = "0";
            modalZoomOverlay.style.zIndex = "99999";
            modalZoomOverlay.style.display = "none";
            modalZoomOverlay.style.alignItems = "center";
            modalZoomOverlay.style.justifyContent = "center";
            modalZoomOverlay.style.background = "rgba(0, 0, 0, 0.72)";
            modalZoomOverlay.style.backdropFilter = "blur(3px)";
            modalZoomOverlay.style.padding = "24px";
            modalZoomOverlay.style.cursor = "zoom-out";

            modalZoomImage = document.createElement("img");
            modalZoomImage.className = "custom-draft-card-search-zoom-image";
            modalZoomImage.alt = "Card preview";

            modalZoomImage.style.maxWidth = "min(92vw, 520px)";
            modalZoomImage.style.maxHeight = "92vh";
            modalZoomImage.style.objectFit = "contain";
            modalZoomImage.style.borderRadius = "16px";
            modalZoomImage.style.boxShadow = "0 24px 80px rgba(0, 0, 0, 0.75)";
            modalZoomImage.style.cursor = "zoom-out";

            modalZoomOverlay.appendChild(modalZoomImage);
            document.body.appendChild(modalZoomOverlay);

            modalZoomOverlay.addEventListener("click", function () {
                closeModalImageZoom();
            });
        }

        function openModalImageZoom(imageSrc, imageAlt) {
            const cleanImageSrc = String(imageSrc || "").trim();

            if (!cleanImageSrc) {
                return;
            }

            ensureModalZoomOverlay();

            modalZoomImage.src = cleanImageSrc;
            modalZoomImage.alt = imageAlt || "Card preview";
            modalZoomOverlay.classList.remove("hidden");
            modalZoomOverlay.setAttribute("aria-hidden", "false");
            modalZoomOverlay.style.display = "flex";
        }

        function closeModalImageZoom() {
            if (!modalZoomOverlay || !modalZoomImage) {
                return;
            }

            modalZoomOverlay.classList.add("hidden");
            modalZoomOverlay.setAttribute("aria-hidden", "true");
            modalZoomOverlay.style.display = "none";
            modalZoomImage.src = "";
            modalZoomImage.alt = "Card preview";
        }

        function bindModalZoomableImages() {
            document.querySelectorAll("#customDraftCardSearchOverlay .custom-draft-card-zoomable").forEach(function (imageElement) {
                if (imageElement.dataset.sharedCardSearchZoomBound === "1") {
                    return;
                }

                imageElement.dataset.sharedCardSearchZoomBound = "1";

                imageElement.addEventListener("click", function (event) {
                    event.preventDefault();
                    event.stopPropagation();

                    openModalImageZoom(
                        imageElement.dataset.zoomSrc || imageElement.src || "",
                        imageElement.dataset.zoomAlt || imageElement.alt || "Card preview"
                    );
                });

                imageElement.addEventListener("keydown", function (event) {
                    if (event.key === "Enter" || event.key === " ") {
                        event.preventDefault();
                        event.stopPropagation();

                        openModalImageZoom(
                            imageElement.dataset.zoomSrc || imageElement.src || "",
                            imageElement.dataset.zoomAlt || imageElement.alt || "Card preview"
                        );
                    }
                });
            });
        }

        function showMessage(messageText, isError) {
            if (typeof config.showMessage === "function") {
                config.showMessage(messageText, isError);
                return;
            }

            if (window.iMomirToast) {
                if (isError) {
                    window.iMomirToast.error(messageText || "");
                } else {
                    window.iMomirToast.success(messageText || "");
                }
                return;
            }

            console.log(messageText || "");
        }

        async function confirmAction(options) {
            if (typeof config.confirmAction === "function") {
                return await config.confirmAction(options || {});
            }

            if (window.iMomirConfirm && typeof window.iMomirConfirm.show === "function") {
                return await window.iMomirConfirm.show(options || {});
            }

            return window.confirm((options && options.message) || "Continue?");
        }

        function getExistingCardLookup() {
            const lookup = {
                cardUuids: new Set(clientAddedCardUuidLookup),
                printingKeys: new Set(clientAddedPrintingKeyLookup)
            };

            if (typeof config.getExistingCardLookup === "function") {
                const pageLookup = config.getExistingCardLookup() || {};

                Array.from(pageLookup.cardUuids || []).forEach(function (cardUuid) {
                    lookup.cardUuids.add(cardUuid);
                });

                Array.from(pageLookup.printingKeys || []).forEach(function (printingKey) {
                    lookup.printingKeys.add(printingKey);
                });
            }

            return lookup;
        }

        function getSearchCardPrintingKey(card) {
            if (!card) {
                return "";
            }

            return buildPrintingKey(
                card.card_name || "",
                card.set_code || "",
                card.collector_number || ""
            );
        }

        function isSearchCardAlreadyAdded(card) {
            if (activeChangePrintingState) {
                return false;
            }

            if (card && card.already_in_set) {
                return true;
            }

            const lookup = getExistingCardLookup();
            const cardUuid = String(card && card.card_uuid ? card.card_uuid : "").trim();
            const printingKey = getSearchCardPrintingKey(card);

            if (cardUuid && lookup.cardUuids.has(cardUuid)) {
                return true;
            }

            if (printingKey && lookup.printingKeys.has(printingKey)) {
                return true;
            }

            return false;
        }

        function getSelectedMultiSelectValues(containerElement) {
            if (!containerElement) {
                return [];
            }

            return Array.from(containerElement.querySelectorAll('input[type="checkbox"]:checked'))
                .map(function (checkbox) {
                    return String(checkbox.value || "").trim().toLowerCase();
                })
                .filter(Boolean);
        }

        function getMultiSelectLabelForValue(containerElement, rawValue) {
            const cleanValue = String(rawValue || "").trim().toLowerCase();

            const matchingInput = containerElement
                ? containerElement.querySelector('input[type="checkbox"][value="' + CSS.escape(cleanValue) + '"]')
                : null;

            if (matchingInput) {
                const matchingLabel = matchingInput.closest("label");

                if (matchingLabel) {
                    const labelSpan = matchingLabel.querySelector("span");

                    if (labelSpan && labelSpan.textContent) {
                        return labelSpan.textContent.trim();
                    }
                }
            }

            return rawValue;
        }

        function updateMultiSelectLabel(containerElement) {
            if (!containerElement) {
                return;
            }

            const labelElement = containerElement.querySelector(".custom-draft-multi-select-label");
            const selectedValues = getSelectedMultiSelectValues(containerElement);
            const totalValues = containerElement.querySelectorAll('input[type="checkbox"]').length;
            const placeholderText = containerElement.dataset.placeholder || "Any";
            const allLabelText = containerElement.dataset.allLabel || "All";

            if (!labelElement) {
                return;
            }

            if (!selectedValues.length) {
                labelElement.textContent = placeholderText;
                return;
            }

            if (totalValues > 0 && selectedValues.length === totalValues) {
                labelElement.textContent = allLabelText;
                return;
            }

            labelElement.textContent = selectedValues
                .map(function (selectedValue) {
                    return getMultiSelectLabelForValue(containerElement, selectedValue);
                })
                .join(", ");
        }

        function clearMultiSelectValues(containerElement) {
            if (!containerElement) {
                return;
            }

            Array.from(containerElement.querySelectorAll('input[type="checkbox"]')).forEach(function (checkbox) {
                checkbox.checked = false;
            });

            updateMultiSelectLabel(containerElement);
        }

        function closeMultiSelect(containerElement) {
            if (!containerElement) {
                return;
            }

            const button = containerElement.querySelector(".custom-draft-multi-select-button");
            const menu = containerElement.querySelector(".custom-draft-multi-select-menu");

            if (button) {
                button.setAttribute("aria-expanded", "false");
            }

            if (menu) {
                menu.classList.add("hidden");
            }
        }

        function bindMultiSelect(containerElement, callback) {
            if (!containerElement || typeof callback !== "function") {
                return;
            }

            const button = containerElement.querySelector(".custom-draft-multi-select-button");
            const menu = containerElement.querySelector(".custom-draft-multi-select-menu");

            if (button && menu) {
                button.addEventListener("click", function (event) {
                    event.preventDefault();
                    event.stopPropagation();

                    const shouldOpen = menu.classList.contains("hidden");

                    document.querySelectorAll(".custom-draft-multi-select").forEach(function (otherContainer) {
                        if (otherContainer !== containerElement) {
                            closeMultiSelect(otherContainer);
                        }
                    });

                    menu.classList.toggle("hidden", !shouldOpen);
                    button.setAttribute("aria-expanded", shouldOpen ? "true" : "false");
                });
            }

            Array.from(containerElement.querySelectorAll('input[type="checkbox"]')).forEach(function (checkbox) {
                checkbox.addEventListener("change", function () {
                    updateMultiSelectLabel(containerElement);
                    callback();
                });
            });

            updateMultiSelectLabel(containerElement);
        }

        function rowMatchesRarityFilter(rowRarity, selectedRarities) {
            if (!selectedRarities || !selectedRarities.length) {
                return true;
            }

            return selectedRarities.indexOf(String(rowRarity || "").trim().toLowerCase()) !== -1;
        }

        function rowMatchesColorFilter(row, selectedColorValues) {
            if (!selectedColorValues || !selectedColorValues.length) {
                return true;
            }

            const colors = normalizeColorArray(row.dataset.colorIdentity || "[]");
            const lowerColors = colors.map(function (colorSymbol) {
                return String(colorSymbol || "").toLowerCase();
            });

            const typeLine = String(row.dataset.typeLine || "").toLowerCase();
            const isLand = typeLine.indexOf("land") !== -1;
            const isColorless = colors.length === 0;
            const isMulti = colors.length >= 2;
            const isMonoColor = colors.length === 1;

            const selectedValues = selectedColorValues.map(function (selectedValue) {
                return String(selectedValue || "").trim().toLowerCase();
            });

            const selectedColorSymbols = selectedValues.filter(function (selectedValue) {
                return ["w", "u", "b", "r", "g"].indexOf(selectedValue) !== -1;
            });

            if (selectedValues.indexOf("colorless") !== -1 && isColorless && !isLand) {
                return true;
            }

            if (selectedValues.indexOf("land") !== -1 && isLand) {
                return true;
            }

            if (selectedValues.indexOf("multi_any") !== -1 && isMulti) {
                return true;
            }

            if (selectedValues.indexOf("multi_has_selected") !== -1 && isMulti) {
                if (!selectedColorSymbols.length) {
                    return true;
                }

                return lowerColors.some(function (cardColor) {
                    return selectedColorSymbols.indexOf(cardColor) !== -1;
                });
            }

            if (selectedValues.indexOf("multi_selected") !== -1 && isMulti) {
                if (!selectedColorSymbols.length) {
                    return true;
                }

                const hasOnlySelectedColors = lowerColors.every(function (cardColor) {
                    return selectedColorSymbols.indexOf(cardColor) !== -1;
                });

                const hasAtLeastOneSelectedColor = lowerColors.some(function (cardColor) {
                    return selectedColorSymbols.indexOf(cardColor) !== -1;
                });

                return hasOnlySelectedColors && hasAtLeastOneSelectedColor;
            }

            if (selectedColorSymbols.length && isMonoColor) {
                return selectedColorSymbols.indexOf(lowerColors[0]) !== -1;
            }

            return false;
        }

        function rowMatchesManaFilter(row, operatorValue, targetValueRaw) {
            if (!operatorValue) {
                return true;
            }

            const targetValue = Number(targetValueRaw);

            if (!Number.isFinite(targetValue)) {
                return true;
            }

            const rowManaValue = Number(row.dataset.manaValue);

            if (!Number.isFinite(rowManaValue)) {
                return false;
            }

            if (operatorValue === "=") {
                return rowManaValue === targetValue;
            }

            if (operatorValue === "<=") {
                return rowManaValue <= targetValue;
            }

            if (operatorValue === ">=") {
                return rowManaValue >= targetValue;
            }

            if (operatorValue === "<") {
                return rowManaValue < targetValue;
            }

            if (operatorValue === ">") {
                return rowManaValue > targetValue;
            }

            return true;
        }

        function rowMatchesDigitalFilter(row, digitalFilterValue) {
            const cleanDigitalFilter = String(digitalFilterValue || "").trim().toLowerCase();

            if (!cleanDigitalFilter) {
                return true;
            }

            const isDigital = isDigitalSetCode(row.dataset.setCode || "");

            if (cleanDigitalFilter === "exclude") {
                return !isDigital;
            }

            if (cleanDigitalFilter === "only") {
                return isDigital;
            }

            return true;
        }

        function setModalTitle(titleText, subtitleText) {
            const modalTitle = document.querySelector(".custom-draft-card-search-window-header h2");
            const modalSubtitle = document.querySelector(".custom-draft-card-search-window-header .page-subtitle");

            if (modalTitle) {
                modalTitle.textContent = titleText || "Add Cards";
            }

            if (modalSubtitle) {
                modalSubtitle.textContent = subtitleText || "Search by card name, set, color identity, rarity, mana value, year, spell type, EDHREC rank, or saltiness.";
            }
        }

        function setSearchStatus(messageText, isError) {
            if (!searchStatus) {
                return;
            }

            const cleanMessage = String(messageText || "").trim();

            searchStatus.textContent = cleanMessage;
            searchStatus.classList.toggle("custom-draft-card-search-status-error", Boolean(isError));
            searchStatus.classList.toggle("hidden", !cleanMessage);
        }

        function clearSearchResults() {
            if (!searchResults) {
                return;
            }

            searchResults.innerHTML = "";
            searchResults.classList.add("hidden");
        }

        function hidePagination() {
            if (searchPaginationBar) {
                searchPaginationBar.classList.add("custom-draft-search-pagination-hidden");
            }

            if (searchPageInput) {
                searchPageInput.value = 1;
                searchPageInput.max = 1;
            }

            if (searchPageTotal) {
                searchPageTotal.textContent = "of —";
            }

            if (searchPrevButton) {
                searchPrevButton.disabled = true;
            }

            if (searchNextButton) {
                searchNextButton.disabled = true;
            }
        }

        function updatePaginationBar(page, totalPages) {
            if (!searchPaginationBar) {
                return;
            }

            searchPaginationBar.classList.remove("custom-draft-search-pagination-hidden");

            if (searchPageInput) {
                searchPageInput.value = page;
                searchPageInput.max = totalPages;
            }

            if (searchPageTotal) {
                searchPageTotal.textContent = "of " + totalPages;
            }

            if (searchPrevButton) {
                searchPrevButton.disabled = page <= 1;
            }

            if (searchNextButton) {
                searchNextButton.disabled = page >= totalPages;
            }
        }

        function setPaginationLoadingState(page) {
            if (!searchPaginationBar) {
                return;
            }

            searchPaginationBar.classList.remove("custom-draft-search-pagination-hidden");

            if (searchPageInput) {
                searchPageInput.value = page || currentSearchPage || 1;
                searchPageInput.max = currentSearchTotalPages || 1;
            }

            if (searchPageTotal) {
                searchPageTotal.textContent = "of " + (currentSearchTotalPages || "—");
            }

            if (searchPrevButton) {
                searchPrevButton.disabled = true;
            }

            if (searchNextButton) {
                searchNextButton.disabled = true;
            }
        }

        function getSearchLoadingPhrase() {
            if (
                window.iMomirCustomDraftSearchPhrases
                && typeof window.iMomirCustomDraftSearchPhrases.getLoadingPhrase === "function"
            ) {
                return window.iMomirCustomDraftSearchPhrases.getLoadingPhrase({
                    colorValues: getSelectedMultiSelectValues(searchColorFilter),
                    typeValue: searchTypeFilter ? searchTypeFilter.value : ""
                });
            }

            return "Searching the library...";
        }

        function getAdvancedSearchHasAnyValue() {
            return Boolean(
                (searchInput && (searchInput.value || "").trim()) ||
                getSelectedMultiSelectValues(searchRarityFilter).length > 0 ||
                getSelectedMultiSelectValues(searchColorFilter).length > 0 ||
                (searchManaOperatorFilter && (searchManaOperatorFilter.value || "").trim()) ||
                (searchManaValueFilter && (searchManaValueFilter.value || "").trim()) ||
                (searchTypeFilter && (searchTypeFilter.value || "").trim()) ||
                (searchSetCodeFilter && (searchSetCodeFilter.value || "").trim()) ||
                (searchYearStartFilter && (searchYearStartFilter.value || "").trim()) ||
                (searchYearEndFilter && (searchYearEndFilter.value || "").trim())
            );
        }

        function buildAdvancedCardSearchUrl(page) {
            const params = new URLSearchParams();
            const requestedPage = page || 1;

            if (searchInput && (searchInput.value || "").trim()) {
                params.set("q", (searchInput.value || "").trim());
            }

            getSelectedMultiSelectValues(searchRarityFilter).forEach(function (rarityValue) {
                params.append("rarity", rarityValue);
            });

            getSelectedMultiSelectValues(searchColorFilter).forEach(function (colorValue) {
                params.append("color_identity", colorValue);
            });

            if (searchManaOperatorFilter && searchManaOperatorFilter.value) {
                params.set("mana_operator", searchManaOperatorFilter.value);
            }

            if (searchManaValueFilter && (searchManaValueFilter.value || "").trim()) {
                params.set("mana_value", (searchManaValueFilter.value || "").trim());
            }

            if (searchTypeFilter && searchTypeFilter.value) {
                params.set("type", searchTypeFilter.value);
            }

            if (searchSetCodeFilter && (searchSetCodeFilter.value || "").trim()) {
                params.set("set_code", (searchSetCodeFilter.value || "").trim().toUpperCase());
            }

            if (searchYearStartFilter && (searchYearStartFilter.value || "").trim()) {
                params.set("year_start", (searchYearStartFilter.value || "").trim());
            }

            if (searchYearEndFilter && (searchYearEndFilter.value || "").trim()) {
                params.set("year_end", (searchYearEndFilter.value || "").trim());
            }

            if (searchDigitalFilter && searchDigitalFilter.value) {
                params.set("digital", searchDigitalFilter.value);
            }

            if (searchPageSizeSelect && searchPageSizeSelect.value) {
                params.set("page_size", searchPageSizeSelect.value);
            }

            params.set("page", String(requestedPage));

            if (searchSortSelect && searchSortSelect.value) {
                params.set("sort", searchSortSelect.value);
            }

            return config.searchUrl + "?" + params.toString();
        }

        function getSearchResultsForActiveMode(results) {
            if (!activeChangePrintingState) {
                return results || [];
            }

            const targetCardName = String(activeChangePrintingState.cardName || "").trim().toLowerCase();

            return (results || []).filter(function (card) {
                return String(card.card_name || "").trim().toLowerCase() === targetCardName;
            });
        }

        function getGroupedSearchCards(cards) {
            const groupLookup = new Map();

            (cards || []).forEach(function (card) {
                const cardName = String(card.card_name || "Unknown Card").trim() || "Unknown Card";
                const groupKey = cardName.toLowerCase();

                if (!groupLookup.has(groupKey)) {
                    groupLookup.set(groupKey, {
                        key: groupKey,
                        card_name: cardName,
                        cards: []
                    });
                }

                groupLookup.get(groupKey).cards.push(card);
            });

            const groups = Array.from(groupLookup.values());

            groups.forEach(function (group) {
                const selectedSortValue = searchSortSelect ? searchSortSelect.value : "name_asc";

                group.representative_card = getMostRecentCardPrinting(group.cards);

                group.cards.sort(function (cardA, cardB) {
                    const cardAAlreadyAdded = isSearchCardAlreadyAdded(cardA);
                    const cardBAlreadyAdded = isSearchCardAlreadyAdded(cardB);

                    if (cardAAlreadyAdded !== cardBAlreadyAdded) {
                        return cardAAlreadyAdded ? -1 : 1;
                    }

                    const rowA = buildVirtualSearchCardRow(cardA);
                    const rowB = buildVirtualSearchCardRow(cardB);

                    const sortCompare = compareCardRows(rowA, rowB, selectedSortValue);

                    if (sortCompare !== 0) {
                        return sortCompare;
                    }

                    return getCardReleaseDateSortValue(cardB) - getCardReleaseDateSortValue(cardA);
                });

                group.already_in_set_count = group.cards.filter(function (card) {
                    return isSearchCardAlreadyAdded(card);
                }).length;
            });

            groups.sort(function (groupA, groupB) {
                const firstA = groupA.cards[0] || {};
                const firstB = groupB.cards[0] || {};

                const virtualRowA = buildVirtualSearchCardRow(firstA, groupA.card_name);
                const virtualRowB = buildVirtualSearchCardRow(firstB, groupB.card_name);

                return compareCardRows(
                    virtualRowA,
                    virtualRowB,
                    searchSortSelect ? searchSortSelect.value : "name_asc"
                );
            });

            return groups;
        }

        function createSearchResultPrintingRow(card) {
            const row = document.createElement("div");
            row.className = "custom-draft-card-search-row custom-draft-card-search-printing-row";

            const manaValue = card.mana_value === null || card.mana_value === undefined || card.mana_value === ""
                ? ""
                : String(card.mana_value);

            row.dataset.cardSearch = [
                card.card_name || "",
                card.set_code || "",
                card.collector_number || "",
                card.rarity || "",
                card.type_line || "",
                card.color_identity_json || "[]",
                manaValue
            ].join(" ").toLowerCase();

            row.dataset.cardUuid = String(card.card_uuid || "");
            row.dataset.printingKey = getSearchCardPrintingKey(card);
            row.dataset.rarity = String(card.rarity || "").toLowerCase();
            row.dataset.colorIdentity = String(card.color_identity_json || "[]").toLowerCase();
            row.dataset.manaValue = manaValue;
            row.dataset.typeLine = String(card.type_line || "").toLowerCase();
            row.dataset.cardName = String(card.card_name || "").toLowerCase();
            row.dataset.displayCardName = String(card.card_name || "");
            row.dataset.setCode = String(card.set_code || "").toUpperCase();
            row.dataset.releaseYear = String(card.release_year || "");
            row.dataset.collectorNumber = String(card.collector_number || "");
            row.dataset.rarityDisplay = String(card.rarity || "");
            row.dataset.imageSrc = String(card.image_src || "");
            row.dataset.edhrecRank = card.edhrec_rank === null || card.edhrec_rank === undefined ? "" : String(card.edhrec_rank);
            row.dataset.edhrecSaltiness = card.edhrec_saltiness === null || card.edhrec_saltiness === undefined ? "" : String(card.edhrec_saltiness);
            row.dataset.sortPrice = card.sort_price === null || card.sort_price === undefined ? "" : String(card.sort_price);

            const alreadyAdded = isSearchCardAlreadyAdded(card);

            if (alreadyAdded) {
                row.classList.add("custom-draft-card-search-row-disabled");
            }

            const imageWrap = document.createElement("div");
            imageWrap.className = "custom-draft-card-search-image-wrap";

            const image = document.createElement("img");
            image.className = "custom-draft-card-search-image custom-draft-card-zoomable";
            image.src = card.image_src || "";
            image.alt = card.card_name || "Card";
            image.setAttribute("role", "button");
            image.setAttribute("tabindex", "0");
            image.dataset.zoomSrc = card.image_src || "";
            image.dataset.zoomAlt = card.card_name || "Card";

            imageWrap.appendChild(image);

            const main = document.createElement("div");
            main.className = "custom-draft-card-search-main";

            const title = document.createElement("div");
            title.className = "custom-draft-card-search-title";
            title.textContent = card.card_name || "Unknown Card";

            const meta = document.createElement("div");
            meta.className = "custom-draft-card-search-meta";

            const displayManaValue = manaValue || "?";

            const metaLineOne = document.createElement("div");
            metaLineOne.className = "custom-draft-card-search-meta-line";
            metaLineOne.textContent = [
                card.set_code || "",
                card.release_year ? card.release_year : "",
                card.collector_number ? "#" + card.collector_number : "",
                card.rarity || "",
                "MV " + displayManaValue,
                "EDHREC " + (card.edhrec_rank === null || card.edhrec_rank === undefined ? "?" : card.edhrec_rank),
                "Salt " + (card.edhrec_saltiness === null || card.edhrec_saltiness === undefined ? "?" : Number(card.edhrec_saltiness).toFixed(2)),
                "Price " + (card.sort_price === null || card.sort_price === undefined ? "?" : "$" + Number(card.sort_price).toFixed(2)),
                "Color Identity: " + getColorIdentityLabel(card.color_identity_json || "[]", card.type_line || "")
            ].filter(Boolean).join(" • ");

            const metaLineTwo = document.createElement("div");
            metaLineTwo.className = "custom-draft-card-search-type-line";
            metaLineTwo.textContent = card.type_line || "";

            meta.appendChild(metaLineOne);
            meta.appendChild(metaLineTwo);

            main.appendChild(title);
            main.appendChild(meta);

            const actions = document.createElement("div");
            actions.className = "custom-draft-card-search-actions";

            const actionButton = document.createElement("button");
            actionButton.type = "button";
            actionButton.className = "action-button secondary-button custom-draft-card-add-button";

            if (activeChangePrintingState) {
                const isCurrentPrinting = card.card_uuid === activeChangePrintingState.currentCardUuid;

                actionButton.textContent = isCurrentPrinting ? "Current" : "Switch";
                actionButton.disabled = Boolean(isCurrentPrinting);

                actionButton.addEventListener("click", async function () {
                    await changeCardPrinting(card.card_uuid, actionButton);
                });
            } else {
                actionButton.textContent = alreadyAdded ? "Added" : "Add";
                actionButton.disabled = alreadyAdded;

                actionButton.addEventListener("click", async function () {
                    await addCard(card.card_uuid, actionButton);
                });
            }

            actions.appendChild(actionButton);

            row.appendChild(imageWrap);
            row.appendChild(main);
            row.appendChild(actions);

            return row;
        }

        function refreshSearchResultAddedStates() {
            if (!searchResults) {
                return;
            }

            if (activeChangePrintingState) {
                Array.from(searchResults.querySelectorAll(".custom-draft-card-search-row-disabled")).forEach(function (row) {
                    row.classList.remove("custom-draft-card-search-row-disabled");
                });

                Array.from(searchResults.querySelectorAll(".custom-draft-card-search-group-added-pill")).forEach(function (addedPill) {
                    addedPill.classList.add("hidden");
                    addedPill.textContent = "Added";
                });

                Array.from(searchResults.querySelectorAll(".custom-draft-card-search-group-meta")).forEach(function (groupMeta) {
                    groupMeta.textContent = String(groupMeta.textContent || "").replace(/\s•\s\d+\s+already added/g, "");
                });

                return;
            }

            Array.from(searchResults.querySelectorAll(".custom-draft-card-search-row")).forEach(function (row) {
                const cardUuid = String(row.dataset.cardUuid || "").trim();
                const printingKey = String(row.dataset.printingKey || "").trim();
                const lookup = getExistingCardLookup();

                const isAlreadyAdded = (
                    (cardUuid && lookup.cardUuids.has(cardUuid))
                    || (printingKey && lookup.printingKeys.has(printingKey))
                );

                if (!isAlreadyAdded) {
                    return;
                }

                row.classList.add("custom-draft-card-search-row-disabled");

                const addButton = row.querySelector(".custom-draft-card-add-button");
                if (addButton && !activeChangePrintingState) {
                    addButton.textContent = "Added";
                    addButton.disabled = true;
                }
            });

            Array.from(searchResults.querySelectorAll(".custom-draft-card-search-group")).forEach(function (groupElement) {
                const disabledRows = Array.from(groupElement.querySelectorAll(".custom-draft-card-search-row-disabled"));
                const addedPill = groupElement.querySelector(".custom-draft-card-search-group-added-pill");
                const groupMeta = groupElement.querySelector(".custom-draft-card-search-group-meta");

                if (addedPill) {
                    addedPill.classList.toggle("hidden", disabledRows.length === 0);
                    addedPill.textContent = disabledRows.length > 1
                        ? disabledRows.length + " Added"
                        : "Added";
                }

                if (groupMeta && disabledRows.length > 0 && groupMeta.textContent.indexOf("already added") === -1) {
                    groupMeta.textContent = groupMeta.textContent
                        ? groupMeta.textContent + " • " + disabledRows.length + " already added"
                        : disabledRows.length + " already added";
                }
            });
        }

        function collapseSearchResultGroup(groupElement) {
            if (!groupElement) {
                return;
            }

            const headerButton = groupElement.querySelector(".custom-draft-card-search-group-header");
            const expandIcon = groupElement.querySelector(".custom-draft-card-search-group-expand-icon");

            groupElement.classList.remove("custom-draft-card-search-group-expanded");

            if (headerButton) {
                headerButton.setAttribute("aria-expanded", "false");
            }

            if (expandIcon) {
                expandIcon.textContent = "+";
            }
        }

        function updateSearchResultGroupFromAddedRow(resultRow) {
            if (!resultRow) {
                return;
            }

            const groupElement = resultRow.closest(".custom-draft-card-search-group");

            if (!groupElement) {
                return;
            }

            const groupImage = groupElement.querySelector(".custom-draft-card-search-group-image");
            const groupMeta = groupElement.querySelector(".custom-draft-card-search-group-meta");
            const addedPill = groupElement.querySelector(".custom-draft-card-search-group-added-pill");

            const imageSrc = resultRow.dataset.imageSrc || "";
            const displayCardName = resultRow.dataset.displayCardName || "";
            const setCode = resultRow.dataset.setCode || "";
            const releaseYear = resultRow.dataset.releaseYear || "";
            const collectorNumber = resultRow.dataset.collectorNumber || "";
            const rarityDisplay = resultRow.dataset.rarityDisplay || "";
            const manaValue = resultRow.dataset.manaValue || "";
            const typeLine = resultRow.dataset.typeLine || "";

            if (groupImage && imageSrc) {
                groupImage.src = imageSrc;
                groupImage.alt = displayCardName || "Added card";
                groupImage.dataset.zoomSrc = imageSrc;
                groupImage.dataset.zoomAlt = displayCardName || "Added card";
            }

            if (groupMeta) {
                groupMeta.textContent = [
                    "Added: " + (setCode || "?"),
                    releaseYear || "",
                    collectorNumber ? "#" + collectorNumber : "",
                    rarityDisplay || "",
                    manaValue ? "MV " + manaValue : "",
                    typeLine || ""
                ].filter(Boolean).join(" • ");
            }

            if (addedPill) {
                addedPill.classList.remove("hidden");
            }

            collapseSearchResultGroup(groupElement);
        }

        function createSearchResultGroup(group) {
            const representativeCard = group.representative_card || group.cards[0] || {};
            const groupElement = document.createElement("div");
            groupElement.className = "custom-draft-card-search-group";

            const startsExpanded = false;

            groupElement.classList.toggle("custom-draft-card-search-group-expanded", startsExpanded);

            const headerButton = document.createElement("div");
            headerButton.className = "custom-draft-card-search-group-header";
            headerButton.setAttribute("role", "button");
            headerButton.setAttribute("tabindex", "0");
            headerButton.setAttribute("aria-expanded", startsExpanded ? "true" : "false");

            const imageWrap = document.createElement("div");
            imageWrap.className = "custom-draft-card-search-group-image-wrap";

            const image = document.createElement("img");
            image.className = "custom-draft-card-search-group-image custom-draft-card-zoomable";
            image.src = representativeCard.image_src || "";
            image.alt = group.card_name || "Card";
            image.setAttribute("role", "button");
            image.setAttribute("tabindex", "0");
            image.dataset.zoomSrc = representativeCard.image_src || "";
            image.dataset.zoomAlt = group.card_name || "Card";

            imageWrap.appendChild(image);

            const main = document.createElement("div");
            main.className = "custom-draft-card-search-group-main";

            const titleRow = document.createElement("div");
            titleRow.className = "custom-draft-card-search-group-title-row";

            const title = document.createElement("div");
            title.className = "custom-draft-card-search-group-title";
            title.textContent = group.card_name || "Unknown Card";

            const countBadge = document.createElement("span");
            countBadge.className = "custom-draft-card-search-group-count";
            countBadge.textContent = group.cards.length + " printing(s)";

            titleRow.appendChild(title);
            titleRow.appendChild(countBadge);

            const meta = document.createElement("div");
            meta.className = "custom-draft-card-search-group-meta";
            meta.textContent = [
                representativeCard.set_code ? "Newest: " + representativeCard.set_code : "",
                representativeCard.release_year || "",
                representativeCard.rarity || "",
                representativeCard.type_line || "",
                (!activeChangePrintingState && group.already_in_set_count > 0) ? group.already_in_set_count + " already added" : ""
            ].filter(Boolean).join(" • ");

            main.appendChild(titleRow);
            main.appendChild(meta);

            const addedPill = document.createElement("div");
            addedPill.className = "custom-draft-card-search-group-added-pill";
            addedPill.classList.toggle("hidden", Boolean(activeChangePrintingState) || !(group.already_in_set_count > 0));
            addedPill.textContent = group.already_in_set_count > 1
                ? group.already_in_set_count + " Added"
                : "Added";

            const expandIcon = document.createElement("div");
            expandIcon.className = "custom-draft-card-search-group-expand-icon";
            expandIcon.textContent = startsExpanded ? "−" : "+";

            headerButton.appendChild(imageWrap);
            headerButton.appendChild(main);
            headerButton.appendChild(addedPill);
            headerButton.appendChild(expandIcon);

            const printingsWrap = document.createElement("div");
            printingsWrap.className = "custom-draft-card-search-group-printings";

            group.cards.forEach(function (card) {
                printingsWrap.appendChild(createSearchResultPrintingRow(card));
            });

            function toggleGroupExpanded(event) {
                if (event && event.target && event.target.closest(".custom-draft-card-zoomable")) {
                    return;
                }

                const isExpanded = groupElement.classList.contains("custom-draft-card-search-group-expanded");
                const nextExpanded = !isExpanded;

                groupElement.classList.toggle("custom-draft-card-search-group-expanded", nextExpanded);
                headerButton.setAttribute("aria-expanded", nextExpanded ? "true" : "false");
                expandIcon.textContent = nextExpanded ? "−" : "+";
            }

            headerButton.addEventListener("click", toggleGroupExpanded);

            headerButton.addEventListener("keydown", function (event) {
                if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    toggleGroupExpanded(event);
                }
            });

            groupElement.appendChild(headerButton);
            groupElement.appendChild(printingsWrap);

            return groupElement;
        }

        function renderSearchResults(cards) {
            if (!searchResults) {
                return;
            }

            searchResults.innerHTML = "";

            if (!cards || !cards.length) {
                const emptyRow = document.createElement("div");
                emptyRow.className = "custom-draft-card-search-empty";
                emptyRow.textContent = "No matching cards found.";
                searchResults.appendChild(emptyRow);
                searchResults.classList.remove("hidden");
                return;
            }

            const groups = getGroupedSearchCards(cards);

            groups.forEach(function (group) {
                searchResults.appendChild(createSearchResultGroup(group));
            });

            refreshSearchResultAddedStates();

            bindModalZoomableImages();

            if (typeof config.bindZoomableImages === "function") {
                config.bindZoomableImages();
            }

            searchResults.classList.remove("hidden");
            filterSearchResults();
        }

        function filterSearchResults() {
            if (!searchResults) {
                return;
            }

            const selectedRarities = getSelectedMultiSelectValues(searchRarityFilter);
            const selectedColors = getSelectedMultiSelectValues(searchColorFilter);
            const manaOperator = searchManaOperatorFilter ? (searchManaOperatorFilter.value || "").trim() : "";
            const manaValue = searchManaValueFilter ? (searchManaValueFilter.value || "").trim() : "";
            const typeValue = searchTypeFilter ? (searchTypeFilter.value || "").trim().toLowerCase() : "";
            const digitalValue = searchDigitalFilter ? (searchDigitalFilter.value || "").trim().toLowerCase() : "";

            const groups = Array.from(searchResults.querySelectorAll(".custom-draft-card-search-group"));

            groups.forEach(function (groupElement) {
                const rows = Array.from(groupElement.querySelectorAll(".custom-draft-card-search-row"));
                let visibleCount = 0;

                rows.forEach(function (row) {
                    const rowRarity = row.dataset.rarity || "";
                    const rowTypeLine = row.dataset.typeLine || "";

                    const rarityMatches = rowMatchesRarityFilter(rowRarity, selectedRarities);
                    const colorMatches = rowMatchesColorFilter(row, selectedColors);
                    const manaMatches = rowMatchesManaFilter(row, manaOperator, manaValue);
                    const typeMatches = !typeValue || rowTypeLine.indexOf(typeValue) !== -1;
                    const digitalMatches = rowMatchesDigitalFilter(row, digitalValue);

                    const isVisible = rarityMatches && colorMatches && manaMatches && typeMatches && digitalMatches;

                    row.classList.toggle("hidden", !isVisible);

                    if (isVisible) {
                        visibleCount += 1;
                    }
                });

                groupElement.classList.toggle("hidden", visibleCount === 0);

                const countBadge = groupElement.querySelector(".custom-draft-card-search-group-count");
                if (countBadge) {
                    const totalCount = rows.length;
                    countBadge.textContent = visibleCount === totalCount
                        ? totalCount + " printing(s)"
                        : visibleCount + " of " + totalCount + " printing(s)";
                }
            });
        }

        async function runCardSearch(page) {
            if (!searchButton) {
                return;
            }

            if (!config.searchUrl) {
                setSearchStatus("Card search URL was not configured.", true);
                return;
            }

            if (!getAdvancedSearchHasAnyValue()) {
                setSearchStatus("Enter a text search or choose at least one search option.", true);
                clearSearchResults();
                hidePagination();
                return;
            }

            const requestedPage = page || 1;

            searchButton.disabled = true;
            searchButton.classList.add("action-button-loading");
            searchButton.textContent = "Searching...";
            setSearchStatus(getSearchLoadingPhrase(), false);
            setPaginationLoadingState(requestedPage);

            try {
                const response = await fetch(buildAdvancedCardSearchUrl(requestedPage), {
                    method: "GET",
                    headers: {
                        "Accept": "application/json"
                    }
                });

                const payload = await response.json();

                if (!response.ok || !payload.ok) {
                    throw new Error(payload.message || "Card search failed.");
                }

                currentSearchPage = payload.page || 1;
                currentSearchTotalPages = payload.total_pages || 1;

                const totalCount = payload.total_count || 0;
                const pageSize = payload.page_size || 50;

                const modeResults = getSearchResultsForActiveMode(payload.results || []);
                renderSearchResults(modeResults);

                if (activeChangePrintingState) {
                    setSearchStatus("Loaded " + modeResults.length + " printing(s) for " + activeChangePrintingState.cardName + ".", false);
                } else {
                    const start = (currentSearchPage - 1) * pageSize + 1;
                    const end = Math.min(currentSearchPage * pageSize, totalCount);
                    setSearchStatus(
                        "Showing " + start + "–" + end + " of " + totalCount + " result(s).",
                        false
                    );
                }

                updatePaginationBar(currentSearchPage, currentSearchTotalPages);
            } catch (error) {
                console.error(error);
                clearSearchResults();
                hidePagination();
                setSearchStatus(error.message || "Card search failed.", true);
            } finally {
                searchButton.disabled = false;
                searchButton.classList.remove("action-button-loading");
                searchButton.textContent = "Search";
            }
        }

        function buildBulkImportFormDataOrShowError() {
            const importTextValue = bulkImportText ? (bulkImportText.value || "").trim() : "";
            const hasFile = bulkImportFile && bulkImportFile.files && bulkImportFile.files.length > 0;

            if (!importTextValue && !hasFile) {
                setSearchStatus("Paste a list or upload a list file.", true);
                clearSearchResults();
                return null;
            }

            const formData = new FormData();

            if (importTextValue) {
                formData.append("import_text", importTextValue);
            }

            if (hasFile) {
                formData.append("import_file", bulkImportFile.files[0]);
            }

            return formData;
        }

        async function runBulkImportSearch() {
            if (!bulkImportButton || !config.bulkImportUrl) {
                return;
            }

            const formData = buildBulkImportFormDataOrShowError();

            if (!formData) {
                return;
            }

            bulkImportButton.disabled = true;
            bulkImportButton.classList.add("action-button-loading");
            bulkImportButton.textContent = "Loading List...";
            setSearchStatus("", false);

            try {
                const response = await fetch(config.bulkImportUrl, {
                    method: "POST",
                    headers: {
                        "Accept": "application/json"
                    },
                    body: formData
                });

                const payload = await response.json();

                if (!response.ok || !payload.ok) {
                    throw new Error(payload.message || "Bulk import search failed.");
                }

                const modeResults = getSearchResultsForActiveMode(payload.results || []);
                renderSearchResults(modeResults);

                let statusText = "Loaded " + modeResults.length + " result(s) from " + (payload.parsed_count || 0) + " parsed card line(s).";

                if (payload.unmatched_count) {
                    statusText += " " + payload.unmatched_count + " card name(s) were not matched.";
                }

                setSearchStatus(statusText, false);
            } catch (error) {
                console.error(error);
                clearSearchResults();
                setSearchStatus(error.message || "Bulk import search failed.", true);
            } finally {
                bulkImportButton.disabled = false;
                bulkImportButton.classList.remove("action-button-loading");
                bulkImportButton.textContent = "Load List into Search Results";
            }
        }

        async function runBulkImportAddMostRecent() {
            if (!bulkImportAddMostRecentButton || !config.bulkImportAddMostRecentUrl) {
                return;
            }

            const formData = buildBulkImportFormDataOrShowError();

            if (!formData) {
                return;
            }

            const confirmed = await confirmAction({
                title: "Add Most Recent Printings",
                message: config.bulkImportAddMostRecentConfirmMessage || "This will add the most recent matching printing for each parsed card name directly.",
                confirmText: "Add Cards",
                cancelText: "Cancel"
            });

            if (!confirmed) {
                return;
            }

            bulkImportAddMostRecentButton.disabled = true;
            bulkImportAddMostRecentButton.classList.add("action-button-loading");
            const bulkImportAddMostRecentDefaultText = config.bulkImportAddMostRecentText || "Add Most Recent to Set";
            bulkImportAddMostRecentButton.textContent = "Adding...";
            setSearchStatus("", false);

            try {
                const response = await fetch(config.bulkImportAddMostRecentUrl, {
                    method: "POST",
                    headers: {
                        "Accept": "application/json"
                    },
                    body: formData
                });

                const payload = await response.json();

                if (!response.ok || !payload.ok) {
                    throw new Error(payload.message || "Direct import failed.");
                }

                const statusText = payload.message || (
                    "Imported " + (payload.added_count || 0) + " card(s)."
                );

                setSearchStatus(statusText + " Reloading card list...", false);

                window.setTimeout(function () {
                    window.location.reload();
                }, 700);
            } catch (error) {
                console.error(error);
                setSearchStatus(error.message || "Direct import failed.", true);
            } finally {
                bulkImportAddMostRecentButton.disabled = false;
                bulkImportAddMostRecentButton.classList.remove("action-button-loading");
                bulkImportAddMostRecentButton.textContent = bulkImportAddMostRecentDefaultText;
            }
        }

        async function addCard(cardUuid, button) {
            if (!cardUuid || !button || !config.addUrl) {
                return;
            }

            button.disabled = true;
            button.classList.add("action-button-loading");
            button.textContent = "Adding...";

            try {
                const response = await fetch(config.addUrl, {
                    method: "POST",
                    headers: {
                        "Accept": "application/json",
                        "Content-Type": "application/json"
                    },
                    body: JSON.stringify({
                        card_uuid: cardUuid
                    })
                });

                const payload = await response.json();

                if (!response.ok || !payload.ok) {
                    throw new Error(payload.message || "Failed to add card.");
                }

                button.classList.remove("action-button-loading");
                button.textContent = payload.inserted ? "Added" : "Already Added";
                button.disabled = true;

                const resultRow = button.closest(".custom-draft-card-search-row");
                if (resultRow) {
                    const resultCardUuid = String(resultRow.dataset.cardUuid || cardUuid || "").trim();
                    const resultPrintingKey = String(resultRow.dataset.printingKey || "").trim();

                    if (resultCardUuid) {
                        clientAddedCardUuidLookup.add(resultCardUuid);
                    }

                    if (resultPrintingKey) {
                        clientAddedPrintingKeyLookup.add(resultPrintingKey);
                    }

                    resultRow.classList.add("custom-draft-card-search-row-disabled");
                    updateSearchResultGroupFromAddedRow(resultRow);
                }

                if (typeof config.onAddSuccess === "function") {
                    config.onAddSuccess(payload, {
                        cardUuid: cardUuid,
                        button: button,
                        resultRow: resultRow,
                        setStatus: setSearchStatus
                    });
                }

                setSearchStatus(payload.message || "Card added. Search results remain open.", false);
            } catch (error) {
                console.error(error);
                button.disabled = false;
                button.classList.remove("action-button-loading");
                button.textContent = "Add";
                showMessage(error.message || "Failed to add card.", true);
            }
        }

        async function changeCardPrinting(cardUuid, button) {
            if (!activeChangePrintingState || !cardUuid || !button) {
                return;
            }

            button.disabled = true;
            button.classList.add("action-button-loading");
            button.textContent = "Switching...";

            try {
                const response = await fetch(activeChangePrintingState.updateUrl, {
                    method: "POST",
                    headers: {
                        "Accept": "application/json",
                        "Content-Type": "application/json"
                    },
                    body: JSON.stringify({
                        card_uuid: cardUuid
                    })
                });

                const payload = await response.json();

                if (!response.ok || !payload.ok) {
                    throw new Error(payload.message || "Failed to change printing.");
                }

                const previousCardUuid = activeChangePrintingState.currentCardUuid || "";

                if (previousCardUuid && previousCardUuid !== cardUuid) {
                    clientAddedCardUuidLookup.delete(previousCardUuid);
                }

                button.classList.remove("action-button-loading");
                button.textContent = "Switched";

                const resultRow = button.closest(".custom-draft-card-search-row");

                if (typeof config.onChangePrintingSuccess === "function") {
                    config.onChangePrintingSuccess(payload, {
                        cardUuid: cardUuid,
                        button: button,
                        resultRow: resultRow,
                        activeChangePrintingState: activeChangePrintingState,
                        setStatus: setSearchStatus,
                        close: close,
                        updateSearchResultGroupFromAddedRow: updateSearchResultGroupFromAddedRow
                    });
                } else {
                    setSearchStatus(payload.message || "Printing updated.", false);
                    close();
                }
            } catch (error) {
                console.error(error);
                button.disabled = false;
                button.classList.remove("action-button-loading");
                button.textContent = "Switch";
                showMessage(error.message || "Failed to change printing.", true);
            }
        }

        function setBulkImportPanelOpen(isOpen) {
            if (!bulkImportPanel || !bulkImportToggleButton) {
                return;
            }

            bulkImportPanel.classList.toggle("custom-draft-bulk-import-panel-open", Boolean(isOpen));
            bulkImportPanel.setAttribute("aria-hidden", isOpen ? "false" : "true");
            bulkImportToggleButton.classList.toggle("custom-draft-bulk-import-toggle-active", Boolean(isOpen));
            bulkImportToggleButton.setAttribute("aria-expanded", isOpen ? "true" : "false");
        }

        function resetModalResultsForModeChange() {
            clearSearchResults();
            hidePagination();
            setSearchStatus("", false);

            if (searchButton) {
                searchButton.disabled = false;
                searchButton.classList.remove("action-button-loading");
                searchButton.textContent = "Search";
            }

            if (bulkImportButton) {
                bulkImportButton.disabled = false;
                bulkImportButton.classList.remove("action-button-loading");
                bulkImportButton.textContent = "Load List into Search Results";
            }

            if (bulkImportAddMostRecentButton) {
                bulkImportAddMostRecentButton.disabled = false;
                bulkImportAddMostRecentButton.classList.remove("action-button-loading");
                bulkImportAddMostRecentButton.textContent = config.bulkImportAddMostRecentText || "Add Most Recent to Set";
            }
        }

        function open() {
            if (!searchOverlay) {
                return;
            }

            searchOverlay.classList.remove("hidden");
            searchOverlay.setAttribute("aria-hidden", "false");
            document.body.classList.add("custom-draft-search-maximized-active");

            if (searchInput) {
                window.setTimeout(function () {
                    searchInput.focus();
                }, 50);
            }
        }

        function close() {
            if (!searchOverlay) {
                return;
            }

            setBulkImportPanelOpen(false);
            searchOverlay.classList.add("hidden");
            searchOverlay.setAttribute("aria-hidden", "true");
            document.body.classList.remove("custom-draft-search-maximized-active");

            activeChangePrintingState = null;

            setModalTitle(
                "Add Cards",
                "Search by card name, set, color identity, rarity, mana value, year, spell type, EDHREC rank, or saltiness."
            );
        }

        function openAddCards() {
            activeChangePrintingState = null;

            /*
             * Important:
             * Change Printing and Add Cards share the same DOM.
             * If the modal previously rendered Change Printing rows, those rows have
             * Switch buttons wired to changeCardPrinting(). They must not remain visible
             * when reopening the modal in Add Cards mode.
             */
            resetModalResultsForModeChange();

            /*
             * These caches are only a client-side convenience. The authoritative
             * "already added" state comes from getExistingCardLookup(), which reads
             * the current page/deck/set state.
             *
             * Clearing them here prevents an old printing from staying marked as
             * Added after that card was changed to a different printing.
             */
            clientAddedCardUuidLookup.clear();
            clientAddedPrintingKeyLookup.clear();

            setModalTitle(
                "Add Cards",
                "Search by card name, set, color identity, rarity, mana value, year, spell type, EDHREC rank, or saltiness."
            );

            open();
        }

        async function openChangePrinting(changeState) {
            activeChangePrintingState = changeState || null;

            if (!activeChangePrintingState) {
                return;
            }

            setModalTitle(
                "Change Printing",
                "Choose a replacement printing for " + activeChangePrintingState.cardName + ". The special slot category will be preserved."
            );

            clearSearchResults();
            setSearchStatus("", false);

            if (searchInput) {
                searchInput.value = activeChangePrintingState.cardName || "";
            }

            clearMultiSelectValues(searchRarityFilter);
            clearMultiSelectValues(searchColorFilter);

            if (searchManaOperatorFilter) {
                searchManaOperatorFilter.value = "";
            }

            if (searchManaValueFilter) {
                searchManaValueFilter.value = "";
            }

            if (searchTypeFilter) {
                searchTypeFilter.value = "";
            }

            if (searchSetCodeFilter) {
                searchSetCodeFilter.value = "";
            }

            if (searchYearStartFilter) {
                searchYearStartFilter.value = "";
            }

            if (searchYearEndFilter) {
                searchYearEndFilter.value = "";
            }

            if (searchSortSelect) {
                searchSortSelect.value = "year_newest";
            }

            open();
            await runCardSearch(1);
        }

        function clearSearchFilters() {
            if (searchInput) {
                searchInput.value = "";
            }

            clearMultiSelectValues(searchRarityFilter);
            clearMultiSelectValues(searchColorFilter);

            if (searchManaOperatorFilter) {
                searchManaOperatorFilter.value = "";
            }

            if (searchManaValueFilter) {
                searchManaValueFilter.value = "";
            }

            if (searchTypeFilter) {
                searchTypeFilter.value = "";
            }

            if (searchSetCodeFilter) {
                searchSetCodeFilter.value = "";
            }

            if (searchYearStartFilter) {
                searchYearStartFilter.value = "";
            }

            if (searchYearEndFilter) {
                searchYearEndFilter.value = "";
            }

            if (searchDigitalFilter) {
                searchDigitalFilter.value = "exclude";
            }

            if (searchSortSelect) {
                searchSortSelect.value = "name_asc";
            }

            if (bulkImportText) {
                bulkImportText.value = "";
            }

            if (bulkImportFile) {
                bulkImportFile.value = "";
            }

            clearSearchResults();
            hidePagination();
            setSearchStatus("", false);
        }

        function bindEvents() {
            if (bulkImportToggleButton) {
                bulkImportToggleButton.addEventListener("click", function () {
                    const isOpen = bulkImportPanel
                        ? bulkImportPanel.classList.contains("custom-draft-bulk-import-panel-open")
                        : false;

                    setBulkImportPanelOpen(!isOpen);
                });
            }

            if (closeButton) {
                closeButton.addEventListener("click", close);
            }

            if (searchButton) {
                searchButton.addEventListener("click", function () {
                    runCardSearch(1);
                });
            }

            if (bulkImportButton) {
                bulkImportButton.addEventListener("click", runBulkImportSearch);
            }

            if (bulkImportAddMostRecentButton) {
                bulkImportAddMostRecentButton.addEventListener("click", runBulkImportAddMostRecent);
            }

            if (searchInput) {
                searchInput.addEventListener("keydown", function (event) {
                    if (event.key === "Enter") {
                        event.preventDefault();
                        runCardSearch(1);
                    }
                });
            }

            [
                searchManaOperatorFilter,
                searchManaValueFilter,
                searchTypeFilter,
                searchSetCodeFilter,
                searchSortSelect,
                searchYearStartFilter,
                searchYearEndFilter,
                searchDigitalFilter,
                searchPageSizeSelect
            ].forEach(function (searchFilterElement) {
                if (!searchFilterElement) {
                    return;
                }

                searchFilterElement.addEventListener("keydown", function (event) {
                    if (event.key === "Enter") {
                        event.preventDefault();
                        runCardSearch(1);
                    }
                });
            });

            if (searchPageSizeSelect) {
                searchPageSizeSelect.addEventListener("change", function () {
                    if (typeof config.onPageSizeChange === "function") {
                        config.onPageSizeChange(searchPageSizeSelect.value || "500");
                    }

                    if (searchResults && !searchResults.classList.contains("hidden") && getAdvancedSearchHasAnyValue()) {
                        runCardSearch(1);
                    }
                });
            }

            if (searchPrevButton) {
                searchPrevButton.addEventListener("click", function () {
                    if (currentSearchPage > 1) {
                        runCardSearch(currentSearchPage - 1);
                    }
                });
            }

            if (searchNextButton) {
                searchNextButton.addEventListener("click", function () {
                    if (currentSearchPage < currentSearchTotalPages) {
                        runCardSearch(currentSearchPage + 1);
                    }
                });
            }

            if (searchPageInput) {
                searchPageInput.addEventListener("change", function () {
                    const requestedPage = parseInt(searchPageInput.value, 10);

                    if (!Number.isFinite(requestedPage)) {
                        return;
                    }

                    const clampedPage = Math.max(1, Math.min(requestedPage, currentSearchTotalPages));
                    runCardSearch(clampedPage);
                });

                searchPageInput.addEventListener("keydown", function (event) {
                    if (event.key === "Enter") {
                        event.preventDefault();
                        searchPageInput.blur();
                    }
                });
            }

            bindMultiSelect(searchRarityFilter, filterSearchResults);
            bindMultiSelect(searchColorFilter, filterSearchResults);

            [
                searchManaOperatorFilter,
                searchManaValueFilter,
                searchTypeFilter,
                searchDigitalFilter
            ].forEach(function (filterElement) {
                if (filterElement) {
                    filterElement.addEventListener("input", filterSearchResults);
                    filterElement.addEventListener("change", filterSearchResults);
                }
            });

            if (searchClearFiltersButton) {
                searchClearFiltersButton.addEventListener("click", clearSearchFilters);
            }

            document.addEventListener("click", function (event) {
                if (!event.target.closest(".custom-draft-multi-select")) {
                    document.querySelectorAll(".custom-draft-multi-select").forEach(function (containerElement) {
                        closeMultiSelect(containerElement);
                    });
                }
            });

            document.addEventListener("keydown", function (event) {
                if (event.key === "Escape") {
                    closeModalImageZoom();
                }
            });
        }

        bindEvents();

        return {
            open: open,
            close: close,
            openAddCards: openAddCards,
            openChangePrinting: openChangePrinting,
            runSearch: runCardSearch,
            clearSearchFilters: clearSearchFilters,
            isOpen: function () {
                return Boolean(searchOverlay && !searchOverlay.classList.contains("hidden"));
            }
        };
    }

    window.iMomirCardSearchModal = {
        init: initCardSearchModal
    };
})();