/* ==========================================
   iMomir Client Refresh Tracker
   ------------------------------------------
   Lightweight page-local refresh/change helper.
   No database state. No server calls.
   
   Usage:
   - iMomirRefresh.check("object-key", signature, refreshCallback)
   - iMomirRefresh.changed("object-key", signature, refreshCallback)
   - iMomirRefresh.refreshCheck(0, "object-key", signature, refreshCallback)
   - iMomirRefresh.refreshCheck(1, "object-key", signature, refreshCallback)
   ========================================== */
(function () {
    if (window.iMomirRefresh) {
        return;
    }

    const stateByKey = new Map();

    function normalizeKey(objectKey) {
        return String(objectKey || "").trim();
    }

    function normalizeSignature(signature) {
        if (signature === undefined || signature === null) {
            return "";
        }

        if (typeof signature === "string") {
            return signature;
        }

        try {
            return JSON.stringify(signature);
        } catch (error) {
            return String(signature);
        }
    }

    function runRefreshCallback(refreshCallback, context) {
        if (typeof refreshCallback !== "function") {
            return;
        }

        refreshCallback(context);
    }

    function check(objectKey, signature, refreshCallback) {
        const key = normalizeKey(objectKey);

        if (!key) {
            return {
                key: "",
                changed: false,
                currentSignature: "",
                previousSignature: "",
                initialized: false
            };
        }

        const currentSignature = normalizeSignature(signature);
        const previousEntry = stateByKey.get(key);
        const previousSignature = previousEntry ? previousEntry.signature : "";
        const initialized = Boolean(previousEntry);
        const changed = initialized && previousSignature !== currentSignature;

        const context = {
            key: key,
            changed: changed,
            currentSignature: currentSignature,
            previousSignature: previousSignature,
            initialized: initialized
        };

        if (!initialized) {
            stateByKey.set(key, {
                signature: currentSignature,
                updatedAt: Date.now()
            });

            return context;
        }

        if (changed) {
            stateByKey.set(key, {
                signature: currentSignature,
                updatedAt: Date.now()
            });

            runRefreshCallback(refreshCallback, context);
        }

        return context;
    }

    function changed(objectKey, signature, refreshCallback) {
        const key = normalizeKey(objectKey);

        if (!key) {
            return {
                key: "",
                changed: false,
                currentSignature: "",
                previousSignature: "",
                initialized: false
            };
        }

        const currentSignature = normalizeSignature(signature);
        const previousEntry = stateByKey.get(key);
        const previousSignature = previousEntry ? previousEntry.signature : "";
        const initialized = Boolean(previousEntry);

        stateByKey.set(key, {
            signature: currentSignature,
            updatedAt: Date.now()
        });

        const context = {
            key: key,
            changed: true,
            currentSignature: currentSignature,
            previousSignature: previousSignature,
            initialized: initialized
        };

        runRefreshCallback(refreshCallback, context);

        return context;
    }

    function refreshCheck(mode, objectKey, signature, refreshCallback) {
        const parsedMode = Number(mode);

        if (parsedMode === 1) {
            return changed(objectKey, signature, refreshCallback);
        }

        return check(objectKey, signature, refreshCallback);
    }

    function forget(objectKey) {
        const key = normalizeKey(objectKey);

        if (!key) {
            return;
        }

        stateByKey.delete(key);
    }

    function clear() {
        stateByKey.clear();
    }

    window.iMomirRefresh = {
        check: check,
        changed: changed,
        refreshCheck: refreshCheck,
        forget: forget,
        clear: clear
    };

    window.refreshCheck = refreshCheck;
})();

document.addEventListener("DOMContentLoaded", function () {
    initializeManaKeypad();
    initializeSetsPage();
    initializeRefreshCards();
    initializeSettingsConsole();
    initializeConfigPanels();
    initializeGameModeCards();
    initializeConfigShortcutNavigation();
    initializeAppNavigationMenus();
    initializeResultCardZoom();
    initializeMomirSelectResultLinks();
    initializeAlternateBleedReprocessing();
    initializeChaosDraftPage();
});

function initializeManaKeypad() {
    const manaInput = document.getElementById("manaValue");
    const manaForm = document.getElementById("manaForm");
    const keypadButtons = document.querySelectorAll(".keypad-btn[data-key]");
    const clearButton = document.getElementById("clearBtn");
    const backspaceButton = document.getElementById("backspaceBtn");

    if (!manaInput || !manaForm) {
        return;
    }

    function appendDigit(digit) {
        if (manaInput.value.length >= 2) {
            return;
        }

        if (!/^\d$/.test(digit)) {
            return;
        }

        manaInput.value += digit;
    }

    function clearValue() {
        manaInput.value = "";
    }

    function backspaceValue() {
        manaInput.value = manaInput.value.slice(0, -1);
    }

    keypadButtons.forEach(function (button) {
        button.addEventListener("click", function () {
            const digit = button.getAttribute("data-key");
            appendDigit(digit);
        });
    });

    if (clearButton) {
        clearButton.addEventListener("click", function () {
            clearValue();
        });
    }

    if (backspaceButton) {
        backspaceButton.addEventListener("click", function () {
            backspaceValue();
        });
    }

    manaForm.addEventListener("submit", function (event) {
        const value = manaInput.value.trim();

        if (value === "") {
            event.preventDefault();
            alert("Please enter a mana value.");
        }
    });
}

function initializeSetsPage() {
    const allSetsCheckbox = document.getElementById("allSetsEnabled");
    const setsListWrapper = document.getElementById("setsListWrapper");
    const setSearchInput = document.getElementById("setSearchInput");
    const setTypeFilter = document.getElementById("setTypeFilter");
    const setYearRange = document.getElementById("setYearRange");
    const setYearStart = document.getElementById("setYearStart");
    const setYearEnd = document.getElementById("setYearEnd");
    const setYearStartLabel = document.getElementById("setYearStartLabel");
    const setYearEndLabel = document.getElementById("setYearEndLabel");
    const deselectAllSetsButton = document.getElementById("deselectAllSetsButton");
    const selectVisibleSetsButton = document.getElementById("selectVisibleSetsButton");
    const setCheckboxes = document.querySelectorAll(".set-checkbox");
    const setRows = document.querySelectorAll(".set-row");

    if (!allSetsCheckbox || !setsListWrapper) {
        return;
    }

    function syncAllSetsState() {
        const disableIndividualSets = allSetsCheckbox.checked;

        if (disableIndividualSets) {
            setsListWrapper.classList.add("sets-disabled");
        } else {
            setsListWrapper.classList.remove("sets-disabled");
        }

        setCheckboxes.forEach(function (checkbox) {
            checkbox.disabled = disableIndividualSets;
        });
    }

    function syncYearLabels() {
        if (!setYearStart || !setYearEnd) {
            return;
        }

        let startValue = Number(setYearStart.value);
        let endValue = Number(setYearEnd.value);

        if (startValue > endValue) {
            if (document.activeElement === setYearStart) {
                endValue = startValue;
                setYearEnd.value = String(endValue);
            } else {
                startValue = endValue;
                setYearStart.value = String(startValue);
            }
        }

        if (setYearStartLabel) {
            setYearStartLabel.textContent = String(startValue);
        }

        if (setYearEndLabel) {
            setYearEndLabel.textContent = String(endValue);
        }

        if (setYearRange) {
            const minYear = Number(setYearStart.min);
            const maxYear = Number(setYearStart.max);
            const span = Math.max(1, maxYear - minYear);

            const startPercent = ((startValue - minYear) / span) * 100;
            const endPercent = ((endValue - minYear) / span) * 100;

            setYearRange.style.setProperty("--range-start", `${startPercent}%`);
            setYearRange.style.setProperty("--range-end", `${endPercent}%`);
        }
    }

    function filterSetRows() {
        const searchValue = setSearchInput ? setSearchInput.value.trim().toLowerCase() : "";
        const typeValue = setTypeFilter ? setTypeFilter.value.trim().toLowerCase() : "";
        const startYear = setYearStart ? Number(setYearStart.value) : 1993;
        const endYear = setYearEnd ? Number(setYearEnd.value) : 9999;

        setRows.forEach(function (row) {
            const haystack = row.getAttribute("data-set-search") || "";
            const rowType = (row.getAttribute("data-set-type") || "").toLowerCase();
            const rowYearText = row.getAttribute("data-set-year") || "";
            const rowYear = Number(rowYearText);

            const matchesSearch = searchValue === "" || haystack.includes(searchValue);
            const matchesType = typeValue === "" || rowType === typeValue;
            const matchesYear =
                rowYearText === "" ||
                (Number.isFinite(rowYear) && rowYear >= startYear && rowYear <= endYear);

            if (matchesSearch && matchesType && matchesYear) {
                row.classList.remove("hidden");
            } else {
                row.classList.add("hidden");
            }
        });
    }

    function deselectAllSets() {
        if (allSetsCheckbox.checked) {
            return;
        }

        setCheckboxes.forEach(function (checkbox) {
            checkbox.checked = false;
        });
    }

    function selectAllVisibleSets() {
        if (allSetsCheckbox.checked) {
            return;
        }

        setRows.forEach(function (row) {
            if (row.classList.contains("hidden")) {
                return;
            }

            const checkbox = row.querySelector(".set-checkbox");
            if (checkbox && !checkbox.disabled) {
                checkbox.checked = true;
            }
        });
    }

    if (deselectAllSetsButton) {
        deselectAllSetsButton.addEventListener("click", function () {
            deselectAllSets();
        });
    }

    if (selectVisibleSetsButton) {
        selectVisibleSetsButton.addEventListener("click", function () {
            selectAllVisibleSets();
        });
    }

    allSetsCheckbox.addEventListener("change", syncAllSetsState);

    if (setSearchInput) {
        setSearchInput.addEventListener("input", filterSetRows);
    }

    if (setTypeFilter) {
        setTypeFilter.addEventListener("change", filterSetRows);
    }

    if (setYearStart) {
        setYearStart.addEventListener("input", function () {
            syncYearLabels();
            filterSetRows();
        });
    }

    if (setYearEnd) {
        setYearEnd.addEventListener("input", function () {
            syncYearLabels();
            filterSetRows();
        });
    }

    syncAllSetsState();
    syncYearLabels();
    filterSetRows();
}

function initializeRefreshCards() {
    const refreshButton = document.getElementById("refreshCardsButton");
    const forcedRefreshButton = document.getElementById("forcedRefreshCardsButton");
    const refreshSpinner = document.getElementById("refreshSpinner");
    const refreshStage = document.getElementById("refreshStage");
    const refreshMessage = document.getElementById("refreshMessage");
    const refreshError = document.getElementById("refreshError");
    const refreshDetailLines = document.getElementById("refreshDetailLines");
    const importCardsCount = document.getElementById("importCardsCount");
    const importSetsCount = document.getElementById("importSetsCount");
    const importLastRefresh = document.getElementById("importLastRefresh");
    const importSourceLastUpdated = document.getElementById("importSourceLastUpdated");

    const clearHistoryButton = document.getElementById("clearHistoryButton");
    const historyCount = document.getElementById("historyCount");

    if (!refreshButton) {
        return;
    }

    let refreshPollTimer = null;
    let lastRefreshFinishedAtPrompted = null;
    let refreshObservedRunning = Boolean(refreshButton.disabled);

    function setRefreshRunningUi(isRunning) {
        refreshButton.disabled = isRunning;

        if (forcedRefreshButton) {
            forcedRefreshButton.disabled = isRunning;
        }

        if (refreshSpinner) {
            refreshSpinner.classList.toggle("hidden", !isRunning);
        }
    }

    function applyRefreshStatus(status) {
        if (refreshStage) {
            refreshStage.textContent = status.stage || "Idle";
        }

        if (refreshMessage) {
            refreshMessage.textContent = status.message || "";
        }

        if (refreshError) {
            if (status.error) {
                refreshError.textContent = status.error;
                refreshError.classList.remove("hidden");
            } else {
                refreshError.textContent = "";
                refreshError.classList.add("hidden");
            }
        }

        if (refreshDetailLines) {
            const detailLines = Array.isArray(status.detail_lines) ? status.detail_lines : [];

            if (detailLines.length > 0) {
                const nextLogText = detailLines.join("\n");
                const logChanged = refreshDetailLines.textContent !== nextLogText;

                refreshDetailLines.textContent = nextLogText;
                refreshDetailLines.classList.remove("hidden");

                if (logChanged) {
                    refreshDetailLines.scrollTop = refreshDetailLines.scrollHeight;
                }
            } else {
                refreshDetailLines.textContent = "";
                refreshDetailLines.classList.add("hidden");
            }
        }

        if (importCardsCount && status.cards_imported !== undefined) {
            importCardsCount.textContent = String(status.cards_imported);
        }

        if (importSetsCount && status.sets_represented !== undefined) {
            importSetsCount.textContent = String(status.sets_represented);
        }

        if (importLastRefresh && status.finished_at) {
            importLastRefresh.textContent = status.finished_at;
        }

        if (importSourceLastUpdated && status.source_last_updated) {
            importSourceLastUpdated.textContent = status.source_last_updated;
        }

        if (status.is_running) {
            refreshObservedRunning = true;
        }

        setRefreshRunningUi(Boolean(status.is_running));
    }

    async function fetchRefreshStatus() {
        const response = await fetch("/refresh-cards/status", {
            method: "GET",
            headers: {
                "Accept": "application/json"
            }
        });

        if (!response.ok) {
            throw new Error("Failed to get refresh status.");
        }

        return await response.json();
    }

    async function pollRefreshStatus() {
        try {
            const status = await fetchRefreshStatus();
            applyRefreshStatus(status);

            if (!status.is_running && refreshPollTimer) {
                clearInterval(refreshPollTimer);
                refreshPollTimer = null;
            }

            if (
                refreshObservedRunning &&
                !status.is_running &&
                status.stage === "Complete" &&
                status.finished_at &&
                status.finished_at !== lastRefreshFinishedAtPrompted
            ) {
                lastRefreshFinishedAtPrompted = status.finished_at;
                refreshObservedRunning = false;

                window.alert(
                    "Card database download complete."
                );
            }
        } catch (error) {
            if (refreshError) {
                refreshError.textContent = error.message;
                refreshError.classList.remove("hidden");
            }

            setRefreshRunningUi(false);

            if (refreshPollTimer) {
                clearInterval(refreshPollTimer);
                refreshPollTimer = null;
            }
        }
    }

    async function startRefresh(forceDownload) {
        try {
            refreshObservedRunning = true;
            setRefreshRunningUi(true);

            if (refreshError) {
                refreshError.textContent = "";
                refreshError.classList.add("hidden");
            }

            if (refreshStage) {
                refreshStage.textContent = "Starting";
            }

            if (refreshMessage) {
                refreshMessage.textContent = forceDownload
                    ? "Starting forced refresh..."
                    : "Checking whether download is needed...";
            }

            const response = await fetch("/refresh-cards/start", {
                method: "POST",
                headers: {
                    "Accept": "application/json",
                    "Content-Type": "application/json"
                },
                body: JSON.stringify({
                    force_download: forceDownload
                })
            });

            const payload = await response.json();

            if (!response.ok || !payload.ok) {
                throw new Error(payload.message || "Failed to start refresh.");
            }

            await pollRefreshStatus();

            if (!refreshPollTimer) {
                refreshPollTimer = setInterval(pollRefreshStatus, 1000);
            }
        } catch (error) {
            setRefreshRunningUi(false);

            if (refreshError) {
                refreshError.textContent = error.message;
                refreshError.classList.remove("hidden");
            }
        }
    }

    refreshButton.addEventListener("click", async function () {
        await startRefresh(false);
    });

    if (forcedRefreshButton) {
        forcedRefreshButton.addEventListener("click", async function () {
            await startRefresh(true);
        });
    }

    if (clearHistoryButton) {
        clearHistoryButton.addEventListener("click", async function () {
            const confirmed = window.confirm(
                "Clear the recent card history?\n\nThis will allow previously shown cards to be selected again immediately."
            );

            if (!confirmed) {
                return;
            }

            try {
                clearHistoryButton.disabled = true;

                const response = await fetch("/history/clear", {
                    method: "POST",
                    headers: {
                        "Accept": "application/json"
                    }
                });

                const payload = await response.json();

                if (!response.ok || !payload.ok) {
                    throw new Error(payload.message || "Failed to clear history.");
                }

                if (historyCount) {
                    historyCount.textContent = "0";
                }
            } catch (error) {
                window.alert(error.message || "Failed to clear history.");
            } finally {
                clearHistoryButton.disabled = false;
            }
        });
    }

    pollRefreshStatus();
}

function initializeSettingsConsole() {
    const configForm = document.getElementById("configForm");

    if (!configForm) {
        return;
    }

    const screen = configForm.closest(".screen");

    if (!screen) {
        return;
    }

    if (screen.dataset.settingsConsoleInitialized === "1") {
        return;
    }

    screen.dataset.settingsConsoleInitialized = "1";
    screen.classList.add("settings-console-screen");

    const sectionMetadata = {
        reminders: {
            description: "Release checks, reminder status, and notification preferences."
        },
        card_database: {
            description: "Card database status, refresh controls, and source information."
        },
        draft_modes: {
            description: "Choose the Chaos Draft experience used when opening Play → Draft."
        },
        chaos_print_settings: {
            description: "Printing, PDF output, labels, exports, pack tracking, and draft output."
        },
        momir_modes: {
            description: "Choose the active Momir-style random card mode."
        },
        other_modes: {
            description: "Configure related modes that use the Momir play interface."
        },
        card_repeats: {
            description: "Control duplicate card draws and recent-card history."
        },
        primary_types: {
            description: "Choose the primary card types available to Momir draws."
        },
        supplemental_types: {
            description: "Enable supplemental and specialty card types."
        },
        other_filters: {
            description: "Fine-tune additional filters applied to Momir card selection."
        },
        momir_print_settings: {
            description: "Printer and output defaults for Momir and Other Modes."
        },
        exports: {
            description: "Manage generated files and automatic export cleanup."
        },
        backup: {
            description: "Create, download, import, and restore iMomir backups."
        },
        image_maintenance: {
            description: "Repair and reprocess generated alternate-image derivatives."
        },
        plugins: {
            description: "View installed plugins and add optional iMomir capabilities."
        },
        image_upscaling: {
            description: "Choose the active Upscaler and configure image upscaling behavior."
        },
        danger_zone: {
            description: "Diagnostics and destructive maintenance operations."
        }
    };

    const categoryDefinitions = [
        {
            key: "system",
            eyebrow: "APPLICATION",
            title: "System Settings",
            description: "Application-wide reminders, release information, and notification preferences.",
            iconClass: "fa-solid fa-sliders",
            sections: [
                "reminders"
            ],
            parent: "form"
        },
        {
            key: "chaos",
            eyebrow: "GAME MODE",
            title: "Chaos Draft Settings",
            description: "Configure the Draft play experience and all Chaos Draft-specific output.",
            iconClass: "fa-solid fa-shuffle",
            sections: [
                "draft_modes",
                "chaos_print_settings"
            ],
            parent: "form"
        },
        {
            key: "momir",
            eyebrow: "GAME MODE",
            title: "Momir Settings",
            description: "Configure Momir modes, card selection rules, repeat behavior, filters, and printing.",
            iconClass: "fa-solid fa-wand-magic-sparkles",
            sections: [
                "momir_modes",
                "other_modes",
                "card_repeats",
                "primary_types",
                "supplemental_types",
                "other_filters",
                "momir_print_settings"
            ],
            parent: "form"
        },
        {
            key: "database",
            eyebrow: "CARD DATA",
            title: "Database Settings",
            description: "Manage the local card database, refresh status, and card data source.",
            iconClass: "fa-solid fa-database",
            sections: [
                "card_database"
            ],
            parent: "form"
        },
        {
            key: "plugins",
            eyebrow: "EXTENSIONS",
            title: "Plugins",
            description: "Install and manage optional capabilities for iMomir.",
            iconClass: "fa-solid fa-puzzle-piece",
            sections: [
                "plugins"
            ],
            parent: "screen"
        },
        {
            key: "advanced",
            eyebrow: "ADMINISTRATION",
            title: "Advanced",
            description: "Export retention, backups, image tools, diagnostics, and destructive maintenance tools.",
            iconClass: "fa-solid fa-toolbox",
            sections: [
                "exports",
                "backup",
                "image_maintenance",
                "image_upscaling",
                "danger_zone"
            ],
            parent: "screen"
        }
    ];

    function decorateSection(panel, sectionName) {
        const metadata = sectionMetadata[sectionName] || {};
        const header = panel.querySelector(".collapsible-header");
        const body = panel.querySelector(".collapsible-body");

        if (!header || !body) {
            return;
        }

        panel.classList.add("settings-console-panel");

        if (!header.querySelector(".settings-section-header-copy")) {
            const currentTitle = header.firstElementChild;

            if (currentTitle) {
                const headerCopy = document.createElement("span");
                headerCopy.className = "settings-section-header-copy";

                header.insertBefore(headerCopy, currentTitle);
                headerCopy.appendChild(currentTitle);

                currentTitle.classList.add("settings-section-title");

                if (metadata.description) {
                    const description = document.createElement("span");
                    description.className = "settings-section-description";
                    description.textContent = metadata.description;

                    headerCopy.appendChild(description);
                }
            }
        }

        if (
            metadata.showSave !== false
            && !body.querySelector(".settings-section-save-row")
        ) {
            const saveRow = document.createElement("div");
            saveRow.className = "settings-section-save-row";

            const saveButton = document.createElement("button");
            saveButton.type = "submit";
            saveButton.setAttribute("form", "configForm");
            saveButton.name = "return_section";
            saveButton.value = sectionName;
            saveButton.className = "action-button settings-section-save-button";

            const saveIcon = document.createElement("i");
            saveIcon.className = "fa-solid fa-floppy-disk";

            const saveText = document.createElement("span");
            saveText.textContent = "Save Settings";

            saveButton.appendChild(saveIcon);
            saveButton.appendChild(saveText);

            saveRow.appendChild(saveButton);

            body.appendChild(saveRow);
        }
    }

    function createCategory(definition, panels) {
        const category = document.createElement("section");
        category.className =
            "settings-category settings-category-" + definition.key;

        const categoryHeader = document.createElement("div");
        categoryHeader.className = "settings-category-header";

        const icon = document.createElement("div");
        icon.className = "settings-category-icon";

        const iconElement = document.createElement("i");
        iconElement.className = definition.iconClass;

        icon.appendChild(iconElement);

        const copy = document.createElement("div");
        copy.className = "settings-category-copy";

        const eyebrow = document.createElement("div");
        eyebrow.className = "settings-category-eyebrow";
        eyebrow.textContent = definition.eyebrow;

        const title = document.createElement("h2");
        title.className = "settings-category-title";
        title.textContent = definition.title;

        const description = document.createElement("p");
        description.className = "settings-category-description";
        description.textContent = definition.description;

        copy.appendChild(eyebrow);
        copy.appendChild(title);
        copy.appendChild(description);

        categoryHeader.appendChild(icon);
        categoryHeader.appendChild(copy);

        const panelContainer = document.createElement("div");
        panelContainer.className = "settings-category-panels";

        panels.forEach(function (panel) {
            panelContainer.appendChild(panel);
        });

        category.appendChild(categoryHeader);
        category.appendChild(panelContainer);

        return category;
    }

    /*
     * Remove the old visual separator that previously divided
     * the flat settings list.
     */
    Array.from(configForm.children).forEach(function (child) {
        if (child.classList && child.classList.contains("config-section-divider")) {
            child.remove();
        }
    });

    categoryDefinitions.forEach(function (definition) {
        const panels = [];

        definition.sections.forEach(function (sectionName) {
            const panel = document.getElementById("section_" + sectionName);

            if (!panel) {
                return;
            }

            decorateSection(panel, sectionName);
            panels.push(panel);
        });

        if (!panels.length) {
            return;
        }

        const category = createCategory(definition, panels);

        if (definition.parent === "form") {
            configForm.appendChild(category);
            return;
        }

        const footer = screen.querySelector(".app-footer");

        if (footer) {
            screen.insertBefore(category, footer);
        } else {
            screen.appendChild(category);
        }
    });
}

function initializeAlternateBleedReprocessing() {
    const startButton = document.getElementById(
        "reprocessAlternateBleedButton"
    );

    if (!startButton) {
        return;
    }

    const remainingElement = document.getElementById(
        "alternateBleedRemaining"
    );

    const processedElement = document.getElementById(
        "alternateBleedProcessed"
    );

    const correctedElement = document.getElementById(
        "alternateBleedCorrected"
    );

    const missingElement = document.getElementById(
        "alternateBleedMissing"
    );

    const failedElement = document.getElementById(
        "alternateBleedFailed"
    );

    const messageElement = document.getElementById(
        "alternateBleedStatusMessage"
    );

    const failureList = document.getElementById(
        "alternateBleedFailureList"
    );

    let pollTimer = null;

    function updateStatus(status) {
        const isRunning = Boolean(
            status.is_running
        );

        startButton.disabled = isRunning;

        startButton.textContent = isRunning
            ? "Reprocessing Alternate Images..."
            : "Reprocess Bleed-Removed Alternate Images";

        if (remainingElement) {
            remainingElement.textContent =
                String(status.remaining || 0);
        }

        if (processedElement) {
            processedElement.textContent =
                String(status.processed || 0);
        }

        if (correctedElement) {
            correctedElement.textContent =
                String(status.corrected || 0);
        }

        if (missingElement) {
            missingElement.textContent =
                String(
                    status.missing_originals || 0
                );
        }

        if (failedElement) {
            failedElement.textContent =
                String(status.failed || 0);
        }

        if (messageElement) {
            messageElement.textContent =
                status.message || "";
        }

        if (failureList) {
            const failures = Array.isArray(
                status.failure_samples
            )
                ? status.failure_samples
                : [];

            if (failures.length) {
                failureList.classList.remove(
                    "hidden"
                );

                failureList.textContent =
                    failures.join("\n");
            } else {
                failureList.classList.add(
                    "hidden"
                );

                failureList.textContent = "";
            }
        }

        if (!isRunning && pollTimer) {
            window.clearInterval(
                pollTimer
            );

            pollTimer = null;
        }
    }

    async function loadStatus() {
        try {
            const response = await fetch(
                "/maintenance/alternate-bleed-reprocess/status",
                {
                    cache: "no-store"
                }
            );

            const status = await response.json();

            updateStatus(status);

        } catch (error) {
            if (messageElement) {
                messageElement.textContent =
                    "Unable to load reprocessing status.";
            }
        }
    }

    function startPolling() {
        if (pollTimer) {
            return;
        }

        pollTimer = window.setInterval(
            loadStatus,
            1000
        );
    }

    startButton.addEventListener(
        "click",
        async function () {
            startButton.disabled = true;
            startButton.textContent = "Starting Reprocessing...";

            if (messageElement) {
                messageElement.textContent =
                    "Starting alternate image bleed reprocessing...";
            }

            try {
                const response = await fetch(
                    "/maintenance/alternate-bleed-reprocess/start",
                    {
                        method: "POST",
                        headers: {
                            "Content-Type":
                                "application/json"
                        },
                        body: "{}"
                    }
                );

                const result = await response.json();

                if (!response.ok) {
                    throw new Error(
                        result.message
                        || "Unable to start reprocessing."
                    );
                }

                if (messageElement) {
                    messageElement.textContent =
                        result.message || "";
                }

                await loadStatus();
                startPolling();

            } catch (error) {
                startButton.disabled = false;
                startButton.textContent =
                    "Reprocess Bleed-Removed Alternate Images";

                if (messageElement) {
                    messageElement.textContent =
                        error.message || "Unable to start reprocessing.";
                }

                console.error(
                    "Alternate bleed reprocessing failed to start:",
                    error
                );
            }
        }
    );

    loadStatus().then(function () {
        fetch(
            "/maintenance/alternate-bleed-reprocess/status",
            {
                cache: "no-store"
            }
        )
            .then(function (response) {
                return response.json();
            })
            .then(function (status) {
                if (status.is_running) {
                    startPolling();
                }
            })
            .catch(function () {
                return;
            });
    });
}

function initializeConfigPanels() {
    const panels = document.querySelectorAll(".collapsible-panel");

    if (!panels.length) {
        return;
    }

    panels.forEach(function (panel) {
        const header = panel.querySelector(".collapsible-header");
        const body = panel.querySelector(".collapsible-body");

        if (!header || !body) {
            return;
        }

        header.addEventListener("click", function () {
            panel.classList.toggle("is-open");
        });
    });
}

function initializeGameModeCards() {
    const gameModeLists = document.querySelectorAll(".game-mode-card-list");
    const momirInput = document.getElementById("momir_mode");
    const chaosDraftInput = document.getElementById("chaos_draft_mode");
    const printButtons = document.querySelectorAll(".print-selected-token-button");

    if (!gameModeLists.length || !momirInput || !chaosDraftInput) {
        return;
    }

    function getScopeInput(scope) {
        return scope === "chaos" ? chaosDraftInput : momirInput;
    }

    function applySelection(scope, selectedValue, selectedPrintHref) {
        const hiddenInput = getScopeInput(scope);
        hiddenInput.value = selectedValue;

        document.querySelectorAll('.game-mode-card[data-mode-scope="' + scope + '"]').forEach(function (button) {
            const isSelected = button.getAttribute("data-mode-value") === selectedValue;
            button.classList.toggle("game-mode-card-selected", isSelected);
        });

        if (scope === "momir" && selectedPrintHref) {
            printButtons.forEach(function (printButton) {
                printButton.setAttribute("href", selectedPrintHref);
            });
        }
    }

    gameModeLists.forEach(function (gameModeList) {
        gameModeList.addEventListener("click", function (event) {
            const modeButton = event.target.closest(".game-mode-card");
            if (!modeButton) {
                return;
            }

            const selectedValue = modeButton.getAttribute("data-mode-value") || "";
            const selectedScope = modeButton.getAttribute("data-mode-scope") || "momir";
            const selectedPrintHref = modeButton.getAttribute("data-mode-print-href") || "";

            if (!selectedValue) {
                return;
            }

            applySelection(selectedScope, selectedValue, selectedPrintHref);
        });
    });
}

function initializeAppNavigationMenus() {
    const menuToggles = document.querySelectorAll("[data-app-menu-toggle]");
    const qrOpenButton = document.getElementById("appQrButton");
    const qrModal = document.getElementById("appQrModal");
    const qrCloseButton = document.getElementById("appQrCloseButton");

    function closeAllMenus(exceptMenu) {
        document.querySelectorAll(".app-nav-menu.is-open").forEach(function (menu) {
            if (menu !== exceptMenu) {
                menu.classList.remove("is-open");
                const toggle = menu.querySelector("[data-app-menu-toggle]");
                if (toggle) {
                    toggle.setAttribute("aria-expanded", "false");
                }
            }
        });
    }

    function closeQrModal() {
        if (!qrModal) {
            return;
        }

        qrModal.classList.add("hidden");
        qrModal.setAttribute("aria-hidden", "true");
        document.body.style.overflow = "";
    }

    menuToggles.forEach(function (toggle) {
        toggle.addEventListener("click", function (event) {
            event.preventDefault();
            event.stopPropagation();

            const menu = toggle.closest(".app-nav-menu");
            if (!menu) {
                return;
            }

            const shouldOpen = !menu.classList.contains("is-open");
            closeAllMenus(menu);
            menu.classList.toggle("is-open", shouldOpen);
            toggle.setAttribute("aria-expanded", shouldOpen ? "true" : "false");
        });
    });

    document.addEventListener("click", function (event) {
        if (!event.target.closest(".app-nav-menu")) {
            closeAllMenus(null);
        }
    });

    document.addEventListener("keydown", function (event) {
        if (event.key === "Escape") {
            closeAllMenus(null);
            closeQrModal();
        }
    });

    if (qrOpenButton && qrModal) {
        qrOpenButton.addEventListener("click", function () {
            closeAllMenus(null);
            qrModal.classList.remove("hidden");
            qrModal.setAttribute("aria-hidden", "false");
            document.body.style.overflow = "hidden";
        });

        qrModal.addEventListener("click", function (event) {
            if (event.target === qrModal) {
                closeQrModal();
            }
        });
    }

    if (qrCloseButton) {
        qrCloseButton.addEventListener("click", function () {
            closeQrModal();
        });
    }
}

function initializeConfigShortcutNavigation() {
    const panels = document.querySelectorAll(".collapsible-panel");

    if (!panels.length) {
        return;
    }

    const url = new URL(window.location.href);
    const openParam = (url.searchParams.get("open") || "").trim();
    const scrollParam = (url.searchParams.get("scroll") || "").trim();

    if (openParam) {
        const sectionNames = openParam
            .split(",")
            .map(function (value) {
                return value.trim();
            })
            .filter(Boolean);

        sectionNames.forEach(function (sectionName) {
            const panel = document.querySelector('.collapsible-panel[data-section="' + sectionName + '"]');
            if (panel) {
                panel.classList.add("is-open");
            }
        });
    }

    if (scrollParam) {
        const scrollTarget = document.getElementById("section_" + scrollParam);
        if (scrollTarget) {
            setTimeout(function () {
                scrollTarget.scrollIntoView({
                    behavior: "smooth",
                    block: "start"
                });
            }, 150);
        }
    }
}

function initializeResultCardZoom() {
    const cardImage = document.getElementById("resultCardImage");
    const zoomOverlay = document.getElementById("cardZoomOverlay");
    const zoomBackdrop = document.getElementById("cardZoomBackdrop");

    if (!cardImage || !zoomOverlay || !zoomBackdrop) {
        return;
    }

    function openZoom() {
        zoomOverlay.classList.remove("hidden");
        zoomOverlay.setAttribute("aria-hidden", "false");
        document.body.style.overflow = "hidden";
    }

    function closeZoom() {
        zoomOverlay.classList.add("hidden");
        zoomOverlay.setAttribute("aria-hidden", "true");
        document.body.style.overflow = "";
    }

    cardImage.addEventListener("click", function () {
        openZoom();
    });

    cardImage.addEventListener("keydown", function (event) {
        if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            openZoom();
        }
    });

    zoomOverlay.addEventListener("click", function () {
        closeZoom();
    });

    document.addEventListener("keydown", function (event) {
        if (event.key === "Escape" && !zoomOverlay.classList.contains("hidden")) {
            closeZoom();
        }
    });
}

function initializeMomirSelectResultLinks() {
    const selectedTypeDropdown = document.getElementById("resultSelectedType");
    const manaLinks = document.querySelectorAll('[data-mana-link="1"]');
    const againButton = document.getElementById("resultAgainButton");

    if (!selectedTypeDropdown) {
        return;
    }

    function updateResultLinks() {
        const selectedTypeValue = (selectedTypeDropdown.value || "").trim();

        manaLinks.forEach(function (link) {
            const manaValue = link.getAttribute("data-mana-value") || "";
            const url = new URL(link.href, window.location.origin);

            url.searchParams.set("mana_value", manaValue);

            if (selectedTypeValue) {
                url.searchParams.set("selected_type", selectedTypeValue);
            } else {
                url.searchParams.delete("selected_type");
            }

            link.href = url.pathname + url.search;
        });

        if (againButton) {
            const againUrl = new URL(againButton.href, window.location.origin);
            const currentManaValue = againUrl.searchParams.get("mana_value") || "";

            againUrl.searchParams.set("mana_value", currentManaValue);

            if (selectedTypeValue) {
                againUrl.searchParams.set("selected_type", selectedTypeValue);
            } else {
                againUrl.searchParams.delete("selected_type");
            }

            againButton.href = againUrl.pathname + againUrl.search;
        }
    }

    selectedTypeDropdown.addEventListener("change", function () {
        updateResultLinks();
    });

    updateResultLinks();
}

function initializeChaosDraftPage() {
    const spinButton = document.getElementById("chaosSpinButton");
    const viewButton = document.getElementById("chaosViewButton");
    const openButton = document.getElementById("chaosOpenButton");
    const savePackButton = document.getElementById("chaosSavePackButton");
    const autoSavePackToggle = document.getElementById("chaosAutoSavePackToggle");
    const nextButton = document.getElementById("chaosNextButton");
    const openRow = document.getElementById("chaosDraftOpenRow");
    const spinnerShell = document.getElementById("chaosDraftSpinner");
    const spinnerTrack = document.getElementById("chaosDraftSpinnerTrack");
    const idleCta = document.getElementById("chaosDraftIdleCta");
    const spinCtaButton = document.getElementById("chaosSpinButton");
    const pointer = document.getElementById("chaosDraftPointer");
    const message = document.getElementById("chaosDraftMessage");
    const chaosDraftScreen = document.getElementById("chaosDraftScreen");

    if (!chaosDraftScreen) {
        return;
    }

    const chaosSpinUrl = chaosDraftScreen.dataset.chaosSpinUrl || "/chaos-draft/spin";
    const chaosOpenUrl = chaosDraftScreen.dataset.chaosOpenUrl || "/chaos-draft/open";
    const chaosViewDataUrl = chaosDraftScreen.dataset.chaosViewDataUrl || "/chaos-draft/view-data";
    const chaosExportUrl = chaosDraftScreen.dataset.chaosExportUrl || "/chaos-draft/export";
    const chaosExportZipUrl = chaosDraftScreen.dataset.chaosExportZipUrl || "/chaos-draft/export-zip";
    const cardFaceDataUrl = chaosDraftScreen.dataset.cardFaceDataUrl || "/card-face-data";
    const printCardBacksEnabled = chaosDraftScreen.dataset.printCardBacks === "1";
    const busyOverlay = document.getElementById("chaosDraftBusyOverlay");
    const busyTitle = document.getElementById("chaosDraftBusyTitle");
    const busyText = document.getElementById("chaosDraftBusyText");
    const exportMainButton = document.getElementById("chaosExportMainButton");
    const exportMenuButton = document.getElementById("chaosExportMenuButton");
    const exportMenu = document.getElementById("chaosExportMenu");
    const exportCopyButton = document.getElementById("chaosExportCopyButton");
    const exportSaveButton = document.getElementById("chaosExportSaveButton");
    const inlineViewPanel = document.getElementById("chaosDraftInlineViewPanel");
    const inlineViewGrid = document.getElementById("chaosPackInlineGrid");
    const inlineViewTitle = document.getElementById("chaosPackInlineTitle");
    const inlineViewSubtitle = document.getElementById("chaosPackInlineSubtitle");
    const packZoomOverlay = document.getElementById("chaosPackZoomOverlay");
    const packZoomBackdrop = document.getElementById("chaosPackZoomBackdrop");
    const packZoomImage = document.getElementById("chaosPackZoomImage");
    const printExportPrintButton = document.getElementById("printExportPrintButton");

    if (window.iMomirPrintExportModal) {
        window.iMomirPrintExportModal.init({
            openButtonId: "chaosOpenButton",
            printUrl: chaosOpenUrl,
            exportZipUrl: chaosExportZipUrl
        });
    }
    const openPrintInNewTab = chaosDraftScreen
        ? chaosDraftScreen.getAttribute("data-open-print-in-new-tab") === "1"
        : true;

    const soundEnabled = chaosDraftScreen
        ? chaosDraftScreen.getAttribute("data-sound-enabled") === "1"
        : true;

    const chaosExportFormat = chaosDraftScreen
        ? (chaosDraftScreen.getAttribute("data-chaos-export-format") || "none").toLowerCase()
        : "none";

    if (
        !spinCtaButton ||
        !spinnerShell ||
        !spinnerTrack ||
        !message ||
        !idleCta ||
        !pointer ||
        !busyOverlay ||
        !busyTitle ||
        !busyText
    ) {
        return;
    }

    // Allow clicking outside the busy modal to cancel
    busyOverlay.addEventListener("click", function (e) {
        if (
            e.target.classList.contains("chaos-draft-busy-overlay") ||
            e.target.classList.contains("chaos-draft-busy-backdrop")
        ) {
            cancelOpenPack("user-click-outside");
        }
    });

    let currentSpinResult = null;
    let animationInProgress = false;
    let openInProgress = false;
    let openAbortController = null;
    let audioContext = null;
    let rouletteTickCardSpacing = 0;
    let rouletteNextTickThreshold = null;
    let rouletteLastTranslateX = 0;
    let rouletteTickTimer = null;
    let currentWinningPack = null;
    let selectedExportAction = "copy";
    let inlinePackViewLoaded = false;
    let currentPackSavedToDb = false;
    let inlinePackFaceMetadataByUuid = {};
    let activeInlineZoomSourceImage = null;
    let activeInlineZoomFlipButton = null;

    const jackpotBoosterTypes = new Set([
        "collector",
        "vip",
        "premium"
    ]);

    const jackpotSetCodes = new Set([
        "LEA", // Alpha
        "LEB",  // Beta
        "2ED",
        "ARN",
        "LEG",
        "ATQ",
        "3ED",
        "30A",
        "PTK",
        "USG"
    ]);

    const badPackSetCodes = new Set([
        "HML",
        "PCY",
        "FEM",
        "DRK",
        "CHR"
    ]);

    function showBusyOverlay(titleText, bodyText) {
        busyTitle.textContent = titleText || "Working";
        busyText.textContent = bodyText || "";
        busyOverlay.classList.remove("hidden");
        busyOverlay.setAttribute("aria-hidden", "false");
    }

    function hideBusyOverlay() {
        busyOverlay.classList.add("hidden");
        busyOverlay.setAttribute("aria-hidden", "true");
    }

    function hideInlinePackView() {
        if (inlineViewPanel) {
            inlineViewPanel.classList.add("hidden");
        }

        if (inlineViewGrid) {
            inlineViewGrid.innerHTML = "";
        }

        if (inlineViewTitle) {
            inlineViewTitle.textContent = "Pack contents";
        }

        if (inlineViewSubtitle) {
            inlineViewSubtitle.textContent = "";
        }

        inlinePackViewLoaded = false;
    }

    function getInlinePackCardUuid(imageElement) {
        if (!imageElement) {
            return "";
        }

        const hostElement = imageElement.closest("[data-card-uuid]");

        return String(
            imageElement.dataset.cardUuid
            || (hostElement ? hostElement.dataset.cardUuid : "")
            || ""
        ).trim();
    }

    function getInlinePackFaceMetadata(imageElement) {
        const cardUuid = getInlinePackCardUuid(imageElement);

        if (!cardUuid) {
            return null;
        }

        return inlinePackFaceMetadataByUuid[cardUuid] || null;
    }

    function updateInlinePackFlipButtonFace(buttonElement, faceName) {
        if (!buttonElement) {
            return;
        }

        if (faceName === "back") {
            buttonElement.classList.add("chaos-pack-inline-flip-button-flipped");
            buttonElement.setAttribute("aria-label", "Show front face");
            buttonElement.setAttribute("title", "Show front face");
        } else {
            buttonElement.classList.remove("chaos-pack-inline-flip-button-flipped");
            buttonElement.setAttribute("aria-label", "Show back face");
            buttonElement.setAttribute("title", "Show back face");
        }
    }

    function setInlinePackImageFace(imageElement, metadata, faceName, flipButton) {
        if (!imageElement || !metadata) {
            return;
        }

        const targetFace = faceName === "back" ? "back" : "front";
        const targetSrc = targetFace === "back" ? metadata.back_src : metadata.front_src;
        const targetAlt = targetFace === "back" ? metadata.back_alt : metadata.front_alt;

        if (!targetSrc) {
            return;
        }

        imageElement.classList.add("chaos-pack-inline-card-flipping");

        window.setTimeout(function () {
            imageElement.src = targetSrc;
            imageElement.alt = targetAlt || imageElement.alt || "";
            imageElement.dataset.currentFace = targetFace;
            updateInlinePackFlipButtonFace(flipButton, targetFace);
        }, 120);

        window.setTimeout(function () {
            imageElement.classList.remove("chaos-pack-inline-card-flipping");
        }, 300);
    }

    function flipInlinePackImage(imageElement, flipButton) {
        const metadata = getInlinePackFaceMetadata(imageElement);

        if (!metadata || !metadata.is_dual_faced || !metadata.back_src) {
            return;
        }

        const currentFace = imageElement.dataset.currentFace === "back" ? "back" : "front";
        const nextFace = currentFace === "back" ? "front" : "back";

        setInlinePackImageFace(imageElement, metadata, nextFace, flipButton);

        if (imageElement === packZoomImage && activeInlineZoomSourceImage) {
            const sourceButton = activeInlineZoomSourceImage
                .closest(".chaos-pack-inline-image-wrap")
                ?.querySelector(".chaos-pack-inline-flip-button");

            setInlinePackImageFace(activeInlineZoomSourceImage, metadata, nextFace, sourceButton);
        }
    }

    function createInlinePackFlipButton(imageElement) {
        const metadata = getInlinePackFaceMetadata(imageElement);

        if (!metadata || !metadata.is_dual_faced || !metadata.back_src) {
            return null;
        }

        const button = document.createElement("button");
        button.type = "button";
        button.className = "chaos-pack-inline-flip-button";
        button.innerHTML = '<i class="fa-solid fa-rotate"></i>';
        button.setAttribute("aria-label", "Show back face");
        button.setAttribute("title", "Show back face");

        button.addEventListener("click", function (event) {
            event.preventDefault();
            event.stopPropagation();
            flipInlinePackImage(imageElement, button);
        });

        return button;
    }

    function addInlinePackFlipButtonToImage(imageElement) {
        const metadata = getInlinePackFaceMetadata(imageElement);

        if (!metadata || !metadata.is_dual_faced || !metadata.back_src) {
            return;
        }

        const imageWrap = imageElement.closest(".chaos-pack-inline-image-wrap");

        if (!imageWrap || imageWrap.querySelector(".chaos-pack-inline-flip-button")) {
            return;
        }

        imageWrap.classList.add("chaos-pack-inline-flip-host");
        imageElement.dataset.currentFace = imageElement.dataset.currentFace || "front";

        const button = createInlinePackFlipButton(imageElement);

        if (button) {
            imageWrap.appendChild(button);
        }
    }

    async function loadInlinePackFaceMetadata(cards) {
        const cardUuids = [];

        (cards || []).forEach(function (cardData) {
            const cardUuid = String(cardData.card_uuid || "").trim();

            if (cardUuid && !cardUuids.includes(cardUuid)) {
                cardUuids.push(cardUuid);
            }
        });

        inlinePackFaceMetadataByUuid = {};

        if (!cardUuids.length) {
            return;
        }

        try {
            const response = await fetch(cardFaceDataUrl, {
                method: "POST",
                headers: {
                    "Accept": "application/json",
                    "Content-Type": "application/json"
                },
                body: JSON.stringify({
                    chaos_card_uuids: cardUuids
                })
            });

            const payload = await response.json();

            if (!response.ok || !payload.ok) {
                console.warn("Inline pack flip: card-face-data failed.", payload);
                return;
            }

            inlinePackFaceMetadataByUuid = payload.chaos_cards || {};
        } catch (error) {
            console.warn("Inline pack flip: card-face-data failed.", error);
        }
    }

    function openPackZoom(imageElement) {
        if (!packZoomOverlay || !packZoomImage || !imageElement) {
            return;
        }

        const metadata = getInlinePackFaceMetadata(imageElement);
        const currentFace = imageElement.dataset.currentFace === "back" ? "back" : "front";
        const imageSrc = currentFace === "back" && metadata
            ? metadata.back_src
            : (imageElement.src || "");
        const imageAlt = currentFace === "back" && metadata
            ? metadata.back_alt
            : (imageElement.alt || "Pack card image");

        if (!imageSrc) {
            return;
        }

        activeInlineZoomSourceImage = imageElement;

        packZoomImage.src = imageSrc;
        packZoomImage.alt = imageAlt;
        packZoomImage.dataset.cardUuid = getInlinePackCardUuid(imageElement);
        packZoomImage.dataset.currentFace = currentFace;

        if (activeInlineZoomFlipButton) {
            activeInlineZoomFlipButton.remove();
            activeInlineZoomFlipButton = null;
        }

        const zoomContent = packZoomImage.closest(".card-zoom-content");

        if (zoomContent) {
            zoomContent.classList.add("chaos-pack-inline-flip-host");

            if (metadata && metadata.is_dual_faced && metadata.back_src) {
                activeInlineZoomFlipButton = createInlinePackFlipButton(packZoomImage);

                if (activeInlineZoomFlipButton) {
                    updateInlinePackFlipButtonFace(activeInlineZoomFlipButton, currentFace);
                    zoomContent.appendChild(activeInlineZoomFlipButton);
                }
            }
        }

        packZoomOverlay.classList.remove("hidden");
        packZoomOverlay.setAttribute("aria-hidden", "false");
        document.body.style.overflow = "hidden";
    }

    function closePackZoom() {
        if (!packZoomOverlay || !packZoomImage) {
            return;
        }

        if (activeInlineZoomFlipButton) {
            activeInlineZoomFlipButton.remove();
            activeInlineZoomFlipButton = null;
        }

        activeInlineZoomSourceImage = null;

        packZoomOverlay.classList.add("hidden");
        packZoomOverlay.setAttribute("aria-hidden", "true");
        packZoomImage.src = "";
        packZoomImage.alt = "";
        packZoomImage.dataset.cardUuid = "";
        packZoomImage.dataset.currentFace = "front";
        document.body.style.overflow = "";
    }

    function formatPackCardPrice(priceValue) {
        if (priceValue === null || priceValue === undefined || priceValue === "") {
            return "Price unavailable";
        }

        const numericPrice = Number(priceValue);
        if (!Number.isFinite(numericPrice)) {
            return "Price unavailable";
        }

        return `$${numericPrice.toFixed(2)}`;
    }

    function buildPackBadgeMarkup(cardData) {
        const badges = [];

        if (cardData.price !== null && cardData.price !== undefined && Number.isFinite(Number(cardData.price))) {
            const numericPrice = Number(cardData.price);
            const priceClass = numericPrice > 2 ? " chaos-pack-inline-price-high" : "";
            badges.push(
                `<span class="chaos-pack-inline-price${priceClass}">${formatPackCardPrice(numericPrice)}</span>`
            );
        }

        const specialBadges = Array.isArray(cardData.special_badges) ? cardData.special_badges : [];
        specialBadges.forEach(function (badgeText) {
            badges.push(`<span class="chaos-pack-inline-badge">${badgeText}</span>`);
        });

        return badges.join("");
    }

    async function renderInlinePackView(payload) {
        if (!inlineViewPanel || !inlineViewGrid || !payload) {
            return;
        }

        const cards = Array.isArray(payload.cards) ? payload.cards : [];
        await loadInlinePackFaceMetadata(cards);

        if (inlineViewTitle) {
            inlineViewTitle.textContent = payload.pack_display_name || "Pack contents";
        }

        if (inlineViewSubtitle) {
            const subtitleParts = [];
            subtitleParts.push(`${payload.pack_total_cards || cards.length} cards`);

            if (payload.bonus_pack_opened) {
                subtitleParts.push("Bonus pack opened");
            }

            inlineViewSubtitle.textContent = subtitleParts.join(" • ");
        }

        inlineViewGrid.innerHTML = "";

        cards.forEach(function (cardData) {
            const cardElement = document.createElement("div");
            cardElement.className = "chaos-pack-inline-card";
            cardElement.dataset.cardUuid = cardData.card_uuid || "";
            cardElement.dataset.cardName = cardData.card_name || "";

            const badgesMarkup = buildPackBadgeMarkup(cardData);

            cardElement.innerHTML = `
                <div class="chaos-pack-inline-image-wrap" data-card-uuid="${cardData.card_uuid || ""}">
                    <img
                        src="${cardData.image_src}"
                        alt="${cardData.card_name}"
                        class="chaos-pack-inline-image"
                        role="button"
                        tabindex="0"
                        data-card-uuid="${cardData.card_uuid || ""}"
                    >
                </div>
                <div class="chaos-pack-inline-info">
                    <div class="chaos-pack-inline-name">${cardData.card_name}</div>
                    <div class="chaos-pack-inline-meta-row">${badgesMarkup}</div>
                </div>
            `;

            const imageElement = cardElement.querySelector(".chaos-pack-inline-image");
            if (imageElement) {
                imageElement.dataset.currentFace = "front";

                imageElement.addEventListener("click", function () {
                    openPackZoom(imageElement);
                });

                imageElement.addEventListener("keydown", function (event) {
                    if (event.key === "Enter" || event.key === " ") {
                        event.preventDefault();
                        openPackZoom(imageElement);
                    }
                });

                addInlinePackFlipButtonToImage(imageElement);
            }

            inlineViewGrid.appendChild(cardElement);
        });

        inlineViewPanel.classList.remove("hidden");
        inlinePackViewLoaded = true;
    }

    async function loadInlinePackView() {
        const response = await fetch(chaosViewDataUrl, {
            method: "GET",
            headers: {
                "Accept": "application/json"
            }
        });

        const payload = await response.json();

        if (!response.ok || !payload.ok) {
            throw new Error(payload.message || "Failed to load pack view.");
        }

        await renderInlinePackView(payload);
    }

    function resetOpenPackUiState() {
        hideBusyOverlay();

        openInProgress = false;
        openAbortController = null;

        if (viewButton) {
            viewButton.disabled = false;
            viewButton.classList.remove("action-button-loading");
            viewButton.textContent = "View";
        }

        if (openButton) {
            openButton.disabled = false;
            openButton.classList.remove("action-button-loading");
            openButton.textContent = "Print / Export";
        }

        setSavePackButtonState(false);

        if (exportMainButton) {
            exportMainButton.disabled = chaosExportFormat === "none";
        }

        if (exportMenuButton) {
            exportMenuButton.disabled = chaosExportFormat === "none";
        }

        if (nextButton) {
            nextButton.disabled = false;
        }

        if (currentSpinResult) {
            spinCtaButton.disabled = true;
        } else {
            spinCtaButton.disabled = false;
        }
    }

    function cancelOpenPack(reason) {
        if (openAbortController) {
            try {
                openAbortController.abort();
            } catch (e) {
            }
        }

        resetOpenPackUiState();

        if (reason) {
            console.warn("Chaos Draft open cancelled:", reason);
        }
    }

    function getAudioContext() {
        if (!soundEnabled) {
            return null;
        }

        const AudioContextClass = window.AudioContext || window.webkitAudioContext;
        if (!AudioContextClass) {
            return null;
        }

        if (!audioContext) {
            audioContext = new AudioContextClass();
        }

        if (audioContext.state === "suspended") {
            audioContext.resume().catch(function () {
            });
        }

        return audioContext;
    }

    function playTone(frequency, durationSeconds, type, volume, whenOffsetSeconds) {
        const ctx = getAudioContext();
        if (!ctx) {
            return;
        }

        const oscillator = ctx.createOscillator();
        const gainNode = ctx.createGain();
        const startAt = ctx.currentTime + (whenOffsetSeconds || 0);

        oscillator.type = type || "sine";
        oscillator.frequency.setValueAtTime(frequency, startAt);

        gainNode.gain.setValueAtTime(0.0001, startAt);
        gainNode.gain.exponentialRampToValueAtTime(Math.max(volume || 0.03, 0.0001), startAt + 0.01);
        gainNode.gain.exponentialRampToValueAtTime(0.0001, startAt + durationSeconds);

        oscillator.connect(gainNode);
        gainNode.connect(ctx.destination);

        oscillator.start(startAt);
        oscillator.stop(startAt + durationSeconds + 0.02);
    }

    function playDecayingTone(frequency, durationSeconds, type, volume, whenOffsetSeconds) {
        const ctx = getAudioContext();
        if (!ctx) {
            return;
        }

        const oscillator = ctx.createOscillator();
        const gainNode = ctx.createGain();
        const startAt = ctx.currentTime + (whenOffsetSeconds || 0);

        oscillator.type = type || "sine";
        oscillator.frequency.setValueAtTime(frequency, startAt);

        gainNode.gain.setValueAtTime(0.0001, startAt);
        gainNode.gain.exponentialRampToValueAtTime(
            Math.max(volume || 0.03, 0.0001),
            startAt + 0.008
        );
        gainNode.gain.exponentialRampToValueAtTime(
            0.0001,
            startAt + durationSeconds
        );

        oscillator.connect(gainNode);
        gainNode.connect(ctx.destination);

        oscillator.start(startAt);
        oscillator.stop(startAt + durationSeconds + 0.03);
    }

    function playRouletteTick() {
        //playTone(1150, 0.028, "triangle", 0.010, 0.00);
        //playTone(820, 0.040, "triangle", 0.006, 0.008);
        playTone(484, 0.115, "square", 0.012, 0.000);
        playTone(968, 0.070, "square", 0.0035, 0.000);
    }

    /*function playMinorWinSound() {
        //playTone(740, 0.11, "triangle", 0.04, 0.00);
        playTone(932, 0.12, "triangle", 0.04, 0.07);
        playTone(1175, 0.18, "triangle", 0.05, 0.14);
    }*/

    function playMinorWinSound() {
        // Trumpet-style victory fanfare
        // Using sawtooth for brass-like tone

        const baseVolume = 0.045;

        // Note sequence (ascending, triumphant)
        playTone(784, 0.18, "sawtooth", baseVolume, 0.00);   // G5
        playTone(988, 0.18, "sawtooth", baseVolume, 0.14);   // B5
        playTone(1175, 0.22, "sawtooth", baseVolume, 0.28);  // D6

        // Final sustained victory note
        playTone(1568, 0.60, "sawtooth", baseVolume + 0.01, 0.46); // G6

        // Add brightness layer (harmonics)
        playTone(3136, 0.50, "triangle", 0.012, 0.46);
    }

    /*function playJackpotSound() {
        playTone(523, 0.14, "triangle", 0.04, 0.00);
        playTone(659, 0.14, "triangle", 0.04, 0.08);
        playTone(784, 0.16, "triangle", 0.045, 0.16);
        playTone(1047, 0.32, "triangle", 0.055, 0.28);
    }*/

    function playJackpotSound() {
        // Epic trumpet-style jackpot fanfare
        // Bigger rise, longer finish, brighter harmonic layer

        const v = 0.055;

        // Opening fanfare
        playTone(784, 0.16, "sawtooth", v, 0.00);    // G5
        playTone(988, 0.16, "sawtooth", v, 0.12);    // B5
        playTone(1175, 0.18, "sawtooth", v, 0.24);   // D6
        playTone(1568, 0.22, "sawtooth", v + 0.004, 0.38); // G6

        // Heroic second rise
        playTone(1175, 0.18, "sawtooth", v, 0.58);   // D6
        playTone(1568, 0.20, "sawtooth", v + 0.004, 0.72); // G6
        playTone(1976, 0.24, "sawtooth", v + 0.006, 0.88); // B6

        // Final victory hold
        playTone(2350, 0.95, "sawtooth", v + 0.010, 1.06); // D7

        // Bright brass shimmer
        playTone(4700, 0.72, "triangle", 0.012, 1.08);
        playTone(3136, 0.82, "triangle", 0.010, 1.06);
    }

    function playBadPackSound() {
        // Classic Price Is Right losing horns: descending "wahh waaahhh"

        const v = 0.045;

        // First horn (shorter)
        playDecayingTone(370, 0.55, "sawtooth", v, 0.00);   // F#4-ish
        playDecayingTone(740, 0.40, "triangle", 0.010, 0.00); // harmonic layer

        // Second horn (longer, lower, sadder)
        playDecayingTone(277, 0.95, "sawtooth", v + 0.004, 0.38); // C#4-ish
        playDecayingTone(554, 0.70, "triangle", 0.010, 0.38);     // harmonic
    }

    function isBigWinPack(packInfo) {
        if (!packInfo) {
            return false;
        }

        const boosterName = String(packInfo.booster_name || "").trim().toLowerCase();
        const setCode = String(packInfo.set_code || "").trim().toUpperCase();

        if (jackpotBoosterTypes.has(boosterName)){
            return true;
        }

        if (jackpotSetCodes.has(setCode)){
            return true;
        }

        return false;
    }

    function isBadPack(packInfo) {
        if (!packInfo) {
            return false;
        }

        const setCode = String(packInfo.set_code || "").trim().toUpperCase();

        return badPackSetCodes.has(setCode);
    }

    function stopRouletteTicks() {
        rouletteTickCardSpacing = 0;
        rouletteNextTickThreshold = null;
        rouletteLastTranslateX = 0;
    }

    function startRouletteTicks(cardSpacing, startTranslateX) {
        rouletteTickCardSpacing = Math.max(1, Number(cardSpacing) || 0);
        rouletteLastTranslateX = Number(startTranslateX) || 0;

        if (rouletteTickCardSpacing <= 0) {
            rouletteNextTickThreshold = null;
            return;
        }

        rouletteNextTickThreshold =
            rouletteLastTranslateX - rouletteTickCardSpacing;
    }

    function updateRouletteTicks(currentTranslateX) {
        if (!rouletteTickCardSpacing || rouletteNextTickThreshold === null) {
            rouletteLastTranslateX = currentTranslateX;
            return;
        }

        // Spinner moves left over time, so translateX becomes more negative.
        // Fire one tick each time we cross another card spacing.
        while (currentTranslateX <= rouletteNextTickThreshold) {
            playRouletteTick();
            rouletteNextTickThreshold -= rouletteTickCardSpacing;
        }

        rouletteLastTranslateX = currentTranslateX;
    }

    function playWinningSoundForPack(packInfo) {
        if (isBadPack(packInfo)) {
            playBadPackSound(); // you'll define this next
            return;
        }

        if (isBigWinPack(packInfo)) {
            playJackpotSound();
        } else {
            playMinorWinSound();
        }
    }

    function getWinningVisualClassForPack(packInfo) {
        if (isBadPack(packInfo)) {
            return "chaos-pack-card-winning-badpack";
        }

        if (isBigWinPack(packInfo)) {
            return "chaos-pack-card-winning-jackpot";
        }

        return "chaos-pack-card-winning-normal";
    }

    document.addEventListener("keydown", function (e) {
        if (e.key === "Escape") {
            cancelOpenPack("escape-key");
        }
    });

    window.addEventListener("pageshow", function () {
        resetOpenPackUiState();
    });

    window.addEventListener("pagehide", function () {
        hideBusyOverlay();
    });

    function closeExportMenu() {
        if (!exportMenu || !exportMenuButton) {
            return;
        }

        exportMenu.classList.add("hidden");
        exportMenuButton.setAttribute("aria-expanded", "false");
    }

    function toggleExportMenu() {
        if (!exportMenu || !exportMenuButton || exportMenuButton.disabled) {
            return;
        }

        const willOpen = exportMenu.classList.contains("hidden");
        exportMenu.classList.toggle("hidden", !willOpen);
        exportMenuButton.setAttribute("aria-expanded", willOpen ? "true" : "false");
    }

    function applySelectedExportActionUi() {
        if (exportCopyButton) {
            exportCopyButton.classList.toggle(
                "chaos-export-menu-item-active",
                selectedExportAction === "copy"
            );
        }

        if (exportSaveButton) {
            exportSaveButton.classList.toggle(
                "chaos-export-menu-item-active",
                selectedExportAction === "save"
            );
        }

        if (exportMainButton) {
            exportMainButton.textContent = selectedExportAction === "save" ? "Save" : "Copy";
        }
    }

    function setSelectedExportAction(actionName) {
        const normalizedAction = String(actionName || "").trim().toLowerCase();

        if (normalizedAction !== "copy" && normalizedAction !== "save") {
            return;
        }

        selectedExportAction = normalizedAction;
        applySelectedExportActionUi();
    }

    async function runSelectedExportAction() {
        if (selectedExportAction === "save") {
            await requestChaosExport(true);
            return;
        }

        await requestChaosExport(false);
    }

    async function copyTextToClipboard(textValue) {
        const normalizedText = String(textValue || "");

        if (navigator.clipboard && navigator.clipboard.writeText) {
            try {
                await navigator.clipboard.writeText(normalizedText);
                return true;
            } catch (error) {
                // Fall through to legacy copy method.
            }
        }

        const hiddenTextArea = document.createElement("textarea");
        hiddenTextArea.value = normalizedText;
        hiddenTextArea.setAttribute("readonly", "");
        hiddenTextArea.style.position = "fixed";
        hiddenTextArea.style.top = "-1000px";
        hiddenTextArea.style.left = "-1000px";
        hiddenTextArea.style.opacity = "0";
        hiddenTextArea.style.pointerEvents = "none";

        document.body.appendChild(hiddenTextArea);

        try {
            hiddenTextArea.focus();
            hiddenTextArea.select();
            hiddenTextArea.setSelectionRange(0, hiddenTextArea.value.length);

            const copySucceeded = document.execCommand("copy");

            if (!copySucceeded) {
                throw new Error("Legacy clipboard copy command failed.");
            }

            return true;
        } finally {
            document.body.removeChild(hiddenTextArea);
        }
    }

    function setSavePackButtonState(isBusy, savedText) {
        if (!savePackButton) {
            return;
        }

        savePackButton.disabled = Boolean(isBusy) || !currentSpinResult || currentPackSavedToDb;
        savePackButton.classList.toggle("action-button-loading", Boolean(isBusy));

        if (savedText) {
            savePackButton.textContent = savedText;
        } else if (currentPackSavedToDb) {
            savePackButton.textContent = "Saved";
        } else {
            savePackButton.textContent = "Save";
        }
    }

    async function saveCurrentPackToDb(showAlertOnSuccess) {
        if (!savePackButton) {
            return null;
        }

        if (!currentSpinResult || animationInProgress) {
            throw new Error("No completed Chaos Draft pack is ready to save.");
        }

        if (currentPackSavedToDb) {
            return {
                ok: true,
                already_saved: true,
                message: "Pack was already saved."
            };
        }

        setSavePackButtonState(true, "Saving...");

        try {
            const response = await fetch("/chaos-draft/save-pack", {
                method: "POST",
                headers: {
                    "Accept": "application/json"
                }
            });

            const payload = await response.json();

            if (!response.ok || !payload.ok) {
                throw new Error(payload.message || "Failed to save pack.");
            }

            currentPackSavedToDb = true;
            setSavePackButtonState(false, "Saved");

            if (showAlertOnSuccess) {
                window.alert(payload.message || "Pack saved to the Pack Tracking Database.");
            }

            return payload;
        } catch (error) {
            currentPackSavedToDb = false;
            setSavePackButtonState(false, "Save");
            throw error;
        }
    }

    async function requestChaosExport(saveToFile) {
        const response = await fetch(chaosExportUrl, {
            method: "POST",
            headers: {
                "Accept": "application/json"
            }
        });

        const payload = await response.json();

        if (!response.ok || !payload.ok) {
            throw new Error(payload.message || "Failed to export Chaos Draft pack.");
        }

        if (saveToFile) {
            window.location.href = payload.download_url;
            return;
        }

        await copyTextToClipboard(payload.export_text);
    }

    function hideOpenRow() {
        if (openRow) {
            openRow.classList.remove("chaos-draft-open-row-visible");
        }

        if (viewButton) {
            viewButton.disabled = true;
            viewButton.classList.remove("action-button-loading");
            viewButton.textContent = "View";
        }

        if (openButton) {
            openButton.disabled = true;
            openButton.classList.remove("action-button-loading");
            openButton.textContent = "Print / Export";
        }

        if (savePackButton) {
            savePackButton.disabled = true;
            savePackButton.classList.remove("action-button-loading");
            savePackButton.textContent = "Save";
        }

        if (exportMainButton) {
            exportMainButton.disabled = true;
        }

        if (exportMenuButton) {
            exportMenuButton.disabled = true;
        }

        closeExportMenu();
    }

    function showOpenRow() {
        if (openRow) {
            openRow.classList.add("chaos-draft-open-row-visible");
        }

        if (viewButton) {
            viewButton.disabled = false;
            viewButton.classList.remove("action-button-loading");
            viewButton.textContent = "View";
        }

        if (openButton) {
            openButton.disabled = false;
            openButton.classList.remove("action-button-loading");
            openButton.textContent = "Print / Export";
        }

        setSavePackButtonState(false);

        if (exportMainButton) {
            exportMainButton.disabled = chaosExportFormat === "none";
        }

        if (exportMenuButton) {
            exportMenuButton.disabled = chaosExportFormat === "none";
        }
    }

    function setButtonsForIdle() {
        spinCtaButton.disabled = openInProgress;
        idleCta.classList.remove("hidden");
        idleCta.classList.remove("chaos-draft-idle-cta-sinking");
        spinnerTrack.classList.add("hidden");
        pointer.classList.add("hidden");

        hideOpenRow();

        if (nextButton) {
            nextButton.disabled = false;
        }
    }

    function setButtonsForAnimating() {
        spinCtaButton.disabled = true;
        spinnerTrack.classList.remove("hidden");
        pointer.classList.remove("hidden");

        hideOpenRow();

        if (nextButton) {
            nextButton.disabled = true;
        }
    }

    function setButtonsForComplete() {
        spinCtaButton.disabled = openInProgress;
        idleCta.classList.add("hidden");
        spinnerTrack.classList.remove("hidden");
        pointer.classList.remove("hidden");

        showOpenRow();

        if (nextButton) {
            nextButton.disabled = false;
        }
    }

    function clearWinningState() {
        const allCards = spinnerTrack.querySelectorAll(".chaos-pack-card");
        allCards.forEach(function (card) {
            card.classList.remove(
                "chaos-pack-card-winning",
                "chaos-pack-card-winning-normal",
                "chaos-pack-card-winning-jackpot",
                "chaos-pack-card-winning-badpack"
            );
        });
    }

    function buildRepeatedPackSequence(displayPacks, repeatCount) {
        const sequence = [];

        for (let repeatIndex = 0; repeatIndex < repeatCount; repeatIndex += 1) {
            displayPacks.forEach(function (pack, packIndex) {
                sequence.push({
                    ...pack,
                    base_index: packIndex,
                    repeat_index: repeatIndex
                });
            });
        }

        return sequence;
    }

    function renderSpinnerCards(spinResult, repeatCount) {
        const displayPacks = spinResult.display_packs || [];
        spinnerTrack.innerHTML = "";

        const repeatedSequence = buildRepeatedPackSequence(displayPacks, repeatCount);

        repeatedSequence.forEach(function (pack, absoluteIndex) {
            const packCard = document.createElement("div");
            packCard.className = "chaos-pack-card";
            packCard.setAttribute("data-chaos-card-index", String(absoluteIndex));
            packCard.setAttribute("data-base-index", String(pack.base_index));
            packCard.setAttribute("data-repeat-index", String(pack.repeat_index));

            packCard.innerHTML = `
                <div class="chaos-pack-card-image-wrap">
                    <img src="${pack.image_src}" alt="${pack.display_name}" class="chaos-pack-card-image">
                </div>
                <div class="chaos-pack-card-title">${pack.display_name}</div>
            `;

            spinnerTrack.appendChild(packCard);
        });

        return repeatedSequence;
    }

    function getCenteredTranslateForCard(cardElement) {
        const spinnerWindow = spinnerShell.querySelector(".chaos-draft-spinner-window");
        if (!spinnerWindow || !cardElement) {
            return 0;
        }

        const cardCenter = cardElement.offsetLeft + (cardElement.offsetWidth / 2);
        const windowCenter = spinnerWindow.clientWidth / 2;

        return -(cardCenter - windowCenter);
    }

    function easeOutCubic(t) {
        return 1 - Math.pow(1 - t, 3);
    }

    function animateTrackToTarget(finalAbsoluteIndex, visiblePackCount) {
        const spinnerWindow = spinnerShell.querySelector(".chaos-draft-spinner-window");
        const allCards = spinnerTrack.querySelectorAll(".chaos-pack-card");
        const finalCard = spinnerTrack.querySelector(`[data-chaos-card-index="${finalAbsoluteIndex}"]`);

        if (!spinnerWindow || !allCards.length || !finalCard) {
            stopRouletteTicks();
            animationInProgress = false;
            setButtonsForIdle();
            return;
        }

        clearWinningState();

        spinnerTrack.style.transition = "none";
        spinnerTrack.style.transform = "translateX(0px)";

        const finalTranslate = getCenteredTranslateForCard(finalCard);

        const firstCard = allCards[0];
        const secondCard = allCards[1];
        const oneCardTravel = secondCard
            ? (secondCard.offsetLeft - firstCard.offsetLeft)
            : (finalCard.offsetWidth + 14);

        const jostleCardWidths = (-0.18) + (Math.random() * 0.36);
        const jostleOffsetPx = jostleCardWidths * oneCardTravel;
        const approachTranslate = finalTranslate + jostleOffsetPx;

        const startTranslate = 0;
        const packCount = Number(visiblePackCount || 0);

        let durationMs = 7600 + Math.round(Math.random() * 1100);

        if (packCount > 0 && packCount <= 3) {
            durationMs = 1800 + Math.round(Math.random() * 350);
        } else if (packCount <= 6) {
            durationMs = 2600 + Math.round(Math.random() * 500);
        } else if (packCount <= 10) {
            durationMs = 4200 + Math.round(Math.random() * 700);
        }

        startRouletteTicks(oneCardTravel, startTranslate);

        let animationStart = null;

        function snapToCenter() {
            spinnerTrack.style.transition = "transform 180ms ease-out";
            spinnerTrack.style.transform = `translateX(${finalTranslate}px)`;

            window.setTimeout(function () {
                const winningVisualClass = getWinningVisualClassForPack(currentWinningPack);

                spinnerTrack.style.transition = "none";
                finalCard.classList.add("chaos-pack-card-winning");
                finalCard.classList.add(winningVisualClass);
                stopRouletteTicks();
                playWinningSoundForPack(currentWinningPack);
                animationInProgress = false;
                setButtonsForComplete();
            }, 190);
        }

        function step(timestamp) {
            if (!animationStart) {
                animationStart = timestamp;
            }

            const elapsed = timestamp - animationStart;
            const progress = Math.min(elapsed / durationMs, 1);
            const easedProgress = easeOutCubic(progress);
            const currentTranslate = startTranslate + ((approachTranslate - startTranslate) * easedProgress);

            spinnerTrack.style.transform = `translateX(${currentTranslate}px)`;
            updateRouletteTicks(currentTranslate);

            if (progress < 1) {
                window.requestAnimationFrame(step);
                return;
            }

            spinnerTrack.style.transform = `translateX(${approachTranslate}px)`;
            snapToCenter();
        }

        window.requestAnimationFrame(step);
    }

    function runSpinAnimation(spinResult) {
        animationInProgress = true;

        const displayPacks = spinResult.display_packs || [];
        const winningStopIndex = Number(spinResult.winning_stop_index || 0);

        if (!displayPacks.length) {
            stopRouletteTicks();
            animationInProgress = false;
            message.classList.remove("hidden");
            spinnerShell.classList.add("hidden");
            message.textContent = "No Chaos Draft packs were available.";
            setButtonsForIdle();
            return;
        }

        const repeatCount = 7;
        const repeatedSequence = renderSpinnerCards(spinResult, repeatCount);

        message.classList.add("hidden");
        spinnerShell.classList.remove("hidden");

        currentWinningPack = spinResult.winning_pack || null;
        const winningRepeatIndex = Math.floor(repeatCount / 2);
        const finalAbsoluteIndex = (winningRepeatIndex * displayPacks.length) + winningStopIndex;

        if (!repeatedSequence.length || finalAbsoluteIndex < 0 || finalAbsoluteIndex >= repeatedSequence.length) {
            stopRouletteTicks();
            animationInProgress = false;
            setButtonsForIdle();
            message.classList.remove("hidden");
            message.textContent = "Chaos Draft spin failed to resolve the winning pack.";
            return;
        }

        animateTrackToTarget(finalAbsoluteIndex);
    }

    async function runSpin() {
        if (animationInProgress) {
            return;
        }

        currentSpinResult = null;
        currentPackSavedToDb = false;
        animationInProgress = true;

        hideOpenRow();

        idleCta.classList.add("chaos-draft-idle-cta-sinking");
        spinCtaButton.disabled = true;

        if (nextButton) {
            nextButton.disabled = true;
        }

        message.classList.add("hidden");
        spinnerShell.classList.remove("hidden");
        spinnerTrack.innerHTML = "";
        spinnerTrack.style.transform = "translateX(0px)";

        window.setTimeout(async function () {
            try {
                idleCta.classList.add("hidden");
                spinnerTrack.classList.remove("hidden");
                pointer.classList.remove("hidden");

                const response = await fetch(chaosSpinUrl, {
                    method: "POST",
                    headers: {
                        "Accept": "application/json"
                    }
                });

                const payload = await response.json();

                if (!response.ok || !payload.ok) {
                    throw new Error(payload.message || "Failed to spin Chaos Draft packs.");
                }

                currentSpinResult = payload.spin_result;

                if (!currentSpinResult || !currentSpinResult.winning_pack || !currentSpinResult.chosen_variant) {
                    throw new Error("Chaos Draft spin result was incomplete.");
                }

                runSpinAnimation(currentSpinResult);
            } catch (error) {
                stopRouletteTicks();
                animationInProgress = false;
                message.classList.remove("hidden");
                spinnerTrack.classList.add("hidden");
                pointer.classList.add("hidden");
                idleCta.classList.remove("hidden");
                idleCta.classList.remove("chaos-draft-idle-cta-sinking");
                message.textContent = error.message || "Failed to spin Chaos Draft packs.";
                setButtonsForIdle();
            }
        }, 180);
    }

    async function runNext() {
        if (animationInProgress || openInProgress) {
            return;
        }

        stopRouletteTicks();
        hideBusyOverlay();
        currentWinningPack = null;
        currentSpinResult = null;
        currentPackSavedToDb = false;

        try {
            await fetch("/chaos-draft/next", {
                method: "POST",
                headers: {
                    "Accept": "application/json"
                }
            });
        } catch (error) {
        }

        spinnerTrack.innerHTML = "";
        spinnerTrack.style.transform = "translateX(0px)";
        spinnerShell.classList.remove("hidden");
        idleCta.classList.remove("hidden");
        spinnerTrack.classList.add("hidden");
        pointer.classList.add("hidden");
        message.classList.add("hidden");

        hideInlinePackView();
        closePackZoom();
        hideOpenRow();
        setButtonsForIdle();
    }

    spinCtaButton.addEventListener("click", function () {
        runSpin();
    });

    if (nextButton) {
        nextButton.addEventListener("click", function () {
            runNext();
        });
    }

    if (viewButton) {
        viewButton.addEventListener("click", async function () {
            if (!currentSpinResult || animationInProgress || openInProgress) {
                window.alert("No completed Chaos Draft spin is ready to view.");
                return;
            }

            try {
                viewButton.disabled = true;
                viewButton.classList.add("action-button-loading");
                viewButton.textContent = "Loading...";

                await loadInlinePackView();
            } catch (error) {
                window.alert(error.message || "Failed to load pack view.");
            } finally {
                viewButton.disabled = false;
                viewButton.classList.remove("action-button-loading");
                viewButton.textContent = "View";
            }
        });
    }

    if (printExportPrintButton) {
        printExportPrintButton.addEventListener("click", function () {
            if (
                autoSavePackToggle
                && autoSavePackToggle.checked
                && savePackButton
                && !currentPackSavedToDb
            ) {
                saveCurrentPackToDb(false).catch(function (saveError) {
                    console.error(saveError);
                    setSavePackButtonState(false, "Save Failed");

                    setTimeout(function () {
                        if (!currentPackSavedToDb) {
                            setSavePackButtonState(false, "Save");
                        }
                    }, 2200);
                });
            }
        });
    }

    if (savePackButton) {
        savePackButton.addEventListener("click", async function () {
            try {
                await saveCurrentPackToDb(false);
            } catch (error) {
                console.error(error);
                setSavePackButtonState(false, "Save Failed");

                setTimeout(function () {
                    if (!currentPackSavedToDb) {
                        setSavePackButtonState(false, "Save");
                    }
                }, 2200);
            }
        });
    }

    if (exportMainButton) {
        exportMainButton.addEventListener("click", async function () {
            try {
                await runSelectedExportAction();
            } catch (error) {
                window.alert(error.message || "Failed to export Chaos Draft pack.");
            }
        });
    }

    if (exportMenuButton) {
        exportMenuButton.addEventListener("click", function (event) {
            event.stopPropagation();
            toggleExportMenu();
        });
    }

    if (exportCopyButton) {
        exportCopyButton.addEventListener("click", async function () {
            setSelectedExportAction("copy");
            closeExportMenu();

            try {
                await runSelectedExportAction();
            } catch (error) {
                window.alert(error.message || "Failed to export Chaos Draft pack.");
            }
        });
    }

    if (exportSaveButton) {
        exportSaveButton.addEventListener("click", async function () {
            setSelectedExportAction("save");
            closeExportMenu();

            try {
                await runSelectedExportAction();
            } catch (error) {
                window.alert(error.message || "Failed to export Chaos Draft pack.");
            }
        });
    }

    document.addEventListener("click", function () {
        closeExportMenu();
    });

    if (exportMenu) {
        exportMenu.addEventListener("click", function (event) {
            event.stopPropagation();
        });
    }

    if (packZoomOverlay) {
        packZoomOverlay.addEventListener("click", function (event) {
            if (event.target && event.target.closest && event.target.closest(".chaos-pack-inline-flip-button")) {
                return;
            }

            closePackZoom();
        });
    }

    if (packZoomBackdrop) {
        packZoomBackdrop.addEventListener("click", function () {
            closePackZoom();
        });
    }

    document.addEventListener("keydown", function (event) {
        if (event.key === "Escape" && packZoomOverlay && !packZoomOverlay.classList.contains("hidden")) {
            closePackZoom();
        }
    });

    setSelectedExportAction("copy");

    spinnerShell.classList.remove("hidden");
    idleCta.classList.remove("hidden");
    idleCta.classList.remove("chaos-draft-idle-cta-sinking");
    spinnerTrack.classList.add("hidden");
    pointer.classList.add("hidden");
    message.classList.add("hidden");
    hideBusyOverlay();
    hideInlinePackView();
    closePackZoom();
    hideOpenRow();
    setButtonsForIdle();
}

/* ==========================================
   iMomir Toast + Confirm UI Helpers
   ========================================== */
(function () {
    if (window.iMomirToast && window.iMomirConfirm) {
        return;
    }

    function ensureToastHost() {
        let host = document.getElementById("imomirToastHost");

        if (host) {
            return host;
        }

        host = document.createElement("div");
        host.id = "imomirToastHost";
        host.style.position = "fixed";
        host.style.right = "18px";
        host.style.bottom = "18px";
        host.style.zIndex = "10050";
        host.style.display = "flex";
        host.style.flexDirection = "column";
        host.style.gap = "10px";
        host.style.width = "min(420px, calc(100vw - 36px))";

        document.body.appendChild(host);
        return host;
    }

    function showToast(message, type, timeoutMs) {
        const host = ensureToastHost();
        const cleanType = String(type || "info").trim().toLowerCase();

        const toast = document.createElement("div");
        toast.className = "imomir-toast imomir-toast-" + cleanType;
        toast.textContent = message || "";

        toast.style.border = "1px solid var(--border)";
        toast.style.borderRadius = "14px";
        toast.style.padding = "12px 14px";
        toast.style.boxShadow = "0 14px 32px rgba(0, 0, 0, 0.38)";
        toast.style.background = cleanType === "error"
            ? "linear-gradient(180deg, rgba(82, 30, 30, 0.98), rgba(48, 22, 22, 0.98))"
            : cleanType === "success"
                ? "linear-gradient(180deg, rgba(30, 82, 48, 0.98), rgba(22, 48, 32, 0.98))"
                : "linear-gradient(180deg, rgba(34, 38, 46, 0.98), rgba(26, 29, 36, 0.98))";
        toast.style.color = "#ffffff";
        toast.style.fontWeight = "700";
        toast.style.lineHeight = "1.35";
        toast.style.opacity = "0";
        toast.style.transform = "translateY(8px)";
        toast.style.transition = "opacity 0.16s ease, transform 0.16s ease";

        host.appendChild(toast);

        window.requestAnimationFrame(function () {
            toast.style.opacity = "1";
            toast.style.transform = "translateY(0)";
        });

        window.setTimeout(function () {
            toast.style.opacity = "0";
            toast.style.transform = "translateY(8px)";

            window.setTimeout(function () {
                toast.remove();

                if (host.children.length === 0) {
                    host.remove();
                }
            }, 180);
        }, Number(timeoutMs) || 2800);
    }

    function ensureConfirmModal() {
        let overlay = document.getElementById("imomirConfirmOverlay");

        if (overlay) {
            return overlay;
        }

        overlay = document.createElement("div");
        overlay.id = "imomirConfirmOverlay";
        overlay.className = "hidden";
        overlay.setAttribute("aria-hidden", "true");

        overlay.innerHTML = [
            '<div id="imomirConfirmBackdrop"></div>',
            '<div id="imomirConfirmDialog" role="dialog" aria-modal="true" aria-labelledby="imomirConfirmTitle">',
            '  <div id="imomirConfirmTitle"></div>',
            '  <div id="imomirConfirmMessage"></div>',
            '  <div id="imomirConfirmActions">',
            '    <button type="button" id="imomirConfirmCancelButton" class="action-button secondary-button">Cancel</button>',
            '    <button type="button" id="imomirConfirmOkButton" class="action-button">Continue</button>',
            '  </div>',
            '</div>'
        ].join("");

        overlay.style.position = "fixed";
        overlay.style.inset = "0";
        overlay.style.zIndex = "10040";
        overlay.style.display = "none";
        overlay.style.alignItems = "center";
        overlay.style.justifyContent = "center";
        overlay.style.padding = "22px";

        const backdrop = overlay.querySelector("#imomirConfirmBackdrop");
        backdrop.style.position = "absolute";
        backdrop.style.inset = "0";
        backdrop.style.background = "rgba(0, 0, 0, 0.72)";
        backdrop.style.backdropFilter = "blur(3px)";

        const dialog = overlay.querySelector("#imomirConfirmDialog");
        dialog.style.position = "relative";
        dialog.style.zIndex = "1";
        dialog.style.width = "min(100%, 440px)";
        dialog.style.border = "1px solid var(--border)";
        dialog.style.borderRadius = "20px";
        dialog.style.background = "linear-gradient(180deg, rgba(34, 38, 46, 0.98), rgba(26, 29, 36, 0.98))";
        dialog.style.boxShadow = "0 22px 60px rgba(0, 0, 0, 0.48)";
        dialog.style.padding = "22px";

        const title = overlay.querySelector("#imomirConfirmTitle");
        title.style.fontSize = "1.35rem";
        title.style.fontWeight = "800";
        title.style.marginBottom = "8px";
        title.style.color = "var(--text)";

        const message = overlay.querySelector("#imomirConfirmMessage");
        message.style.color = "var(--muted)";
        message.style.lineHeight = "1.45";
        message.style.marginBottom = "18px";

        const actions = overlay.querySelector("#imomirConfirmActions");
        actions.style.display = "grid";
        actions.style.gridTemplateColumns = "1fr 1fr";
        actions.style.gap = "12px";

        document.body.appendChild(overlay);
        return overlay;
    }

    function showConfirm(options) {
        const overlay = ensureConfirmModal();
        const title = overlay.querySelector("#imomirConfirmTitle");
        const message = overlay.querySelector("#imomirConfirmMessage");
        const cancelButton = overlay.querySelector("#imomirConfirmCancelButton");
        const okButton = overlay.querySelector("#imomirConfirmOkButton");
        const backdrop = overlay.querySelector("#imomirConfirmBackdrop");

        const settings = options || {};

        title.textContent = settings.title || "Confirm Action";
        message.textContent = settings.message || "Continue?";
        cancelButton.textContent = settings.cancelText || "Cancel";
        okButton.textContent = settings.confirmText || "Continue";

        okButton.classList.toggle("campaign-danger-button", Boolean(settings.danger));

        overlay.classList.remove("hidden");
        overlay.style.display = "flex";
        overlay.setAttribute("aria-hidden", "false");

        return new Promise(function (resolve) {
            let resolved = false;

            function close(result) {
                if (resolved) {
                    return;
                }

                resolved = true;

                overlay.classList.add("hidden");
                overlay.style.display = "none";
                overlay.setAttribute("aria-hidden", "true");

                cancelButton.removeEventListener("click", onCancel);
                okButton.removeEventListener("click", onOk);
                backdrop.removeEventListener("click", onCancel);
                document.removeEventListener("keydown", onKeyDown);

                resolve(result);
            }

            function onCancel() {
                close(false);
            }

            function onOk() {
                close(true);
            }

            function onKeyDown(event) {
                if (event.key === "Escape") {
                    close(false);
                }
            }

            cancelButton.addEventListener("click", onCancel);
            okButton.addEventListener("click", onOk);
            backdrop.addEventListener("click", onCancel);
            document.addEventListener("keydown", onKeyDown);

            okButton.focus();
        });
    }

    window.iMomirToast = {
        show: showToast,
        success: function (message, timeoutMs) {
            showToast(message, "success", timeoutMs);
        },
        error: function (message, timeoutMs) {
            showToast(message, "error", timeoutMs);
        },
        info: function (message, timeoutMs) {
            showToast(message, "info", timeoutMs);
        }
    };

    window.iMomirConfirm = {
        show: showConfirm
    };
})();

(function () {
    const appTabs = document.querySelector(".app-tabs");
    const menuToggleButton = document.getElementById("appMenuToggleButton");

    if (!appTabs || !menuToggleButton) {
        return;
    }

    const menuStorageKey = "imomir-main-menu-collapsed";

    function isMenuCollapsed() {
        return appTabs.classList.contains("app-tabs-collapsed");
    }

    function updateWorkspaceNavigationOffset() {
        /*
         * A collapsed navigation floats over the upper-left corner rather
         * than reserving vertical space for the complete navigation bar.
         */
        if (isMenuCollapsed()) {
            document.documentElement.style.setProperty(
                "--imomir-workspace-nav-offset",
                "0px"
            );

            return;
        }

        const tabsRect = appTabs.getBoundingClientRect();
        const navigationBottom = Math.ceil(tabsRect.bottom + 8);

        document.documentElement.style.setProperty(
            "--imomir-workspace-nav-offset",
            navigationBottom + "px"
        );
    }

    function updateMenuAccessibilityState() {
        const collapsed = isMenuCollapsed();

        menuToggleButton.setAttribute(
            "aria-expanded",
            collapsed ? "false" : "true"
        );

        menuToggleButton.setAttribute(
            "aria-label",
            collapsed ? "Expand navigation" : "Minimize navigation"
        );

        menuToggleButton.title = collapsed
            ? "Expand Navigation"
            : "Minimize Navigation";
    }

    function setMenuCollapsed(collapsed, savePreference) {
        appTabs.classList.toggle(
            "app-tabs-collapsed",
            Boolean(collapsed)
        );

        document.body.classList.toggle(
            "app-menu-is-collapsed",
            Boolean(collapsed)
        );

        updateMenuAccessibilityState();

        /*
         * Wait for the class change to be applied before measuring the
         * expanded menu's size.
         */
        window.requestAnimationFrame(function () {
            updateWorkspaceNavigationOffset();
        });

        if (savePreference) {
            try {
                window.localStorage.setItem(
                    menuStorageKey,
                    collapsed ? "1" : "0"
                );
            } catch (error) {
                /*
                 * The menu still works when localStorage is unavailable.
                 */
            }
        }
    }

    function loadSavedMenuState() {
        let savedValue = "0";

        try {
            savedValue = window.localStorage.getItem(menuStorageKey) || "0";
        } catch (error) {
            savedValue = "0";
        }

        setMenuCollapsed(savedValue === "1", false);
    }

    menuToggleButton.addEventListener("click", function () {
        setMenuCollapsed(!isMenuCollapsed(), true);
    });

    window.addEventListener("resize", function () {
        updateWorkspaceNavigationOffset();
    });

    if (window.ResizeObserver) {
        const navigationResizeObserver = new ResizeObserver(function () {
            updateWorkspaceNavigationOffset();
        });

        navigationResizeObserver.observe(appTabs);
    }

    loadSavedMenuState();
})();