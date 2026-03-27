document.addEventListener("DOMContentLoaded", function () {
    initializeManaKeypad();
    initializeSetsPage();
    initializeRefreshCards();
    initializeConfigPanels();
    initializeGameModeCards();
    initializeConfigShortcutNavigation();
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
    const importCardsCount = document.getElementById("importCardsCount");
    const importSetsCount = document.getElementById("importSetsCount");
    const importLastRefresh = document.getElementById("importLastRefresh");
    const importSourceLastUpdated = document.getElementById("importSourceLastUpdated");

    const downloadCardImagesButton = document.getElementById("downloadCardImagesButton");
    const redownloadCardImagesButton = document.getElementById("redownloadCardImagesButton");
    const clearHistoryButton = document.getElementById("clearHistoryButton");
    const historyCount = document.getElementById("historyCount");
    const imageDownloadSpinner = document.getElementById("imageDownloadSpinner");
    const imageDownloadStage = document.getElementById("imageDownloadStage");
    const imageDownloadMessage = document.getElementById("imageDownloadMessage");
    const imageDownloadError = document.getElementById("imageDownloadError");
    const imageCardsProcessed = document.getElementById("imageCardsProcessed");
    const imageCardsDownloaded = document.getElementById("imageCardsDownloaded");
    const imageCardsDisabled = document.getElementById("imageCardsDisabled");
    const imageDownloadFinishedAt = document.getElementById("imageDownloadFinishedAt");

    if (!refreshButton) {
        return;
    }

    let refreshPollTimer = null;
    let imagePollTimer = null;
    let lastRefreshFinishedAtPrompted = null;

    function setRefreshRunningUi(isRunning) {
        refreshButton.disabled = isRunning;

        if (forcedRefreshButton) {
            forcedRefreshButton.disabled = isRunning;
        }

        if (refreshSpinner) {
            refreshSpinner.classList.toggle("hidden", !isRunning);
        }
    }

    function setImageRunningUi(isRunning) {
        if (downloadCardImagesButton) {
            downloadCardImagesButton.disabled = isRunning;
        }

        if (redownloadCardImagesButton) {
            redownloadCardImagesButton.disabled = isRunning;
        }

        if (imageDownloadSpinner) {
            imageDownloadSpinner.classList.toggle("hidden", !isRunning);
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

        setRefreshRunningUi(Boolean(status.is_running));
    }

    function applyImageStatus(status) {
        if (imageDownloadStage) {
            imageDownloadStage.textContent = status.stage || "Idle";
        }

        if (imageDownloadMessage) {
            imageDownloadMessage.textContent = status.message || "";
        }

        if (imageDownloadError) {
            if (status.error) {
                imageDownloadError.textContent = status.error;
                imageDownloadError.classList.remove("hidden");
            } else {
                imageDownloadError.textContent = "";
                imageDownloadError.classList.add("hidden");
            }
        }

        if (imageCardsProcessed && status.cards_processed !== undefined) {
            imageCardsProcessed.textContent = String(status.cards_processed);
        }

        if (imageCardsDownloaded && status.cards_downloaded !== undefined) {
            imageCardsDownloaded.textContent = String(status.cards_downloaded);
        }

        if (imageCardsDisabled && status.cards_disabled !== undefined) {
            imageCardsDisabled.textContent = String(status.cards_disabled);
        }

        if (imageDownloadFinishedAt && status.finished_at) {
            imageDownloadFinishedAt.textContent = status.finished_at;
        }

        setImageRunningUi(Boolean(status.is_running));
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

    async function fetchImageStatus() {
        const response = await fetch("/download-card-images/status", {
            method: "GET",
            headers: {
                "Accept": "application/json"
            }
        });

        if (!response.ok) {
            throw new Error("Failed to get card image download status.");
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
                !status.is_running &&
                status.stage === "Complete" &&
                status.finished_at &&
                status.finished_at !== lastRefreshFinishedAtPrompted
            ) {
                lastRefreshFinishedAtPrompted = status.finished_at;

                // Intentionally do not prompt for bulk image download here.
                // Card images are downloaded on demand as cards are viewed or printed.
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

    async function pollImageStatus() {
        try {
            const status = await fetchImageStatus();
            applyImageStatus(status);

            if (!status.is_running && imagePollTimer) {
                clearInterval(imagePollTimer);
                imagePollTimer = null;
            }
        } catch (error) {
            if (imageDownloadError) {
                imageDownloadError.textContent = error.message;
                imageDownloadError.classList.remove("hidden");
            }

            setImageRunningUi(false);

            if (imagePollTimer) {
                clearInterval(imagePollTimer);
                imagePollTimer = null;
            }
        }
    }

    async function startRefresh(forceDownload) {
        try {
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

    async function startImageDownload(forceRedownload) {
        try {
            setImageRunningUi(true);

            if (imageDownloadError) {
                imageDownloadError.textContent = "";
                imageDownloadError.classList.add("hidden");
            }

            if (imageDownloadStage) {
                imageDownloadStage.textContent = "Starting";
            }

            if (imageDownloadMessage) {
                imageDownloadMessage.textContent = forceRedownload
                    ? "Starting full image redownload..."
                    : "Starting missing image download...";
            }

            const response = await fetch("/download-card-images/start", {
                method: "POST",
                headers: {
                    "Accept": "application/json",
                    "Content-Type": "application/json"
                },
                body: JSON.stringify({
                    force_redownload: forceRedownload
                })
            });

            const payload = await response.json();

            if (!response.ok || !payload.ok) {
                throw new Error(payload.message || "Failed to start card image download.");
            }

            await pollImageStatus();

            if (!imagePollTimer) {
                imagePollTimer = setInterval(pollImageStatus, 1000);
            }
        } catch (error) {
            setImageRunningUi(false);

            if (imageDownloadError) {
                imageDownloadError.textContent = error.message;
                imageDownloadError.classList.remove("hidden");
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

    if (downloadCardImagesButton) {
        downloadCardImagesButton.addEventListener("click", async function () {
            await startImageDownload(false);
        });
    }

    if (redownloadCardImagesButton) {
        redownloadCardImagesButton.addEventListener("click", async function () {
            const confirmed = window.confirm(
                "ReDownload Card Images will reprocess all matching cards and can take a long time.\n\nContinue?"
            );

            if (!confirmed) {
                return;
            }

            await startImageDownload(true);
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
    pollImageStatus();
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
    const hiddenInput = document.getElementById("game_mode");
    const cardButtons = document.querySelectorAll(".game-mode-card");
    const printButton = document.getElementById("printSelectedTokenButton");

    if (!hiddenInput || !cardButtons.length) {
        return;
    }

    function applySelection(selectedValue, selectedPrintHref) {
        hiddenInput.value = selectedValue;

        cardButtons.forEach(function (button) {
            const isSelected = button.getAttribute("data-mode-value") === selectedValue;
            button.classList.toggle("game-mode-card-selected", isSelected);
        });

        if (printButton && selectedPrintHref) {
            printButton.setAttribute("href", selectedPrintHref);
        }
    }

    cardButtons.forEach(function (button) {
        button.addEventListener("click", function () {
            const selectedValue = button.getAttribute("data-mode-value") || "custom";
            const selectedPrintHref = button.getAttribute("data-mode-print-href") || "";
            applySelection(selectedValue, selectedPrintHref);
        });
    });
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