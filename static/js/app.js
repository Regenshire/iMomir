document.addEventListener("DOMContentLoaded", function () {
    initializeManaKeypad();
    initializeSetsPage();
    initializeRefreshCards();
    initializeConfigPanels();
    initializeGameModeCards();
    initializeConfigShortcutNavigation();
    initializeResultCardZoom();
    initializeMomirSelectResultLinks();
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
    const openButton = document.getElementById("chaosOpenButton");
    const nextButton = document.getElementById("chaosNextButton");
    const openRow = document.getElementById("chaosDraftOpenRow");
    const spinnerShell = document.getElementById("chaosDraftSpinner");
    const spinnerTrack = document.getElementById("chaosDraftSpinnerTrack");
    const idleCta = document.getElementById("chaosDraftIdleCta");
    const spinCtaButton = document.getElementById("chaosSpinButton");
    const pointer = document.getElementById("chaosDraftPointer");
    const message = document.getElementById("chaosDraftMessage");
    const chaosDraftScreen = document.getElementById("chaosDraftScreen");
    const busyOverlay = document.getElementById("chaosDraftBusyOverlay");
    const busyTitle = document.getElementById("chaosDraftBusyTitle");
    const busyText = document.getElementById("chaosDraftBusyText");
    const openPrintInNewTab = chaosDraftScreen
        ? chaosDraftScreen.getAttribute("data-open-print-in-new-tab") === "1"
        : true;

    const soundEnabled = chaosDraftScreen
        ? chaosDraftScreen.getAttribute("data-sound-enabled") === "1"
        : true;

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

    function resetOpenPackUiState() {
        hideBusyOverlay();

        openInProgress = false;
        openAbortController = null;

        if (openButton) {
            openButton.disabled = false;
            openButton.classList.remove("action-button-loading");
            openButton.textContent = "Open Pack";
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

    function hideOpenRow() {
        if (openRow) {
            openRow.classList.remove("chaos-draft-open-row-visible");
        }

        if (openButton) {
            openButton.disabled = true;
            openButton.classList.remove("action-button-loading");
            openButton.textContent = "Open Pack";
        }
    }

    function showOpenRow() {
        if (openRow) {
            openRow.classList.add("chaos-draft-open-row-visible");
        }

        if (openButton) {
            openButton.disabled = false;
            openButton.classList.remove("action-button-loading");
            openButton.textContent = "Open Pack";
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

    function animateTrackToTarget(finalAbsoluteIndex) {
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
        const durationMs = 7600 + Math.round(Math.random() * 1100);

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

                const response = await fetch("/chaos-draft/spin", {
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

    if (openButton) {
        openButton.addEventListener("click", async function () {
            const openTimeoutMs = 30000;
            let timeoutId = null;

            try {
                if (!currentSpinResult || animationInProgress || openInProgress) {
                    throw new Error("No completed Chaos Draft spin is ready to open.");
                }

                openInProgress = true;
                openAbortController = new AbortController();

                openButton.disabled = true;
                spinCtaButton.disabled = true;

                if (nextButton) {
                    nextButton.disabled = true;
                }

                openButton.classList.add("action-button-loading");
                openButton.textContent = "Opening Pack...";

                showBusyOverlay(
                    "Opening Pack",
                    "Building your Chaos Draft pack PDF. Large or special packs can take several seconds."
                );

                timeoutId = window.setTimeout(function () {
                    if (openAbortController) {
                        openAbortController.abort();
                    }
                }, openTimeoutMs);

                const response = await fetch("/chaos-draft/open", {
                    method: "POST",
                    headers: {
                        "Accept": "application/json"
                    },
                    signal: openAbortController.signal
                });

                const payload = await response.json();

                if (!response.ok || !payload.ok || !payload.download_url) {
                    throw new Error(payload.message || "Failed to open pack.");
                }

                hideBusyOverlay();

                if (openPrintInNewTab) {
                    resetOpenPackUiState();
                    window.open(payload.download_url, "_blank", "noopener");
                } else {
                    resetOpenPackUiState();
                    window.location.href = payload.download_url;
                }
            } catch (error) {
                const wasUserCancel =
                    error &&
                    error.name === "AbortError" &&
                    !openInProgress;

                hideBusyOverlay();

                if (!wasUserCancel) {
                    const errorMessage = error && error.name === "AbortError"
                        ? "Opening the pack timed out after 30 seconds."
                        : (error.message || "Failed to open Chaos Draft pack.");

                    window.alert(errorMessage);
                }

                resetOpenPackUiState();
            } finally {
                if (timeoutId) {
                    window.clearTimeout(timeoutId);
                }
            }
        });
    }

    spinnerShell.classList.remove("hidden");
    idleCta.classList.remove("hidden");
    idleCta.classList.remove("chaos-draft-idle-cta-sinking");
    spinnerTrack.classList.add("hidden");
    pointer.classList.add("hidden");
    message.classList.add("hidden");
    hideBusyOverlay();
    hideOpenRow();
    setButtonsForIdle();
}