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
    initializeAppNavigationMenus();
    initializeAlternateBleedReprocessing();
    initializeChaosDraftPage();
});

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