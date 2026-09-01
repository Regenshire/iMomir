(function () {
    const campaignPackDetailScreen = document.getElementById(
        "campaignPackDetailScreen"
    );

    const zoomOverlay = document.getElementById("campaignPackZoomOverlay");
    const zoomBackdrop = document.getElementById("campaignPackZoomBackdrop");
    const zoomImage = document.getElementById("campaignPackZoomImage");
    const zoomableImages = Array.from(document.querySelectorAll(".campaign-pack-detail-zoomable"));
    const changePrintingButtons = Array.from(document.querySelectorAll(".campaign-pack-change-printing-button"));
    const changePrintingOverlay = document.getElementById("campaignPackChangePrintingOverlay");
    const changePrintingCloseButton = document.getElementById("campaignPackChangePrintingCloseButton");
    const changePrintingSubtitle = document.getElementById("campaignPackChangePrintingSubtitle");
    const changePrintingStatus = document.getElementById("campaignPackChangePrintingStatus");
    const changePrintingResults = document.getElementById("campaignPackChangePrintingResults");

    const batchUpscaleButton = document.getElementById(
        "campaignPackBatchUpscaleButton"
    );

    const printExportButton = document.getElementById(
        "campaignPackOpenPrintExportButton"
    );

    let activeChangePrintingState = null;

    function showCampaignPackMessage(messageText, isError) {
        const cleanMessage = messageText || "";

        if (!cleanMessage) {
            return;
        }

        if (window.iMomirToast) {
            if (isError) {
                window.iMomirToast.error(cleanMessage);
            } else {
                window.iMomirToast.success(cleanMessage);
            }
            return;
        }

        console.log(cleanMessage);
    }

    if (printExportButton && window.iMomirPrintExportModal) {
        window.iMomirPrintExportModal.init({
            openButtonId: "campaignPackOpenPrintExportButton",
            printUrl: printExportButton.dataset.printUrl || "",
            exportZipUrl: printExportButton.dataset.exportZipUrl || "",
            showMessage: showCampaignPackMessage
        });
    }

    function getCampaignPackBatchUpscaleCardUuids() {
        const seenCardUuids = new Set();

        const cardUuids = [];

        Array.from(
            document.querySelectorAll(
                ".chaos-pack-view-card[data-card-uuid]"
            )
        ).forEach(function (cardElement) {
            const cardUuid = String(
                cardElement.dataset.cardUuid
                || ""
            ).trim();

            if (
                !cardUuid
                || seenCardUuids.has(
                    cardUuid
                )
            ) {
                return;
            }

            seenCardUuids.add(
                cardUuid
            );

            cardUuids.push(
                cardUuid
            );
        });

        return cardUuids;
    }

    function setChangePrintingStatus(messageText, isError) {
        if (!changePrintingStatus) {
            return;
        }

        const cleanMessage = String(messageText || "").trim();

        changePrintingStatus.textContent = cleanMessage;
        changePrintingStatus.classList.toggle("custom-draft-card-search-status-error", Boolean(isError));
        changePrintingStatus.classList.toggle("hidden", !cleanMessage);
    }

    function closeChangePrintingModal() {
        if (!changePrintingOverlay) {
            return;
        }

        changePrintingOverlay.classList.add("hidden");
        changePrintingOverlay.setAttribute("aria-hidden", "true");
        activeChangePrintingState = null;

        if (changePrintingResults) {
            changePrintingResults.innerHTML = "";
            changePrintingResults.classList.add("hidden");
        }

        setChangePrintingStatus("", false);
    }

    function openChangePrintingModal(button) {
        if (!button || !changePrintingOverlay) {
            return;
        }

        activeChangePrintingState = {
            cardName: button.dataset.cardName || "",
            currentCardUuid: button.dataset.currentCardUuid || "",
            printingOptionsUrl: button.dataset.printingOptionsUrl || "",
            updateUrl: button.dataset.updateUrl || ""
        };

        if (!activeChangePrintingState.cardName || !activeChangePrintingState.printingOptionsUrl || !activeChangePrintingState.updateUrl) {
            return;
        }

        if (changePrintingSubtitle) {
            changePrintingSubtitle.textContent = "Choose a replacement printing for " + activeChangePrintingState.cardName + ".";
        }

        if (changePrintingResults) {
            changePrintingResults.innerHTML = "";
            changePrintingResults.classList.add("hidden");
        }

        setChangePrintingStatus("Loading printings...", false);

        changePrintingOverlay.classList.remove("hidden");
        changePrintingOverlay.setAttribute("aria-hidden", "false");

        loadChangePrintingOptions();
    }

    function getCampaignPackReleaseDateSortValue(card) {
        const rawDate = card && card.release_date ? String(card.release_date).trim() : "";

        if (!rawDate) {
            const rawYear = card && card.release_year ? String(card.release_year).trim() : "";
            const parsedYear = Number(rawYear);

            if (!Number.isFinite(parsedYear)) {
                return 0;
            }

            return parsedYear * 10000;
        }

        const cleanDate = rawDate.replace(/[^0-9]/g, "");
        const parsedDate = Number(cleanDate);

        if (!Number.isFinite(parsedDate)) {
            return 0;
        }

        return parsedDate;
    }

    function renderChangePrintingOptions(cards) {
        if (!changePrintingResults) {
            return;
        }

        changePrintingResults.innerHTML = "";

        if (!cards || !cards.length) {
            const emptyRow = document.createElement("div");
            emptyRow.className = "custom-draft-card-search-empty";
            emptyRow.textContent = "No alternate printings found.";
            changePrintingResults.appendChild(emptyRow);
            changePrintingResults.classList.remove("hidden");
            return;
        }

        const sortedCards = cards.slice().sort(function (cardA, cardB) {
            const cardACurrent = cardA.card_uuid === activeChangePrintingState.currentCardUuid;
            const cardBCurrent = cardB.card_uuid === activeChangePrintingState.currentCardUuid;

            if (cardACurrent !== cardBCurrent) {
                return cardACurrent ? -1 : 1;
            }

            return getCampaignPackReleaseDateSortValue(cardB) - getCampaignPackReleaseDateSortValue(cardA);
        });

        sortedCards.forEach(function (card) {
            const row = document.createElement("div");
            row.className = "custom-draft-card-search-row custom-draft-card-search-printing-row";

            const isCurrentPrinting = card.card_uuid === activeChangePrintingState.currentCardUuid;

            if (isCurrentPrinting) {
                row.classList.add("custom-draft-card-search-row-disabled");
            }

            const imageWrap = document.createElement("div");
            imageWrap.className = "custom-draft-card-search-image-wrap";

            const image = document.createElement("img");
            image.className = "custom-draft-card-search-image";
            image.src = card.image_src || "";
            image.alt = card.card_name || "Card";

            imageWrap.appendChild(image);

            const main = document.createElement("div");
            main.className = "custom-draft-card-search-main";

            const title = document.createElement("div");
            title.className = "custom-draft-card-search-title";
            title.textContent = card.card_name || "Unknown Card";

            const meta = document.createElement("div");
            meta.className = "custom-draft-card-search-meta";

            const metaLineOne = document.createElement("div");
            metaLineOne.className = "custom-draft-card-search-meta-line";
            metaLineOne.textContent = [
                card.set_code || "",
                card.release_year ? card.release_year : "",
                card.collector_number ? "#" + card.collector_number : "",
                card.rarity || "",
                card.type_line || ""
            ].filter(Boolean).join(" • ");

            meta.appendChild(metaLineOne);
            main.appendChild(title);
            main.appendChild(meta);

            const actions = document.createElement("div");
            actions.className = "custom-draft-card-search-actions";

            const switchButton = document.createElement("button");
            switchButton.type = "button";
            switchButton.className = "action-button secondary-button custom-draft-card-add-button";
            switchButton.textContent = isCurrentPrinting ? "Current" : "Switch";
            switchButton.disabled = Boolean(isCurrentPrinting);

            switchButton.addEventListener("click", async function () {
                await changeCampaignPackCardPrinting(card, switchButton);
            });

            actions.appendChild(switchButton);

            row.appendChild(imageWrap);
            row.appendChild(main);
            row.appendChild(actions);

            changePrintingResults.appendChild(row);
        });

        changePrintingResults.classList.remove("hidden");
    }

    async function loadChangePrintingOptions() {
        if (!activeChangePrintingState || !activeChangePrintingState.printingOptionsUrl) {
            return;
        }

        try {
            const response = await fetch(activeChangePrintingState.printingOptionsUrl, {
                method: "GET",
                headers: {
                    "Accept": "application/json"
                }
            });

            const payload = await response.json();

            if (!response.ok || !payload.ok) {
                throw new Error(payload.message || "Could not load printings.");
            }

            renderChangePrintingOptions(payload.results || []);
            setChangePrintingStatus("Loaded " + ((payload.results || []).length) + " printing(s).", false);
        } catch (error) {
            console.error(error);
            setChangePrintingStatus(error.message || "Could not load printings.", true);
        }
    }

    async function changeCampaignPackCardPrinting(card, button) {
        if (!activeChangePrintingState || !card || !card.card_uuid || !button) {
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
                    card_uuid: card.card_uuid
                })
            });

            const payload = await response.json();

            if (!response.ok || !payload.ok) {
                throw new Error(payload.message || "Failed to change printing.");
            }

            button.classList.remove("action-button-loading");
            button.textContent = "Switched";

            closeChangePrintingModal();
            showCampaignPackMessage(payload.message || "Printing updated.", false);

            window.setTimeout(function () {
                window.location.reload();
            }, 250);
        } catch (error) {
            console.error(error);
            button.disabled = false;
            button.classList.remove("action-button-loading");
            button.textContent = "Switch";
            setChangePrintingStatus(error.message || "Failed to change printing.", true);
        }
    }

    function updateCampaignPackFoilBadge(cardUuid, isFoil) {
        if (!cardUuid) {
            return;
        }

        const matchingCards = Array.from(document.querySelectorAll(
            '.chaos-pack-view-card[data-card-uuid="' + CSS.escape(cardUuid) + '"]'
        ));

        matchingCards.forEach(function (cardElement) {
            cardElement.dataset.isFoil = isFoil ? "1" : "0";

            const gearButton = cardElement.querySelector(".alternate-image-button");
            if (gearButton) {
                gearButton.dataset.isFoil = isFoil ? "1" : "0";
            }

            let foilBadge = cardElement.querySelector(".campaign-pack-foil-badge");

            if (isFoil) {
                if (!foilBadge) {
                    const badgeWrap = document.createElement("div");
                    badgeWrap.className = "chaos-pack-view-badges campaign-pack-foil-badge";

                    const badge = document.createElement("span");
                    badge.className = "chaos-pack-view-badge";
                    badge.textContent = "Foil";

                    badgeWrap.appendChild(badge);

                    const infoWrap = cardElement.querySelector(".chaos-pack-view-info");
                    const existingBadges = infoWrap ? infoWrap.querySelector(".chaos-pack-view-badges") : null;

                    if (infoWrap && existingBadges) {
                        infoWrap.insertBefore(badgeWrap, existingBadges);
                    } else if (infoWrap) {
                        infoWrap.appendChild(badgeWrap);
                    }
                }
            } else if (foilBadge) {
                foilBadge.remove();
            }
        });
    }

    document.addEventListener("imomir:card-image-refreshed", function (event) {
        const detail = event.detail || {};

        if (detail.isFoil === null || detail.isFoil === undefined) {
            return;
        }

        updateCampaignPackFoilBadge(detail.cardUuid || "", Boolean(detail.isFoil));
    });

    const cardFaceDataUrl = campaignPackDetailScreen
        ? campaignPackDetailScreen.dataset.cardFaceDataUrl || ""
        : "";

    const packDisplayName = campaignPackDetailScreen
        ? campaignPackDetailScreen.dataset.packDisplayName || "this pack"
        : "this pack";

    const campaignPackFaceMetadataByUuid = {};
    let activeZoomSourceImage = null;
    let activeZoomFlipButton = null;

    function getCardUuidForCampaignPackImage(imageElement) {
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

    function getCampaignPackFaceMetadata(imageElement) {
        const cardUuid = getCardUuidForCampaignPackImage(imageElement);

        if (!cardUuid) {
            return null;
        }

        return campaignPackFaceMetadataByUuid[cardUuid] || null;
    }

    function setCampaignPackImageFace(imageElement, metadata, faceName, flipButton) {
        if (!imageElement || !metadata) {
            return;
        }

        const targetFace = faceName === "back" ? "back" : "front";
        const targetSrc = targetFace === "back" ? metadata.back_src : metadata.front_src;
        const targetAlt = targetFace === "back" ? metadata.back_alt : metadata.front_alt;

        if (!targetSrc) {
            return;
        }

        imageElement.classList.add("campaign-pack-card-flipping");

        window.setTimeout(function () {
            imageElement.src = targetSrc;
            imageElement.alt = targetAlt || imageElement.alt || "";
            imageElement.dataset.currentFace = targetFace;

            if (targetFace === "back") {
                flipButton?.classList.add("campaign-pack-flip-button-flipped");
                flipButton?.setAttribute("aria-label", "Show front face");
                flipButton?.setAttribute("title", "Show front face");
            } else {
                flipButton?.classList.remove("campaign-pack-flip-button-flipped");
                flipButton?.setAttribute("aria-label", "Show back face");
                flipButton?.setAttribute("title", "Show back face");
            }
        }, 120);

        window.setTimeout(function () {
            imageElement.classList.remove("campaign-pack-card-flipping");
        }, 300);
    }

    function flipCampaignPackImage(imageElement, flipButton) {
        const metadata = getCampaignPackFaceMetadata(imageElement);

        if (!metadata || !metadata.is_dual_faced || !metadata.back_src) {
            return;
        }

        const currentFace = imageElement.dataset.currentFace === "back" ? "back" : "front";
        const nextFace = currentFace === "back" ? "front" : "back";

        setCampaignPackImageFace(imageElement, metadata, nextFace, flipButton);

        if (imageElement === zoomImage && activeZoomSourceImage) {
            const sourceButton = activeZoomSourceImage
                .closest(".chaos-pack-view-image-wrap")
                ?.querySelector(".campaign-pack-flip-button");

            setCampaignPackImageFace(activeZoomSourceImage, metadata, nextFace, sourceButton);
        }
    }

    function createCampaignPackFlipButton(imageElement) {
        const metadata = getCampaignPackFaceMetadata(imageElement);

        if (!metadata || !metadata.is_dual_faced || !metadata.back_src) {
            return null;
        }

        const button = document.createElement("button");
        button.type = "button";
        button.className = "campaign-pack-flip-button";
        button.innerHTML = '<i class="fa-solid fa-rotate"></i>';
        button.setAttribute("aria-label", "Show back face");
        button.setAttribute("title", "Show back face");

        button.addEventListener("click", function (event) {
            event.preventDefault();
            event.stopPropagation();
            flipCampaignPackImage(imageElement, button);
        });

        return button;
    }

    function addCampaignPackFlipButtonToCard(imageElement) {
        const metadata = getCampaignPackFaceMetadata(imageElement);

        if (!metadata || !metadata.is_dual_faced || !metadata.back_src) {
            return;
        }

        const imageWrap = imageElement.closest(".chaos-pack-view-image-wrap");

        if (!imageWrap || imageWrap.querySelector(".campaign-pack-flip-button")) {
            return;
        }

        imageWrap.classList.add("campaign-pack-flip-host");
        imageElement.dataset.currentFace = "front";

        const button = createCampaignPackFlipButton(imageElement);

        if (button) {
            imageWrap.appendChild(button);
        }
    }

    async function loadCampaignPackFaceMetadata() {
        const cardUuids = [];

        zoomableImages.forEach(function (imageElement) {
            const cardUuid = getCardUuidForCampaignPackImage(imageElement);

            if (cardUuid && !cardUuids.includes(cardUuid)) {
                cardUuids.push(cardUuid);
            }
        });

        if (!cardUuids.length) {
            console.warn("Campaign Pack flip: no card UUIDs found.");
            return;
        }

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
            console.warn("Campaign Pack flip: card-face-data failed.", payload);
            return;
        }

        Object.assign(campaignPackFaceMetadataByUuid, payload.chaos_cards || {});

        zoomableImages.forEach(function (imageElement) {
            const cardUuid = getCardUuidForCampaignPackImage(imageElement);
            const metadata = campaignPackFaceMetadataByUuid[cardUuid];

            if (metadata && metadata.is_dual_faced && metadata.back_src) {
                imageElement.dataset.currentFace = "front";
                addCampaignPackFlipButtonToCard(imageElement);
            }
        });
    }

    if (zoomOverlay && zoomBackdrop && zoomImage && zoomableImages.length) {
        function openZoom(imageElement) {
            const metadata = getCampaignPackFaceMetadata(imageElement);
            const currentFace = imageElement.dataset.currentFace === "back" ? "back" : "front";
            const imageSrc = currentFace === "back" && metadata
                ? metadata.back_src
                : (imageElement.src || imageElement.getAttribute("data-zoom-src") || "");
            const imageAlt = currentFace === "back" && metadata
                ? metadata.back_alt
                : (imageElement.alt || imageElement.getAttribute("data-zoom-alt") || "");

            if (!imageSrc) {
                return;
            }

            activeZoomSourceImage = imageElement;

            zoomImage.src = imageSrc;
            zoomImage.alt = imageAlt;
            zoomImage.dataset.cardUuid = getCardUuidForCampaignPackImage(imageElement);
            zoomImage.dataset.currentFace = currentFace;

            if (activeZoomFlipButton) {
                activeZoomFlipButton.remove();
                activeZoomFlipButton = null;
            }

            const zoomContent = zoomImage.closest(".card-zoom-content");

            if (zoomContent) {
                zoomContent.classList.add("campaign-pack-flip-host");

                if (metadata && metadata.is_dual_faced && metadata.back_src) {
                    activeZoomFlipButton = createCampaignPackFlipButton(zoomImage);

                    if (activeZoomFlipButton) {
                        if (currentFace === "back") {
                            activeZoomFlipButton.classList.add("campaign-pack-flip-button-flipped");
                            activeZoomFlipButton.setAttribute("aria-label", "Show front face");
                            activeZoomFlipButton.setAttribute("title", "Show front face");
                        }

                        zoomContent.appendChild(activeZoomFlipButton);
                    }
                }
            }

            zoomOverlay.classList.remove("hidden");
            zoomOverlay.setAttribute("aria-hidden", "false");
        }

        function closeZoom() {
            if (activeZoomFlipButton) {
                activeZoomFlipButton.remove();
                activeZoomFlipButton = null;
            }

            activeZoomSourceImage = null;

            zoomOverlay.classList.add("hidden");
            zoomOverlay.setAttribute("aria-hidden", "true");
            zoomImage.src = "";
            zoomImage.alt = "";
            zoomImage.dataset.cardUuid = "";
            zoomImage.dataset.currentFace = "front";
        }

        zoomableImages.forEach(function (imageElement) {
            imageElement.addEventListener("click", function () {
                openZoom(imageElement);
            });

            imageElement.addEventListener("keydown", function (event) {
                if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    openZoom(imageElement);
                }
            });
        });

        zoomBackdrop.addEventListener("click", closeZoom);

        zoomImage.addEventListener("click", function (event) {
            if (event.target && event.target.closest && event.target.closest(".campaign-pack-flip-button")) {
                return;
            }

            closeZoom();
        });

        document.addEventListener("keydown", function (event) {
            if (event.key !== "Escape") {
                return;
            }

            if (changePrintingOverlay && !changePrintingOverlay.classList.contains("hidden")) {
                closeChangePrintingModal();
                return;
            }

            if (!zoomOverlay.classList.contains("hidden")) {
                closeZoom();
            }
        });
    }

    loadCampaignPackFaceMetadata();
    const packViewCopySplit = document.getElementById("packViewCopySplit");
    const packViewCopyMainButton = document.getElementById("packViewCopyMainButton");
    const packViewCopyMenuButton = document.getElementById("packViewCopyMenuButton");
    const packViewCopyMenu = document.getElementById("packViewCopyMenu");
    const packViewCopyFormatButtons = Array.from(document.querySelectorAll(".pack-view-copy-format-button"));

    let selectedPackViewExportFormat = "archidekt";

    async function copyPackViewTextToClipboard(textValue) {
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
                throw new Error("Clipboard copy failed.");
            }

            return true;
        } finally {
            document.body.removeChild(hiddenTextArea);
        }
    }

    function closePackViewCopyMenu() {
        if (!packViewCopyMenu || !packViewCopyMenuButton) {
            return;
        }

        packViewCopyMenu.classList.add("hidden");
        packViewCopyMenuButton.setAttribute("aria-expanded", "false");
    }

    function togglePackViewCopyMenu() {
        if (!packViewCopyMenu || !packViewCopyMenuButton) {
            return;
        }

        const willOpen = packViewCopyMenu.classList.contains("hidden");
        packViewCopyMenu.classList.toggle("hidden", !willOpen);
        packViewCopyMenuButton.setAttribute("aria-expanded", willOpen ? "true" : "false");
    }

    function setSelectedPackViewExportFormat(exportFormat) {
        const normalizedFormat = String(exportFormat || "").trim().toLowerCase();

        if (
            normalizedFormat !== "archidekt"
            && normalizedFormat !== "moxfield"
            && normalizedFormat !== "archidekt_full"
            && normalizedFormat !== "moxfield_full"
        ) {
            return;
        }

        selectedPackViewExportFormat = normalizedFormat;

        packViewCopyFormatButtons.forEach(function (button) {
            button.classList.toggle(
                "chaos-export-menu-item-active",
                (button.getAttribute("data-export-format") || "") === selectedPackViewExportFormat
            );
        });
    }

    async function copyPackViewExport() {
        if (!packViewCopySplit || !packViewCopyMainButton) {
            return;
        }

        const exportUrl = packViewCopySplit.getAttribute("data-export-url") || "";

        if (!exportUrl) {
            throw new Error("Pack export URL was missing.");
        }

        const originalText = packViewCopyMainButton.textContent;

        packViewCopyMainButton.disabled = true;
        packViewCopyMainButton.classList.add("action-button-loading");
        packViewCopyMainButton.textContent = "Copying...";

        if (packViewCopyMenuButton) {
            packViewCopyMenuButton.disabled = true;
        }

        try {
            const response = await fetch(exportUrl, {
                method: "POST",
                headers: {
                    "Accept": "application/json",
                    "Content-Type": "application/json"
                },
                body: JSON.stringify({
                    export_format: selectedPackViewExportFormat
                })
            });

            const payload = await response.json();

            if (!response.ok || !payload.ok) {
                throw new Error(payload.message || "Failed to export pack.");
            }

            await copyPackViewTextToClipboard(payload.export_text || "");

            packViewCopyMainButton.textContent = "Copied";

            window.setTimeout(function () {
                packViewCopyMainButton.textContent = originalText || "Copy";
            }, 1400);
        } finally {
            packViewCopyMainButton.disabled = false;
            packViewCopyMainButton.classList.remove("action-button-loading");

            if (packViewCopyMenuButton) {
                packViewCopyMenuButton.disabled = false;
            }
        }
    }

    changePrintingButtons.forEach(function (button) {
        button.addEventListener("click", function () {
            openChangePrintingModal(button);
        });
    });

    if (batchUpscaleButton) {
        batchUpscaleButton.addEventListener(
            "click",
            function () {
                const cardUuids =
                    getCampaignPackBatchUpscaleCardUuids();

                if (!cardUuids.length) {
                    showCampaignPackMessage(
                        "This pack does not contain any cards to upscale.",
                        true
                    );

                    return;
                }

                if (
                    !window.iMomirUpscaleBatch
                    || !window.iMomirUpscaleBatch.requestCards
                ) {
                    showCampaignPackMessage(
                        "Batch Upscale controls are not available.",
                        true
                    );

                    return;
                }

                window.iMomirUpscaleBatch.requestCards(
                    cardUuids,
                    {
                        title:
                            "Batch Upscale Pack",

                        sourceLabel:
                            packDisplayName,

                        message: (
                            "Upscale all "
                            + cardUuids.length
                            + " card(s) currently shown in "
                            + packDisplayName
                            + "?"
                        )
                    }
                );
            }
        );
    }

    if (changePrintingCloseButton) {
        changePrintingCloseButton.addEventListener("click", closeChangePrintingModal);
    }

    if (packViewCopyMainButton) {
        packViewCopyMainButton.addEventListener("click", async function () {
            try {
                await copyPackViewExport();
            } catch (error) {
                window.alert(error.message || "Failed to copy pack export.");
            }
        });
    }

    if (packViewCopyMenuButton) {
        packViewCopyMenuButton.addEventListener("click", function (event) {
            event.stopPropagation();
            togglePackViewCopyMenu();
        });
    }

    packViewCopyFormatButtons.forEach(function (button) {
        button.addEventListener("click", async function () {
            setSelectedPackViewExportFormat(button.getAttribute("data-export-format") || "archidekt");
            closePackViewCopyMenu();

            try {
                await copyPackViewExport();
            } catch (error) {
                window.alert(error.message || "Failed to copy pack export.");
            }
        });
    });

    document.addEventListener("click", function () {
        closePackViewCopyMenu();
    });

    setSelectedPackViewExportFormat("archidekt");
})();