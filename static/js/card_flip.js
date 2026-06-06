(function () {
    const metadataUrl = "/card-face-data";

    const metadataCache = {
        chaosCards: {},
        cards: {}
    };

    let pendingEnhanceTimer = null;

    function uniqueValues(values) {
        const seenValues = new Set();

        return (values || [])
            .map(function (value) {
                return String(value || "").trim();
            })
            .filter(function (value) {
                if (!value || seenValues.has(value)) {
                    return false;
                }

                seenValues.add(value);
                return true;
            });
    }

    function getCardImageForHost(hostElement) {
        if (!hostElement) {
            return null;
        }

        if (hostElement.matches && hostElement.matches("img")) {
            return hostElement;
        }

        return hostElement.querySelector(
            "img.campaign-test-draft-picked-thumb, " +
            "img.campaign-test-draft-card-image, " +
            "img.custom-draft-current-card-image, " +
            "img.chaos-pack-view-image, " +
            "img.chaos-pack-inline-image, " +
            "img.card-image, " +
            "img"
        );
    }

    function getImageWrapForImage(imageElement) {
        if (!imageElement) {
            return null;
        }

        return imageElement.closest(
            ".campaign-test-draft-card-image-shell, " +
            ".custom-draft-current-card-image-wrap, " +
            ".chaos-pack-view-image-wrap, " +
            ".chaos-pack-inline-image-wrap, " +
            ".card-zoom-content, " +
            ".card-display, " +
            ".deckbuilder-card, " +
            ".campaign-test-draft-picked-row, " +
            ".campaign-test-draft-virtual-card, " +
            ".chaos-pack-inline-card, " +
            ".chaos-pack-view-card, " +
            ".custom-draft-current-grid-card"
        ) || imageElement.parentElement;
    }

    function getCardIdentifier(hostElement) {
        if (!hostElement) {
            return {
                type: "",
                value: ""
            };
        }

        const chaosCardHost = hostElement.closest("[data-card-uuid]");
        const standardCardHost = hostElement.closest("[data-card-key]");

        if (chaosCardHost && chaosCardHost.dataset.cardUuid) {
            return {
                type: "chaos",
                value: String(chaosCardHost.dataset.cardUuid || "").trim()
            };
        }

        if (standardCardHost && standardCardHost.dataset.cardKey) {
            return {
                type: "standard",
                value: String(standardCardHost.dataset.cardKey || "").trim()
            };
        }

        if (hostElement.dataset.cardUuid) {
            return {
                type: "chaos",
                value: String(hostElement.dataset.cardUuid || "").trim()
            };
        }

        if (hostElement.dataset.cardKey) {
            return {
                type: "standard",
                value: String(hostElement.dataset.cardKey || "").trim()
            };
        }

        return {
            type: "",
            value: ""
        };
    }

    function getCachedMetadata(identifier) {
        if (!identifier || !identifier.type || !identifier.value) {
            return null;
        }

        if (identifier.type === "chaos") {
            return metadataCache.chaosCards[identifier.value] || null;
        }

        if (identifier.type === "standard") {
            return metadataCache.cards[identifier.value] || null;
        }

        return null;
    }

    function setCachedMetadata(identifier, metadata) {
        if (!identifier || !identifier.type || !identifier.value) {
            return;
        }

        if (identifier.type === "chaos") {
            metadataCache.chaosCards[identifier.value] = metadata || {
                is_dual_faced: false
            };
        }

        if (identifier.type === "standard") {
            metadataCache.cards[identifier.value] = metadata || {
                is_dual_faced: false
            };
        }
    }

    function collectCandidateHosts(rootElement) {
        const root = rootElement || document;
        const hosts = Array.from(root.querySelectorAll("[data-card-uuid], [data-card-key]"));

        if (root.matches && root.matches("[data-card-uuid], [data-card-key]")) {
            hosts.push(root);
        }

        return hosts;
    }

    function collectUncachedIdentifiers(rootElement) {
        const chaosCardUuids = [];
        const cardKeys = [];

        collectCandidateHosts(rootElement).forEach(function (hostElement) {
            const imageElement = getCardImageForHost(hostElement);

            if (!imageElement) {
                return;
            }

            const identifier = getCardIdentifier(hostElement);

            if (!identifier.value || getCachedMetadata(identifier)) {
                return;
            }

            if (identifier.type === "chaos") {
                chaosCardUuids.push(identifier.value);
            } else if (identifier.type === "standard") {
                cardKeys.push(identifier.value);
            }
        });

        return {
            chaos_card_uuids: uniqueValues(chaosCardUuids),
            card_keys: uniqueValues(cardKeys)
        };
    }

    async function loadMetadataForRoot(rootElement) {
        const identifiers = collectUncachedIdentifiers(rootElement);

        if (!identifiers.chaos_card_uuids.length && !identifiers.card_keys.length) {
            return;
        }

        const response = await fetch(metadataUrl, {
            method: "POST",
            headers: {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-Requested-With": "XMLHttpRequest"
            },
            body: JSON.stringify(identifiers)
        });

        const payload = await response.json();

        if (!response.ok || !payload.ok) {
            throw new Error(payload.message || "Could not load card face data.");
        }

        identifiers.chaos_card_uuids.forEach(function (cardUuid) {
            setCachedMetadata(
                {
                    type: "chaos",
                    value: cardUuid
                },
                (payload.chaos_cards || {})[cardUuid]
            );
        });

        identifiers.card_keys.forEach(function (cardKey) {
            setCachedMetadata(
                {
                    type: "standard",
                    value: cardKey
                },
                (payload.cards || {})[cardKey]
            );
        });
    }

    function imageSrcWithoutCacheBust(src) {
        return String(src || "").replace(/[?&]v=\d+$/, "");
    }

    function syncVisiblePreviewImages(sourceImageElement, targetFace, targetSrc, targetAlt) {
        const sourceCurrentSrc = imageSrcWithoutCacheBust(sourceImageElement.src || "");
        const sourcePreviousSrc = imageSrcWithoutCacheBust(sourceImageElement.dataset.cardFlipLastSrc || "");
        const sourceFrontSrc = imageSrcWithoutCacheBust(sourceImageElement.dataset.cardFlipFrontSrc || "");
        const sourceBackSrc = imageSrcWithoutCacheBust(sourceImageElement.dataset.cardFlipBackSrc || "");

        const candidateOldSources = [
            sourceCurrentSrc,
            sourcePreviousSrc,
            targetFace === "back" ? sourceFrontSrc : sourceBackSrc
        ].filter(Boolean);

        const previewSelectors = [
            ".campaign-test-draft-hover-preview:not(.hidden) img",
            ".card-zoom-overlay:not(.hidden) img.card-zoom-image",
            "#deckbuilderHoverPreview:not(.hidden) img",
            "#campaignTestDraftHoverPreview:not(.hidden) img",
            "#chaosPackZoomOverlay:not(.hidden) img",
            "#customDraftCardZoomOverlay:not(.hidden) img",
            "#cardZoomOverlay:not(.hidden) img"
        ];

        document.querySelectorAll(previewSelectors.join(", ")).forEach(function (previewImage) {
            const previewSrc = imageSrcWithoutCacheBust(previewImage.src || "");

            if (!candidateOldSources.includes(previewSrc)) {
                return;
            }

            previewImage.src = targetSrc;
            previewImage.alt = targetAlt || previewImage.alt || "";
            previewImage.dataset.cardFlipFace = targetFace;
        });
    }

    function updateFlipControlPosition(buttonElement, targetFace) {
        if (!buttonElement) {
            return;
        }

        buttonElement.classList.toggle("imomir-card-flip-control-back", targetFace === "back");
        buttonElement.classList.toggle("imomir-card-flip-control-front", targetFace !== "back");
        buttonElement.setAttribute(
            "aria-label",
            targetFace === "back" ? "Flip to front face" : "Flip to back face"
        );
        buttonElement.setAttribute(
            "title",
            targetFace === "back" ? "Flip to front face" : "Flip to back face"
        );
    }

    function setCardFace(imageElement, buttonElement, metadata, faceName) {
        const targetFace = faceName === "back" ? "back" : "front";
        const targetSrc = targetFace === "back" ? metadata.back_src : metadata.front_src;
        const targetAlt = targetFace === "back" ? metadata.back_alt : metadata.front_alt;

        if (!targetSrc || !imageElement || imageElement.dataset.cardFlipFace === targetFace) {
            return;
        }

        const wrapElement = getImageWrapForImage(imageElement);

        if (wrapElement) {
            wrapElement.classList.add("imomir-card-flip-animating");
        }

        imageElement.dataset.cardFlipLastSrc = imageElement.src || "";

        window.setTimeout(function () {
            imageElement.src = targetSrc;
            imageElement.alt = targetAlt || imageElement.alt || "";
            imageElement.dataset.cardFlipFace = targetFace;

            if (imageElement.dataset.zoomSrc !== undefined) {
                imageElement.dataset.zoomSrc = targetSrc;
            }

            if (imageElement.dataset.zoomAlt !== undefined) {
                imageElement.dataset.zoomAlt = targetAlt || imageElement.alt || "";
            }

            updateFlipControlPosition(buttonElement, targetFace);
            syncVisiblePreviewImages(imageElement, targetFace, targetSrc, targetAlt);
        }, 120);

        window.setTimeout(function () {
            if (wrapElement) {
                wrapElement.classList.remove("imomir-card-flip-animating");
            }
        }, 280);
    }

    function flipCardFace(imageElement, buttonElement, metadata) {
        const currentFace = imageElement.dataset.cardFlipFace === "back" ? "back" : "front";

        setCardFace(
            imageElement,
            buttonElement,
            metadata,
            currentFace === "back" ? "front" : "back"
        );
    }

    function shouldUseStackVisibleButton(imageElement) {
        return Boolean(
            imageElement
            && imageElement.closest(".deckbuilder-stack-column-body, .campaign-test-draft-stack-column-body")
            && imageElement.closest(".deckbuilder-card, .campaign-test-draft-picked-row")
        );
    }

    function bindFlipControl(imageElement, metadata) {
        if (!imageElement || !metadata || !metadata.is_dual_faced || !metadata.front_src || !metadata.back_src) {
            return;
        }

        if (imageElement.dataset.cardFlipBound === "1") {
            return;
        }

        const wrapElement = getImageWrapForImage(imageElement);

        if (!wrapElement) {
            return;
        }

        imageElement.dataset.cardFlipBound = "1";
        imageElement.dataset.cardFlipFace = imageElement.dataset.cardFlipFace || "front";
        imageElement.dataset.cardFlipFrontSrc = metadata.front_src;
        imageElement.dataset.cardFlipBackSrc = metadata.back_src;
        imageElement.dataset.cardFlipLastSrc = imageElement.src || "";

        wrapElement.classList.add("imomir-card-flip-wrap");

        const buttonElement = document.createElement("button");
        buttonElement.type = "button";
        buttonElement.className = "imomir-card-flip-control imomir-card-flip-control-front";
        buttonElement.setAttribute("aria-label", "Flip to back face");
        buttonElement.setAttribute("title", "Flip to back face");
        buttonElement.innerHTML = '<i class="fa-solid fa-rotate"></i>';

        if (shouldUseStackVisibleButton(imageElement)) {
            buttonElement.classList.add("imomir-card-flip-control-stack-visible");
        }

        if (
            imageElement.closest(".card-zoom-content")
            || imageElement.classList.contains("chaos-pack-view-image")
            || imageElement.classList.contains("card-image-zoomable")
        ) {
            buttonElement.classList.add("imomir-card-flip-control-zoom-page");
        }

        let hoverTimer = null;

        function clearHoverTimer() {
            if (hoverTimer) {
                window.clearTimeout(hoverTimer);
                hoverTimer = null;
            }
        }

        buttonElement.addEventListener("click", function (event) {
            event.preventDefault();
            event.stopPropagation();
            clearHoverTimer();
            flipCardFace(imageElement, buttonElement, metadata);
        });

        buttonElement.addEventListener("mouseenter", function () {
            clearHoverTimer();

            hoverTimer = window.setTimeout(function () {
                flipCardFace(imageElement, buttonElement, metadata);
                hoverTimer = null;
            }, 2000);
        });

        buttonElement.addEventListener("mouseleave", clearHoverTimer);

        buttonElement.addEventListener("mousedown", function (event) {
            event.preventDefault();
            event.stopPropagation();
        });

        buttonElement.addEventListener("dblclick", function (event) {
            event.preventDefault();
            event.stopPropagation();
        });

        wrapElement.appendChild(buttonElement);
    }

    function applyFlipControls(rootElement) {
        collectCandidateHosts(rootElement || document).forEach(function (hostElement) {
            const identifier = getCardIdentifier(hostElement);
            const metadata = getCachedMetadata(identifier);

            if (!metadata || !metadata.is_dual_faced) {
                return;
            }

            const imageElement = getCardImageForHost(hostElement);

            bindFlipControl(imageElement, metadata);
        });
    }

    async function enhance(rootElement) {
        try {
            await loadMetadataForRoot(rootElement || document);
            applyFlipControls(rootElement || document);
        } catch (error) {
            console.warn("Card flip setup failed:", error);
        }
    }

    function scheduleEnhance(rootElement) {
        if (pendingEnhanceTimer) {
            window.clearTimeout(pendingEnhanceTimer);
        }

        pendingEnhanceTimer = window.setTimeout(function () {
            pendingEnhanceTimer = null;
            enhance(rootElement || document);
        }, 100);
    }

    function observeDynamicCards() {
        const observer = new MutationObserver(function (mutations) {
            let shouldEnhance = false;

            mutations.forEach(function (mutation) {
                Array.from(mutation.addedNodes || []).forEach(function (node) {
                    if (!node || node.nodeType !== 1) {
                        return;
                    }

                    if (
                        node.matches?.("[data-card-uuid], [data-card-key]")
                        || node.querySelector?.("[data-card-uuid], [data-card-key]")
                    ) {
                        shouldEnhance = true;
                    }
                });
            });

            if (shouldEnhance) {
                scheduleEnhance(document);
            }
        });

        observer.observe(document.body, {
            childList: true,
            subtree: true
        });
    }

    document.addEventListener("DOMContentLoaded", function () {
        enhance(document);
        observeDynamicCards();
    });

    window.iMomirCardFlip = {
        enhance: enhance,
        refresh: function () {
            return enhance(document);
        },
        resetImageBinding: function (imageElement) {
            if (!imageElement) {
                return;
            }

            delete imageElement.dataset.cardFlipBound;
            delete imageElement.dataset.cardFlipFace;
            delete imageElement.dataset.cardFlipFrontSrc;
            delete imageElement.dataset.cardFlipBackSrc;
            delete imageElement.dataset.cardFlipLastSrc;

            const wrapElement = getImageWrapForImage(imageElement);

            if (wrapElement) {
                Array.from(wrapElement.querySelectorAll(".imomir-card-flip-control")).forEach(function (buttonElement) {
                    buttonElement.remove();
                });
            }
        },
        prepareZoomImage: function (zoomImageElement, sourceImageElement, zoomRootElement) {
            if (!zoomImageElement || !sourceImageElement) {
                return;
            }

            const sourceHost = sourceImageElement.closest("[data-card-uuid], [data-card-key]") || sourceImageElement;

            const cardUuid = (
                sourceImageElement.dataset.cardUuid
                || sourceHost.dataset.cardUuid
                || ""
            ).trim();

            const cardKey = (
                sourceImageElement.dataset.cardKey
                || sourceHost.dataset.cardKey
                || ""
            ).trim();

            zoomImageElement.removeAttribute("data-card-uuid");
            zoomImageElement.removeAttribute("data-card-key");

            if (cardUuid) {
                zoomImageElement.dataset.cardUuid = cardUuid;
            }

            if (cardKey) {
                zoomImageElement.dataset.cardKey = cardKey;
            }

            zoomImageElement.dataset.cardFlipFace = sourceImageElement.dataset.cardFlipFace || "front";

            this.resetImageBinding(zoomImageElement);

            return enhance(zoomRootElement || zoomImageElement.closest(".card-zoom-overlay") || document);
        }
    };
})();