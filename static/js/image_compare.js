(function () {
    let overlay = null;
    let titleElement = null;
    let modelSelect = null;
    let runButton = null;

    let sourceImage = null;
    let sourceMeta = null;

    let outputImage = null;
    let outputLabel = null;
    let outputMeta = null;

    let statusContainer = null;
    let statusElement = null;
    let statusStage = null;
    let statusSpinner = null;
    let statusElapsed = null;
    let statusProgressBar = null;

    let activeRunId = "";
    let runStatusPollTimer = null;
    let runStatusElapsedTimer = null;
    let runStatusStartedAt = 0;

    let acceptButton = null;
    let discardButton = null;
    let revertButton = null;
    let flipButton = null;

    let currentControlUrl = "";
    let currentData = null;
    let currentCandidate = null;

    let currentFace = "front";
    let currentCandidatesByFace = {};
    let feedbackByFace = {};

    const lastUpscaleModelStorageKey =
        "imomir_last_upscale_model";

    let zoomOverlay = null;
    let zoomTitleElement = null;
    let zoomSlider = null;
    let zoomValueLabel = null;

    let zoomSourceImage = null;
    let zoomOutputImage = null;
    let zoomOutputEmpty = null;
    let zoomOutputLabel = null;

    let zoomLevel = 100;
    let zoomPanX = 0;
    let zoomPanY = 0;
    let zoomDragState = null;

    let devFeedbackPanel = null;
    let sourceConditionInput = null;
    let sourceConditionValue = null;
    let feedbackNotesInput = null;
    let feedbackRatingInputs = {};

    const sourceConditionLabels = {
        0: "Low Quality",
        1: "Ok Quality",
        2: "Good Quality"
    };

    const qualityRatingLabels = {
        "-2": "Much Worse",
        "-1": "Worse",
        "0": "Same",
        "1": "Improved",
        "2": "Perfect"
    };

    const feedbackRatingDefinitions = [
        ["card_title", "Card Title"],
        ["mana_cost", "Mana Cost"],
        ["artwork", "Artwork"],
        ["rules_text", "Rules Text"],
        ["power_toughness", "Power/Toughness"],
        ["frame", "Frame"],
        ["bottom_text", "Bottom Text"],
        ["card_overall", "Card Overall"]
    ];


    function createScaleLabels(labelValues) {
        const scaleLabels =
            document.createElement("div");

        scaleLabels.className =
            "imomir-dev-feedback-scale-labels";

        labelValues.forEach(
            function (labelText) {
                const label =
                    document.createElement(
                        "span"
                    );

                label.textContent =
                    labelText;

                scaleLabels.appendChild(
                    label
                );
            }
        );

        return scaleLabels;
    }


    function buildDevFeedbackPanel() {
        const panel =
            document.createElement(
                "div"
            );

        panel.className =
            "imomir-dev-feedback-panel hidden";

        const heading =
            document.createElement(
                "div"
            );

        heading.className =
            "imomir-dev-feedback-heading";

        heading.textContent =
            "DEV MODE - Dev Feedback System";

        const subtitle =
            document.createElement(
                "div"
            );

        subtitle.className =
            "imomir-dev-feedback-subtitle";

        subtitle.textContent =
            "Rate the source and this specific upscale candidate. Feedback is saved when you Accept or Discard the candidate.";

        panel.appendChild(
            heading
        );

        panel.appendChild(
            subtitle
        );

        const sourceRow =
            document.createElement(
                "div"
            );

        sourceRow.className =
            "imomir-dev-feedback-row "
            + "imomir-dev-feedback-source-row";

        const sourceLabel =
            document.createElement(
                "div"
            );

        sourceLabel.className =
            "imomir-dev-feedback-row-label";

        sourceLabel.textContent =
            "Scryfall Source Condition";

        const sourceControl =
            document.createElement(
                "div"
            );

        sourceControl.className =
            "imomir-dev-feedback-slider-wrap";

        sourceConditionInput =
            document.createElement(
                "input"
            );

        sourceConditionInput.type =
            "range";

        sourceConditionInput.min =
            "0";

        sourceConditionInput.max =
            "2";

        sourceConditionInput.step =
            "1";

        sourceConditionInput.value =
            "1";

        sourceConditionInput.className =
            "imomir-dev-feedback-slider";

        sourceControl.appendChild(
            sourceConditionInput
        );

        sourceControl.appendChild(
            createScaleLabels([
                "Low Quality",
                "Ok Quality",
                "Good Quality"
            ])
        );

        sourceConditionValue =
            document.createElement(
                "div"
            );

        sourceConditionValue.className =
            "imomir-dev-feedback-current-value";

        sourceConditionValue.textContent =
            "Ok Quality";

        sourceConditionInput.addEventListener(
            "input",
            function () {
                sourceConditionValue.textContent =
                    sourceConditionLabels[
                        Number(
                            sourceConditionInput.value
                        )
                    ]
                    || "Ok Quality";
            }
        );

        sourceRow.appendChild(
            sourceLabel
        );

        sourceRow.appendChild(
            sourceControl
        );

        sourceRow.appendChild(
            sourceConditionValue
        );

        panel.appendChild(
            sourceRow
        );

        const improvementHeading =
            document.createElement(
                "div"
            );

        improvementHeading.className =
            "imomir-dev-feedback-section-heading";

        improvementHeading.textContent =
            "Targeted Improvements";

        panel.appendChild(
            improvementHeading
        );

        feedbackRatingInputs = {};

        feedbackRatingDefinitions.forEach(
            function (definition) {
                const ratingKey =
                    definition[0];

                const ratingLabelText =
                    definition[1];

                const row =
                    document.createElement(
                        "div"
                    );

                row.className =
                    "imomir-dev-feedback-row";

                const label =
                    document.createElement(
                        "div"
                    );

                label.className =
                    "imomir-dev-feedback-row-label";

                label.textContent =
                    ratingLabelText;

                const sliderWrap =
                    document.createElement(
                        "div"
                    );

                sliderWrap.className =
                    "imomir-dev-feedback-slider-wrap";

                const slider =
                    document.createElement(
                        "input"
                    );

                slider.type = "range";
                slider.min = "-2";
                slider.max = "2";
                slider.step = "1";
                slider.value = "0";

                slider.className =
                    "imomir-dev-feedback-slider";

                sliderWrap.appendChild(
                    slider
                );

                sliderWrap.appendChild(
                    createScaleLabels([
                        "Much Worse",
                        "Worse",
                        "Same",
                        "Improved",
                        "Perfect"
                    ])
                );

                const currentValue =
                    document.createElement(
                        "div"
                    );

                currentValue.className =
                    "imomir-dev-feedback-current-value";

                currentValue.textContent =
                    "Same";

                slider.addEventListener(
                    "input",
                    function () {
                        currentValue.textContent =
                            qualityRatingLabels[
                                String(
                                    slider.value
                                )
                            ]
                            || "Same";
                    }
                );

                feedbackRatingInputs[
                    ratingKey
                ] = slider;

                row.appendChild(
                    label
                );

                row.appendChild(
                    sliderWrap
                );

                row.appendChild(
                    currentValue
                );

                panel.appendChild(
                    row
                );
            }
        );

        const notesLabel =
            document.createElement(
                "label"
            );

        notesLabel.className =
            "imomir-dev-feedback-notes";

        const notesTitle =
            document.createElement(
                "span"
            );

        notesTitle.textContent =
            "Notes";

        feedbackNotesInput =
            document.createElement(
                "textarea"
            );

        feedbackNotesInput.rows = 3;
        feedbackNotesInput.maxLength = 8000;

        feedbackNotesInput.placeholder =
            "Optional: halos, malformed symbols, text damage, frame artifacts, model-specific observations, etc.";

        notesLabel.appendChild(
            notesTitle
        );

        notesLabel.appendChild(
            feedbackNotesInput
        );

        panel.appendChild(
            notesLabel
        );

        return panel;
    }


    function resetDevFeedback() {
        if (sourceConditionInput) {
            sourceConditionInput.value =
                "1";
        }

        if (sourceConditionValue) {
            sourceConditionValue.textContent =
                "Ok Quality";
        }

        Object.keys(
            feedbackRatingInputs
        ).forEach(
            function (ratingKey) {
                const slider =
                    feedbackRatingInputs[
                        ratingKey
                    ];

                if (!slider) {
                    return;
                }

                slider.value = "0";

                slider.dispatchEvent(
                    new Event(
                        "input"
                    )
                );
            }
        );

        if (feedbackNotesInput) {
            feedbackNotesInput.value =
                "";
        }
    }


    function setDevFeedbackVisible(
        isVisible
    ) {
        if (!devFeedbackPanel) {
            return;
        }

        devFeedbackPanel.classList.toggle(
            "hidden",
            !isVisible
        );
    }


    function collectDevFeedback() {
        const ratings = {};

        Object.keys(
            feedbackRatingInputs
        ).forEach(
            function (ratingKey) {
                const slider =
                    feedbackRatingInputs[
                        ratingKey
                    ];

                ratings[
                    ratingKey
                ] = (
                    slider
                    ? Number(
                        slider.value
                    )
                    : 0
                );
            }
        );

        return {
            source_condition: (
                sourceConditionInput
                ? Number(
                    sourceConditionInput.value
                )
                : 1
            ),

            ratings: ratings,

            notes: (
                feedbackNotesInput
                ? String(
                    feedbackNotesInput.value
                    || ""
                ).trim()
                : ""
            )
        };
    }

        function clampZoomLevel(
        rawValue
    ) {
        const parsedValue = Number(
            rawValue
        );

        if (!Number.isFinite(
            parsedValue
        )) {
            return 100;
        }

        return Math.max(
            25,
            Math.min(
                500,
                parsedValue
            )
        );
    }


    function applyZoomTransform() {
        const scale = (
            zoomLevel / 100
        );

        const transformValue = (
            "translate3d("
            + zoomPanX
            + "px, "
            + zoomPanY
            + "px, 0) "
            + "scale("
            + scale
            + ")"
        );

        [
            zoomSourceImage,
            zoomOutputImage
        ].forEach(
            function (imageElement) {
                if (!imageElement) {
                    return;
                }

                imageElement.style.transform =
                    transformValue;
            }
        );

        if (zoomValueLabel) {
            zoomValueLabel.textContent =
                Math.round(
                    zoomLevel
                )
                + "%";
        }

        if (zoomSlider) {
            zoomSlider.value =
                String(
                    zoomLevel
                );
        }
    }


    function setZoomLevel(
        rawValue
    ) {
        zoomLevel = clampZoomLevel(
            rawValue
        );

        applyZoomTransform();
    }


    function resetZoomView() {
        zoomLevel = 100;
        zoomPanX = 0;
        zoomPanY = 0;

        applyZoomTransform();
    }


    function syncZoomViewerImages() {
        if (
            zoomSourceImage
            && sourceImage
            && sourceImage.src
        ) {
            zoomSourceImage.src =
                sourceImage.src;
        }

        const hasOutput = Boolean(
            outputImage
            && outputImage.src
            && !outputImage.classList.contains(
                "hidden"
            )
        );

        if (
            hasOutput
            && zoomOutputImage
        ) {
            zoomOutputImage.src =
                outputImage.src;

            zoomOutputImage.classList.remove(
                "hidden"
            );

            if (zoomOutputEmpty) {
                zoomOutputEmpty.classList.add(
                    "hidden"
                );
            }

        } else {
            if (zoomOutputImage) {
                zoomOutputImage.src = "";

                zoomOutputImage.classList.add(
                    "hidden"
                );
            }

            if (zoomOutputEmpty) {
                zoomOutputEmpty.classList.remove(
                    "hidden"
                );
            }
        }

        if (zoomOutputLabel) {
            zoomOutputLabel.textContent =
                outputLabel
                && outputLabel.textContent
                    ? outputLabel.textContent
                    : "Upscaled Output";
        }
    }


    function closeZoomViewer() {
        if (!zoomOverlay) {
            return;
        }

        zoomOverlay.classList.add(
            "hidden"
        );

        zoomOverlay.setAttribute(
            "aria-hidden",
            "true"
        );

        document.body.classList.remove(
            "imomir-zoom-comparison-open"
        );

        zoomDragState = null;
    }


    function openZoomViewer() {
        ensureZoomViewer();

        syncZoomViewerImages();

        resetZoomView();

        if (zoomTitleElement) {
            zoomTitleElement.textContent =
                (
                    currentData
                    && currentData.title
                        ? currentData.title
                        : "Card"
                )
                + " — Zoom Comparison";
        }

        zoomOverlay.classList.remove(
            "hidden"
        );

        zoomOverlay.setAttribute(
            "aria-hidden",
            "false"
        );

        document.body.classList.add(
            "imomir-zoom-comparison-open"
        );
    }


    function beginZoomPan(
        event
    ) {
        if (
            event.button !== undefined
            && event.button !== 0
        ) {
            return;
        }

        zoomDragState = {
            pointerId: (
                event.pointerId
            ),

            startX: (
                event.clientX
            ),

            startY: (
                event.clientY
            ),

            startPanX: (
                zoomPanX
            ),

            startPanY: (
                zoomPanY
            ),

            viewport: (
                event.currentTarget
            )
        };

        if (
            event.currentTarget
            && event.currentTarget
                .setPointerCapture
        ) {
            event.currentTarget
                .setPointerCapture(
                    event.pointerId
                );
        }

        event.currentTarget.classList.add(
            "imomir-zoom-viewport-panning"
        );

        event.preventDefault();
    }


    function continueZoomPan(
        event
    ) {
        if (!zoomDragState) {
            return;
        }

        zoomPanX = (
            zoomDragState.startPanX
            + (
                event.clientX
                - zoomDragState.startX
            )
        );

        zoomPanY = (
            zoomDragState.startPanY
            + (
                event.clientY
                - zoomDragState.startY
            )
        );

        applyZoomTransform();

        event.preventDefault();
    }


    function endZoomPan(
        event
    ) {
        if (!zoomDragState) {
            return;
        }

        const viewport =
            zoomDragState.viewport;

        if (viewport) {
            viewport.classList.remove(
                "imomir-zoom-viewport-panning"
            );
        }

        zoomDragState = null;

        event.preventDefault();
    }


    function handleZoomWheel(
        event
    ) {
        event.preventDefault();

        const zoomDirection = (
            event.deltaY < 0
                ? 1
                : -1
        );

        const zoomStep = (
            event.shiftKey
                ? 50
                : 25
        );

        setZoomLevel(
            zoomLevel
            + (
                zoomDirection
                * zoomStep
            )
        );
    }


    function bindZoomViewport(
        viewport
    ) {
        if (!viewport) {
            return;
        }

        viewport.addEventListener(
            "pointerdown",
            beginZoomPan
        );

        viewport.addEventListener(
            "pointermove",
            continueZoomPan
        );

        viewport.addEventListener(
            "pointerup",
            endZoomPan
        );

        viewport.addEventListener(
            "pointercancel",
            endZoomPan
        );

        viewport.addEventListener(
            "wheel",
            handleZoomWheel,
            {
                passive: false
            }
        );
    }


    function ensureZoomViewer() {
        if (zoomOverlay) {
            return;
        }

        zoomOverlay =
            document.createElement(
                "div"
            );

        zoomOverlay.className =
            "imomir-zoom-comparison-overlay hidden";

        zoomOverlay.setAttribute(
            "aria-hidden",
            "true"
        );

        const header =
            document.createElement(
                "div"
            );

        header.className =
            "imomir-zoom-comparison-header";

        zoomTitleElement =
            document.createElement(
                "div"
            );

        zoomTitleElement.className =
            "imomir-zoom-comparison-title";

        const closeButton =
            document.createElement(
                "button"
            );

        closeButton.type = "button";

        closeButton.className =
            "imomir-zoom-comparison-close";

        closeButton.innerHTML =
            "&times;";

        closeButton.title =
            "Close Zoom Comparison";

        closeButton.setAttribute(
            "aria-label",
            "Close Zoom Comparison"
        );

        header.appendChild(
            zoomTitleElement
        );

        header.appendChild(
            closeButton
        );

        const controls =
            document.createElement(
                "div"
            );

        controls.className =
            "imomir-zoom-comparison-controls";

        const zoomLabel =
            document.createElement(
                "span"
            );

        zoomLabel.className =
            "imomir-zoom-control-label";

        zoomLabel.textContent =
            "Zoom";

        const minimumLabel =
            document.createElement(
                "span"
            );

        minimumLabel.className =
            "imomir-zoom-limit-label";

        minimumLabel.textContent =
            "25%";

        zoomSlider =
            document.createElement(
                "input"
            );

        zoomSlider.type = "range";
        zoomSlider.min = "25";
        zoomSlider.max = "500";
        zoomSlider.step = "5";
        zoomSlider.value = "100";

        zoomSlider.className =
            "imomir-zoom-slider";

        zoomSlider.setAttribute(
            "aria-label",
            "Comparison zoom"
        );

        const maximumLabel =
            document.createElement(
                "span"
            );

        maximumLabel.className =
            "imomir-zoom-limit-label";

        maximumLabel.textContent =
            "500%";

        zoomValueLabel =
            document.createElement(
                "strong"
            );

        zoomValueLabel.className =
            "imomir-zoom-value";

        zoomValueLabel.textContent =
            "100%";

        const resetButton =
            document.createElement(
                "button"
            );

        resetButton.type = "button";

        resetButton.className =
            "action-button secondary-button "
            + "imomir-zoom-reset-button";

        resetButton.textContent =
            "Reset";

        const helpText =
            document.createElement(
                "span"
            );

        helpText.className =
            "imomir-zoom-help";

        helpText.textContent =
            "Drag either card to pan both. "
            + "Mouse wheel also changes zoom.";

        controls.appendChild(
            zoomLabel
        );

        controls.appendChild(
            minimumLabel
        );

        controls.appendChild(
            zoomSlider
        );

        controls.appendChild(
            maximumLabel
        );

        controls.appendChild(
            zoomValueLabel
        );

        controls.appendChild(
            resetButton
        );

        controls.appendChild(
            helpText
        );

        const grid =
            document.createElement(
                "div"
            );

        grid.className =
            "imomir-zoom-comparison-grid";

        const sourcePanel =
            document.createElement(
                "div"
            );

        sourcePanel.className =
            "imomir-zoom-comparison-panel";

        const sourceHeader =
            document.createElement(
                "div"
            );

        sourceHeader.className =
            "imomir-zoom-comparison-panel-header";

        sourceHeader.textContent =
            "Original Scryfall";

        const sourceViewport =
            document.createElement(
                "div"
            );

        sourceViewport.className =
            "imomir-zoom-comparison-viewport";

        zoomSourceImage =
            document.createElement(
                "img"
            );

        zoomSourceImage.className =
            "imomir-zoom-comparison-image";

        zoomSourceImage.alt =
            "Original Scryfall zoom comparison";

        sourceViewport.appendChild(
            zoomSourceImage
        );

        sourcePanel.appendChild(
            sourceHeader
        );

        sourcePanel.appendChild(
            sourceViewport
        );

        const outputPanel =
            document.createElement(
                "div"
            );

        outputPanel.className =
            "imomir-zoom-comparison-panel";

        zoomOutputLabel =
            document.createElement(
                "div"
            );

        zoomOutputLabel.className =
            "imomir-zoom-comparison-panel-header";

        zoomOutputLabel.textContent =
            "Upscaled Output";

        const outputViewport =
            document.createElement(
                "div"
            );

        outputViewport.className =
            "imomir-zoom-comparison-viewport";

        zoomOutputImage =
            document.createElement(
                "img"
            );

        zoomOutputImage.className =
            "imomir-zoom-comparison-image";

        zoomOutputImage.alt =
            "Upscaled zoom comparison";

        zoomOutputEmpty =
            document.createElement(
                "div"
            );

        zoomOutputEmpty.className =
            "imomir-zoom-comparison-empty";

        zoomOutputEmpty.textContent =
            "Run an upscale first to compare both images.";

        outputViewport.appendChild(
            zoomOutputImage
        );

        outputViewport.appendChild(
            zoomOutputEmpty
        );

        outputPanel.appendChild(
            zoomOutputLabel
        );

        outputPanel.appendChild(
            outputViewport
        );

        grid.appendChild(
            sourcePanel
        );

        grid.appendChild(
            outputPanel
        );

        zoomOverlay.appendChild(
            header
        );

        zoomOverlay.appendChild(
            controls
        );

        zoomOverlay.appendChild(
            grid
        );

        document.body.appendChild(
            zoomOverlay
        );

        bindZoomViewport(
            sourceViewport
        );

        bindZoomViewport(
            outputViewport
        );

        closeButton.addEventListener(
            "click",
            closeZoomViewer
        );

        resetButton.addEventListener(
            "click",
            resetZoomView
        );

        zoomSlider.addEventListener(
            "input",
            function () {
                setZoomLevel(
                    zoomSlider.value
                );
            }
        );
    }


    function bindImageZoomTrigger(
        imageElement
    ) {
        if (!imageElement) {
            return;
        }

        imageElement.classList.add(
            "imomir-image-compare-image-zoomable"
        );

        imageElement.setAttribute(
            "role",
            "button"
        );

        imageElement.setAttribute(
            "tabindex",
            "0"
        );

        imageElement.setAttribute(
            "title",
            "Open synchronized zoom comparison"
        );

        imageElement.setAttribute(
            "aria-label",
            "Open synchronized zoom comparison"
        );

        imageElement.addEventListener(
            "click",
            openZoomViewer
        );

        imageElement.addEventListener(
            "keydown",
            function (event) {
                if (
                    event.key === "Enter"
                    || event.key === " "
                ) {
                    event.preventDefault();

                    openZoomViewer();
                }
            }
        );
    }




    function ensureViewer() {
        if (overlay) {
            return;
        }

        overlay = document.createElement("div");
        overlay.className =
            "imomir-image-compare-overlay hidden";

        const dialog = document.createElement("div");
        dialog.className =
            "imomir-image-compare-dialog";

        const header = document.createElement("div");
        header.className =
            "imomir-image-compare-header";

        titleElement = document.createElement("div");
        titleElement.className =
            "imomir-image-compare-title";

        const headerActions =
            document.createElement("div");

        headerActions.className =
            "imomir-upscale-header-actions";

        flipButton =
            document.createElement("button");

        flipButton.type = "button";

        flipButton.className =
            "imomir-upscale-face-flip hidden";

        flipButton.innerHTML =
            '<i class="fa-solid fa-rotate"></i>';

        flipButton.setAttribute(
            "aria-label",
            "Show back face"
        );

        flipButton.setAttribute(
            "title",
            "Show back face"
        );

        const closeButton =
            document.createElement("button");

        closeButton.type = "button";
        closeButton.className =
            "imomir-image-compare-close";

        closeButton.innerHTML = "&times;";

        closeButton.setAttribute(
            "aria-label",
            "Close Upscale controls"
        );

        header.appendChild(
            titleElement
        );

        headerActions.appendChild(
            flipButton
        );

        headerActions.appendChild(
            closeButton
        );

        header.appendChild(
            headerActions
        );

        const controls =
            document.createElement("div");

        controls.className =
            "imomir-upscale-controls";

        modelSelect =
            document.createElement("select");

        modelSelect.className =
            "imomir-upscale-model-select";

        modelSelect.addEventListener(
            "change",
            function () {
                saveUpscaleModel(
                    modelSelect.value
                );
            }
        );

        runButton =
            document.createElement("button");

        runButton.type = "button";
        runButton.className =
            "action-button";

        runButton.innerHTML =
            '<i class="fa-solid fa-wand-magic-sparkles"></i> Run Upscale';

        controls.appendChild(modelSelect);
        controls.appendChild(runButton);

        const grid =
            document.createElement("div");

        grid.className =
            "imomir-image-compare-grid";

        const sourcePanel =
            document.createElement("div");

        sourcePanel.className =
            "imomir-image-compare-panel";

        const sourceLabel =
            document.createElement("div");

        sourceLabel.className =
            "imomir-image-compare-label";

        sourceLabel.textContent =
            "Original Scryfall";

        sourceImage =
            document.createElement("img");

        sourceImage.className =
            "imomir-image-compare-image";

        sourceMeta =
            document.createElement("div");

        sourceMeta.className =
            "imomir-image-compare-meta";

        sourcePanel.appendChild(sourceLabel);
        sourcePanel.appendChild(sourceImage);
        sourcePanel.appendChild(sourceMeta);

        bindImageZoomTrigger(
            sourceImage
        );

        const outputPanel =
            document.createElement("div");

        outputPanel.className =
            "imomir-image-compare-panel";

        outputLabel =
            document.createElement("div");

        outputLabel.className =
            "imomir-image-compare-label";

        outputImage =
            document.createElement("img");

        outputImage.className =
            "imomir-image-compare-image";

        outputMeta =
            document.createElement("div");

        outputMeta.className =
            "imomir-image-compare-meta";

        outputPanel.appendChild(outputLabel);
        outputPanel.appendChild(outputImage);
        outputPanel.appendChild(outputMeta);

        bindImageZoomTrigger(
            outputImage
        );

        grid.appendChild(sourcePanel);
        grid.appendChild(outputPanel);

        statusContainer =
            document.createElement("div");

        statusContainer.className =
            "imomir-upscale-status";

        statusSpinner =
            document.createElement("div");

        statusSpinner.className =
            "imomir-upscale-status-spinner hidden";

        const statusBody =
            document.createElement("div");

        statusBody.className =
            "imomir-upscale-status-body";

        const statusHeader =
            document.createElement("div");

        statusHeader.className =
            "imomir-upscale-status-header";

        statusStage =
            document.createElement("strong");

        statusStage.textContent =
            "Ready";

        statusElapsed =
            document.createElement("span");

        statusElapsed.className =
            "imomir-upscale-status-elapsed";

        statusHeader.appendChild(
            statusStage
        );

        statusHeader.appendChild(
            statusElapsed
        );

        statusElement =
            document.createElement("div");

        statusElement.className =
            "imomir-upscale-status-message";

        const statusProgress =
            document.createElement("div");

        statusProgress.className =
            "imomir-upscale-status-progress";

        statusProgressBar =
            document.createElement("div");

        statusProgressBar.className =
            "imomir-upscale-status-progress-bar";

        statusProgress.appendChild(
            statusProgressBar
        );

        statusBody.appendChild(
            statusHeader
        );

        statusBody.appendChild(
            statusElement
        );

        statusBody.appendChild(
            statusProgress
        );

        statusContainer.appendChild(
            statusSpinner
        );

        statusContainer.appendChild(
            statusBody
        );

        devFeedbackPanel = (
            buildDevFeedbackPanel()
        );

        const footer =
            document.createElement("div");

        footer.className =
            "imomir-upscale-footer";

        revertButton =
            document.createElement("button");

        revertButton.type = "button";
        revertButton.className =
            "action-button secondary-button";

        revertButton.textContent =
            "Revert to Scryfall";

        discardButton =
            document.createElement("button");

        discardButton.type = "button";
        discardButton.className =
            "action-button secondary-button";

        discardButton.textContent =
            "Discard Candidate";

        acceptButton =
            document.createElement("button");

        acceptButton.type = "button";
        acceptButton.className =
            "action-button";

        acceptButton.textContent =
            "Accept & Use Upscale";

        footer.appendChild(revertButton);
        footer.appendChild(discardButton);
        footer.appendChild(acceptButton);

        dialog.appendChild(header);
        dialog.appendChild(controls);
        dialog.appendChild(grid);
        dialog.appendChild(
            statusContainer
        );
        dialog.appendChild(devFeedbackPanel);
        dialog.appendChild(footer);

        overlay.appendChild(dialog);
        document.body.appendChild(overlay);

        closeButton.addEventListener(
            "click",
            closeViewer
        );

        flipButton.addEventListener(
            "click",
            flipActiveFace
        );

        overlay.addEventListener(
            "click",
            function (event) {
                if (event.target === overlay) {
                    closeViewer();
                }
            }
        );

        runButton.addEventListener(
            "click",
            runUpscale
        );

        acceptButton.addEventListener(
            "click",
            acceptCandidate
        );

        discardButton.addEventListener(
            "click",
            discardCandidate
        );

        revertButton.addEventListener(
            "click",
            revertUpscale
        );

        sourceImage.addEventListener(
            "load",
            function () {
                sourceMeta.textContent =
                    sourceImage.naturalWidth
                    + " × "
                    + sourceImage.naturalHeight;
            }
        );

        document.addEventListener(
            "keydown",
            function (event) {
                if (
                    event.key !== "Escape"
                ) {
                    return;
                }

                if (
                    zoomOverlay
                    && !zoomOverlay.classList.contains(
                        "hidden"
                    )
                ) {
                    closeZoomViewer();
                    return;
                }

                if (
                    overlay
                    && !overlay.classList.contains(
                        "hidden"
                    )
                ) {
                    closeViewer();
                }
            }
        );
    }

    function stopRunStatusTracking() {
        if (
            runStatusPollTimer
        ) {
            window.clearTimeout(
                runStatusPollTimer
            );

            runStatusPollTimer = null;
        }

        if (
            runStatusElapsedTimer
        ) {
            window.clearInterval(
                runStatusElapsedTimer
            );

            runStatusElapsedTimer = null;
        }

        activeRunId = "";
        runStatusStartedAt = 0;
    }


    function renderRunElapsedTime() {
        if (
            !statusElapsed
            || !runStatusStartedAt
        ) {
            return;
        }

        const elapsedSeconds = Math.max(
            0,
            (
                performance.now()
                - runStatusStartedAt
            )
            / 1000
        );

        statusElapsed.textContent =
            elapsedSeconds.toFixed(1)
            + "s";
    }


    function setUpscaleStatus(
        stage,
        message,
        options
    ) {
        const settings =
            options || {};

        if (statusStage) {
            statusStage.textContent =
                String(
                    stage
                    || "Status"
                );
        }

        if (statusElement) {
            statusElement.textContent =
                String(
                    message
                    || ""
                );
        }

        if (statusSpinner) {
            statusSpinner.classList.toggle(
                "hidden",
                !Boolean(
                    settings.busy
                )
            );
        }

        if (statusProgressBar) {
            const percent = Math.max(
                0,
                Math.min(
                    100,
                    Number(
                        settings.percent
                        || 0
                    )
                )
            );

            statusProgressBar.style.width =
                percent + "%";
        }

        if (
            statusContainer
        ) {
            statusContainer.classList.toggle(
                "is-error",
                Boolean(
                    settings.error
                )
            );
        }
    }


    function startRunElapsedTimer() {
        runStatusStartedAt =
            performance.now();

        renderRunElapsedTime();

        runStatusElapsedTimer =
            window.setInterval(
                renderRunElapsedTime,
                250
            );
    }


    async function pollRunStatus() {
        if (
            !activeRunId
            || !currentData
            || !currentData
                .run_status_url
        ) {
            return;
        }

        try {
            const statusUrl =
                currentData
                    .run_status_url
                + "?run_id="
                + encodeURIComponent(
                    activeRunId
                )
                + "&_="
                + Date.now();

            const response = await fetch(
                statusUrl,
                {
                    cache: "no-store",
                    headers: {
                        "Accept":
                            "application/json"
                    }
                }
            );

            const data =
                await response.json();

            if (
                response.ok
                && data.ok
                && data.found
            ) {
                const status =
                    data.status
                    || {};

                setUpscaleStatus(
                    status.stage
                    || "Working",
                    status.message
                    || "Upscale is running...",
                    {
                        busy: Boolean(
                            status.is_running
                        ),

                        percent: Number(
                            status.percent
                            || 0
                        ),

                        error: Boolean(
                            status.error
                        )
                    }
                );
            }

        } catch (error) {
            console.debug(
                "Upscale status poll failed.",
                error
            );
        }

        if (activeRunId) {
            runStatusPollTimer =
                window.setTimeout(
                    pollRunStatus,
                    500
                );
        }
    }


    function formatUpscaleTimingSummary(
        timings
    ) {
        timings = timings || {};

        const totalMs = Number(
            timings.total_ms
            || 0
        );

        const aiMs = Number(
            timings.plugin_call_ms
            || 0
        );

        const sourceMs = Number(
            timings.source_images_ms
            || 0
        );

        const finalizeMs = Number(
            timings.finalize_candidates_ms
            || 0
        );

        if (!totalMs) {
            return "";
        }

        return (
            "Completed in "
            + (
                totalMs / 1000
            ).toFixed(1)
            + "s"
            + " · AI "
            + (
                aiMs / 1000
            ).toFixed(1)
            + "s"
            + " · Sources "
            + (
                sourceMs / 1000
            ).toFixed(1)
            + "s"
            + " · Finalize "
            + (
                finalizeMs / 1000
            ).toFixed(1)
            + "s"
        );
    }

    function setBusy(isBusy) {
        runButton.disabled = isBusy;
        modelSelect.disabled = isBusy;
        acceptButton.disabled = isBusy;
        discardButton.disabled = isBusy;
        revertButton.disabled = isBusy;

        if (sourceConditionInput) {
            sourceConditionInput.disabled =
                isBusy;
        }

        Object.keys(
            feedbackRatingInputs
        ).forEach(
            function (ratingKey) {
                feedbackRatingInputs[
                    ratingKey
                ].disabled = isBusy;
            }
        );

        if (feedbackNotesInput) {
            feedbackNotesInput.disabled =
                isBusy;
        }
    }

    function getSavedUpscaleModel() {
        try {
            return String(
                window.localStorage.getItem(
                    lastUpscaleModelStorageKey
                )
                || ""
            ).trim();

        } catch (error) {
            console.warn(
                "Could not read saved Upscale model.",
                error
            );

            return "";
        }
    }


    function saveUpscaleModel(
        modelValue
    ) {
        const cleanValue = String(
            modelValue || ""
        ).trim();

        if (!cleanValue) {
            return;
        }

        try {
            window.localStorage.setItem(
                lastUpscaleModelStorageKey,
                cleanValue
            );

        } catch (error) {
            console.warn(
                "Could not save Upscale model.",
                error
            );
        }
    }

    function saveCurrentFaceFeedback() {
        if (
            !currentData
            || !currentData.dev_feedback_enabled
            || !currentCandidatesByFace[
                currentFace
            ]
            || !devFeedbackPanel
            || devFeedbackPanel.classList.contains(
                "hidden"
            )
        ) {
            return;
        }

        feedbackByFace[
            currentFace
        ] = collectDevFeedback();
    }


    function getCurrentFaceData() {
        if (!currentData) {
            return null;
        }

        if (
            currentData.faces
            && currentData.faces[
                currentFace
            ]
        ) {
            return currentData.faces[
                currentFace
            ];
        }

        return {
            face: (
                currentData.face
                || "front"
            ),

            title: currentData.title,
            source: currentData.source,

            current_upscaled: (
                currentData.current_upscaled
            )
        };
    }


    function updateFaceFlipButton() {
        if (!flipButton) {
            return;
        }

        const canFlip = Boolean(
            currentData
            && currentData.is_dual_faced
            && currentData.faces
            && currentData.faces.front
            && currentData.faces.back
        );

        flipButton.classList.toggle(
            "hidden",
            !canFlip
        );

        if (!canFlip) {
            return;
        }

        const showingBack = (
            currentFace === "back"
        );

        flipButton.classList.toggle(
            "imomir-upscale-face-flip-flipped",
            showingBack
        );

        const label = (
            showingBack
                ? "Show front face"
                : "Show back face"
        );

        flipButton.setAttribute(
            "aria-label",
            label
        );

        flipButton.setAttribute(
            "title",
            label
        );
    }


    function renderActiveFace() {
        const faceData =
            getCurrentFaceData();

        if (!faceData) {
            return;
        }

        currentCandidate = (
            currentCandidatesByFace[
                currentFace
            ]
            || null
        );

        titleElement.textContent =
            faceData.title
            || currentData.title
            || "Image Upscaling";

        if (
            faceData.source
            && faceData.source.src
        ) {
            sourceImage.src =
                faceData.source.src;
        }

        let displayedOutput = (
            currentCandidate
            || faceData.current_upscaled
        );

        let outputLabelText = (
            currentCandidate
                ? "Upscaled — Candidate"
                : (
                    faceData.current_upscaled
                        ? "Upscaled — In Use"
                        : "Upscaled Output"
                )
        );

        if (
            currentData
            && currentData
                .show_generated_bleed_in_upscale_window
            && displayedOutput
            && displayedOutput
                .has_generated_bleed
            && displayedOutput
                .fullbleed_src
        ) {
            displayedOutput = Object.assign(
                {},
                displayedOutput,
                {
                    src: (
                        displayedOutput
                            .fullbleed_src
                    ),

                    showing_generated_bleed:
                        true
                }
            );

            outputLabelText = (
                currentCandidate
                    ? "Upscaled + Bleed — Candidate"
                    : "Upscaled + Bleed — In Use"
            );
        }

        showOutput(
            displayedOutput,
            outputLabelText
        );

        revertButton.classList.toggle(
            "hidden",

            Boolean(
                Object.keys(
                    currentCandidatesByFace
                ).length
            )
            || !faceData.current_upscaled
        );

        updateFaceFlipButton();
    }


    function flipActiveFace() {
        if (
            !currentData
            || !currentData.is_dual_faced
            || !currentData.faces
        ) {
            return;
        }

        saveCurrentFaceFeedback();

        currentFace = (
            currentFace === "back"
                ? "front"
                : "back"
        );

        renderActiveFace();
    }

    function populateModels(data) {
        modelSelect.innerHTML = "";

        (data.plugins || []).forEach(
            function (plugin) {
                (plugin.models || []).forEach(
                    function (model) {
                        const capabilities =
                            model.capabilities
                            || {};

                        if (
                            capabilities.batch
                            !== true
                        ) {
                            return;
                        }

                        if (
                            data.is_dual_faced
                            && capabilities.double_faced
                            !== true
                        ) {
                            return;
                        }

                        const option =
                            document.createElement(
                                "option"
                            );

                        option.value =
                            plugin.plugin_id
                            + "::"
                            + model.model_id;

                        option.dataset.capabilities =
                            JSON.stringify(
                                capabilities
                            );

                        option.dataset.requirements =
                            JSON.stringify(
                                model.requirements
                                || {}
                            );

                        option.textContent =
                            plugin.plugin_name
                            + " — "
                            + model.label;

                        modelSelect.appendChild(
                            option
                        );
                    }
                );
            }
        );

        const savedModelValue =
            getSavedUpscaleModel();

        if (savedModelValue) {
            const savedOptionExists =
                Array.from(
                    modelSelect.options
                ).some(
                    function (option) {
                        return (
                            option.value
                            === savedModelValue
                        );
                    }
                );

            if (savedOptionExists) {
                modelSelect.value =
                    savedModelValue;
            }
        }

        runButton.disabled =
            modelSelect.options.length === 0;
    }

    function showOutput(
        output,
        label
    ) {
        if (!output || !output.src) {
            outputImage.src = "";
            outputImage.classList.add(
                "hidden"
            );

            outputLabel.textContent =
                "Upscaled Output";

            outputMeta.textContent =
                "No accepted upscale.";

            return;
        }

        outputImage.classList.remove(
            "hidden"
        );

        outputImage.src = output.src;

        outputLabel.textContent =
            label;

        const metaParts = [];

        if (
            !output.showing_generated_bleed
            && output.width
            && output.height
        ) {
            metaParts.push(
                output.width
                + " × "
                + output.height
            );
        }

        if (
            output.showing_generated_bleed
        ) {
            if (
                output.bleed_size_mm
                !== undefined
                && output.bleed_size_mm
                !== null
            ) {
                metaParts.push(
                    output.bleed_size_mm
                    + " mm generated bleed"
                );
            }

            else {
                metaParts.push(
                    "Generated bleed"
                );
            }
        }

        if (output.model_label) {
            metaParts.push(
                output.model_label
            );
        }

        if (
            output.processing_ms
            !== undefined
            && output.processing_ms
            !== null
        ) {
            metaParts.push(
                output.processing_ms
                + " ms"
            );
        }

        if (
            output.peak_gpu_memory_mb
            !== undefined
            && output.peak_gpu_memory_mb
            !== null
        ) {
            metaParts.push(
                output.peak_gpu_memory_mb
                + " MB GPU"
            );
        }

        const holofoilStamp = (
            output.holofoil_stamp
            && typeof output.holofoil_stamp
            === "object"
        )
            ? output.holofoil_stamp
            : null;

        if (
            holofoilStamp
            && holofoilStamp.replacement
            === "background"
        ) {
            metaParts.push(
                holofoilStamp.detected
                    ? "Holofoil removed"
                    : "Holofoil not detected"
            );
        }

        outputMeta.textContent =
            metaParts.join(" • ");
    }

    function renderControl(data) {
        currentData = data;
        currentCandidate = null;

        currentCandidatesByFace = {};
        feedbackByFace = {};

        currentFace = (
            data.face === "back"
                ? "back"
                : "front"
        );

        setDevFeedbackVisible(
            false
        );

        resetDevFeedback();

        populateModels(data);

        renderActiveFace();

        acceptButton.classList.add(
            "hidden"
        );

        discardButton.classList.add(
            "hidden"
        );

        const faceData =
            getCurrentFaceData();

        statusElement.textContent =
            faceData
            && faceData.current_upscaled
                ? (
                    data.is_dual_faced
                        ? "An accepted upscale is currently in use for this face. Use the flip button to review both sides."
                        : "An accepted upscale is currently in use."
                )
                : (
                    data.is_dual_faced
                        ? "Choose a model and run one batch to upscale both faces."
                        : "Choose a model and run an upscale."
                );
    }

    async function loadControl() {
        const response = await fetch(
            currentControlUrl,
            {
                cache: "no-store"
            }
        );

        const data =
            await response.json();

        if (
            !response.ok
            || !data.ok
        ) {
            throw new Error(
                data.message
                || "Could not load Upscale controls."
            );
        }

        renderControl(data);
    }

    async function openUrl(url) {
        ensureViewer();

        currentControlUrl =
            String(url || "").trim();

        if (!currentControlUrl) {
            return;
        }

        overlay.classList.remove(
            "hidden"
        );

        overlay.setAttribute(
            "aria-hidden",
            "false"
        );

        statusElement.textContent =
            "Loading Upscale controls...";

        await loadControl();
    }

    function closeViewer() {
        if (!overlay) {
            return;
        }

        closeZoomViewer();
        stopRunStatusTracking();

        overlay.classList.add(
            "hidden"
        );

        overlay.setAttribute(
            "aria-hidden",
            "true"
        );
    }

    async function runUpscale() {
        const selectedValue =
            modelSelect.value || "";

        saveUpscaleModel(
            selectedValue
        );

        const separatorIndex =
            selectedValue.indexOf("::");

        if (separatorIndex === -1) {
            return;
        }

        const pluginId =
            selectedValue.substring(
                0,
                separatorIndex
            );

        const modelId =
            selectedValue.substring(
                separatorIndex + 2
            );

        setBusy(true);

        stopRunStatusTracking();

        activeRunId = (
            "manual-"
            + Date.now()
            + "-"
            + Math.random()
                .toString(36)
                .slice(2, 10)
        );

        setUpscaleStatus(
            "Starting",
            (
                currentData.is_dual_faced
                    ? "Preparing front and back Upscale..."
                    : "Preparing Upscale..."
            ),
            {
                busy: true,
                percent: 2
            }
        );

        startRunElapsedTimer();

        runStatusPollTimer =
            window.setTimeout(
                pollRunStatus,
                250
            );

        try {
            const response = await fetch(
                currentData.run_url,
                {
                    method: "POST",
                    headers: {
                        "Content-Type":
                            "application/json",
                        "Accept":
                            "application/json"
                    },
                    body: JSON.stringify({
                        plugin_id: pluginId,
                        model_id: modelId,
                        face: currentData.face,
                        run_id: activeRunId
                    })
                }
            );

            const data =
                await response.json();

            if (
                !response.ok
                || !data.ok
            ) {
                throw new Error(
                    data.message
                    || "Upscale failed."
                );
            }

            currentCandidatesByFace = (
                data.candidates
                && typeof data.candidates
                === "object"
            )
                ? data.candidates
                : {};

            if (
                !Object.keys(
                    currentCandidatesByFace
                ).length
                && data.candidate
            ) {
                currentCandidatesByFace[
                    currentFace
                ] = data.candidate;
            }

            feedbackByFace = {};

            currentCandidate = (
                currentCandidatesByFace[
                    currentFace
                ]
                || data.candidate
                || null
            );

            renderActiveFace();

            acceptButton.classList.remove(
                "hidden"
            );

            discardButton.classList.remove(
                "hidden"
            );

            if (
                currentData.dev_feedback_enabled
            ) {
                resetDevFeedback();

                setDevFeedbackVisible(
                    true
                );
            }

            const timingSummary =
                formatUpscaleTimingSummary(
                    data.timings_ms
                );

            stopRunStatusTracking();

            setUpscaleStatus(
                "Ready for Review",
                (
                    data.is_dual_faced
                        ? "Front and back candidates are ready. Use the flip button to review both faces."
                        : "Review the candidate beside the original."
                )
                + (
                    timingSummary
                        ? " " + timingSummary + "."
                        : ""
                ),
                {
                    busy: false,
                    percent: 100
                }
            );

        } catch (error) {
            stopRunStatusTracking();

            setUpscaleStatus(
                "Upscale Failed",
                error.message,
                {
                    busy: false,
                    percent: 100,
                    error: true
                }
            );
        }

        setBusy(false);
    }

    function setPageUpscaleIndicator(
        cardUuid,
        isActive
    ) {
        if (!cardUuid) {
            return;
        }

        document.querySelectorAll(
            '.imomir-upscale-card-button[data-card-uuid="'
            + CSS.escape(cardUuid)
            + '"]'
        ).forEach(function (button) {
            button.classList.toggle(
                "imomir-upscale-card-active",
                Boolean(isActive)
            );

            const row = button.closest(
                ".custom-draft-current-card-row"
            );

            if (row) {
                row.dataset.hasUpscaledImage =
                    isActive ? "1" : "0";
            }
        });
    }

    async function acceptCandidate() {
        const candidateEntries =
            Object.entries(
                currentCandidatesByFace
                || {}
            );

        if (
            !candidateEntries.length
            && currentCandidate
        ) {
            candidateEntries.push([
                currentFace,
                currentCandidate
            ]);
        }

        if (!candidateEntries.length) {
            return;
        }

        saveCurrentFaceFeedback();

        setBusy(true);

        try {
            const batchCandidates =
                candidateEntries.map(
                    function (
                        [
                            face,
                            candidate
                        ]
                    ) {
                        let feedback = null;

                        if (
                            currentData
                            && currentData
                                .dev_feedback_enabled
                        ) {
                            if (
                                feedbackByFace[
                                    face
                                ]
                            ) {
                                feedback =
                                    feedbackByFace[
                                        face
                                    ];

                            } else if (
                                face
                                === currentFace
                            ) {
                                feedback =
                                    collectDevFeedback();
                            }
                        }

                        return {
                            face: face,

                            upscaled_image_id:
                                candidate
                                    .upscaled_image_id,

                            feedback: feedback
                        };
                    }
                );

            const response = await fetch(
                (
                    currentData
                    && currentData
                        .accept_batch_url
                )
                    || (
                        "/upscaling/"
                        + "candidates/"
                        + "accept-batch"
                    ),
                {
                    method: "POST",

                    headers: {
                        "Content-Type":
                            "application/json",

                        "Accept":
                            "application/json"
                    },

                    body: JSON.stringify({
                        candidates:
                            batchCandidates
                    })
                }
            );

            const rawResponse =
                await response.text();

            let data = {};

            try {
                data = rawResponse
                    ? JSON.parse(
                        rawResponse
                    )
                    : {};

            } catch (parseError) {
                throw new Error(
                    "Accept Upscale returned "
                    + "an invalid server "
                    + "response."
                );
            }

            if (
                !response.ok
                || !data.ok
            ) {
                throw new Error(
                    data.message
                    || (
                        "Could not accept "
                        + "Upscale batch."
                    )
                );
            }

            setPageUpscaleIndicator(
                currentData.card_uuid,
                true
            );
            currentCandidate = null;
            currentCandidatesByFace = {};

            closeViewer();

        } catch (error) {
            statusElement.textContent =
                error.message;

        } finally {
            setBusy(false);
        }
    }

    async function discardCandidate() {
        if (!currentCandidate) {
            return;
        }

        setBusy(true);

        try {
            const response = await fetch(
                currentCandidate.discard_url,
                {
                    method: "POST",
                    headers: {
                        "Content-Type":
                            "application/json",
                        "Accept":
                            "application/json"
                    },
                    body: JSON.stringify({
                        feedback: (
                            currentData.dev_feedback_enabled
                            ? collectDevFeedback()
                            : null
                        )
                    })
                }
            );

            const data =
                await response.json();

            if (
                !response.ok
                || !data.ok
            ) {
                throw new Error(
                    data.message
                    || "Could not discard candidate."
                );
            }

            await loadControl();

            statusElement.textContent =
                data.feedback_warning
                    ? "Candidate discarded, but Dev Feedback could not be written: "
                        + data.feedback_warning
                    : "Candidate discarded.";

        } catch (error) {
            statusElement.textContent =
                error.message;
        }

        setBusy(false);
    }

    async function revertUpscale() {
        if (!currentData) {
            return;
        }

        setBusy(true);

        try {
            const response = await fetch(
                currentData.revert_url,
                {
                    method: "POST",
                    headers: {
                        "Content-Type":
                            "application/json",
                        "Accept":
                            "application/json"
                    },
                    body: JSON.stringify({
                        face: currentData.face
                    })
                }
            );

            const data =
                await response.json();

            if (
                !response.ok
                || !data.ok
            ) {
                throw new Error(
                    data.message
                    || "Could not revert Upscale."
                );
            }

            await loadControl();

            setPageUpscaleIndicator(
                currentData.card_uuid,
                false
            );

            statusElement.textContent =
                "Reverted to the original Scryfall image.";

        } catch (error) {
            statusElement.textContent =
                error.message;
        }

        setBusy(false);
    }

    document.addEventListener(
        "click",
        function (event) {
            const trigger =
                event.target.closest(
                    "[data-upscale-control-url]"
                );

            if (!trigger) {
                return;
            }

            const url =
                trigger.dataset
                    .upscaleControlUrl
                || "";

            if (!url) {
                return;
            }

            event.preventDefault();
            event.stopPropagation();

            openUrl(url).catch(
                function (error) {
                    window.alert(
                        error.message
                    );
                }
            );
        }
    );

    window.iMomirUpscaleControl = {
        openUrl: openUrl,
        close: closeViewer
    };
})();