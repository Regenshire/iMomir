(function () {
    const START_URL =
        "/upscaling/batch/start";

    const STATUS_URL =
        "/upscaling/batch/status";

    const MODELS_URL =
        "/upscaling/models";

    const DELETE_URL =
        "/upscaling/batch/delete";

    const LAST_MODEL_STORAGE_KEY =
        "imomir_last_upscale_model";

    let overlay = null;
    let titleElement = null;
    let messageElement = null;
    let detailElement = null;
    let inputWrap = null;
    let countInput = null;

    let modelWrap = null;
    let modelSelect = null;

    let holofoilWrap = null;
    let holofoilSelect = null;
    let holofoilEnabled = false;

    let replaceWrap = null;
    let replaceCheckbox = null;

    let progressWrap = null;
    let progressBar = null;
    let progressText = null;
    let currentCardElement = null;
    let countersElement = null;
    let modelElement = null;
    let failuresElement = null;
    let deleteButton = null;
    let cancelButton = null;
    let startButton = null;
    let refreshButton = null;
    let closeButton = null;

    let pendingRequest = null;
    let pollTimer = null;

    let deleteConfirmationArmed =
        false;

    function ensureModal() {
        if (overlay) {
            return;
        }

        overlay = document.createElement("div");
        overlay.className = "imomir-batch-upscale-overlay hidden";
        overlay.setAttribute("aria-hidden", "true");

        const dialog = document.createElement("div");
        dialog.className = "imomir-batch-upscale-dialog";
        dialog.setAttribute("role", "dialog");
        dialog.setAttribute("aria-modal", "true");

        const header = document.createElement("div");
        header.className = "imomir-batch-upscale-header";

        titleElement = document.createElement("div");
        titleElement.className = "imomir-batch-upscale-title";
        titleElement.textContent = "Batch Upscale";

        const headerClose = document.createElement("button");
        headerClose.type = "button";
        headerClose.className = "imomir-batch-upscale-close";
        headerClose.innerHTML = "&times;";
        headerClose.setAttribute("aria-label", "Close Batch Upscale");

        header.appendChild(titleElement);
        header.appendChild(headerClose);

        const body = document.createElement("div");
        body.className = "imomir-batch-upscale-body";

        messageElement = document.createElement("div");
        messageElement.className = "imomir-batch-upscale-message";

        detailElement = document.createElement("div");
        detailElement.className = "imomir-batch-upscale-detail";

        inputWrap = document.createElement("label");
        inputWrap.className = "imomir-batch-upscale-count-wrap hidden";

        const inputLabel = document.createElement("span");
        inputLabel.textContent = "Number of cards";

        countInput = document.createElement("input");
        countInput.type = "number";
        countInput.min = "1";
        countInput.max = "5000";
        countInput.step = "1";
        countInput.value = "100";
        countInput.className = "imomir-batch-upscale-count-input";

        inputWrap.appendChild(inputLabel);
        inputWrap.appendChild(countInput);

        modelWrap =
            document.createElement(
                "label"
            );

        modelWrap.className =
            "imomir-batch-upscale-model-wrap";

        const modelLabel =
            document.createElement(
                "span"
            );

        modelLabel.textContent =
            "Upscale Model";

        modelSelect =
            document.createElement(
                "select"
            );

        modelSelect.className =
            "imomir-batch-upscale-model-select";

        modelSelect.disabled = true;

        modelWrap.appendChild(
            modelLabel
        );

        modelWrap.appendChild(
            modelSelect
        );

        holofoilWrap =
            document.createElement(
                "label"
            );

        holofoilWrap.className =
            "imomir-batch-upscale-model-wrap hidden";

        const holofoilLabel =
            document.createElement(
                "span"
            );

        holofoilLabel.textContent =
            "Holofoil Stamp";

        holofoilSelect =
            document.createElement(
                "select"
            );

        holofoilSelect.className =
            "imomir-batch-upscale-model-select";

        const holofoilKeepOption =
            document.createElement(
                "option"
            );

        holofoilKeepOption.value =
            "none";

        holofoilKeepOption.textContent =
            "Holofoil Keep";

        const holofoilBackgroundOption =
            document.createElement(
                "option"
            );

        holofoilBackgroundOption.value =
            "background";

        holofoilBackgroundOption.textContent =
            "Background Color";

        holofoilSelect.appendChild(
            holofoilKeepOption
        );

        holofoilSelect.appendChild(
            holofoilBackgroundOption
        );

        holofoilWrap.appendChild(
            holofoilLabel
        );

        holofoilWrap.appendChild(
            holofoilSelect
        );

        replaceWrap =
            document.createElement(
                "label"
            );

        replaceWrap.className =
            "imomir-batch-upscale-replace-wrap";

        replaceCheckbox =
            document.createElement(
                "input"
            );

        replaceCheckbox.type =
            "checkbox";

        replaceCheckbox.checked =
            false;

        const replaceText =
            document.createElement(
                "span"
            );

        replaceText.textContent =
            "Replace existing Scryfall Upscales";

        replaceWrap.appendChild(
            replaceCheckbox
        );

        replaceWrap.appendChild(
            replaceText
        );

        progressWrap = document.createElement("div");
        progressWrap.className = "imomir-batch-upscale-progress-section hidden";

        const progressTrack = document.createElement("div");
        progressTrack.className = "imomir-batch-upscale-progress-track";

        progressBar = document.createElement("div");
        progressBar.className = "imomir-batch-upscale-progress-bar";
        progressBar.style.width = "0%";

        progressTrack.appendChild(progressBar);

        progressText = document.createElement("div");
        progressText.className = "imomir-batch-upscale-progress-text";

        currentCardElement = document.createElement("div");
        currentCardElement.className = "imomir-batch-upscale-current-card";

        countersElement = document.createElement("div");
        countersElement.className = "imomir-batch-upscale-counters";

        modelElement = document.createElement("div");
        modelElement.className = "imomir-batch-upscale-model";

        failuresElement = document.createElement("div");
        failuresElement.className = "imomir-batch-upscale-failures hidden";

        progressWrap.appendChild(progressTrack);
        progressWrap.appendChild(progressText);
        progressWrap.appendChild(currentCardElement);
        progressWrap.appendChild(countersElement);
        progressWrap.appendChild(modelElement);
        progressWrap.appendChild(failuresElement);

        body.appendChild(messageElement);
        body.appendChild(detailElement);
        body.appendChild(inputWrap);
        body.appendChild(modelWrap);
        body.appendChild(holofoilWrap);
        body.appendChild(replaceWrap);
        body.appendChild(progressWrap);

        const footer = document.createElement("div");
        footer.className = "imomir-batch-upscale-footer";

        deleteButton =
            document.createElement(
                "button"
            );

        deleteButton.type =
            "button";

        deleteButton.className =
            "action-button secondary-button "
            + "imomir-batch-upscale-delete-button";

        deleteButton.textContent =
            "Delete Upscales";

        cancelButton = document.createElement("button");
        cancelButton.type = "button";
        cancelButton.className = "action-button secondary-button";
        cancelButton.textContent = "Cancel";

        startButton = document.createElement("button");
        startButton.type = "button";
        startButton.className = "action-button";
        startButton.textContent = "Start Batch Upscale";

        closeButton = document.createElement("button");
        closeButton.type = "button";
        closeButton.className = "action-button secondary-button hidden";
        closeButton.textContent = "Close";

        refreshButton = document.createElement("button");
        refreshButton.type = "button";
        refreshButton.className = "action-button hidden";
        refreshButton.textContent = "Refresh Page";

        footer.appendChild(deleteButton);
        footer.appendChild(cancelButton);
        footer.appendChild(startButton);
        footer.appendChild(closeButton);
        footer.appendChild(refreshButton);

        dialog.appendChild(header);
        dialog.appendChild(body);
        dialog.appendChild(footer);
        overlay.appendChild(dialog);
        document.body.appendChild(overlay);

        headerClose.addEventListener("click", closeModal);
        cancelButton.addEventListener("click", closeModal);
        closeButton.addEventListener("click", closeModal);

        refreshButton.addEventListener("click", function () {
            window.location.reload();
        });

        deleteButton.addEventListener(
            "click",
            deletePendingUpscales
        );

        modelSelect.addEventListener(
            "change",
            function () {
                saveUpscaleModel(
                    modelSelect.value
                );
            }
        );

        startButton.addEventListener(
            "click",
            startPendingBatch
        );

        overlay.addEventListener("click", function (event) {
            if (event.target === overlay) {
                closeModal();
            }
        });

        document.addEventListener("keydown", function (event) {
            if (
                event.key === "Escape"
                && overlay
                && !overlay.classList.contains("hidden")
            ) {
                closeModal();
            }
        });
    }

    function openModal() {
        ensureModal();

        overlay.classList.remove("hidden");
        overlay.setAttribute(
            "aria-hidden",
            "false"
        );

        document.body.classList.add(
            "imomir-batch-upscale-open"
        );
    }

    function closeModal() {
        if (!overlay) {
            return;
        }

        overlay.classList.add("hidden");

        overlay.setAttribute(
            "aria-hidden",
            "true"
        );

        document.body.classList.remove(
            "imomir-batch-upscale-open"
        );

        if (pollTimer) {
            window.clearTimeout(
                pollTimer
            );

            pollTimer = null;
        }
    }

    function getSavedUpscaleModel() {
        try {
            return String(
                window.localStorage.getItem(
                    LAST_MODEL_STORAGE_KEY
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
                LAST_MODEL_STORAGE_KEY,
                cleanValue
            );

        } catch (error) {
            console.warn(
                "Could not save Upscale model.",
                error
            );
        }
    }


    async function loadBatchModels() {
        modelSelect.innerHTML = "";
        modelSelect.disabled = true;
        holofoilEnabled = false;
        holofoilWrap.classList.add(
            "hidden"
        );
        holofoilSelect.disabled = true;
        startButton.disabled = true;

        try {
            const response = await fetch(
                MODELS_URL,
                {
                    headers: {
                        "Accept":
                            "application/json"
                    },

                    cache: "no-store"
                }
            );

            const data =
                await parseJsonResponse(
                    response
                );

            if (
                !response.ok
                || !data.ok
            ) {
                throw new Error(
                    data.message
                    || (
                        "Could not load "
                        + "Upscale models."
                    )
                );
            }

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

            if (
                !modelSelect.options.length
            ) {
                throw new Error(
                    "No Upscale models "
                    + "are available."
                );
            }

            const savedModel =
                getSavedUpscaleModel();

            if (savedModel) {
                const savedOptionExists =
                    Array.from(
                        modelSelect.options
                    ).some(
                        function (option) {
                            return (
                                option.value
                                === savedModel
                            );
                        }
                    );

                if (savedOptionExists) {
                    modelSelect.value =
                        savedModel;
                }
            }

            holofoilEnabled = Boolean(
                data.holofoil_stamp_enabled
            );

            const defaultHolofoilReplacement = String(
                data.holofoil_stamp_replacement_default
                || "none"
            ).trim().toLowerCase();

            holofoilSelect.value = (
                defaultHolofoilReplacement
                === "background"
                    ? "background"
                    : "none"
            );

            holofoilWrap.classList.toggle(
                "hidden",
                !holofoilEnabled
            );

            modelSelect.disabled =
                false;

            holofoilSelect.disabled =
                !holofoilEnabled;

            startButton.disabled =
                false;

        } catch (error) {
            messageElement.textContent =
                error.message;

            holofoilEnabled = false;

            holofoilWrap.classList.add(
                "hidden"
            );

            modelSelect.disabled =
                true;

            holofoilSelect.disabled =
                true;

            startButton.disabled =
                true;
        }
    }

    function setConfirmationMode(options) {
        pendingRequest = options;

        deleteConfirmationArmed =
            false;

        openModal();

        titleElement.textContent =
            options.title
            || "Batch Upscale";

        messageElement.textContent =
            options.message
            || "Start a batch upscale?";

        detailElement.textContent = (
            "This can take a substantial amount of time. "
            + "Cards currently using an Alternate Image are always skipped. "
            + "With Replace disabled, cards that already have an accepted Upscale are skipped. "
            + "With Replace enabled, existing Scryfall Upscales are replaced by the newly generated result. "
            + "Successful cards are accepted automatically and failures do not stop the batch."
        );

        inputWrap.classList.toggle(
            "hidden",
            options.mode !== "next"
        );

        modelWrap.classList.remove(
            "hidden"
        );

        holofoilWrap.classList.add(
            "hidden"
        );

        replaceWrap.classList.remove(
            "hidden"
        );

        replaceCheckbox.checked =
            false;

        progressWrap.classList.add(
            "hidden"
        );

        failuresElement.classList.add(
            "hidden"
        );

        if (options.mode === "next") {
            countInput.value = String(
                options.defaultLimit
                || 100
            );
        }

        deleteButton.classList.remove(
            "hidden"
        );

        deleteButton.disabled =
            false;

        deleteButton.textContent =
            "Delete Upscales";

        cancelButton.classList.remove(
            "hidden"
        );

        startButton.classList.remove(
            "hidden"
        );

        startButton.disabled =
            true;

        startButton.classList.remove(
            "action-button-loading"
        );

        startButton.textContent =
            "Start Batch Upscale";

        closeButton.classList.add(
            "hidden"
        );

        refreshButton.classList.add(
            "hidden"
        );

        loadBatchModels();
    }

    function setProgressMode() {
        inputWrap.classList.add(
            "hidden"
        );

        modelWrap.classList.add(
            "hidden"
        );

        holofoilWrap.classList.add(
            "hidden"
        );

        replaceWrap.classList.add(
            "hidden"
        );

        deleteButton.classList.add(
            "hidden"
        );

        progressWrap.classList.remove(
            "hidden"
        );

        cancelButton.classList.add(
            "hidden"
        );

        startButton.classList.add(
            "hidden"
        );

        closeButton.classList.remove(
            "hidden"
        );

        refreshButton.classList.add(
            "hidden"
        );
    }

    function setCompleteMode() {
        closeButton.classList.remove(
            "hidden"
        );

        refreshButton.classList.remove(
            "hidden"
        );
    }

    function normalizeCardUuids(cardUuids) {
        const seen = new Set();
        const result = [];

        (cardUuids || []).forEach(
            function (rawValue) {
                const value = String(
                    rawValue || ""
                ).trim();

                if (
                    !value
                    || seen.has(value)
                ) {
                    return;
                }

                seen.add(value);
                result.push(value);
            }
        );

        return result;
    }

    async function parseJsonResponse(
        response
    ) {
        const rawText =
            await response.text();

        if (!rawText) {
            return {};
        }

        try {
            return JSON.parse(
                rawText
            );

        } catch (error) {
            throw new Error(
                "Batch Upscale returned an invalid server response."
            );
        }
    }

    function buildPendingScopePayload() {
        if (!pendingRequest) {
            return null;
        }

        const payload = {
            source_label:
                pendingRequest.sourceLabel
                || pendingRequest.title
                || "Batch Upscale"
        };

        if (
            pendingRequest.mode
            === "next"
        ) {
            let limitValue =
                Number.parseInt(
                    countInput.value
                    || "100",
                    10
                );

            if (
                !Number.isFinite(
                    limitValue
                )
            ) {
                limitValue = 100;
            }

            limitValue = Math.max(
                1,
                Math.min(
                    5000,
                    limitValue
                )
            );

            countInput.value =
                String(
                    limitValue
                );

            payload.limit =
                limitValue;

        } else if (
            pendingRequest.mode
            === "pack"
        ) {
            const trackedPackId =
                Number.parseInt(
                    pendingRequest
                        .trackedPackId,
                    10
                );

            if (
                !Number.isFinite(
                    trackedPackId
                )
                || trackedPackId <= 0
            ) {
                throw new Error(
                    "The selected pack "
                    + "could not be identified."
                );
            }

            payload.tracked_pack_id =
                trackedPackId;

        } else {
            payload.card_uuids =
                normalizeCardUuids(
                    pendingRequest
                        .cardUuids
                );

            if (
                !payload
                    .card_uuids
                    .length
            ) {
                throw new Error(
                    "No cards were selected "
                    + "for Batch Upscale."
                );
            }
        }

        return payload;
    }

    async function startPendingBatch() {
        if (!pendingRequest) {
            return;
        }

        const selectedValue = String(
            modelSelect.value
            || ""
        ).trim();

        const separatorIndex =
            selectedValue.indexOf(
                "::"
            );

        if (separatorIndex <= 0) {
            messageElement.textContent =
                "Select an Upscale model.";

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

        let payload = null;

        try {
            payload =
                buildPendingScopePayload();

        } catch (error) {
            messageElement.textContent =
                error.message;

            return;
        }

        payload.plugin_id =
            pluginId;

        payload.model_id =
            modelId;

        payload.replace_existing =
            Boolean(
                replaceCheckbox.checked
            );

        payload.holofoil_stamp_replacement = (
            holofoilEnabled
                ? String(
                    holofoilSelect.value
                    || "none"
                ).trim().toLowerCase()
                : "none"
        );

        saveUpscaleModel(
            selectedValue
        );

        startButton.disabled = true;

        startButton.classList.add(
            "action-button-loading"
        );

        startButton.textContent =
            "Starting...";

        try {
            const response = await fetch(
                START_URL,
                {
                    method: "POST",

                    headers: {
                        "Accept":
                            "application/json",

                        "Content-Type":
                            "application/json"
                    },

                    body: JSON.stringify(
                        payload
                    )
                }
            );

            const data =
                await parseJsonResponse(
                    response
                );

            if (
                response.status === 409
                && data.status
            ) {
                setProgressMode();

                renderStatus(
                    data.status
                );

                schedulePoll();

                return;
            }

            if (
                !response.ok
                || !data.ok
            ) {
                throw new Error(
                    data.message
                    || (
                        "Could not start "
                        + "Batch Upscale."
                    )
                );
            }

            if (!data.started) {
                setProgressMode();

                messageElement.textContent =
                    data.message
                    || (
                        "No eligible cards "
                        + "were found."
                    );

                renderStatus(
                    data.status
                    || {}
                );

                setCompleteMode();

                return;
            }

            setProgressMode();

            renderStatus(
                data.status
                || {}
            );

            schedulePoll();

        } catch (error) {
            messageElement.textContent =
                error.message
                || (
                    "Could not start "
                    + "Batch Upscale."
                );

            startButton.disabled =
                false;

            startButton.classList.remove(
                "action-button-loading"
            );

            startButton.textContent =
                "Start Batch Upscale";
        }
    }

    async function deletePendingUpscales() {
        if (!pendingRequest) {
            return;
        }

        if (!deleteConfirmationArmed) {
            deleteConfirmationArmed =
                true;

            deleteButton.textContent =
                "Confirm Delete Upscales";

            detailElement.textContent = (
                "This permanently removes all generated Scryfall Upscale files and database records for the cards in this scope. "
                + "Alternate Images are not deleted. "
                + "Cards without an Alternate Image will fall back to Scryfall. "
                + "Click Confirm Delete Upscales to continue."
            );

            return;
        }

        let payload = null;

        try {
            payload =
                buildPendingScopePayload();

        } catch (error) {
            messageElement.textContent =
                error.message;

            return;
        }

        deleteButton.disabled =
            true;

        deleteButton.textContent =
            "Deleting...";

        try {
            const response = await fetch(
                DELETE_URL,
                {
                    method: "POST",

                    headers: {
                        "Accept":
                            "application/json",

                        "Content-Type":
                            "application/json"
                    },

                    body: JSON.stringify(
                        payload
                    )
                }
            );

            const data =
                await parseJsonResponse(
                    response
                );

            if (
                !response.ok
                || !data.ok
            ) {
                throw new Error(
                    data.message
                    || (
                        "Could not delete "
                        + "Upscales."
                    )
                );
            }

            messageElement.textContent =
                data.message
                || "Upscales deleted.";

            detailElement.textContent = (
                "Deleted files: "
                + Number(
                    data.deleted_files
                    || 0
                )
                + ". Cards now use their normal image-source hierarchy."
            );

            deleteConfirmationArmed =
                false;

            deleteButton.disabled =
                false;

            deleteButton.textContent =
                "Delete Upscales";

            refreshButton.classList.remove(
                "hidden"
            );

        } catch (error) {
            messageElement.textContent =
                error.message;

            deleteConfirmationArmed =
                false;

            deleteButton.disabled =
                false;

            deleteButton.textContent =
                "Delete Upscales";
        }
    }

    function renderStatus(status) {
        status = status || {};

        const total = Number(
            status.total_cards || 0
        );

        const processed = Number(
            status.processed_cards || 0
        );

        const completed = Number(
            status.completed_cards || 0
        );

        const skipped = Number(
            status.skipped_cards || 0
        );

        const failed = Number(
            status.failed_cards || 0
        );

        const percent = (
            total > 0
                ? Math.max(
                    0,
                    Math.min(
                        100,
                        (
                            processed
                            / total
                        )
                        * 100
                    )
                )
                : 0
        );

        progressBar.style.width =
            percent.toFixed(1)
            + "%";

        progressText.textContent = (
            total > 0
                ? (
                    processed
                    + " of "
                    + total
                    + " cards processed ("
                    + Math.round(percent)
                    + "%)"
                )
                : "Preparing card list..."
        );

        messageElement.textContent =
            status.message
            || status.stage
            || "Batch Upscale";

        detailElement.textContent = (
            status.is_running
                ? (
                    "The batch continues "
                    + "even if this window "
                    + "is closed."
                )
                : (
                    status.error
                    || (
                        "Batch processing "
                        + "has finished."
                    )
                )
        );

        currentCardElement.textContent = (
            status.current_card_name
                ? (
                    "Currently Upscaling: "
                    + status
                        .current_card_name
                )
                : (
                    status.is_running
                        ? "Preparing next card..."
                        : (
                            "No card currently "
                            + "processing."
                        )
                )
        );

        countersElement.textContent = (
            "Accepted: "
            + completed
            + "  •  Skipped: "
            + skipped
            + "  •  Failed: "
            + failed
        );

        modelElement.textContent = (
            status.model_label
                ? (
                    "Model: "
                    + status.model_label
                )
                : ""
        );

        const failures = (
            Array.isArray(
                status.failure_samples
            )
                ? status.failure_samples
                : []
        );

        if (failures.length) {
            failuresElement.innerHTML =
                "";

            const heading =
                document.createElement(
                    "strong"
                );

            heading.textContent =
                "Recent Failures";

            failuresElement.appendChild(
                heading
            );

            const list =
                document.createElement(
                    "ul"
                );

            failures
                .slice(-5)
                .forEach(
                    function (failure) {
                        const item =
                            document
                                .createElement(
                                    "li"
                                );

                        item.textContent = (
                            (
                                failure
                                    .card_name
                                || failure
                                    .card_uuid
                                || "Card"
                            )
                            + ": "
                            + (
                                failure.error
                                || "Unknown error"
                            )
                        );

                        list.appendChild(
                            item
                        );
                    }
                );

            failuresElement.appendChild(
                list
            );

            failuresElement.classList
                .remove(
                    "hidden"
                );

        } else {
            failuresElement.classList
                .add(
                    "hidden"
                );
        }

        if (!status.is_running) {
            setCompleteMode();
        }
    }

    async function pollStatus() {
        try {
            const response = await fetch(
                STATUS_URL,
                {
                    headers: {
                        "Accept":
                            "application/json"
                    },

                    cache: "no-store"
                }
            );

            const data =
                await parseJsonResponse(
                    response
                );

            if (
                !response.ok
                || !data.ok
            ) {
                throw new Error(
                    data.message
                    || (
                        "Could not load "
                        + "Batch Upscale "
                        + "status."
                    )
                );
            }

            renderStatus(
                data.status
                || {}
            );

            if (
                data.status
                && data.status
                    .is_running
            ) {
                schedulePoll();
            }

        } catch (error) {
            messageElement.textContent =
                error.message
                || (
                    "Could not load "
                    + "Batch Upscale "
                    + "status."
                );

            schedulePoll(
                2000
            );
        }
    }

    function schedulePoll(delay) {
        if (pollTimer) {
            window.clearTimeout(
                pollTimer
            );
        }

        pollTimer =
            window.setTimeout(
                pollStatus,
                Number(
                    delay || 750
                )
            );
    }

    function requestCards(
        cardUuids,
        options
    ) {
        const normalized =
            normalizeCardUuids(
                cardUuids
            );

        const settings =
            options || {};

        if (!normalized.length) {
            if (window.iMomirToast) {
                window.iMomirToast.error(
                    "Select at least one card first."
                );
            }

            return;
        }

        setConfirmationMode({
            mode: "cards",

            title:
                settings.title
                || "Batch Upscale Cards",

            sourceLabel:
                settings.sourceLabel
                || settings.title
                || "Selected Cards",

            cardUuids:
                normalized,

            message:
                settings.message
                || (
                    "Upscale all "
                    + normalized.length
                    + " card(s) in this batch?"
                )
        });
    }

    function requestTrackedPack(
        trackedPackId,
        options
    ) {
        const settings =
            options || {};

        const cleanTrackedPackId =
            Number.parseInt(
                trackedPackId,
                10
            );

        if (
            !Number.isFinite(
                cleanTrackedPackId
            )
            || cleanTrackedPackId <= 0
        ) {
            if (window.iMomirToast) {
                window.iMomirToast.error(
                    "The selected pack could not be identified."
                );
            }

            return;
        }

        setConfirmationMode({
            mode: "pack",

            title:
                settings.title
                || "Batch Upscale Pack",

            sourceLabel:
                settings.sourceLabel
                || settings.title
                || "Campaign Pack",

            trackedPackId:
                cleanTrackedPackId,

            message:
                settings.message
                || (
                    "Upscale all eligible "
                    + "cards in this pack?"
                )
        });
    }

    function requestNext(
        defaultLimit,
        options
    ) {
        const settings =
            options || {};

        setConfirmationMode({
            mode: "next",

            title:
                settings.title
                || "Upscale Next X Cards",

            sourceLabel:
                settings.sourceLabel
                || "Next Eligible Cards",

            defaultLimit:
                Number(
                    defaultLimit || 100
                ),

            message:
                settings.message
                || (
                    "Choose how many of "
                    + "the next eligible "
                    + "Scryfall cards to "
                    + "upscale."
                )
        });
    }

    window.iMomirUpscaleBatch = {
        requestCards:
            requestCards,

        requestTrackedPack:
            requestTrackedPack,

        requestNext:
            requestNext,

        openStatus:
            function () {
                openModal();
                setProgressMode();
                pollStatus();
            }
    };
})();