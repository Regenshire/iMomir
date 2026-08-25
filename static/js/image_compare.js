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

    let statusElement = null;

    let acceptButton = null;
    let discardButton = null;
    let revertButton = null;

    let currentControlUrl = "";
    let currentData = null;
    let currentCandidate = null;

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

        header.appendChild(titleElement);
        header.appendChild(closeButton);

        const controls =
            document.createElement("div");

        controls.className =
            "imomir-upscale-controls";

        modelSelect =
            document.createElement("select");

        modelSelect.className =
            "imomir-upscale-model-select";

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

        grid.appendChild(sourcePanel);
        grid.appendChild(outputPanel);

        statusElement =
            document.createElement("div");

        statusElement.className =
            "imomir-upscale-status";

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
        dialog.appendChild(statusElement);
        dialog.appendChild(footer);

        overlay.appendChild(dialog);
        document.body.appendChild(overlay);

        closeButton.addEventListener(
            "click",
            closeViewer
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
                    event.key === "Escape"
                    && overlay
                    && !overlay.classList.contains(
                        "hidden"
                    )
                ) {
                    closeViewer();
                }
            }
        );
    }

    function setBusy(isBusy) {
        runButton.disabled = isBusy;
        modelSelect.disabled = isBusy;
        acceptButton.disabled = isBusy;
        discardButton.disabled = isBusy;
        revertButton.disabled = isBusy;
    }

    function populateModels(data) {
        modelSelect.innerHTML = "";

        (data.plugins || []).forEach(
            function (plugin) {
                (plugin.models || []).forEach(
                    function (model) {
                        const option =
                            document.createElement(
                                "option"
                            );

                        option.value =
                            plugin.plugin_id
                            + "::"
                            + model.model_id;

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
            output.width
            && output.height
        ) {
            metaParts.push(
                output.width
                + " × "
                + output.height
            );
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

        outputMeta.textContent =
            metaParts.join(" • ");
    }

    function renderControl(data) {
        currentData = data;
        currentCandidate = null;

        titleElement.textContent =
            data.title
            || "Image Upscaling";

        sourceImage.src =
            data.source.src;

        populateModels(data);

        showOutput(
            data.current_upscaled,
            data.current_upscaled
                ? "Upscaled — In Use"
                : "Upscaled Output"
        );

        acceptButton.classList.add(
            "hidden"
        );

        discardButton.classList.add(
            "hidden"
        );

        revertButton.classList.toggle(
            "hidden",
            !data.current_upscaled
        );

        statusElement.textContent =
            data.current_upscaled
                ? "An accepted upscale is currently in use."
                : "Choose a model and run an upscale.";
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

        statusElement.textContent =
            "Running Upscale...";

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
                    || "Upscale failed."
                );
            }

            currentCandidate =
                data.candidate;

            showOutput(
                currentCandidate,
                "Upscaled — Candidate"
            );

            acceptButton.classList.remove(
                "hidden"
            );

            discardButton.classList.remove(
                "hidden"
            );

            statusElement.textContent =
                "Review the candidate beside the original.";

        } catch (error) {
            statusElement.textContent =
                error.message;
        }

        setBusy(false);
    }

    async function acceptCandidate() {
        if (!currentCandidate) {
            return;
        }

        setBusy(true);

        try {
            const response = await fetch(
                currentCandidate.accept_url,
                {
                    method: "POST",
                    headers: {
                        "Accept":
                            "application/json"
                    }
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
                    || "Could not accept Upscale."
                );
            }

            await loadControl();

            statusElement.textContent =
                "Upscaled image accepted and is now in use.";

        } catch (error) {
            statusElement.textContent =
                error.message;
        }

        setBusy(false);
    }

    async function discardCandidate() {
        if (!currentCandidate) {
            return;
        }

        setBusy(true);

        try {
            await fetch(
                currentCandidate.discard_url,
                {
                    method: "POST",
                    headers: {
                        "Accept":
                            "application/json"
                    }
                }
            );

            await loadControl();

            statusElement.textContent =
                "Candidate discarded.";

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