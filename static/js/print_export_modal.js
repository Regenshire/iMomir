(function () {
    function getElement(id) {
        return document.getElementById(id);
    }

    function getFilenameFromContentDisposition(contentDisposition, fallbackFilename) {
        const fallback = fallbackFilename || "iMomir_export";

        if (!contentDisposition) {
            return fallback;
        }

        const utfMatch = contentDisposition.match(/filename\*=UTF-8''([^;]+)/i);
        if (utfMatch && utfMatch[1]) {
            try {
                return decodeURIComponent(utfMatch[1].replace(/"/g, "").trim()) || fallback;
            } catch (error) {
                return utfMatch[1].replace(/"/g, "").trim() || fallback;
            }
        }

        const normalMatch = contentDisposition.match(/filename="?([^"]+)"?/i);
        if (normalMatch && normalMatch[1]) {
            return normalMatch[1].trim() || fallback;
        }

        return fallback;
    }

    function getErrorTextFromResponse(response, fallbackMessage) {
        return response.text().then(function (rawText) {
            const cleanText = String(rawText || "").trim();

            if (cleanText) {
                return cleanText;
            }

            return fallbackMessage || "Print / Export failed.";
        });
    }

    function initPrintExportModal(config) {
        config = config || {};

        const openButton = getElement(config.openButtonId || "deckbuilderOpenPrintExportButton");
        const modal = getElement("printExportModal");
        const backdrop = getElement("printExportBackdrop");
        const closeButton = getElement("printExportCloseButton");
        const form = getElement("printExportForm");
        const printButton = getElement("printExportPrintButton");
        const exportZipButton = getElement("printExportExportZipButton");
        const settingsToggle = getElement("printExportSettingsToggle");
        const settingsPanel = getElement("printExportSettingsPanel");

        const statusPanel = getElement("printExportStatusPanel");
        const statusTitle = getElement("printExportStatusTitle");
        const statusMessage = getElement("printExportStatusMessage");
        const statusCloseButton = getElement("printExportStatusCloseButton");

        const stepPrepare = getElement("printExportStatusStepPrepare");
        const stepGenerate = getElement("printExportStatusStepGenerate");
        const stepDeliver = getElement("printExportStatusStepDeliver");
        const stepComplete = getElement("printExportStatusStepComplete");

        let activePrintUrl = config.printUrl || "";
        let activeExportZipUrl = config.exportZipUrl || "";

        let activeObjectUrl = "";

        function showMessage(messageText, isError) {
            if (typeof config.showMessage === "function") {
                config.showMessage(messageText, isError);
                return;
            }

            if (window.iMomirToast) {
                if (isError) {
                    window.iMomirToast.error(messageText || "Print / Export failed.");
                } else {
                    window.iMomirToast.success(messageText || "Print / Export complete.");
                }
            }
        }

        function setUrls(nextPrintUrl, nextExportZipUrl) {
            activePrintUrl = nextPrintUrl || config.printUrl || "";
            activeExportZipUrl = nextExportZipUrl || config.exportZipUrl || "";
        }

        function setModalVisible(isVisible) {
            if (!modal) {
                return;
            }

            modal.classList.toggle("hidden", !isVisible);
            modal.setAttribute("aria-hidden", isVisible ? "false" : "true");
        }

        function setSettingsVisible(isVisible) {
            if (!settingsPanel || !settingsToggle) {
                return;
            }

            settingsPanel.classList.toggle("hidden", !isVisible);
            settingsPanel.setAttribute("aria-hidden", isVisible ? "false" : "true");
            settingsToggle.setAttribute("aria-expanded", isVisible ? "true" : "false");
        }

        function setButtonsWorking(isWorking) {
            [printButton, exportZipButton, openButton].forEach(function (button) {
                if (!button) {
                    return;
                }

                button.disabled = Boolean(isWorking) || button.dataset.originalDisabled === "1";
                button.classList.toggle("action-button-loading", Boolean(isWorking));
            });
        }

        function resetStatusPanel() {
            if (!statusPanel) {
                return;
            }

            statusPanel.classList.add("hidden");
            statusPanel.setAttribute("aria-hidden", "true");
            statusPanel.classList.remove("print-export-status-panel-error");
            statusPanel.classList.remove("print-export-status-panel-complete");

            if (statusTitle) {
                statusTitle.textContent = "Preparing Print / Export";
            }

            if (statusMessage) {
                statusMessage.textContent = "Preparing request...";
            }

            if (statusCloseButton) {
                statusCloseButton.classList.add("hidden");
            }

            setStatusStep("prepare");
        }

        function showStatusPanel() {
            if (!statusPanel) {
                return;
            }

            statusPanel.classList.remove("hidden");
            statusPanel.setAttribute("aria-hidden", "false");
        }

        function setStatusStep(activeStep) {
            const stepLookup = {
                prepare: stepPrepare,
                generate: stepGenerate,
                deliver: stepDeliver,
                complete: stepComplete
            };

            Object.keys(stepLookup).forEach(function (stepKey) {
                const stepElement = stepLookup[stepKey];

                if (!stepElement) {
                    return;
                }

                stepElement.classList.toggle("print-export-status-step-active", stepKey === activeStep);
                stepElement.classList.toggle(
                    "print-export-status-step-complete",
                    ["prepare", "generate", "deliver", "complete"].indexOf(stepKey)
                        < ["prepare", "generate", "deliver", "complete"].indexOf(activeStep)
                );
            });
        }

        function updateStatus(activeStep, titleText, messageText) {
            showStatusPanel();
            setStatusStep(activeStep);

            if (statusTitle) {
                statusTitle.textContent = titleText || "Print / Export";
            }

            if (statusMessage) {
                statusMessage.textContent = messageText || "";
            }
        }

        function setStatusComplete(titleText, messageText) {
            showStatusPanel();
            setStatusStep("complete");

            if (statusPanel) {
                statusPanel.classList.remove("print-export-status-panel-error");
                statusPanel.classList.add("print-export-status-panel-complete");
            }

            if (statusTitle) {
                statusTitle.textContent = titleText || "Complete";
            }

            if (statusMessage) {
                statusMessage.textContent = messageText || "The file is ready.";
            }

            if (statusCloseButton) {
                statusCloseButton.classList.remove("hidden");
            }

            setButtonsWorking(false);
        }

        function setStatusError(messageText) {
            showStatusPanel();

            if (statusPanel) {
                statusPanel.classList.remove("print-export-status-panel-complete");
                statusPanel.classList.add("print-export-status-panel-error");
            }

            if (statusTitle) {
                statusTitle.textContent = "Print / Export failed";
            }

            if (statusMessage) {
                statusMessage.textContent = messageText || "Print / Export failed.";
            }

            if (statusCloseButton) {
                statusCloseButton.classList.remove("hidden");
            }

            setButtonsWorking(false);
        }

        function revokeActiveObjectUrlLater() {
            if (!activeObjectUrl) {
                return;
            }

            const objectUrlToRevoke = activeObjectUrl;
            activeObjectUrl = "";

            window.setTimeout(function () {
                try {
                    URL.revokeObjectURL(objectUrlToRevoke);
                } catch (error) {
                    // Ignore cleanup failures.
                }
            }, 60000);
        }

        function downloadBlob(blob, filename) {
            activeObjectUrl = URL.createObjectURL(blob);

            const link = document.createElement("a");
            link.href = activeObjectUrl;
            link.download = filename || "iMomir_export.zip";
            link.style.display = "none";

            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);

            revokeActiveObjectUrlLater();
        }

        function openPdfBlob(blob) {
            activeObjectUrl = URL.createObjectURL(blob);

            const link = document.createElement("a");
            link.href = activeObjectUrl;
            link.target = "_blank";
            link.rel = "noopener";
            link.style.display = "none";

            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);

            revokeActiveObjectUrlLater();
        }

        async function runPrintExport(actionType) {
            if (!form) {
                showMessage("Print / Export form was not found.", true);
                return;
            }

            const isExport = actionType === "export";
            const actionUrl = isExport ? activeExportZipUrl : activePrintUrl;

            if (!actionUrl) {
                showMessage("Print / Export URL was not configured.", true);
                return;
            }

            resetStatusPanel();
            setButtonsWorking(true);

            updateStatus(
                "prepare",
                isExport ? "Preparing Export" : "Preparing PDF",
                "Collecting selected settings..."
            );

            const formData = new FormData(form);

            window.setTimeout(function () {
                updateStatus(
                    "generate",
                    isExport ? "Generating Zip Export" : "Generating PDF",
                    isExport
                        ? "Rendering card images and building XML files. This may take a while for larger decks."
                        : "Rendering card images into a PDF. This may take a while for larger decks."
                );
            }, 150);

            try {
                const response = await fetch(actionUrl, {
                    method: "POST",
                    body: formData,
                    headers: {
                        "X-Requested-With": "XMLHttpRequest"
                    }
                });

                if (!response.ok) {
                    const errorText = await getErrorTextFromResponse(
                        response,
                        isExport ? "Export to Zip failed." : "Print to PDF failed."
                    );

                    throw new Error(errorText);
                }

                updateStatus(
                    "deliver",
                    isExport ? "Downloading Zip" : "Opening PDF",
                    isExport
                        ? "The zip file is ready. Starting download..."
                        : "The PDF is ready. Opening in a new tab..."
                );

                const blob = await response.blob();

                const contentDisposition = response.headers.get("Content-Disposition") || "";
                const fallbackFilename = isExport ? "iMomir_image_export.zip" : "iMomir_print.pdf";
                const filename = getFilenameFromContentDisposition(contentDisposition, fallbackFilename);

                if (isExport) {
                    downloadBlob(blob, filename);
                } else {
                    openPdfBlob(blob);
                }

                setStatusComplete(
                    isExport ? "Export Complete" : "PDF Ready",
                    isExport
                        ? "The zip export has been downloaded."
                        : "The PDF has been opened in a new tab."
                );

                showMessage(
                    isExport ? "Export to Zip complete." : "PDF generated.",
                    false
                );
            } catch (error) {
                console.error(error);
                setStatusError(error.message || "Print / Export failed.");
                showMessage(error.message || "Print / Export failed.", true);
            }
        }

        function bindEvents() {
            if (openButton) {
                openButton.addEventListener("click", function (event) {
                    event.preventDefault();
                    event.stopPropagation();

                    if (form) {
                        form.reset();
                    }

                    if (typeof config.beforeOpen === "function") {
                        config.beforeOpen();
                    }

                    resetStatusPanel();
                    setSettingsVisible(false);
                    setModalVisible(true);
                });
            }

            if (backdrop) {
                backdrop.addEventListener("click", function () {
                    setModalVisible(false);
                });
            }

            if (closeButton) {
                closeButton.addEventListener("click", function () {
                    setModalVisible(false);
                });
            }

            if (statusCloseButton) {
                statusCloseButton.addEventListener("click", function () {
                    resetStatusPanel();
                });
            }

            if (settingsToggle) {
                settingsToggle.addEventListener("click", function () {
                    const shouldOpen = settingsPanel
                        ? settingsPanel.classList.contains("hidden")
                        : true;

                    setSettingsVisible(shouldOpen);
                });
            }

            if (printButton) {
                printButton.addEventListener("click", function () {
                    runPrintExport("print");
                });
            }

            if (exportZipButton) {
                exportZipButton.addEventListener("click", function () {
                    runPrintExport("export");
                });
            }

            [printButton, exportZipButton].forEach(function (button) {
                if (!button) {
                    return;
                }

                if (button.disabled) {
                    button.dataset.originalDisabled = "1";
                }
            });
        }

        bindEvents();

        return {
            open: function (options) {
                options = options || {};

                if (options.printUrl || options.exportZipUrl) {
                    setUrls(options.printUrl || "", options.exportZipUrl || "");
                }

                if (form) {
                    form.reset();
                }

                if (typeof options.beforeOpen === "function") {
                    options.beforeOpen();
                }

                resetStatusPanel();
                setSettingsVisible(false);
                setModalVisible(true);
            },
            close: function () {
                setModalVisible(false);
            },
            setUrls: setUrls,
            isOpen: function () {
                return Boolean(modal && !modal.classList.contains("hidden"));
            }
        };
    }

    window.iMomirPrintExportModal = {
        init: initPrintExportModal
    };
})();