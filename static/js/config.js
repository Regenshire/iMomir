document.addEventListener("DOMContentLoaded", function () {
    initializeRefreshCards();
    initializeSettingsConsole();
    initializeConfigPanels();
    initializeGameModeCards();
    initializeConfigShortcutNavigation();
});

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