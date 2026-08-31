(function () {
    const pluginCards = document.querySelectorAll(
        ".plugin-card[data-plugin-id]"
    );

    if (!pluginCards.length) {
        return;
    }

    function getRole(
        card,
        role
    ) {
        return card.querySelector(
            '[data-plugin-role="' + role + '"]'
        );
    }

    function getPluginState(
        plugin,
        install
    ) {
        if (install.is_running) {
            return {
                className: "installing",
                label: (
                    install.stage
                    || "Installing"
                )
            };
        }

        if (install.error) {
            return {
                className: "error",
                label: "Installation Failed"
            };
        }

        if (plugin.development) {
            return {
                className: "development",
                label: "Development"
            };
        }

        if (plugin.ready) {
            return {
                className: "ready",
                label: "Installed"
            };
        }

        if (plugin.installed) {
            return {
                className: "incomplete",
                label: "Incomplete"
            };
        }

        return {
            className: "not-installed",
            label: "Not Installed"
        };
    }

    function updatePluginGrouping(
        card,
        plugin
    ) {
        const section = card.closest(
            "#section_plugins"
        );

        if (!section) {
            return;
        }

        const installedGroup = section.querySelector(
            '[data-plugin-group="installed"]'
        );

        const installedList = section.querySelector(
            '[data-plugin-list="installed"]'
        );

        const availableList = section.querySelector(
            '[data-plugin-list="available"]'
        );

        const availableEmpty = section.querySelector(
            '[data-plugin-empty="available"]'
        );

        const targetList = (
            plugin.installed
            ? installedList
            : availableList
        );

        if (
            targetList
            && card.parentElement !== targetList
        ) {
            targetList.appendChild(
                card
            );
        }

        if (
            installedGroup
            && installedList
        ) {
            installedGroup.classList.toggle(
                "hidden",
                installedList.children.length === 0
            );
        }

        if (availableList) {
            availableList.classList.toggle(
                "hidden",
                availableList.children.length === 0
            );
        }

        if (
            availableEmpty
            && availableList
        ) {
            availableEmpty.classList.toggle(
                "hidden",
                availableList.children.length !== 0
            );
        }
    }

    function renderPluginStatus(
        card,
        plugin,
        install
    ) {
        const state = getPluginState(
            plugin,
            install
        );

        const badge = getRole(
            card,
            "badge"
        );

        const version = getRole(
            card,
            "version"
        );

        const spinner = getRole(
            card,
            "spinner"
        );

        const statusTitle = getRole(
            card,
            "status-title"
        );

        const statusMessage = getRole(
            card,
            "status-message"
        );

        const errorElement = getRole(
            card,
            "error"
        );

        const installButton = getRole(
            card,
            "install-button"
        );

        const uninstallForm = getRole(
            card,
            "uninstall-form"
        );

        const uninstallButton = getRole(
            card,
            "uninstall-button"
        );

        if (badge) {
            badge.className = (
                "plugin-status-badge "
                + "plugin-status-"
                + state.className
            );

            badge.textContent = state.label;
        }

        if (version) {
            version.textContent = (
                plugin.version
                || (
                    plugin.installed
                    ? "Unknown"
                    : "Latest release"
                )
            );
        }

        if (spinner) {
            spinner.classList.toggle(
                "hidden",
                !install.is_running
            );
        }

        if (statusTitle) {
            if (
                install.is_running
                || install.error
            ) {
                statusTitle.textContent = (
                    install.stage
                    || state.label
                );
            } else if (plugin.development) {
                statusTitle.textContent =
                    "Development source is active.";
            } else {
                statusTitle.textContent =
                    state.label;
            }
        }

        if (statusMessage) {
            if (
                install.is_running
                || install.error
            ) {
                statusMessage.textContent = (
                    install.message
                    || ""
                );
            } else if (plugin.development) {
                statusMessage.textContent =
                    "iMomir is currently using this plugin directly from plugin_src.";
            } else {
                statusMessage.textContent = (
                    plugin.message
                    || ""
                );
            }
        }

        if (errorElement) {
            errorElement.textContent = (
                install.error
                || ""
            );

            errorElement.classList.toggle(
                "hidden",
                !install.error
            );
        }

        if (installButton) {
            installButton.disabled = Boolean(
                install.is_running
            );

            installButton.classList.toggle(
                "secondary-button",
                Boolean(
                    plugin.installed
                    && !plugin.development
                )
            );

            if (install.is_running) {
                installButton.textContent =
                    "Installing...";
            } else if (plugin.development) {
                installButton.textContent =
                    "Install Release Copy";
            } else if (plugin.installed) {
                installButton.textContent =
                    "Update / Reinstall";
            } else {
                installButton.textContent =
                    "Download & Install";
            }
        }

        if (uninstallForm) {
            uninstallForm.classList.toggle(
                "hidden",
                !plugin.installed
                || plugin.development
            );
        }

        if (uninstallButton) {
            uninstallButton.disabled = Boolean(
                install.is_running
            );
        }

        card.dataset.installRunning = (
            install.is_running
            ? "true"
            : "false"
        );

        updatePluginGrouping(
            card,
            plugin
        );
    }

    function showPluginError(
        card,
        message
    ) {
        const spinner = getRole(
            card,
            "spinner"
        );

        const statusTitle = getRole(
            card,
            "status-title"
        );

        const errorElement = getRole(
            card,
            "error"
        );

        if (spinner) {
            spinner.classList.add(
                "hidden"
            );
        }

        if (statusTitle) {
            statusTitle.textContent =
                "Installation Failed";
        }

        if (errorElement) {
            errorElement.textContent = (
                message
                || "Plugin installation failed."
            );

            errorElement.classList.remove(
                "hidden"
            );
        }
    }

    async function fetchPluginJson(
        url,
        options
    ) {
        const response = await fetch(
            url,
            options
        );

        const data = await response.json();

        if (
            !response.ok
            || !data.ok
        ) {
            throw new Error(
                data.error
                || "Plugin request failed."
            );
        }

        return data;
    }

    async function pollPluginStatus(
        card
    ) {
        const statusUrl = (
            card.dataset.pluginStatusUrl
            || ""
        ).trim();

        if (!statusUrl) {
            return;
        }

        try {
            const data = await fetchPluginJson(
                statusUrl,
                {
                    headers: {
                        "Accept": "application/json"
                    },
                    cache: "no-store"
                }
            );

            renderPluginStatus(
                card,
                data.plugin || {},
                data.install || {}
            );

            if (
                data.install
                && data.install.is_running
            ) {
                window.setTimeout(
                    function () {
                        pollPluginStatus(
                            card
                        );
                    },
                    1000
                );
            }

        } catch (error) {
            const errorElement = getRole(
                card,
                "error"
            );

            if (errorElement) {
                errorElement.textContent = (
                    error.message
                    || "Could not refresh plugin status."
                );

                errorElement.classList.remove(
                    "hidden"
                );
            }

            if (
                card.dataset.installRunning
                === "true"
            ) {
                window.setTimeout(
                    function () {
                        pollPluginStatus(
                            card
                        );
                    },
                    2000
                );
            }
        }
    }

    async function startPluginInstall(
        event,
        card
    ) {
        event.preventDefault();

        const installUrl = (
            card.dataset.pluginInstallUrl
            || ""
        ).trim();

        if (!installUrl) {
            return;
        }

        const installButton = getRole(
            card,
            "install-button"
        );

        const spinner = getRole(
            card,
            "spinner"
        );

        const statusTitle = getRole(
            card,
            "status-title"
        );

        const statusMessage = getRole(
            card,
            "status-message"
        );

        const errorElement = getRole(
            card,
            "error"
        );

        const uninstallButton = getRole(
            card,
            "uninstall-button"
        );

        const previousButtonText = (
            installButton
            ? installButton.textContent
            : ""
        );

        if (installButton) {
            installButton.disabled = true;
            installButton.textContent =
                "Installing...";
        }

        if (uninstallButton) {
            uninstallButton.disabled = true;
        }

        if (spinner) {
            spinner.classList.remove(
                "hidden"
            );
        }

        if (statusTitle) {
            statusTitle.textContent =
                "Starting";
        }

        if (statusMessage) {
            statusMessage.textContent =
                "Starting plugin installation.";
        }

        if (errorElement) {
            errorElement.textContent = "";
            errorElement.classList.add(
                "hidden"
            );
        }

        card.dataset.installRunning =
            "true";

        try {
            await fetchPluginJson(
                installUrl,
                {
                    method: "POST",
                    headers: {
                        "Accept": "application/json"
                    }
                }
            );

            pollPluginStatus(
                card
            );

        } catch (error) {
            card.dataset.installRunning =
                "false";

            if (installButton) {
                installButton.disabled = false;
                installButton.textContent =
                    previousButtonText;
            }

            if (uninstallButton) {
                uninstallButton.disabled = false;
            }

            showPluginError(
                card,
                error.message
            );
        }
    }

    pluginCards.forEach(
        function (card) {
            const installForm = getRole(
                card,
                "install-form"
            );

            if (installForm) {
                installForm.addEventListener(
                    "submit",
                    function (event) {
                        startPluginInstall(
                            event,
                            card
                        );
                    }
                );
            }

            if (
                card.dataset.installRunning
                === "true"
            ) {
                pollPluginStatus(
                    card
                );
            }
        }
    );
})();