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