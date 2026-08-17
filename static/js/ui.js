/* ==========================================
   iMomir Shared UI Controllers
   ========================================== */

(function () {
    "use strict";

    class UINavigationController {
        constructor(options = {}) {
            this.navigationSelector =
                options.navigationSelector
                || "[data-ui-navigation-action]";

            this.defaultFallbackUrl =
                options.defaultFallbackUrl
                || "/";

            this.isInitialized = false;

            this.handleDocumentClick =
                this.handleDocumentClick.bind(this);
        }

        initialize() {
            if (this.isInitialized) {
                return;
            }

            document.addEventListener(
                "click",
                this.handleDocumentClick
            );

            this.isInitialized = true;
        }

        destroy() {
            if (!this.isInitialized) {
                return;
            }

            document.removeEventListener(
                "click",
                this.handleDocumentClick
            );

            this.isInitialized = false;
        }

        handleDocumentClick(event) {
            const navigationElement =
                event.target.closest(
                    this.navigationSelector
                );

            if (!navigationElement) {
                return;
            }

            const action = String(
                navigationElement.dataset
                    .uiNavigationAction
                || ""
            )
                .trim()
                .toLowerCase();

            if (!action) {
                return;
            }

            event.preventDefault();

            if (action === "back") {
                this.navigateBack(
                    navigationElement
                );

                return;
            }

            if (action === "forward") {
                this.navigateForward(
                    navigationElement
                );
            }
        }

        getSafeFallbackUrl(
            navigationElement
        ) {
            const fallbackUrl = String(
                navigationElement.dataset
                    .uiNavigationFallback
                || navigationElement.getAttribute(
                    "href"
                )
                || this.defaultFallbackUrl
            ).trim();

            if (
                fallbackUrl.startsWith("/")
                && !fallbackUrl.startsWith("//")
            ) {
                return fallbackUrl;
            }

            return this.defaultFallbackUrl;
        }

        hasSameOriginReferrer() {
            if (!document.referrer) {
                return false;
            }

            try {
                const referrerUrl = new URL(
                    document.referrer,
                    window.location.href
                );

                return (
                    referrerUrl.origin
                    === window.location.origin
                );

            } catch (error) {
                return false;
            }
        }

        navigateBack(
            navigationElement
        ) {
            if (
                this.hasSameOriginReferrer()
                && window.history.length > 1
            ) {
                window.history.back();
                return;
            }

            window.location.assign(
                this.getSafeFallbackUrl(
                    navigationElement
                )
            );
        }

        navigateForward(
            navigationElement
        ) {
            if (window.history.length > 1) {
                window.history.forward();
                return;
            }

            window.location.assign(
                this.getSafeFallbackUrl(
                    navigationElement
                )
            );
        }
    }


    /*
     * Shared iMomir UI namespace.
     *
     * Future reusable UI controllers can be added here:
     *
     * window.iMomirUI.navigation
     * window.iMomirUI.dialogs
     * window.iMomirUI.notifications
     * window.iMomirUI.tooltips
     *
     * without putting those systems into app.js.
     */
    window.iMomirUI =
        window.iMomirUI || {};

    window.iMomirUI.UINavigationController =
        UINavigationController;

    window.iMomirUI.navigation =
        new UINavigationController();


    function initializeSharedUi() {
        window.iMomirUI.navigation.initialize();
    }


    if (
        document.readyState === "loading"
    ) {
        document.addEventListener(
            "DOMContentLoaded",
            initializeSharedUi,
            {
                once: true
            }
        );

    } else {
        initializeSharedUi();
    }
})();