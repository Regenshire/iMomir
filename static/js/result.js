document.addEventListener("DOMContentLoaded", function () {
    initializeResultCardZoom();
    initializeMomirSelectResultLinks();
});

function initializeResultCardZoom() {
    const cardImage = document.getElementById("resultCardImage");
    const zoomOverlay = document.getElementById("cardZoomOverlay");
    const zoomBackdrop = document.getElementById("cardZoomBackdrop");

    if (!cardImage || !zoomOverlay || !zoomBackdrop) {
        return;
    }

    function openZoom() {
        zoomOverlay.classList.remove("hidden");
        zoomOverlay.setAttribute("aria-hidden", "false");
        document.body.style.overflow = "hidden";
    }

    function closeZoom() {
        zoomOverlay.classList.add("hidden");
        zoomOverlay.setAttribute("aria-hidden", "true");
        document.body.style.overflow = "";
    }

    cardImage.addEventListener("click", function () {
        openZoom();
    });

    cardImage.addEventListener("keydown", function (event) {
        if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            openZoom();
        }
    });

    zoomOverlay.addEventListener("click", function () {
        closeZoom();
    });

    document.addEventListener("keydown", function (event) {
        if (event.key === "Escape" && !zoomOverlay.classList.contains("hidden")) {
            closeZoom();
        }
    });
}

function initializeMomirSelectResultLinks() {
    const selectedTypeDropdown = document.getElementById("resultSelectedType");
    const manaLinks = document.querySelectorAll('[data-mana-link="1"]');
    const againButton = document.getElementById("resultAgainButton");

    if (!selectedTypeDropdown) {
        return;
    }

    function updateResultLinks() {
        const selectedTypeValue = (selectedTypeDropdown.value || "").trim();

        manaLinks.forEach(function (link) {
            const manaValue = link.getAttribute("data-mana-value") || "";
            const url = new URL(link.href, window.location.origin);

            url.searchParams.set("mana_value", manaValue);

            if (selectedTypeValue) {
                url.searchParams.set("selected_type", selectedTypeValue);
            } else {
                url.searchParams.delete("selected_type");
            }

            link.href = url.pathname + url.search;
        });

        if (againButton) {
            const againUrl = new URL(againButton.href, window.location.origin);
            const currentManaValue = againUrl.searchParams.get("mana_value") || "";

            againUrl.searchParams.set("mana_value", currentManaValue);

            if (selectedTypeValue) {
                againUrl.searchParams.set("selected_type", selectedTypeValue);
            } else {
                againUrl.searchParams.delete("selected_type");
            }

            againButton.href = againUrl.pathname + againUrl.search;
        }
    }

    selectedTypeDropdown.addEventListener("change", function () {
        updateResultLinks();
    });

    updateResultLinks();
}