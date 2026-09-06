(function () {
    "use strict";

    const modal = document.getElementById(
        "printTemplateSelectorModal"
    );

    if (!modal) {
        return;
    }

    const closeButton = document.getElementById(
        "printTemplateSelectorCloseButton"
    );

    const cancelButton = document.getElementById(
        "printTemplateSelectorCancelButton"
    );

    const applyButton = document.getElementById(
        "printTemplateSelectorApplyButton"
    );

    const clearFiltersButton = document.getElementById(
        "printTemplateSelectorClearFilters"
    );

    const searchInput = document.getElementById(
        "printTemplateSelectorSearch"
    );

    const scopeLabel = document.getElementById(
        "printTemplateSelectorScopeLabel"
    );

    const resultCount = document.getElementById(
        "printTemplateSelectorResultCount"
    );

    const emptyState = document.getElementById(
        "printTemplateSelectorEmpty"
    );

    const cards = Array.from(
        modal.querySelectorAll(
            "[data-print-template-card]"
        )
    );

    const filterSelects = Array.from(
        modal.querySelectorAll(
            "[data-print-template-filter]"
        )
    );

    let activeTarget = null;
    let activeScope = "";
    let pendingTemplateId = "";

    function normalize(value) {
        return String(
            value || ""
        )
            .trim()
            .toLowerCase();
    }

    function supportsActiveScope(card) {
        if (activeScope === "momir") {
            return (
                card.dataset.momirSupport
                === "true"
            );
        }

        if (
            activeScope === "chaos"
            || activeScope === "cardprint"
        ) {
            return (
                card.dataset.cardprintSupport
                === "true"
            );
        }

        return false;
    }

    function matchesWildcardValue(
        templateValue,
        selectedValue
    ) {
        const selected = normalize(
            selectedValue
        );

        if (!selected) {
            return true;
        }

        const template = normalize(
            templateValue
        );

        return template === selected;
    }

    function getFilterValue(filterName) {
        const filter = filterSelects.find(
            (item) => (
                item.dataset.printTemplateFilter
                === filterName
            )
        );

        return filter
            ? filter.value
            : "";
    }

    function matchesSearch(card) {
        const query = normalize(
            searchInput.value
        );

        if (!query) {
            return true;
        }

        const searchableText = normalize([
            card.dataset.templateName,
            card.dataset.description,
            card.dataset.printer,
            card.dataset.printerModel,
            card.dataset.paperStock,
            card.dataset.paperStockType,
            card.dataset.paperSize,
            card.dataset.layout,
            card.dataset.orientation,
            card.dataset.silhouetteModel,
            card.dataset.tags,
        ].join(" "));

        return searchableText.includes(
            query
        );
    }

    function matchesFilters(card) {
        if (!supportsActiveScope(card)) {
            return false;
        }

        if (!matchesSearch(card)) {
            return false;
        }

        if (!matchesWildcardValue(
            card.dataset.printer,
            getFilterValue("printer")
        )) {
            return false;
        }

        if (!matchesWildcardValue(
            card.dataset.printerModel,
            getFilterValue("printerModel")
        )) {
            return false;
        }

        if (!matchesWildcardValue(
            card.dataset.paperStock,
            getFilterValue("paperStock")
        )) {
            return false;
        }

        if (!matchesWildcardValue(
            card.dataset.paperStockType,
            getFilterValue(
                "paperStockType"
            )
        )) {
            return false;
        }

        if (!matchesWildcardValue(
            card.dataset.paperSize,
            getFilterValue("paperSize")
        )) {
            return false;
        }

        if (!matchesWildcardValue(
            card.dataset.layout,
            getFilterValue("layout")
        )) {
            return false;
        }

        if (!matchesWildcardValue(
            card.dataset.orientation,
            getFilterValue(
                "orientation"
            )
        )) {
            return false;
        }

        const silhouetteFilter = (
            getFilterValue("silhouette")
        );

        if (
            silhouetteFilter
            && card.dataset.silhouette
            !== silhouetteFilter
        ) {
            return false;
        }

        return true;
    }

    function clearCardSelection() {
        cards.forEach((card) => {
            card.classList.remove(
                "is-selected"
            );
        });
    }

    function selectTemplate(
        templateId
    ) {
        pendingTemplateId = (
            templateId || ""
        );

        clearCardSelection();

        const selectedCard = cards.find(
            (card) => (
                card.dataset.templateId
                === pendingTemplateId
            )
        );

        if (
            selectedCard
            && !selectedCard.classList.contains(
                "hidden"
            )
        ) {
            selectedCard.classList.add(
                "is-selected"
            );

            selectedCard.scrollIntoView({
                block: "nearest",
            });

            applyButton.disabled = false;
            return;
        }

        applyButton.disabled = true;
    }

    function applyFilters() {
        let visibleCount = 0;

        cards.forEach((card) => {
            const visible = (
                matchesFilters(card)
            );

            card.classList.toggle(
                "hidden",
                !visible
            );

            if (visible) {
                visibleCount += 1;
            }
        });

        resultCount.textContent = (
            `${visibleCount} `
            + (
                visibleCount === 1
                    ? "template"
                    : "templates"
            )
        );

        emptyState.classList.toggle(
            "hidden",
            visibleCount !== 0
        );

        selectTemplate(
            pendingTemplateId
        );
    }

    function getUniqueValues(
        datasetKey
    ) {
        const values = new Map();

        cards.forEach((card) => {
            if (!supportsActiveScope(card)) {
                return;
            }

            const value = String(
                card.dataset[
                    datasetKey
                ]
                || ""
            ).trim();

            if (
                !value
                || normalize(value) === "any"
            ) {
                return;
            }

            values.set(
                normalize(value),
                value
            );
        });

        return Array.from(
            values.values()
        ).sort(
            (left, right) => (
                left.localeCompare(
                    right,
                    undefined,
                    {
                        sensitivity: "base",
                    }
                )
            )
        );
    }

    function populateFilter(
        filterName,
        datasetKey,
        allLabel
    ) {
        const select = filterSelects.find(
            (item) => (
                item.dataset.printTemplateFilter
                === filterName
            )
        );

        if (!select) {
            return;
        }

        select.innerHTML = "";

        const allOption = (
            document.createElement(
                "option"
            )
        );

        allOption.value = "";
        allOption.textContent = allLabel;

        select.appendChild(
            allOption
        );

        getUniqueValues(
            datasetKey
        ).forEach((value) => {
            const option = (
                document.createElement(
                    "option"
                )
            );

            option.value = value;
            option.textContent = value;

            select.appendChild(
                option
            );
        });
    }

    function populateFilters() {
        populateFilter(
            "printer",
            "printer",
            "All Printers"
        );

        populateFilter(
            "printerModel",
            "printerModel",
            "All Models"
        );

        populateFilter(
            "paperStock",
            "paperStock",
            "All Paper Stock"
        );

        populateFilter(
            "paperStockType",
            "paperStockType",
            "All Stock Types"
        );

        populateFilter(
            "paperSize",
            "paperSize",
            "All Paper Sizes"
        );

        populateFilter(
            "layout",
            "layout",
            "All Layouts"
        );

        populateFilter(
            "orientation",
            "orientation",
            "All Orientations"
        );

        const silhouetteSelect = (
            filterSelects.find(
                (item) => (
                    item.dataset.printTemplateFilter
                    === "silhouette"
                )
            )
        );

        if (silhouetteSelect) {
            silhouetteSelect.value = "";
        }
    }

    function clearFilters() {
        searchInput.value = "";

        filterSelects.forEach(
            (select) => {
                select.value = "";
            }
        );

        applyFilters();
    }

    function closeModal() {
        modal.classList.add(
            "hidden"
        );

        modal.setAttribute(
            "aria-hidden",
            "true"
        );

        document.body.classList.remove(
            "print-template-selector-open"
        );

        activeTarget = null;
        activeScope = "";
        pendingTemplateId = "";
    }

    function openModal(
        target,
        scope
    ) {
        activeTarget = target;
        activeScope = normalize(
            scope
        );

        if (!activeTarget) {
            return;
        }

        scopeLabel.textContent = (
            activeScope === "momir"
                ? "Showing templates that support the Momir print flow."
                : "Showing templates that support the Card Print / Chaos Draft flow."
        );

        populateFilters();

        searchInput.value = "";

        pendingTemplateId = (
            activeTarget.value
            || ""
        );

        modal.classList.remove(
            "hidden"
        );

        modal.setAttribute(
            "aria-hidden",
            "false"
        );

        document.body.classList.add(
            "print-template-selector-open"
        );

        applyFilters();

        window.setTimeout(() => {
            searchInput.focus();
        }, 0);
    }

    document.querySelectorAll(
        "[data-print-template-selector-open]"
    ).forEach((button) => {
        button.addEventListener(
            "click",
            () => {
                const targetId = (
                    button.dataset
                        .printTemplateTarget
                );

                const scope = (
                    button.dataset
                        .printTemplateScope
                );

                if (!targetId) {
                    return;
                }

                openModal(
                    document.getElementById(
                        targetId
                    ),
                    scope
                );
            }
        );
    });

    cards.forEach((card) => {
        card.addEventListener(
            "click",
            () => {
                if (
                    card.classList.contains(
                        "hidden"
                    )
                ) {
                    return;
                }

                selectTemplate(
                    card.dataset.templateId
                );
            }
        );
    });

    filterSelects.forEach(
        (select) => {
            select.addEventListener(
                "change",
                applyFilters
            );
        }
    );

    searchInput.addEventListener(
        "input",
        applyFilters
    );

    clearFiltersButton.addEventListener(
        "click",
        clearFilters
    );

    applyButton.addEventListener(
        "click",
        () => {
            if (
                !activeTarget
                || !pendingTemplateId
            ) {
                return;
            }

            const optionExists = Array.from(
                activeTarget.options
                || []
            ).some(
                (option) => (
                    option.value
                    === pendingTemplateId
                )
            );

            if (!optionExists) {
                return;
            }

            activeTarget.value = (
                pendingTemplateId
            );

            activeTarget.dispatchEvent(
                new Event(
                    "change",
                    {
                        bubbles: true,
                    }
                )
            );

            closeModal();
        }
    );

    closeButton.addEventListener(
        "click",
        closeModal
    );

    cancelButton.addEventListener(
        "click",
        closeModal
    );

    modal.addEventListener(
        "click",
        (event) => {
            if (event.target === modal) {
                closeModal();
            }
        }
    );

    document.addEventListener(
        "keydown",
        (event) => {
            if (
                event.key === "Escape"
                && !modal.classList.contains(
                    "hidden"
                )
            ) {
                closeModal();
            }
        }
    );
})();