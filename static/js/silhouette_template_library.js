(function () {
    const modal = document.getElementById(
        "silhouetteTemplateLibraryModal"
    );

    if (!modal) {
        return;
    }

    const grid = document.getElementById(
        "silhouetteTemplateLibraryGrid"
    );

    const emptyState = document.getElementById(
        "silhouetteTemplateLibraryEmpty"
    );

    const statusElement = document.getElementById(
        "silhouetteTemplateLibraryStatus"
    );

    const filterSelect = document.getElementById(
        "silhouetteTemplateLibraryFilter"
    );

    const filterSummary = document.getElementById(
        "silhouetteTemplateLibraryFilterSummary"
    );

    const uploadToggleButton = document.getElementById(
        "silhouetteTemplateUploadToggle"
    );

    const uploadSection = document.getElementById(
        "silhouetteTemplateUploadSection"
    );

    const uploadForm = document.getElementById(
        "silhouetteTemplateUploadForm"
    );

    const uploadFileInput = document.getElementById(
        "silhouetteTemplateUploadFile"
    );

    const uploadNameInput = document.getElementById(
        "silhouetteTemplateUploadName"
    );

    const uploadDescriptionInput = document.getElementById(
        "silhouetteTemplateUploadDescription"
    );

    const uploadPrintTemplateSelect = document.getElementById(
        "silhouetteTemplateUploadPrintTemplate"
    );

    const uploadButton = document.getElementById(
        "silhouetteTemplateUploadButton"
    );

    let templates = [];
    let printTemplateOptions = [];
    let activePrintTemplate = "";
    let selectedPrintTemplateFilter = "";


    function setStatus(
        message,
        isError
    ) {
        const cleanMessage = String(
            message || ""
        ).trim();

        if (!statusElement) {
            return;
        }

        statusElement.textContent =
            cleanMessage;

        statusElement.classList.toggle(
            "hidden",
            !cleanMessage
        );

        statusElement.classList.toggle(
            "is-error",
            Boolean(
                cleanMessage
                && isError
            )
        );
    }

    function setUploadSectionExpanded(isExpanded) {
        const expanded = Boolean(isExpanded);

        if (uploadSection) {
            uploadSection.classList.toggle(
                "hidden",
                !expanded
            );
        }

        if (uploadToggleButton) {
            uploadToggleButton.setAttribute(
                "aria-expanded",
                expanded ? "true" : "false"
            );

            uploadToggleButton.innerHTML = expanded
                ? (
                    'Hide Add Template'
                )
                : (
                    'Add Silhouette Template'
                );
        }

        if (
            expanded
            && uploadPrintTemplateSelect
            && selectedPrintTemplateFilter
            && printTemplateOptions.some(
                function (option) {
                    return (
                        option.value
                        === selectedPrintTemplateFilter
                    );
                }
            )
        ) {
            uploadPrintTemplateSelect.value =
                selectedPrintTemplateFilter;
        }
    }


    function closeModal() {
        modal.classList.add(
            "hidden"
        );

        modal.setAttribute(
            "aria-hidden",
            "true"
        );

        setStatus(
            "",
            false
        );

        setUploadSectionExpanded(false);
    }

    function populatePrintTemplateOptions() {
        if (!uploadPrintTemplateSelect) {
            return;
        }

        const previousValue = String(
            uploadPrintTemplateSelect.value
            || ""
        ).trim();

        uploadPrintTemplateSelect.innerHTML =
            "";

        printTemplateOptions.forEach(
            function (option) {
                const optionElement =
                    document.createElement(
                        "option"
                    );

                optionElement.value =
                    option.value || "";

                optionElement.textContent =
                    option.label
                    || option.value
                    || "Print Template";

                uploadPrintTemplateSelect
                    .appendChild(
                        optionElement
                    );
            }
        );

        if (
            activePrintTemplate
            && printTemplateOptions.some(
                function (option) {
                    return (
                        option.value
                        === activePrintTemplate
                    );
                }
            )
        ) {
            uploadPrintTemplateSelect.value =
                activePrintTemplate;

        } else if (previousValue) {
            uploadPrintTemplateSelect.value =
                previousValue;
        }
    }

    function populateTemplateFilterOptions() {
        if (!filterSelect) {
            return;
        }

        filterSelect.innerHTML = "";

        const allOption =
            document.createElement(
                "option"
            );

        allOption.value = "";
        allOption.textContent = "All";

        filterSelect.appendChild(
            allOption
        );

        printTemplateOptions.forEach(
            function (option) {
                const optionElement =
                    document.createElement(
                        "option"
                    );

                optionElement.value =
                    option.value || "";

                optionElement.textContent =
                    option.label
                    || option.value
                    || "Print Template";

                filterSelect.appendChild(
                    optionElement
                );
            }
        );

        const hasRequestedFilter =
            selectedPrintTemplateFilter
            && printTemplateOptions.some(
                function (option) {
                    return (
                        option.value
                        === selectedPrintTemplateFilter
                    );
                }
            );

        if (!hasRequestedFilter) {
            selectedPrintTemplateFilter = "";
        }

        filterSelect.value =
            selectedPrintTemplateFilter;
    }

    function renderTemplates() {
        if (!grid) {
            return;
        }

        grid.innerHTML = "";

        const filteredTemplates =
            templates.filter(
                function (template) {
                    return (
                        !selectedPrintTemplateFilter
                        || template.print_template
                            === selectedPrintTemplateFilter
                    );
                }
            );

        const sortedTemplates =
            filteredTemplates.slice().sort(
                function (
                    left,
                    right
                ) {
                    const leftCompatible =
                        left.print_template
                        === activePrintTemplate
                            ? 0
                            : 1;

                    const rightCompatible =
                        right.print_template
                        === activePrintTemplate
                            ? 0
                            : 1;

                    if (
                        leftCompatible
                        !== rightCompatible
                    ) {
                        return (
                            leftCompatible
                            - rightCompatible
                        );
                    }

                    return String(
                        left.name || ""
                    ).localeCompare(
                        String(
                            right.name || ""
                        )
                    );
                }
            );

        sortedTemplates.forEach(
            function (template) {
                const card =
                    document.createElement(
                        "article"
                    );

                card.className =
                    "silhouette-template-library-card";

                if (
                    template.print_template
                    === activePrintTemplate
                ) {
                    card.classList.add(
                        "is-compatible"
                    );
                }

                const isCompatible =
                    template.print_template
                    === activePrintTemplate;

                if (isCompatible) {
                    const compatibleLabel =
                        document.createElement(
                            "div"
                        );

                    compatibleLabel.className =
                        "silhouette-template-library-compatible-label";

                    compatibleLabel.textContent =
                        "Matches Current Print Template";

                    card.appendChild(
                        compatibleLabel
                    );
                }

                const header =
                    document.createElement(
                        "div"
                    );

                header.className =
                    "silhouette-template-library-card-header";

                const titleWrap =
                    document.createElement(
                        "div"
                    );

                titleWrap.className =
                    "silhouette-template-library-card-title-wrap";

                const title =
                    document.createElement(
                        "h3"
                    );

                title.textContent =
                    template.name
                    || template.filename
                    || "Silhouette Template";

                const templateLabel =
                    document.createElement(
                        "span"
                    );

                templateLabel.className =
                    "silhouette-template-library-print-template";

                templateLabel.textContent =
                    template.print_template_label
                    || template.print_template
                    || "Unlinked";

                titleWrap.appendChild(
                    title
                );

                titleWrap.appendChild(
                    templateLabel
                );

                const downloadLink =
                    document.createElement(
                        "a"
                    );

                downloadLink.className =
                    "action-button secondary-button silhouette-template-library-download";

                downloadLink.href =
                    template.download_url
                    || "#";

                downloadLink.innerHTML =
                    '<i class="fa-solid fa-download"></i>'
                    + '<span>Download Template</span>';

                downloadLink.setAttribute(
                    "download",
                    template.filename
                    || "template.studio3"
                );

                header.appendChild(
                    titleWrap
                );

                const description =
                    document.createElement(
                        "p"
                    );

                description.className =
                    "silhouette-template-library-description";

                description.textContent =
                    template.description
                    || "No description provided.";

                const filename =
                    document.createElement(
                        "div"
                    );

                filename.className =
                    "silhouette-template-library-filename";

                filename.textContent =
                    template.filename
                    || "";

                card.appendChild(
                    header
                );

                card.appendChild(
                    description
                );

                card.appendChild(
                    filename
                );

                card.appendChild(
                    downloadLink
                );

                grid.appendChild(
                    card
                );
            }
        );

        if (emptyState) {
            emptyState.textContent =
                selectedPrintTemplateFilter
                    ? "No Silhouette templates are linked to the selected Print Template."
                    : "No Silhouette templates are currently available.";

            emptyState.classList.toggle(
                "hidden",
                sortedTemplates.length > 0
            );
        }

        if (filterSummary) {
            filterSummary.textContent =
                "Showing "
                + sortedTemplates.length
                + " of "
                + templates.length
                + " template"
                + (templates.length === 1 ? "" : "s");
        }
    }


    async function loadOptions() {
        const optionsUrl = String(
            modal.dataset.optionsUrl
            || ""
        ).trim();

        if (!optionsUrl) {
            throw new Error(
                "Silhouette template options URL is not configured."
            );
        }

        setStatus(
            "Loading Silhouette templates...",
            false
        );

        const response = await fetch(
            optionsUrl,
            {
                headers: {
                    "Accept":
                        "application/json"
                }
            }
        );

        const payload =
            await response.json();

        if (
            !response.ok
            || !payload.ok
        ) {
            throw new Error(
                payload.message
                || "Silhouette templates could not be loaded."
            );
        }

        templates = Array.isArray(
            payload.templates
        )
            ? payload.templates
            : [];

        printTemplateOptions =
            Array.isArray(
                payload.print_template_options
            )
                ? payload.print_template_options
                : [];

        populatePrintTemplateOptions();
        populateTemplateFilterOptions();
        renderTemplates();

        setStatus(
            "",
            false
        );
    }


    async function openModal(button) {
        activePrintTemplate = String(
            button
            && button.dataset.activePrintTemplate
            || ""
        ).trim().toLowerCase();

        selectedPrintTemplateFilter =
            activePrintTemplate;

        setUploadSectionExpanded(false);

        modal.classList.remove(
            "hidden"
        );

        modal.setAttribute(
            "aria-hidden",
            "false"
        );

        try {
            await loadOptions();

        } catch (error) {
            setStatus(
                error
                && error.message
                    ? error.message
                    : "Silhouette templates could not be loaded.",
                true
            );
        }
    }


    async function uploadTemplate(event) {
        event.preventDefault();

        if (
            !uploadForm
            || !uploadFileInput
            || !uploadFileInput.files
            || !uploadFileInput.files[0]
        ) {
            setStatus(
                "Choose a .studio3 file to upload.",
                true
            );

            return;
        }

        const uploadFile =
            uploadFileInput.files[0];

        if (
            !String(
                uploadFile.name || ""
            ).toLowerCase().endsWith(
                ".studio3"
            )
        ) {
            setStatus(
                "Only .studio3 files can be uploaded.",
                true
            );

            return;
        }

        const uploadUrl = String(
            modal.dataset.uploadUrl
            || ""
        ).trim();

        if (!uploadUrl) {
            setStatus(
                "Silhouette template upload URL is not configured.",
                true
            );

            return;
        }

        const formData =
            new FormData();

        formData.append(
            "template_file",
            uploadFile
        );

        formData.append(
            "name",
            uploadNameInput
                ? uploadNameInput.value
                : ""
        );

        formData.append(
            "description",
            uploadDescriptionInput
                ? uploadDescriptionInput.value
                : ""
        );

        formData.append(
            "print_template",
            uploadPrintTemplateSelect
                ? uploadPrintTemplateSelect.value
                : ""
        );

        if (uploadButton) {
            uploadButton.disabled =
                true;
        }

        setStatus(
            "Uploading Silhouette template...",
            false
        );

        try {
            const response =
                await fetch(
                    uploadUrl,
                    {
                        method: "POST",
                        body: formData,
                        headers: {
                            "Accept":
                                "application/json"
                        }
                    }
                );

            const payload =
                await response.json();

            if (
                !response.ok
                || !payload.ok
            ) {
                throw new Error(
                    payload.message
                    || "Silhouette template upload failed."
                );
            }

            templates = Array.isArray(
                payload.templates
            )
                ? payload.templates
                : templates;

            uploadForm.reset();

            populatePrintTemplateOptions();
            populateTemplateFilterOptions();
            setUploadSectionExpanded(false);
            renderTemplates();

            setStatus(
                payload.message
                || "Silhouette template uploaded.",
                false
            );

        } catch (error) {
            setStatus(
                error
                && error.message
                    ? error.message
                    : "Silhouette template upload failed.",
                true
            );

        } finally {
            if (uploadButton) {
                uploadButton.disabled =
                    false;
            }
        }
    }


    document.querySelectorAll(
        "[data-silhouette-template-library-open]"
    ).forEach(
        function (button) {
            button.addEventListener(
                "click",
                function () {
                    openModal(
                        button
                    );
                }
            );
        }
    );


    modal.querySelectorAll(
        "[data-silhouette-template-library-close]"
    ).forEach(
        function (button) {
            button.addEventListener(
                "click",
                closeModal
            );
        }
    );

    if (
        uploadToggleButton
        && uploadSection
    ) {
        uploadToggleButton.addEventListener(
            "click",
            function () {
                setUploadSectionExpanded(
                    uploadSection.classList.contains(
                        "hidden"
                    )
                );
            }
        );
    }

    if (filterSelect) {
        filterSelect.addEventListener(
            "change",
            function () {
                selectedPrintTemplateFilter =
                    String(
                        filterSelect.value
                        || ""
                    ).trim().toLowerCase();

                renderTemplates();
            }
        );
    }
    
    if (uploadForm) {
        uploadForm.addEventListener(
            "submit",
            uploadTemplate
        );
    }


    document.addEventListener(
        "keydown",
        function (event) {
            if (
                event.key === "Escape"
                && !modal.classList.contains(
                    "hidden"
                )
            ) {
                event.preventDefault();
                closeModal();
            }
        }
    );
})();