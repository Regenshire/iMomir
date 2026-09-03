(function () {
    const modal = document.getElementById("cardBackSelectorModal");

    if (!modal) {
        return;
    }

    const grid = document.getElementById("cardBackSelectorGrid");
    const emptyState = document.getElementById("cardBackSelectorEmpty");
    const statusElement = document.getElementById("cardBackSelectorStatus");
    const uploadInput = document.getElementById("cardBackSelectorUploadInput");
    const uploadButton = document.getElementById("cardBackSelectorUploadButton");
    const applyButton = document.getElementById("cardBackSelectorApplyButton");

    const contextMenu = document.getElementById(
        "cardBackSelectorContextMenu"
    );

    const deleteMenuItem = document.getElementById(
        "cardBackSelectorDeleteMenuItem"
    );

    const deleteConfirm = document.getElementById(
        "cardBackDeleteConfirm"
    );

    const deleteConfirmName = document.getElementById(
        "cardBackDeleteConfirmName"
    );

    const deleteConfirmCancel = document.getElementById(
        "cardBackDeleteConfirmCancel"
    );

    const deleteConfirmButton = document.getElementById(
        "cardBackDeleteConfirmButton"
    );

    let activeButton = null;
    let options = [];
    let selectedKey = "";
    let defaultKey = "";
    let maxUploadSizeBytes = 0;
    let loading = false;

    let contextMenuOption = null;
    let pendingDeleteOption = null;

    function setStatus(message, isError) {
        const cleanMessage = String(message || "").trim();

        if (!statusElement) {
            return;
        }

        statusElement.textContent = cleanMessage;
        statusElement.classList.toggle("hidden", !cleanMessage);
        statusElement.classList.toggle(
            "is-error",
            Boolean(cleanMessage && isError)
        );
    }

    function hideContextMenu() {
        if (!contextMenu) {
            return;
        }

        contextMenu.classList.add("hidden");
        contextMenuOption = null;
    }

    function showContextMenu(event, option) {
        if (
            !contextMenu
            || !option
            || option.source !== "custom"
        ) {
            return;
        }

        event.preventDefault();
        event.stopPropagation();

        contextMenuOption = option;

        contextMenu.classList.remove("hidden");

        let left = event.clientX;
        let top = event.clientY;

        contextMenu.style.left = `${left}px`;
        contextMenu.style.top = `${top}px`;

        const menuRect =
            contextMenu.getBoundingClientRect();

        const viewportPadding = 8;

        if (
            left + menuRect.width
            > window.innerWidth - viewportPadding
        ) {
            left = Math.max(
                viewportPadding,
                window.innerWidth
                - menuRect.width
                - viewportPadding
            );
        }

        if (
            top + menuRect.height
            > window.innerHeight - viewportPadding
        ) {
            top = Math.max(
                viewportPadding,
                window.innerHeight
                - menuRect.height
                - viewportPadding
            );
        }

        contextMenu.style.left = `${left}px`;
        contextMenu.style.top = `${top}px`;
    }

    function closeDeleteConfirm() {
        if (!deleteConfirm) {
            return;
        }

        deleteConfirm.classList.add("hidden");

        deleteConfirm.setAttribute(
            "aria-hidden",
            "true"
        );

        pendingDeleteOption = null;
    }

    function openDeleteConfirm(option) {
        if (
            !deleteConfirm
            || !option
            || option.source !== "custom"
        ) {
            return;
        }

        pendingDeleteOption = option;

        if (deleteConfirmName) {
            deleteConfirmName.textContent =
                optionLabel(option);
        }

        deleteConfirm.classList.remove("hidden");

        deleteConfirm.setAttribute(
            "aria-hidden",
            "false"
        );

        if (deleteConfirmButton) {
            deleteConfirmButton.focus();
        }
    }

    function getTargetInput(button) {
        if (!button) {
            return null;
        }

        const inputId = String(
            button.dataset.cardBackTargetInput || ""
        ).trim();

        return inputId
            ? document.getElementById(inputId)
            : null;
    }

    function getCurrentSelectedKey(button) {
        const targetInput = getTargetInput(button);

        if (targetInput) {
            return String(targetInput.value || "").trim();
        }

        return String(
            button
            && button.dataset.selectedCardBackKey
            || ""
        ).trim();
    }

    function optionLabel(option) {
        if (!option) {
            return "Card Back";
        }

        return String(
            option.label
            || option.filename
            || "Card Back"
        );
    }

    function updateActiveButton(option) {
        if (!activeButton) {
            return;
        }

        activeButton.dataset.selectedCardBackKey =
            selectedKey;

        if (option) {
            activeButton.title =
                "Card Back: " + optionLabel(option);

            activeButton.setAttribute(
                "aria-label",
                "Card Back: " + optionLabel(option)
            );
        }
    }

    function selectKey(nextKey) {
        selectedKey = String(
            nextKey || ""
        ).trim();

        if (!selectedKey && defaultKey) {
            selectedKey = defaultKey;
        }

        if (grid) {
            grid.querySelectorAll(
                "[data-card-back-key]"
            ).forEach(function (button) {
                button.classList.toggle(
                    "is-selected",
                    String(
                        button.dataset.cardBackKey
                        || ""
                    ) === selectedKey
                );
            });
        }

        if (applyButton) {
            applyButton.disabled =
                !selectedKey || loading;
        }
    }

    function buildOptionButton(option) {
        const button =
            document.createElement("button");

        button.type = "button";
        button.className =
            "card-back-selector-option";

        button.dataset.cardBackKey =
            option.key || "";

        button.title =
            optionLabel(option);

        const imageWrap =
            document.createElement("span");

        imageWrap.className =
            "card-back-selector-option-image-wrap";

        const image =
            document.createElement("img");

        image.src =
            option.image_url || "";

        image.alt =
            optionLabel(option);

        image.loading = "lazy";

        imageWrap.appendChild(image);

        const title =
            document.createElement("span");

        title.className =
            "card-back-selector-option-title";

        title.textContent =
            optionLabel(option);

        const source =
            document.createElement("span");

        source.className =
            "card-back-selector-option-source";

        source.textContent =
            option.source === "custom"
                ? "Custom Upload"
                : "Included";

        button.appendChild(imageWrap);
        button.appendChild(title);
        button.appendChild(source);

        button.addEventListener(
            "click",
            function () {
                hideContextMenu();
                selectKey(option.key || "");
            }
        );

        if (option.source === "custom") {
            button.addEventListener(
                "contextmenu",
                function (event) {
                    showContextMenu(
                        event,
                        option
                    );
                }
            );
        }

        return button;
    }

    function renderOptions() {
        if (!grid) {
            return;
        }

        grid.innerHTML = "";

        options.forEach(function (option) {
            grid.appendChild(
                buildOptionButton(option)
            );
        });

        if (emptyState) {
            emptyState.classList.toggle(
                "hidden",
                options.length > 0
            );
        }

        selectKey(
            selectedKey || defaultKey
        );
    }

    async function loadOptions() {
        const optionsUrl = String(
            modal.dataset.optionsUrl || ""
        ).trim();

        if (!optionsUrl) {
            throw new Error(
                "Card Back options URL is not configured."
            );
        }

        loading = true;

        setStatus(
            "Loading card backs...",
            false
        );

        if (applyButton) {
            applyButton.disabled = true;
        }

        try {
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
                    || "Card Back options could not be loaded."
                );
            }

            options = Array.isArray(
                payload.options
            )
                ? payload.options
                : [];

            defaultKey = String(
                payload.default_key || ""
            ).trim();

            maxUploadSizeBytes = Number(
                payload.max_upload_size_bytes || 0
            );

            if (
                !Number.isFinite(
                    maxUploadSizeBytes
                )
                || maxUploadSizeBytes < 0
            ) {
                maxUploadSizeBytes = 0;
            }

            renderOptions();
            setStatus("", false);

        } finally {
            loading = false;

            if (applyButton) {
                applyButton.disabled =
                    !selectedKey;
            }
        }
    }

    function closeModal() {
        hideContextMenu();
        closeDeleteConfirm();
        
        modal.classList.add("hidden");
        modal.setAttribute(
            "aria-hidden",
            "true"
        );

        setStatus("", false);

        if (uploadInput) {
            uploadInput.value = "";
        }

        const returnButton =
            activeButton;

        activeButton = null;

        if (returnButton) {
            returnButton.focus();
        }
    }

    async function openModal(button) {
        activeButton = button;

        selectedKey =
            getCurrentSelectedKey(button);

        modal.classList.remove("hidden");

        modal.setAttribute(
            "aria-hidden",
            "false"
        );

        try {
            await loadOptions();

        } catch (error) {
            setStatus(
                error.message
                || "Card Back options could not be loaded.",
                true
            );
        }
    }

    async function deletePendingCardBack() {
        const optionToDelete =
            pendingDeleteOption;

        if (
            !optionToDelete
            || optionToDelete.source !== "custom"
        ) {
            return;
        }

        const deleteUrl = String(
            modal.dataset.deleteUrl || ""
        ).trim();

        if (!deleteUrl) {
            setStatus(
                "Card Back delete URL is not configured.",
                true
            );

            return;
        }

        if (deleteConfirmButton) {
            deleteConfirmButton.disabled = true;
        }

        try {
            const formData =
                new FormData();

            formData.append(
                "card_back_key",
                optionToDelete.key
            );

            const response = await fetch(
                deleteUrl,
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
                    || "Card Back could not be deleted."
                );
            }

            const deletedKey = String(
                payload.deleted_key
                || optionToDelete.key
                || ""
            ).trim();

            options = Array.isArray(
                payload.options
            )
                ? payload.options
                : [];

            defaultKey = String(
                payload.default_key
                || defaultKey
                || ""
            ).trim();

            const targetInput =
                getTargetInput(activeButton);

            if (
                targetInput
                && String(
                    targetInput.value || ""
                ).trim() === deletedKey
            ) {
                targetInput.value =
                    defaultKey;

                targetInput.dispatchEvent(
                    new Event(
                        "change",
                        {
                            bubbles: true
                        }
                    )
                );

                const fallbackOption =
                    options.find(
                        function (option) {
                            return String(
                                option.key || ""
                            ) === defaultKey;
                        }
                    ) || null;

                updateActiveButton(
                    fallbackOption
                );
            }

            if (selectedKey === deletedKey) {
                selectedKey =
                    defaultKey;
            }

            closeDeleteConfirm();

            renderOptions();

            setStatus(
                payload.message
                || "Custom card back deleted.",
                false
            );

        } catch (error) {
            setStatus(
                error.message
                || "Card Back could not be deleted.",
                true
            );

        } finally {
            if (deleteConfirmButton) {
                deleteConfirmButton.disabled = false;
            }
        }
    }

    async function uploadCardBack() {
        if (
            !uploadInput
            || !uploadInput.files
            || !uploadInput.files[0]
        ) {
            setStatus(
                "Choose an image file first.",
                true
            );

            return;
        }

        const uploadFile =
            uploadInput.files[0];

        if (
            maxUploadSizeBytes > 0
            && uploadFile.size
                > maxUploadSizeBytes
        ) {
            const maxSizeMb = (
                maxUploadSizeBytes
                / (1024 * 1024)
            );

            setStatus(
                `The selected card back image exceeds the ${maxSizeMb} MB size limit.`,
                true
            );

            return;
        }

        const uploadUrl = String(
            modal.dataset.uploadUrl || ""
        ).trim();

        if (!uploadUrl) {
            setStatus(
                "Card Back upload URL is not configured.",
                true
            );

            return;
        }

        const formData =
            new FormData();

        formData.append(
            "card_back_file",
            uploadFile
        );

        uploadButton.disabled = true;

        setStatus(
            "Uploading card back...",
            false
        );

        try {
            const response = await fetch(
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
                    || "Card Back upload failed."
                );
            }

            options = Array.isArray(
                payload.options
            )
                ? payload.options
                : options;

            selectedKey = String(
                payload.option
                && payload.option.key
                || ""
            ).trim();

            renderOptions();

            uploadInput.value = "";

            setStatus(
                payload.message
                || "Card back uploaded.",
                false
            );

        } catch (error) {
            setStatus(
                error.message
                || "Card Back upload failed.",
                true
            );

        } finally {
            uploadButton.disabled = false;
        }
    }

    async function applySelection() {
        if (
            !activeButton
            || !selectedKey
        ) {
            return;
        }

        const targetInput =
            getTargetInput(activeButton);

        const saveUrl = String(
            activeButton.dataset.cardBackSaveUrl
            || ""
        ).trim();

        const selectedOption =
            options.find(function (option) {
                return String(
                    option.key || ""
                ) === selectedKey;
            }) || null;

        applyButton.disabled = true;

        try {
            if (saveUrl) {
                const formData =
                    new FormData();

                formData.append(
                    "card_back_key",
                    selectedKey
                );

                const response =
                    await fetch(
                        saveUrl,
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
                        || "Card Back selection could not be saved."
                    );
                }
            }

            if (targetInput) {
                targetInput.value =
                    selectedKey;

                targetInput.dispatchEvent(
                    new Event(
                        "change",
                        {
                            bubbles: true
                        }
                    )
                );
            }

            updateActiveButton(
                selectedOption
            );

            closeModal();

        } catch (error) {
            setStatus(
                error.message
                || "Card Back selection could not be saved.",
                true
            );

            applyButton.disabled = false;
        }
    }

    document.querySelectorAll(
        "[data-card-back-selector-open]"
    ).forEach(function (button) {
        button.addEventListener(
            "click",
            function () {
                openModal(button);
            }
        );
    });

    modal.querySelectorAll(
        "[data-card-back-selector-close]"
    ).forEach(function (button) {
        button.addEventListener(
            "click",
            closeModal
        );
    });

    if (uploadButton) {
        uploadButton.addEventListener(
            "click",
            uploadCardBack
        );
    }

    if (deleteMenuItem) {
        deleteMenuItem.addEventListener(
            "click",
            function (event) {
                event.preventDefault();
                event.stopPropagation();

                const option =
                    contextMenuOption;

                hideContextMenu();

                if (option) {
                    openDeleteConfirm(
                        option
                    );
                }
            }
        );
    }

    if (deleteConfirmCancel) {
        deleteConfirmCancel.addEventListener(
            "click",
            closeDeleteConfirm
        );
    }

    if (deleteConfirmButton) {
        deleteConfirmButton.addEventListener(
            "click",
            deletePendingCardBack
        );
    }

    if (applyButton) {
        applyButton.addEventListener(
            "click",
            applySelection
        );
    }

    document.addEventListener(
        "click",
        function () {
            hideContextMenu();
        }
    );

    if (contextMenu) {
        contextMenu.addEventListener(
            "click",
            function (event) {
                event.stopPropagation();
            }
        );
    }

    window.addEventListener(
        "resize",
        hideContextMenu
    );

    const modalBody =
        modal.querySelector(
            ".card-back-selector-body"
        );

    if (modalBody) {
        modalBody.addEventListener(
            "scroll",
            hideContextMenu,
            {
                passive: true
            }
        );
    }

    document.addEventListener(
        "keydown",
        function (event) {
            if (
                event.key !== "Escape"
                || modal.classList.contains(
                    "hidden"
                )
            ) {
                return;
            }

            event.preventDefault();

            if (
                deleteConfirm
                && !deleteConfirm.classList.contains(
                    "hidden"
                )
            ) {
                closeDeleteConfirm();
                return;
            }

            if (
                contextMenu
                && !contextMenu.classList.contains(
                    "hidden"
                )
            ) {
                hideContextMenu();
                return;
            }

            closeModal();
        }
    );

    window.iMomirCardBackSelector = {
        open: openModal,
        close: closeModal
    };
})();