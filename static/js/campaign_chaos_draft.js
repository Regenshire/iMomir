(function () {
    const screen = document.getElementById("chaosDraftScreen");
    const newDraftButton = document.getElementById("campaignNewDraftButton");
    const campaignSelect = document.getElementById("chaosCampaignSelect");
    const playerSelect = document.getElementById("campaignPlayerSelect");

    const currentPlayerCard = document.getElementById(
        "campaignCurrentPlayerCard"
    );
    const currentPlayerPortraitWrap = document.getElementById(
        "campaignCurrentPlayerPortraitWrap"
    );
    const currentPlayerName = document.getElementById(
        "campaignCurrentPlayerName"
    );

    if (!screen) {
        return;
    }

    async function postJson(url, body, fallbackMessage) {
        const cleanUrl = String(url || "").trim();

        if (!cleanUrl) {
            throw new Error(fallbackMessage);
        }

        const response = await fetch(cleanUrl, {
            method: "POST",
            headers: {
                "Accept": "application/json",
                "Content-Type": "application/json"
            },
            body: JSON.stringify(body || {})
        });

        const payload = await response.json();

        if (!response.ok || !payload.ok) {
            throw new Error(
                payload.message || fallbackMessage
            );
        }

        return payload;
    }

    function renderCurrentPlayer() {
        if (
            !playerSelect
            || !currentPlayerPortraitWrap
            || !currentPlayerName
        ) {
            return;
        }

        const selectedOption =
            playerSelect.options[playerSelect.selectedIndex];

        const playerName = (
            selectedOption?.dataset.playerName || ""
        ).trim();

        const portraitUrl = (
            selectedOption?.dataset.portrait || ""
        ).trim();

        if (!playerName) {
            if (currentPlayerCard) {
                currentPlayerCard.classList.add(
                    "campaign-current-player-card-empty"
                );
            }

            currentPlayerPortraitWrap.innerHTML =
                '<div id="campaignCurrentPlayerPlaceholder" class="campaign-current-player-placeholder">?</div>';

            currentPlayerName.textContent =
                "No Player Selected";

            return;
        }

        if (currentPlayerCard) {
            currentPlayerCard.classList.remove(
                "campaign-current-player-card-empty"
            );
        }

        if (portraitUrl) {
            currentPlayerPortraitWrap.innerHTML =
                '<img id="campaignCurrentPlayerPortrait" src="' +
                portraitUrl +
                '" alt="' +
                playerName.replace(/"/g, "&quot;") +
                '" class="campaign-current-player-portrait">';
        } else {
            currentPlayerPortraitWrap.innerHTML =
                '<div id="campaignCurrentPlayerPlaceholder" class="campaign-current-player-placeholder">' +
                playerName.charAt(0).toUpperCase() +
                "</div>";
        }

        currentPlayerName.textContent = playerName;
    }

    if (newDraftButton) {
        newDraftButton.addEventListener(
            "click",
            async function () {
                let confirmed = true;

                if (
                    window.iMomirConfirm
                    && typeof window.iMomirConfirm.show === "function"
                ) {
                    confirmed = await window.iMomirConfirm.show({
                        title: "Start New Draft",
                        message:
                            "Start a new draft? This resets pack availability for this draft.",
                        confirmText: "Start New Draft",
                        cancelText: "Cancel"
                    });
                } else {
                    confirmed = window.confirm(
                        "Start a new draft? This resets pack availability for this draft."
                    );
                }

                if (!confirmed) {
                    return;
                }

                newDraftButton.disabled = true;
                newDraftButton.classList.add(
                    "action-button-loading"
                );
                newDraftButton.textContent =
                    "Starting...";

                try {
                    const response = await fetch(
                        screen.dataset.chaosNewDraftUrl || "",
                        {
                            method: "POST",
                            headers: {
                                "Accept": "application/json"
                            }
                        }
                    );

                    const payload = await response.json();

                    if (!response.ok || !payload.ok) {
                        throw new Error(
                            payload.message
                            || "Failed to start new draft."
                        );
                    }

                    window.location.reload();
                } catch (error) {
                    console.error(error);

                    newDraftButton.disabled = false;
                    newDraftButton.classList.remove(
                        "action-button-loading"
                    );
                    newDraftButton.textContent =
                        "New Draft";

                    window.alert(
                        error.message
                        || "Failed to start new draft."
                    );
                }
            }
        );
    }

    if (campaignSelect) {
        campaignSelect.addEventListener(
            "change",
            async function () {
                try {
                    await postJson(
                        screen.dataset.campaignSelectUrl,
                        {
                            campaign_id:
                                campaignSelect.value || ""
                        },
                        "Failed to select campaign."
                    );

                    window.location.reload();
                } catch (error) {
                    console.error(error);
                }
            }
        );
    }

    if (playerSelect) {
        playerSelect.addEventListener(
            "change",
            async function () {
                renderCurrentPlayer();

                try {
                    await postJson(
                        screen.dataset.playerSelectUrl,
                        {
                            player_id:
                                playerSelect.value || ""
                        },
                        "Failed to select player."
                    );
                } catch (error) {
                    console.error(error);
                }
            }
        );

        renderCurrentPlayer();
    }
})();