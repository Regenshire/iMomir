import json

from urllib.parse import urlparse

from flask import request


class UINavigation:
    """
    Standardized browser-style navigation actions for iMomir UI pages.

    Back and Forward use the browser's actual navigation history on the
    client. This class supplies consistent action names and safe same-origin
    fallback URLs for templates when browser history is unavailable.
    """

    BACK_ACTION = "back"
    FORWARD_ACTION = "forward"
    DEFAULT_FALLBACK_URL = "/"

    def _normalize_internal_url(self, value):
        raw_value = str(
            value or ""
        ).strip()

        if not raw_value:
            return ""

        if (
            "\r" in raw_value
            or "\n" in raw_value
        ):
            return ""

        try:
            parsed_value = urlparse(
                raw_value
            )
        except ValueError:
            return ""

        if (
            parsed_value.scheme
            or parsed_value.netloc
        ):
            try:
                current_origin = urlparse(
                    request.host_url
                )
            except (
                RuntimeError,
                ValueError,
            ):
                return ""

            if parsed_value.scheme not in {
                "http",
                "https",
            }:
                return ""

            if (
                parsed_value.netloc
                != current_origin.netloc
            ):
                return ""

            normalized_url = (
                parsed_value.path
                or "/"
            )

            if parsed_value.query:
                normalized_url += (
                    f"?{parsed_value.query}"
                )

        else:
            normalized_url = raw_value

        if (
            not normalized_url.startswith("/")
            or normalized_url.startswith("//")
        ):
            return ""

        return normalized_url

    def get_back_fallback_url(
        self,
        default_url=None,
    ):
        fallback_url = (
            self._normalize_internal_url(
                default_url
                or self.DEFAULT_FALLBACK_URL
            )
            or self.DEFAULT_FALLBACK_URL
        )

        referrer_url = (
            self._normalize_internal_url(
                request.referrer
            )
        )

        current_url = (
            self._normalize_internal_url(
                request.full_path
                if request.query_string
                else request.path
            )
        )

        if (
            referrer_url
            and referrer_url != current_url
        ):
            return referrer_url

        return fallback_url

    def get_forward_fallback_url(
        self,
        default_url=None,
    ):
        return (
            self._normalize_internal_url(
                default_url
                or request.path
                or self.DEFAULT_FALLBACK_URL
            )
            or self.DEFAULT_FALLBACK_URL
        )

    def build_template_context(self):
        return {
            "back_action": (
                self.BACK_ACTION
            ),
            "forward_action": (
                self.FORWARD_ACTION
            ),
            "back_fallback_url": (
                self.get_back_fallback_url()
            ),
            "forward_fallback_url": (
                self.get_forward_fallback_url()
            ),
        }

class UIListFilter:
    """
    Reusable server-side filtering and sorting for small UI lists.

    This operates on already-loaded row dictionaries rather than
    reaching into the database. Routes decide which fields are
    searchable/filterable while this class standardizes the behavior.
    """

    def _normalize_choice(
        self,
        value,
        allowed_values,
        default_value,
    ):
        normalized_value = str(
            value or ""
        ).strip().lower()

        if normalized_value in allowed_values:
            return normalized_value

        return default_value

    def _normalize_sort_value(
        self,
        value,
        value_type,
    ):
        if value_type == "number":
            try:
                return float(value or 0)
            except (TypeError, ValueError):
                return 0.0

        return str(
            value or ""
        ).casefold()

    def filter_rows(
        self,
        rows,
        *,
        search_text="",
        search_fields=(),
        status_value="all",
        status_field=None,
        status_options=None,
        sort_option="",
        sort_options=None,
        default_sort_option="",
        page_value=1,
        page_size_value=20,
        allowed_page_sizes=(10, 20, 50, 100),
        default_page_size=20,
    ):
        source_rows = list(
            rows or []
        )

        normalized_search_text = str(
            search_text or ""
        ).strip()

        filtered_rows = list(source_rows)

        if normalized_search_text:
            search_needle = (
                normalized_search_text.casefold()
            )

            filtered_rows = [
                row
                for row in filtered_rows
                if any(
                    search_needle
                    in str(
                        row.get(field_name) or ""
                    ).casefold()
                    for field_name in search_fields
                )
            ]

        normalized_status_value = "all"

        if status_field and status_options:
            allowed_status_values = {
                "all"
            }

            allowed_status_values.update(
                status_options.keys()
            )

            normalized_status_value = (
                self._normalize_choice(
                    status_value,
                    allowed_status_values,
                    "all",
                )
            )

            if normalized_status_value != "all":
                expected_status_value = (
                    status_options[
                        normalized_status_value
                    ]
                )

                filtered_rows = [
                    row
                    for row in filtered_rows
                    if row.get(status_field)
                    == expected_status_value
                ]

        sort_options = (
            sort_options or {}
        )

        if sort_options:
            fallback_sort_option = (
                default_sort_option
                if default_sort_option in sort_options
                else next(iter(sort_options))
            )

            normalized_sort_option = (
                self._normalize_choice(
                    sort_option,
                    set(sort_options.keys()),
                    fallback_sort_option,
                )
            )

            sort_definition = (
                sort_options[
                    normalized_sort_option
                ]
            )

            sort_fields = (
                sort_definition.get(
                    "fields"
                )
                or [
                    {
                        "field": (
                            sort_definition.get(
                                "field"
                            )
                            or ""
                        ),
                        "type": (
                            sort_definition.get(
                                "type"
                            )
                            or "text"
                        ),
                        "reverse": bool(
                            sort_definition.get(
                                "reverse",
                                False,
                            )
                        ),
                    }
                ]
            )

            # Python sorting is stable. Apply lower-priority
            # fields first so the first definition remains
            # the primary sort key.
            for sort_field_definition in reversed(
                sort_fields
            ):
                sort_field = (
                    sort_field_definition.get(
                        "field"
                    )
                    or ""
                )

                sort_value_type = (
                    sort_field_definition.get(
                        "type"
                    )
                    or "text"
                )

                sort_reverse = bool(
                    sort_field_definition.get(
                        "reverse",
                        False,
                    )
                )

                filtered_rows.sort(
                    key=lambda row, field_name=sort_field, value_type=sort_value_type: (
                        self._normalize_sort_value(
                            row.get(field_name),
                            value_type,
                        )
                    ),
                    reverse=sort_reverse,
                )

        else:
            normalized_sort_option = ""

        filtered_count = len(filtered_rows)

        normalized_allowed_page_sizes = tuple(
            int(page_size)
            for page_size in allowed_page_sizes
            if int(page_size) > 0
        )

        if not normalized_allowed_page_sizes:
            normalized_allowed_page_sizes = (20,)

        try:
            normalized_default_page_size = int(
                default_page_size
            )
        except (TypeError, ValueError):
            normalized_default_page_size = 20

        if (
            normalized_default_page_size
            not in normalized_allowed_page_sizes
        ):
            normalized_default_page_size = (
                normalized_allowed_page_sizes[0]
            )

        try:
            normalized_page_size = int(
                page_size_value
            )
        except (TypeError, ValueError):
            normalized_page_size = (
                normalized_default_page_size
            )

        if (
            normalized_page_size
            not in normalized_allowed_page_sizes
        ):
            normalized_page_size = (
                normalized_default_page_size
            )

        try:
            normalized_page = int(page_value)
        except (TypeError, ValueError):
            normalized_page = 1

        normalized_page = max(
            1,
            normalized_page,
        )

        total_pages = max(
            1,
            (
                filtered_count
                + normalized_page_size
                - 1
            )
            // normalized_page_size,
        )

        normalized_page = min(
            normalized_page,
            total_pages,
        )

        start_index = (
            normalized_page - 1
        ) * normalized_page_size

        end_index = min(
            start_index + normalized_page_size,
            filtered_count,
        )

        paged_rows = filtered_rows[
            start_index:end_index
        ]

        return {
            "rows": paged_rows,
            "search_text": (
                normalized_search_text
            ),
            "status_value": (
                normalized_status_value
            ),
            "sort_option": (
                normalized_sort_option
            ),
            "total_count": len(source_rows),
            "filtered_count": filtered_count,
            "pagination": {
                "page": normalized_page,
                "page_size": normalized_page_size,
                "allowed_page_sizes": (
                    normalized_allowed_page_sizes
                ),
                "total_count": filtered_count,
                "total_pages": total_pages,
                "has_previous": (
                    normalized_page > 1
                ),
                "has_next": (
                    normalized_page < total_pages
                ),
                "previous_page": max(
                    1,
                    normalized_page - 1,
                ),
                "next_page": min(
                    total_pages,
                    normalized_page + 1,
                ),
                "start_item": (
                    start_index + 1
                    if filtered_count
                    else 0
                ),
                "end_item": end_index,
            },
        }

class UICardCollection:
    """
    Server-side configuration and normalization layer for reusable
    iMomir card-collection interfaces.

    Routes remain responsible for loading cards and building route URLs.
    This class owns the stable UI contract consumed by card_collection.js:
    feature flags, filter flags, client state defaults, normalized cards,
    per-card action URLs, labels, and collection-level endpoints.

    The class deliberately does not query the database and does not render
    HTML. That keeps it reusable for Custom Draft Sets, Chaos Draft views,
    Campaign packs, previews, deck-style collections, and future card lists.
    """

    FEATURE_DEFAULTS = {
        "filters": True,
        "selection": True,
        "print_export": True,
        "copy": True,
        "stats": True,
        "list_view": True,
        "grid_view": True,
        "maximize": True,
        "card_size": True,
        "zoom": True,
        "flip": True,
        "alternate_image": True,
        "change_printing": True,

        # Optional page-specific extension capabilities.
        "special_slot": False,
        "remove_card": False,
        "foil_toggle": False,
    }

    # Named profiles prevent route-specific feature combinations from being
    # duplicated throughout app.py. Every profile can still be overridden by
    # the caller with the `features=` argument to build().
    FEATURE_PROFILES = {
        "default": {},
        "custom_draft_set": {
            "special_slot": True,
            "remove_card": True,
            "foil_toggle": True,
        },
        "chaos_draft_view": {
            # /play/draft -> /chaos-draft/view must not allow replacing the
            # underlying pack printing from this UI.
            "change_printing": False,
        },
        "campaign_pack": {
            "foil_toggle": True,
        },
        "campaign_pack_preview": {
            # Preview cards do not have tracked-pack-card IDs yet.
            "change_printing": False,
            "foil_toggle": False,
        },
    }

    FILTER_DEFAULTS = {
        "text": True,
        "rarity": True,
        "color_identity": True,
        "mana_value": True,
        "spell_type": True,
        "duplicates": True,
        "alternate_image": True,
        "special_slot": False,
        "set_code": True,
        "digital": True,
        "per_page": True,
        "sort": True,
    }

    CARD_SIZE_OPTIONS = (
        70,
        85,
        100,
        115,
        130,
        150,
    )

    CARD_BASE_WIDTH_PX = 176

    PAGE_SIZE_OPTIONS = (
        50,
        100,
        250,
        500,
        1000,
    )

    SORT_OPTIONS = (
        ("name_asc", "Name A-Z"),
        ("name_desc", "Name Z-A"),
        ("set_asc", "Set Code A-Z"),
        ("set_desc", "Set Code Z-A"),
        ("rarity_low_high", "Rarity Common → Mythic"),
        ("rarity_high_low", "Rarity Mythic → Common"),
        ("mv_low_high", "Mana Value Low → High"),
        ("mv_high_low", "Mana Value High → Low"),
        ("edhrec_rank_best", "EDHREC Rank Best"),
        ("edhrec_rank_worst", "EDHREC Rank Worst"),
        ("edhrec_salt_high", "EDHREC Salt High"),
        ("edhrec_salt_low", "EDHREC Salt Low"),
        ("price_high", "Price High"),
        ("price_low", "Price Low"),
    )

    COPY_FORMATS = (
        ("simple", "Simple"),
        ("detailed", "Detailed"),
    )

    LABEL_DEFAULTS = {
        "card_singular": "card",
        "card_plural": "cards",
        "copy": "Copy",
        "print_export": "Print / Export Selected Cards",
        "stats": "Stats",
        "filters": "Filters",
        "card_size": "Card Size",
        "list_view": "List View",
        "grid_view": "Grid View",
        "maximize": "Maximize",
        "restore": "Restore",
        "empty": "No cards are available.",
    }

    ENDPOINT_DEFAULTS = {
        # Collection-level operations.
        "print": "",
        "export_zip": "",
        "copy": "",
        "face_data": "/card-face-data",

        # Optional route-level fallbacks. Per-card action URLs take priority.
        "change_printing_options": "",
        "change_printing_update": "",
        "foil_update": "",
        "special_slot_update": "",
        "remove": "",
    }

    CLIENT_STATE_DEFAULTS = {
        "persist": True,
        "persist_filters": False,
        "persist_selection": False,
    }

    def _normalize_bool(self, value):
        if isinstance(value, bool):
            return value

        if isinstance(value, (int, float)):
            return bool(value)

        return str(value or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }

    def _merge_flags(self, defaults, overrides):
        merged = dict(defaults)

        for key, value in dict(overrides or {}).items():
            if key in merged:
                merged[key] = self._normalize_bool(value)

        return merged

    def _merge_mapping(self, defaults, overrides):
        merged = dict(defaults or {})

        for key, value in dict(overrides or {}).items():
            if value is None:
                continue

            merged[str(key)] = value

        return merged

    def _normalize_choice(self, value, allowed_values, default_value):
        normalized = str(value or "").strip().lower()

        if normalized in allowed_values:
            return normalized

        return default_value

    def _normalize_integer_choice(self, value, allowed_values, default_value):
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = int(default_value)

        if parsed in allowed_values:
            return parsed

        return int(default_value)

    def _parse_json_list(self, value):
        if isinstance(value, (list, tuple, set)):
            return [
                str(item)
                for item in value
                if str(item).strip()
            ]

        if not value:
            return []

        try:
            parsed = json.loads(str(value))
        except (
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ):
            return []

        if not isinstance(parsed, list):
            return []

        return [
            str(item)
            for item in parsed
            if str(item).strip()
        ]

    def _normalize_options(self, options, fallback_options):
        normalized = []

        for option in list(fallback_options if options is None else options):
            if isinstance(option, dict):
                value = str(option.get("value") or "").strip()
                label = str(option.get("label") or value).strip()
            elif isinstance(option, (list, tuple)) and len(option) >= 2:
                value = str(option[0] or "").strip()
                label = str(option[1] or value).strip()
            else:
                value = str(option or "").strip()
                label = value

            if not value:
                continue

            normalized.append({
                "value": value,
                "label": label or value,
            })

        return normalized

    def _normalize_actions(self, actions):
        normalized = {}

        for key, value in dict(actions or {}).items():
            clean_key = str(key or "").strip()

            if not clean_key or value is None:
                continue

            # Action payloads are intentionally allowed to be strings,
            # booleans, IDs, or small dictionaries. This lets individual
            # consumers attach endpoint URLs plus page-specific metadata
            # without forcing another schema revision.
            normalized[clean_key] = value

        return normalized

    def _resolve_feature_profile(self, profile_name, overrides):
        clean_profile = str(profile_name or "default").strip().lower()

        if clean_profile not in self.FEATURE_PROFILES:
            raise ValueError(
                f"Unknown card collection feature profile: {clean_profile}"
            )

        profile_features = self._merge_flags(
            self.FEATURE_DEFAULTS,
            self.FEATURE_PROFILES[clean_profile],
        )

        return self._merge_flags(
            profile_features,
            overrides,
        )

    def normalize_card(self, card, index=0):
        """
        Convert page-specific card records into the canonical card-collection
        representation.

        Routes may add page-specific action URLs through the `actions`
        dictionary. The client controller treats those actions as capabilities
        for that specific card and does not need to know which route supplied
        them.
        """

        source = dict(card or {})

        ui_card_id = (
            source.get("ui_card_id")
            or source.get("custom_set_card_id")
            or source.get("tracked_pack_card_id")
            or source.get("card_uuid")
            or source.get("card_key")
            or f"card-{index + 1}"
        )

        selection_id = (
            source.get("selection_id")
            or source.get("custom_set_card_id")
            or source.get("tracked_pack_card_id")
            or ui_card_id
        )

        color_identity = source.get("color_identity")

        if color_identity is None:
            color_identity = source.get("color_identity_json")

        colors = source.get("colors")

        if colors is None:
            colors = source.get("colors_json")

        badges = source.get("badges")

        if badges is None:
            badges = source.get("special_badges")

        if not isinstance(badges, (list, tuple)):
            badges = []

        image_src = str(
            source.get("image_src")
            or source.get("image_url")
            or ""
        )

        zoom_src = str(
            source.get("zoom_src")
            or source.get("image_preview_src")
            or image_src
            or ""
        )

        card_name = str(source.get("card_name") or source.get("name") or "")
        set_code = str(
            source.get("set_code")
            or source.get("card_set_code")
            or ""
        )
        collector_number = str(source.get("collector_number") or "")
        rarity = str(source.get("rarity") or "")
        type_line = str(source.get("type_line") or "")

        search_text = str(source.get("search_text") or "").strip()

        if not search_text:
            search_text = " ".join(
                part
                for part in (
                    card_name,
                    set_code,
                    collector_number,
                    rarity,
                    type_line,
                    str(source.get("mana_value") or ""),
                    " ".join(self._parse_json_list(color_identity)),
                )
                if part
            )

        return {
            "ui_card_id": str(ui_card_id),
            "selection_id": str(selection_id),
            "card_uuid": str(source.get("card_uuid") or ""),
            "card_key": str(source.get("card_key") or ""),
            "card_name": card_name,
            "set_code": set_code,
            "collector_number": collector_number,
            "rarity": rarity,
            "type_line": type_line,
            "mana_value": source.get("mana_value"),
            "colors": self._parse_json_list(colors),
            "color_identity": self._parse_json_list(color_identity),
            "edhrec_rank": source.get("edhrec_rank"),
            "edhrec_saltiness": source.get("edhrec_saltiness"),
            "sort_price": source.get("sort_price"),
            "display_price": (
                source.get("display_price")
                or source.get("price")
                or ""
            ),
            "currency": str(source.get("currency") or "USD"),
            "image_src": image_src,
            "zoom_src": zoom_src,
            "finish_type": str(source.get("finish_type") or ""),
            "is_foil": self._normalize_bool(
                source.get(
                    "is_foil",
                    source.get("sheet_is_foil", False),
                )
            ),
            "is_digital": self._normalize_bool(
                source.get("is_digital", False)
            ),
            "has_alternate_source": self._normalize_bool(
                source.get("has_alternate_source", False)
            ),
            "alternate_remove_bleed": self._normalize_bool(
                source.get("alternate_remove_bleed", False)
            ),
            "special_category": str(
                source.get("special_category")
                or source.get("special_category_index")
                or "0"
            ),
            "badges": [str(item) for item in badges],
            "search_text": search_text.casefold(),
            "actions": self._normalize_actions(source.get("actions")),
        }

    def normalize_cards(self, cards):
        return [
            self.normalize_card(card, index=index)
            for index, card in enumerate(cards or [])
        ]

    def build(
        self,
        collection_id,
        title,
        cards=None,
        *,
        subtitle="",
        profile="default",
        features=None,
        filters=None,
        filter_options=None,
        endpoints=None,
        labels=None,
        client_state=None,
        default_view_mode="list",
        default_card_size=100,
        default_page_size=100,
        default_sort="name_asc",
        copy_formats=None,
    ):
        clean_collection_id = str(collection_id or "").strip()

        if not clean_collection_id:
            raise ValueError("Card collection ID is required.")

        feature_flags = self._resolve_feature_profile(
            profile,
            features,
        )

        filter_flags = self._merge_flags(
            self.FILTER_DEFAULTS,
            filters,
        )

        if not feature_flags["filters"]:
            filter_flags = {
                key: False
                for key in filter_flags
            }

        # Filters tied to disabled capabilities should disappear automatically.
        if not feature_flags["alternate_image"]:
            filter_flags["alternate_image"] = False

        if not feature_flags["special_slot"]:
            filter_flags["special_slot"] = False

        allowed_view_modes = set()

        if feature_flags["list_view"]:
            allowed_view_modes.add("list")

        if feature_flags["grid_view"]:
            allowed_view_modes.add("grid")

        if not allowed_view_modes:
            # A collection always needs one render mode. Use Grid View as the
            # inert fallback even when both UI toggles are intentionally hidden.
            allowed_view_modes.add("grid")

        fallback_view_mode = (
            "list"
            if "list" in allowed_view_modes
            else "grid"
        )

        normalized_view_mode = self._normalize_choice(
            default_view_mode,
            allowed_view_modes,
            fallback_view_mode,
        )

        normalized_card_size = self._normalize_integer_choice(
            default_card_size,
            self.CARD_SIZE_OPTIONS,
            100,
        )

        normalized_page_size = self._normalize_integer_choice(
            default_page_size,
            self.PAGE_SIZE_OPTIONS,
            100,
        )

        allowed_sort_values = {
            value
            for value, _label in self.SORT_OPTIONS
        }

        normalized_sort = self._normalize_choice(
            default_sort,
            allowed_sort_values,
            "name_asc",
        )

        normalized_cards = self.normalize_cards(cards)
        normalized_copy_formats = self._normalize_options(
            copy_formats,
            self.COPY_FORMATS,
        )

        normalized_labels = self._merge_mapping(
            self.LABEL_DEFAULTS,
            labels,
        )

        normalized_endpoints = self._merge_mapping(
            self.ENDPOINT_DEFAULTS,
            endpoints,
        )

        normalized_client_state = self._merge_mapping(
            self.CLIENT_STATE_DEFAULTS,
            client_state,
        )

        # Make bool-like state options predictable before JSON serialization.
        for bool_key in (
            "persist",
            "persist_filters",
            "persist_selection",
        ):
            normalized_client_state[bool_key] = self._normalize_bool(
                normalized_client_state.get(bool_key)
            )

        normalized_client_state["storage_key"] = str(
            normalized_client_state.get("storage_key")
            or f"iMomir.cardCollection.{clean_collection_id}"
        )

        return {
            "schema_version": 1,
            "collection_id": clean_collection_id,
            "title": str(title or "Cards"),
            "subtitle": str(subtitle or ""),
            "cards": normalized_cards,
            "card_count": len(normalized_cards),
            "features": feature_flags,
            "filters": filter_flags,
            "filter_options": dict(filter_options or {}),
            "endpoints": normalized_endpoints,
            "labels": normalized_labels,
            "client_state": normalized_client_state,
            "options": {
                "default_view_mode": normalized_view_mode,
                "default_card_size": normalized_card_size,
                "default_page_size": normalized_page_size,
                "default_sort": normalized_sort,
                "card_base_width_px": self.CARD_BASE_WIDTH_PX,
                "card_size_options": list(self.CARD_SIZE_OPTIONS),
                "page_size_options": list(self.PAGE_SIZE_OPTIONS),
                "sort_options": [
                    {
                        "value": value,
                        "label": label,
                    }
                    for value, label in self.SORT_OPTIONS
                ],
                "copy_formats": normalized_copy_formats,
            },
        }


ui_card_collection = UICardCollection()

ui_list_filter = UIListFilter()


ui_navigation = UINavigation()


def register_ui_navigation(app):
    if app.extensions.get(
        "imomir_ui_navigation"
    ):
        return

    app.extensions[
        "imomir_ui_navigation"
    ] = ui_navigation

    @app.context_processor
    def inject_ui_navigation():
        return {
            "ui_navigation": (
                ui_navigation.build_template_context()
            )
        }