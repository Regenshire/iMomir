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