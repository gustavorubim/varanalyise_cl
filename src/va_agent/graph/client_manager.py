"""Resilient GenAI client manager with dynamic key refresh and retry.

Provides a singleton ``GenAIClientManager`` that wraps ``google.genai.Client``
with:
- A pluggable API-key callback (defaults to ``GOOGLE_API_KEY`` env var)
- Lazy re-authentication on a configurable interval
- Immediate key refresh on 401/403 auth errors
- Exponential-backoff retry for transient errors (429, 5xx)
- Fail-fast on client errors (400, 404)
"""

from __future__ import annotations

import logging
import os
import random
import time
from typing import Any, Callable

from google import genai
from google.genai import types
from google.genai.errors import APIError, ClientError, ServerError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# API key callback
# ---------------------------------------------------------------------------

_api_key_callback: Callable[[], str] | None = None


def _default_get_api_key() -> str:
    """Read GOOGLE_API_KEY from the environment (backward-compatible default)."""
    key = os.environ.get("GOOGLE_API_KEY", "")
    if not key:
        raise RuntimeError(
            "GOOGLE_API_KEY environment variable is not set and no custom "
            "API key callback has been registered via set_api_key_callback()."
        )
    return key


def set_api_key_callback(fn: Callable[[], str]) -> None:
    """Register a custom ``() -> str`` callable that returns the API key."""
    global _api_key_callback
    _api_key_callback = fn


def _get_api_key() -> str:
    cb = _api_key_callback or _default_get_api_key
    return cb()


# ---------------------------------------------------------------------------
# Retryable status codes
# ---------------------------------------------------------------------------

_AUTH_CODES = {401, 403}
_TRANSIENT_CODES = {429, 500, 502, 503, 504}
_FAIL_FAST_CODES = {400, 404}


# ---------------------------------------------------------------------------
# GenAIClientManager
# ---------------------------------------------------------------------------

class GenAIClientManager:
    """Manages a ``genai.Client`` with automatic key refresh and retry."""

    def __init__(
        self,
        *,
        refresh_interval_s: float = 600.0,
        max_retries: int = 5,
        base_delay_s: float = 1.0,
        max_delay_s: float = 60.0,
    ) -> None:
        self.refresh_interval_s = refresh_interval_s
        self.max_retries = max_retries
        self.base_delay_s = base_delay_s
        self.max_delay_s = max_delay_s

        self._client: genai.Client | None = None
        self._created_at: float = 0.0  # monotonic timestamp

    # -- internal helpers ---------------------------------------------------

    def _create_client(self) -> genai.Client:
        key = _get_api_key()
        client = genai.Client(api_key=key)
        self._client = client
        self._created_at = time.monotonic()
        logger.debug("GenAI client created (refresh in %ss)", self.refresh_interval_s)
        return client

    def _ensure_client(self) -> genai.Client:
        """Return existing client or recreate if refresh interval has elapsed."""
        if (
            self._client is None
            or (time.monotonic() - self._created_at) >= self.refresh_interval_s
        ):
            return self._create_client()
        return self._client

    def _force_refresh(self) -> genai.Client:
        """Immediately recreate the client (e.g. after auth error)."""
        logger.info("Forcing GenAI client refresh (auth error)")
        return self._create_client()

    # -- public API ---------------------------------------------------------

    def get_client(self) -> genai.Client:
        """Return the current (or freshly-created) raw client."""
        return self._ensure_client()

    def generate_content(
        self,
        *,
        model: str,
        contents: Any,
        config: types.GenerateContentConfig | None = None,
    ) -> Any:
        """Call ``client.models.generate_content`` with retry logic.

        Raises on non-retryable errors; retries on transient/auth errors.
        """
        auth_retried = False

        for attempt in range(self.max_retries + 1):
            client = self._ensure_client()
            try:
                kwargs: dict[str, Any] = {"model": model, "contents": contents}
                if config is not None:
                    kwargs["config"] = config
                return client.models.generate_content(**kwargs)

            except (ClientError, ServerError, APIError) as exc:
                code: int = exc.code

                # Fail fast on non-retryable client errors
                if code in _FAIL_FAST_CODES:
                    raise

                # Auth errors: refresh key and retry once
                if code in _AUTH_CODES:
                    if auth_retried:
                        raise
                    auth_retried = True
                    logger.warning("Auth error %d — refreshing client and retrying", code)
                    self._force_refresh()
                    continue

                # Transient errors: exponential backoff with full jitter
                if code in _TRANSIENT_CODES:
                    if attempt >= self.max_retries:
                        raise
                    delay = random.uniform(
                        0, min(self.max_delay_s, self.base_delay_s * (2 ** attempt))
                    )
                    logger.warning(
                        "Transient error %d (attempt %d/%d) — retrying in %.1fs",
                        code, attempt + 1, self.max_retries, delay,
                    )
                    time.sleep(delay)
                    continue

                # Unknown error code — raise immediately
                raise

        # Should not be reached, but just in case
        raise RuntimeError("Exhausted retries in generate_content")  # pragma: no cover


# ---------------------------------------------------------------------------
# Module-level singleton (mirrors sql_tools.py pattern)
# ---------------------------------------------------------------------------

_manager: GenAIClientManager | None = None


def set_client_manager(mgr: GenAIClientManager) -> None:
    """Install the global client manager (called by ``build.py`` during setup)."""
    global _manager
    _manager = mgr


def _get_manager() -> GenAIClientManager:
    if _manager is None:
        raise RuntimeError(
            "GenAIClientManager not initialized — call set_client_manager() first"
        )
    return _manager


def get_client() -> genai.Client:
    """Return the raw ``genai.Client`` from the singleton manager."""
    return _get_manager().get_client()


def generate_content(
    *,
    model: str,
    contents: Any,
    config: types.GenerateContentConfig | None = None,
) -> Any:
    """Module-level convenience: delegates to the singleton manager."""
    return _get_manager().generate_content(model=model, contents=contents, config=config)
