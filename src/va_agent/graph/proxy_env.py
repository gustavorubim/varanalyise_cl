"""Helpers for handling problematic proxy environment settings."""

from __future__ import annotations

import os
from typing import Callable
from urllib.parse import urlparse

_PROXY_KEYS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
)


def _is_dead_local_proxy(value: str) -> bool:
    parsed = urlparse(value)
    host = (parsed.hostname or "").lower()
    if host not in {"127.0.0.1", "localhost"}:
        return False
    port = parsed.port
    return port == 9


def sanitize_dead_proxy_env(
    logger: Callable[[str], None] | None = None,
) -> dict[str, object]:
    """Detect and clear known-dead local proxy env settings.

    Some shells set proxy variables to ``http://127.0.0.1:9`` as a network
    blocker, which causes every outbound LLM request to fail with WinError
    10061. When detected, we clear proxy vars for this process.
    """
    poisoned = [
        key for key in _PROXY_KEYS if os.getenv(key) and _is_dead_local_proxy(os.getenv(key, ""))
    ]
    if not poisoned:
        return {"changed": False, "cleared": []}

    cleared: list[str] = []
    for key in _PROXY_KEYS:
        if key in os.environ:
            os.environ.pop(key, None)
            cleared.append(key)

    if logger is not None:
        logger(
            "Detected dead proxy env (127.0.0.1:9); cleared proxy variables for "
            "this process to allow LLM connectivity."
        )

    return {"changed": True, "cleared": cleared, "detected": poisoned}
