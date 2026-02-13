"""Tests for proxy environment sanitization."""

from __future__ import annotations

from va_agent.graph.proxy_env import sanitize_dead_proxy_env

_PROXY_KEYS = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")


def _clear_proxies(monkeypatch):
    for key in _PROXY_KEYS:
        monkeypatch.delenv(key, raising=False)


def test_sanitize_dead_proxy_env_clears_localhost_9(monkeypatch):
    _clear_proxies(monkeypatch)
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:9")
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9")

    result = sanitize_dead_proxy_env()

    assert result["changed"] is True
    assert "HTTP_PROXY" in result["cleared"]
    assert "HTTPS_PROXY" in result["cleared"]


def test_sanitize_dead_proxy_env_keeps_normal_proxy(monkeypatch):
    _clear_proxies(monkeypatch)
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.example.com:8080")

    result = sanitize_dead_proxy_env()

    assert result["changed"] is False
