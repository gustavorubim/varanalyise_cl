"""Unit tests for GenAIClientManager."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest
from google.genai.errors import ClientError, ServerError

from va_agent.graph import client_manager as cm
from va_agent.graph.client_manager import GenAIClientManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_client_error(code: int) -> ClientError:
    return ClientError(code, {"error": {"message": "test"}})


def _make_server_error(code: int) -> ServerError:
    return ServerError(code, {"error": {"message": "test"}})


# ---------------------------------------------------------------------------
# API key callback tests
# ---------------------------------------------------------------------------

class TestApiKeyCallback:
    def test_default_reads_env(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_API_KEY", "test-key-123")
        # Reset callback to default
        cm._api_key_callback = None
        assert cm._get_api_key() == "test-key-123"

    def test_custom_callback_used(self):
        cm.set_api_key_callback(lambda: "custom-key")
        try:
            assert cm._get_api_key() == "custom-key"
        finally:
            cm._api_key_callback = None

    def test_missing_key_raises(self, monkeypatch):
        monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
        cm._api_key_callback = None
        with pytest.raises(RuntimeError, match="GOOGLE_API_KEY"):
            cm._get_api_key()


# ---------------------------------------------------------------------------
# Client lifecycle tests
# ---------------------------------------------------------------------------

class TestClientLifecycle:
    @patch.object(cm, "_get_api_key", return_value="k")
    @patch("va_agent.graph.client_manager.genai.Client")
    def test_created_on_first_call(self, mock_client_cls, _mock_key):
        mgr = GenAIClientManager()
        client = mgr.get_client()
        mock_client_cls.assert_called_once_with(api_key="k")
        assert client is mock_client_cls.return_value

    @patch.object(cm, "_get_api_key", return_value="k")
    @patch("va_agent.graph.client_manager.genai.Client")
    def test_reused_within_interval(self, mock_client_cls, _mock_key):
        mgr = GenAIClientManager(refresh_interval_s=600.0)
        c1 = mgr.get_client()
        c2 = mgr.get_client()
        assert c1 is c2
        assert mock_client_cls.call_count == 1

    @patch.object(cm, "_get_api_key", return_value="k")
    @patch("va_agent.graph.client_manager.genai.Client")
    @patch("va_agent.graph.client_manager.time.monotonic")
    def test_recreated_after_interval(self, mock_mono, mock_client_cls, _mock_key):
        mock_mono.return_value = 0.0
        mgr = GenAIClientManager(refresh_interval_s=10.0)

        mgr.get_client()
        assert mock_client_cls.call_count == 1

        # Advance past the refresh interval
        mock_mono.return_value = 11.0
        mgr.get_client()
        assert mock_client_cls.call_count == 2


# ---------------------------------------------------------------------------
# Auth retry tests (401/403)
# ---------------------------------------------------------------------------

class TestAuthRetry:
    @patch.object(cm, "_get_api_key", return_value="k")
    @patch("va_agent.graph.client_manager.genai.Client")
    def test_401_triggers_refresh_and_retry(self, mock_client_cls, _mock_key):
        mock_gen = mock_client_cls.return_value.models.generate_content
        ok_resp = MagicMock(name="ok_response")
        mock_gen.side_effect = [
            _make_client_error(401),
            ok_resp,
        ]

        mgr = GenAIClientManager()
        result = mgr.generate_content(model="m", contents="hi")

        assert result is ok_resp
        # Client should have been recreated (original + force refresh)
        assert mock_client_cls.call_count == 2

    @patch.object(cm, "_get_api_key", return_value="k")
    @patch("va_agent.graph.client_manager.genai.Client")
    def test_403_triggers_refresh_and_retry(self, mock_client_cls, _mock_key):
        mock_gen = mock_client_cls.return_value.models.generate_content
        mock_gen.side_effect = [
            _make_client_error(403),
            MagicMock(name="ok_response"),
        ]

        mgr = GenAIClientManager()
        result = mgr.generate_content(model="m", contents="hi")

        assert mock_client_cls.call_count == 2

    @patch.object(cm, "_get_api_key", return_value="k")
    @patch("va_agent.graph.client_manager.genai.Client")
    def test_persistent_auth_error_raises(self, mock_client_cls, _mock_key):
        mock_gen = mock_client_cls.return_value.models.generate_content
        mock_gen.side_effect = _make_client_error(401)

        mgr = GenAIClientManager()
        with pytest.raises(ClientError):
            mgr.generate_content(model="m", contents="hi")

        # First call creates client, auth error triggers one refresh, then raises
        assert mock_client_cls.call_count == 2


# ---------------------------------------------------------------------------
# Transient retry tests (429, 5xx)
# ---------------------------------------------------------------------------

class TestTransientRetry:
    @patch("va_agent.graph.client_manager.time.sleep")
    @patch.object(cm, "_get_api_key", return_value="k")
    @patch("va_agent.graph.client_manager.genai.Client")
    def test_429_retried_then_succeeds(self, mock_client_cls, _mock_key, mock_sleep):
        ok_resp = MagicMock(name="ok")
        mock_gen = mock_client_cls.return_value.models.generate_content
        mock_gen.side_effect = [
            _make_server_error(429),
            _make_server_error(429),
            ok_resp,
        ]

        mgr = GenAIClientManager(max_retries=5, base_delay_s=0.01, max_delay_s=0.1)
        result = mgr.generate_content(model="m", contents="hi")

        assert result is ok_resp
        assert mock_sleep.call_count == 2

    @patch("va_agent.graph.client_manager.time.sleep")
    @patch.object(cm, "_get_api_key", return_value="k")
    @patch("va_agent.graph.client_manager.genai.Client")
    def test_500_exhausts_retries(self, mock_client_cls, _mock_key, mock_sleep):
        mock_gen = mock_client_cls.return_value.models.generate_content
        mock_gen.side_effect = _make_server_error(500)

        mgr = GenAIClientManager(max_retries=3, base_delay_s=0.01, max_delay_s=0.1)
        with pytest.raises(ServerError):
            mgr.generate_content(model="m", contents="hi")

        # 1 initial + 3 retries = 4 calls, 3 sleeps
        assert mock_gen.call_count == 4
        assert mock_sleep.call_count == 3

    @patch("va_agent.graph.client_manager.time.sleep")
    @patch.object(cm, "_get_api_key", return_value="k")
    @patch("va_agent.graph.client_manager.genai.Client")
    def test_503_retried(self, mock_client_cls, _mock_key, mock_sleep):
        ok_resp = MagicMock(name="ok")
        mock_gen = mock_client_cls.return_value.models.generate_content
        mock_gen.side_effect = [_make_server_error(503), ok_resp]

        mgr = GenAIClientManager(max_retries=5, base_delay_s=0.01, max_delay_s=0.1)
        result = mgr.generate_content(model="m", contents="hi")

        assert result is ok_resp


# ---------------------------------------------------------------------------
# Fail-fast tests (400, 404)
# ---------------------------------------------------------------------------

class TestFailFast:
    @patch.object(cm, "_get_api_key", return_value="k")
    @patch("va_agent.graph.client_manager.genai.Client")
    def test_400_raises_immediately(self, mock_client_cls, _mock_key):
        mock_gen = mock_client_cls.return_value.models.generate_content
        mock_gen.side_effect = _make_client_error(400)

        mgr = GenAIClientManager()
        with pytest.raises(ClientError):
            mgr.generate_content(model="m", contents="hi")

        assert mock_gen.call_count == 1

    @patch.object(cm, "_get_api_key", return_value="k")
    @patch("va_agent.graph.client_manager.genai.Client")
    def test_404_raises_immediately(self, mock_client_cls, _mock_key):
        mock_gen = mock_client_cls.return_value.models.generate_content
        mock_gen.side_effect = _make_client_error(404)

        mgr = GenAIClientManager()
        with pytest.raises(ClientError):
            mgr.generate_content(model="m", contents="hi")

        assert mock_gen.call_count == 1


# ---------------------------------------------------------------------------
# Module-level accessor tests
# ---------------------------------------------------------------------------

class TestModuleAccessors:
    def setup_method(self):
        """Reset module singleton before each test."""
        cm._manager = None

    def test_get_client_raises_without_init(self):
        with pytest.raises(RuntimeError, match="not initialized"):
            cm.get_client()

    def test_generate_content_raises_without_init(self):
        with pytest.raises(RuntimeError, match="not initialized"):
            cm.generate_content(model="m", contents="x")

    @patch.object(cm, "_get_api_key", return_value="k")
    @patch("va_agent.graph.client_manager.genai.Client")
    def test_generate_content_delegates(self, mock_client_cls, _mock_key):
        ok_resp = MagicMock(name="ok")
        mock_client_cls.return_value.models.generate_content.return_value = ok_resp

        mgr = GenAIClientManager()
        cm.set_client_manager(mgr)

        result = cm.generate_content(model="m", contents="hi")
        assert result is ok_resp

    @patch.object(cm, "_get_api_key", return_value="k")
    @patch("va_agent.graph.client_manager.genai.Client")
    def test_get_client_returns_client(self, mock_client_cls, _mock_key):
        mgr = GenAIClientManager()
        cm.set_client_manager(mgr)

        client = cm.get_client()
        assert client is mock_client_cls.return_value
