"""Application configuration via environment variables."""

from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Settings loaded from environment with VA_ prefix."""

    model_config = {
        "env_prefix": "VA_",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }

    # LLM (google_genai provider — requires GOOGLE_API_KEY env var)
    model_name: str = "gemini-3-flash-preview"
    temperature: float = 0.0

    # SQL safety
    max_rows: int = 500
    query_timeout: int = 30  # seconds

    # Paths
    db_path: Path = Path("runs/warehouse.db")
    runs_dir: Path = Path("runs")
    cache_dir: Path = Path(".cache")

    # Agent
    verbose: bool = False

    # Retry / client manager
    llm_refresh_interval_s: float = 600.0   # key refresh interval (seconds)
    llm_max_retries: int = 5                # max retries for transient errors
    llm_base_delay_s: float = 1.0           # initial backoff delay
    llm_max_delay_s: float = 60.0           # backoff cap

    def ensure_dirs(self) -> None:
        """Create required directories if they don't exist."""
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    @property
    def db_uri(self) -> str:
        """SQLite URI with read-only mode for safe connections."""
        posix = self.db_path.resolve().as_posix()
        return f"file:{posix}?mode=ro"
