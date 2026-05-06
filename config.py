"""
config.py — single source of truth for all runtime constants.

Every value can be overridden with an env var prefixed KALSHI_:
  KALSHI_PORT=8080 python server/app.py
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KALSHI_")

    # Server
    port: int = 5991

    # Database
    db_path: str = "kalshi_prices.db"

    # Kalshi API
    base_url: str = "https://external-api.kalshi.com/trade-api/v2"

    # Feed
    reconnect_delay_s: float = 5.0
    max_match_age_h: float = 48.0
    poll_interval_s: float = 2.5

    # Tennis series tickers to track (match winner markets)
    series_tickers: list[str] = [
        "KXATPMATCH",
        "KXWTAMATCH",
        "KXATPGSPREAD",
    ]

    # Connection pool
    max_streams_per_host: int = 20
    feed_queue_maxsize: int = 20_000

    # Circuit breaker
    cb_max_failures: int = 5
    cb_reset_after_s: float = 300.0

    # Logging
    log_level: str = "INFO"


settings = Settings()
