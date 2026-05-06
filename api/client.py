"""
api/client.py — async Kalshi HTTP client.

Kalshi API is public REST (no auth required for data reads).
Base URL: https://external-api.kalshi.com/trade-api/v2

Key endpoints used:
  GET /series          — list all series (filter by tags=tennis)
  GET /markets         — list markets with filters (series_ticker, status, min_updated_ts)
  GET /markets/{ticker} — single market details
  GET /events/{ticker}  — event details

The feed creates one KalshiClient per aiohttp.ClientSession (one session
shared across all concurrent polling streams).
"""
from __future__ import annotations
import asyncio
import logging
import ssl
from typing import Any

import aiohttp
import certifi

from config import settings

log = logging.getLogger(__name__)


class KalshiClient:
    """
    Async wrapper around the public Kalshi Trade API v2.

    Usage:
        connector = aiohttp.TCPConnector(limit_per_host=20)
        async with aiohttp.ClientSession(connector=connector) as session:
            client = KalshiClient(session)
            series = await client.get_series(tags=["tennis"])
            markets, cursor = await client.get_markets(series_ticker="KXATPMATCH")
    """

    def __init__(self, session: aiohttp.ClientSession) -> None:
        self._session = session
        self._base = settings.base_url

    # ── Series ─────────────────────────────────────────────────────────────────

    async def get_series(
        self,
        tags: list[str] | None = None,
        category: str | None = None,
        include_volume: bool = False,
    ) -> list[dict]:
        """Fetch all series, optionally filtered by tags or category."""
        params: dict[str, str] = {}
        if include_volume:
            params["include_volume"] = "true"
        if category:
            params["category"] = category
        # Note: the tags query param on /series may not filter server-side;
        # we filter client-side if needed.
        url = f"{self._base}/series"
        data = await self._get(url, params=params)
        series = data.get("series") or []
        if tags and series:
            tagset = {t.lower() for t in tags}
            series = [
                s for s in series
                if any(t.lower() in tagset for t in (s.get("tags") or []))
            ]
        return series

    # ── Markets ────────────────────────────────────────────────────────────────

    async def get_markets(
        self,
        series_ticker: str | None = None,
        event_ticker: str | None = None,
        status: str = "open",
        min_updated_ts: int | None = None,
        limit: int = 500,
        cursor: str | None = None,
    ) -> tuple[list[dict], str | None]:
        """
        Fetch markets with optional filters.
        Returns (markets, next_cursor).

        min_updated_ts: Unix seconds — only return markets updated after this time.
        Used for efficient polling to detect price changes.
        """
        params: dict[str, str] = {
            "status": status,
            "limit": str(min(limit, 1000)),
        }
        if series_ticker:
            params["series_ticker"] = series_ticker
        if event_ticker:
            params["event_ticker"] = event_ticker
        if min_updated_ts is not None:
            params["min_updated_ts"] = str(min_updated_ts)
        if cursor:
            params["cursor"] = cursor

        url = f"{self._base}/markets"
        data = await self._get(url, params=params)
        markets = data.get("markets") or []
        next_cursor = data.get("cursor")
        return markets, next_cursor

    async def get_all_markets(
        self,
        series_ticker: str,
        status: str = "open",
        min_updated_ts: int | None = None,
    ) -> list[dict]:
        """Fetch all pages of markets for a series."""
        all_markets: list[dict] = []
        cursor: str | None = None

        while True:
            markets, cursor = await self.get_markets(
                series_ticker=series_ticker,
                status=status,
                min_updated_ts=min_updated_ts,
                limit=1000,
                cursor=cursor,
            )
            all_markets.extend(markets)
            if not cursor or len(markets) < 1000:
                break

        return all_markets

    # ── Single market ──────────────────────────────────────────────────────────

    async def get_market(self, ticker: str) -> dict | None:
        """Fetch a single market by ticker."""
        url = f"{self._base}/markets/{ticker}"
        data = await self._get(url)
        return data.get("market")

    # ── Event ──────────────────────────────────────────────────────────────────

    async def get_event(self, event_ticker: str) -> dict | None:
        """Fetch event details."""
        url = f"{self._base}/events/{event_ticker}"
        data = await self._get(url)
        return data.get("event")

    # ── Internal ───────────────────────────────────────────────────────────────

    async def _get(self, url: str, params: dict[str, str] | None = None) -> dict:
        timeout = aiohttp.ClientTimeout(connect=5, total=15)
        for attempt in range(3):
            try:
                async with self._session.get(url, params=params, timeout=timeout) as resp:
                    if resp.status == 429:
                        retry_after = resp.headers.get("Retry-After", "2")
                        wait = float(retry_after) if retry_after.isdigit() else 2.0
                        log.debug("429 rate limited, waiting %.1fs (attempt %d)", wait, attempt + 1)
                        await asyncio.sleep(wait)
                        continue
                    resp.raise_for_status()
                    return await resp.json()
            except asyncio.CancelledError:
                raise
            except aiohttp.ClientResponseError as e:
                if e.status == 429:
                    await asyncio.sleep(2.0 * (attempt + 1))
                    continue
                log.warning("GET %s failed: %s", url, e)
                return {}
            except Exception:
                log.warning("GET %s failed", url, exc_info=True)
                return {}
        log.warning("GET %s failed after 3 retries (429)", url)
        return {}


def make_session(max_per_host: int = 20) -> aiohttp.ClientSession:
    """Factory for the shared aiohttp session."""
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    connector = aiohttp.TCPConnector(
        limit_per_host=max_per_host,
        ttl_dns_cache=600,
        enable_cleanup_closed=True,
        force_close=False,
        ssl=ssl_context,
    )
    return aiohttp.ClientSession(
        connector=connector,
        connector_owner=True,
    )
