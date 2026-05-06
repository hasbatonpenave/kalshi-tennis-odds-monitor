"""
feed/stream.py — per-series polling coroutine + CircuitBreaker + market parsing.

Unlike Betclic's gRPC streaming, Kalshi uses REST polling.
Each "stream" polls GET /markets?series_ticker=X&min_updated_ts=... every 2-3s,
detects price changes, and pushes OddsUpdate to the shared queue.

Kalshi market structure for tennis:
  - Each tennis match is an "event" (event_ticker)
  - Each player outcome is a separate binary market
  - We group markets by event_ticker to pair player outcomes
  - YES prices are treated as implied probabilities (0.0–1.0)
"""
from __future__ import annotations
import asyncio
import logging
import re
import time

from api.client import KalshiClient
from api.models import OddsUpdate, MatchMeta
from config import settings

log = logging.getLogger(__name__)


# ── Circuit breaker ───────────────────────────────────────────────────────────

class CircuitBreaker:
    """
    Per-stream failure tracker with exponential backoff and a long park period.

    States:
      CLOSED  → stream is healthy
      OPEN    → too many failures, park for cb_reset_after_s before retrying
    """

    def __init__(self) -> None:
        self._failures = 0
        self._open_until = 0.0

    @property
    def is_open(self) -> bool:
        if self._open_until and time.monotonic() < self._open_until:
            return True
        if self._open_until:
            self._failures = 0
            self._open_until = 0.0
        return False

    def record_success(self) -> None:
        self._failures = 0
        self._open_until = 0.0

    def next_delay(self) -> float:
        self._failures += 1
        if self._failures >= settings.cb_max_failures:
            self._open_until = time.monotonic() + settings.cb_reset_after_s
            log.warning(
                "circuit OPEN after %d failures — parking for %.0fs",
                self._failures, settings.cb_reset_after_s,
            )
            return settings.cb_reset_after_s
        delay = min(settings.reconnect_delay_s * (2 ** (self._failures - 1)), 60.0)
        return delay


# ── Market text parsing ───────────────────────────────────────────────────────

def _extract_vs_names(text: str) -> tuple[str, str] | None:
    """Extract 'PlayerA' and 'PlayerB' from text containing 'vs'."""
    # Look for "X vs Y" pattern with word boundaries near "vs"
    m = re.search(r'(\S+)\s+vs\.?\s+(\S+)', text, re.IGNORECASE)
    if m:
        # Clean trailing punctuation from names
        a = re.sub(r'[^\w\s\-]+$', '', m.group(1))
        b = re.sub(r'[^\w\s\-]+$', '', m.group(2))
        return a, b
    return None


def _parse_match_name(title: str, rules: str, yes_sub: str, no_sub: str) -> str:
    """Build 'PlayerA vs PlayerB' from market data."""
    if yes_sub and no_sub:
        return f"{yes_sub} vs {no_sub}"
    for text in (title, rules):
        names = _extract_vs_names(text)
        if names:
            return f"{names[0]} vs {names[1]}"
    return ""


def _parse_players(title: str, rules: str, yes_sub: str, no_sub: str) -> list[str]:
    """Extract player names from market fields."""
    players = []
    seen = set()
    for name in (yes_sub, no_sub):
        if name and name not in seen:
            players.append(name)
            seen.add(name)
    if not players:
        for text in (title, rules):
            names = _extract_vs_names(text)
            if names:
                for n in names:
                    if n not in seen:
                        players.append(n)
                        seen.add(n)
                break
    return players


_ROUND_RE = re.compile(
    r'(Round\s+Of\s+\d+|Final|Semi[- ]?Final|Quarter[- ]?Final|'
    r'Qualification\s+Round\s+\d+|Round\s+Robin)',
    re.IGNORECASE,
)


def _parse_round(title: str, rules: str) -> str:
    """Extract round info like 'Round Of 128' from market text."""
    for text in (title, rules):
        m = _ROUND_RE.search(text)
        if m:
            return m.group(1)
    return ""


def _parse_tournament(title: str, rules: str) -> str:
    """Extract tournament name from market text."""
    for text in (title, rules):
        m = re.search(r'(?:ATP|WTA)\s+(\S+(?:\s+\S+){0,3})', text)
        if m:
            tour = m.group(1).rstrip('.,;:')
            prefix = 'ATP' if 'ATP' in m.group(0) else 'WTA'
            return f"{prefix} {tour}"
    return ""


def _classify_market(title: str, rules: str, series_ticker: str) -> str:
    """
    Classify market type from title/rules text and series ticker.
    Returns: 'moneyline', 'game_spread', 'set_betting', 'total_games', or 'other'
    """
    text = (title + " " + rules).lower()

    if "win the" in text and "vs" in text:
        return "moneyline"
    if "spread" in text or "game spread" in text or series_ticker.endswith("GSPREAD"):
        return "game_spread"
    if "set" in text and ("score" in text or "exact" in text or "betting" in text):
        return "set_betting"
    if "total" in text and ("game" in text or "over" in text or "under" in text):
        return "total_games"
    if "game differential" in text or series_ticker.endswith("GAMEDIFF"):
        return "game_differential"

    return "other"


# ── Market grouping ───────────────────────────────────────────────────────────

def group_markets_by_event(
    markets: list[dict], series_ticker: str
) -> dict[str, dict]:
    """
    Group Kalshi markets by event_ticker, pairing player outcomes.

    Kalshi's yes_sub_title == no_sub_title (both reference the same player).
    To get both players, we collect yes_sub_title from all markets in an event.
    """
    events: dict[str, dict] = {}

    # Pass 1: collect markets per event
    for mkt in markets:
        et = mkt.get("event_ticker", "")
        if not et:
            continue
        if et not in events:
            events[et] = {
                "markets": [],
                "players_set": set(),
                "title": mkt.get("title", ""),
                "rules": mkt.get("rules_primary", ""),
                "status": mkt.get("status", "active"),
                "close_time": mkt.get("close_time", ""),
                "occurrence_datetime": mkt.get("occurrence_datetime", ""),
                "series_ticker": series_ticker,
            }
        events[et]["markets"].append(mkt)
        yes_sub = mkt.get("yes_sub_title", "")
        if yes_sub:
            events[et]["players_set"].add(yes_sub)

    # Pass 2: build match names and players from collected data
    result: dict[str, dict] = {}
    for et, info in events.items():
        players = list(info["players_set"])
        title = info["title"]
        rules = info["rules"]

        match_name = ""
        if len(players) >= 2:
            match_name = f"{players[0]} vs {players[1]}"
        elif len(players) == 1:
            # Try regex extraction from title/rules for the opponent
            names = _extract_vs_names(title) or _extract_vs_names(rules)
            if names:
                match_name = f"{names[0]} vs {names[1]}"
                if names[0] not in info["players_set"]:
                    players.append(names[0])
                if names[1] not in info["players_set"]:
                    players.append(names[1])
            else:
                match_name = players[0]

        result[et] = {
            "markets": info["markets"],
            "match_name": match_name,
            "players": players,
            "round": _parse_round(title, rules),
            "tournament": _parse_tournament(title, rules),
            "status": info["status"],
            "close_time": info["close_time"],
            "occurrence_datetime": info["occurrence_datetime"],
            "series_ticker": series_ticker,
        }

    return result


def build_odds_update(
    event_ticker: str,
    event_info: dict,
    series_ticker: str,
    prev_odds: dict[str, dict[str, float]] | None,
) -> OddsUpdate | None:
    """
    Build an OddsUpdate from an event's markets.

    For moneyline markets, extracts YES prices per player.
    Returns None if no price changes detected.
    """
    markets = event_info["markets"]
    market_type = "other"

    # Classify market type from the first market's text
    if markets:
        first = markets[0]
        title = first.get("title", "")
        rules = first.get("rules_primary", "")
        market_type = _classify_market(title, rules, series_ticker)

    # Extract odds: for binary markets, use yes_bid/yes_ask/last_price
    odds: dict[str, float] = {}
    for mkt in markets:
        player = mkt.get("yes_sub_title", "") or mkt.get("title", "")
        if not player:
            continue

        # Use last_price as the primary odds value
        last_str = mkt.get("last_price_dollars", "")
        if last_str:
            try:
                odds[player] = float(last_str)
            except (ValueError, TypeError):
                continue

    if not odds:
        return None

    # Check for changes vs previous
    if prev_odds and market_type in prev_odds:
        prev = prev_odds[market_type]
        if prev == odds:
            return None  # no change

    meta = MatchMeta(
        match_name=event_info.get("match_name", ""),
        series=event_info.get("tournament", ""),
        tournament=event_info.get("tournament", ""),
        round=event_info.get("round", ""),
        match_date=event_info.get("occurrence_datetime") or event_info.get("close_time", ""),
        is_live=False,
        status=event_info.get("status", "active"),
        players=event_info.get("players", []),
    )

    return OddsUpdate(
        source="kalshi",
        match_id=event_ticker,
        market=market_type,
        odds=odds,
        meta=meta,
        ts=time.time(),
    )


# ── Series polling coroutine ──────────────────────────────────────────────────

async def run_series_stream(
    client: KalshiClient,
    series_ticker: str,
    meta_cache: dict[str, MatchMeta],
    prev_odds: dict[str, dict[str, dict[str, float]]],
    queue: asyncio.Queue[OddsUpdate],
    stop_ev: asyncio.Event,
) -> None:
    """
    Long-lived coroutine that polls a Kalshi series for price changes.

    Polls GET /markets?series_ticker=X&status=open every poll_interval_s seconds.
    Groups markets by event, detects changes client-side, and pushes
    OddsUpdate to the shared queue.

    Note: Kalshi's min_updated_ts is incompatible with series_ticker/status
    filters, so we fetch the full market list each poll and compare.
    """
    breaker = CircuitBreaker()

    while not stop_ev.is_set():
        if breaker.is_open:
            try:
                await asyncio.wait_for(stop_ev.wait(), timeout=settings.cb_reset_after_s)
                return
            except asyncio.TimeoutError:
                pass
            continue

        try:
            markets = await client.get_all_markets(
                series_ticker=series_ticker,
                status="open",
            )

            if markets:
                events = group_markets_by_event(markets, series_ticker)

                for event_ticker, event_info in events.items():
                    if stop_ev.is_set():
                        return

                    meta_cache[event_ticker] = MatchMeta(
                        match_name=event_info.get("match_name", ""),
                        series=event_info.get("tournament", ""),
                        tournament=event_info.get("tournament", ""),
                        round=event_info.get("round", ""),
                        match_date=event_info.get("occurrence_datetime")
                        or event_info.get("close_time", ""),
                        is_live=False,
                        status=event_info.get("status", "active"),
                        players=event_info.get("players", []),
                    )

                    update = build_odds_update(
                        event_ticker, event_info, series_ticker,
                        prev_odds.get(event_ticker),
                    )
                    if update is None:
                        continue

                    prev_odds.setdefault(event_ticker, {})[update.market] = update.odds

                    try:
                        queue.put_nowait(update)
                    except asyncio.QueueFull:
                        try:
                            queue.get_nowait()
                            queue.put_nowait(update)
                        except (asyncio.QueueEmpty, asyncio.QueueFull):
                            pass

            breaker.record_success()

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            if stop_ev.is_set():
                return
            delay = breaker.next_delay()
            log.warning(
                "series stream %s error (%s): %s — retry in %.1fs",
                series_ticker, type(exc).__name__, exc, delay,
            )
            try:
                await asyncio.wait_for(stop_ev.wait(), timeout=delay)
                return
            except asyncio.TimeoutError:
                pass
            continue

        # Wait for next poll interval
        try:
            await asyncio.wait_for(stop_ev.wait(), timeout=settings.poll_interval_s)
            return
        except asyncio.TimeoutError:
            pass


def _parse_updated_ts(updated_time: str) -> int:
    """Parse Kalshi ISO datetime string to Unix timestamp (seconds)."""
    if not updated_time:
        return 0
    try:
        # Handle "2026-05-05T18:16:00.304264Z" format
        from datetime import datetime, timezone
        s = updated_time.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return int(dt.timestamp())
    except (ValueError, TypeError):
        return 0


# ── Initial snapshot (first poll for a series) ────────────────────────────────

async def fetch_initial_snapshot(
    client: KalshiClient,
    series_ticker: str,
) -> list[dict]:
    """Fetch all open markets for a series (used on startup)."""
    return await client.get_all_markets(
        series_ticker=series_ticker,
        status="open",
    )
