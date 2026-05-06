"""
feed/manager.py — spawns and manages one polling coroutine per tennis series.

Discovers tennis series on Kalshi, takes initial snapshots, spawns per-series
polling tasks, and refreshes the series list every 5 minutes.

On Kalshi, tennis markets are organized as series (templates) with events
(individual matches). We poll per series for efficiency since the /markets
endpoint supports series_ticker + min_updated_ts filtering.
"""
from __future__ import annotations
import asyncio
import logging
import time

from api.client import KalshiClient, make_session
from api.models import OddsUpdate, MatchMeta
from config import settings
from feed.stream import (
    run_series_stream, group_markets_by_event, build_odds_update,
    fetch_initial_snapshot,
)

log = logging.getLogger(__name__)

# Series tickers we always want to track (discovered from API research)
_DEFAULT_TENNIS_SERIES = [
    "KXATPMATCH",
    "KXWTAMATCH",
    "KXATPGSPREAD",
]


class FeedManager:
    """
    Public interface consumed by server/app.py:

        manager = FeedManager(queue)
        await manager.run()          # long-running coroutine
        manager.stop()               # signal graceful shutdown
        manager.get_stats()          # -> dict
        manager.get_meta(mid)        # -> MatchMeta
        manager.get_all_meta()       # -> dict[str, MatchMeta]
    """

    def __init__(self, queue: asyncio.Queue[OddsUpdate]) -> None:
        self._queue = queue
        self._stop_ev = asyncio.Event()
        self._meta: dict[str, MatchMeta] = {}
        self._prev_odds: dict[str, dict[str, dict[str, float]]] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._stats = {
            "streams": 0, "updates": 0, "matches": 0, "series": 0,
            "last_update": None,
        }

        # Track update count
        self._update_count = 0

    # ── Public API ─────────────────────────────────────────────────────────

    def stop(self) -> None:
        self._stop_ev.set()

    def get_stats(self) -> dict:
        return {
            **self._stats,
            "updates": self._update_count,
            "active_tasks": sum(1 for t in self._tasks.values() if not t.done()),
        }

    def get_meta(self, match_id: str) -> MatchMeta:
        return self._meta.get(match_id, MatchMeta())

    def get_all_meta(self) -> dict[str, MatchMeta]:
        return dict(self._meta)

    # ── Series discovery ───────────────────────────────────────────────────

    async def _discover_tennis_series(self, client: KalshiClient) -> list[str]:
        """
        Discover active tennis series from Kalshi API.
        Falls back to hardcoded defaults if the API doesn't return results.
        """
        tickers = list(_DEFAULT_TENNIS_SERIES)
        try:
            all_series = await client.get_series(
                tags=["tennis"],
                include_volume=True,
            )
            if all_series:
                # Filter to active series with volume, prefer match-related
                active = [
                    s for s in all_series
                    if s.get("ticker") and float(s.get("volume_fp", "0")) > 0
                ]
                # Sort by volume
                active.sort(
                    key=lambda s: float(s.get("volume_fp", "0")),
                    reverse=True,
                )
                discovered = [s["ticker"] for s in active]
                # Merge with defaults (discovered first, then defaults not yet seen)
                seen = set(discovered)
                for t in tickers:
                    if t not in seen:
                        discovered.append(t)
                tickers = discovered
                log.info(
                    "discovered %d active tennis series (top: %s)",
                    len(active),
                    ", ".join(tickers[:8]),
                )
        except Exception as exc:
            log.warning("series discovery failed, using defaults: %s", exc)

        return tickers

    # ── Main coroutine ─────────────────────────────────────────────────────

    async def run(self, refresh_min: float = 5.0) -> None:
        async with make_session(settings.max_streams_per_host) as session:
            client = KalshiClient(session)

            while not self._stop_ev.is_set():
                # 1. Discover tennis series
                all_series = await self._discover_tennis_series(client)
                self._stats["series"] = len(all_series)

                # Limit streams to top series to avoid overwhelming Kalshi rate limits.
                # 77 concurrent polling streams triggers 429 errors.
                max_series = 8
                tracked_series = all_series[:max_series]
                if len(all_series) > max_series:
                    log.info(
                        "limiting to top %d/%d tennis series (rate-limit safety)",
                        max_series, len(all_series),
                    )

                new_series = [
                    s for s in tracked_series
                    if s not in self._tasks or self._tasks[s].done()
                ]
                snapshot_series = new_series

                # 2. Take initial snapshots with rate-limit-friendly spacing
                for i, st in enumerate(snapshot_series):
                    if i > 0:
                        await asyncio.sleep(0.5)  # avoid 429

                    try:
                        markets = await fetch_initial_snapshot(client, st)
                        if markets:
                            events = group_markets_by_event(markets, st)
                            self._stats["matches"] += len(events)
                            log.info(
                                "series %s: %d markets across %d events",
                                st, len(markets), len(events),
                            )

                            for event_ticker, event_info in events.items():
                                update = build_odds_update(
                                    event_ticker, event_info, st,
                                    self._prev_odds.get(event_ticker),
                                )
                                if update is None:
                                    continue
                                self._prev_odds.setdefault(event_ticker, {})[
                                    update.market
                                ] = update.odds
                                self._meta[event_ticker] = update.meta
                                try:
                                    self._queue.put_nowait(update)
                                    self._update_count += 1
                                except asyncio.QueueFull:
                                    pass
                    except Exception as exc:
                        log.error(
                            "initial snapshot failed for %s: %s", st, exc
                        )

                # 3. Spawn polling tasks for tracked series, prune untracked
                for st in list(self._tasks.keys()):
                    if st not in tracked_series:
                        self._tasks[st].cancel()
                        del self._tasks[st]
                        self._stats["streams"] -= 1

                spawned = 0
                for st in tracked_series:
                    if st in self._tasks and not self._tasks[st].done():
                        continue
                    t = asyncio.create_task(
                        run_series_stream(
                            client, st, self._meta, self._prev_odds,
                            self._queue, self._stop_ev,
                        ),
                        name=f"kalshi-{st}",
                    )
                    self._tasks[st] = t
                    self._stats["streams"] += 1
                    spawned += 1

                # 4. Prune dead tasks
                dead = [k for k, t in self._tasks.items() if t.done()]
                for k in dead:
                    self._stats["streams"] -= 1
                    del self._tasks[k]

                active = len(self._tasks)
                self._stats["last_update"] = time.time()
                log.info(
                    "%d active series streams (%d new, %d pruned, %d matches)",
                    active, spawned, len(dead), self._stats["matches"],
                )

                # 5. Wait for next refresh or stop signal
                try:
                    await asyncio.wait_for(
                        self._stop_ev.wait(), timeout=refresh_min * 60
                    )
                    break
                except asyncio.TimeoutError:
                    pass

            # Graceful shutdown
            log.info("shutting down %d tasks…", len(self._tasks))
            for t in self._tasks.values():
                t.cancel()
            if self._tasks:
                await asyncio.gather(
                    *self._tasks.values(), return_exceptions=True
                )
            log.info("feed shutdown complete")
