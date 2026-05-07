"""
api/models.py — Pydantic data contracts for Kalshi tennis odds.

Kalshi uses binary markets: each outcome is a separate YES/NO market.
A tennis match "Basilashvili vs Merida" has two markets:
  - KXATPMATCH-26MAY06BASMER-BAS  (Will Basilashvili win?)
  - KXATPMATCH-26MAY06BASMER-MER  (Will Merida win?)
We pair these by event_ticker and treat YES prices as implied probabilities.
"""
from __future__ import annotations
from pydantic import BaseModel, Field, field_validator


# ── Match metadata (cached in feed, embedded in updates) ──────────────────────

class MatchMeta(BaseModel):
    match_name: str = ""         # "Basilashvili vs Merida"
    series: str = ""             # "ATP Rome"
    tournament: str = ""         # "Internazionali BNL d'Italia"
    round: str = ""              # "Round Of 128"
    match_date: str | None = None  # ISO datetime
    is_live: bool = False
    score: str | None = None     # "6-4, 3-2" or None for pre-match
    status: str = "active"       # "active", "closed", "settled"
    players: list[str] = []      # ["Basilashvili", "Merida"]

    def update_live(self, is_live: bool, score: str | None = None) -> MatchMeta:
        return self.model_copy(update={"is_live": is_live, "score": score})


# ── Queue payload (feed → server) ─────────────────────────────────────────────

class OddsUpdate(BaseModel):
    """
    Typed payload pushed to asyncio.Queue by feed layer, consumed by server.
    For Kalshi, odds are YES prices (0.0–1.0 scale, implied probability).
    """
    source: str = "kalshi"
    match_id: str                   # event_ticker e.g. "KXATPMATCH-26MAY06BASMER"
    market: str                     # "moneyline", "game_spread", "set_betting", "total_games"
    odds: dict[str, float]          # {"Player A": 0.61, "Player B": 0.40}
    meta: MatchMeta
    ts: float

    @field_validator("odds")
    @classmethod
    def odds_must_be_valid(cls, v):
        for name, odd in v.items():
            if odd < 1.001:
                raise ValueError(f"odd {name}={odd} below minimum 1.001")
        return v


# ── Storage types ─────────────────────────────────────────────────────────────

class PricePoint(BaseModel):
    ts: float
    odd: float


# ── SSE wire types ────────────────────────────────────────────────────────────

class SSEPriceEvent(BaseModel):
    type: str = "price"
    match_id: str
    market: str
    odds: dict[str, float]
    meta: MatchMeta
    ts: float


class SSESnapshot(BaseModel):
    type: str = "snapshot"
    prices: dict    # {match_id: {market: {selection: odd}}}
    ts: float
