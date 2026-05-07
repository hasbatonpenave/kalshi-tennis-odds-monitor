import pytest
import time
from api.models import (
    OddsUpdate, MatchMeta, PricePoint,
)


def make_update(**kwargs) -> OddsUpdate:
    defaults = dict(
        match_id="KXATPMATCH-26MAY06BASMER",
        market="moneyline",
        odds={"Nikoloz Basilashvili": 2.50, "Daniel Merida": 1.64},
        meta=MatchMeta(
            match_name="Basilashvili vs Merida",
            series="ATP Rome",
            tournament="Internazionali BNL d'Italia",
            round="Round Of 128",
            players=["Nikoloz Basilashvili", "Daniel Merida"],
        ),
        ts=time.time(),
    )
    return OddsUpdate(**(defaults | kwargs))


def test_odds_update_valid():
    u = make_update()
    assert u.match_id == "KXATPMATCH-26MAY06BASMER"
    assert u.odds["Nikoloz Basilashvili"] == 2.50
    assert u.meta.match_name == "Basilashvili vs Merida"
    assert u.source == "kalshi"


def test_odds_update_rejects_below_minimum():
    with pytest.raises(Exception, match="below minimum"):
        make_update(odds={"Player A": 0.5, "Player B": 1.5})


def test_odds_update_rejects_one():
    with pytest.raises(Exception, match="below minimum"):
        make_update(odds={"Player A": 1.0, "Player B": 2.0})


def test_match_meta_update_live():
    meta = MatchMeta(
        match_name="Federer vs Nadal",
        is_live=False,
    )
    updated = meta.update_live(is_live=True, score="6-4, 3-2")
    assert updated.is_live is True
    assert updated.score == "6-4, 3-2"
    assert updated.match_name == "Federer vs Nadal"


def test_match_meta_update_live_does_not_mutate_original():
    meta = MatchMeta(match_name="Test", is_live=False)
    updated = meta.update_live(is_live=True)
    assert meta.is_live is False
    assert updated.is_live is True


def test_price_point():
    p = PricePoint(ts=1234567890.0, odd=1.82)
    assert p.odd == 1.82


def test_odds_update_serializes_to_json():
    u = make_update()
    j = u.model_dump_json()
    assert '"match_id"' in j
    assert '"odds"' in j
    assert '"moneyline"' in j


def test_match_meta_defaults():
    meta = MatchMeta()
    assert meta.match_name == ""
    assert meta.players == []
    assert meta.is_live is False


def test_odds_update_empty_odds():
    """Empty odds dict should be valid (edge case for initial state)."""
    u = make_update(odds={})
    assert u.odds == {}
