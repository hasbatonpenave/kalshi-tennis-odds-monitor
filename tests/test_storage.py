import time
import tempfile
import os
import pytest
from api.models import OddsUpdate, MatchMeta, PricePoint
from storage.sqlite import SQLiteRepository


@pytest.fixture
def repo():
    tmp_path = tempfile.mktemp(suffix=".db")
    r = SQLiteRepository(tmp_path)
    r.start()
    yield r
    r.stop()
    r.join(timeout=5)
    try:
        os.unlink(tmp_path)
    except OSError:
        pass


def make_update(match_id="KXATPMATCH-TEST", odds=None, market="moneyline") -> OddsUpdate:
    return OddsUpdate(
        match_id=match_id,
        market=market,
        odds=odds or {"Player A": 1.82, "Player B": 2.22},
        meta=MatchMeta(
            match_name="Player A vs Player B",
            series="ATP Test",
            is_live=True,
        ),
        ts=time.time(),
    )


def test_enqueue_and_retrieve(repo):
    repo.enqueue(make_update())
    time.sleep(3)

    history = repo.get_history("KXATPMATCH-TEST", "Player A", "moneyline", 10)
    assert len(history) == 1
    assert abs(history[0].odd - 1.82) < 0.001


def test_multiple_selections_stored(repo):
    repo.enqueue(make_update())
    time.sleep(3)

    for sel, expected_odd in [("Player A", 1.82), ("Player B", 2.22)]:
        history = repo.get_history("KXATPMATCH-TEST", sel, "moneyline", 10)
        assert len(history) == 1, f"Missing history for {sel}"
        assert abs(history[0].odd - expected_odd) < 0.001


def test_history_ordered_oldest_first(repo):
    t1 = time.time()
    time.sleep(0.01)
    t2 = time.time()

    u1 = OddsUpdate(
        match_id="m2", market="moneyline", odds={"Player A": 2.00},
        meta=MatchMeta(), ts=t1,
    )
    u2 = OddsUpdate(
        match_id="m2", market="moneyline", odds={"Player A": 1.82},
        meta=MatchMeta(), ts=t2,
    )
    repo.enqueue(u1)
    repo.enqueue(u2)
    time.sleep(3)

    history = repo.get_history("m2", "Player A", "moneyline", 10)
    assert len(history) == 2
    assert history[0].ts < history[1].ts
    assert history[0].odd == pytest.approx(2.00)
    assert history[1].odd == pytest.approx(1.82)


def test_empty_history_returns_empty_list(repo):
    result = repo.get_history("nonexistent", "Player X", "moneyline", 10)
    assert result == []


def test_limit_respected(repo):
    for i in range(20):
        u = OddsUpdate(
            match_id="m3", market="moneyline",
            odds={"Player A": 2.00 + i * 0.01},
            meta=MatchMeta(), ts=time.time() + i,
        )
        repo.enqueue(u)
    time.sleep(3)

    history = repo.get_history("m3", "Player A", "moneyline", 5)
    assert len(history) == 5


def test_different_markets(repo):
    repo.enqueue(make_update(market="moneyline", odds={"A": 1.67}))
    repo.enqueue(make_update(market="game_spread", odds={"A": 2.22}))
    time.sleep(3)

    ml_hist = repo.get_history("KXATPMATCH-TEST", "A", "moneyline", 10)
    gs_hist = repo.get_history("KXATPMATCH-TEST", "A", "game_spread", 10)
    assert len(ml_hist) == 1
    assert len(gs_hist) == 1
    assert ml_hist[0].odd == pytest.approx(1.67)
    assert gs_hist[0].odd == pytest.approx(2.22)
