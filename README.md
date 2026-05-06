# Kalshi Tennis Odds Monitor

Real-time tennis odds monitoring platform for [Kalshi](https://kalshi.com) prediction markets. Tracks ATP/WTA match winner odds, game spreads, and other tennis markets via the public Kalshi Trade API v2.

## Quick Start

```bash
cd kalshi_tennis_dashboard
pip install -r requirements.txt
python -m server.app
```

Open http://localhost:5003 in a browser, or use the dashboards:
- `frontend/dashboard.html` — Live odds table
- `frontend/chart.html` — Price history charts
- `frontend/stream.html` — Raw SSE event log

## Architecture

```
Kalshi REST API (public, no auth)
     │  GET /markets?series_ticker=X&status=open&min_updated_ts=...
     ▼
api/client.py          KalshiClient — shared aiohttp session
     │
     ▼
feed/stream.py         Per-series polling coroutine + CircuitBreaker
feed/manager.py        FeedManager — spawns streams, refreshes every 5min
     │
     ▼  asyncio.Queue[OddsUpdate]
     │
server/app.py          consume_feed() consumer
     ├── Updates in-memory prices dict
     ├── Fans out SSE events to subscribers
     └── Enqueues to SQLiteRepository (background thread)
           │
           ├── GET /stream   SSE (initial snapshot + push updates)
           ├── GET /prices   Current in-memory odds snapshot
           ├── GET /markets  Match metadata for active matches
           ├── GET /status   Feed health (streams, updates, SSE clients)
           └── GET /history  Price history from SQLite
```

## API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/stream` | GET | SSE stream with initial snapshot + real-time price updates |
| `/prices` | GET | Current in-memory odds snapshot (all matches × markets) |
| `/markets` | GET | Match metadata for all active matches |
| `/status` | GET | Feed health: streams, updates, SSE clients, match count |
| `/history` | GET | Price history from SQLite. Params: `match_id`, `selection`, `market`, `limit` |

### Example requests

```bash
# Get current odds
curl http://localhost:5003/prices | python3 -m json.tool

# Get feed status
curl http://localhost:5003/status

# Get price history for a player
curl "http://localhost:5003/history?match_id=KXATPMATCH-26MAY06BASMER&selection=Nikoloz%20Basilashvili&market=moneyline&limit=100"

# Stream live updates
curl -N http://localhost:5003/stream
```

## Configuration

All settings via environment variables with `KALSHI_` prefix:

| Variable | Default | Description |
|---|---|---|
| `KALSHI_PORT` | `5003` | Server port |
| `KALSHI_DB_PATH` | `kalshi_prices.db` | SQLite database path |
| `KALSHI_BASE_URL` | `https://external-api.kalshi.com/trade-api/v2` | Kalshi API base URL |
| `KALSHI_POLL_INTERVAL_S` | `2.5` | Seconds between polling cycles |
| `KALSHI_MAX_MATCH_AGE_H` | `48.0` | Max age of matches to track (hours) |
| `KALSHI_CB_MAX_FAILURES` | `5` | Failures before circuit breaker opens |
| `KALSHI_CB_RESET_AFTER_S` | `300.0` | Seconds before circuit breaker resets |
| `KALSHI_LOG_LEVEL` | `INFO` | Python log level |

## Kalshi API Notes

Kalshi is a CFTC-regulated prediction market. Each tennis match is organized as:

- **Series**: A template for a type of market (e.g., `KXATPMATCH` for ATP match winner)
- **Event**: A specific match (e.g., `KXATPMATCH-26MAY06BASMER` = Basilashvili vs Merida)
- **Market**: A binary (Yes/No) contract per outcome (e.g., "Will Basilashvili win?")

The public Trade API v2 requires no authentication for data reads. Since there is no public WebSocket, this dashboard simulates streaming by polling the REST API at configurable intervals (default 2.5s) using `min_updated_ts` to detect changes efficiently.

### Tracked tennis series

- `KXATPMATCH` — ATP match winner (moneyline)
- `KXWTAMATCH` — WTA match winner (moneyline)
- `KXATPGSPREAD` — ATP game spread
- Additional series discovered automatically from the API

## Running Tests

```bash
pip install pytest
python -m pytest tests/ -v
```

## Project Structure

```
kalshi_tennis_dashboard/
├── config.py              # pydantic-settings, KALSHI_ env prefix
├── requirements.txt       # Python dependencies
├── README.md
├── api/
│   ├── models.py          # Pydantic data contracts
│   └── client.py          # KalshiClient — async HTTP wrapper
├── feed/
│   ├── stream.py          # Per-series poller + CircuitBreaker
│   └── manager.py         # FeedManager — spawns/manages streams
├── storage/
│   ├── repository.py      # Abstract PriceRepository ABC
│   └── sqlite.py          # SQLite background writer thread
├── server/
│   └── app.py             # FastAPI app: SSE + REST endpoints
├── frontend/
│   ├── dashboard.html     # Live odds table
│   ├── chart.html         # Price history chart (Chart.js)
│   └── stream.html        # Raw SSE event log
└── tests/
    ├── test_circuit_breaker.py
    ├── test_models.py
    └── test_storage.py
```
