# pizza-watch 🍕

A data scraper for [pizzint.watch](https://www.pizzint.watch/) — the Pentagon Pizza Index, an OSINT dashboard tracking geopolitical tension via pizza shop activity and traffic patterns around the Pentagon.

## What it does

Scrapes the pizzint.watch SSR payload (no headless browser needed — data is embedded directly in the HTML) and persists snapshots to a **DuckLake** (`pizza_lake.duckdb`).

### Tables

| Table | Description |
|---|---|
| `commute_snapshots` | One row per scrape — optempo level/label, anomaly flags, speed ratios, metro stats, time window |
| `corridor_snapshots` | One row per corridor per scrape — 7 traffic corridors around the Pentagon |
| `market_snapshots` | One row per Polymarket signal per scrape — slug, region, price, 24h volume |

## Usage

```bash
# Install dependencies
uv sync

# Single scrape (backfill / one-shot)
uv run python scraper.py --once

# Tail mode — poll every 5 minutes (default)
uv run python scraper.py --interval 300

# Custom DB path
uv run python scraper.py --once --db my_lake.duckdb
```

## Stack

- [`httpx`](https://www.python-httpx.org/) — HTTP client
- [`pydantic`](https://docs.pydantic.dev/) — data validation & modelling
- [`duckdb`](https://duckdb.org/) — DuckLake storage
- [`loguru`](https://loguru.readthedocs.io/) — structured logging
- [`uv`](https://github.com/astral-sh/uv) — package management
