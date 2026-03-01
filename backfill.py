"""
pizzint.watch — Phase 1 historical backfill
--------------------------------------------
One-shot script that recovers all historical data currently exposed by the API:

  1. /api/dashboard-data  → sparkline_24h per place (~24 hourly ticks × 9 places)
  2. /api/polymarket/timeseries?token_id=<id>  → ~7–9 days of price history per market
     (one request per token_id found in the SSR market list)

Both endpoints are Cloudflare-cached (max-age=14400), so requests are cheap.
Dedup is enforced by ON CONFLICT DO NOTHING — safe to re-run.

Usage
-----
  uv run python backfill.py                      # default DB: pizza_lake.duckdb
  uv run python backfill.py --db my_lake.duckdb
"""

import argparse
import time
from datetime import UTC, datetime

import duckdb
import httpx
from loguru import logger
from pydantic import BaseModel

from scraper import (
    _count,
    _fetch_with_retry,
    _log_scrape,
    fetch_dashboard,
    fetch_snapshot,
    init_db,
    persist_dashboard,
    persist_snapshot,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

URL_TIMESERIES = "https://www.pizzint.watch/api/polymarket/timeseries"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# Polite delay between timeseries requests — well within Cloudflare cache TTL
_TIMESERIES_DELAY_S = 1.0

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class TimeseriesPoint(BaseModel):
    t: int  # unix timestamp seconds
    p: float  # price [0, 1]


class TimeseriesResponse(BaseModel):
    history: list[TimeseriesPoint]


# ---------------------------------------------------------------------------
# DDL additions for timeseries table
# ---------------------------------------------------------------------------

TIMESERIES_DDL = """
CREATE TABLE IF NOT EXISTS market_timeseries (
    token_id    VARCHAR NOT NULL,
    slug        VARCHAR,
    ts          TIMESTAMPTZ NOT NULL,
    price       DOUBLE NOT NULL,
    PRIMARY KEY (token_id, ts)
);
"""


def _ensure_timeseries_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(TIMESERIES_DDL)
    con.commit()


# ---------------------------------------------------------------------------
# Fetch + persist timeseries for one market
# ---------------------------------------------------------------------------


def _backfill_timeseries(
    con: duckdb.DuckDBPyConnection,
    token_id: str,
    slug: str,
    client: httpx.Client,
) -> int:
    url = f"{URL_TIMESERIES}?token_id={token_id}"
    t0 = time.monotonic()
    try:
        resp = _fetch_with_retry(url, client)
        data = TimeseriesResponse.model_validate(resp.json())
        _log_scrape(con, url, True, resp.status_code, int((time.monotonic() - t0) * 1000))
    except Exception as exc:
        _log_scrape(con, url, False, None, int((time.monotonic() - t0) * 1000), str(exc))
        logger.error("[PIZZINT] Timeseries fetch failed for {} ({}): {}", slug, token_id, exc)
        return 0

    before = _count(con, "market_timeseries")
    for point in data.history:
        ts = datetime.fromtimestamp(point.t, tz=UTC)
        con.execute(
            """
            INSERT INTO market_timeseries (token_id, slug, ts, price)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (token_id, ts) DO NOTHING
            """,
            [token_id, slug, ts, point.p],
        )
    con.commit()
    inserted = _count(con, "market_timeseries") - before
    logger.info(
        "[PIZZINT]   {} — {} points fetched, {} new inserted",
        slug,
        len(data.history),
        inserted,
    )
    return inserted


# ---------------------------------------------------------------------------
# Main backfill routine
# ---------------------------------------------------------------------------


def run_backfill(db_path: str) -> None:
    con = init_db(db_path)
    _ensure_timeseries_table(con)

    # ------------------------------------------------------------------
    # Step 1: dashboard sparklines  (~24h × 9 places)
    # ------------------------------------------------------------------
    logger.info("[PIZZINT] Backfill step 1/2 — dashboard sparklines")
    t0 = time.monotonic()
    try:
        dashboard = fetch_dashboard()
        inserted = persist_dashboard(con, dashboard)
        _log_scrape(con, "dashboard-data", True, 200, int((time.monotonic() - t0) * 1000))
        logger.success("[PIZZINT] Step 1 complete — {} sparkline ticks inserted", inserted)
    except Exception as exc:
        duration_ms = int((time.monotonic() - t0) * 1000)
        _log_scrape(con, "dashboard-data", False, None, duration_ms, str(exc))
        logger.error("[PIZZINT] Step 1 failed: {}", exc)

    # ------------------------------------------------------------------
    # Step 2: polymarket timeseries per token_id  (~7–9 days × 39 markets)
    # ------------------------------------------------------------------
    logger.info("[PIZZINT] Backfill step 2/2 — polymarket timeseries (SSR market list)")
    t0 = time.monotonic()
    try:
        snap = fetch_snapshot()
        persist_snapshot(con, snap)
        _log_scrape(con, "SSR", True, 200, int((time.monotonic() - t0) * 1000))
    except Exception as exc:
        _log_scrape(con, "SSR", False, None, int((time.monotonic() - t0) * 1000), str(exc))
        logger.error("[PIZZINT] SSR fetch for market list failed: {}", exc)
        con.close()
        return

    all_markets = snap.doomsday.markets + snap.doomsday.lowVolume
    logger.info("[PIZZINT] {} markets to fetch timeseries for", len(all_markets))

    total_inserted = 0
    with httpx.Client(headers=HEADERS, timeout=30, follow_redirects=True) as client:
        for i, mkt in enumerate(all_markets, 1):
            logger.info("[PIZZINT] ({}/{}) {}", i, len(all_markets), mkt.slug)
            total_inserted += _backfill_timeseries(con, mkt.tokenId, mkt.slug, client)
            if i < len(all_markets):
                time.sleep(_TIMESERIES_DELAY_S)  # polite delay between requests

    logger.success(
        "[PIZZINT] Step 2 complete — {} total timeseries points inserted across {} markets",
        total_inserted,
        len(all_markets),
    )

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    place_rows = _count(con, "place_popularity")
    ts_rows = _count(con, "market_timeseries")
    logger.success(
        "[PIZZINT] Backfill done — place_popularity={} rows | market_timeseries={} rows",
        place_rows,
        ts_rows,
    )
    con.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="pizzint.watch Phase 1 historical backfill")
    parser.add_argument("--db", default="pizza_lake.duckdb", help="DuckDB file path")
    args = parser.parse_args()
    run_backfill(args.db)


if __name__ == "__main__":
    main()
