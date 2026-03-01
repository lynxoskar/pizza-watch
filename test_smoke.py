"""
Smoke test: fetch live snapshots from all endpoints, persist to a temp DuckLake,
then assert schema + data sanity for every table.
Run with:  uv run python test_smoke.py
"""

import os
import tempfile
import traceback
from datetime import datetime

import duckdb

from scraper import (
    _log_scrape,
    fetch_dashboard,
    fetch_osint_feed,
    fetch_snapshot,
    init_db,
    persist_dashboard,
    persist_osint_feed,
    persist_snapshot,
)

PASS = "\033[32m PASS\033[0m"
FAIL = "\033[31m FAIL\033[0m"


def check(label: str, expr: bool, detail: str = "") -> bool:
    status = PASS if expr else FAIL
    print(f"  [{status}] {label}" + (f" — {detail}" if detail else ""))
    return expr


def scalar_int(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    """Execute a COUNT-style scalar query and return the integer result."""
    row = con.execute(sql).fetchone()
    assert row is not None, f"Expected a row from: {sql}"
    return int(row[0])


def row1(con: duckdb.DuckDBPyConnection, sql: str) -> tuple[object, ...]:
    """Execute a query and return the first row, asserting it exists."""
    row = con.execute(sql).fetchone()
    assert row is not None, f"Expected a row from: {sql}"
    return row


def run_smoke_test() -> int:
    failures = 0

    # -----------------------------------------------------------------------
    print("\n── 1. Fetch all endpoints ─────────────────────────────────────")
    # -----------------------------------------------------------------------
    snap = None
    dashboard = None
    osint = None

    try:
        snap = fetch_snapshot()
        check("SSR snapshot fetched", snap is not None)
    except Exception as exc:
        check("SSR snapshot fetched", False, str(exc))
        traceback.print_exc()
        failures += 1

    try:
        dashboard = fetch_dashboard()
        check("dashboard-data fetched", dashboard is not None)
    except Exception as exc:
        check("dashboard-data fetched", False, str(exc))
        traceback.print_exc()
        failures += 1

    try:
        osint = fetch_osint_feed()
        check("osint-feed fetched", osint is not None)
    except Exception as exc:
        check("osint-feed fetched", False, str(exc))
        traceback.print_exc()
        failures += 1

    if snap is None or dashboard is None or osint is None:
        print("\nCannot continue — one or more fetches failed.")
        return failures + 1

    # -----------------------------------------------------------------------
    print("\n── 2. Snapshot schema ─────────────────────────────────────────")
    # -----------------------------------------------------------------------
    failures += 0 if check("scraped_at is datetime", isinstance(snap.scraped_at, datetime)) else 1
    failures += (
        0
        if check("commute.timestamp is datetime", isinstance(snap.commute.timestamp, datetime))
        else 1
    )
    failures += (
        0
        if check(
            "optempo level in 1–5",
            1 <= snap.commute.optempo.level <= 5,
            f"got {snap.commute.optempo.level}",
        )
        else 1
    )
    failures += 0 if check("optempo label non-empty", bool(snap.commute.optempo.label)) else 1
    failures += (
        0
        if check(
            "at least 1 corridor",
            len(snap.commute.corridors) >= 1,
            f"got {len(snap.commute.corridors)}",
        )
        else 1
    )
    for corridor in snap.commute.corridors:
        failures += (
            0
            if check(
                f"corridor '{corridor.id}' speed_ratio > 0",
                corridor.speedRatio > 0,
                f"got {corridor.speedRatio}",
            )
            else 1
        )
    failures += 0 if check("at least 1 market", len(snap.doomsday.markets) >= 1) else 1
    for mkt in snap.doomsday.markets:
        failures += (
            0
            if check(
                f"market '{mkt.slug}' price in [0,1]",
                0.0 <= mkt.price <= 1.0,
                f"got {mkt.price}",
            )
            else 1
        )

    # -----------------------------------------------------------------------
    print("\n── 3. Dashboard schema ────────────────────────────────────────")
    # -----------------------------------------------------------------------
    failures += 0 if check("dashboard.success is True", dashboard.success) else 1
    failures += (
        0
        if check("at least 1 place", len(dashboard.data) >= 1, f"got {len(dashboard.data)}")
        else 1
    )
    for place in dashboard.data:
        failures += 0 if check(f"place '{place.name}' has place_id", bool(place.place_id)) else 1
        failures += (
            0
            if check(
                f"place '{place.name}' has sparkline",
                len(place.sparkline_24h) >= 1,
                f"got {len(place.sparkline_24h)}",
            )
            else 1
        )

    # -----------------------------------------------------------------------
    print("\n── 4. OSINT feed schema ───────────────────────────────────────")
    # -----------------------------------------------------------------------
    failures += 0 if check("osint.success is True", osint.success) else 1
    failures += (
        0 if check("at least 1 tweet", len(osint.tweets) >= 1, f"got {len(osint.tweets)}") else 1
    )
    for tweet in osint.tweets[:5]:  # spot-check first 5
        failures += 0 if check(f"tweet '{tweet.id}' has text", bool(tweet.text)) else 1
        failures += 0 if check(f"tweet '{tweet.id}' has handle", bool(tweet.handle)) else 1

    # -----------------------------------------------------------------------
    print("\n── 5. DuckLake persist ────────────────────────────────────────")
    # -----------------------------------------------------------------------
    tmp_dir = tempfile.mkdtemp()
    db_path = os.path.join(tmp_dir, "smoke_test.duckdb")
    con = init_db(db_path)

    try:
        persist_snapshot(con, snap)
        _log_scrape(con, "SSR", True, 200, 0)
        check("persist_snapshot completed", True)
    except Exception as exc:
        check("persist_snapshot completed", False, str(exc))
        traceback.print_exc()
        failures += 1

    try:
        persist_dashboard(con, dashboard)
        _log_scrape(con, "dashboard-data", True, 200, 0)
        check("persist_dashboard completed", True)
    except Exception as exc:
        check("persist_dashboard completed", False, str(exc))
        traceback.print_exc()
        failures += 1

    try:
        persist_osint_feed(con, osint)
        _log_scrape(con, "osint-feed", True, 200, 0)
        check("persist_osint_feed completed", True)
    except Exception as exc:
        check("persist_osint_feed completed", False, str(exc))
        traceback.print_exc()
        failures += 1

    # -----------------------------------------------------------------------
    print("\n── 6. DuckLake schema & row counts ────────────────────────────")
    # -----------------------------------------------------------------------
    tables = {
        row[0]
        for row in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
    }
    for tbl in (
        "commute_snapshots",
        "corridor_snapshots",
        "market_snapshots",
        "place_popularity",
        "osint_tweets",
        "scrape_log",
    ):
        failures += 0 if check(f"table '{tbl}' exists", tbl in tables) else 1

    commute_rows = scalar_int(con, "SELECT COUNT(*) FROM commute_snapshots")
    failures += (
        0 if check("commute_snapshots has 1 row", commute_rows == 1, f"got {commute_rows}") else 1
    )

    corridor_rows = scalar_int(con, "SELECT COUNT(*) FROM corridor_snapshots")
    failures += (
        0
        if check(
            "corridor_snapshots matches corridors in snapshot",
            corridor_rows == len(snap.commute.corridors),
            f"db={corridor_rows} snap={len(snap.commute.corridors)}",
        )
        else 1
    )

    expected_markets = len(snap.doomsday.markets) + len(snap.doomsday.lowVolume)
    market_rows = scalar_int(con, "SELECT COUNT(*) FROM market_snapshots")
    failures += (
        0
        if check(
            "market_snapshots matches markets in snapshot",
            market_rows == expected_markets,
            f"db={market_rows} snap={expected_markets}",
        )
        else 1
    )

    expected_ticks = sum(len(p.sparkline_24h) for p in dashboard.data)
    tick_rows = scalar_int(con, "SELECT COUNT(*) FROM place_popularity")
    failures += (
        0
        if check(
            "place_popularity row count matches sparkline ticks",
            tick_rows == expected_ticks,
            f"db={tick_rows} expected={expected_ticks}",
        )
        else 1
    )

    tweet_rows = scalar_int(con, "SELECT COUNT(*) FROM osint_tweets")
    failures += (
        0
        if check(
            "osint_tweets row count matches feed",
            tweet_rows == len(osint.tweets),
            f"db={tweet_rows} feed={len(osint.tweets)}",
        )
        else 1
    )

    log_rows = scalar_int(con, "SELECT COUNT(*) FROM scrape_log")
    failures += 0 if check("scrape_log has rows", log_rows >= 3, f"got {log_rows}") else 1

    # -----------------------------------------------------------------------
    print("\n── 7. DuckLake data sanity ────────────────────────────────────")
    # -----------------------------------------------------------------------
    sanity = row1(con, "SELECT optempo_level, optempo_label, is_anomalous FROM commute_snapshots")
    failures += (
        0
        if check(
            "optempo_level round-tripped",
            sanity[0] == snap.commute.optempo.level,
            f"got {sanity[0]}",
        )
        else 1
    )
    failures += (
        0
        if check(
            "optempo_label round-tripped",
            sanity[1] == snap.commute.optempo.label,
            f"got {sanity[1]}",
        )
        else 1
    )

    failures += (
        0
        if check(
            "no NULL prices in market_snapshots",
            scalar_int(con, "SELECT COUNT(*) FROM market_snapshots WHERE price IS NULL") == 0,
        )
        else 1
    )
    failures += (
        0
        if check(
            "no NULL corridor_ids",
            scalar_int(con, "SELECT COUNT(*) FROM corridor_snapshots WHERE corridor_id IS NULL")
            == 0,
        )
        else 1
    )
    failures += (
        0
        if check(
            "no NULL place_ids in place_popularity",
            scalar_int(con, "SELECT COUNT(*) FROM place_popularity WHERE place_id IS NULL") == 0,
        )
        else 1
    )
    failures += (
        0
        if check(
            "no NULL tweet ids",
            scalar_int(con, "SELECT COUNT(*) FROM osint_tweets WHERE id IS NULL") == 0,
        )
        else 1
    )

    # -----------------------------------------------------------------------
    print("\n── 8. Dedup idempotency ───────────────────────────────────────")
    # -----------------------------------------------------------------------
    persist_dashboard(con, dashboard)
    tick_rows_after = scalar_int(con, "SELECT COUNT(*) FROM place_popularity")
    failures += (
        0
        if check(
            "place_popularity: re-persist adds no duplicates",
            tick_rows_after == tick_rows,
            f"before={tick_rows} after={tick_rows_after}",
        )
        else 1
    )

    persist_osint_feed(con, osint)
    tweet_rows_after = scalar_int(con, "SELECT COUNT(*) FROM osint_tweets")
    failures += (
        0
        if check(
            "osint_tweets: re-persist adds no duplicates",
            tweet_rows_after == tweet_rows,
            f"before={tweet_rows} after={tweet_rows_after}",
        )
        else 1
    )

    con.close()

    # -----------------------------------------------------------------------
    print("\n── Result ─────────────────────────────────────────────────────")
    if failures == 0:
        print("  \033[32mAll checks passed ✓\033[0m\n")
    else:
        print(f"  \033[31m{failures} check(s) failed ✗\033[0m\n")

    return failures


if __name__ == "__main__":
    import sys

    sys.exit(run_smoke_test())
