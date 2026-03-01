"""
Smoke test: fetch a live snapshot, persist it, then assert schema + data sanity.
Run with:  uv run python test_smoke.py
"""

import os
import tempfile
import traceback
from datetime import datetime

import duckdb

from scraper import fetch_snapshot, init_db, persist_snapshot

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
    print("\n── 1. Fetch snapshot ──────────────────────────────────────────")
    # -----------------------------------------------------------------------
    snap = None
    try:
        snap = fetch_snapshot()
        check("fetch returned a PizzintSnapshot", snap is not None)
    except Exception as exc:
        check("fetch returned a PizzintSnapshot", False, str(exc))
        traceback.print_exc()
        failures += 1

    if snap is None:
        print("\nCannot continue without a snapshot.")
        return failures + 1

    # -----------------------------------------------------------------------
    print("\n── 2. Snapshot schema ─────────────────────────────────────────")
    # -----------------------------------------------------------------------
    failures += 0 if check("scraped_at is a datetime", isinstance(snap.scraped_at, datetime)) else 1

    c = snap.commute
    failures += (
        0 if check("commute.timestamp is a datetime", isinstance(c.timestamp, datetime)) else 1
    )
    failures += (
        0
        if check("optempo level in 1–5", 1 <= c.optempo.level <= 5, f"got {c.optempo.level}")
        else 1
    )
    failures += 0 if check("optempo label non-empty", bool(c.optempo.label)) else 1
    failures += 0 if check("optempo description non-empty", bool(c.optempo.description)) else 1
    failures += (
        0 if check("at least 1 corridor", len(c.corridors) >= 1, f"got {len(c.corridors)}") else 1
    )

    for corridor in c.corridors:
        failures += (
            0
            if check(
                f"corridor '{corridor.id}' speed_ratio > 0",
                corridor.speedRatio > 0,
                f"got {corridor.speedRatio}",
            )
            else 1
        )

    d = snap.doomsday
    failures += (
        0 if check("doomsday.timestamp is a datetime", isinstance(d.timestamp, datetime)) else 1
    )
    failures += 0 if check("at least 1 market", len(d.markets) >= 1, f"got {len(d.markets)}") else 1

    for mkt in d.markets:
        failures += (
            0
            if check(
                f"market '{mkt.slug}' price in [0, 1]",
                0.0 <= mkt.price <= 1.0,
                f"got {mkt.price}",
            )
            else 1
        )
        failures += 0 if check(f"market '{mkt.slug}' has a region", bool(mkt.region)) else 1

    # -----------------------------------------------------------------------
    print("\n── 3. DuckLake persist ────────────────────────────────────────")
    # -----------------------------------------------------------------------
    tmp_dir = tempfile.mkdtemp()
    db_path = os.path.join(tmp_dir, "smoke_test.duckdb")

    con = init_db(db_path)
    try:
        persist_snapshot(con, snap)
        check("persist completed without exception", True)
    except Exception as exc:
        check("persist completed without exception", False, str(exc))
        traceback.print_exc()
        failures += 1

    # -----------------------------------------------------------------------
    print("\n── 4. DuckLake schema & row counts ────────────────────────────")
    # -----------------------------------------------------------------------
    tables = {
        row[0]
        for row in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
    }
    for tbl in ("commute_snapshots", "corridor_snapshots", "market_snapshots"):
        failures += 0 if check(f"table '{tbl}' exists", tbl in tables) else 1

    commute_rows = scalar_int(con, "SELECT COUNT(*) FROM commute_snapshots")
    failures += (
        0 if check("commute_snapshots has 1 row", commute_rows == 1, f"got {commute_rows}") else 1
    )

    corridor_rows = scalar_int(con, "SELECT COUNT(*) FROM corridor_snapshots")
    failures += (
        0
        if check(
            "corridor_snapshots row count matches corridors in snapshot",
            corridor_rows == len(c.corridors),
            f"db={corridor_rows} snap={len(c.corridors)}",
        )
        else 1
    )

    expected_markets = len(snap.doomsday.markets) + len(snap.doomsday.lowVolume)
    market_rows = scalar_int(con, "SELECT COUNT(*) FROM market_snapshots")
    failures += (
        0
        if check(
            "market_snapshots row count matches markets in snapshot",
            market_rows == expected_markets,
            f"db={market_rows} snap={expected_markets}",
        )
        else 1
    )

    # -----------------------------------------------------------------------
    print("\n── 5. DuckLake data sanity ────────────────────────────────────")
    # -----------------------------------------------------------------------
    sanity = row1(con, "SELECT optempo_level, optempo_label, is_anomalous FROM commute_snapshots")
    failures += (
        0
        if check(
            "optempo_level persisted correctly", sanity[0] == c.optempo.level, f"got {sanity[0]}"
        )
        else 1
    )
    failures += (
        0
        if check(
            "optempo_label persisted correctly", sanity[1] == c.optempo.label, f"got {sanity[1]}"
        )
        else 1
    )
    failures += (
        0
        if check(
            "is_anomalous persisted correctly",
            sanity[2] == c.summary.isAnomalous,
            f"got {sanity[2]}",
        )
        else 1
    )

    null_prices = scalar_int(con, "SELECT COUNT(*) FROM market_snapshots WHERE price IS NULL")
    failures += (
        0
        if check("no NULL prices in market_snapshots", null_prices == 0, f"got {null_prices}")
        else 1
    )

    null_ids = scalar_int(con, "SELECT COUNT(*) FROM corridor_snapshots WHERE corridor_id IS NULL")
    failures += 0 if check("no NULL corridor_ids", null_ids == 0, f"got {null_ids}") else 1

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
