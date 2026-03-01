"""
pizzint.watch scraper
---------------------
Fetches the Pentagon Pizza Index data from https://pizzint.watch by parsing
the Next.js SSR payload embedded in the HTML (no headless browser needed).

Outputs to a DuckLake: a DuckDB file (`pizza_lake.duckdb`) with two tables:
  - commute_snapshots   : per-scrape traffic / optempo summary
  - market_snapshots    : per-scrape polymarket signals

Usage
-----
  # Single scrape (backfill / one-shot):
  python scraper.py --once

  # Tail mode (continuous polling):
  python scraper.py --interval 300      # poll every 5 minutes (default)

  # Backfill by repeating a single historic fetch N times (demo):
  python scraper.py --once --db pizza_lake.duckdb
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from typing import List, Optional

import duckdb
import httpx
from loguru import logger
from pydantic import BaseModel, Field, model_validator


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class TimeWindow(BaseModel):
    label: str
    hourET: int
    window: str
    isWeekend: bool
    expectedRatio: float
    sensitivityMultiplier: float


class OptempSummary(BaseModel):
    corridorsTotal: int
    dominantSignal: str
    crossCorrelation: float
    tier1AvgDeviation: float
    tier2AvgDeviation: float
    corridorsReporting: int


class Optempo(BaseModel):
    color: str
    label: str
    level: int
    value: float
    summary: OptempSummary
    timeWindow: TimeWindow
    description: str
    rawDeviation: float


class CorridorScore(BaseModel):
    id: str
    tier: int
    deviation: float
    speedRatio: float
    contribution: float
    anomalySignal: float
    expectedRatio: float


class Corridor(BaseModel):
    id: str
    name: str
    tier: int
    color: str
    status: str
    direction: str
    shortName: str
    confidence: float
    speedRatio: float
    roadClosure: bool
    currentSpeed: float
    freeFlowSpeed: float
    optempoWeight: float
    currentTravelTime: float
    freeFlowTravelTime: float


class CommuteSummary(BaseModel):
    isAnomalous: bool
    anomalyLevel: str
    avgSpeedRatio: float
    corridorsTotal: int
    anomalyDescription: str
    corridorsReporting: int
    weightedSpeedRatio: float


class Metro(BaseModel):
    current: float
    baseline: float
    popularityRatio: float


class CommuteData(BaseModel):
    metro: Metro
    optempo: Optempo
    success: bool
    summary: CommuteSummary
    timestamp: datetime
    corridors: List[Corridor]


class Market(BaseModel):
    slug: str
    label: str
    region: str
    price: float
    tokenId: str
    seriesId: Optional[str] = None
    eventSlug: Optional[str] = None
    image: Optional[str] = None
    volume: Optional[float] = None
    volume_24h: Optional[float] = None
    endDate: Optional[float] = None


class DoomsdayData(BaseModel):
    markets: List[Market]
    lowVolume: List[Market]
    timestamp: datetime


class PizzintSnapshot(BaseModel):
    scraped_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    commute: CommuteData
    doomsday: DoomsdayData


# ---------------------------------------------------------------------------
# Scraping logic
# ---------------------------------------------------------------------------

URL = "https://www.pizzint.watch/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


def _extract_payload(html: str) -> dict:
    """
    Parse the Next.js __next_f SSR chunks to extract the initialDoomsdayData
    and initialCommuteData objects embedded in the page HTML.
    """
    idx = html.find("initialDoomsdayData")
    if idx < 0:
        raise ValueError("Could not find initialDoomsdayData in HTML")

    # Walk back to the enclosing self.__next_f.push call
    start = html.rfind("self.__next_f.push", 0, idx)
    end = html.find("</script>", idx)
    chunk = html[start:end]

    # The argument is a JSON-encoded string (double-escaped)
    m = re.search(r'self\.__next_f\.push\(\[1,"(.*)"\]\)', chunk, re.DOTALL)
    if not m:
        raise ValueError("Could not parse __next_f.push chunk")

    raw = m.group(1).encode().decode("unicode_escape")

    # Locate the top-level object
    payload_start = raw.find('{"initialDoomsdayData"')
    if payload_start < 0:
        raise ValueError("Could not locate top-level payload object")

    # Walk to the matching closing brace
    payload_chars = list(raw[payload_start:])
    depth = 0
    for i, c in enumerate(payload_chars):
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                raw_json = "".join(payload_chars[: i + 1])
                break
    else:
        raise ValueError("Unbalanced braces in payload")

    return json.loads(raw_json)


def fetch_snapshot() -> PizzintSnapshot:
    logger.info("Fetching {}", URL)
    with httpx.Client(headers=HEADERS, timeout=30, follow_redirects=True) as client:
        resp = client.get(URL)
        resp.raise_for_status()

    payload = _extract_payload(resp.text)

    commute = CommuteData.model_validate(payload["initialCommuteData"])
    doomsday = DoomsdayData.model_validate(payload["initialDoomsdayData"])

    return PizzintSnapshot(commute=commute, doomsday=doomsday)


# ---------------------------------------------------------------------------
# DuckLake persistence
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS commute_snapshots (
    scraped_at          TIMESTAMPTZ NOT NULL,
    data_timestamp      TIMESTAMPTZ NOT NULL,
    optempo_level       INTEGER,
    optempo_label       VARCHAR,
    optempo_description VARCHAR,
    is_anomalous        BOOLEAN,
    anomaly_level       VARCHAR,
    avg_speed_ratio     DOUBLE,
    weighted_speed_ratio DOUBLE,
    corridors_total     INTEGER,
    corridors_reporting INTEGER,
    cross_correlation   DOUBLE,
    tier1_avg_deviation DOUBLE,
    tier2_avg_deviation DOUBLE,
    dominant_signal     VARCHAR,
    time_window_label   VARCHAR,
    time_window         VARCHAR,
    hour_et             INTEGER,
    is_weekend          BOOLEAN,
    metro_current       DOUBLE,
    metro_baseline      DOUBLE,
    metro_popularity_ratio DOUBLE,
    raw_deviation       DOUBLE
);

CREATE TABLE IF NOT EXISTS corridor_snapshots (
    scraped_at          TIMESTAMPTZ NOT NULL,
    data_timestamp      TIMESTAMPTZ NOT NULL,
    corridor_id         VARCHAR NOT NULL,
    name                VARCHAR,
    tier                INTEGER,
    status              VARCHAR,
    direction           VARCHAR,
    short_name          VARCHAR,
    confidence          DOUBLE,
    speed_ratio         DOUBLE,
    current_speed       DOUBLE,
    free_flow_speed     DOUBLE,
    optempo_weight      DOUBLE,
    current_travel_time DOUBLE,
    free_flow_travel_time DOUBLE,
    road_closure        BOOLEAN
);

CREATE TABLE IF NOT EXISTS market_snapshots (
    scraped_at  TIMESTAMPTZ NOT NULL,
    data_timestamp TIMESTAMPTZ NOT NULL,
    slug        VARCHAR NOT NULL,
    label       VARCHAR,
    region      VARCHAR,
    price       DOUBLE,
    volume      DOUBLE,
    volume_24h  DOUBLE,
    end_date    TIMESTAMPTZ,
    is_low_volume BOOLEAN
);
"""


def init_db(path: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(path)
    con.execute(DDL)
    con.commit()
    logger.info("DuckLake initialised at {}", path)
    return con


def persist_snapshot(con: duckdb.DuckDBPyConnection, snap: PizzintSnapshot) -> None:
    scraped_at = snap.scraped_at
    ct = snap.commute.timestamp
    o = snap.commute.optempo
    s = snap.commute.summary
    tw = o.timeWindow
    os_ = o.summary
    m = snap.commute.metro

    con.execute(
        """
        INSERT INTO commute_snapshots VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        [
            scraped_at, ct,
            o.level, o.label, o.description,
            s.isAnomalous, s.anomalyLevel,
            s.avgSpeedRatio, s.weightedSpeedRatio,
            s.corridorsTotal, s.corridorsReporting,
            os_.crossCorrelation, os_.tier1AvgDeviation, os_.tier2AvgDeviation,
            os_.dominantSignal,
            tw.label, tw.window, tw.hourET, tw.isWeekend,
            m.current, m.baseline, m.popularityRatio,
            o.rawDeviation,
        ],
    )

    for corridor in snap.commute.corridors:
        con.execute(
            """
            INSERT INTO corridor_snapshots VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            [
                scraped_at, ct,
                corridor.id, corridor.name, corridor.tier,
                corridor.status, corridor.direction, corridor.shortName,
                corridor.confidence, corridor.speedRatio,
                corridor.currentSpeed, corridor.freeFlowSpeed,
                corridor.optempoWeight,
                corridor.currentTravelTime, corridor.freeFlowTravelTime,
                corridor.roadClosure,
            ],
        )

    dt = snap.doomsday.timestamp
    all_markets = [
        (mkt, False) for mkt in snap.doomsday.markets
    ] + [
        (mkt, True) for mkt in snap.doomsday.lowVolume
    ]
    for mkt, is_low in all_markets:
        end_ts = (
            datetime.fromtimestamp(mkt.endDate / 1000, tz=timezone.utc)
            if mkt.endDate is not None
            else None
        )
        con.execute(
            """
            INSERT INTO market_snapshots VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                scraped_at, dt,
                mkt.slug, mkt.label, mkt.region,
                mkt.price, mkt.volume, mkt.volume_24h,
                end_ts, is_low,
            ],
        )

    con.commit()
    logger.success(
        "Persisted snapshot — optempo={} ({}) | markets={} | corridors={}",
        o.level, o.label,
        len(all_markets),
        len(snap.commute.corridors),
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="pizzint.watch scraper → DuckLake")
    parser.add_argument("--db", default="pizza_lake.duckdb", help="DuckDB file path")
    parser.add_argument("--once", action="store_true", help="Scrape once and exit")
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Poll interval in seconds for tail mode (default: 300)",
    )
    args = parser.parse_args()

    con = init_db(args.db)

    if args.once:
        snap = fetch_snapshot()
        persist_snapshot(con, snap)
        con.close()
        return

    # Tail mode
    logger.info("Tail mode: polling every {}s. Ctrl-C to stop.", args.interval)
    while True:
        try:
            snap = fetch_snapshot()
            persist_snapshot(con, snap)
        except httpx.HTTPStatusError as exc:
            logger.error("HTTP error: {}", exc)
        except httpx.RequestError as exc:
            logger.error("Request error: {}", exc)
        except ValueError as exc:
            logger.error("Parse error: {}", exc)
        except Exception as exc:
            logger.exception("Unexpected error: {}", exc)

        logger.info("Sleeping {}s …", args.interval)
        try:
            time.sleep(args.interval)
        except KeyboardInterrupt:
            logger.info("Interrupted. Closing DB.")
            break

    con.close()


if __name__ == "__main__":
    main()
