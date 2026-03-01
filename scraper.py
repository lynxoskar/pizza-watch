"""
pizzint.watch scraper
---------------------
Fetches the Pentagon Pizza Index data from https://pizzint.watch by parsing
the Next.js SSR payload embedded in the HTML (no headless browser needed).
Also tails /api/dashboard-data (place popularity) and /api/osint-feed (OSINT tweets).

Outputs to a DuckLake (DuckDB file) with tables:
  - commute_snapshots   : per-scrape traffic / optempo summary
  - corridor_snapshots  : per-scrape per-corridor traffic
  - market_snapshots    : per-scrape polymarket prices
  - place_popularity    : hourly pizza-place popularity ticks (deduped)
  - osint_tweets        : OSINT tweet feed (deduped on tweet id)
  - scrape_log          : every HTTP attempt with status + duration

Usage
-----
  python scraper.py --once               # single SSR scrape and exit
  python scraper.py --interval 300       # tail mode, poll every 5 minutes
"""

import argparse
import json
import re
import time
from datetime import UTC, datetime
from typing import Any

import duckdb
import httpx
from loguru import logger
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

URL_SSR = "https://www.pizzint.watch/"
URL_DASHBOARD = "https://www.pizzint.watch/api/dashboard-data"
URL_OSINT = "https://www.pizzint.watch/api/osint-feed?includeTruth=true"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# Retry config
_MAX_RETRIES = 5
_BACKOFF_CAP_S = 300  # 5 minutes max between retries
_RETRY_AFTER_DEFAULT_S = 600  # 10 minutes if 429 has no Retry-After header
_BACKOFF_5XX_S = 30

# Tail cadences
_CADENCE_SSR_S = 300  # every 5 min — SSR page (commute + markets)
_CADENCE_DASHBOARD_S = 3600  # every 60 min — place popularity
_CADENCE_OSINT_S = 900  # every 15 min — OSINT tweets

# ---------------------------------------------------------------------------
# Pydantic models — SSR payload
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
    corridors: list[Corridor]


class Market(BaseModel):
    slug: str
    label: str
    region: str
    price: float
    tokenId: str
    seriesId: str | None = None
    eventSlug: str | None = None
    image: str | None = None
    volume: float | None = None
    volume_24h: float | None = None
    endDate: float | None = None


class DoomsdayData(BaseModel):
    markets: list[Market]
    lowVolume: list[Market]
    timestamp: datetime


class PizzintSnapshot(BaseModel):
    scraped_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    commute: CommuteData
    doomsday: DoomsdayData


# ---------------------------------------------------------------------------
# Pydantic models — API endpoints
# ---------------------------------------------------------------------------


class SparklinePoint(BaseModel):
    place_id: str
    current_popularity: int | None = None  # null when place is closed / no data
    recorded_at: datetime


class PlaceData(BaseModel):
    place_id: str
    name: str
    current_popularity: int | None = None
    percentage_of_usual: float | None = None
    is_spike: bool = False
    spike_magnitude: float | None = None
    data_source: str | None = None
    recorded_at: datetime
    data_freshness: str | None = None
    sparkline_24h: list[SparklinePoint] = Field(default_factory=list)
    is_closed_now: bool = False


class DashboardResponse(BaseModel):
    success: bool
    data: list[PlaceData]
    overall_index: float | None = None
    defcon_level: int | None = None
    timestamp: datetime


class OsintTweet(BaseModel):
    id: str
    text: str
    url: str
    timestamp: datetime
    handle: str
    isAlert: bool = False


class OsintFeedResponse(BaseModel):
    success: bool
    tweets: list[OsintTweet]
    timestamp: datetime
    source: str | None = None
    sourceCount: int | None = None


# ---------------------------------------------------------------------------
# HTTP client with retry + rate-limit handling  [bd-1ew]
# ---------------------------------------------------------------------------


def _fetch_with_retry(url: str, client: httpx.Client) -> httpx.Response:
    """
    GET `url` with exponential backoff retry.

    Retry policy:
      - 429: respect Retry-After header, else wait _RETRY_AFTER_DEFAULT_S
      - 5xx: wait _BACKOFF_5XX_S then retry with backoff
      - network errors: backoff 2^attempt seconds, capped at _BACKOFF_CAP_S
      - max _MAX_RETRIES attempts total before re-raising
    """
    last_exc: Exception | None = None
    for attempt in range(_MAX_RETRIES):
        try:
            resp = client.get(url)
            if resp.status_code == 429:
                wait = int(resp.headers.get("retry-after", _RETRY_AFTER_DEFAULT_S))
                logger.warning("[PIZZINT] 429 rate-limited on {} — waiting {}s", url, wait)
                time.sleep(wait)
                continue
            if resp.status_code >= 500:
                wait = min(_BACKOFF_5XX_S * (2**attempt), _BACKOFF_CAP_S)
                logger.warning(
                    "[PIZZINT] HTTP {} on {} (attempt {}/{}) — retrying in {}s",
                    resp.status_code,
                    url,
                    attempt + 1,
                    _MAX_RETRIES,
                    wait,
                )
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except httpx.RequestError as exc:
            last_exc = exc
            wait = min(2**attempt, _BACKOFF_CAP_S)
            logger.warning(
                "[PIZZINT] Request error on {} (attempt {}/{}) — retrying in {}s: {}",
                url,
                attempt + 1,
                _MAX_RETRIES,
                wait,
                exc,
            )
            time.sleep(wait)

    raise last_exc or RuntimeError(f"Failed to fetch {url} after {_MAX_RETRIES} attempts")


# ---------------------------------------------------------------------------
# Fetch functions
# ---------------------------------------------------------------------------


def _extract_payload(html: str) -> dict[str, Any]:
    """Parse the Next.js __next_f SSR chunks embedded in the HTML."""
    idx = html.find("initialDoomsdayData")
    if idx < 0:
        raise ValueError("Could not find initialDoomsdayData in HTML")

    start = html.rfind("self.__next_f.push", 0, idx)
    end = html.find("</script>", idx)
    chunk = html[start:end]

    m = re.search(r'self\.__next_f\.push\(\[1,"(.*)"\]\)', chunk, re.DOTALL)
    if not m:
        raise ValueError("Could not parse __next_f.push chunk")

    raw = m.group(1).encode().decode("unicode_escape")

    payload_start = raw.find('{"initialDoomsdayData"')
    if payload_start < 0:
        raise ValueError("Could not locate top-level payload object")

    payload_chars = list(raw[payload_start:])
    depth = 0
    for i, c in enumerate(payload_chars):
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return json.loads("".join(payload_chars[: i + 1]))

    raise ValueError("Unbalanced braces in SSR payload")


def fetch_snapshot() -> PizzintSnapshot:
    """Fetch the SSR page and extract commute + market snapshot."""
    logger.info("[PIZZINT] Fetching SSR snapshot from {}", URL_SSR)
    with httpx.Client(headers=HEADERS, timeout=30, follow_redirects=True) as client:
        resp = _fetch_with_retry(URL_SSR, client)

    payload = _extract_payload(resp.text)
    commute = CommuteData.model_validate(payload["initialCommuteData"])
    doomsday = DoomsdayData.model_validate(payload["initialDoomsdayData"])
    return PizzintSnapshot(commute=commute, doomsday=doomsday)


def fetch_dashboard() -> DashboardResponse:
    """Fetch /api/dashboard-data — place popularity + defcon."""
    logger.info("[PIZZINT] Fetching dashboard-data from {}", URL_DASHBOARD)
    with httpx.Client(headers=HEADERS, timeout=30, follow_redirects=True) as client:
        resp = _fetch_with_retry(URL_DASHBOARD, client)
    return DashboardResponse.model_validate(resp.json())


def fetch_osint_feed() -> OsintFeedResponse:
    """Fetch /api/osint-feed — OSINT tweets."""
    logger.info("[PIZZINT] Fetching osint-feed from {}", URL_OSINT)
    with httpx.Client(headers=HEADERS, timeout=30, follow_redirects=True) as client:
        resp = _fetch_with_retry(URL_OSINT, client)
    return OsintFeedResponse.model_validate(resp.json())


# ---------------------------------------------------------------------------
# DuckLake DDL  [bd-clj]
# ---------------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS commute_snapshots (
    scraped_at              TIMESTAMPTZ NOT NULL,
    data_timestamp          TIMESTAMPTZ NOT NULL,
    optempo_level           INTEGER,
    optempo_label           VARCHAR,
    optempo_description     VARCHAR,
    is_anomalous            BOOLEAN,
    anomaly_level           VARCHAR,
    avg_speed_ratio         DOUBLE,
    weighted_speed_ratio    DOUBLE,
    corridors_total         INTEGER,
    corridors_reporting     INTEGER,
    cross_correlation       DOUBLE,
    tier1_avg_deviation     DOUBLE,
    tier2_avg_deviation     DOUBLE,
    dominant_signal         VARCHAR,
    time_window_label       VARCHAR,
    time_window             VARCHAR,
    hour_et                 INTEGER,
    is_weekend              BOOLEAN,
    metro_current           DOUBLE,
    metro_baseline          DOUBLE,
    metro_popularity_ratio  DOUBLE,
    raw_deviation           DOUBLE
);

CREATE TABLE IF NOT EXISTS corridor_snapshots (
    scraped_at              TIMESTAMPTZ NOT NULL,
    data_timestamp          TIMESTAMPTZ NOT NULL,
    corridor_id             VARCHAR NOT NULL,
    name                    VARCHAR,
    tier                    INTEGER,
    status                  VARCHAR,
    direction               VARCHAR,
    short_name              VARCHAR,
    confidence              DOUBLE,
    speed_ratio             DOUBLE,
    current_speed           DOUBLE,
    free_flow_speed         DOUBLE,
    optempo_weight          DOUBLE,
    current_travel_time     DOUBLE,
    free_flow_travel_time   DOUBLE,
    road_closure            BOOLEAN
);

CREATE TABLE IF NOT EXISTS market_snapshots (
    scraped_at      TIMESTAMPTZ NOT NULL,
    data_timestamp  TIMESTAMPTZ NOT NULL,
    slug            VARCHAR NOT NULL,
    label           VARCHAR,
    region          VARCHAR,
    price           DOUBLE,
    volume          DOUBLE,
    volume_24h      DOUBLE,
    end_date        TIMESTAMPTZ,
    is_low_volume   BOOLEAN
);

-- Pizza-place hourly popularity ticks; deduped on (place_id, recorded_at)  [bd-36r]
CREATE TABLE IF NOT EXISTS place_popularity (
    place_id            VARCHAR NOT NULL,
    name                VARCHAR,
    recorded_at         TIMESTAMPTZ NOT NULL,
    current_popularity  INTEGER,  -- nullable when place is closed / no data
    data_freshness      VARCHAR,
    is_spike            BOOLEAN,
    spike_magnitude     DOUBLE,
    PRIMARY KEY (place_id, recorded_at)
);

-- OSINT tweet feed; deduped on tweet id  [bd-36r]
CREATE TABLE IF NOT EXISTS osint_tweets (
    id          VARCHAR PRIMARY KEY,
    text        VARCHAR,
    url         VARCHAR,
    timestamp   TIMESTAMPTZ,
    handle      VARCHAR,
    is_alert    BOOLEAN,
    ingested_at TIMESTAMPTZ NOT NULL
);

-- Scrape attempt log  [bd-3kb]
CREATE TABLE IF NOT EXISTS scrape_log (
    logged_at       TIMESTAMPTZ NOT NULL,
    endpoint        VARCHAR NOT NULL,
    success         BOOLEAN NOT NULL,
    status_code     INTEGER,
    duration_ms     INTEGER,
    error_msg       VARCHAR
);
"""


def _count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    row = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()  # noqa: S608
    assert row is not None
    return int(row[0])


def init_db(path: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(path)
    con.execute(DDL)
    con.commit()
    logger.info("[PIZZINT] DuckLake initialised at {}", path)
    return con


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------


def _log_scrape(
    con: duckdb.DuckDBPyConnection,
    endpoint: str,
    success: bool,
    status_code: int | None = None,
    duration_ms: int | None = None,
    error_msg: str | None = None,
) -> None:
    con.execute(
        "INSERT INTO scrape_log VALUES (?, ?, ?, ?, ?, ?)",
        [datetime.now(UTC), endpoint, success, status_code, duration_ms, error_msg],
    )
    con.commit()


def persist_snapshot(con: duckdb.DuckDBPyConnection, snap: PizzintSnapshot) -> None:
    """Persist SSR commute + market snapshot."""
    scraped_at = snap.scraped_at
    ct = snap.commute.timestamp
    o = snap.commute.optempo
    s = snap.commute.summary
    tw = o.timeWindow
    os_ = o.summary
    m = snap.commute.metro

    con.execute(
        "INSERT INTO commute_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            scraped_at,
            ct,
            o.level,
            o.label,
            o.description,
            s.isAnomalous,
            s.anomalyLevel,
            s.avgSpeedRatio,
            s.weightedSpeedRatio,
            s.corridorsTotal,
            s.corridorsReporting,
            os_.crossCorrelation,
            os_.tier1AvgDeviation,
            os_.tier2AvgDeviation,
            os_.dominantSignal,
            tw.label,
            tw.window,
            tw.hourET,
            tw.isWeekend,
            m.current,
            m.baseline,
            m.popularityRatio,
            o.rawDeviation,
        ],
    )

    for corridor in snap.commute.corridors:
        con.execute(
            "INSERT INTO corridor_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                scraped_at,
                ct,
                corridor.id,
                corridor.name,
                corridor.tier,
                corridor.status,
                corridor.direction,
                corridor.shortName,
                corridor.confidence,
                corridor.speedRatio,
                corridor.currentSpeed,
                corridor.freeFlowSpeed,
                corridor.optempoWeight,
                corridor.currentTravelTime,
                corridor.freeFlowTravelTime,
                corridor.roadClosure,
            ],
        )

    dt = snap.doomsday.timestamp
    all_markets = [(mkt, False) for mkt in snap.doomsday.markets] + [
        (mkt, True) for mkt in snap.doomsday.lowVolume
    ]
    for mkt, is_low in all_markets:
        end_ts = (
            datetime.fromtimestamp(mkt.endDate / 1000, tz=UTC) if mkt.endDate is not None else None
        )
        con.execute(
            "INSERT INTO market_snapshots VALUES (?,?,?,?,?,?,?,?,?,?)",
            [
                scraped_at,
                dt,
                mkt.slug,
                mkt.label,
                mkt.region,
                mkt.price,
                mkt.volume,
                mkt.volume_24h,
                end_ts,
                is_low,
            ],
        )

    con.commit()
    logger.success(
        "[PIZZINT] SSR snapshot — optempo={} ({}) | markets={} | corridors={}",
        o.level,
        o.label,
        len(all_markets),
        len(snap.commute.corridors),
    )


def persist_dashboard(con: duckdb.DuckDBPyConnection, resp: DashboardResponse) -> int:
    """
    Persist place popularity ticks from a dashboard response.
    Deduped on (place_id, recorded_at) via ON CONFLICT DO NOTHING.
    Returns the number of new rows inserted.
    """
    before = _count(con, "place_popularity")
    for place in resp.data:
        for tick in place.sparkline_24h:
            con.execute(
                """
                INSERT INTO place_popularity
                    (place_id, name, recorded_at, current_popularity,
                     data_freshness, is_spike, spike_magnitude)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (place_id, recorded_at) DO NOTHING
                """,
                [
                    tick.place_id,
                    place.name,
                    tick.recorded_at,
                    tick.current_popularity,
                    place.data_freshness,
                    place.is_spike,
                    place.spike_magnitude,
                ],
            )
    con.commit()
    inserted = _count(con, "place_popularity") - before
    logger.success(
        "[PIZZINT] Dashboard — {} places | {} new popularity ticks inserted",
        len(resp.data),
        inserted,
    )
    return inserted


def persist_osint_feed(con: duckdb.DuckDBPyConnection, resp: OsintFeedResponse) -> int:
    """
    Persist OSINT tweets, deduped on tweet id via ON CONFLICT DO NOTHING.
    Returns the number of new rows inserted.
    """
    ingested_at = datetime.now(UTC)
    before = _count(con, "osint_tweets")
    for tweet in resp.tweets:
        con.execute(
            """
            INSERT INTO osint_tweets
                (id, text, url, timestamp, handle, is_alert, ingested_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO NOTHING
            """,
            [
                tweet.id,
                tweet.text,
                tweet.url,
                tweet.timestamp,
                tweet.handle,
                tweet.isAlert,
                ingested_at,
            ],
        )
    con.commit()
    inserted = _count(con, "osint_tweets") - before
    logger.success(
        "[PIZZINT] OSINT feed — {} tweets received | {} new inserted",
        len(resp.tweets),
        inserted,
    )
    return inserted


# ---------------------------------------------------------------------------
# Timed scrape wrappers (log every attempt)
# ---------------------------------------------------------------------------


def _timed_fetch_snapshot(con: duckdb.DuckDBPyConnection) -> PizzintSnapshot | None:
    t0 = time.monotonic()
    try:
        snap = fetch_snapshot()
        persist_snapshot(con, snap)
        _log_scrape(con, URL_SSR, True, 200, int((time.monotonic() - t0) * 1000))
        return snap
    except Exception as exc:
        _log_scrape(con, URL_SSR, False, None, int((time.monotonic() - t0) * 1000), str(exc))
        logger.error("[PIZZINT] SSR fetch failed: {}", exc)
        return None


def _timed_fetch_dashboard(con: duckdb.DuckDBPyConnection) -> None:
    t0 = time.monotonic()
    try:
        resp = fetch_dashboard()
        persist_dashboard(con, resp)
        _log_scrape(con, URL_DASHBOARD, True, 200, int((time.monotonic() - t0) * 1000))
    except Exception as exc:
        _log_scrape(con, URL_DASHBOARD, False, None, int((time.monotonic() - t0) * 1000), str(exc))
        logger.error("[PIZZINT] Dashboard fetch failed: {}", exc)


def _timed_fetch_osint(con: duckdb.DuckDBPyConnection) -> None:
    t0 = time.monotonic()
    try:
        resp = fetch_osint_feed()
        persist_osint_feed(con, resp)
        _log_scrape(con, URL_OSINT, True, 200, int((time.monotonic() - t0) * 1000))
    except Exception as exc:
        _log_scrape(con, URL_OSINT, False, None, int((time.monotonic() - t0) * 1000), str(exc))
        logger.error("[PIZZINT] OSINT fetch failed: {}", exc)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="pizzint.watch scraper → DuckLake")
    parser.add_argument("--db", default="pizza_lake.duckdb", help="DuckDB file path")
    parser.add_argument("--once", action="store_true", help="Scrape all endpoints once and exit")
    parser.add_argument(
        "--interval",
        type=int,
        default=_CADENCE_SSR_S,
        help="SSR poll interval in seconds for tail mode (default: 300)",
    )
    args = parser.parse_args()

    con = init_db(args.db)

    if args.once:
        _timed_fetch_snapshot(con)
        _timed_fetch_dashboard(con)
        _timed_fetch_osint(con)
        con.close()
        return

    # Tail mode — track last-run timestamps per endpoint
    logger.info(
        "[PIZZINT] Tail mode — SSR={}s | dashboard={}s | osint={}s. Ctrl-C to stop.",
        args.interval,
        _CADENCE_DASHBOARD_S,
        _CADENCE_OSINT_S,
    )

    last_ssr = 0.0
    last_dashboard = 0.0
    last_osint = 0.0

    try:
        while True:
            now = time.monotonic()

            if now - last_ssr >= args.interval:
                _timed_fetch_snapshot(con)
                last_ssr = time.monotonic()

            if now - last_dashboard >= _CADENCE_DASHBOARD_S:
                _timed_fetch_dashboard(con)
                last_dashboard = time.monotonic()

            if now - last_osint >= _CADENCE_OSINT_S:
                _timed_fetch_osint(con)
                last_osint = time.monotonic()

            time.sleep(10)  # check cadences every 10s

    except KeyboardInterrupt:
        logger.info("[PIZZINT] Interrupted. Closing DB.")

    con.close()


if __name__ == "__main__":
    main()
