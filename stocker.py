#!/usr/bin/env python3

# api-portal.etoro.com/getting-started/authentication

import os
import sys
import uuid
import argparse
import sqlite3

from time import sleep
from datetime import date, datetime, timezone
from dateutil.relativedelta import relativedelta
import pytz

import warnings

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import urllib3

import requests

from dotenv import load_dotenv

load_dotenv()

# -------------------- Konfiguration -----------------------
API_KEY = os.getenv("ETORO_API_KEY")
USER_KEY = os.getenv("ETORO_USER_KEY")

REQUEST_TIMEOUT = 3
DB_PATH = os.getenv("ETORO_DB_PATH", "etoro_candles.db")
# ----------------------------------------------------------

if not API_KEY or not USER_KEY:
    print("Fehler: ETORO_API_KEY und ETORO_USER_KEY müssen gesetzt sein.", file=sys.stderr)
    sys.exit(1)

HEADERS = {
    "x-request-id": str(uuid.uuid4()),
    "x-api-key": API_KEY,
    "x-user-key": USER_KEY,
    "Accept": "application/json"
}

def init_db(conn):
    cur = conn.cursor()

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS candles (
        instrument_id TEXT NOT NULL,
        date TEXT NOT NULL,  
        open REAL,
        PRIMARY KEY (instrument_id, date)
    )"""
    )
    
    cur.execute("CREATE INDEX IF NOT EXISTS idx_instrument_date ON candles(instrument_id, date)")
    
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS instruments (
        instrument_id TEXT PRIMARY KEY,
        symbol TEXT,
        name TEXT,
        last_candles_fetch TEXT,
        weekday_bitmask INT
    )"""
    )
    
    conn.commit()

def fetch_candles_from_db(conn, instrument_id, from_dt, to_dt):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT date, open
        FROM candles
        WHERE instrument_id = ?
          AND date BETWEEN ? AND ?
        ORDER BY date ASC
    """,
        (str(instrument_id), from_dt.isoformat(), to_dt.isoformat()),
    )
    rows = cur.fetchall()
    candles = []
    for r in rows:
        candles.append(
            {
                "date": r[0],
                "open": r[1]
            }
        )
    return candles

def store_candles_to_db(conn, instrument_id, candles, weekday_bitmask):
    cur = conn.cursor()
    to_insert = []
    for c in candles:
        date = c.get("fromDate")
        open = c.get("open")
        to_insert.append((str(instrument_id), date, open))

    cur.executemany(
        """
        INSERT OR REPLACE INTO candles (instrument_id, date, open)
        VALUES (?, ?, ?)
    """,
        to_insert,
    )

    now_iso = datetime.now(timezone.utc).replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

    cur.execute(
        """
        INSERT INTO instruments (instrument_id, last_candles_fetch, weekday_bitmask)
        VALUES (?, ?, ?)
        ON CONFLICT(instrument_id) DO UPDATE SET last_candles_fetch=excluded.last_candles_fetch, weekday_bitmask=excluded.weekday_bitmask
    """,
        (str(instrument_id), now_iso, weekday_bitmask),
    )

    conn.commit()

def save_instrument(conn, id, symbol, name):
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO instruments (instrument_id, symbol, name)
        VALUES (?, ?, ?)
        ON CONFLICT(instrument_id) DO UPDATE SET symbol=excluded.symbol, name=excluded.name
    """,
        (str(id), symbol, name),
    )

    conn.commit() 

def fetch_instruments(conn):
    instruments = find_instrument_ids_from_watchlists()
    if not instruments:
        print("Keine Watchlist-Instrumente gefunden.", file=sys.stderr)
        sys.exit(1)

    inst = []
    for iid, r in instruments.items():
        instrument_id = r.get("itemId")
        market = r.get("market")

        if market is None:
            symbol = market = "-"
            name = "-"
        else:
            symbol = market.get("symbolName")
            name = market.get("displayName")            
            
        save_instrument(conn, instrument_id, symbol, name)

        inst.append( {
            "instrumentId": instrument_id,
            "symbolName": symbol,
            "displayName": name
        }) 

def load_instruments(conn):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT instrument_id, symbol, name
        FROM instruments
        """
    )

    rows = cur.fetchall()
    instruments = []
    for r in rows:
        instruments.append(
            {
                "instrumentId": r[0],
                "symbolName": r[1],
                "displayName": r[2],
            }
        )
    return instruments

def get_instrument_fetch_bitmask(conn, instrument_id):
    cur = conn.cursor()

    cur.execute("SELECT weekday_bitmask FROM instruments WHERE instrument_id = ?", (str(instrument_id),))
    row = cur.fetchone()
    
    if not row:
        return None
    
    return row[0]

def http_get(path, params=None):
    url = f"https://public-api.etoro.com/api/v1{path}"

    r = requests.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
    sleep(1) # 60 requests per second
      
    if r.status_code == 429:
        print(f"WARN: Rate limit erreicht", file=sys.stderr)
        sleep(3)
        return http_get(path, params)
    
    r.raise_for_status()
    return r.json()

def get_watchlists():
    return http_get("/watchlists")["watchlists"]

def get_watchlist_detail(wl_id):
    return http_get(f"/watchlists/{wl_id}")

def find_instrument_ids_from_watchlists():
    unique = {}

    wlists = get_watchlists()
    for wl in wlists:
        wl_id = wl.get("watchlistId")
        if not wl_id:
            continue
        try:
            detail = get_watchlist_detail(wl_id)
        except Exception as e:
            print(f"WARN: Watchlist {wl_id} nicht geladen: {e}", file=sys.stderr)

        print(f"Lade Watchlist: {wl.get('name')}")

        if assets := wl['items']:
            for a in assets:
                if a.get("itemType") == "Instrument" and (key := a.get("itemId")):
                    unique[key] = a

    return unique

def api_get_candles(instrument_id, granularity="OneDay", count: int = 1000):
    path = f"/market-data/instruments/{instrument_id}/history/candles/asc/{granularity}/{count}"
    return http_get(path)

# --- Calculation ---
def pct_change(start, end):
    if start is None or end is None or start == 0:
        return None
    return (end - start) / start * 100.0

def avg_annual_return_5y(candles):
    if not candles:
        return None
    start_price = candles[0]['open']
    end_price = candles[-1]['open']
    years = 5.0
    if not start_price or start_price <= 0:
        return None
    try:
        return ((end_price / start_price) ** (1.0 / years) - 1.0) * 100.0
    except Exception:
        return None

def compute_metrics_with_cache(conn, instrument_id, fetch_remotely: bool):
    ts_now = datetime.now(timezone.utc)

    to_dt = ts_now
    from_5y = ts_now - relativedelta(years=5)
    from_3m = ts_now - relativedelta(months=3)
    from_1m = ts_now - relativedelta(months=1)

    cached = fetch_candles_from_db(conn, instrument_id, from_5y, to_dt)

    weekday = date.today().isoweekday()
    weekday_bitmask_now = weekday_bitmask = 1 << weekday

    def parse_db_date(dstr):
        return datetime.fromisoformat(dstr.replace("Z", "+00:00")).astimezone(pytz.UTC)
       
    weekday_bitmask = get_instrument_fetch_bitmask(conn, instrument_id)
   
    if weekday_bitmask is None:
        weekday_bitmask = weekday_bitmask_now

        granularity = 'OneWeek'
        count = 1000
    else:
        last_cached = parse_db_date(cached[-1]["date"])

        delta = ts_now - last_cached
        days = delta.days 

        if (weekday_bitmask & weekday_bitmask_now) and days < 1000:
            granularity = 'OneDay'
            count = days
        else:
            weekday_bitmask |= weekday_bitmask_now

            granularity = 'OneWeek'
            count = 1000
        
    if fetch_remotely and (len(cached) == 0 or count):
        api_resp = api_get_candles(instrument_id, granularity=granularity, count=count)

        candles = api_resp["candles"][0]["candles"]
        if candles:
            store_candles_to_db(conn, instrument_id, candles, weekday_bitmask)

        cached = fetch_candles_from_db(conn, instrument_id, from_5y, to_dt)

    c_sorted = sorted(cached, key=lambda x: x["date"])

    cagr_5y = avg_annual_return_5y(c_sorted) if len(c_sorted) >= 2 else None
    latest_price = c_sorted[-1]["open"] if c_sorted else None

    price_3m_start = None
    price_1m_start = None

    for c in c_sorted:
        dstr = c.get("date")
        
        d = parse_db_date(dstr) if isinstance(dstr, str) else None
   
        if d:
            if price_3m_start is None and d >= from_3m:
                price_3m_start = c.get("open")
            if price_1m_start is None and d >= from_1m:
                price_1m_start = c.get("open")
            if price_3m_start is not None and price_1m_start is not None:
                break

    change_3m = pct_change(price_3m_start, latest_price) if price_3m_start and latest_price else None
    change_1m = pct_change(price_1m_start, latest_price) if price_1m_start and latest_price else None

    return {"5y_annual_change": cagr_5y, "3m_change": change_3m, "1m_change": change_1m}

def print_row(symbol, name, iid, metrics):
    def fmt(v):
        return "-" if v is None else f"{v:.2f}"
    print(f"{symbol}\t{name}\t{iid}\t{fmt(metrics.get('5y_annual_change'))}\t{fmt(metrics.get('3m_change'))}\t{fmt(metrics.get('1m_change'))}")

def parse_args():
    p = argparse.ArgumentParser(description="Filter watchlist stocks by performance criteria with SQLite candle cache")

    p.add_argument("-x", type=float, default=20, required=False, help="min avg annual growth (percent) over last 5 years")
    p.add_argument("-y", type=float, default=10, required=False, help="min decline percent over last 3 months (positive number)")
    p.add_argument("-z", type=float, default=0, required=False, help="min rise percent in last month")

    p.add_argument('--fetch', action=argparse.BooleanOptionalAction, help="fetch remote data")

    try:
        return p.parse_args()
    except: 
        #p.print_usage()
        p.print_help()
        sys.exit(1)

def main():
    args = parse_args()

    min_x = args.x
    min_y = args.y
    min_z = args.z

    fetch = args.fetch
    if fetch is None:
        fetch = False

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    if fetch:
        inst = fetch_instruments(conn)
    else:
        inst = load_instruments(conn)

    print("Symbol\tName\tInstrumentId\t5y_annual_change(%)\t3m_change(%)\t1m_change(%)")

    for raw in inst:
        instrument_id = raw['instrumentId']
        symbol = raw['symbolName']
        name = raw['displayName']

        try:
            metrics = compute_metrics_with_cache(conn, instrument_id, fetch)
        except Exception as e:
            print(f"WARN: Fehler beim Laden/Cache für {instrument_id}: {e}", file=sys.stderr)
            continue

        if metrics["5y_annual_change"] is None or metrics["3m_change"] is None or metrics["1m_change"] is None:
            continue

        if metrics["5y_annual_change"] >= min_x and metrics["3m_change"] <= -abs(min_y) and metrics["1m_change"] >= min_z:
            print_row(symbol, name, instrument_id, metrics)

    conn.close()

if __name__ == "__main__":
    main()