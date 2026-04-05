#!/usr/bin/env python3

# api-portal.etoro.com/getting-started/authentication


import os
import sys
import uuid
import argparse
import sqlite3

from time import sleep
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
import pytz

import warnings

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import urllib3

import requests

from dotenv import load_dotenv

load_dotenv()

# --- Konfiguration ---
API_BASE = os.getenv("ETORO_API_BASE", "https://public-api.etoro.com/api/v1")
API_KEY = os.getenv("ETORO_API_KEY")
USER_KEY = os.getenv("ETORO_USER_KEY")
#MODE = os.getenv("MODE", "Demo")
REQUEST_TIMEOUT = 15
DB_PATH = os.getenv("ETORO_DB_PATH", "etoro_candles.db")
# ---------------------

if not API_KEY or not USER_KEY:
    print("Fehler: ETORO_API_KEY und ETORO_USER_KEY müssen gesetzt sein.", file=sys.stderr)
    sys.exit(1)

HEADERS = {
    "x-request-id": str(uuid.uuid4()),
    "x-api-key": API_KEY,
    "x-user-key": USER_KEY,
    "Accept": "application/json",
    #"mode": MODE,
}


# --- SQLite helpers ---
def init_db(conn):
    cur = conn.cursor()
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS candles (
        instrument_id TEXT NOT NULL,
        date TEXT NOT NULL,        -- ISO date (YYYY-MM-DD or full ISO)
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        PRIMARY KEY (instrument_id, date)
    )"""
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_instrument_date ON candles(instrument_id, date)")
    # neue Tabelle zur Nachverfolgung des letzten erfolgreichen Fetch-Zeitpunkts
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS instruments (
        instrument_id TEXT PRIMARY KEY,
        last_candles_fetch TEXT  -- ISO UTC timestamp mit Z oder NULL
    )"""
    )
    conn.commit()


def fetch_candles_from_db(conn, instrument_id, from_dt, to_dt):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT date, open, high, low, close, volume
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
                "open": r[1],
                "high": r[2],
                "low": r[3],
                "close": r[4],
                "volume": r[5],
            }
        )
    return candles


def store_candles_to_db(conn, instrument_id, candles):
    cur = conn.cursor()
    to_insert = []
    for c in candles:
        date = c.get("fromDate") or c.get("date")  # beide Formen unterstützten
        open_ = c.get("open")
        high = c.get("high")
        low = c.get("low")
        close = c.get("close")
        volume = c.get("volume")
        to_insert.append((str(instrument_id), date, open_, high, low, close, volume))

    cur.executemany(
        """
        INSERT OR REPLACE INTO candles (instrument_id, date, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """,
        to_insert,
    )

    # update last_candles_fetch timestamp für das Instrument (UTC ISO mit Z)
    now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    cur.execute(
        """
        INSERT INTO instruments (instrument_id, last_candles_fetch)
        VALUES (?, ?)
        ON CONFLICT(instrument_id) DO UPDATE SET last_candles_fetch=excluded.last_candles_fetch
    """,
        (str(instrument_id), now_iso),
    )

    conn.commit()


def get_instrument_last_fetch(conn, instrument_id):
    cur = conn.cursor()
    cur.execute("SELECT last_candles_fetch FROM instruments WHERE instrument_id = ?", (str(instrument_id),))
    row = cur.fetchone()
    if not row or row[0] is None:
        return None
    ts = row[0]
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.astimezone(pytz.UTC)
    except Exception:
        return None


# --- HTTP / API ---
def http_get(path, params=None):
    url = f"{API_BASE.rstrip('/')}{path}"
    r = requests.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
    if r.status_code == 429:
        sleep(60)
        return http_get(path, params)
    r.raise_for_status()
    return r.json()


def get_watchlists():
    return http_get("/watchlists")["watchlists"]


def get_watchlist_detail(wl_id):
    try:
        return http_get(f"/watchlists/{wl_id}")
    except requests.HTTPError:
        return http_get(f"/watchlists/{wl_id}/assets")


def extract_assets(obj):
    for k in ("assets", "instruments", "items"):
        if isinstance(obj, dict) and k in obj and isinstance(obj[k], list):
            return obj[k]
    if isinstance(obj, list):
        return obj
    return []


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

        print(f"Lade: Watchlist {wl.get('name')}")

        if assets := extract_assets(wl):
            for a in assets:
                if a.get("itemType") == "Instrument" and (key := a.get("itemId")):
                    unique[key] = a

    return unique


def api_get_candles(instrument_id, from_dt, granularity="OneDay", count: int = 1000):
    path = f"/market-data/instruments/{instrument_id}/history/candles/asc/{granularity}/{count}"
    #params = {"from": from_dt.isoformat()}
    return http_get(path)#, params=params)


# --- Calculation ---
def pct_change(start, end):
    if start is None or end is None or start == 0:
        return None
    return (end - start) / start * 100.0


def avg_annual_return_5y(candles):
    if not candles:
        return None
    start_price = candles[0]["close"]
    end_price = candles[-1]["close"]
    years = 5.0
    if not start_price or start_price <= 0:
        return None
    try:
        return ((end_price / start_price) ** (1.0 / years) - 1.0) * 100.0
    except Exception:
        return None


def compute_metrics_with_cache(conn, instrument_id):
    # referenzzeitpunkt: gespeicherter last_fetch oder jetzt (UTC)
    last_fetch = get_instrument_last_fetch(conn, instrument_id)
    if last_fetch is None:
        reference_now = datetime.utcnow().replace(tzinfo=pytz.UTC)
    else:
        reference_now = last_fetch

    to_dt = reference_now
    from_5y = reference_now - relativedelta(years=5)
    from_3m = reference_now - relativedelta(months=3)
    from_1m = reference_now - relativedelta(months=1)

    cached = fetch_candles_from_db(conn, instrument_id, from_5y, to_dt)

    need_fetch_from = None
    need_fetch_to = None

    if not cached:
        need_fetch_from = from_5y
        need_fetch_to = to_dt
    else:
        def parse_db_date(dstr):
            try:
                return datetime.fromisoformat(dstr.replace("Z", "+00:00")).astimezone(pytz.UTC)
            except Exception:
                try:
                    return datetime.fromisoformat(dstr).astimezone(pytz.UTC)
                except Exception:
                    return None

        first_cached = parse_db_date(cached[0]["date"])
        last_cached = parse_db_date(cached[-1]["date"])

        pad_days = 3

        if first_cached is None or first_cached > (from_5y + relativedelta(days=pad_days + 1)):
            need_fetch_from = from_5y - relativedelta(days=pad_days)
        if last_cached is None or last_cached < to_dt - relativedelta(days=pad_days + 1):
            need_fetch_to = to_dt

    # nur wenn beide Grenzen benötigt werden -> fetch (wie vorher)
    if True or need_fetch_from is not None:
        try:
            api_resp = api_get_candles(instrument_id, need_fetch_from, granularity="OneWeek")
            # API kann verschiedene Formen liefern
            if isinstance(api_resp.get("candles"), list) and api_resp["candles"] and isinstance(api_resp["candles"][0], dict) and "candles" in api_resp["candles"][0]:
                candles = api_resp["candles"][0]["candles"]
            else:
                candles = api_resp.get("candles", [])
            if candles:
                store_candles_to_db(conn, instrument_id, candles)
        except Exception:
            # bei Fehler nicht last_candles_fetch aktualisieren
            raise

        cached = fetch_candles_from_db(conn, instrument_id, from_5y, to_dt)

    try:
        c_sorted = sorted(cached, key=lambda x: x["date"])
    except Exception:
        c_sorted = cached

    cagr_5y = avg_annual_return_5y(c_sorted) if len(c_sorted) >= 2 else None
    latest_price = c_sorted[-1]["close"] if c_sorted else None

    price_3m_start = None
    price_1m_start = None
    for c in c_sorted:
        dstr = c.get("date")
        try:
            d = datetime.fromisoformat(dstr.replace("Z", "+00:00")).astimezone(pytz.UTC) if isinstance(dstr, str) else None
        except Exception:
            d = None
        if d:
            if price_3m_start is None and d >= from_3m:
                price_3m_start = c.get("close")
            if price_1m_start is None and d >= from_1m:
                price_1m_start = c.get("close")
            if price_3m_start is not None and price_1m_start is not None:
                break

    change_3m = pct_change(price_3m_start, latest_price) if price_3m_start and latest_price else None
    change_1m = pct_change(price_1m_start, latest_price) if price_1m_start and latest_price else None

    return {"5y_cagr": cagr_5y, "3m_change": change_3m, "1m_change": change_1m}


# --- CLI & main ---
def parse_args():
    p = argparse.ArgumentParser(description="Filter watchlist stocks by performance criteria with SQLite candle cache")
    p.add_argument("-x", type=float, required=True, help="min avg annual growth (%) over last 5 years")
    p.add_argument("-y", type=float, required=True, help="min decline (%) over last 3 months (positive number)")
    p.add_argument("-z", type=float, required=True, help="min rise (%) in last month")
    return p.parse_args()


def print_row(symbol, name, iid, metrics):
    def fmt(v):
        return "-" if v is None else f"{v:.2f}"
    print(f"{symbol}\t{name}\t{iid}\t{fmt(metrics.get('5y_cagr'))}\t{fmt(metrics.get('3m_change'))}\t{fmt(metrics.get('1m_change'))}")


def main():
    args = parse_args()
    min_x = args.x
    min_y = args.y
    min_z = args.z

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    instruments = find_instrument_ids_from_watchlists()
    if not instruments:
        print("Keine Watchlist-Instrumente gefunden.", file=sys.stderr)
        sys.exit(1)

    print("Symbol\tName\tInstrumentId\t5y_CAGR(%)\t3m_change(%)\t1m_change(%)")

    for iid, raw in instruments.items():
        instrument_id = raw.get("itemId")
        market = raw.get("market")

        if market is None:
            symbol = market = "-"
            name = "-"
        else:
            symbol = market.get("symbolName") or "-"
            name = market.get("displayName") or "-"

        try:
            metrics = compute_metrics_with_cache(conn, instrument_id)
        except Exception as e:
            print(f"WARN: Fehler beim Laden/Cache für {instrument_id}: {e}", file=sys.stderr)
            continue

        if metrics["5y_cagr"] is None or metrics["3m_change"] is None or metrics["1m_change"] is None:
            continue

        if metrics["5y_cagr"] >= min_x and metrics["3m_change"] <= -abs(min_y) and metrics["1m_change"] >= min_z:
            print_row(symbol, name, instrument_id, metrics)

    conn.close()


if __name__ == "__main__":
    main()



'''


import os
import sys
import uuid
import argparse
import sqlite3
import requests
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pytz

from dotenv import load_dotenv

load_dotenv()

# --- Konfiguration ---
API_BASE = os.getenv("ETORO_API_BASE", "https://public-api.etoro.com/api/v1")
API_KEY = os.getenv("ETORO_API_KEY")
USER_KEY = os.getenv("ETORO_USER_KEY")
MODE = os.getenv("MODE", "Demo")
REQUEST_TIMEOUT = 15
DB_PATH = os.getenv("ETORO_DB_PATH", "etoro_candles.db")
# ---------------------

if not API_KEY or not USER_KEY:
    print("Fehler: ETORO_API_KEY und ETORO_USER_KEY müssen gesetzt sein.", file=sys.stderr)
    sys.exit(1)

HEADERS = {
    "x-request-id": str(uuid.uuid4()),
    "x-api-key": API_KEY,
    "x-user-key": USER_KEY,
    "Accept": "application/json",
    "mode": MODE,
}

# --- SQLite helpers ---
def init_db(conn):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS candles (
        instrument_id TEXT NOT NULL,
        date TEXT NOT NULL,        -- ISO date (YYYY-MM-DD or full ISO)
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        PRIMARY KEY (instrument_id, date)
    )""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_instrument_date ON candles(instrument_id, date)")
    conn.commit()

def fetch_candles_from_db(conn, instrument_id, from_dt, to_dt):
    cur = conn.cursor()
    cur.execute("""
        SELECT date, open, high, low, close, volume
        FROM candles
        WHERE instrument_id = ?
          AND date BETWEEN ? AND ?
        ORDER BY date ASC
    """, (str(instrument_id), from_dt.isoformat(), to_dt.isoformat()))
    rows = cur.fetchall()
    # convert to list of dicts matching API shape
    candles = []
    for r in rows:
        candles.append({
            "date": r[0],
            "open": r[1],
            "high": r[2],
            "low": r[3],
            "close": r[4],
            "volume": r[5],
        })
    return candles

def store_candles_to_db(conn, instrument_id, candles):
    cur = conn.cursor()
    to_insert = []
    for c in candles:
        date = c.get("fromDate")
        open_ = c.get("open")
        high = c.get("high")
        low = c.get("low")
        close = c.get("close")
        volume = c.get("volume")
        to_insert.append((str(instrument_id), date, open_, high, low, close, volume))
    cur.executemany("""
        INSERT OR REPLACE INTO candles (instrument_id, date, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, to_insert)
    conn.commit()

# --- HTTP / API ---
def http_get(path, params=None):
    url = f"{API_BASE.rstrip('/')}{path}"
    r = requests.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

def get_watchlists():
    return http_get("/watchlists")["watchlists"]

def get_watchlist_detail(wl_id):
    try:
        return http_get(f"/watchlists/{wl_id}")
    except requests.HTTPError:
        return http_get(f"/watchlists/{wl_id}/assets")

def extract_assets(obj):
    for k in ("assets","instruments","items"):
        if isinstance(obj, dict) and k in obj and isinstance(obj[k], list):
            return obj[k]
    if isinstance(obj, list):
        return obj
    return []

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

        print(f"Lade: Watchlist {wl.get('name')}")

        if assets := extract_assets(wl):
            for a in assets:
                if a.get('itemType') == 'Instrument' and (key := a.get("itemId")):
                    unique[key] = a

    return unique

def api_get_candles(instrument_id, from_dt, to_dt, granularity="OneDay", count: int = 1000):
    path = f"/market-data/instruments/{instrument_id}/history/candles/asc/{granularity}/{count}"

    params = {"from": from_dt.isoformat(), "to": to_dt.isoformat()}
    return http_get(path, params=params)

# --- Calculation ---
def pct_change(start, end):
    if start is None or end is None or start == 0:
        return None
    return (end - start) / start * 100.0

def avg_annual_return_5y(candles):
    if not candles:
        return None
    start_price = candles[0]['close']
    end_price = candles[-1]['close']
    years = 5.0
    if not start_price or start_price <= 0:
        return None
    try:
        return ( (end_price / start_price) ** (1.0/years) - 1.0 ) * 100.0
    except Exception:
        return None

def compute_metrics_with_cache(conn, instrument_id):

    today = datetime.utcnow().replace(tzinfo=pytz.UTC)


    to_dt = today
    from_5y = today - relativedelta(years=5)
    from_3m = today - relativedelta(months=3)
    from_1m = today - relativedelta(months=1)

    # 1) get cached candles for full 5y range
    cached = fetch_candles_from_db(conn, instrument_id, from_5y - relativedelta(days=3), to_dt)
    # determine if we need to fetch missing range from API
    need_fetch_from = None
    need_fetch_to = None
    if not cached:
        need_fetch_from = from_5y - relativedelta(days=3)
        need_fetch_to = to_dt
    else:
        # cached is sorted ascending; check first/last dates
        first_cached = datetime.fromisoformat(cached[0]['date'].replace("Z","+00:00")) if cached[0]['date'].endswith("Z") else datetime.fromisoformat(cached[0]['date'])
        last_cached = datetime.fromisoformat(cached[-1]['date'].replace("Z","+00:00")) if cached[-1]['date'].endswith("Z") else datetime.fromisoformat(cached[-1]['date'])
        
        if first_cached > (from_5y + relativedelta(days=4)):
            need_fetch_from = from_5y
        if last_cached < to_dt - relativedelta(days=4):
            need_fetch_to = to_dt

    # fetch missing candles and store
    if need_fetch_from is not None and need_fetch_to is not None:
        try:
            api_resp = api_get_candles(instrument_id, need_fetch_from, need_fetch_to, granularity="OneDay")
        except Exception as e:
            raise
        candles = api_resp["candles"][0]['candles']
        store_candles_to_db(conn, instrument_id, candles)
        # reload cached
        cached = fetch_candles_from_db(conn, instrument_id, from_5y - relativedelta(days=3), to_dt)
   # ensure sorted
    try:
        c_sorted = sorted(cached, key=lambda x: x['date'])
    except Exception:
        c_sorted = cached

    # derive metrics
    cagr_5y = avg_annual_return_5y(c_sorted) if len(c_sorted) >= 2 else None

    latest_price = c_sorted[-1]['close'] if c_sorted else None

    price_3m_start = None
    price_1m_start = None
    for c in c_sorted:
        # parse date robustly
        dstr = c.get("date")
        try:
            d = datetime.fromisoformat(dstr.replace("Z","+00:00")) if isinstance(dstr, str) else None
        except Exception:
            d = None
        if d:
            if price_3m_start is None and d >= from_3m:
                price_3m_start = c.get("close")
            if price_1m_start is None and d >= from_1m:
                price_1m_start = c.get("close")
            if price_3m_start is not None and price_1m_start is not None:
                break

    change_3m = pct_change(price_3m_start, latest_price) if price_3m_start and latest_price else None
    change_1m = pct_change(price_1m_start, latest_price) if price_1m_start and latest_price else None

    return {
        "5y_cagr": cagr_5y,
        "3m_change": change_3m,
        "1m_change": change_1m,
    }

# --- CLI & main ---
def parse_args():
    p = argparse.ArgumentParser(description="Filter watchlist stocks by performance criteria with SQLite candle cache")
    p.add_argument("-x", type=float, required=True, help="min avg annual growth (%) over last 5 years")
    p.add_argument("-y", type=float, required=True, help="min decline (%) over last 3 months (positive number)")
    p.add_argument("-z", type=float, required=True, help="min rise (%) in last month")
    return p.parse_args()

def print_row(symbol, name, iid, metrics):
    def fmt(v):
        return "-" if v is None else f"{v:.2f}"
    print(f"{symbol}\t{name}\t{iid}\t{fmt(metrics.get('5y_cagr'))}\t{fmt(metrics.get('3m_change'))}\t{fmt(metrics.get('1m_change'))}")

def main():
    args = parse_args()
    min_x = args.x
    min_y = args.y
    min_z = args.z

    # open DB
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    instruments = find_instrument_ids_from_watchlists()
    if not instruments:
        print("Keine Watchlist-Instrumente gefunden.", file=sys.stderr)
        sys.exit(1)

    print("Symbol\tName\tInstrumentId\t5y_CAGR(%)\t3m_change(%)\t1m_change(%)")

    for iid, raw in instruments.items():
        instrument_id = raw.get('itemId')
        market = raw.get('market')

        if market is None:
            symbol = market = "-"
        else:
            symbol = market.get("symbolName") or "-"
            name = market.get("displayName") or "-"

        try:
            metrics = compute_metrics_with_cache(conn, instrument_id)
        except Exception as e:
            print(f"WARN: Fehler beim Laden/Cache für {instrument_id}: {e}", file=sys.stderr)
            continue

        if metrics["5y_cagr"] is None or metrics["3m_change"] is None or metrics["1m_change"] is None:
            continue

        if metrics["5y_cagr"] >= min_x and metrics["3m_change"] <= -abs(min_y) and metrics["1m_change"] >= min_z:
            print_row(symbol, name, instrument_id, metrics)

    conn.close()

if __name__ == "__main__":
    main()
'''

# curl --request GET --url https://public-api.etoro.com/api/v1/market-data/instruments/3006/history/candles/desc/OneDay/1000 --header 'x-request-id: 7ef2ac31-1311-4c17-8f3a-246fc4bdf1f3' --header 'x-api-key: sdgdskldFPLGfjHn1421dgnlxdGTbngdflg6290bRjslfihsjhSDsdgGHH25hjf' --header 'x-user-key: eyJjaSI6IjYwY2FiYjBiLTU1OTctNDQ4NS04ZjYzLTdlOWUwNTZlMGJiOCIsImVhbiI6IlVucmVnaXN0ZXJlZEFwcGxpY2F0aW9uIiwiZWsiOiIzNTZhRXYuYlVRdmhzTUoyZ0NUb2RjZHQ5eDJNTXE5NUk0SVRUbW12bU53U0tyaG9VOHJsTnBHMTNGVW4ueW9hY3NBZEE5MHB3M3pkZnNmMGdVb2lHeS54bTItYjIwV0pJb3J5MHY1WTU4SV8ifQ__'