#!/usr/bin/env python3

# api-portal.etoro.com/getting-started/authentication

import os
import sys
import uuid
import argparse
import sqlite3

from sqlalchemy import (
    create_engine,
    Engine,

    Column,
    String,
    Integer,
    Float,

    PrimaryKeyConstraint,
    Index,
    
    select,
    and_,
)

from sqlalchemy.orm import declarative_base, Session

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

Base = declarative_base()

class Candle(Base):
    __tablename__ = "candles"
    instrument_id = Column(String, nullable=False)
    date = Column(String, nullable=False) # Zulu-DATETIME
    open = Column(Float)
    __table_args__ = (
        PrimaryKeyConstraint("instrument_id", "date", name="pk_candles"),
        Index("idx_instrument_date", "instrument_id", "date"),
    )

class Instrument(Base):
    __tablename__ = "instruments"
    instrument_id = Column(String, primary_key=True)
    symbol = Column(String)
    name = Column(String)
    last_candles_fetch = Column(String) # Zulu-DATETIME
    weekday_bitmask = Column(Integer)

def init_db() -> Engine:
    engine = create_engine(f"sqlite:///{DB_PATH}", future=True)
    Base.metadata.create_all(engine)

    return engine

def load_candles(session: Session, instrument_id, from_dt, to_dt) -> list[Candle]:
    stmt = (
        select(Candle)
        .where(
            and_(
                Candle.instrument_id == str(instrument_id),
                Candle.date.between(from_dt.isoformat(), to_dt.isoformat()),
            )
        )
        .order_by(Candle.date.asc())
    )
    return session.execute(stmt).scalars().all()

def save_candles(session: Session, instrument_id, candles, weekday_bitmask):
    
    for c in candles:
        date_str = c.get("fromDate")
        open_price = c.get("open")

        # use SQLAlchemy merge which does INSERT OR REPLACE semantics for primary key collisions
        session.merge(Candle(instrument_id=str(instrument_id), date=date_str, open=open_price))

    now_iso = datetime.now(timezone.utc).replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

    # upsert instrument's last_candles_fetch and weekday_bitmask
    inst = session.get(Instrument, str(instrument_id))
   
    if inst is None:
        inst = Instrument(instrument_id=str(instrument_id), last_candles_fetch=now_iso, weekday_bitmask=weekday_bitmask)
    else:
        inst.last_candles_fetch = now_iso
        inst.weekday_bitmask = weekday_bitmask

    session.merge(inst)
    session.commit()

def save_instrument(session: Session, id, symbol, name) -> Instrument:
    i = session.get(Instrument, str(id))

    if i is None:
        i = Instrument(instrument_id=str(id), symbol=symbol, name=name)
    else:
        if symbol is not None:
            i.symbol = symbol
        if name is not None:
            i.name = name

    session.merge(i)
    session.commit()

    return i

def fetch_instruments(session: Session) -> list[Instrument]:
    instruments = fetch_watchlists_and_instruments()
    if not instruments:
        print("Keine Watchlist-Instrumente gefunden.", file=sys.stderr)
        sys.exit(1)

    inst = []
    for iid, r in instruments.items():
        instrument_id = r.get("itemId")
        market = r.get("market")

        if market is None:
            symbol = None
            name = None
        else:
            symbol = market.get("symbolName")
            name = market.get("displayName")            
            
        i = save_instrument(session, instrument_id, symbol, name)

        inst.append(i) 

    return inst

def load_instruments(session: Session) -> list[Instrument]:
    stmt = select(Instrument).order_by(Instrument.instrument_id)
    return session.execute(stmt).scalars().all()

def load_instrument_weekday_bitmask(session: Session, instrument_id) -> int | None:
    inst = session.get(Instrument, str(instrument_id))

    if not inst:
        return None
    
    return inst.weekday_bitmask

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

def fetch_watchlists():
    return http_get("/watchlists")["watchlists"]

def fetch_watchlist_detail(wl_id):
    return http_get(f"/watchlists/{wl_id}")

def fetch_watchlists_and_instruments():
    unique = {}

    wlists = fetch_watchlists()
    for wl in wlists:
        wl_id = wl.get("watchlistId")
        if not wl_id:
            continue
        try:
            detail = fetch_watchlist_detail(wl_id)
        except Exception as e:
            print(f"WARN: Watchlist {wl_id} nicht geladen: {e}", file=sys.stderr)

        print(f"Lade Watchlist: {wl.get('name')}")

        if assets := wl['items']:
            for a in assets:
                if a.get("itemType") == "Instrument" and (key := a.get("itemId")):
                    unique[key] = a

    return unique

def fetch_candles(instrument_id, granularity="OneDay", count: int = 1000):
    path = f"/market-data/instruments/{instrument_id}/history/candles/asc/{granularity}/{count}"
    result = http_get(path)

    return result["candles"][0]["candles"]

# --- Calculation ---
def pct_change(start, end):
    return (end - start) / start * 100.0

def parse_db_date(dstr):
    return datetime.fromisoformat(dstr.replace("Z", "+00:00")).astimezone(pytz.UTC)

def avg_annual_return_5y(candles: list[Candle], from_5y):
    
    if not candles or len(candles) < 2:
        return None
    
    dstr = candles[0].date  
    d = parse_db_date(dstr)
    
    if d < from_5y:
        return None

    start_price = candles[0].open
    end_price = candles[-1].open

    years = 5.0
    return ((end_price / start_price) ** (1.0 / years) - 1.0) * 100.0
    
def compute_metrics_with_cache(session: Session, instrument_id, fetch_remotely: bool):
    dt_now = datetime.now(timezone.utc)

    from_5y = dt_now - relativedelta(years=5)
    from_3m = dt_now - relativedelta(months=3)
    from_1m = dt_now - relativedelta(months=1)

    cached = load_candles(session, instrument_id, from_5y, dt_now)

    weekday_now = dt_now.isoweekday()
    weekday_bitmask_now = weekday_bitmask = 1 << weekday_now
       
    weekday_bitmask = load_instrument_weekday_bitmask(session, instrument_id)
   
    if weekday_bitmask is None:
        weekday_bitmask = weekday_bitmask_now

        granularity = 'OneWeek'
        count = 1000
    else:
        last_cached = parse_db_date(cached[-1].date)

        delta = dt_now - last_cached
        days = delta.days 

        if (weekday_bitmask & weekday_bitmask_now) and days < 1000:
            granularity = 'OneDay'
            count = days
        else:
            weekday_bitmask |= weekday_bitmask_now

            granularity = 'OneWeek'
            count = 1000
        
    if fetch_remotely and (len(cached) == 0 or count):
        candles = fetch_candles(instrument_id, granularity=granularity, count=count)

        if candles:
            save_candles(session, instrument_id, candles, weekday_bitmask)

        cached = load_candles(session, instrument_id, from_5y, dt_now)

    c_sorted = sorted(cached, key=lambda x: x.date)

    cagr_5y = avg_annual_return_5y(c_sorted, from_5y)
    
    latest_price = c_sorted[-1].open if c_sorted else None

    price_3m_start = None
    price_1m_start = None

    for c in c_sorted:
        dstr = c.date
        
        d = parse_db_date(dstr)
   
        if price_3m_start is None and d >= from_3m:
            price_3m_start = c.open
        if price_1m_start is None and d >= from_1m:
            price_1m_start = c.open
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

    p.add_argument("-x", type=float, default=20, required=False, help="min avg annual percentual change over last 5 years")
    p.add_argument("-y", type=float, default=-10, required=False, help="min percentual change over last 3 months")
    p.add_argument("-z", type=float, default=0, required=False, help="min percentual change in last month")

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

    engine = init_db()
    with Session(engine) as session:

        if fetch:
            inst = fetch_instruments(session)
        else:
            inst = load_instruments(session)

        print("Symbol\tName\tInstrumentId\t5y_annual_change(%)\t3m_change(%)\t1m_change(%)")

        for i in inst:
            instrument_id = i.instrument_id
            symbol = i.symbol
            name = i.name

            try:
                metrics = compute_metrics_with_cache(session, instrument_id, fetch)
            except Exception as e:
                print(f"WARN: Fehler beim Laden/Cache für {instrument_id}: {e}", file=sys.stderr)
                continue

            if metrics["5y_annual_change"] is None or metrics["3m_change"] is None or metrics["1m_change"] is None:
                continue

            if metrics["5y_annual_change"] >= min_x and metrics["3m_change"] <= min_y and metrics["1m_change"] >= min_z:
                print_row(symbol, name, instrument_id, metrics)

if __name__ == "__main__":
    main()