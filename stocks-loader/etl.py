# etl.py — JSON → Postgres (no files)
# pip install: pandas requests sqlalchemy psycopg2-binary

import os, pandas as pd, requests as rq
from sqlalchemy import create_engine, text

PG_HOST=os.environ["PG_HOST"]
PG_PORT=os.environ.get("PG_PORT","5432")
PG_DATABASE=os.environ["PG_DATABASE"]
PG_USER=os.environ["PG_USER"]
PG_PASSWORD=os.environ["PG_PASSWORD"]

# Must return JSON array of rows. Example schema shown below.
DATA_API=os.environ["DATA_API"]   # e.g. https://example.com/intraday_bullish.json

REQUIRED_COLS = [
  "TradeDateHour","Ticker","Exchange","StockName","Sector","LastGBP","GapPct",
  "RelVol","AvgVol30d","Range52wPos","ATRpct","FloatShares","FreeFloatPct",
  "ShortInterestPct","MarketCapGBP","Conviction","Catalyst","SourceURL","NewsTimestamp"
]

def fetch_json():
    r = rq.get(DATA_API, timeout=60)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise ValueError("DATA_API must return a JSON array of row objects.")
    return data

def normalize(rows):
    df = pd.DataFrame(rows)
    # Ensure all required columns exist
    for c in REQUIRED_COLS:
        if c not in df.columns:
            df[c] = None
    # Types
    df["TradeDateHour"] = pd.to_datetime(df["TradeDateHour"], utc=True, errors="coerce")
    df["NewsTimestamp"] = pd.to_datetime(df["NewsTimestamp"], utc=True, errors="coerce")
    return df[REQUIRED_COLS]

def engine():
    uri = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    return create_engine(uri, pool_pre_ping=True, pool_recycle=300)

def upsert(df, eng):
    with eng.begin() as conn:
        conn.exec_driver_sql("CREATE TEMP TABLE _stage (LIKE BullishStocks INCLUDING ALL);")
        df.to_sql("_stage", conn, if_exists="append", index=False)
        conn.execute(text("""
        INSERT INTO BullishStocks AS t (
          TradeDateHour, Ticker, Exchange, StockName, Sector, LastGBP, GapPct, RelVol,
          AvgVol30d, Range52wPos, ATRpct, FloatShares, FreeFloatPct, ShortInterestPct,
          MarketCapGBP, Conviction, Catalyst, SourceURL, NewsTimestamp
        )
        SELECT
          TradeDateHour, Ticker, Exchange, StockName, Sector, LastGBP, GapPct, RelVol,
          AvgVol30d, Range52wPos, ATRpct, FloatShares, FreeFloatPct, ShortInterestPct,
          MarketCapGBP, Conviction, Catalyst, SourceURL, NewsTimestamp
        FROM _stage
        ON CONFLICT (TradeDateHour, Ticker) DO UPDATE SET
          Exchange=EXCLUDED.Exchange,
          StockName=EXCLUDED.StockName,
          Sector=EXCLUDED.Sector,
          LastGBP=EXCLUDED.LastGBP,
          GapPct=EXCLUDED.GapPct,
          RelVol=EXCLUDED.RelVol,
          AvgVol30d=EXCLUDED.AvgVol30d,
          Range52wPos=EXCLUDED.Range52wPos,
          ATRpct=EXCLUDED.ATRpct,
          FloatShares=EXCLUDED.FloatShares,
          FreeFloatPct=EXCLUDED.FreeFloatPct,
          ShortInterestPct=EXCLUDED.ShortInterestPct,
          MarketCapGBP=EXCLUDED.MarketCapGBP,
          Conviction=EXCLUDED.Conviction,
          Catalyst=EXCLUDED.Catalyst,
          SourceURL=EXCLUDED.SourceURL,
          NewsTimestamp=EXCLUDED.NewsTimestamp;
        """))
    print(f"Upserted {len(df)} rows.")

if __name__ == "__main__":
    rows = fetch_json()
    df = normalize(rows)
    eng = engine()
    upsert(df, eng)
    print("Done.")
