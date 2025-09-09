# pip install pandas requests sqlalchemy psycopg2-binary openpyxl
import os, io, pandas as pd, requests as rq
from sqlalchemy import create_engine, text

PG_HOST=os.environ["PG_HOST"]; PG_PORT=os.environ.get("PG_PORT","5432")
PG_DATABASE=os.environ["PG_DATABASE"]; PG_USER=os.environ["PG_USER"]
PG_PASSWORD=os.environ["PG_PASSWORD"]

DATA_URL=os.environ.get("DATA_URL","")   # HTTPS link to your hourly CSV/XLSX
LOCAL_FILE=os.environ.get("LOCAL_FILE","")  # fallback for testing

def load_dataframe():
    if DATA_URL:
        r=rq.get(DATA_URL,timeout=60); r.raise_for_status()
        b=io.BytesIO(r.content)
        return pd.read_csv(b) if DATA_URL.lower().endswith(".csv") else pd.read_excel(b)
    if LOCAL_FILE:
        return pd.read_csv(LOCAL_FILE) if LOCAL_FILE.lower().endswith(".csv") else pd.read_excel(LOCAL_FILE)
    raise RuntimeError("Set DATA_URL or LOCAL_FILE")

def engine():
    uri=f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    return create_engine(uri, pool_pre_ping=True, pool_recycle=300)

def upsert(df, eng):
    if "TradeDateHour" in df.columns:
        df["TradeDateHour"]=pd.to_datetime(df["TradeDateHour"], utc=True, errors="coerce")
    if "NewsTimestamp" not in df.columns:
        df["NewsTimestamp"]=pd.NaT
    with eng.begin() as c:
        c.exec_driver_sql("CREATE TEMP TABLE _stage (LIKE IntradayBullishStocks INCLUDING ALL);")
        df.to_sql("_stage", c, if_exists="append", index=False)
        c.execute(text("""
        INSERT INTO IntradayBullishStocks AS t (
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
          Exchange=EXCLUDED.Exchange, StockName=EXCLUDED.StockName, Sector=EXCLUDED.Sector,
          LastGBP=EXCLUDED.LastGBP, GapPct=EXCLUDED.GapPct, RelVol=EXCLUDED.RelVol,
          AvgVol30d=EXCLUDED.AvgVol30d, Range52wPos=EXCLUDED.Range52wPos, ATRpct=EXCLUDED.ATRpct,
          FloatShares=EXCLUDED.FloatShares, FreeFloatPct=EXCLUDED.FreeFloatPct,
          ShortInterestPct=EXCLUDED.ShortInterestPct, MarketCapGBP=EXCLUDED.MarketCapGBP,
          Conviction=EXCLUDED.Conviction, Catalyst=EXCLUDED.Catalyst,
          SourceURL=EXCLUDED.SourceURL, NewsTimestamp=EXCLUDED.NewsTimestamp;
        """))

if __name__=="__main__":
    upsert(load_dataframe(), engine())
    print("Done.")
