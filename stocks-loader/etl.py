# etl.py — JSON → Postgres (no files), lower-case columns to match Postgres
# pip install: pandas requests sqlalchemy psycopg2-binary

import os, pandas as pd, requests as rq
from sqlalchemy import create_engine, text

PG_HOST=os.environ["PG_HOST"]
PG_PORT=os.environ.get("PG_PORT","5432")
PG_DATABASE=os.environ["PG_DATABASE"]
PG_USER=os.environ["PG_USER"]
PG_PASSWORD=os.environ["PG_PASSWORD"]

DATA_API=os.environ["DATA_API"]   # HTTPS endpoint returning JSON array

REQUIRED_COLS = [
  "tradedatehour","ticker","exchange","stockname","sector","lastgbp","gappct",
  "relvol","avgvol30d","range52wpos","atrpct","floatshares","freefloatpct",
  "shortinterestpct","marketcapgbp","conviction","catalyst","sourceurl","newstimestamp"
]

def fetch_json():
    r = rq.get(DATA_API, timeout=60)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise ValueError("DATA_API must return a JSON array of row objects.")
    return data

def normalize(rows):
    # Build DF and force lower-case column names to match Postgres
    df = pd.DataFrame(rows)
    df.columns = [c.lower() for c in df.columns]

    # Ensure all required columns exist
    for c in REQUIRED_COLS:
        if c not in df.columns:
            df[c] = None

    # Coerce datetimes
    df["tradedatehour"] = pd.to_datetime(df["tradedatehour"], utc=True, errors="coerce")
    if "newstimestamp" in df.columns:
        df["newstimestamp"] = pd.to_datetime(df["newstimestamp"], utc=True, errors="coerce")

    # Reorder
    return df[REQUIRED_COLS]

def engine():
    uri = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    return create_engine(uri, pool_pre_ping=True, pool_recycle=300)

def upsert(df, eng):
    ddl = """
    CREATE TABLE IF NOT EXISTS public.bullishstocks (
      tradedatehour      timestamptz   NOT NULL,
      ticker             text          NOT NULL,
      exchange           text,
      stockname          text,
      sector             text,
      lastgbp            numeric(18,6),
      gappct             numeric(9,4),
      relvol             numeric(12,4),
      avgvol30d          bigint,
      range52wpos        numeric(9,4),
      atrpct             numeric(9,4),
      floatshares        bigint,
      freefloatpct       numeric(9,4),
      shortinterestpct   numeric(9,4),
      marketcapgbp       bigint,
      conviction         text,
      catalyst           text,
      sourceurl          text,
      newstimestamp      timestamptz,
      PRIMARY KEY (tradedatehour, ticker)
    );
    """
    with eng.begin() as conn:
        # 1) Ensure table exists
        conn.exec_driver_sql(ddl)

        # 2) Temp stage table has the SAME (lower-case) columns
        conn.exec_driver_sql("CREATE TEMP TABLE _stage (LIKE public.bullishstocks INCLUDING ALL);")

        # 3) Insert DataFrame (columns are lower-case, so pandas quotes "tradedatehour" etc. exactly)
        df.to_sql("_stage", conn, if_exists="append", index=False)

        # 4) Merge into target
        conn.execute(text("""
        INSERT INTO public.bullishstocks AS t (
          tradedatehour, ticker, exchange, stockname, sector, lastgbp, gappct, relvol,
          avgvol30d, range52wpos, atrpct, floatshares, freefloatpct, shortinterestpct,
          marketcapgbp, conviction, catalyst, sourceurl, newstimestamp
        )
        SELECT
          tradedatehour, ticker, exchange, stockname, sector, lastgbp, gappct, relvol,
          avgvol30d, range52wpos, atrpct, floatshares, freefloatpct, shortinterestpct,
          marketcapgbp, conviction, catalyst, sourceurl, newstimestamp
        FROM _stage
        ON CONFLICT (tradedatehour, ticker) DO UPDATE SET
          exchange = EXCLUDED.exchange,
          stockname = EXCLUDED.stockname,
          sector = EXCLUDED.sector,
          lastgbp = EXCLUDED.lastgbp,
          gappct = EXCLUDED.gappct,
          relvol = EXCLUDED.relvol,
          avgvol30d = EXCLUDED.avgvol30d,
          range52wpos = EXCLUDED.range52wpos,
          atrpct = EXCLUDED.atrpct,
          floatshares = EXCLUDED.floatshares,
          freefloatpct = EXCLUDED.freefloatpct,
          shortinterestpct = EXCLUDED.shortinterestpct,
          marketcapgbp = EXCLUDED.marketcapgbp,
          conviction = EXCLUDED.conviction,
          catalyst = EXCLUDED.catalyst,
          sourceurl = EXCLUDED.sourceurl,
          newstimestamp = EXCLUDED.newstimestamp;
        """))
    print(f"Upserted {len(df)} rows.")

if __name__ == "__main__":
    rows = fetch_json()
    df = normalize(rows)
    eng = engine()
    upsert(df, eng)
    print("Done.")
