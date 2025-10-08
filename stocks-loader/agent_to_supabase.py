# agent_to_supabase.py
# pip install openai==1.* requests pandas python-dateutil

import os, json, time, math, requests as rq
import pandas as pd
from datetime import datetime, timezone
from dateutil import parser as dtparser
from typing import List, Dict, Any

# === ENV VARS (set these in your GitHub Actions secrets) ===
OPENAI_API_KEY        = os.environ["OPENAI_API_KEY"]
SUPABASE_PROJECT_URL  = os.environ["SUPABASE_PROJECT_URL"].rstrip("/")
SUPABASE_SERVICE_ROLE = os.environ["SUPABASE_SERVICE_ROLE"]

# Optional overrides
OPENAI_MODEL          = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # fast & cheap; adjust if you prefer
MAX_ROWS              = int(os.getenv("MAX_ROWS", "100"))         # cap rows per run
BATCH_SIZE            = int(os.getenv("BATCH_SIZE", "200"))       # upsert chunk size to Supabase
TIMEOUT_SECS          = int(os.getenv("TIMEOUT_SECS", "60"))

# === TABLE NAME IN SUPABASE ===
TABLE = "intradaybullishstocks"  # must exist with the lower-case schema & PK (tradedatehour, ticker)

# === PROMPT (your exact logic, tuned for JSON output) ===
PROMPT = f"""
You are a precise data extractor. Return up to {MAX_ROWS} UK- and Europe-listed stocks priced under £5 per share
(or local-currency equivalent converted to GBP) that look likely to jump based on fresh, positive catalysts in the past 24–48 hours.

Hard filters:
- Last price < £5 (converted from local currency to GBP if needed).
- Average daily volume ≥ 100,000 shares.
- Exclude tickers with clearly negative same-day news.

Catalyst focus (any of):
- Earnings beats or raised guidance.
- Regulatory approvals (e.g., CE mark, FDA/EMA, licenses).
- Major contracts / orders / partnerships.
- M&A (including bids/approvals), divestments that unlock value.
- Analyst upgrades / price-target hikes.
- Unusually high volume driven by positive news/rumors.

For each stock, return an object with EXACT lower-case keys, matching this schema:
[
  {{
    "tradedatehour": "YYYY-MM-DDTHH:00:00Z",   // UTC hour snapshot
    "ticker": "ABC.L",
    "exchange": "LSE" | "AIM" | "XETRA" | "EPA" | "...",
    "stockname": "Full company name",
    "sector": "GICS/industry (short)",
    "lastgbp": 1.23,                            // last price in GBP
    "gappct": 9.5,                              // premarket/open gap % (or 0 if not available)
    "relvol": 2.7,                              // today volume / 30d average (or 1 if unavailable)
    "avgvol30d": 250000,                        // integer shares
    "range52wpos": 0.62,                        // 0=52w low, 1=52w high (approx if needed)
    "atrpct": 4.2,                              // 14d ATR as % of price (approx if needed)
    "floatshares": 120000000,                   // integer if known (else null)
    "freefloatpct": 85.0,                       // % if known (else null)
    "shortinterestpct": 3.1,                    // % if known (else null)
    "marketcapgbp": 320000000,                  // integer GBP (approx ok)
    "conviction": "High" | "Medium" | "Watchlist",
    "catalyst": "1–2 sentences on the fresh, positive catalyst with specifics",
    "sourceurl": "PRIMARY SOURCE (RNS/regulator/company PR). Use Reuters/FT only if primary not available.",
    "newstimestamp": "YYYY-MM-DDTHH:MM:00Z"     // UTC time of the news item
  }}
]

Sort as:
- High conviction first, then by: larger gappct desc, higher relvol desc, newer newstimestamp desc.
- Break ties by smaller floatshares (if known), then higher shortinterestpct.

Output ONLY valid JSON array (no prose, no markdown).
If you cannot find any valid stocks under these constraints, output [] (an empty JSON array).
"""

# ============== OpenAI client (v1) ==============
from openai import OpenAI
client = OpenAI(api_key=OPENAI_API_KEY)

def call_model() -> List[Dict[str, Any]]:
    """Call the model and return a JSON list (possibly empty)."""
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "system", "content": "You are a precise data extractor. Always output valid JSON."},
            {"role": "user", "content": PROMPT},
        ],
        temperature=0.2,
    )
    content = resp.choices[0].message.content.strip()
    try:
        data = json.loads(content)
        if isinstance(data, dict) and "data" in data:
            data = data["data"]
        if not isinstance(data, list):
            raise ValueError("Model returned a non-list JSON structure.")
        return data[:MAX_ROWS]
    except Exception as e:
        raise RuntimeError(f"Model did not return valid JSON. Error: {e}\nReturned: {content[:1200]}")

# ============== Normalization / Validation ==============
REQUIRED = [
    "tradedatehour","ticker","exchange","stockname","sector","lastgbp","gappct","relvol",
    "avgvol30d","range52wpos","atrpct","floatshares","freefloatpct","shortinterestpct",
    "marketcapgbp","conviction","catalyst","sourceurl","newstimestamp"
]

def normalize_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not rows:
        return []
    df = pd.DataFrame(rows)
    # Lower-case columns to match DB schema exactly
    df.columns = [c.lower() for c in df.columns]
    # Ensure all required cols exist
    for c in REQUIRED:
        if c not in df.columns:
            df[c] = None
    # Coerce date-times
    def to_iso_hour(ts):
        if pd.isna(ts): return None
        try:
            dt = dtparser.parse(str(ts))
            dt = dt.astimezone(timezone.utc)
            # floor to hour for tradedatehour
            return dt.replace(minute=0, second=0, microsecond=0).isoformat().replace("+00:00","Z")
        except Exception:
            return None

    def to_iso(ts):
        if pd.isna(ts): return None
        try:
            dt = dtparser.parse(str(ts)).astimezone(timezone.utc)
            return dt.isoformat().replace("+00:00","Z")
        except Exception:
            return None

    # tradedatehour: if missing, use current hour UTC
    if "tradedatehour" in df:
        df["tradedatehour"] = df["tradedatehour"].apply(lambda x: to_iso_hour(x) or to_iso_hour(datetime.utcnow()))
    else:
        df["tradedatehour"] = to_iso_hour(datetime.utcnow())

    # newstimestamp
    df["newstimestamp"] = df["newstimestamp"].apply(to_iso)

    # Coerce numerics safely
    numeric_cols = ["lastgbp","gappct","relvol","avgvol30d","range52wpos","atrpct",
                    "floatshares","freefloatpct","shortinterestpct","marketcapgbp"]
    for col in numeric_cols:
        def to_num(x):
            try:
                if x is None or (isinstance(x, str) and x.strip() == ""): return None
                return float(x)
            except Exception:
                return None
        df[col] = df[col].apply(to_num)

    # Minimal sanity: drop rows missing essentials
    df = df.dropna(subset=["tradedatehour","ticker","stockname","sourceurl"], how="any")

    # Keep only required columns in order
    df = df[REQUIRED]
    # Convert numerics to int where appropriate
    for col in ["avgvol30d","floatshares","marketcapgbp"]:
        df[col] = df[col].apply(lambda v: int(v) if pd.notna(v) and float(v).is_integer() else (int(v) if pd.notna(v) else None))

    return df.to_dict(orient="records")

# ============== Supabase upsert ==============
def supabase_upsert(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    url = f"{SUPABASE_PROJECT_URL}/rest/v1/{TABLE}"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        chunk = rows[i:i+BATCH_SIZE]
        # Retry a couple of times on transient errors
        for attempt in range(3):
            try:
                r = rq.post(url, headers=headers, json=chunk, timeout=TIMEOUT_SECS)
                if r.status_code in (200, 201, 204):
                    total += len(chunk)
                    break
                else:
                    raise RuntimeError(f"Supabase upsert failed: {r.status_code} {r.text[:500]}")
            except Exception as e:
                if attempt < 2:
                    time.sleep(2 ** attempt)
                    continue
                raise
    return total

# ============== MAIN ==============
def main():
    model_rows = call_model()                 # 1) get rows from the model (JSON)
    rows = normalize_rows(model_rows)         # 2) normalize/validate to DB schema
    upserted = supabase_upsert(rows)          # 3) upsert directly into Supabase REST
    print(json.dumps({
        "timestamp_utc": datetime.utcnow().isoformat() + "Z",
        "model": OPENAI_MODEL,
        "received": len(model_rows),
        "normalized": len(rows),
        "upserted": upserted
    }, indent=2))

if __name__ == "__main__":
    main()
