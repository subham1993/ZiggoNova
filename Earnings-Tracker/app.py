# app.py
"""
Monthly Stock Earnings Tracker — Flask Edition (Render-ready)
- No Streamlit
- Accessible from anywhere (deploy on Render free tier)
- Uses Postgres in production (via DATABASE_URL) and falls back to local SQLite for dev

Quickstart (local)
------------------
1) pip install -r requirements.txt
2) python app.py  # http://localhost:5000

Deploy to Render (free)
-----------------------
- Add this repo to Render as a Web Service
- Build: pip install -r requirements.txt
- Start: gunicorn app:app
- Add a free Render Postgres; set DATABASE_URL env var in the web service
"""

from __future__ import annotations
import os
from datetime import date, datetime, timedelta
import calendar
from typing import Tuple, List, Dict, Any

import pandas as pd
from flask import (
    Flask,
    g,
    request,
    redirect,
    url_for,
    render_template_string,
    jsonify,
    flash,
)
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ---------- Config ----------
DB_PATH = os.environ.get("EARNINGS_DB", "earnings.db")
SECRET_KEY = os.environ.get("FLASK_SECRET_KEY", "dev-secret-change-me")
DATABASE_URL = os.environ.get("DATABASE_URL")  # Provided automatically by Render Postgres

app = Flask(__name__)
app.config.update(SECRET_KEY=SECRET_KEY)

# ---------- Database Engine & Migration ----------

def get_engine() -> Engine:
    """Create (once per process) and return the SQLAlchemy engine.
    Uses Postgres when DATABASE_URL is set; otherwise SQLite file.
    """
    if "engine" not in g:
        if DATABASE_URL:
            # Render Postgres URL may be in postgresql:// or postgres://
            g.engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        else:
            g.engine = create_engine(f"sqlite:///{DB_PATH}", pool_pre_ping=True)
        _run_migrations(g.engine)
    return g.engine

@app.teardown_appcontext
def close_engine(exception):
    g.pop("engine", None)


def _run_migrations(engine: Engine) -> None:
    """Create tables if missing and ensure "stock" column exists on earnings.
    Works for both Postgres and SQLite.
    """
    with engine.begin() as conn:
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS earnings (
                id SERIAL PRIMARY KEY,
                d TEXT NOT NULL,
                stock TEXT NOT NULL,
                amount REAL NOT NULL
            )
            """
        ))
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS targets (
                id SERIAL PRIMARY KEY,
                y INTEGER NOT NULL,
                m INTEGER NOT NULL,
                target REAL NOT NULL,
                UNIQUE(y,m)
            )
            """
        ))
        # Try portable column add; ignore if already exists
        try:
            conn.execute(text("ALTER TABLE earnings ADD COLUMN IF NOT EXISTS stock TEXT DEFAULT 'General'"))
        except Exception:
            # SQLite older than 3.35 may not support IF NOT EXISTS; try plain ALTER guarded by pragma
            pass

# ---------- Helpers ----------

def to_df(query: str, params: tuple = ()):
    eng = get_engine()
    with eng.connect() as conn:
        return pd.read_sql_query(text(query), conn, params=params)

def upsert_target(year: int, month: int, target: float):
    eng = get_engine()
    stmt = text(
        """
        INSERT INTO targets (y,m,target) VALUES (:y,:m,:t)
        ON CONFLICT (y,m) DO UPDATE SET target = excluded.target
        """
    )
    with eng.begin() as conn:
        conn.execute(stmt, {"y": year, "m": month, "t": float(target)})

def insert_earning(d: date, stock: str, amount: float):
    stock = (stock or "").strip() or "General"
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text("INSERT INTO earnings (d, stock, amount) VALUES (:d,:s,:a)"),
                     {"d": d.isoformat(), "s": stock, "a": float(amount)})

def delete_earning(entry_id: int):
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text("DELETE FROM earnings WHERE id=:id"), {"id": entry_id})

def month_bounds(year: int, month: int) -> Tuple[date, date]:
    first = date(year, month, 1)
    last = date(year, month, calendar.monthrange(year, month)[1])
    return first, last

def business_days_between(start: date, end: date) -> int:
    day_count = 0
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            day_count += 1
        cur += timedelta(days=1)
    return max(0, day_count)

# ---------- Views ----------

BASE_HTML = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Monthly Stock Earnings Tracker</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <style>
    .metric { font-weight: 700; font-size: 1.1rem; }
    .card-soft { border-radius: 1rem; box-shadow: 0 10px 25px rgba(0,0,0,0.05); }
    .table-sm td, .table-sm th { padding: .35rem .5rem; }
  </style>
</head>
<body class="bg-light">
<nav class="navbar navbar-expand-lg bg-white border-bottom mb-4">
  <div class="container">
    <span class="navbar-brand">💹 Monthly Stock Earnings Tracker</span>
  </div>
</nav>
<div class="container">
  {% with messages = get_flashed_messages() %}
    {% if messages %}
      <div class="alert alert-info">{{ messages[0] }}</div>
    {% endif %}
  {% endwith %}
  {{ body|safe }}
</div>
<script>
function post(path, params) {
  const form = document.createElement('form');
  form.method = 'POST';
  form.action = path;
  for (const key in params) {
    const input = document.createElement('input');
    input.type = 'hidden';
    input.name = key;
    input.value = params[key];
    form.appendChild(input);
  }
  document.body.appendChild(form);
  form.submit();
}
</script>
</body>
</html>
"""

INDEX_BODY = r"""
<div class="row g-4">
  <div class="col-lg-3">
    <div class="card card-soft p-3">
      <h5>Settings & Add Entry</h5>
      <form class="mb-3" method="GET">
        <div class="row g-2">
          <div class="col-6">
            <label class="form-label">Year</label>
            <input class="form-control" type="number" name="year" value="{{ year }}" required>
          </div>
          <div class="col-6">
            <label class="form-label">Month</label>
            <input class="form-control" type="number" name="month" min="1" max="12" value="{{ month }}" required>
          </div>
        </div>
        <div class="form-text">Press Enter to switch month</div>
      </form>

      <form class="mb-3" method="POST" action="{{ url_for('save_target') }}">
        <label class="form-label">Monthly target</label>
        <input class="form-control" type="number" step="0.01" name="target" value="{{ current_target }}">
        <input type="hidden" name="year" value="{{ year }}">
        <input type="hidden" name="month" value="{{ month }}">
        <button class="btn btn-primary w-100 mt-2">Save target</button>
      </form>

      <hr>
      <h6>Add Earning Entry</h6>
      <form method="POST" action="{{ url_for('add_entry') }}">
        <label class="form-label">Date</label>
        <input class="form-control" type="date" name="d" value="{{ today_iso }}" required>
        <label class="form-label mt-2">Stock name</label>
        <input class="form-control" type="text" name="stock" placeholder="e.g., AAPL">
        <label class="form-label mt-2">Earning for the day</label>
        <input class="form-control" type="number" step="0.01" name="amount" value="0.00">
        <button class="btn btn-success w-100 mt-3">Add</button>
      </form>
    </div>
  </div>
  <div class="col-lg-9">
    <div class="card card-soft p-3 mb-3">
      <h5 class="mb-3">Monthly Overview</h5>
      <div class="row text-center">
        <div class="col"><div class="metric">Target<br><span class="text-muted">{{ fmt(new_target) }}</span></div></div>
        <div class="col"><div class="metric">Earned so far<br><span class="text-muted">{{ fmt(earned) }}</span></div></div>
        <div class="col"><div class="metric">Remaining<br><span class="text-muted">{{ fmt(remaining_amt) }}</span></div></div>
        <div class="col"><div class="metric">Days left ({{ 'weekdays' if use_weekdays else 'days' }})<br><span class="text-muted">{{ base_days_left }}</span></div></div>
        <div class="col"><div class="metric">Needed per day<br><span class="text-muted">{{ fmt(required_per_day) }}</span></div></div>
      </div>
      {% if new_target > 0 %}
        {% if earned >= new_target %}
          <div class="alert alert-success mt-3">🎯 Goal achieved for this month! Great job.</div>
        {% elif earned >= 0.5 * new_target %}
          <div class="alert alert-info mt-3">Halfway there — you’ve reached at least 50% of your target.</div>
        {% endif %}
      {% endif %}
      <form class="form-check form-switch mt-2" method="GET">
        <input type="hidden" name="year" value="{{ year }}">
        <input type="hidden" name="month" value="{{ month }}">
        <input class="form-check-input" type="checkbox" role="switch" id="use_weekdays" name="use_weekdays" value="1" {% if use_weekdays %}checked{% endif %} onchange="this.form.submit()">
        <label class="form-check-label" for="use_weekdays">Use weekdays only for 'days left'</label>
      </form>
    </div>

    <div class="row g-3">
      <div class="col-md-6">
        <div class="card card-soft p-3">
          <div class="small text-muted">Daily earnings (bar)</div>
          <canvas id="dailyBar" height="220"></canvas>
        </div>
      </div>
      <div class="col-md-6">
        <div class="card card-soft p-3">
          <div class="small text-muted">Per-stock share (pie)</div>
          <canvas id="stockPie" height="220"></canvas>
        </div>
      </div>
    </div>

    <div class="card card-soft p-3 mt-3">
      <h5>This month's entries</h5>
      {% if month_rows|length == 0 %}
        <div class="alert alert-light">No entries for this month yet.</div>
      {% else %}
        <form method="POST" action="{{ url_for('delete_selected') }}">
          <input type="hidden" name="year" value="{{ year }}">
          <input type="hidden" name="month" value="{{ month }}">
          <div class="table-responsive">
            <table class="table table-sm align-middle">
              <thead><tr><th>Delete</th><th>ID</th><th>Date</th><th>Stock</th><th class="text-end">Amount</th></tr></thead>
              <tbody>
                {% for r in month_rows %}
                <tr>
                  <td><input class="form-check-input" type="checkbox" name="del" value="{{ r['id'] }}"></td>
                  <td>{{ r['id'] }}</td>
                  <td>{{ r['d'] }}</td>
                  <td>{{ r['stock'] }}</td>
                  <td class="text-end">{{ fmt(r['amount']) }}</td>
                </tr>
                {% endfor %}
              </tbody>
            </table>
          </div>
          <button class="btn btn-outline-danger">Delete selected</button>
        </form>

        <hr>
        <div class="small text-muted mb-2">Cumulative by day (line)</div>
        <canvas id="cumLine" height="220"></canvas>

        <h5 class="mt-4">Per-stock totals for the selected month</h5>
        <div class="table-responsive">
          <table class="table table-sm">
            <thead><tr><th>Stock</th><th class="text-end">Total</th></tr></thead>
            <tbody>
              {% for s in per_stock %}
              <tr><td>{{ s['stock'] }}</td><td class="text-end">{{ fmt(s['total']) }}</td></tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
      {% endif %}
    </div>

    <div class="card card-soft p-3 mt-3">
      <details>
        <summary>📅 Monthly summary history</summary>
        {% if ym_list|length == 0 %}
          <div class="text-muted small">No history yet.</div>
        {% else %}
          <form class="row g-2 mt-2" method="GET">
            <div class="col-6 col-md-4">
              <label class="form-label">Pick a month</label>
              <select class="form-select" name="sel_ym" onchange="const [y,m]=this.value.split('-'); this.form.year.value=y; this.form.month.value=m; this.form.submit();">
                {% for ym in ym_list %}
                  <option value="{{ ym }}" {% if ym==default_ym %}selected{% endif %}>{{ ym }}</option>
                {% endfor %}
              </select>
            </div>
            <input type="hidden" name="year" value="{{ year }}">
            <input type="hidden" name="month" value="{{ month }}">
          </form>
          {% if summary %}
          <div class="row text-center mt-3">
            <div class="col"><div class="metric">Total earned<br><span class="text-muted">{{ fmt(summary.total) }}</span></div></div>
            <div class="col"><div class="metric"># entries<br><span class="text-muted">{{ summary.num }}</span></div></div>
            <div class="col"><div class="metric">Top stock<br><span class="text-muted">{{ summary.top or '—' }}</span></div></div>
            <div class="col"><div class="metric">Worst stock<br><span class="text-muted">{{ summary.worst or '—' }}</span></div></div>
          </div>
          {% endif %}
        {% endif %}
      </details>
    </div>
  </div>
</div>

<script>
async function loadCharts() {
  const resp = await fetch('{{ url_for('month_data_api') }}?year={{ year }}&month={{ month }}');
  const data = await resp.json();
  // Daily bar
  const ctx1 = document.getElementById('dailyBar');
  new Chart(ctx1, { type: 'bar', data: { labels: data.daily.labels, datasets: [{ label: 'Amount', data: data.daily.values }] } });
  // Stock pie
  const ctx2 = document.getElementById('stockPie');
  new Chart(ctx2, { type: 'pie', data: { labels: data.by_stock.labels, datasets: [{ data: data.by_stock.values }] } });
  // Cumulative line
  const ctx3 = document.getElementById('cumLine');
  new Chart(ctx3, { type: 'line', data: { labels: data.cumulative.labels, datasets: [{ label: 'Cumulative', data: data.cumulative.values }] } });
}
loadCharts();
</script>
"""

@app.route("/")
def index():
    today = date.today()
    year = int(request.args.get("year", today.year))
    month = int(request.args.get("month", today.month))

    # toggle weekdays via checkbox
    use_weekdays = request.args.get("use_weekdays", "1") == "1"

    # Target value
    tgt_df = to_df("SELECT target FROM targets WHERE y=:y AND m=:m", {"y": year, "m": month})
    current_target = float(tgt_df["target"].iloc[0]) if not tgt_df.empty else 0.0

    # Month data
    first_day, last_day = month_bounds(year, month)
    month_df = to_df(
        "SELECT id, d, stock, amount FROM earnings WHERE d BETWEEN :a AND :b ORDER BY d ASC, id ASC",
        {"a": first_day.isoformat(), "b": last_day.isoformat()},
    )
    if not month_df.empty:
        month_df["d"] = pd.to_datetime(month_df["d"]).dt.date

    earned = float(month_df["amount"].sum()) if not month_df.empty else 0.0
    new_target = current_target
    remaining_amt = float(new_target - earned)
    now = date.today()
    clamped_today = min(max(now, first_day), last_day)
    calendar_days_left = max(0, (last_day - clamped_today).days)
    weekdays_left = business_days_between(clamped_today + timedelta(days=1), last_day)
    base_days_left = weekdays_left if use_weekdays else calendar_days_left
    required_per_day = (remaining_amt / base_days_left) if base_days_left > 0 else 0.0

    # Table rows
    month_rows: List[Dict[str, Any]] = []
    if not month_df.empty:
        for _, r in month_df.iterrows():
            month_rows.append({
                "id": int(r["id"]),
                "d": r["d"].isoformat() if isinstance(r["d"], date) else str(r["d"]),
                "stock": str(r["stock"]),
                "amount": float(r["amount"]),
            })

    # Per-stock totals
    per_stock: List[Dict[str, Any]] = []
    if not month_df.empty:
        by_stock = month_df.groupby("stock", as_index=False)["amount"].sum().sort_values("amount", ascending=False)
        for _, r in by_stock.iterrows():
            per_stock.append({"stock": r["stock"], "total": float(r["amount"])})

    # History summary
    months_df = to_df("SELECT DISTINCT substr(d,1,7) AS ym FROM earnings ORDER BY ym DESC")
    ym_list = months_df["ym"].tolist() if not months_df.empty else []
    default_ym = f"{year:04d}-{month:02d}"

    summary = None
    if default_ym in ym_list:
        y_sel, m_sel = map(int, default_ym.split("-"))
        f, l = month_bounds(y_sel, m_sel)
        hist = to_df(
            "SELECT d, stock, amount FROM earnings WHERE d BETWEEN :a AND :b",
            {"a": f.isoformat(), "b": l.isoformat()},
        )
        if not hist.empty:
            total = float(hist["amount"].sum())
            num = int(len(hist))
            by_s = hist.groupby("stock", as_index=False)["amount"].sum()
            top = by_s.sort_values("amount", ascending=False).head(1)
            worst = by_s.sort_values("amount", ascending=True).head(1)
            summary = type("S", (), {
                "total": total,
                "num": num,
                "top": top.iloc[0]["stock"] if not top.empty else None,
                "worst": worst.iloc[0]["stock"] if not worst.empty else None,
            })

    def fmt(x: float) -> str:
        return f"{x:,.2f}"

    body = render_template_string(
        INDEX_BODY,
        year=year,
        month=month,
        today_iso=today.isoformat(),
        current_target=current_target,
        new_target=new_target,
        earned=earned,
        remaining_amt=remaining_amt,
        base_days_left=base_days_left,
        required_per_day=required_per_day,
        use_weekdays=use_weekdays,
        month_rows=month_rows,
        per_stock=per_stock,
        ym_list=ym_list,
        default_ym=default_ym,
        summary=summary,
        fmt=fmt,
    )

    return render_template_string(BASE_HTML, body=body)

@app.post("/save_target")
def save_target():
    year = int(request.form["year"]) 
    month = int(request.form["month"]) 
    target = float(request.form.get("target", 0.0))
    upsert_target(year, month, target)
    flash("Target saved.")
    return redirect(url_for("index", year=year, month=month))

@app.post("/add_entry")
def add_entry():
    d = request.form.get("d") or date.today().isoformat()
    stock = request.form.get("stock", "").strip()
    amount = float(request.form.get("amount", 0.0))
    insert_earning(datetime.fromisoformat(d).date(), stock, amount)
    flash(f"Added {amount:+.2f} for {stock or 'General'} on {d}.")
    year = int(request.args.get("year", datetime.fromisoformat(d).year))
    month = int(request.args.get("month", datetime.fromisoformat(d).month))
    return redirect(url_for("index", year=year, month=month))

@app.post("/delete_selected")
def delete_selected():
    year = int(request.form.get("year"))
    month = int(request.form.get("month"))
    ids = request.form.getlist("del")
    if ids:
        for _id in ids:
            delete_earning(int(_id))
        flash(f"Deleted {len(ids)} entr{'y' if len(ids)==1 else 'ies' }.")
    else:
        flash("No rows were selected.")
    return redirect(url_for("index", year=year, month=month))

@app.get("/api/month_data")
def month_data_api():
    year = int(request.args.get("year"))
    month = int(request.args.get("month"))
    first_day, last_day = month_bounds(year, month)
    df = to_df(
        "SELECT d, stock, amount FROM earnings WHERE d BETWEEN :a AND :b ORDER BY d ASC",
        {"a": first_day.isoformat(), "b": last_day.isoformat()},
    )
    if df.empty:
        return jsonify({
            "daily": {"labels": [], "values": []},
            "by_stock": {"labels": [], "values": []},
            "cumulative": {"labels": [], "values": []},
        })
    df["d"] = pd.to_datetime(df["d"]).dt.date

    # Daily
    daily = df.groupby("d", as_index=False)["amount"].sum()
    daily_labels = [d.isoformat() for d in daily["d"].tolist()]
    daily_values = [float(x) for x in daily["amount"].tolist()]

    # By stock (pie uses absolute for share)
    by_stock = df.groupby("stock", as_index=False)["amount"].sum()
    by_labels = by_stock["stock"].tolist()
    by_values = [abs(float(x)) for x in by_stock["amount"].tolist()]

    # Cumulative
    cum = daily.copy()
    cum["cumulative"] = cum["amount"].cumsum()
    cum_labels = daily_labels
    cum_values = [float(x) for x in cum["cumulative"].tolist()]

    return jsonify({
        "daily": {"labels": daily_labels, "values": daily_values},
        "by_stock": {"labels": by_labels, "values": by_values},
        "cumulative": {"labels": cum_labels, "values": cum_values},
    })

if __name__ == "__main__":
    # Dev server; for production use gunicorn/uwsgi
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
